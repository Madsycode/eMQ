"""Deploy a real N-broker MMQP fabric on this host, drive load and report scalability.

Each run gets isolated certificates, ports, configurations, state and logs under
--output. Brokers share a static membership with pinned certificates and dial
mutual-TLS (or QUIC) backplane links on demand, when they first exchange work.
Traffic profiles: "bench" runs EmpyKlient load generators and writes a report,
"demo" runs the paced dashboard demo clients, "idle" starts no clients. A comma
list such as --brokers 3,10,30 runs a sweep and compares the deployments.
Uses only the built Empy executables, Python's standard library and OpenSSL.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import threading
import time

from collector import Collector
from config import EMPY_MAX_SOURCES, Settings, Source
from server import DashboardServer

EMPY_NO_WINDOW = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
EMPY_TOKEN_ENV = "EMPY_CLUSTER_TELEMETRY_TOKEN"
EMPY_KEY_ENV = "EMPY_CLUSTER_EPOCH_KEY"
EMPY_CLIENTS_PER_BENCH = 200


def stop_process(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def usage(pid):
    """Resident memory (bytes) and CPU time (seconds) of a live process, or None."""
    try:
        if os.name == "nt":
            import ctypes
            from ctypes import wintypes

            class Counters(ctypes.Structure):
                _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
                    (name, ctypes.c_size_t) for name in ("PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                                                         "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage",
                                                         "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")]

            kernel = ctypes.windll.kernel32
            kernel.OpenProcess.restype = wintypes.HANDLE
            handle = kernel.OpenProcess(0x1000, False, pid)
            if not handle:
                return None
            try:
                counters = Counters()
                counters.cb = ctypes.sizeof(Counters)
                times = [wintypes.FILETIME() for _ in range(4)]
                if not kernel.K32GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb) \
                        or not kernel.GetProcessTimes(handle, *map(ctypes.byref, times)):
                    return None
                ticks = sum((value.dwHighDateTime << 32 | value.dwLowDateTime) for value in times[2:])
                return counters.WorkingSetSize, ticks / 1e7
            finally:
                kernel.CloseHandle(handle)
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        pages = int(Path(f"/proc/{pid}/statm").read_text().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE"), (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, IndexError, AttributeError):
        return None


def percentiles(histogram):
    """Exact merge of the bench processes' deterministic latency buckets."""
    buckets = sorted((int(bound), int(count)) for bound, count in histogram.items() if count)
    total = sum(count for _, count in buckets)
    result = {"count": total}
    for name, fraction in (("p50", 0.5), ("p90", 0.9), ("p99", 0.99), ("p999", 0.999)):
        target, seen, value = math.ceil(total * fraction), 0, 0
        for bound, count in buckets:
            seen += count
            if seen >= target:
                value = bound
                break
        result[name] = value if total else 0
    return result


class Fabric:
    """One isolated deployment: certificates, configuration and broker processes."""

    def __init__(self, args, count, directory, cleanup):
        self.args, self.count, self.directory, self.cleanup = args, count, directory, cleanup
        self.binary = args.bin.resolve()
        suffix = ".exe" if os.name == "nt" else ""
        self.broker = self.binary / ("EmpyBroker" + suffix)
        self.client = self.binary / ("EmpyKlient" + suffix)
        self.names = [f"broker-{index + 1:03}" for index in range(count)]
        self.run = secrets.token_hex(4)  # Unique topics per run: earlier QoS 1 sessions may still be retained.
        self.processes = {}
        self.records = []
        self.environment = os.environ.copy()
        self.environment[EMPY_KEY_ENV] = secrets.token_hex(32)
        self.environment[EMPY_TOKEN_ENV] = secrets.token_hex(32)
        if os.name == "nt":
            windows = Path(os.environ["SystemRoot"])
            self.environment["PATH"] = os.pathsep.join((str(windows / "System32"), str(windows)))

    def openssl(self, *arguments):
        return subprocess.run([self.args.openssl, *arguments], cwd=self.directory, check=True, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, creationflags=EMPY_NO_WINDOW).stdout

    def certificates(self):
        self.openssl("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-keyout", "ca.key",
                     "-out", "ca.pem", "-days", "2", "-subj", "/CN=Empy cluster test",
                     "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign")
        (self.directory / "extensions.cnf").write_text(
            "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\n"
            "extendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n", encoding="utf-8")

        def leaf(index):
            # Every member presents a distinct certificate pinned by SHA-256 in all manifests.
            name = f"broker{index}"
            self.openssl("req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-keyout", f"{name}.key",
                         "-out", f"{name}.csr", "-subj", "/CN=localhost")
            self.openssl("x509", "-req", "-in", f"{name}.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-set_serial", str(index + 2),
                         "-days", "2", "-extfile", "extensions.cnf", "-out", f"{name}.pem")
            return hashlib.sha256(self.openssl("x509", "-in", f"{name}.pem", "-outform", "DER")).hexdigest()

        with ThreadPoolExecutor(max_workers=min(16, os.cpu_count() or 4)) as pool:
            self.fingerprints = list(pool.map(leaf, range(self.count)))

    def configure(self):
        streams = []
        for _ in range(self.count * 3):
            stream = socket.socket()
            stream.bind(("127.0.0.1", 0))
            streams.append(stream)
        ports = [stream.getsockname()[1] for stream in streams]
        self.clients, self.backplane, self.telemetry = ports[:self.count], ports[self.count:2 * self.count], ports[2 * self.count:]
        args = self.args
        peers = "".join(f"    - identifier: {name}\n      host: 127.0.0.1\n      port: {self.backplane[i]}\n"
                        f"      server_name: localhost\n      fingerprint: {self.fingerprints[i]}\n" for i, name in enumerate(self.names))
        replicas = min(args.replicas, self.count)
        for i, name in enumerate(self.names):
            (self.directory / f"broker{i}.yaml").write_text(
                f"address: 127.0.0.1\nport: {self.clients[i]}\nworkers: {args.workers}\nmax_sessions: 4096\n"
                f"request_timeout_ms: 15000\n"
                f"telemetry:\n  port: {self.telemetry[i]}\n  interval_ms: 1000\n  max_sessions: 256\n  token_env: {EMPY_TOKEN_ENV}\n"
                f"cluster:\n  identifier: {name}\n  port: {self.backplane[i]}\n  state_directory: state{i}\n"
                f"  epoch: 1\n  shards: {args.shards or max(8, 2 * self.count)}\n  replicas: {replicas}\n  fanout: {args.fanout}\n"
                f"  lease_ms: {args.lease_ms}\n  quic: {'true' if args.quic else 'false'}\n"
                f"  certificate: broker{i}.pem\n  private_key: broker{i}.key\n  authority: ca.pem\n"
                f"  epoch_key_env: {EMPY_KEY_ENV}\n  peers:\n" + peers, encoding="utf-8")
        failover = "".join(f"  - host: 127.0.0.1\n    port: {port}\n" for port in self.clients[:8])
        (self.directory / "client.yaml").write_text(
            "auto_reconnect: true\noperation_timeout_ms: 20000\nfailover_endpoints:\n" + failover, encoding="utf-8")
        for stream in streams:
            stream.close()

    def launch(self, name, executable, *arguments, stdout=None):
        log = self.cleanup.enter_context((self.directory / f"{name}.log").open("wb"))
        output = self.cleanup.enter_context(stdout.open("wb")) if stdout else log
        process = subprocess.Popen([str(executable), *map(str, arguments)], cwd=self.binary, env=self.environment,
                                   stdout=output, stderr=log if stdout else subprocess.STDOUT, creationflags=EMPY_NO_WINDOW)
        self.cleanup.callback(stop_process, process)
        self.processes[name] = process
        self.records.append({"name": name, "pid": process.pid})
        (self.directory / "processes.json").write_text(json.dumps(self.records, indent=4), encoding="utf-8")
        return process

    def ready(self, names, marker, timeout):
        pending = set(names)
        deadline = time.monotonic() + timeout
        while pending:
            for name in sorted(pending):
                text = (self.directory / f"{name}.log").read_text(errors="replace")
                if marker in text:
                    pending.discard(name)
                elif self.processes[name].poll() is not None:
                    raise RuntimeError(f"{name} exited: {text[-2000:]}")
            if pending and time.monotonic() > deadline:
                raise RuntimeError(f"{len(pending)} processes did not report '{marker}'; inspect {self.directory}")
            time.sleep(0.1)

    def start(self):
        started = time.monotonic()
        wave = max(1, self.args.start_concurrency)
        for first in range(0, self.count, wave):
            names = []
            for i in range(first, min(self.count, first + wave)):
                names.append(f"broker{i}")
                self.launch(names[-1], self.broker, "--config", self.directory / f"broker{i}.yaml", "--seconds", self.args.seconds)
            self.ready(names, "MMQP broker listening", 60)
        return time.monotonic() - started

    def sources(self):
        token = self.environment[EMPY_TOKEN_ENV]
        return tuple(Source(name, name.replace("broker-", "Broker "), f"http://127.0.0.1:{self.telemetry[i]}", token)
                     for i, name in enumerate(self.names))

    def brokers(self):
        return [self.processes[f"broker{i}"] for i in range(self.count)]

    def usage(self):
        values = [usage(process.pid) for process in self.brokers() if process.poll() is None]
        return [value for value in values if value]


def mesh(state):
    """Reporting brokers and confirmed peer links from one collector state."""
    online = [source for source in state["sources"] if source["state"] == "online"]
    links = sum(peer["connected"] for source in online for peer in source["data"]["cluster"]["peers"])
    return len(online), links


def totals(state, keys=("publications", "forwarded", "retransmitted", "rejected")):
    online = [source for source in state["sources"] if source["state"] == "online"]
    return {key: sum(source["data"]["metrics"][key] for source in online) for key in keys}


def wait_reporting(fabric, collector, timeout):
    """Every broker serves telemetry. Backplane links are dialed lazily, so none are required yet."""
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        if mesh(collector.state())[0] == fabric.count:
            return time.monotonic() - started
        time.sleep(0.25)
    raise RuntimeError(f"Only {mesh(collector.state())[0]}/{fabric.count} brokers report telemetry after {timeout}s")


def demo(fabric):
    """The original paced dashboard demo traffic, spread across the first brokers."""
    args, clients, config, client = fabric.args, fabric.clients, fabric.directory / "client.yaml", fabric.client
    count = min(3, fabric.count)
    for i in range(count):
        fabric.launch(f"subscriber{i}", client, "subscribe", "sensors/#", "--port", clients[i], "--config", config,
                      "--seconds", args.seconds, "--verbose")
    fabric.launch("responder", client, "respond", "control/status", "ready", "--port", clients[1 % fabric.count],
                  "--config", config, "--seconds", args.seconds, "--verbose")
    fabric.ready([f"subscriber{i}" for i in range(count)] + ["responder"], "Profile ready", 30)
    for i in range(count):
        fabric.launch(f"publisher{i}", client, "publish", f"sensors/zone-{i + 1}/environment",
                      json.dumps({"temperature": 21 + i, "humidity": 44 + i, "unit": "C", "source": "local-demo"}),
                      "--port", clients[i], "--config", config, "--count", 20000,
                      "--interval-ms", 1700 + i * 400, "--seconds", args.seconds)
    fabric.launch("requester", client, "request", "control/status", "status", "--port", clients[2 % fabric.count],
                  "--config", config, "--count", 20000, "--interval-ms", 3000, "--seconds", args.seconds)


def split(total, parts):
    return [total // parts + (index < total % parts) for index in range(parts)]


def bench(fabric, collector):
    """Run EmpyKlient bench processes against every broker and merge their results."""
    args = fabric.args
    processes = args.bench_processes or max(1, math.ceil((args.publishers + args.subscribers) / EMPY_CLIENTS_PER_BENCH))
    processes = min(processes, max(1, min(args.publishers, args.subscribers)))
    publishers, subscribers = split(args.publishers, processes), split(args.subscribers, processes)
    endpoints = ",".join(f"127.0.0.1:{port}" for port in fabric.clients)
    before, cpu_before = totals(collector.state()), sum(cpu for _, cpu in fabric.usage())
    names, offset = [], 0
    for k in range(processes):
        name = f"bench{k}"
        names.append(name)
        # Each process owns its topic prefix, so loss and duplicates are computed locally and exactly.
        fabric.launch(name, fabric.client, "bench", f"bench/{fabric.run}/p{k}", "--endpoints", endpoints,
                      "--publishers", publishers[k], "--subscribers", subscribers[k],
                      "--topics", max(1, min(args.topics, publishers[k])), "--rate", args.rate, "--payload", args.payload,
                      "--qos", args.qos, "--seconds", args.duration, "--drain-ms", 5000, "--offset", offset, "--verbose",
                      stdout=fabric.directory / f"{name}.json")
        offset += publishers[k] + subscribers[k]
    started = time.monotonic()
    peak = []
    while any(fabric.processes[name].poll() is None for name in names):
        if time.monotonic() - started > args.duration + 180:
            raise RuntimeError("Bench processes did not finish; inspect the bench logs")
        sample = fabric.usage()
        if sample and sum(rss for rss, _ in sample) > sum(rss for rss, _ in peak):
            peak = sample
        time.sleep(1)
    cpu_after = sum(cpu for _, cpu in fabric.usage())
    after = totals(collector.state())
    merged, histogram, seconds = {}, {}, 0.0
    for name in names:
        if fabric.processes[name].returncode:
            raise RuntimeError(f"{name} failed: {(fabric.directory / f'{name}.log').read_text(errors='replace')[-2000:]}")
        lines = [line for line in (fabric.directory / f"{name}.json").read_text().splitlines() if line.startswith("{")]
        result = json.loads(lines[-1])
        for key in ("sent", "accepted", "rejected", "failed", "expected", "received", "duplicates", "reordered", "lost"):
            merged[key] = merged.get(key, 0) + result[key]
        for bound, count in result["histogram"]:
            histogram[bound] = histogram.get(bound, 0) + count
        merged["maxLatencyUs"] = max(merged.get("maxLatencyUs", 0), result["latencyUs"]["max"])
        seconds = max(seconds, result["seconds"])
    latency = percentiles(histogram)
    latency["max"] = merged.pop("maxLatencyUs", 0)
    rss = [value for value, _ in peak]
    elapsed = max(1e-9, time.monotonic() - started)
    return {"processes": processes, "seconds": round(seconds, 3), **merged,
            "lossRate": round(merged["lost"] / merged["expected"], 6) if merged["expected"] else 0.0,
            "acceptedPerSecond": round(merged["accepted"] / max(seconds, 1e-9), 1),
            "deliveredPerSecond": round(merged["received"] / max(seconds, 1e-9), 1), "latencyUs": latency,
            "brokerMemory": {"totalMiB": round(sum(rss) / 2 ** 20, 1), "maxMiB": round(max(rss, default=0) / 2 ** 20, 1),
                             "meanMiB": round(sum(rss) / max(1, len(rss)) / 2 ** 20, 1)},
            "brokerCpu": {"seconds": round(cpu_after - cpu_before, 2),
                          "hostPercent": round(100 * (cpu_after - cpu_before) / elapsed / (os.cpu_count() or 1), 1)},
            "telemetry": {key: after[key] - before[key] for key in after}}


def markdown(reports):
    head = ("| Brokers | Active links | Start s | Ready s | Accepted/s | Delivered/s | Lost | Dup | p50 ms | p99 ms | Max ms "
            "| Broker RSS MiB | Broker CPU % |\n|" + " --- |" * 13 + "\n")
    rows = []
    for report in reports:
        result = report.get("bench") or {}
        latency = result.get("latencyUs", {})
        value = lambda key, scale=1, digits=1: "—" if key not in latency else f"{latency[key] / scale:.{digits}f}"
        rows.append(f"| {report['brokers']} | {report.get('activeLinks', 0)} / {report['links']} | {report['startupSeconds']} | {report['readySeconds']} "
                    f"| {result.get('acceptedPerSecond', '—')} | {result.get('deliveredPerSecond', '—')} | {result.get('lost', '—')} "
                    f"| {result.get('duplicates', '—')} | {value('p50', 1000, 2)} | {value('p99', 1000, 2)} | {value('max', 1000, 1)} "
                    f"| {result.get('brokerMemory', {}).get('totalMiB', '—')} | {result.get('brokerCpu', {}).get('hostPercent', '—')} |")
    return head + "\n".join(rows) + "\n"


def run(args, count, directory, serve):
    directory.mkdir(parents=True)
    with ExitStack() as cleanup:
        fabric = Fabric(args, count, directory, cleanup)
        print(f"[{count} brokers] artifacts: {directory}", flush=True)
        fabric.certificates()
        fabric.configure()
        startup = fabric.start()
        print(f"[{count} brokers] all brokers listening after {startup:.1f}s; waiting for telemetry", flush=True)
        collector = Collector(Settings(f"Cluster test · {count} brokers", fabric.sources(), 1.0, 1.5, 900))
        collector.start()
        cleanup.callback(collector.stop)
        server = None
        if serve:
            server = cleanup.enter_context(DashboardServer(("127.0.0.1", args.port), collector))
            threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.25}, daemon=True,
                             name="EmpyDashboard").start()
            cleanup.callback(server.shutdown)
            print(f"[{count} brokers] dashboard: http://127.0.0.1:{server.server_port}", flush=True)
        ready_seconds = wait_reporting(fabric, collector, args.ready_timeout or 30 + count)
        report = {"brokers": count, "links": count * (count - 1) // 2, "transport": "QUIC" if args.quic else "TLS",
                  "shards": args.shards or max(8, 2 * count), "replicas": min(args.replicas, count), "fanout": args.fanout,
                  "startupSeconds": round(startup, 1), "readySeconds": round(ready_seconds, 1), "host": {"cpus": os.cpu_count()},
                  "load": {key: getattr(args, key) for key in ("publishers", "subscribers", "topics", "rate", "payload", "qos", "duration")}}
        print(f"[{count} brokers] all reporting after {ready_seconds:.1f}s; {report['links']} possible links", flush=True)
        if args.traffic == "demo":
            demo(fabric)
        elif args.traffic == "bench":
            print(f"[{count} brokers] bench: {args.publishers} publishers x {args.rate}/s, {args.subscribers} subscribers, "
                  f"{args.payload} B, QoS {args.qos}, {args.duration}s", flush=True)
            report["bench"] = bench(fabric, collector)
            time.sleep(2)  # One more telemetry sample after the last deliveries.
            report["activeLinks"] = mesh(collector.state())[1] // 2
            (directory / "report.json").write_text(json.dumps(report, indent=4), encoding="utf-8")
            (directory / "report.md").write_text(markdown([report]), encoding="utf-8")
            print(markdown([report]), flush=True)
        if server:
            deadline = time.monotonic() + max(0, args.seconds - startup - ready_seconds - 5)
            print(f"[{count} brokers] dashboard stays live; Ctrl+C stops the deployment", flush=True)
            try:
                while time.monotonic() < deadline and all(process.poll() is None for process in fabric.brokers()):
                    time.sleep(0.5)
            except KeyboardInterrupt:
                pass
        return report


def main(argv=None):
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bin", type=Path, default=root / "Targets/BuildRelease/Release")
    parser.add_argument("--openssl", default="openssl")
    parser.add_argument("--output", type=Path, default=root / "Targets/Cluster")
    parser.add_argument("--brokers", default="10", help="Broker count, or a comma list for a sweep (e.g. 3,10,30,100)")
    parser.add_argument("--quic", action="store_true", help="Use the native QUIC backplane instead of mutual TLS")
    parser.add_argument("--shards", type=int, default=0, help="Profile shards (default: max(8, 2 x brokers))")
    parser.add_argument("--replicas", type=int, default=3)
    parser.add_argument("--fanout", type=int, default=4)
    parser.add_argument("--lease-ms", type=int, default=3000)
    parser.add_argument("--workers", type=int, default=1, help="I/O workers per broker")
    parser.add_argument("--start-concurrency", type=int, default=16, help="Brokers launched per startup wave")
    parser.add_argument("--ready-timeout", type=int, default=0, help="Seconds to wait for telemetry (default: 30 + brokers)")
    parser.add_argument("--traffic", choices=("bench", "demo", "idle"), default="bench")
    parser.add_argument("--publishers", type=int, default=20)
    parser.add_argument("--subscribers", type=int, default=20)
    parser.add_argument("--topics", type=int, default=4)
    parser.add_argument("--rate", type=int, default=20, help="Publications per second per publisher (0 = unpaced)")
    parser.add_argument("--payload", type=int, default=128)
    parser.add_argument("--qos", type=int, choices=(0, 1), default=1)
    parser.add_argument("--duration", type=int, default=60, help="Bench send window in seconds")
    parser.add_argument("--bench-processes", type=int, default=0, help=f"Load processes (default: one per {EMPY_CLIENTS_PER_BENCH} clients)")
    parser.add_argument("--port", type=int, default=8080, help="Dashboard port")
    parser.add_argument("--no-dashboard", action="store_true", help="Run headless and exit after the bench")
    parser.add_argument("--seconds", type=int, default=3600, help="Deployment lifetime (brokers stop afterwards)")
    args = parser.parse_args(argv)
    try:
        counts = [int(value) for value in args.brokers.split(",")]
    except ValueError:
        parser.error("--brokers needs integers")
    if any(not 1 <= count <= EMPY_MAX_SOURCES for count in counts):
        parser.error(f"Broker counts must be in 1..{EMPY_MAX_SOURCES}")
    if not 30 <= args.seconds <= 86400 or not 1 <= args.duration <= 86400 or args.payload < 16 or args.rate < 0:
        parser.error("Invalid lifetime, duration, payload (>= 16 B) or rate")
    if args.traffic == "bench" and (args.publishers < 1 or args.subscribers < 1):
        parser.error("Bench traffic needs at least one publisher and one subscriber")
    suffix = ".exe" if os.name == "nt" else ""
    if not (args.bin / ("EmpyBroker" + suffix)).is_file() or not (args.bin / ("EmpyKlient" + suffix)).is_file():
        parser.error("Build EmpyBroker and EmpyKlient first, or specify --bin")
    if os.name == "nt":
        import ctypes
        ctypes.windll.kernel32.SetErrorMode(0x0001 | 0x0002)
    base = args.output.resolve() / (time.strftime("%Y%m%d-%H%M%S-") + secrets.token_hex(3))
    sweep = len(counts) > 1
    reports = []
    try:
        for count in counts:
            directory = base / f"brokers-{count:03}" if sweep else base
            reports.append(run(args, count, directory, not args.no_dashboard and not sweep))
    except KeyboardInterrupt:
        print("Stopped.", flush=True)
    except (RuntimeError, OSError, subprocess.CalledProcessError) as failure:
        print(f"Cluster test failed: {failure}", file=sys.stderr)
        return 1
    if sweep and reports:
        (base / "sweep.json").write_text(json.dumps(reports, indent=4), encoding="utf-8")
        (base / "sweep.md").write_text(markdown(reports), encoding="utf-8")
        print("\nScalability sweep:\n" + markdown(reports) + f"\nReports: {base}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
