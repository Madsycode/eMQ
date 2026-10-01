"""Run a real three-broker TLS fabric and live MMQP clients behind the dashboard.

Uses the built Empy executables, Python's standard library and OpenSSL. Every run
gets isolated certificates, state, ports and logs; the OS trust store is untouched.
"""

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import threading
import time

from collector import Collector
from config import load
from server import DashboardServer


def stop_process(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin", type=Path, default=root / "Targets/Build/Debug")
    parser.add_argument("--openssl", default="openssl")
    parser.add_argument("--output", type=Path, default=root / "Targets/DashboardDemo")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--seconds", type=int, default=900, help="Demo lifetime (default: 900 seconds)")
    args = parser.parse_args()
    if not 30 <= args.seconds <= 86400:
        parser.error("--seconds must be in 30..86400")
    binary = args.bin.resolve()
    suffix = ".exe" if os.name == "nt" else ""
    broker = binary / ("EmpyBroker" + suffix)
    client = binary / ("EmpyKlient" + suffix)
    if not broker.is_file() or not client.is_file():
        parser.error("Build EmpyBroker and EmpyKlient first, or specify --bin")
    directory = args.output.resolve() / (time.strftime("%Y%m%d-%H%M%S-") + secrets.token_hex(3))
    directory.mkdir(parents=True)
    environment = os.environ.copy()
    environment["EMPY_DEMO_CLUSTER_KEY"] = secrets.token_hex(32)
    environment["EMPY_DEMO_TELEMETRY_TOKEN"] = secrets.token_hex(32)
    # The collector and broker processes share only this run's monitoring token.
    os.environ["EMPY_DEMO_TELEMETRY_TOKEN"] = environment["EMPY_DEMO_TELEMETRY_TOKEN"]
    if os.name == "nt":
        windows = Path(os.environ["SystemRoot"])
        environment["PATH"] = os.pathsep.join((str(windows / "System32"), str(windows)))
    processes = []
    with ExitStack() as cleanup:
        def openssl(*arguments):
            result = subprocess.run([args.openssl, *arguments], cwd=directory, check=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            return result.stdout

        def reserve():
            stream = socket.socket()
            stream.bind(("127.0.0.1", 0))
            cleanup.callback(stream.close)
            return stream

        def launch(name, executable, *arguments):
            log = cleanup.enter_context((directory / f"{name}.log").open("wb"))
            process = subprocess.Popen([str(executable), *map(str, arguments)], cwd=binary, env=environment,
                                       stdout=log, stderr=subprocess.STDOUT,
                                       creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            cleanup.callback(stop_process, process)
            processes.append({"name": name, "pid": process.pid})
            (directory / "processes.json").write_text(json.dumps(processes, indent=4), encoding="utf-8")
            return process

        def ready(name, process, marker):
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                text = (directory / f"{name}.log").read_text(errors="replace")
                if marker in text:
                    return
                if process.poll() is not None:
                    raise RuntimeError(f"{name} exited: {text[-2000:]}")
                time.sleep(0.05)
            raise RuntimeError(f"{name} did not become ready; inspect {directory}")

        openssl("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
                "-keyout", "ca.key", "-out", "ca.pem", "-days", "1", "-subj", "/CN=Empy dashboard demo",
                "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign")
        (directory / "extensions.cnf").write_text(
            "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\n"
            "extendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n", encoding="utf-8")
        fingerprints = []
        for i in range(3):
            openssl("req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-keyout", f"broker{i}.key",
                    "-out", f"broker{i}.csr", "-subj", "/CN=localhost")
            openssl("x509", "-req", "-in", f"broker{i}.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-set_serial", str(i + 2),
                    "-days", "1", "-extfile", "extensions.cnf", "-out", f"broker{i}.pem")
            fingerprints.append(hashlib.sha256(openssl("x509", "-in", f"broker{i}.pem", "-outform", "DER")).hexdigest())
        reservations = [reserve() for _ in range(9)]
        ports = [stream.getsockname()[1] for stream in reservations]
        clients, backplane, telemetry = ports[:3], ports[3:6], ports[6:]
        peers = "".join(f"    - identifier: broker-{i + 1:02}\n      host: 127.0.0.1\n      port: {backplane[i]}\n"
                        f"      server_name: localhost\n      fingerprint: {fingerprints[i]}\n" for i in range(3))
        for i in range(3):
            (directory / f"broker{i}.yaml").write_text(
                f"address: 127.0.0.1\nport: {clients[i]}\nrequest_timeout_ms: 15000\n"
                f"telemetry:\n  port: {telemetry[i]}\n  token_env: EMPY_DEMO_TELEMETRY_TOKEN\n"
                f"cluster:\n  identifier: broker-{i + 1:02}\n  port: {backplane[i]}\n  state_directory: state{i}\n"
                f"  epoch: 1\n  shards: 8\n  replicas: 3\n  fanout: 2\n  quic: false\n"
                f"  certificate: broker{i}.pem\n  private_key: broker{i}.key\n  authority: ca.pem\n"
                "  epoch_key_env: EMPY_DEMO_CLUSTER_KEY\n  peers:\n" + peers, encoding="utf-8")
        (directory / "client.yaml").write_text(
            "auto_reconnect: true\noperation_timeout_ms: 20000\nfailover_endpoints:\n"
            + "".join(f"  - host: 127.0.0.1\n    port: {port}\n" for port in clients), encoding="utf-8")
        config_path = directory / "dashboard.json"
        config_path.write_text(json.dumps({"name": "Local research fabric", "brokers": [
            {"id": f"broker-{i + 1:02}", "name": f"Broker {i + 1:02}", "url": f"http://127.0.0.1:{telemetry[i]}",
             "tokenEnv": "EMPY_DEMO_TELEMETRY_TOKEN"} for i in range(3)]}, indent=4), encoding="utf-8")
        for stream in reservations:
            stream.close()
        print(f"Demo artifacts: {directory}", flush=True)
        for i in range(3):
            name = f"broker{i}"
            process = launch(name, broker, "--config", directory / f"broker{i}.yaml", "--seconds", args.seconds)
            ready(name, process, "MMQP broker listening")
        for i in range(3):
            name = f"subscriber{i}"
            process = launch(name, client, "subscribe", "sensors/#", "--port", clients[i], "--config", directory / "client.yaml",
                             "--seconds", args.seconds, "--verbose")
            ready(name, process, "Profile ready")
        responder = launch("responder", client, "respond", "control/status", "ready", "--port", clients[1],
                           "--config", directory / "client.yaml", "--seconds", args.seconds, "--verbose")
        ready("responder", responder, "Profile ready")
        for i in range(3):
            launch(f"publisher{i}", client, "publish", f"sensors/zone-{i + 1}/environment",
                   json.dumps({"temperature": 21 + i, "humidity": 44 + i, "unit": "C", "source": "local-demo"}),
                   "--port", clients[i], "--config", directory / "client.yaml", "--count", 20000,
                   "--interval-ms", 1700 + i * 400, "--seconds", args.seconds)
        launch("requester", client, "request", "control/status", "status", "--port", clients[2], "--config", directory / "client.yaml",
               "--count", 20000, "--interval-ms", 3000, "--seconds", args.seconds)
        collector = Collector(load(config_path))
        with DashboardServer(("127.0.0.1", args.port), collector) as server:
            collector.start()
            cleanup.callback(collector.stop)
            cancelled = threading.Event()
            cleanup.callback(cancelled.set)

            def expire():
                if not cancelled.wait(args.seconds):
                    server.shutdown()

            threading.Thread(target=expire, daemon=True, name="EmpyDemoDeadline").start()
            print(f"Empy dashboard: http://127.0.0.1:{server.server_port}", flush=True)
            print("3 real brokers / 8 real clients. Ctrl+C stops this demo and its processes.", flush=True)
            try:
                server.serve_forever(poll_interval=0.25)
            except KeyboardInterrupt:
                pass


if __name__ == "__main__":
    main()
