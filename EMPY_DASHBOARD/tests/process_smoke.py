"""Real broker/client -> authenticated telemetry -> collector -> HTTP smoke test.

Verifies packaged Windows runtime manifests and starts executables with a minimal
PATH. No browser or third-party Python package is needed. Artifacts are retained.
"""

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import threading
import time
from urllib import request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from collector import Collector
from config import Settings, Source
from demo import stop_process
from server import DashboardServer


def until(condition, timeout=12):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(0.05)
    raise AssertionError("Timed out waiting for live monitoring state")


def main():
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=root / "Targets/DashboardSmoke")
    args = parser.parse_args()
    binary = args.bin.resolve()
    suffix = ".exe" if os.name == "nt" else ""
    directory = args.output.resolve() / (time.strftime("%Y%m%d-%H%M%S-") + secrets.token_hex(3))
    directory.mkdir(parents=True)
    environment = os.environ.copy()
    token = secrets.token_hex(32)
    environment["EMPY_SMOKE_TELEMETRY"] = token
    checked = 0
    if os.name == "nt":
        import ctypes
        ctypes.windll.kernel32.SetErrorMode(0x0001 | 0x0002)
        windows = Path(os.environ["SystemRoot"])
        environment["PATH"] = os.pathsep.join((str(windows / "System32"), str(windows)))
        for manifest in binary.glob("*.exe.runtime.txt"):
            for line in manifest.read_text().splitlines()[1:]:
                digest, filename = line.split("  ", 1)
                assert Path(filename).name == filename
                assert hashlib.sha256((binary / filename).read_bytes()).hexdigest() == digest, filename
                checked += 1
        assert checked > 0, "No Windows runtime manifests found"
        assert (binary / "zlib1.dll").is_file(), "OpenSSL's dynamic compression provider is missing"
    config = directory / "broker.yaml"
    config.write_text("port: 0\ntelemetry:\n  port: 0\n  interval_ms: 250\n  token_env: EMPY_SMOKE_TELEMETRY\n", encoding="utf-8")
    with ExitStack() as cleanup:
        def launch(name, executable, *arguments):
            log = cleanup.enter_context((directory / (name + ".log")).open("wb"))
            process = subprocess.Popen([str(binary / (executable + suffix)), *map(str, arguments)], cwd=binary, env=environment,
                                       stdout=log, stderr=subprocess.STDOUT,
                                       creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            cleanup.callback(stop_process, process)
            return process

        broker = launch("broker", "EmpyBroker", "--config", config, "--seconds", 30)

        def ready():
            text = (directory / "broker.log").read_text(errors="replace")
            if broker.poll() is not None:
                raise AssertionError(text)
            return text if "MMQP broker listening on port" in text else None

        log = until(ready)
        port = int(re.search(r"MMQP broker listening on port (\d+)", log)[1])
        telemetry = int(re.search(r"Telemetry listening on port (\d+)", log)[1])
        source = Source("smoke", "Smoke broker", f"http://127.0.0.1:{telemetry}", token)
        collector = Collector(Settings("Process smoke", (source,), interval=0.5, timeout=0.5))
        with DashboardServer(("127.0.0.1", 0), collector) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            collector.start()
            endpoint = f"http://127.0.0.1:{server.server_port}/api/state"

            def state():
                with request.urlopen(endpoint, timeout=3) as response:
                    return json.load(response)

            try:
                until(lambda: state()["sources"][0]["state"] == "online")
                subscriber = launch("subscriber", "EmpyKlient", "subscribe", "monitor/process", "--port", port, "--seconds", 25, "--verbose")
                until(lambda: "Profile ready" in (directory / "subscriber.log").read_text(errors="replace"))
                publisher = launch("publisher", "EmpyKlient", "publish", "monitor/process", "DASHBOARD-NO-PAYLOAD",
                                   "--port", port, "--count", 3, "--interval-ms", 125, "--seconds", 10)
                assert publisher.wait(timeout=10) == 0
                assert subscriber.poll() is None
                until(lambda: (state()["sources"][0]["data"] or {}).get("metrics", {}).get("publications", 0) == 3)
                observed = state()
                data = observed["sources"][0]["data"]
                assert data["metrics"]["receivedBytes"] > 0 and data["metrics"]["sentBytes"] > 0
                with request.urlopen(endpoint.replace("/api/state", "/api/sessions?size=100"), timeout=3) as response:
                    page = json.loads(response.read())
                sessions = [row["session"] for row in page["rows"]]
                assert data["metrics"]["sessions"] == 2 and data["observedSessions"] == 2 and page["total"] == 2
                assert data["metrics"]["online"] == 1 and "sessions" not in data
                assert any(session["subscriptions"] == 1 for session in sessions)
                assert any(session["publications"] == 3 for session in sessions)
                encoded = json.dumps(observed) + json.dumps(page)
                assert token not in encoded and "DASHBOARD-NO-PAYLOAD" not in encoded and "monitor/process" not in encoded
                stop_process(broker)
                until(lambda: state()["sources"][0]["state"] == "unreachable")
                failed = state()["sources"][0]
                assert failed["data"] is not None and failed["rates"] is None
                result = {"result": "PASS", "binary": str(binary), "runtimeManifestEntries": checked,
                          "checks": ["Packaged executables with minimal PATH", "Authenticated native telemetry",
                                     "Real paced publications and subscriber", "Broker/session counters through dashboard HTTP",
                                     "No payload, topic or credential exposure", "Unreachable source retains history without live rates"]}
                (directory / "result.json").write_text(json.dumps(result, indent=4), encoding="utf-8")
                print(json.dumps(result, indent=2), flush=True)
                print(f"Artifacts: {directory}", flush=True)
            finally:
                server.shutdown()
                thread.join()
                collector.stop()


if __name__ == "__main__":
    main()
