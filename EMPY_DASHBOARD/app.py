"""Run the Empy monitoring dashboard with Python 3.10+; no third-party packages."""

import argparse
from pathlib import Path
import sys

from collector import Collector
from config import load
from server import DashboardServer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path(__file__).resolve().parents[1] / "Configs/Dashboard.json")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    collector = Collector(load(args.config))
    with DashboardServer((args.host, args.port), collector) as server:
        collector.start()
        host = f"[{args.host}]" if ":" in args.host else args.host
        print(f"Empy dashboard: http://{host}:{server.server_port}", flush=True)
        print(f"Monitoring {len(collector.settings.sources)} broker(s). Press Ctrl+C to stop.", flush=True)
        try:
            server.serve_forever(poll_interval=0.25)
        except KeyboardInterrupt:
            pass
        finally:
            collector.stop()


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as failure:
        print(f"Dashboard: {failure}", file=sys.stderr)
        sys.exit(1)
