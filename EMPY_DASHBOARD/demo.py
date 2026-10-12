"""Run a real three-broker TLS fabric and live MMQP clients behind the dashboard.

A preset of cluster.py: three brokers, paced demo traffic and the dashboard. Every
run gets isolated certificates, state, ports and logs; the OS trust store is untouched.
"""

from pathlib import Path
import sys

from cluster import main, stop_process  # noqa: F401  stop_process is shared with the process tests


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    sys.exit(main(["--brokers", "3", "--traffic", "demo", "--shards", "8", "--seconds", "900",
                   "--bin", str(root / "Targets/Debug/bin"), "--output", str(root / "Targets/Runs/DashboardDemo"), *sys.argv[1:]]))
