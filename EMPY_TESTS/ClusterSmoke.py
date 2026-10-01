"""Separate-process crash/restart and Windows runtime-DLL smoke test.

Uses only Python's standard library and the OpenSSL command-line utility.
Artifacts stay in a fresh directory under --output; no OS trust changes.
"""

import argparse
import hashlib
import os
from pathlib import Path
import secrets
import socket
import ssl
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin", required=True, type=Path)
    parser.add_argument("--openssl", default="openssl")
    parser.add_argument("--output", type=Path, default=Path("Targets/ProcessSmoke"))
    parser.add_argument("--quic", action="store_true")
    parser.add_argument("--rpc-fault", choices=("request-before", "request-after", "response-before", "response-after"),
                        help="Kill a Debug broker at the selected RPC admission boundary using GDB")
    parser.add_argument("--gdb", type=Path, help="GDB executable, required with --rpc-fault")
    args = parser.parse_args()
    if args.rpc_fault and not args.gdb:
        parser.error("--rpc-fault requires --gdb and a Debug build with symbols")
    binary = args.bin.resolve()
    directory = args.output.resolve() / (time.strftime("%Y%m%d-%H%M%S-") + secrets.token_hex(3))
    directory.mkdir(parents=True)
    extension = ".exe" if os.name == "nt" else ""
    broker = binary / ("EmpyBroker" + extension)
    client = binary / ("EmpyKlient" + extension)
    environment = os.environ.copy()
    environment["EMPY_CLUSTER_KEY"] = secrets.token_hex(32)
    if os.name == "nt":
        import ctypes
        ctypes.windll.kernel32.SetErrorMode(0x0001 | 0x0002)
        windows = Path(os.environ["SystemRoot"])
        environment["PATH"] = os.pathsep.join((str(windows / "System32"), str(windows)))
    processes = []
    logs = []
    reservations = []
    debuggers = set()

    def openssl(*arguments, binary_output=False):
        result = subprocess.run([args.openssl, *arguments], cwd=directory,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return result.stdout if binary_output else result.stdout.decode()

    def launch(name, executable, *arguments):
        stream = (directory / (name + ".log")).open("wb")
        logs.append(stream)
        command = [str(executable), *map(str, arguments)]
        process = subprocess.Popen(command, cwd=binary, env=environment,
                                   stdout=stream, stderr=subprocess.STDOUT,
                                   creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                                   start_new_session=os.name != "nt")
        processes.append(process)
        return process

    def output(name):
        return (directory / (name + ".log")).read_text(errors="replace")

    def await_output(name, text, process, timeout=18):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if text in output(name):
                return
            if process.poll() is not None:
                raise RuntimeError(f"{name} exited {process.returncode}: {output(name)}")
            time.sleep(0.05)
        raise RuntimeError(f"Timed out waiting for {text!r} in {name}: {output(name)}")

    def completed(process, name):
        if process.wait(timeout=20) != 0:
            raise RuntimeError(f"{name} exited {process.returncode}: {output(name)}")

    def reserve():
        while True:
            tcp = socket.socket()
            tcp.bind(("127.0.0.1", 0))
            port = tcp.getsockname()[1]
            udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                udp.bind(("127.0.0.1", port))
            except OSError:
                tcp.close()
                udp.close()
                continue
            reservations.extend((tcp, udp))
            return port

    try:
        for executable, name in ((broker, "broker-help"), (client, "client-help")):
            completed(launch(name, executable, "--help"), name)
        openssl("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
                "-keyout", "ca.key", "-out", "ca.pem", "-days", "1", "-subj", "/CN=Empy smoke CA",
                "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign")
        (directory / "extensions.cnf").write_text(
            "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\n"
            "extendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n")
        fingerprints = []
        for i in range(3):
            openssl("req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
                    "-keyout", f"broker{i}.key", "-out", f"broker{i}.csr", "-subj", "/CN=localhost")
            openssl("x509", "-req", "-in", f"broker{i}.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
                    "-set_serial", str(i + 2), "-days", "1", "-extfile", "extensions.cnf", "-out", f"broker{i}.pem")
            der = openssl("x509", "-in", f"broker{i}.pem", "-outform", "DER", binary_output=True)
            fingerprints.append(hashlib.sha256(der).hexdigest())
        ports = [reserve() for _ in range(3)]
        backplane = [reserve() for _ in range(3)]
        peers = "".join(f"    - identifier: broker{i}\n      host: 127.0.0.1\n      port: {backplane[i]}\n"
                        f"      server_name: localhost\n      fingerprint: {fingerprints[i]}\n" for i in range(3))
        for i in range(3):
            (directory / f"broker{i}.yaml").write_text(
                f"address: 127.0.0.1\nport: {ports[i]}\nrequest_timeout_ms: 30000\ncluster:\n  identifier: broker{i}\n"
                f"  port: {backplane[i]}\n  state_directory: state{i}\n  epoch: 1\n  shards: 4\n"
                f"  replicas: 3\n  fanout: 2\n  quic: {str(args.quic).lower()}\n"
                f"  certificate: broker{i}.pem\n  private_key: broker{i}.key\n  authority: ca.pem\n"
                "  epoch_key_env: EMPY_CLUSTER_KEY\n  peers:\n" + peers)
        (directory / "client.yaml").write_text(
            "auto_reconnect: true\nreconnect_interval_ms: 250\noperation_timeout_ms: 25000\nfailover_endpoints:\n" +
            "".join(f"  - host: 127.0.0.1\n    port: {ports[i]}\n" for i in (1, 2)))
        for reservation in reservations:
            reservation.close()

        def start_broker(i, suffix=""):
            name = f"broker{i}{suffix}"
            arguments = ("--config", directory / f"broker{i}.yaml", "--seconds", 60, "--monitor")
            if args.rpc_fault and i == 0 and not suffix:
                source = Path(__file__).resolve().parents[1] / "EMPY_LIB_MQ/src/Runtime/Broker.cpp"
                lines = source.read_text().splitlines()
                forward = "            cluster->Publish(sender, sequence, publication, targets, SaveExpiry(expiry));"
                line = lines.index(forward) + 1 + int(args.rpc_fault.endswith("-after"))
                kind = 2 if args.rpc_fault.startswith("request-") else 3
                commands = directory / "rpc-fault.gdb"
                commands.write_text(
                    "set pagination off\nset confirm off\nset print thread-events off\nset breakpoint pending on\n"
                    f"break Broker.cpp:{line} if publication.type == {kind}\n"
                    "commands\nsilent\nprintf \"MMQP RPC admission fault reached\\n\"\nkill\nquit\nend\nrun\n")
                process = launch(name, args.gdb.resolve(), "--batch", "--nx", "-x", commands, "--args", broker, *arguments)
                debuggers.add(process)
            else:
                process = launch(name, broker, *arguments)
            await_output(name, "MMQP broker listening", process)
            return process

        nodes = [start_broker(i) for i in range(3)]
        if args.rpc_fault:
            requester_node, responder_node = (0, 1) if args.rpc_fault.startswith("request-") else (2, 0)
            responder = launch("responder", client, "respond", "smoke/rpc", "R", "--port", ports[responder_node],
                               "--config", directory / "client.yaml", "--count", 1, "--seconds", 45, "--verbose")
            await_output("responder", "Profile ready: smoke/rpc", responder)
            requester = launch("requester", client, "request", "smoke/rpc", "Q", "--port", ports[requester_node],
                               "--config", directory / "client.yaml", "--count", 1, "--seconds", 45)
            await_output("broker0", "MMQP RPC admission fault reached", nodes[0], timeout=30)
            await_output("requester", "smoke/rpc R", requester, timeout=30)
            completed(requester, "requester")
            completed(responder, "responder")
            for name, expected in (("requester", "smoke/rpc R"), ("responder", "smoke/rpc Q")):
                values = [line for line in output(name).splitlines() if line.startswith("smoke/rpc ")]
                if values != [expected]:
                    raise RuntimeError(f"Unexpected recovered RPC deliveries in {name}: {values}")
            result = f"PASS: clean-PATH RPC {args.rpc_fault} fault, SID takeover, replayed admission, successful ACK and one response.\n"
            (directory / "result.txt").write_text(result)
            print(result.strip(), flush=True)
            print(f"Artifacts: {directory}", flush=True)
            return
        if not args.quic:
            context = ssl.create_default_context(cafile=str(directory / "ca.pem"))
            context.load_cert_chain(directory / "broker0.pem", directory / "broker0.key")
            for port in backplane:
                with socket.create_connection(("127.0.0.1", port), timeout=3) as stream:
                    with context.wrap_socket(stream, server_hostname="localhost"):
                        pass
        subscriber = launch("subscriber", client, "subscribe", "smoke/value", "--port", ports[0],
                            "--config", directory / "client.yaml", "--count", 2, "--seconds", 45, "--verbose")
        await_output("subscriber", "Profile ready: smoke/value", subscriber)

        def publish(value):
            name = "publish-" + value
            completed(launch(name, client, "publish", "smoke/value", value, "--port", ports[2]), name)

        publish("A")
        await_output("subscriber", "smoke/value A", subscriber)
        nodes[0].kill()
        nodes[0].wait(timeout=5)
        publish("B")
        await_output("subscriber", "smoke/value B", subscriber)
        completed(subscriber, "subscriber")
        values = [line for line in output("subscriber").splitlines() if line.startswith("smoke/value ")]
        if values != ["smoke/value A", "smoke/value B"]:
            raise RuntimeError(f"Unexpected failover deliveries: {values}")
        for node in nodes[1:]:
            node.kill()
            node.wait(timeout=5)
        nodes = [start_broker(i, "-restart") for i in range(3)]
        late = launch("retained", client, "subscribe", "smoke/value", "--port", ports[1], "--count", 1, "--seconds", 20)
        await_output("retained", "smoke/value B", late)
        completed(late, "retained")
        result = ("PASS: clean-PATH executables, three broker processes, cross-node QoS 1, forced primary termination, "
                  "same-client failover, full cluster termination and durable retained replay.\n")
        (directory / "result.txt").write_text(result)
        print(result.strip(), flush=True)
        print(f"Artifacts: {directory}", flush=True)
    finally:
        for reservation in reservations:
            reservation.close()
        for process in processes:
            if process.poll() is None:
                if process in debuggers and os.name == "nt":
                    subprocess.run([str(Path(os.environ["SystemRoot"]) / "System32/taskkill.exe"),
                                    "/PID", str(process.pid), "/T", "/F"],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,
                                   creationflags=subprocess.CREATE_NO_WINDOW)
                elif process in debuggers:
                    import signal
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            process.wait(timeout=10)
        for stream in logs:
            stream.close()


if __name__ == "__main__":
    main()
