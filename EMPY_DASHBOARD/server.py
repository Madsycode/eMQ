"""Local read-only dashboard server. Static assets and telemetry share one origin."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
from pathlib import Path
import socket
import threading
from urllib.parse import parse_qs, urlsplit

EMPY_WEB = Path(__file__).with_name("web")
EMPY_ASSETS = {"/": ("index.html", "text/html"), "/styles.css": ("styles.css", "text/css"),
               **{f"/{name}.js": (f"{name}.js", "text/javascript") for name in ("app", "model", "charts", "topology")}}


class DashboardServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, collector):
        if not ipaddress.ip_address(address[0]).is_loopback:
            raise ValueError("Dashboard must bind to a loopback address; use an authenticated TLS proxy for remote access")
        if ":" in address[0]:
            self.address_family = socket.AF_INET6
        self.collector = collector
        self._slots = threading.BoundedSemaphore(32)
        super().__init__(address, Handler)
        host = f"[{address[0]}]" if ":" in address[0] else address[0]
        self.hosts = {f"{host}:{self.server_port}", f"localhost:{self.server_port}"}

    def process_request(self, request, client_address):
        request.settimeout(5)
        if not self._slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self._slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._slots.release()


class Handler(BaseHTTPRequestHandler):
    server_version = "EmpyDashboard/1"
    sys_version = ""

    def log_message(self, *_):
        pass

    def _send(self, status, body, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type + "; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; "
                         "connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass
        self.close_connection = True

    def do_GET(self):
        host = self.headers.get("Host", "").lower()
        origin = self.headers.get("Origin")
        if (len(self.headers.get_all("Host", [])) != 1 or host not in self.server.hosts
                or (origin and origin.lower() != "http://" + host)):
            self._send(403, b'{"error":"Invalid host or origin"}')
            return
        if self.headers.get("Transfer-Encoding") or self.headers.get("Content-Length", "0") != "0":
            self._send(400, b'{"error":"Request body is not accepted"}')
            return
        url = urlsplit(self.path)
        path = url.path
        if path in ("/api/state", "/api/sessions", "/api/session"):
            try:
                result = self._api(path, parse_qs(url.query, max_num_fields=8))
            except ValueError:
                self._send(400, b'{"error":"Invalid query"}')
                return
            if result is None:
                self._send(404, b'{"error":"Session is not in the latest observation"}')
                return
            self._send(200, json.dumps(result, separators=(",", ":"), allow_nan=False).encode())
        elif path == "/health":
            self._send(200, b'{"status":"ok"}')
        elif path in EMPY_ASSETS:
            asset, mime = EMPY_ASSETS[path]
            self._send(200, (EMPY_WEB / asset).read_bytes(), mime)
        else:
            self._send(404, b'{"error":"Not found"}')

    def _api(self, path, query):
        def value(name, default="", limit=128):
            items = query.get(name, [default])
            if len(items) != 1 or len(items[0]) > limit:
                raise ValueError(name)
            return items[0]

        collector = self.server.collector
        if path == "/api/state":
            return collector.state()
        if path == "/api/session":
            return collector.session(value("key", limit=512))
        state, role = value("state", "all"), value("role", "all")
        if state not in ("all", "online", "offline", "unknown") or role not in ("all", "Publisher", "Subscriber", "Requester", "Responder"):
            raise ValueError("filter")
        page, size = value("page", "0", 6), value("size", "25", 3)
        if not page.isdigit() or not size.isdigit():
            raise ValueError("page")
        return collector.sessions(value("broker", "all", 100), state, role, value("search"), int(page), int(size))

    def do_POST(self):
        self._send(405, b'{"error":"Read-only dashboard"}')

    do_PUT = do_DELETE = do_PATCH = do_POST
