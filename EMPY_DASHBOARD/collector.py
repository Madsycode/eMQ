"""Bounded parallel collection, stale detection, counter deltas, short history and session queries.

The browser receives compact per-broker summaries; full session lists stay in the
collector and are served as filtered pages, so 100+ brokers remain responsive.
"""

from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from http.client import HTTPException
import ssl
import threading
import time
from urllib import error, request

EMPY_MAX_SNAPSHOT_BYTES = 16 * 1024 * 1024
EMPY_MAX_WORKERS = 32
EMPY_PREVIEW_SESSIONS = 6
EMPY_PAGE_LIMIT = 100
EMPY_STATE_RANKS = {"online": 0, "unknown": 1, "offline": 2}
EMPY_COUNTERS = ("receivedPackets", "receivedBytes", "sentPackets", "sentBytes")
EMPY_METRICS = EMPY_COUNTERS + ("accepted", "publications", "duplicates", "rejected", "forwarded",
                              "retransmitted", "sessions", "online", "retainedBytes", "pending")


def _number(value):
    if type(value) is not int or not 0 <= value <= (1 << 64) - 1:
        raise ValueError("Invalid telemetry counter")
    return value


def _text(value, limit=256):
    if not isinstance(value, str) or len(value) > limit:
        raise ValueError("Invalid telemetry label")
    return value


def _boolean(value):
    if type(value) is not bool:
        raise ValueError("Invalid telemetry flag")
    return value


def validate(value):
    """Allowlist schema 1 fields rather than relaying arbitrary upstream JSON."""
    if not isinstance(value, dict) or type(value.get("schema")) is not int or value["schema"] != 1:
        raise ValueError("Unsupported telemetry schema")
    result = {"schema": 1}
    for key in ("identifier", "instance", "address", "transport"):
        result[key] = _text(value[key])
    if not result["identifier"] or not result["instance"] or result["transport"] not in ("TCP", "TLS", "QUIC"):
        raise ValueError("Invalid broker identity or transport")
    for key in ("port", "collectedAt", "uptimeMs", "omittedSessions"):
        result[key] = _number(value[key])
    for key in ("durable", "authenticationRequired"):
        result[key] = _boolean(value[key])
    result["storedBytes"] = None if value["storedBytes"] is None else _number(value["storedBytes"])
    result["metrics"] = {key: _number(value["metrics"][key]) for key in EMPY_METRICS}
    result["limits"] = {key: _number(value["limits"][key]) for key in ("sessions", "inflightPerSession", "retainedBytes", "storedBytes")}
    if result["metrics"]["online"] > result["metrics"]["sessions"]:
        raise ValueError("Inconsistent session totals")
    sessions = value["sessions"]
    if not isinstance(sessions, list) or len(sessions) > 4096:
        raise ValueError("Snapshot session limit exceeded")
    result["sessions"] = []
    seen = set()
    for session in sessions:
        item = {}
        for key in ("identifier", "realm", "transport"):
            item[key] = _text(session[key])
        if not item["identifier"] or item["identifier"] in seen:
            raise ValueError("Duplicate or missing session identity")
        seen.add(item["identifier"])
        for key in ("online", "authenticated", "resumable", "owner"):
            item[key] = _boolean(session[key])
        for key in EMPY_COUNTERS + ("observedAt", "publications", "deliveries", "keepaliveSeconds", "subscriptions",
                                   "registrations", "responders", "requesters", "pending", "pendingBytes", "queuedBytes",
                                   "replay", "replayBytes", "requests", "responseRoutes"):
            item[key] = _number(session[key])
        for key in ("idleMs", "expiresInMs"):
            item[key] = None if session[key] is None else _number(session[key])
        lease = session["lease"]
        item["lease"] = None if lease is None else {"fence": _text(lease["fence"], 20), "remainingMs": _number(lease["remainingMs"])}
        result["sessions"].append(item)
    if len(sessions) + result["omittedSessions"] != result["metrics"]["sessions"]:
        raise ValueError("Inconsistent snapshot session count")
    cluster = value["cluster"]
    result["cluster"] = None
    if cluster is not None:
        result["cluster"] = {key: _number(cluster[key]) for key in
                             ("shards", "replicas", "fanout", "pendingOperations", "activeLeases", "quarantineMs")}
        result["cluster"]["epoch"] = _text(cluster["epoch"], 20)
        result["cluster"]["identifier"] = _text(cluster["identifier"], 64)
        if len(result["cluster"]["identifier"]) != 64 or any(c not in "0123456789abcdef" for c in result["cluster"]["identifier"]):
            raise ValueError("Invalid cluster manifest identity")
        peers = cluster["peers"]
        if not isinstance(peers, list) or len(peers) > 4096:
            raise ValueError("Cluster membership limit exceeded")
        result["cluster"]["peers"] = [{"identifier": _text(peer["identifier"]), "host": _text(peer["host"]),
                                        "port": _number(peer["port"]), "connected": _boolean(peer["connected"]),
                                        "queuedBytes": _number(peer["queuedBytes"])} for peer in peers]
    return result


class _NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def fetch(source, timeout):
    headers = {"Accept": "application/json", "User-Agent": "EmpyDashboard/1"}
    if source.token:
        headers["Authorization"] = "Bearer " + source.token
    started = time.monotonic()
    try:
        context = ssl.create_default_context(cafile=source.authority) if source.url.startswith("https:") else None
        opener = request.build_opener(request.ProxyHandler({}), _NoRedirect(), request.HTTPSHandler(context=context))
        req = request.Request(source.url + "/api/v1/snapshot", headers=headers)
        with opener.open(req, timeout=timeout) as response:
            if response.headers.get_content_type() != "application/json":
                raise ValueError("Invalid telemetry content type")
            body = response.read(EMPY_MAX_SNAPSHOT_BYTES + 1)
            if len(body) > EMPY_MAX_SNAPSHOT_BYTES:
                raise ValueError("Telemetry response too large")
            data = validate(json.loads(body))
        return data, round((time.monotonic() - started) * 1000, 1), None
    except error.HTTPError as failure:
        return None, None, "Telemetry authorization failed" if failure.code in (401, 403) else f"Telemetry HTTP {failure.code}"
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        return None, None, "Invalid telemetry snapshot"
    except (error.URLError, TimeoutError, OSError, HTTPException):
        return None, None, "Telemetry endpoint unreachable"


def roles(session):
    """Same role derivation as the browser model."""
    result = []
    if session["requesters"]:
        result.append("Requester")
    if session["responders"]:
        result.append("Responder")
    if session["registrations"] > session["requesters"]:
        result.append("Publisher")
    if session["subscriptions"] > session["responders"]:
        result.append("Subscriber")
    return result or ["Connected"]


def summarize(data):
    """Broker snapshot without its session list: counts, requests and a small preview."""
    summary = {key: value for key, value in data.items() if key != "sessions"}
    sessions = data["sessions"]
    summary["observedSessions"] = len(sessions)
    summary["requests"] = sum(session["requests"] for session in sessions)
    preview = sorted(sessions, key=lambda session: (not session["online"], session["identifier"]))[:EMPY_PREVIEW_SESSIONS]
    summary["preview"] = [{"identifier": session["identifier"], "online": session["online"], "roles": roles(session)}
                          for session in preview]
    return summary


def rates(previous, current, seconds):
    if not previous or seconds <= 0:
        return None
    keys = ("receivedBytes", "sentBytes", "publications")
    if any(current[key] < previous[key] for key in keys):
        return None
    return {key: (current[key] - previous[key]) / seconds for key in keys}


class Collector:
    def __init__(self, settings):
        self.settings = settings
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._records = {source.identifier: {"id": source.identifier, "name": source.name, "data": None,
                         "summary": None, "state": "connecting", "error": None, "lastSuccessAt": None, "progress": None,
                         "latencyMs": None, "rates": None} for source in settings.sources}
        self._history = deque(maxlen=settings.history)
        self._events = deque(maxlen=100)
        self._sequence = 0
        self.started_at = round(time.time() * 1000)
        self.stale_after = max(3.5, settings.interval * 3 + settings.timeout)

    def _event(self, kind, message, timestamp):
        self._sequence += 1
        self._events.appendleft({"id": self._sequence, "kind": kind, "message": message, "time": timestamp})

    def accept(self, source, result, now=None, timestamp=None):
        now = time.monotonic() if now is None else now
        timestamp = round(time.time() * 1000) if timestamp is None else timestamp
        data, latency, failure = result
        with self._lock:
            record = self._records[source.identifier]
            previous = record["data"]
            previous_state = record["state"]
            state = "unreachable" if failure else "online"
            rate = None
            if data:
                fresh = not previous or (data["instance"], data["uptimeMs"]) != (previous["instance"], previous["uptimeMs"])
                if fresh:
                    if previous and previous["instance"] == data["instance"] and previous_state == "online":
                        elapsed = (data["uptimeMs"] - previous["uptimeMs"]) / 1000
                        if elapsed <= self.stale_after:
                            rate = rates(previous["metrics"], data["metrics"], elapsed)
                    record["progress"] = now
                    record["lastSuccessAt"] = timestamp
                    if previous and previous["instance"] != data["instance"]:
                        self._event("restart", f"{source.name} restarted; counters reset", timestamp)
                    old_online = {s["identifier"] for s in previous["sessions"] if s["online"]} if previous else set()
                    online = {s["identifier"] for s in data["sessions"] if s["online"]}
                    if previous and online != old_online:
                        self._event("sessions", f"{source.name}: {len(online)} observed clients online", timestamp)
                else:
                    rate = record["rates"]
                    if now - record["progress"] > self.stale_after:
                        state, failure, rate = "stale", "Broker snapshot has stopped advancing", None
                record["data"] = data
                record["summary"] = summarize(data)
            if state != previous_state:
                self._event("online" if state == "online" else "warning", f"{source.name}: {state}", timestamp)
            record.update(state=state, error=failure, latencyMs=latency, rates=rate)

    def _sources(self, now):
        """Effective per-source state; the caller holds the lock."""
        sources = []
        instances = set()
        for value in self._records.values():
            item = {key: entry for key, entry in value.items() if key not in ("progress", "summary")}
            item["ageMs"] = None if value["progress"] is None else round(max(0, now - value["progress"]) * 1000)
            if item["state"] == "online" and item["ageMs"] > self.stale_after * 1000:
                item.update(state="stale", error="Broker snapshot has stopped advancing", rates=None)
            if item["state"] == "online":
                instance = item["data"]["instance"]
                if instance in instances:
                    item.update(state="duplicate", error="Another source already reports this broker instance", rates=None)
                else:
                    instances.add(instance)
            sources.append(item)
        return sources

    def state(self, now=None):
        now = time.monotonic() if now is None else now
        with self._lock:
            sources = self._sources(now)
            for item in sources:
                item["data"] = self._records[item["id"]]["summary"]
            return {"schema": 1, "name": self.settings.name, "startedAt": self.started_at,
                    "generatedAt": round(time.time() * 1000), "intervalMs": round(self.settings.interval * 1000),
                    "sources": sources, "history": list(self._history), "events": list(self._events)}

    def _rows(self, now, broker="all"):
        for source in self._sources(now):
            if broker != "all" and broker != source["id"]:
                continue
            for session in (source["data"] or {}).get("sessions", ()):
                state = "unknown" if source["state"] != "online" else "online" if session["online"] else "offline"
                yield {"source": {"id": source["id"], "name": source["name"], "state": source["state"]},
                       "session": session, "state": state, "roles": roles(session),
                       "key": f'{source["id"]}/{session["identifier"]}'}

    def sessions(self, broker="all", state="all", role="all", search="", page=0, size=25, now=None):
        """One filtered page of sessions across all sources, ordered like the browser table."""
        now = time.monotonic() if now is None else now
        size = max(1, min(EMPY_PAGE_LIMIT, size))
        search = search.casefold()
        with self._lock:
            rows, observed = [], 0
            for row in self._rows(now, broker):
                observed += 1
                if state != "all" and row["state"] != state:
                    continue
                if role != "all" and role not in row["roles"]:
                    continue
                session = row["session"]
                if search and search not in f'{session["identifier"]} {row["source"]["name"]} {row["source"]["id"]} {session["realm"]}'.casefold():
                    continue
                rows.append(row)
        rows.sort(key=lambda row: (EMPY_STATE_RANKS[row["state"]], row["source"]["name"], row["session"]["identifier"]))
        pages = max(1, -(-len(rows) // size))
        page = max(0, min(page, pages - 1))
        return {"total": len(rows), "observed": observed, "page": page, "pages": pages, "size": size,
                "rows": rows[page * size:(page + 1) * size]}

    def session(self, key, now=None):
        now = time.monotonic() if now is None else now
        broker, _, identifier = key.partition("/")
        with self._lock:
            for row in self._rows(now, broker):
                if row["session"]["identifier"] == identifier:
                    return row
        return None

    def _run(self):
        workers = min(EMPY_MAX_WORKERS, len(self.settings.sources))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="EmpyTelemetry") as pool:
            while not self._stop.is_set():
                started = time.monotonic()
                tasks = {pool.submit(fetch, source, self.settings.timeout): source for source in self.settings.sources}
                for task in as_completed(tasks):
                    self.accept(tasks[task], task.result())
                state = self.state()
                online = [source for source in state["sources"] if source["state"] == "online"]
                available = [source["rates"] for source in online if source["rates"] is not None]
                links = sum(peer["connected"] for source in online for peer in (source["data"]["cluster"] or {}).get("peers", ()))
                point = {"time": round(time.time() * 1000), "coverage": len(online), "links": links,
                         "online": sum(source["data"]["metrics"]["online"] for source in online)}
                point.update({key: sum(item[key] for item in available) if available else None
                              for key in ("receivedBytes", "sentBytes", "publications")})
                with self._lock:
                    self._history.append(point)
                self._stop.wait(max(0.05, self.settings.interval - (time.monotonic() - started)))

    def start(self):
        self._thread = threading.Thread(target=self._run, name="EmpyCollector", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join()
