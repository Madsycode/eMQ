from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import http.client
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from collector import Collector, EMPY_METRICS, fetch, validate
from config import Settings, Source, load
from server import DashboardServer


def snapshot(uptime=1000, received=100, instance="boot-1"):
    return {"schema": 1, "identifier": "broker-1", "instance": instance, "address": "127.0.0.1", "port": 5600,
            "transport": "TCP", "collectedAt": 100000 + uptime, "uptimeMs": uptime, "omittedSessions": 0,
            "durable": False, "authenticationRequired": False, "cluster": None, "sessions": [], "storedBytes": None,
            "limits": {"sessions": 1024, "inflightPerSession": 256, "retainedBytes": 10000, "storedBytes": 10000},
            "metrics": {**dict.fromkeys(EMPY_METRICS, 0), "receivedBytes": received,
                        "sentBytes": received * 2, "publications": received // 10}}


class ConfigTests(unittest.TestCase):
    def configuration(self, data):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "dashboard.json"
            path.write_text(json.dumps(data), encoding="utf-8")
            return load(path)

    def test_local_config(self):
        result = self.configuration({"brokers": [{"id": "local", "url": "http://127.0.0.1:5601"}]})
        self.assertEqual(result.sources[0].name, "local")
        self.assertEqual(result.interval, 1)

    def test_unsafe_origins_and_duplicate_sources(self):
        for url in ("http://example.com", "http://user:secret@127.0.0.1", "file:///secret", "http://localhost/path", "http://localhost?key=secret"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                self.configuration({"brokers": [{"id": "node", "url": url}]})
        with self.assertRaises(ValueError):
            self.configuration({"brokers": [{"id": "same", "url": "http://localhost:5601"},
                                             {"id": "same", "url": "http://localhost:5602"}]})

    def test_invalid_bounds(self):
        for values in ({"intervalMs": 0}, {"historySamples": 100000}, {"timeoutMs": -1}, {"intervalMs": True}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                self.configuration({"brokers": [{"id": "node", "url": "http://localhost:5601"}], **values})
        for invalid in ([], {"brokers": [12]}, {"brokers": [{"id": "node", "url": "http://localhost:0"}]}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.configuration(invalid)

    def test_token_stays_server_side(self):
        token = "q" * 40
        with patch.dict(os.environ, {"EMPY_TEST_TOKEN": token}):
            config = self.configuration({"brokers": [{"id": "node", "url": "https://example.com", "tokenEnv": "EMPY_TEST_TOKEN"}]})
        self.assertEqual(config.sources[0].token, token)
        self.assertNotIn(token, repr(config))
        self.assertNotIn(token, json.dumps(Collector(config).state()))

    def test_missing_token_fails_closed(self):
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(ValueError):
            self.configuration({"brokers": [{"id": "node", "url": "http://localhost:5601", "tokenEnv": "MISSING"}]})


class SchemaTests(unittest.TestCase):
    def test_allowlist(self):
        value = snapshot()
        value["privateKey"] = "DO-NOT-RELAY"
        self.assertNotIn("DO-NOT-RELAY", json.dumps(validate(value)))

    def test_invalid_counters_and_versions(self):
        for invalid in (-1, float("nan"), "123", True):
            value = snapshot()
            value["metrics"]["receivedBytes"] = invalid
            with self.subTest(value=invalid), self.assertRaises(ValueError):
                validate(value)
        value = snapshot()
        value["schema"] = 99
        with self.assertRaises(ValueError):
            validate(value)

    def test_inconsistent_sessions(self):
        value = snapshot()
        value["metrics"]["online"] = 1
        with self.assertRaises(ValueError):
            validate(value)
        value = snapshot()
        value["metrics"]["sessions"] = 8
        with self.assertRaises(ValueError):
            validate(value)
        value["omittedSessions"] = 8
        self.assertEqual(validate(value)["omittedSessions"], 8)


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.source = Source("node", "Test node", "http://127.0.0.1:5601", "a" * 40)
        self.collector = Collector(Settings("Test fabric", (self.source,)))

    def accept(self, data, now):
        self.collector.accept(self.source, (data, 2.5, None), now=now, timestamp=round(now * 1000))
        return self.collector.state(now=now)["sources"][0]

    def test_distinct_sample_rates(self):
        self.assertIsNone(self.accept(snapshot(), 1)["rates"])
        result = self.accept(snapshot(uptime=3000, received=300), 3)
        self.assertEqual(result["rates"]["receivedBytes"], 100)
        self.assertEqual(result["rates"]["sentBytes"], 200)
        self.assertEqual(result["rates"]["publications"], 10)

    def test_restart_does_not_generate_negative_or_spiked_rates(self):
        self.accept(snapshot(received=10000), 1)
        result = self.accept(snapshot(instance="boot-2", received=10), 2)
        self.assertIsNone(result["rates"])
        self.assertTrue(any(event["kind"] == "restart" for event in self.collector.state(now=2)["events"]))

    def test_counter_regression(self):
        self.accept(snapshot(received=1000), 1)
        result = self.accept(snapshot(uptime=2000, received=2), 2)
        self.assertIsNone(result["rates"])

    def test_unreachable_preserves_data_but_not_rates(self):
        self.accept(snapshot(), 1)
        self.collector.accept(self.source, (None, None, "Telemetry endpoint unreachable"), now=2, timestamp=2000)
        state = self.collector.state(now=2)
        value = state["sources"][0]
        self.assertEqual(value["state"], "unreachable")
        self.assertEqual(value["data"]["instance"], "boot-1")
        self.assertIsNone(value["rates"])
        self.assertNotIn(self.source.token, json.dumps(state))

    def test_stagnant_cache_becomes_stale_even_when_http_works(self):
        self.accept(snapshot(), 1)
        result = self.accept(snapshot(), 9)
        self.assertEqual(result["state"], "stale")
        self.assertEqual(result["ageMs"], 8000)
        self.assertIsNone(result["rates"])

    def test_staleness_without_next_completed_poll(self):
        self.accept(snapshot(), 1)
        self.assertEqual(self.collector.state(now=10)["sources"][0]["state"], "stale")

    def test_recovery_after_gap_starts_a_new_rate_baseline(self):
        self.accept(snapshot(), 1)
        self.collector.accept(self.source, (None, None, "failed"), now=2)
        self.assertIsNone(self.accept(snapshot(uptime=20000, received=5000), 20)["rates"])
        self.assertEqual(self.accept(snapshot(uptime=21000, received=5100), 21)["rates"]["receivedBytes"], 100)

    def test_event_history_is_bounded(self):
        for value in range(120):
            self.accept(snapshot(instance=f"boot-{value}"), value)
        self.assertEqual(len(self.collector.state(now=120)["events"]), 100)

    def test_two_endpoints_for_one_instance_are_not_two_brokers(self):
        alias = Source("alias", "Alias", "http://localhost:5601")
        collector = Collector(Settings("Fabric", (self.source, alias)))
        for source in (self.source, alias):
            collector.accept(source, (snapshot(), 1, None), now=1, timestamp=1000)
        values = collector.state(now=1)["sources"]
        self.assertEqual([value["state"] for value in values], ["online", "duplicate"])
        self.assertIsNone(values[1]["rates"])
        collector.accept(self.source, (None, None, "unreachable"), now=2, timestamp=2000)
        self.assertEqual(collector.state(now=2)["sources"][1]["state"], "online")


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.collector = Collector(Settings("Test fabric", (Source("node", "Test node", "http://127.0.0.1:5601"),)))
        self.server = DashboardServer(("127.0.0.1", 0), self.collector)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_port

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def get(self, path, method="GET", headers=None):
        client = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        client.request(method, path, headers=headers or {})
        response = client.getresponse()
        result = response.status, dict(response.headers), response.read()
        client.close()
        return result

    def test_assets_and_json(self):
        for path in ("/", "/styles.css", "/app.js", "/model.js", "/charts.js", "/topology.js", "/api/state"):
            with self.subTest(path=path):
                status, headers, body = self.get(path)
                self.assertEqual(status, 200)
                self.assertTrue(body)
                self.assertEqual(headers["Cache-Control"], "no-store")
                self.assertIn("frame-ancestors 'none'", headers["Content-Security-Policy"])
        self.assertEqual(json.loads(self.get("/api/state")[2])["name"], "Test fabric")

    def test_no_file_traversal_or_writes(self):
        for path in ("/../config.py", "/%2e%2e/config.py", "/config.py", "/web/index.html"):
            self.assertEqual(self.get(path)[0], 404)
        for method in ("POST", "PUT", "DELETE", "PATCH"):
            self.assertEqual(self.get("/api/state", method)[0], 405)

    def test_dns_rebinding_and_cross_origin(self):
        self.assertEqual(self.get("/api/state", headers={"Host": "evil.example"})[0], 403)
        self.assertEqual(self.get("/api/state", headers={"Origin": "https://evil.example"})[0], 403)
        self.assertEqual(self.get("/api/state", headers={"Origin": f"http://127.0.0.1:{self.port}"})[0], 200)

    def test_remote_bind_is_explicitly_unsupported(self):
        with self.assertRaises(ValueError):
            DashboardServer(("0.0.0.0", 0), self.collector)


class FetchTests(unittest.TestCase):
    def test_removed_ca_file_does_not_stop_collection(self):
        source = Source("node", "Node", "https://localhost:1", authority="missing-ca-file-for-test.pem")
        self.assertEqual(fetch(source, 1), (None, None, "Telemetry endpoint unreachable"))

    def test_authenticated_fetch_and_redirect_refusal(self):
        token = "test-token-" + "x" * 40
        calls = []
        redirect = [False]

        class Endpoint(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                calls.append((self.path, self.headers.get("Authorization")))
                if redirect[0]:
                    self.send_response(302)
                    self.send_header("Location", "/leak-token")
                    self.end_headers()
                    return
                body = json.dumps(snapshot()).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with ThreadingHTTPServer(("127.0.0.1", 0), Endpoint) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                source = Source("node", "Node", f"http://127.0.0.1:{server.server_port}", token)
                data, latency, failure = fetch(source, 1)
                self.assertIsNone(failure)
                self.assertEqual(data["identifier"], "broker-1")
                self.assertGreaterEqual(latency, 0)
                self.assertEqual(calls[0], ("/api/v1/snapshot", "Bearer " + token))
                redirect[0] = True
                self.assertIsNone(fetch(source, 1)[0])
                self.assertEqual(len(calls), 2)
                self.assertNotIn("/leak-token", [value[0] for value in calls])
            finally:
                server.shutdown()
                thread.join()


if __name__ == "__main__":
    unittest.main()
