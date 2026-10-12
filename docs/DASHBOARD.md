# Network monitoring dashboard

The dashboard displays live broker observations using a dark, responsive web UI.
It includes a selectable broker/client topology, traffic history, broker inventory,
session search and filters, detailed queues and request state, and an activity log.
Updates normally arrive once per second. Pause freezes the view; the collector
continues sampling. The JSON export contains the current observation and history.

## Start with an existing broker

Build the C++ targets, then run these commands from the repository root in separate
terminals. Substitute the appropriate build directory/configuration and add `.exe`
on Windows if the shell requires it.

```sh
Targets/Build/Debug/EmpyBroker --config Configs/BrokerDashboard.yaml
python EMPY_DASHBOARD/app.py --config Configs/Dashboard.json
```

Open **http://127.0.0.1:8080**. The dashboard needs Python 3.10 or later. It uses
only the standard library; the browser assets need no npm install or build step.
Use `--port 8081` to change the dashboard port. Ctrl+C stops the dashboard without
stopping separately started brokers. Stop those brokers with their own Ctrl+C.

An existing broker configuration can also enable telemetry with
`--telemetry-port 5601`. The `--monitor` console output remains independent.
Each broker on the same host needs a distinct telemetry port.

## Local clusters and scalability tests

`EMPY_DASHBOARD/cluster.py` deploys a real fabric of 1 to 256 broker processes on
this host, drives load and reports the result. `demo.py` is its three-broker
preset with paced demo clients:

```sh
python EMPY_DASHBOARD/demo.py --bin Targets/Build/Debug --openssl openssl --seconds 900
python EMPY_DASHBOARD/cluster.py --brokers 10 --bin Targets/BuildRelease/Release --openssl openssl
python EMPY_DASHBOARD/cluster.py --brokers 100 --traffic idle --openssl openssl
python EMPY_DASHBOARD/cluster.py --brokers 3,10,30,100 --no-dashboard --rate 2 --openssl openssl
```

Every run writes isolated keys, one CA, a distinct pinned certificate per broker,
YAML configurations, state, process IDs and logs into a fresh
`Targets/Cluster/<run>` directory. The OS trust store is untouched. Brokers start
in waves (`--start-concurrency`, default 16) on free loopback ports, share one
static membership with SHA-256 certificate pins, and use a mutual-TLS backplane,
or native QUIC with `--quic`. Backplane links are dialed on demand, so an idle
fabric has no links and a loaded one only links the brokers that exchange work.
The dashboard serves the deployment at http://127.0.0.1:8080 until Ctrl+C or the
`--seconds` lifetime; `--no-dashboard` exits after the measurement.

| Option | Meaning |
| --- | --- |
| `--brokers N` or `A,B,C` | Fabric size; a comma list runs a sweep and writes `sweep.json`/`sweep.md` |
| `--traffic bench\|demo\|idle` | Load generators, the paced demo clients, or no clients |
| `--publishers`, `--subscribers`, `--topics` | Bench clients, spread round-robin over all brokers; publisher *i* and subscriber *j* use topic `i mod T` / `j mod T` |
| `--rate`, `--payload`, `--qos`, `--duration` | Publications per second per publisher (0 = unpaced), payload bytes (at least 16), QoS and send window |
| `--shards`, `--replicas`, `--fanout`, `--lease-ms`, `--workers` | Fabric placement and broker settings (shards default to max(8, 2 × brokers)) |
| `--bench-processes` | Load processes (default one per 200 clients); each owns its topic prefix |

The bench uses `EmpyKlient bench`, which runs many publishers and subscribers in
one process. Each payload carries its send time, publisher index and sequence.
Subscribers record latency into a fixed log-scale histogram (100 buckets per
decade, merged exactly across processes) and track duplicates and reordering per
publisher. `report.json`/`report.md` contain:

| Field | Meaning |
| --- | --- |
| Start / Ready s | Seconds until every broker listened / served telemetry |
| Active links | Backplane links connected after the load, of n(n−1)/2 possible |
| Accepted/s, Delivered/s | Publications accepted by the ingress (PUBACK for QoS 1, local admission for QoS 0) and unique deliveries, per second of send window |
| Lost, Dup | Expected minus unique deliveries after a 5 s drain; repeated deliveries |
| p50 / p99 / Max ms | Publish-to-application-callback latency on one host clock |
| Broker RSS / CPU % | Peak total resident memory of all brokers; broker CPU time over the run as a share of all host cores |

All brokers, replicas and load generators share one machine, one disk and one
loopback interface. Results describe that host and configuration; they are
functional scalability measurements, not a distributed benchmark. Increase
`--rate` until latency or loss rises to find the host's saturation point; see
[CLUSTERING.md](CLUSTERING.md) for the reliable-path costs measured so far.

`EmpyKlient --interval-ms N` paces repeated publish/request operations together with
`--count N`. Its `--seconds N` limit also bounds the paced publication loop.

## Broker telemetry configuration

Telemetry is disabled unless explicitly enabled in the executable's YAML or CLI:

```yaml
telemetry:
  address: 127.0.0.1
  port: 5601
  interval_ms: 1000
  max_sessions: 1024
  token_env: EMPY_TELEMETRY_TOKEN
```

`token_env` is optional for local use. When set, the variable must contain 32–256
printable ASCII characters with no spaces. Send it as `Authorization: Bearer ...`.
An unset/empty configured variable or invalid token fails startup. Tokens are not
stored in example files, returned in telemetry, or sent to the browser.

- `GET /api/v1/snapshot` returns schema 1 JSON; before the first sample it returns
  503. If sampling later stalls, it preserves the old snapshot and its old uptime.
- `GET /health` reports that the HTTP listener is running. It does not assert
  broker, storage, quorum, or cluster health. Both routes require a configured token.
- Only loopback IP addresses are accepted. There is no CORS access or arbitrary
  proxy route. Unexpected Host/Origin headers and request bodies are rejected.
- Sampling intervals are 250–10,000 ms; session limits are 1–4,096. Broker totals
  still include omitted sessions; the dashboard labels truncated observations.
- The listener allows at most 32 HTTP connections, 8 KiB request headers, a
  three-second connection lifetime, and a 16 MiB cached JSON snapshot. Slow HTTP
  consumers share the cache and do not execute work on the protocol strand.

Embedded applications use `Broker::Inspect(limit)` for a typed snapshot, or create
`TelemetryServer(broker, options)` after `Broker::Listen()`. **Stop telemetry before
stopping/destroying the broker.** Calls to `Inspect` block their caller while taking
an executor-owned value copy; do not call them from the protocol executor. The
executable handles this lifetime order automatically.

## Monitor several brokers

Configure explicit sources in a JSON file:

```json
{
    "name": "Research fabric",
    "intervalMs": 1000,
    "timeoutMs": 1500,
    "historySamples": 300,
    "brokers": [
        {
            "id": "north",
            "name": "Broker North",
            "url": "http://127.0.0.1:5601",
            "tokenEnv": "EMPY_NORTH_TELEMETRY_TOKEN"
        },
        {
            "id": "south",
            "name": "Broker South",
            "url": "http://127.0.0.1:5602",
            "tokenEnv": "EMPY_SOUTH_TELEMETRY_TOKEN"
        }
    ]
}
```

IDs and URLs must be unique; IDs are dashboard configuration keys and can differ
from the actual broker identities. There are at most 256 sources, 30–900 history
samples and 100 activity entries. History is in memory and resets when the
collector restarts. The schema allowlist strips unknown upstream fields.
If two endpoints report the same broker instance, the later configured source
is marked `Duplicate` and excluded from live totals. A second URL cannot supply
the opposite endpoint's confirmation of a cluster link.

For brokers on other hosts, use SSH port forwards to their loopback telemetry
listeners, or an operator-managed, authenticated HTTPS reverse proxy. Remote
plaintext origins, credentials in URLs and redirects are rejected. HTTPS verifies
certificates; an optional `authority` path selects a CA file relative to the JSON
configuration. No verification-disable switch is provided. The dashboard itself
also binds to loopback. For remote operators, use an SSH forward to port 8080, or
an authenticated TLS proxy that supplies the upstream loopback Host header. Keep
session identifiers and tenant realms within the operator trust boundary.

## What the measurements mean

The collector polls at most 32 sources concurrently and keeps full session lists
server side. `/api/state` carries compact per-broker summaries: counters, peer
links, request totals and a preview of six sessions. `/api/sessions` returns one
filtered page (`broker`, `state`, `role`, `search`, `page`, `size` up to 100) and
`/api/session?key=` one session, so 100+ brokers stay responsive in the browser.

Up to 12 brokers are drawn in detail with client leaves. Larger fabrics switch
to a compact ring of up to 256 nodes. A node's halo grows with its share of the
busiest broker's publication rate. Because links are dialed on demand, the ring
draws only the selected broker's live links and links reported by one side
only, instead of thousands of idle pairs. The Brokers view is a searchable table
sortable by load, links, latency or uptime. The overview reports a message-rate
card and active links of n(n−1)/2 possible per cluster manifest. The traffic
chart switches between bytes and messages over 1, 5 or 15 minutes.

| Display | Source and interpretation |
| --- | --- |
| Topology | Configured cluster members plus each broker's actual authenticated ready links. A solid B2B edge requires fresh connected observations from both endpoints. Unmonitored peers remain visible without invented metrics. Cluster manifest identities prevent unrelated membership configurations from merging. |
| Client nodes | Sessions observed at each broker, including offline resumable sessions. Up to six leaves per broker are drawn; the table exposes the complete reported set with pagination. A session can remain as an old observation at one broker after moving to another. |
| Live totals | Reporting brokers only. An unavailable telemetry endpoint does not prove its broker or clients are down. Last observations remain visible and client state becomes `Unknown`. |
| Ingress/egress | Client-facing MMQP packet bytes received / accepted by the transport send queue. These include control packets and retries, exclude TLS/QUIC overhead and B2B traffic, and do not measure application consumption. |
| Rates | Counter deltas over the broker's monotonic uptime. The first sample, restart, counter regression and recovery after an outage have no rate until a new baseline exists. Wall-clock offsets across hosts do not determine the rate or freshness. |
| Broker counters | Per broker instance: accepted connections, routed publications, duplicate ingress, rejections, admitted deliveries and retransmissions. They reset on restart and are not persisted. |
| Client counters | Per session observation at this broker: packets/bytes, accepted publications and admitted deliveries. Restoration/takeover can reset them; `observedAt` identifies the new observation baseline. No client CPU, RSS, device health, or application latency is inferred. |
| Stored record bytes | The local FileStore's value-byte accounting, including session/fabric/inbox/retention records where present. It excludes file framing/filesystem overhead. Summing brokers includes replicated copies; it is not unique payload size or disk utilization. Memory-only brokers report this as unavailable. |
| Retained memory | Standalone retention queue bytes. Cluster retention is in replicated storage; it is not reported as an empty standalone retention queue. |
| Queues / requests | Current outgoing QoS 1 entries/bytes, transport queue bytes, replay entries/bytes, active requester exchanges and responder correlation routes. Payloads and topics are excluded. |
| Ownership / leases | A sample of the local fenced owner and remaining lease time, rather than a guarantee about its future state. Active lease counts also include non-session fabric keys. Connected links alone do not prove a valid durable quorum. |
| Telemetry RTT | Duration of the collector's HTTP fetch, not MMQP request/response latency. |

An unchanged `(instance, uptimeMs)` eventually becomes stale even when HTTP
continues responding. The threshold is `max(3.5 s, 3 × interval + timeout)`.
Sampling can lag behind protocol/durability work; cached observations are always
dated. Views and histories do not establish scientific throughput, failover or
availability results.

## Code and checks

Protocol observations and the bounded HTTP listener are in
`EMPY_LIB_MQ/{include/eMQ,src}/Monitoring`. The library chain remains
`Core <- MQ <- Client`. The Python collector and web server live in
`EMPY_DASHBOARD/{collector,config,server,app}.py`; `cluster.py` deploys local
fabrics, and the bench load generator is `EMPY_IMPL_CLIENT/src/Bench.cpp`. The UI is split into `app.js`
(views), `model.js` (state/aggregation), `topology.js` (interactive SVG),
`charts.js` (history), and `styles.css`. None of this adds network dependencies to
Core or changes MMQP packets or persistent protocol schemas.

```sh
ctest --test-dir Targets/Build --output-on-failure
python -m unittest discover -s EMPY_DASHBOARD/tests -v
node --test EMPY_DASHBOARD/tests/model.test.js
python EMPY_DASHBOARD/tests/process_smoke.py --bin Targets/Build/Debug
```

Python and browser model tests are separate from the C++ build. Node is needed
only for the optional JavaScript checks. `tests/browser.mjs` additionally exercises
a running demo or cluster with Playwright: broker table sorting and selection, topology controls, session
filters/details, pause/resume (including an in-flight refresh), export, mobile
overflow and browser errors. An isolated UI fixture also checks unavailable
sources, invalid navigation and escaped labels. Set
`EMPY_PLAYWRIGHT_MODULE` to an existing Playwright module and
`EMPY_BROWSER_EXECUTABLE` to an installed browser if necessary. Screenshots and
JSON evidence go to ignored `Targets/DashboardQA`. The dashboard has no Playwright
runtime dependency.

The feature plan and verified results are tracked in
[IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md),
[PROJECT_CONTEXT.md](PROJECT_CONTEXT.md) and [VALIDATION.md](VALIDATION.md).
