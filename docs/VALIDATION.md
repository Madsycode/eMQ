# Validation map

Tests are C++ executables linked to the actual shared libraries. No pre-existing broker,
account, public endpoint or external messaging service is used in the tests.
Network suites bind ephemeral loopback ports and create their own temporary test
certificates; they do not modify the Windows trust store.

| CTest suite | Features | Evidence |
| --- | --- | --- |
| `protocol` | F03-F05, F10-F11, F13, F15, F19 | Nine packet types and independent QoS 0/1 REQ/RES correlation golden frames; rejected legacy/COR/zero/truncated IDs; persisted ID allocation/exhaustion; RLF boundaries, fragmentation, invalid flags/UTF-8, TIE, profiles, PID/windows and QUIC policies |
| `runtime` | F02, F26 | Independent runtimes, worker event dispatch, self-detach and cleanup, dispatch queue bounds and YAML integer range rejection |
| `security` | F07, F16-F17 | All 1,089 official NIST Ascon-AEAD128 KAT cases, RFC 5869 HKDF and SHA vectors, delegation, epoch restrictions, AAD/tag/ciphertext alteration, replay and nonce exhaustion |
| `integration` | F06-F18 | Real TCP broker and clients; retention/FIFO, resumption/offline queue, shared subscriptions, QoS filters, request/response, wills, E2E, TLS/HMAC and credential/hostname rejection |
| `cluster` | F20-F25 primitives | Deterministic shard/replica placement and complete trees; authenticated envelopes and parent validation; epoch deduplication; OR-set convergence/compaction with lagging replicas; majority acknowledgements and admission budgets |
| `adversarial` | F03, F06-F11, F13-F14, F16, F18, F25 | Wrong first packet; oversized advertised frame; lost ACK/retry and duplicate suppression; conflicting PID reuse; transactional fan-out admission; keepalive expiry and stale SID; encrypted retention deduplication; broker-assigned alias reuse and tenant-scoped wills |
| `correlation` | F04, F06, F13, F15-F16, F24 | Concurrent calls and equal CIDs across requester sessions; reverse replies with QoS 0/1, aliases and encrypted payloads; wrong topic/responder/ID and ID reuse rejection; no-responder/timeout IDs, resource bounds and late responses; standalone restart with both responder mappings and partially completed fan-out |
| `quic` (optional build) | F06-F07, F11-F13, F19 | Real MsQuic traffic for SAS 1/4/3, QoS 0 datagrams and oversize fallback, concurrent bulk/reliable traffic, reconnects, wrong hostname/HMAC, mutual certificates and cancellation with outstanding sends |
| `recovery` | F06-F07, F12-F17, F20-F25 | Atomic/exclusive/bounded storage and corruption rejection; cipher restart nonce/replay/epoch checks; standalone broker restart and eight-record replay with two in-flight credits; tenant isolation; three-node mutual-TLS ownership/takeover/minority refusal, shared subscriptions, request/response, SID failover and retained FIFO recovery |
| `fabric_quic` (optional build) | F19-F25 | The live three-node quorum and broker routing/restart scenarios over native mutual QUIC |
| `monitoring` | F26 | Real broker/session packet counters, profile roles, disconnect/expiry observations, bounded session snapshots, JSON escaping and omission of payload/topic data; loopback/authentication/Host/Origin/method/header bounds; cancellation of a stalled HTTP reader |
| `Tests/ClusterSmoke.py` (manual process test) | F01, F12-F14, F23-F24, F26 | Actual broker/client executables, minimal Windows PATH, cross-node delivery, forced primary termination, surviving-client failover, full-cluster termination/restart and retained replay |

The adversarial suite also sends raw correlated requests to verify that QoS 1
retries retain one exchange, a new PID cannot reuse a CID, and the receipt digest
rejects a changed CID under an existing PID. Native `quic` exchanges correlated
requests across datagram, ordinary stream and bulk paths in each SAS policy.
Both live recovery suites now keep four same-topic, two-responder exchanges
active across simultaneous requester/responder SID takeover, including equal
requester IDs and response sets partly completed before the owner stops.

`Tests/ClusterSmoke.py --rpc-fault` uses GDB and a Debug build to kill an actual
broker immediately before or after forwarding a REQ or RES, after the admission
checkpoint but before its incoming receipt is durable. The surviving client
resumes on another node; both its ACK future and exactly one correlated reply
must complete. The four deterministic fault positions supplement the normal
lease/failover suites; they are not exhaustive crash/power-loss qualification.

```powershell
$correlationPython = 'C:/Users/pouhela/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe'
& $correlationPython Tests/ClusterSmoke.py --bin Targets/Build/Debug `
    --openssl C:/Libraries/msys64/ucrt64/bin/openssl.exe `
    --rpc-fault request-before --gdb C:/Libraries/msys64/ucrt64/bin/gdb.exe
```

Other positions are `request-after`, `response-before` and `response-after`.
Add `--quic` for the native mutual-QUIC backplane. Child processes use a minimal
Windows PATH; the harness checks that the breakpoint was actually reached.

The clean-PATH process test caught an actual packaging failure missed by the
import scan: the local OpenSSL 3.5.2 DLL dynamically loads `zlib1.dll` for TLS
certificate compression. Without it, GDB captured a call through address zero
inside `COMP_compress_block` during `SSL_connect`. This also produced the user's
Windows application-error dialog. Adding the dynamic provider resolved the
reproduction. CMake now detects enabled dynamic ZLIB/Brotli/Zstd providers from
the selected OpenSSL headers, requires their DLLs and scans their transitive
imports. The default local build enables dynamic ZLIB only.

This is certificate compression, separate from MMQP payload encryption. The
OpenSSL behavior is documented in
[TLS certificate compression](https://docs.openssl.org/3.5/man3/SSL_CTX_set1_cert_comp_preference/).
The process harness suppresses modal crash dialogs for its child processes and
still treats crashes as test failures. Debugger evidence remains under ignored
`Targets/ProcessSmoke/20260929-091913-733965/broker0.log`.

The current results and local dependency paths are recorded in
[PROJECT_CONTEXT.md](PROJECT_CONTEXT.md). Tests run in Debug and Release before
delivery. A test pass does not establish the paper's latency, energy, throughput
or hyperscale claims.

## Dashboard validation

The latest matrix passed 11/11 Debug+QUIC, 11/11 Release+QUIC and 9/9
Release TCP/TLS-only suites. The existing live recovery suites now also inspect
actual cluster membership, held/expired ownership and stored record bytes.

Additional checks use the standard library or optional browser test tooling:

| Check | Verified behavior |
| --- | --- |
| `python -m unittest discover -s EMPY_DASHBOARD/tests -v` | 23 tests: typed configuration/bounds, safe origins and token handling, schema allowlist/counter validation, restart/regression/gap rates, unchanged-cache staleness, duplicate endpoint exclusion, bounded events, static assets, read-only routes, traversal/Host/Origin rejection, bearer fetches, redirect refusal and a removed CA file |
| `node --test EMPY_DASHBOARD/tests/model.test.js` | 7 tests: bidirectional fresh link confirmation, duplicate sources, unmonitored peers, separate cluster manifests, stale-total exclusion, unknown client state, combined session filters and unavailable-value formatting |
| `python EMPY_DASHBOARD/tests/process_smoke.py --bin <output>` | Real packaged broker and client executables through authenticated native telemetry, collector and dashboard HTTP. Verified counters, paced traffic, session disconnects, payload/topic/token omission and unreachable-state preservation. Passed in all three build outputs with a minimal Windows PATH and 75 matching DLL-manifest entries. |
| `node EMPY_DASHBOARD/tests/browser.mjs` with a running demo | 10 checks in headless Edge: live overview, node selection/zoom/fit/client visibility, pause/resume including a pending refresh, inventory navigation, session filters/search/details, JSON export, responsive 1440/390px layout and no browser errors. An isolated UI fixture verifies unavailable sources, invalid navigation and escaped labels. Screenshots are retained. |
| Actual demo broker termination | Changed 3/3 to 2/3 source visibility; preserved old observations and unknown client state; removed unavailable rates from live totals; only the surviving peer link remained confirmed. |

Results and exact environment commands are in
[PROJECT_CONTEXT.md](PROJECT_CONTEXT.md). The guide describes counters, resource
bounds and the local-operator access model in [DASHBOARD.md](DASHBOARD.md).
This functional dashboard run does not validate device CPU/RSS, application
latency, an atomic cluster-wide snapshot or a scientific performance claim.

## Further qualification

1. Run the same suites with supported Linux/GCC and Windows/MSVC toolchains;
   add address/undefined/thread sanitizers where available. The current local
   Windows MinGW run does not establish Linux execution.
2. Maintain a fuzz corpus for incremental framing and every optional property
   combination. Add coverage-guided fuzzing with bounded allocations and CPU
   time, plus independent peer interoperability tests.
3. Test packet loss, congestion, delayed bulk traffic, UDP path-MTU changes,
   certificate rotation, expiry, IPv6 and prolonged disconnects. Verify both
   OpenSSL and Schannel deployment profiles independently.
4. Extend the live B2B tests with asymmetric/long partitions, slow replicas,
   suspend/resume, clock-rate perturbation, stale owners and repeated concurrent
   failover. Current deterministic takeover and minority tests are bounded cases,
   not an exhaustive proof of the fault model.
5. Extend persistence tests to disk-full/write failures and power interruption
   on the target filesystem. Process termination is not a power-loss test.
   Application exactly-once effects need a transaction that
   includes the application's side effect and its consumed-message identity.
6. Record benchmark hardware, OS, compiler flags, transport/provider versions,
   payload distribution, profile population, active sessions, queue limits and
   the failure model. Report warm-up, repetitions, percentile latency, loss,
   memory and energy measurement methodology with raw results.

## Known scope boundaries

- Live `ClusterNode` adds authenticated networking, static membership, durable
  fencing/leases and snapshot adoption to the reusable fabric primitives.
  `SessionQuorum` alone still does not elect an owner. Live profiles use versioned
  snapshots; the OR-set delta implementation is tested independently.
- Online membership migration and independent B2B interoperability are not
  implemented. Read the lease/clock and storage assumptions in the cluster guide.
- Broker and optional cipher state persist, but a new client process establishes
  a fresh MMQP session. Application callbacks are not storage transactions.
- The paper's HMAC handshake has no fresh server nonce. Secure transport remains
  required; persistent/quorum replay records expire and are not permanent freshness.
- Retained replay continues as credit becomes available within an explicit
  backlog byte limit. It is not an unlimited spool.
- Custom SAS 0 needs an application-specific binding; it is explicitly rejected.
