# Project context

## Purpose and authority

Empy is the C++20 implementation of the user's MMQP IoT research framework.
[README.pdf](../README.pdf) defines the protocol, E2E design, QUIC policy, MHF
clustering and ECS-based runtime. The user's five-directory layering is binding.
Use [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) to locate each feature and
[PROTOCOL_DECISIONS.md](PROTOCOL_DECISIONS.md) before changing wire behavior.

## PRT-selected request IDs, 2026-10-12

F04/F15 now use the existing PRT values to determine the request-ID field.
Every REQ (`PRT=2`) and RES (`PRT=3`) carries a nonzero, big-endian U16 request
ID, including at QoS 0. The field follows the optional topic and QoS 1 packet
ID, and precedes the request's response-count byte or response's reason block.
It is independent of the QoS packet ID; MSG (`PRT=1`) carries no request ID.
The `correlationId` API field, responder mappings and request matching are
unchanged. `MMQP_PUBREQ_FLAGS_COR` was removed; bit `0x10` is reserved and must
be zero. Packet lengths and ordinary MSG bytes are unchanged.

Snapshots and inboxes contain encoded publications. This wire change therefore
advances session snapshots to ESS4, broker snapshots to EBR4 and the cluster
manifest to schema 4. Request metadata ERQ1 is unchanged. Old snapshots and
cluster manifests fail before recovery and are preserved; there is no automatic
migration or reset. Deploy matched clients and brokers, using fresh broker
state or a separately validated offline migration. CONREQ remains version 1;
pre-ID legacy payloads cannot always be distinguished from the mandatory ID
and following header bytes. See [PROTOCOL_DECISIONS.md](PROTOCOL_DECISIONS.md)
and [CLUSTERING.md](CLUSTERING.md) for the compatibility boundary.

Validation passed with QUIC enabled: all targets built and all 11 CTest suites
passed in both `Targets/Debug` (Debug, Conan dependencies) and
`Targets/BuildRelease` (Release, installed dependencies). The protocol suite
checks independent QoS 0/1 REQ/RES vectors, separate packet/request IDs, zero or
truncated IDs and rejection of every reserved variable-header bit. Recovery
checks also reject an intact EBR3 snapshot and a schema-3 cluster manifest
without changing their stored values. Existing concurrent/encrypted RPC,
standalone restart and TLS/QUIC cluster-failover tests passed.

```powershell
$env:PATH = 'C:\Libraries\msys64\ucrt64\bin;' + $env:PATH
cmake --build Targets/Debug --parallel 2
ctest --test-dir Targets/Debug --output-on-failure
cmake --build Targets/BuildRelease --parallel 2
ctest --test-dir Targets/BuildRelease --output-on-failure
```

Build and test logs are under ignored `Targets/PrtRequestId` as
`debug-build.log`, `debug-tests.log`, `release-build.log` and `release-tests.log`.

## Windows build repair, 2026-10-12

F01 build setup now preserves the default `MinGW Makefiles` generator when
`CMAKE_GENERATOR` is unset. `EmpyWin.bat` checks that native GCC, G++ and
`mingw32-make` are on `PATH`, then passes their absolute compiler paths through
Conan's `tools.build:compiler_executables`. Conan's bundled MSYS tools otherwise
selected their POSIX GCC while building OpenSSL 3.5.2 for Windows, producing
undeclared `wcslen`, `_alloca` and `wcscpy` errors. Pinning the native compilers
addresses that toolchain mismatch without modifying vendor sources or pinned
dependency versions. The wrapper also honors `CMAKE_BUILD_PARALLEL_LEVEL`.

QUIC remains enabled by default, with Windows builds in `Targets/<configuration>`.
The `EMPY_MSQUIC_ROOT` and `EMPY_MSQUIC_RUNTIME` environment
variables now initialize their CMake cache entries on a fresh configuration.
For an existing cache, use explicit `-D` values to change them. The installed
Schannel MsQuic DLL compiles and links, but the PEM-based QUIC tests require the
OpenSSL provider. The documented 2.6.1 package was restored under ignored
`Targets/Dependencies/MsQuicOpenSSL` and its SHA-256 matched the value below.

Local Windows wrapper command, from the repository root:

```powershell
$env:PATH = 'C:\Libraries\msys64\ucrt64\bin;' + $env:PATH
$env:EMPY_MSQUIC_ROOT = 'C:/Libraries/msquic'
$env:EMPY_MSQUIC_RUNTIME = (Resolve-Path 'Targets/Dependencies/MsQuicOpenSSL/2.6.1/build/native/bin/x64/msquic.dll').Path
$env:CMAKE_BUILD_PARALLEL_LEVEL = '2'
.\EmpyWin.bat Debug
```

The wrapper builds all libraries, executables and tests, and runs CTest. Its
Debug binaries are under `Targets/Debug/Debug`; the separately configured
installed-dependency Debug build is under `Targets/Build/Debug`.

Verified with native MinGW GCC 15.2 and the OpenSSL MsQuic 2.6.1 runtime:

| Build directory | Dependencies | Result |
| --- | --- | --- |
| `Targets/Debug` | Conan Asio 1.38.0 and static OpenSSL 3.5.2 | Complete wrapper run; all targets built; 11/11 CTest suites passed |
| `Targets/Build` | Installed Asio 1.38.0 and shared OpenSSL 3.5.2 | All targets built; 11/11 CTest suites passed |
| `Targets/BuildRelease` | Installed Asio 1.38.0 and shared OpenSSL 3.5.2 | All targets built; 11/11 CTest suites passed |

Invalid configurations and a missing MinGW toolchain return exit code 1 before
Conan runs. The Conan output passed all 17 runtime-manifest hash checks and
broker/client `--help` startup with a Windows-only `PATH`. The installed Debug
output passed `EMPY_DASHBOARD/tests/process_smoke.py`, including real pub/sub,
telemetry and all 26 manifest entries with that minimal `PATH`. The Conan static
OpenSSL build does not require the installed build's OpenSSL or zlib DLLs.

The verified wrapper log is `Targets/BuildFix/wrapper-debug-verified.log`, the
Release build/test log is `Targets/BuildFix/release.log`, and CTest keeps its
logs in each build's `Testing/Temporary` directory. The executable smoke result
is `Targets/BuildFix/Smoke/20261012-105135-bd3f60/result.json`.

## Web monitoring dashboard, 2026-09-29

The requested dark dashboard is in `EMPY_DASHBOARD`. It is a separate read-only
Python 3.10+ web app with standard-library HTTP collection and static ES modules;
no frontend build or third-party runtime package is needed. The C++ dependency
chain is unchanged. See [DASHBOARD.md](DASHBOARD.md) for complete operation and
metric definitions. The manuscript was not changed by this feature.

`Broker::Inspect(limit)` returns typed, executor-owned observations of broker and
session counters, profiles/roles, queues, correlation state and expiry.
`ClusterNode::Inspect` adds actual authenticated ready links, manifest identity,
epoch/shard settings, local record-byte accounting and current fenced leases.
The optional `TelemetryServer` samples outside the HTTP and protocol executors
and serves a bounded cache on loopback. It must stop before its broker. The CLI
handles that lifetime order and accepts `--telemetry-port`; the `telemetry` YAML
block also configures sampling, snapshot limits and an optional token environment
variable. Credentials, payloads and topic filters never enter telemetry.

The collector scopes cluster nodes by manifest, detects restarts with instance/
uptime, excludes stale observations from live totals, preserves unavailable
broker history and marks their client state unknown. Counters are process-local;
rates measure client-facing MMQP bytes, including control/retry packets and
transport-admitted sends. They do not measure device CPU/RSS, TLS/B2B overhead,
application completion or RPC latency. Stored record bytes include local replica
copies and exclude filesystem overhead. Sampling is not an atomic cluster-wide
observation or a quorum-health guarantee.

The UI includes selectable/pannable/zoomable SVG topology, short traffic history,
broker inventory, searchable/filterable/paginated sessions, session details,
activity, JSON export and pause/refresh controls. Real clients appear on broker
nodes; larger session sets stay available in the table. History is memory-only.
Remote monitoring uses SSH forwards or a separately managed authenticated TLS
proxy. The app does not introduce a tenant-access or operator identity service.

`demo.py` creates a real, isolated three-broker mutual-TLS fabric and eight client
processes with live pub/sub and correlated RPC traffic. It uses generated local
certificates without changing OS trust. The executable's new `--interval-ms`
option paces publications/requests while respecting `--seconds`. Run artifacts
are under ignored `Targets/DashboardDemo`; Ctrl+C cleans up owned processes.

```powershell
$dashboardPython = 'C:/Users/pouhela/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe'
& $dashboardPython EMPY_DASHBOARD/app.py --config Configs/Dashboard.json
# Or start all demo brokers, clients and the dashboard:
& $dashboardPython EMPY_DASHBOARD/demo.py --bin Targets/Build/Debug `
    --openssl C:/Libraries/msys64/ucrt64/bin/openssl.exe --seconds 900
# http://127.0.0.1:8080
```

## Manuscript alignment, 2026-09-29

[latex/MMQP.tex](latex/MMQP.tex) is the complete revision of the author's supplied
LaTeX fragment. It retains the four original main sections, all 68 active labels,
the existing citation keys and custom macros, and adds an implementation and
validation section. Three inline TikZ diagrams cover the Ascon envelope, reliable
cluster acceptance/recovery, and the library dependency chain. Additional primary
references are in [latex/MMQP-references.bib](latex/MMQP-references.bib); integration
instructions and the implementation evidence map are in [latex/README.md](latex/README.md).

The text follows the actual NIST Ascon variant, directional aliases, bounded
QoS/replay state, static membership, majority leases, fenced profile snapshots,
replicated full-payload inboxes, and broker/client restart boundaries. It keeps
analytical tree costs separate from full reliable replication costs and records
functional test evidence without asserting scientific performance results.

Document checks passed for braces, environment/math pairing, all 74 unique labels,
original label/citation preservation, reference targets, new bibliography keys,
the nine packet type values, and 1,134 tree-depth cases. Only `sec:mqtt` and
`sec:mqttpackets` are external section references. Verification scripts/report are
under ignored `Targets/Manuscript`. No TeX compiler or complete master document
was available, so this is structural verification, not compilation or visual QA.
The build results in that initial manuscript revision were checked against the
existing successful CTest/process logs; application code was not changed or
retested by the manuscript-only revision.

```powershell
$manuscriptPython = 'C:/Users/pouhela/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe'
& $manuscriptPython Targets/Manuscript/check.py
```

## Correlated request/response, 2026-09-29

The initial two-byte correlation implementation used `COR=0x10` on REQ and RES.
The 2026-10-12 revision above removes that redundant flag and selects the same
mandatory field through PRT. The ID's byte order and position, QoS 0 support,
API and exchange-matching behavior remain as described below.

Exchanges are keyed by requester SID/CID, not topic. Each responder receives its
own mapped CID; routing restores the requester's ID and verifies the canonical
topic, deadline and remaining admitted responder. IDs are never reused in a SID.
Client allocation is serialized on its I/O executor, supports explicit unused
IDs in any order and otherwise selects an unused ID automatically. Broker state
uses a lazy used-ID bitmap, bounded responder mappings and per-session requests.
Partial response sets and pending reliable RPC admissions survive takeover.
Recovery completes interrupted inbox writes with their original identities and
checkpoints their incoming receipts before resumed traffic is served.

`Request(topic, payload, responses, qos, correlationId)` returns the publication
future (local send admission at QoS 0, PUBACK at QoS 1); application responses
arrive through `Receive`. For concurrent
call matching, supply explicit distinct IDs and inspect `event.message.correlationId`.
Responders use `Respond(event.message, payload)` or
`Respond(topic, correlationId, payload, reason, qos)`. The ambiguous topic-only
overload was removed, and all executable/test call sites were updated.

The initial storage markers ESS3/EBR3 and cluster manifest schema 3 are superseded
by ESS4/EBR4 and schema 4 for the PRT-only wire format; ERQ1 is unchanged. CID
remains broker routing metadata outside the topic/epoch payload AAD. Read the
decisions and clustering guides before changing either boundary.

Only the Request/Response subsection of `docs/latex/MMQP.tex` was changed for
this feature. Its preceding 37,722 bytes and following 82,112 bytes are unchanged
from the supplied working manuscript. The updated sequence diagram shows two
requests and replies arriving in reverse order. `Targets/Correlation/latex-scope.json`
records the byte-scope comparison; `Targets/Manuscript/checks.json` records the
structural checks. No TeX compilation or visual verification is claimed.

## Code style

- Four-space indentation, Allman braces, `#pragma once`, PascalCase types and
  methods, `m_` plus PascalCase private members, concise lowercase local names.
- Keep `EMPY_INLINE`, `EMPY_API` family, `EMPY_STATIC`, `EMPY_ASSERT`, logging
  macros and `MMQP_*` protocol constants. Use `_Type` template parameters where
  consistent with the existing templates. Avoid global identifiers with new
  reserved underscore spellings.
- Preserve `eCore` for generic framework code and `eMQ` for messaging/client
  behavior. Existing bundled vendor headers keep their upstream formatting.
- Components are plain state; services implement behavior. Do not import Asio,
  MsQuic, protocol types or client encryption into the generic Core library.

## Initial inspection, 2026-09-18

The checkout was already dirty when work began: old `EMQ_*` paths had been
deleted and the new `EMPY_*` layout was untracked. Preserve those user changes.
The initial libraries mostly contained skeletons. Build blockers included the
broker CMake target variable, a nonexistent MQ API include, a missing client/MQ
link, namespace mismatches and an incorrect runtime friend declaration.

Existing resources: EnTT, fkYAML, Rang and task-thread-pool are bundled in Core.
Standalone Asio 1.38.0 is the declared Conan dependency. The machine has GCC
15.2 under `C:/Libraries/msys64/ucrt64`, Asio in the Conan cache, and MsQuic under
`C:/Libraries/msquic`; these are local verification paths, not portable defaults.

## Thread ownership

The Core entity/event registry and module lifecycle belong to the runtime owner
thread. Worker tasks may post events/work back to that thread. Network protocol
state must be serialized on its I/O executor; application callbacks are posted
to the Core event loop. Never mutate an EnTT registry from arbitrary pool jobs.

## Implemented source map

| Area | Main files |
| --- | --- |
| Core lifecycle | `EMPY_LIB_CORE/include/eCore/Runtime/{Service,Context,Module,Runtime}.h` |
| Entities/events/configuration | `eCore/Common/ECS.h`, `eCore/Auxiliaries/YAML.h` |
| Wire format | `EMPY_LIB_MQ/include/eMQ/Common/{Protocol,Stream,Message,Topic}.h`, matching `src/Common` files |
| Protocol state | `eMQ/Common/State.h`, `src/Common/State.cpp`, `eMQ/Common/Events.h` |
| TCP/TLS | `eMQ/Network/{Connection,IOContext}.h`, `src/Network/IOContext.cpp` |
| QUIC | `eMQ/Network/{QContext,QuicPolicy}.h`, matching `src/Network` files |
| Broker | `eMQ/Runtime/Broker.h`, `src/Runtime/{Broker,Options}.cpp` |
| Monitoring | `eMQ/Monitoring/{Snapshot,Telemetry}.h`, matching `src/Monitoring` files |
| Web dashboard | `EMPY_DASHBOARD/{app,collector,config,server,demo}.py`, `web/{app,model,charts,topology}.js`, `web/styles.css` |
| Shared crypto helpers | `eMQ/Security/Crypto.h`, `src/Security/Crypto.cpp` |
| Client | `EMPY_LIB_CLIENT/include/eClient/Runtime/Client.h`, `src/Runtime/{Client,Options}.cpp` |
| E2E | `eClient/Security/Payload.h`, `src/Security/Payload.cpp`, pinned `src/Vendors/Ascon` |
| Fabric primitives | `eMQ/Cluster/{Fabric,Replication}.h`, matching `src/Cluster` files |
| Live fabric and storage | `eMQ/Cluster/{Node,Storage,Clock}.h`, `src/Cluster/{Node,Storage}.cpp` |
| Windows runtime packaging | `cmake/{Runtime,StageRuntime}.cmake`, per-executable `.runtime.txt` manifests |
| Executables | `EMPY_IMPL_BROKER/src/Serve.cpp`, `EMPY_IMPL_CLIENT/src/Client.cpp` |
| Examples and tests | `Configs/*.yaml`, `Tests/*.cpp`, `Tests/Vectors/Ascon-AEAD128.txt` |

## Feature status

The runnable product supports standalone and statically configured clustered
brokers, with client libraries and executables. The 2026-09-29 implementation
adds live routing, ownership/failover and persistence. Read
[CLUSTERING.md](CLUSTERING.md) for deployment assumptions and schema boundaries.

| IDs | Current state |
| --- | --- |
| F01-F02 | Shared dependency chain repaired; owner-thread ECS/event dispatch, lifecycle and module cleanup implemented |
| F03-F05 | Nine typed packet codecs, bounded incremental framing, strict fields/topics/UTF-8 and exact TIE conversion implemented |
| F06-F11 | Session state machines, resumption, HMAC, negotiated limits, keepalive, wills, atomic profiles and directional aliases implemented |
| F12-F15 | Local/cross-broker pub/sub and shared groups, QoS checkpoints/retries, FIFO/last retention, bounded replay, and concurrent request/response with PRT-selected IDs and durable requester/responder mappings implemented |
| F16-F17 | NIST Ascon-AEAD128, HMAC/HKDF, delegated keys, epoch/cache wiping, nonces/replay windows and optional durable cipher state implemented; hardware key services are deployment integrations |
| F18 | Asynchronous TCP and TLS 1.3 implemented and tested with real sockets |
| F19 | Optional MsQuic transport with SAS 1/4/3, datagrams, bulk streams and verified credentials implemented; custom SAS 0 and broader platform/path testing remain |
| F20-F22 | Live PSR/SSR placement, quorum profile snapshots/queries, tree forwarding and inbox recovery implemented; OR-set deltas remain a reusable component, while live convergence uses fenced snapshots |
| F23 | Static authenticated membership, majority leases, durable fencing/adoption, session snapshots, minority refusal and automatic SID takeover implemented; online membership migration is outside this binding |
| F24 | Bounded atomic FileStore, FileDurabilityStore, broker checkpoints, replicated inboxes/tombstones and retained queues, expiry compaction and restart recovery implemented |
| F25 | Live pinned B2B mTLS/QUIC, epoch MACs, tenant authentication identities/roots/shard allowlists and persistent token replay records implemented |
| F26 | Typed configuration, CLI modes, opt-in bounded telemetry, live dark topology/metrics/session dashboard, activity/history/export and real cluster demo implemented |
| F27 | Golden/vector, runtime, network and cluster-component tests exist; independent interoperability, sanitizer/stress and scientific benchmarks remain |

Do not claim arbitrary online reconfiguration, Byzantine consensus, persistent
exactly-once business effects or reproduction of the paper's performance results.

## Runtime and integration rules

- Call listener registration, entity APIs and `Poll`/`StartService` from the
  application owner thread. Worker jobs can use `Post`/`Dispatch`; data-bearing
  producers should use `TryPost` with a byte weight.
- The Core dispatch queue defaults to 4,096 tasks and 16 MiB of explicitly
  charged data. `SetDispatchLimits` configures it. A queued batch being processed
  is additional to the next waiting batch. Client receive events charge payload
  and topic bytes; protocol ACKs follow accepted event admission.
- All broker/client protocol state is serialized through an Asio strand.
  MsQuic callbacks copy bounded buffers and post to that same actor. Send buffers
  remain alive through native send completion. Stop joins workers from the owner
  thread, after QUIC completion callbacks drain.
- Core events contain copies/moved values, not views into network buffers.
  App callbacks run on the owner thread. Handle callback exceptions in the
  application; they propagate from `Poll`. Never wait on an I/O future from the
  protocol executor itself.
- PIDs and alias maps survive in-memory reconnects. PID exhaustion, unsafe
  profile acknowledgement ambiguity and expired holes in QUIC receive windows
  require a fresh session. There is no automatic PID wrap. Broker snapshots
  preserve protocol state, but a new client process still establishes a fresh SID.
- `PayloadCipher` belongs exclusively to clients. The broker never needs a
  payload root key. Keep auth credentials separate. Provision grants and accepted
  epochs explicitly; do not log secrets or persist them in example YAML files.
- Windows packaging must cover both PE imports and runtime-loaded providers.
  OpenSSL 3.5.2 here needs `zlib1.dll`; omitting it caused a reproducible null-call
  crash in TLS certificate compression. `cmake/Runtime.cmake` checks enabled
  dynamic compression providers, and `StageRuntime.cmake` stages their closure.
  Preserve the minimal-PATH process smoke test; a normal developer PATH hid this.

## Verified environment and reproducible commands

Local Windows toolchain: MinGW GCC 15.2, standalone Asio 1.38.0, OpenSSL 3.5.2.
The optional native tests use Microsoft's MsQuic OpenSSL 2.6.1 runtime. The package
SHA-256 is `e8bfdd6a4d4e561fd5213eab3c617d231d2f2e5e926e3d3de682ffa540938b08`;
it was downloaded into ignored `Targets/Dependencies/MsQuicOpenSSL` with approval.
No system dependency installation or certificate-store changes were made.

The installed `C:/Libraries/msquic` public API v2 headers/import library are used
with that v2 runtime. The NuGet 2.6.1 Windows header has `RtlIpv4StringToAddressA`
constness and missing `RtlIpv6StringToAddressA` declaration problems under this
MinGW distribution. Do not silently patch vendored SDK headers or add
`-fpermissive`; use compatible headers or a separately verified MSVC build.
Schannel's rejection of an ephemeral PEM-derived credential was resolved by
using the OpenSSL provider; Schannel configuration now explicitly selects an
existing Windows credential by `certificate_hash`.

```powershell
$env:PATH = 'C:\Libraries\msys64\ucrt64\bin;' + $env:PATH
$quicRuntime = (Resolve-Path 'Targets/Dependencies/MsQuicOpenSSL/2.6.1/build/native/bin/x64/msquic.dll').Path
cmake -S . -B Targets/Build -G 'MinGW Makefiles' `
    -DCMAKE_MAKE_PROGRAM=C:/Libraries/msys64/ucrt64/bin/mingw32-make.exe `
    -DCMAKE_C_COMPILER=C:/Libraries/msys64/ucrt64/bin/gcc.exe `
    -DCMAKE_CXX_COMPILER=C:/Libraries/msys64/ucrt64/bin/g++.exe `
    -DCMAKE_BUILD_TYPE=Debug -DEMPY_BUILD_TESTS=ON `
    -DEMPY_ASIO_INCLUDE_DIR=C:/Users/pouhela/.conan2/p/asio6e6c781a0fee4/p/include `
    -DOPENSSL_ROOT_DIR=C:/Libraries/msys64/ucrt64 `
    -DEMPY_ENABLE_QUIC=ON -DEMPY_MSQUIC_ROOT=C:/Libraries/msquic `
    -DMSQUIC_INCLUDE_DIR=C:/Libraries/msquic/include `
    -DMSQUIC_LIBRARY=C:/Libraries/msquic/lib/msquic.lib `
    "-DEMPY_MSQUIC_RUNTIME=$quicRuntime"
cmake --build Targets/Build --parallel 4
ctest --test-dir Targets/Build --output-on-failure
```

Release uses the same arguments with `Targets/BuildRelease` and
`-DCMAKE_BUILD_TYPE=Release`. A TCP/TLS-only build sets `EMPY_ENABLE_QUIC=OFF` and
requires no MsQuic headers, import library or runtime.

Verified on 2026-09-29 with the toolchain above:

| Build directory | Configuration | Result |
| --- | --- | --- |
| `Targets/Build` | Debug, QUIC enabled | 11/11 CTest suites passed, 38.60 s |
| `Targets/BuildRelease` | Release, QUIC enabled | 11/11 CTest suites passed, 38.60 s |
| `Targets/BuildTcp` | Release, QUIC disabled | 9/9 CTest suites passed, 21.98 s |

These latest runs include the dashboard's native `monitoring` suite and sampled
storage/ownership assertions in both live TLS/QUIC recovery suites. The Python
collector/server/configuration tests passed 23/23, and the JavaScript model tests
passed 7/7. Headless Microsoft Edge passed 10/10 desktop/mobile dashboard checks,
including a pending-refresh pause, unavailable-source UI fixture and escaped
labels, with no browser errors. The in-app browser and computer-use REPL could not
initialize their kernel assets in this session; installed Playwright/Edge provided
the reproducible browser QA instead.

The actual three-broker demo was also tested by terminating its first broker.
The UI reported 2/3 available sources, retained stale observations, stopped
contributing their rates, marked their client state unknown, and confirmed only
the surviving B2B link. Evidence is in
`Targets/DashboardQA/{browser-result,broker-loss}.json` and screenshots in that
directory. This is a bounded functional failure test, not an availability result.

The new standard-library `EMPY_DASHBOARD/tests/process_smoke.py` passed for all
three output folders. It verifies live executable traffic through authenticated
telemetry and the dashboard HTTP API, payload/topic/credential omission and
unreachable-source handling with a Windows-only child PATH. It also verified all
75 runtime-manifest SHA-256 entries (26 Debug, 26 Release, 23 TCP-only), including
the required dynamic zlib provider. Result artifacts:

- `Targets/DashboardSmoke/20260929-152320-98480a/result.json` (Debug)
- `Targets/DashboardSmoke/20260929-152320-435401/result.json` (Release)
- `Targets/DashboardSmoke/20260929-152646-37b1e9/result.json` (TCP-only)

```powershell
& $dashboardPython -m unittest discover -s EMPY_DASHBOARD/tests -v
node --test EMPY_DASHBOARD/tests/model.test.js
& $dashboardPython EMPY_DASHBOARD/tests/process_smoke.py --bin Targets/Build/Debug
& $dashboardPython EMPY_DASHBOARD/tests/process_smoke.py --bin Targets/BuildRelease/Release
& $dashboardPython EMPY_DASHBOARD/tests/process_smoke.py --bin Targets/BuildTcp/Release
$env:EMPY_PLAYWRIGHT_MODULE = 'file:///C:/Users/pouhela/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/index.mjs'
$env:EMPY_BROWSER_EXECUTABLE = 'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe'
# While demo.py is running:
node EMPY_DASHBOARD/tests/browser.mjs
```

The final build logs contain no compiler warnings or errors. The security suite
checks all 1,089 official Ascon-AEAD128 vectors. Native QUIC tests include verified
credentials, policy negotiation, datagrams, bulk traffic and cancellation.
The recovery suites add live three-node TLS/QUIC ownership, routing, failover,
minority refusal, full restart recovery, tenant realms and durable cipher state.

The correlation delivery adds independent wire vectors, same-topic concurrency,
equal CIDs across requester sessions, reverse replies, encrypted QoS 0/1 RPC,
invalid/reused/late IDs, timeout and no-responder matching, ID exhaustion and
restart after partial multi-responder completion. The live TLS/QUIC suites keep
four concurrent requests across simultaneous requester/responder failover.

The actual executable fault harness also passed all four admission boundaries:

| Fault position | Backplane | Result artifact |
| --- | --- | --- |
| Request before forwarding | TLS | `Targets/Correlation/Faults/20260929-141025-e0635a/result.txt` |
| Request after forwarding | TLS | `Targets/Correlation/Faults/20260929-141054-f36550/result.txt` |
| Response before forwarding | TLS | `Targets/Correlation/Faults/20260929-141054-70e949/result.txt` |
| Response after forwarding | QUIC | `Targets/Correlation/Faults/20260929-141112-01a726/result.txt` |

Each run reached a GDB breakpoint, killed the primary before its incoming
receipt was durable, resumed the surviving client on another broker and verified
successful acceptance plus one response. This checks the persisted admission
path as well as the ordinary post-ACK takeover path.

The rebuilt Release executables also passed the clean-PATH, three-process
pub/sub and crash/restart harness with QUIC
(`Targets/Correlation/ReleaseSmoke/20260929-141235-464013/result.txt`) and the
TCP-only/TLS build (`Targets/Correlation/TcpSmoke/20260929-142248-d2d4fd/result.txt`).
`Targets/Correlation/output-checks.json` verifies all three output folders,
their required DLLs, 75 runtime-manifest SHA-256 entries, and broker/client
startup with a Windows-only PATH. The commands for the final matrix were:

```powershell
cmake --build Targets/Build --parallel 2
ctest --test-dir Targets/Build --output-on-failure
cmake --build Targets/BuildRelease --parallel 2
ctest --test-dir Targets/BuildRelease --output-on-failure
cmake --build Targets/BuildTcp --parallel 2
ctest --test-dir Targets/BuildTcp --output-on-failure
```

Earlier separate-process Release runs passed with both backplanes and a Windows-only
PATH. Each started three actual brokers, waited for subscriber readiness, sent
QoS 1 across brokers, forcibly terminated the subscriber's primary, verified the
surviving client received the next publication, terminated the full cluster,
restarted it and verified retained replay. Evidence:

- TLS: `Targets/ProcessSmoke/20260929-092403-264ce4/result.txt` and adjacent logs.
- QUIC: `Targets/ProcessSmoke/20260929-092403-daeef5/result.txt` and adjacent logs.
- TCP-only binary: `Targets/ProcessSmoke/20260929-092517-5b51d5/result.txt`;
  the same TLS recovery scenario passed without any MsQuic DLL in that output.
- DLL repair: moving `Targets/Build/Debug/zlib1.dll` aside and rebuilding
  `EmpyBrokerRuntime` restored the original SHA-256 without relinking;
  `Targets/Build/runtime-repair.log` records the build.

```powershell
$python = 'C:/Users/pouhela/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe'
& $python Tests/ClusterSmoke.py --bin Targets/BuildRelease/Release `
    --openssl C:/Libraries/msys64/ucrt64/bin/openssl.exe
& $python Tests/ClusterSmoke.py --bin Targets/BuildRelease/Release `
    --openssl C:/Libraries/msys64/ucrt64/bin/openssl.exe --quic
& $python Tests/ClusterSmoke.py --bin Targets/BuildTcp/Release `
    --openssl C:/Libraries/msys64/ucrt64/bin/openssl.exe
```

The ready-to-run directories contain `EmpyCore.dll`, `EmpyMQ.dll`,
`EmpyClient.dll`, OpenSSL's `libcrypto-3-x64.dll`/`libssl-3-x64.dll`,
`libgcc_s_seh-1.dll`, `libstdc++-6.dll`, `libwinpthread-1.dll`, `zlib1.dll`
and (QUIC builds) `msquic.dll`. Executable manifests record the actual imported
subset plus runtime-loaded providers. Do not replace a staged DLL with another
version without rebuilding and rerunning the clean-PATH test.

The earlier standalone Release smoke run loaded `Configs/Broker.yaml` and
`Configs/Client.yaml`, used an ephemeral loopback port, and verified:

- QoS 1 publication of `21.4` to `sensors/room/temperature`, received once by
  a `sensors/#` subscriber.
- Two consecutive requests to `rpc/temperature`, each answered with `21.4`.
- Release monitoring reporting five publications and zero online clients after
  their clean shutdown; all five executable processes exited successfully.

Local smoke logs and its harness remain under ignored `Targets/BuildRelease/Smoke`
and `Targets/BuildRelease/Smoke.ps1`. CTest logs are under each build's
`Testing/Temporary` directory. These generated artifacts are not source dependencies.

The Linux wrapper passed `bash -n`; the Windows wrapper correctly rejected an
invalid configuration with exit code 1. Conan wrapper commands are documented
but were not exercised through a fresh Conan install. Linux and MSVC builds
were not executed on this Windows/MinGW validation run.

## Continuing work and explicit boundaries

1. Preserve the MHF/2 envelope and storage manifest when extending live B2B.
   `ClusterNode` owns lease safety; `SessionQuorum` alone remains only a primitive.
2. Online membership migration, certificate/epoch rotation without downtime and
   descriptor-only payload transfer need separate versioned designs. Current
   membership changes require offline recovery/migration.
3. Keep directory locks, commit-before-ACK behavior and bounded replay admission.
   Cipher state must never be cloned or rolled back; it stores no payload key.
4. Extend fault qualification to Linux/MSVC, long partitions, slow disks,
   suspend/resume and clock perturbation; preserve raw evidence.
5. Qualify the PRT-based request-ID encoding with independent peers and define future CID/PID
   rollover, server-freshness authentication and capability/version negotiation.
