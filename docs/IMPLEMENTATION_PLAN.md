# MMQP implementation plan

Source of truth: [README.pdf](../README.pdf), 21 pages. Page numbers below refer
to the PDF page index. This is an implementation roadmap, not a claim that the
paper's performance or availability results have been reproduced.

## Architecture and delivery order

1. `EMPY_LIB_MQ/include/eMQ/Core` (merged into `EmpyMQ` on 2026-10-12): shared platform, events, entities, workers,
   configuration and the Service / Context / Runtime / Module lifecycle.
2. `EMPY_LIB_MQ` / `EmpyMQ`: shared MMQP wire format, transports, protocol state,
   broker services and reusable fabric primitives; links Core publicly.
3. `EMPY_LIB_CLIENT` / `EmpyClient`: shared client session and application API,
   payload encryption and key handling; links MQ publicly.
4. `EMPY_IMPL_BROKER` / `EmpyBroker`: broker configuration and modules.
5. `EMPY_IMPL_CLIENT` / `EmpyKlient`: executable client example / command line.

Implement in this order: build and lifecycle -> codec -> state and routing ->
TCP integration -> security -> fabric and QUIC -> operational qualification.
Every feature must have positive, negative and resource-bound checks before its
status is upgraded. See [PROTOCOL_DECISIONS.md](PROTOCOL_DECISIONS.md) for choices
that the paper does not fully specify, and [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md)
for the verified implementation status and commands.

## Feature plans

| ID | Feature and source | Owner | Implementation and acceptance criteria |
| --- | --- | --- | --- |
| F01 | Build and shared libraries; user architecture | All five targets | Correct dependency order, public include paths, distinct DLL export macros, configurable dependency discovery, C++20. Build the libraries, executables and tests in Debug and Release. |
| F02 | Service, Context, Runtime, Modules; V, pp. 19-21 | Core | Keep ECS and template architecture. Bind each module to its own service, dispatch worker events on the owner thread, defer detach safely, invoke lifecycle hooks and drain workers before destruction. Test two independent runtimes and self-detaching modules. |
| F03 | Fixed header and remaining length; I-B, Tables 1-2 | MQ/Common | Implement all nine packet types, big-endian 1-4 byte RLF lengths, partial stream assembly and packet limits. Test boundary values, fragmented/coalesced frames, malformed flags and advertised oversized frames without allocating their payload. |
| F04 | Packet bodies; Tables 3-5, 8-15 | MQ/Common | Typed messages and symmetric encode/decode for optional session blocks, profiles, publications, reasons and E2E envelope. Validate reserved bits, UTF-8 strings, aliases, PIDs and trailing data. Use independent golden bytes, not only round trips. |
| F05 | Tiered intervals; I-C3, Tables 6-7 | MQ/Common | Encode exact representable positive intervals and decode all four tiers. Reject zero multipliers, overflow and unrepresentable durations. No silent rounding. |
| F06 | Session establishment and resumption; I-C1 | MQ/Runtime, Client | CONREQ/CONACK state machine, fresh identifiers, admission limits, negotiated policy, bounded handshake timeout, session expiry and reattachment. Test expired, active and missing SIDs, reconnect and state preservation. |
| F07 | Mutual authentication; I-C2, Figure 2 | MQ/Security, Client | Implement the specified HMAC exchange with separate authentication credentials, secure random tokens and constant-time verification. Enforce broker policy before session admission. Track token replays. Document the paper's missing server freshness and require a secure deployment profile before treating the exchange as Internet-safe. |
| F08 | Session policy, keepalive and disconnect; I-C3,7 | MQ/Runtime, Client | Negotiate reason strings and packet limits; broker initiates CONSYN after KAI; enforce response deadlines; handle CONEND and transport failure once. Test idle peers, malformed acknowledgements and graceful shutdown. |
| F09 | Birth and last wills; I-C5 | MQ/Runtime | Publish `$bwm/` and `$lwm/` topics through reserved internal routing. Apply negotiated QoS and resource limits. Test connection, reconnection and both disconnection paths without duplicate wills. |
| F10 | Topic profiles and authorization; I-D, Tables 9-10 | MQ/Common, MQ/Runtime | Atomic mixed SUB/REG/UNS batches, publisher registration checks, requested QoS ceilings, request roles, topic canonicalisation and whole-level `+` / terminal `#`. Test rejection without partial profile changes. |
| F11 | Alias mapping; I-D4, I-E3,4 | MQ/Runtime, Client | Session-scoped directional alias tables, wildcard alias zero, bounded alias allocation, TOP until PROSYN confirms mapping. Test collisions, unknown aliases, repeated acknowledgements and reconnect. |
| F12 | Pub/sub and shared subscriptions; I-A, I-D1, I-E | MQ/Runtime, Client | Route only to eligible profiles, preserve the paper's QoS filter, select one member per shared group and suppress overlapping local matches. Test real TCP publisher/subscriber exchange and subscriber disconnect. |
| F13 | QoS and retransmissions; I-E2, Table 13 | MQ/Common, MQ/Runtime, Client | Ordered checkpoint, unordered receive window, original PID and DUP on retry, bounded outstanding sends and retry deadlines. Wrap the 16-bit wire PID with 64-bit sequences and serial arithmetic bounded by the 256-PID window. Test lost ACKs, out-of-order arrivals, duplicates, queue saturation and continuation across the wrap. Durable application exactly-once effects require transactional application storage. |
| F14 | Retention and expiry; I-D2 | MQ/Runtime | Retain only QoS 1 publications; implement last-value and bounded FIFO policies, per-message expiry and replay to new eligible subscriptions. Test queue eviction, expiry, memory admission and QoS 0 exclusion. |
| F15 | Request/response; I-E1 and the author's request-ID extension | MQ/Common, MQ/Runtime, Client | PRT REQ/RES alone selects a mandatory nonzero big-endian U16 CID at QoS 0/1, separate from the QoS packet ID. Keep the former COR bit reserved and zero. Match exchanges by requester SID/CID and canonical topic; map IDs per responder session to handle equal IDs from different requesters. Preserve IDs through retries, no-responder results, timeouts, partial multi-responder completion and session recovery. Bound active state and never reuse IDs in a SID. Verify same-topic concurrency, reverse replies, invalid/reused IDs, lost ACKs, exhaustion, encrypted traffic and clustered requester/responder failover. |
| F16 | E2E encryption and hierarchy; II, Table 14 | Client/Security; crypto helpers in MQ | Vendor a pinned official Ascon reference, use a maintained SHA-256/HMAC provider, define HKDF salt/info/length, derive topic/epoch keys and support delegated subtree keys. Use random-prefix/counter nonces and canonical topic plus epoch as AAD. Verify published KATs, hierarchy boundaries, altered topic/epoch/tag/ciphertext and plaintext non-release on failure. |
| F17 | Key lifecycle and replay policy; II-C,D,F,G | Client/Security | Bound and wipe key caches; separate auth and payload keys; reject epochs outside policy and nonce/counter exhaustion. Provision secrets outside the broker. Persistent anti-replay and hardware-backed keys need explicit deployment integration. |
| F18 | TCP asynchronous transport; V-B2 | MQ/Network | Repeated accepts, bounded reads/writes, owned buffers, serialized callbacks, connect deadlines and orderly worker shutdown. Keep networking out of Core. Test fragmentation, multiple clients, abrupt close and oversized frames. |
| F19 | QUIC transport and policy; III, Table 15 | MQ/Network | Validate SAS codes, bulk stream bounds and QoS 0 datagram limits. Integrate MsQuic behind an explicit build option with verified certificates, per-stream decoders and send-buffer lifetime through completion. Test control/QoS separation, datagram oversize fallback, cancellation and certificate rejection. Never silently substitute TCP for QUIC. |
| F20 | Profile shards and replica placement; IV-B,C | MQ/Cluster | Stable canonical hashing and deterministic top-R rendezvous selection. Place wildcard predicates in all potentially matching shards. Test membership-order independence, shard limits and wildcard visibility. |
| F21 | Dissemination trees and identity; IV-D,E,H,L | MQ/Cluster | Pin ingress at rank zero, sort destinations deterministically with a broker-ID tie break, bound fan-out and validate parent/rank on every hop. Preserve accepted-message identity across epoch retries. Test reachability, no duplicate nodes, monotonic ranks, subtree recovery and bounded duplicate filters. |
| F22 | Profile convergence and compaction; IV-F | MQ/Cluster | Observed-remove profile assertions with unique origin/version dots, merge and per-origin replica acknowledgements. Compact only causally stable tombstones. Test remove-before-add, concurrent add/remove, duplicate deltas and lagging replicas. |
| F23 | Session replication and epochs; IV-G,L | MQ/Cluster | Replica selection, unique-member majority acknowledgements, immutable versioned snapshots and epoch fencing. A quorum counter alone is not leader election: add a reviewed membership/primary-lease protocol before enabling autonomous multi-process failover. Test stale epochs, duplicate ACKs and minority refusal. |
| F24 | Backpressure, retained descriptors and durability; IV-J,K | MQ/Cluster, broker modules | Bound bytes and records; expose drop/defer/reject decisions and retained descriptors. Define an optional durability adapter outside routing. Qualify crash recovery, fsync, replay and compaction separately from in-memory delivery. |
| F25 | Tenant isolation and back-plane security; IV-M | MQ/Cluster, broker modules | Enforce namespace and shard budgets, epoch-bound HMAC control records, authenticated peer identity and replay checks. Use mutually authenticated QUIC for live B2B links. Test cross-tenant operations, stale epochs and forged records. |
| F26 | Configuration, monitoring and extension points; V | Core/Auxiliaries, MQ/Monitoring, implementations, dashboard | Use typed YAML/JSON and explicit resource limits. Expose executor-owned broker/session/link snapshots through an opt-in read-only endpoint. Collect multiple brokers into a dark web dashboard with live topology, counters, history, session inspection and stale-state handling. Verify real traffic, broker loss, resource/authentication boundaries and browser interactions. Provide runnable configurations and shutdown procedures. |
| F27 | Scientific validation | Tests, docs | Preserve a feature-to-test map; run interoperability vectors and loopback integration before stress tests. Add benchmarks only with recorded machine, transport, payload, concurrency and failure model; do not inherit unverified performance claims from the paper. |

## Interoperability gates

The PDF does not define a complete B2B byte format, cluster membership consensus,
request correlation token, 16-bit PID rollover, full reason-code registry or
cryptographic variant negotiation. Keep every implementation choice versioned and
visible. A research reference implementation is not a substitute for multi-node
failure testing, a security review or wire interoperability testing with another
implementation.

F15 implements a two-byte request ID selected exclusively by PRT REQ/RES.
ESS4/EBR4 and cluster manifest schema 4 reject incompatible stored state, and
the former COR bit is rejected as reserved. Pre-ID payloads cannot always be
distinguished from the new header by inspection. Matched RPC peers are required;
mixed-version negotiation and independent interoperability remain future work.

## Delivered integration, 2026-09-29

The feature plans above remain the acceptance checklist for future changes.
This delivery completes the main runtime integrations that were previously
marked as primitives or missing. Validation evidence is in
[VALIDATION.md](VALIDATION.md); deployment choices are in [CLUSTERING.md](CLUSTERING.md).

| Feature | Delivered implementation | Future qualification or extension |
| --- | --- | --- |
| F01 | Recursive PE runtime-DLL staging, unresolved-import failure, rebuild repair and SHA-256 manifests | MSVC redistributable packaging and Linux toolchain runs |
| F06-F07, F23 | Durable session snapshots, quorum owner leases/fencing/adoption, token replay coordination and automatic client SID failover | Long partitions, suspension and storage faults; wire-level server challenge requires an agreed extension |
| F12-F15 | Cross-broker pub/sub/shared groups, concurrent correlated request/response with persisted responder mappings, FIFO/last-value retention and credit-driven retained replay | Large fan-out/load benchmarks and application transactions |
| F16-F17 | Optional durable cipher nonce reservation, replay windows and monotonic epoch policy, key-grant binding and exclusive ownership | Hardware secret provider and deterministic cross-device publisher prefix provisioning |
| F19-F21 | Native QUIC/TLS B2B, pinned identities, live PSR queries and versioned profile snapshots, authenticated bounded trees, replicated-inbox branch recovery | SAS 0 requires an application-defined policy; independent interoperability/path testing |
| F22 | Live fenced snapshot replacement/expiry plus the reusable observed-remove delta/compaction component | Delta/batch transport optimization; live snapshot semantics are an explicit choice |
| F24 | Atomic flushed FileStore, descriptor adapter, persisted broker state, quorum inbox/retained records, expiry cleanup and bounded replay | Descriptor-only transfer optimization, disk-full and power-loss qualification |
| F25 | Tenant identity/key/root/shard policy, B2B certificate pins, mutual authentication and epoch MACs | Per-device identity-provider policy and online certificate rotation |
| F26 | Typed configuration, runtime manifests, bounded telemetry snapshots/listener, live dark dashboard, real three-broker demo and paced CLI traffic | Operator identity integration at a TLS proxy, durable time-series storage, alerts and larger deployment/load qualification |
| F27 | TLS/QUIC three-node recovery suites and separate-process kill/restart smoke harness | Independent interoperability, sanitizers, long stress runs and measured scientific results |

Static membership is implemented; online consensus-driven membership migration
is not. Client API process-state restoration and exactly-once business effects
are not implied by broker or cipher persistence. These boundaries are deliberate
and must remain visible in later work.

## F26 dashboard implementation reference

| Step | Implementation | Acceptance and future reference |
| --- | --- | --- |
| F26.1 Observations | Typed process-local broker/session counters, queue/profile/ownership snapshots, actual authenticated peer links, instance and cluster manifest identities, local record-byte accounting | Sample on each owning executor. Preserve MMQP bytes and ESS4/EBR4 state. Counters omit secrets/payloads and state their send-admission/reset boundary. |
| F26.2 Telemetry | Opt-in loopback HTTP endpoint with optional environment-sourced bearer token; independent sampler and shared bounded cache | Enforce header/connection/deadline/session/response bounds, GET-only routes, Host/Origin checks and deterministic shutdown before the broker. A stalled sample remains detectably stale. |
| F26.3 Collection | Standard-library Python collector, explicit bounded source list, verified HTTPS or local/tunneled HTTP, schema allowlist, histories and restart-safe deltas | Isolate source errors, reject redirects, exclude stale counters from totals, mark unknown client state, preserve last observations and bound memory history. |
| F26.4 Web views | Dark responsive HTML/CSS/ES modules: live SVG topology, traffic charts, inventory, session search/filter/pagination/detail, activity/export, pause and refresh | Verify actual broker/client data, topology link changes, filters, export, desktop/mobile layout, empty/error states and no browser errors. Keep user-controlled text out of HTML interpolation. |
| F26.5 Operation | Example configs, standalone and three-broker demos, paced CLI traffic, reproducible protocol/collector/UI/process tests | Verify clean-PATH Windows executables and existing DLL staging. Document source membership, metric meanings, remote-access boundary and exact commands; do not claim benchmark or quorum results from the UI. |
