# Live clustering and recovery

The broker supports a configured MHF cluster over mutually authenticated QUIC
or TLS 1.3. `ClusterNode` runs a separate I/O executor; the broker actor waits
for bounded quorum operations without blocking the backplane executor. Core
remains independent of networking. The B2B binding and storage schemas are
private, versioned implementation choices where the paper leaves details open.

## Configuration and startup

Start with [BrokerCluster.yaml](../Configs/BrokerCluster.yaml). Provision one
distinct certificate/key per broker, a common trusted CA and a separate random
epoch HMAC key of at least 32 bytes. The certificate needs client and server
authentication usage. `fingerprint` is the SHA-256 of its DER certificate, written
as 64 hexadecimal characters. The local broker must also appear in `peers`.
`server_name` must match the certificate SAN. Do not share a broker certificate
or state directory between running instances.

All members must agree on the epoch, member IDs, pins, shards, replica count and
fanout. Use three replicas for tolerance of one unavailable replica; an operation
needs `floor(replicas / 2) + 1` responses from its selected replicas. The example
uses three brokers, four logical shards and fanout two. An unavailable majority
rejects writes and session takeover; it cannot preserve availability on both
sides of a partition.

Client-facing transport is configured at the YAML root. B2B transport has its own
listener, certificates, packet/queue bounds and `quic` switch inside `cluster`.
QUIC is the default backplane. A TCP-only build must explicitly select
`cluster.quic: false`, which uses mutual TLS. It never uses plaintext B2B.

The cluster state directory is mandatory and replaces the standalone
`state_directory`. Relative paths resolve against the YAML file. Restrict access
to private keys and state directories using OS permissions. State is checksummed,
not encrypted at rest; E2E-protected publications remain ciphertext at brokers.

For a reproducible local demonstration, the following generates temporary test
credentials/configurations and runs the actual executables. It kills a primary,
checks client failover, kills the full cluster, restarts it and checks retained
replay. Generated logs/configurations remain under `Targets/ProcessSmoke`.

```powershell
python Tests/ClusterSmoke.py --bin Targets/BuildRelease/Release `
    --openssl C:/Libraries/msys64/ucrt64/bin/openssl.exe --quic
```

Omit `--quic` to exercise the mutual-TLS backplane. Test credentials expire after
one day and are never installed in the Windows certificate store. The script
uses a Windows-only PATH for application processes, verifying DLL independence
from compiler/dependency search paths.

## Routing and profiles

Full canonical topic hashes select profile shards (PSRs). Wildcard profiles are
visible in every potentially matching shard. Quorum reads merge the newest
`(fence, revision)` record for each session/shard. The session owner publishes a
complete versioned profile snapshot, including explicit empty replacements for
removed shards. This is the live convergence mechanism; `ObservedProfiles`
remains a separately tested reusable OR-set implementation. The live binding
does not transmit OR-set deltas or claim causal compaction from those deltas.

Eligible recipients are deduplicated by SID. Shared subscriptions select one
member per `(group, filter)` across the cluster, with round-robin cursors at
each ingress. Fairness is local to each ingress. Each request has its own
quorum-owned resource keyed by requester SID and the two-byte correlation ID.
Several requests may coexist on the same topic, including equal IDs from
different requesters. A responder receives a session-local mapped ID and echoes
it in RES; its owner restores the originating requester ID before routing back.
Responder authorization also checks topic, deadline and remaining admitted SIDs.
The requester restores its outstanding and partially completed exchanges when
its SID moves; responder mappings and allocation counters move with the
responder's SID. The requester snapshot is committed before request fan-out.
Reliable REQ and RES frames also have a bounded pending-admission record. A
successor completes any interrupted inbox replication with the original message
identity and records the incoming receipt before accepting resumed traffic.
This covers owner failure on either side of forwarding, before PUBACK is
durable. Recipient deduplication and responder admission still apply.

An immutable message identity binds the origin session, original sequence,
topic, accepted epoch and payload. QoS 0 has a separate persistent origin
sequence. The ingress-pinned bounded tree forwards authenticated MHF/1 records,
validating parent and rank at each hop. Native QUIC uses separate control/data
channels, datagrams for eligible QoS 0 traffic and bounded bulk streams.

QoS 1 admission durably replicates a recipient inbox record to its SSR majority
before acknowledging ingress acceptance. Tree notifications wake recipients;
periodic quorum inbox reads recover from a lost branch or notification. The
recipient commits receipt identity and outgoing PID before sending, then writes
a consumed tombstone. This reference path replicates full payload records;
descriptor-only dissemination and anti-entropy batching are optimizations, not
properties of the current transport. QoS 0 has no durable delivery promise.

Admission across several destinations is not a distributed atomic transaction.
A rejected or interrupted publish may have reached some recipients. Retries
keep the original identity. PUBACK is protocol acceptance, not a transaction
with the application's business effect.

## Session ownership and fencing

Rendezvous placement selects session-state replicas (SSRs). An owner acquires
an exclusive majority lease using a persistent fencing counter and a random
process incarnation. It adopts the highest accepted snapshot returned by the
grant quorum and writes that adoption to a quorum before serving the session.
Acknowledgements and outgoing deliveries follow the durable checkpoint and a
final ownership check. An owner that loses its majority stops client delivery.

Voters grant a lease for `lease_ms`; the holder treats its grant as valid for
only one third of that duration, measured from sending the request. Renewals
cannot resurrect an already expired local lease. Restarted voters quarantine
new grants for a full lease duration because monotonic deadlines do not survive
restart. The implementation assumes crash/partition faults, unique broker
identities and bounded clock-rate drift (ordinary clocks within 1% are well
inside the conservative timing margin). It is not a Byzantine consensus system.

Ownership uses a clock that includes machine suspension: `GetTickCount64` on
Windows and `CLOCK_BOOTTIME` on Linux. Wall-clock synchronization is not used to
grant ownership. Persisted message/session expiry uses Unix milliseconds;
administrators must maintain reasonable wall clocks for TTL semantics.
Expired state is collected only after its expiry and a lease grace period;
late expired writes/index updates cannot resurrect it.

Lease rationale: [Gray and Cheriton, Leases](https://www.cs.cmu.edu/afs/cs.cmu.edu/academic/class/15712-s12/www/papers/gray89.pdf).
Clock behavior: [Windows time](https://learn.microsoft.com/en-us/windows/win32/sysinfo/windows-time),
[Linux clock_gettime](https://man7.org/linux/man-pages/man2/clock_gettime.2.html).

Membership is static for a running cluster. A state manifest binds placement,
epoch and certificate pins; mismatched configuration fails startup. Changing
membership/epoch requires an explicit offline migration and application recovery
plan. Arbitrarily editing `peers` is not an online shard migration protocol.
Never roll state backwards or clone an active broker's identity/directory.

## Persistence and replay

`FileStore` enforces byte/record bounds and an exclusive OS directory lock. Each
record has a format marker, key, length and SHA-256 checksum. Updates use a
temporary file, OS flush and atomic replacement. POSIX replacement also fsyncs
the directory; Windows uses flushed files and write-through replacement.
Abandoned temporary files are ignored, corrupt committed records fail startup.
The OS/storage must honor flush requests. Network filesystems are not qualified.

Cluster nodes open their store in journaled mode (since 2026-10-12). Each replica
Put/Erase appends one checksummed entry to `<hash>.log` and flushes it before the
operation replies, instead of writing, flushing and renaming a record file. When
the journal exceeds the larger of 8 MiB and the live record bytes, all records are
written into one atomically replaced `store.snapshot` and the journal restarts.
Replaying a journal over its snapshot is idempotent, so a crash between the two
steps is safe; a torn final entry is dropped and other damage fails closed.
Existing per-record directories are loaded on first open and their `.rec` files
are removed after the first snapshot. On the test host this cut a replica write
from about 10 ms to 2.7 ms. The durability boundary (flushed before reply) is
unchanged; group commit across operations remains future work.

Brokers also commit per transport read: all frames of one TCP/TLS read or QUIC
stream/datagram event form one transaction, and its single checkpoint precedes
their acknowledgements and deliveries. QoS 0 cluster deliveries are queued and
drained as one group. Inbox records consumed in one drain are acknowledged with
concurrent quorum rounds. An idle inbox poll backs off from 150 ms to 2 s, since
delivery notifications wake the drain immediately. Retained-queue updates write
the lock record and shard index concurrently and do not wait for the lock release.

Standalone broker persistence saves sessions, profiles, aliases, receive
checkpoints, pending deliveries, retained queues, outstanding requests and the
authentication replay cache. Since 2026-10-12 it uses an EBR5 base snapshot plus
an append-only EBJ1 journal. Each commit appends only the sessions, removed SIDs,
retained topics and new authentication tokens it changed, in one checksummed
entry flushed before the commit's ACKs or deliveries are released. The journal
is compacted into a new base once it exceeds the larger of 1 MiB and the base
size. The base carries a generation number, and entries written for an older
base are ignored, so a crash between compaction and journal reset is safe. A
torn final entry is dropped on restart; damage elsewhere fails closed. Existing
EBR4 snapshots are read as generation zero and rewritten as EBR5 at the first
compaction. Cluster persistence uses per-resource quorum records. Active sessions checkpoint periodically; recovery
expiry is conservative and includes the configured lease detection allowance.
Broker restart resumption still requires the client's surviving session state.

Session snapshots are ESS5 since publication PIDs wrap: they store 64-bit
publication sequences, and ESS4 snapshots are still loaded. All members must run
the same build, because older members reject ESS5. The PRT-based request-ID
encoding introduced session format ESS4, standalone snapshot
EBR4, unchanged request metadata ERQ1 and cluster manifest schema 4. Sessions,
inboxes and pending admissions embed wire publications; the new schema prevents
mixing them with the previous COR-marked format. Used-ID bitmaps, response
mappings and partially completed request state remain supported. ESS2/ESS3,
EBR2/EBR3 and manifest schemas 2/3 cannot be loaded by this build.
Upgrade all cluster members together:
drain active work, preserve the old state as a backup, and use explicitly
provisioned fresh state directories and fresh client SIDs, or implement and
validate an offline migration first. No automatic migration, deletion or reset
of existing state is performed. Wire RPC peers must all use PRT to select the
mandatory request ID; the former COR bit must be zero.

Replica scans are paged. A Scan request carries a resume key, and each reply
fits within the backplane packet limit minus 4 KiB, always holding at least one
record, plus a continuation flag. The caller merges keys only up to the smallest
truncated boundary across the majority's replies, so every returned key is still
covered by a full majority. Shards and inboxes larger than one packet therefore
remain readable. This changes the private Scan RPC body, so every member must
run the same build. MHF/1, MHF/2, the manifest schema and snapshot formats are
unchanged.

Each session inbox has its own budget, in addition to the global store budget.
The `cluster` YAML keys `max_inbox_bytes` and `max_inbox_records` set it. Zero
means derived: bytes = min(max_state_bytes, max(max_state_bytes / 16,
max_packet_bytes)) and records = max(max_records / 16, 1). An inbox write over
budget fails with BUSY, and no successful acceptance is reported. A later retry
keeps the original message identity. Usage is rebuilt from the store at startup.

Measured reliable-path costs (10 brokers on one Windows host, one SSD, 2026-10-12):
a QoS 1 publication waits on about six quorum round-trips inside the broker's
single protocol thread (inbox replication, session checkpoints, retained-queue
lock, read, grant, adoption, write and index). With 20 publishers, one topic each,
and every subscriber on a different broker than its publisher, the fabric carried
40 publications per second at a 178 ms median without loss. At 100 offered per
second it accepted 91.5 (p50 174 ms, p99 2.19 s) and at 200 only 35.3, with
CPU below 10%.
Shared topics published from several brokers contend for the retained-queue lock.
The remaining ceiling is synchronous: an asynchronous admission pipeline (and
replica group commit or a per-topic retention owner) is the next scalability step.
Use `EMPY_DASHBOARD/cluster.py` to reproduce and extend these measurements.

Retained replay has a bounded backlog separate from the in-flight window.
PUBACK releases credit and continues replay in order. Configure
`max_replay_bytes`, `max_inflight`, `max_session_bytes` and disk budgets together.
Over-budget operations fail admission; the implementation does not silently
truncate a replay after the first window.

`FileDurabilityStore` implements the reusable retained-descriptor adapter with
identity/checksum validation and expiry compaction. Cluster retention currently
uses quorum-owned topic queues and replicated payload records directly.

For clients, `payload_state_directory` enables durable nonce reservations,
authenticated-envelope replay windows and monotonic epoch policy. The directory
is exclusively locked and bound to the key grant without storing the key.
Ciphertext is released only after reserving its nonce, and plaintext only after
recording its replay identity. A failed state write stops cipher use until it is
reopened. Never clone/roll back this directory. Random publisher prefixes still
have a probabilistic cross-device collision bound; globally allocated publisher
prefixes or a hardware-backed key service remain deployment-specific choices.

This cipher storage does not persist the client's complete MMQP session or make
application callbacks transactional. A new client process establishes a new
SID. A surviving client configured with `auto_reconnect` rotates
`failover_endpoints` and resumes its SID after broker failure. Explicit disconnect
stops retries. Publication PIDs wrap and never require a new session.
Correlation-ID exhaustion and unsafe expired receive-window gaps still require a
fresh session. Configure operation deadlines longer than the anticipated
outage/lease acquisition interval.

## Tenant realms and authentication

`tenants` configures distinct identities, roots, authentication keys and optional
shard allowlists. It replaces root-level `auth_key_env`/`tenant_root`. The first
eight token bytes identify SHA-256(`MMQP/AUTH/` + identity); the other 24 bytes
remain random. The HMAC exchange is unchanged in length. A client selects its
realm using `auth_identity`; a resumed SID cannot change realm.

Namespace checks cover registrations, subscriptions, publications, wills and
inbox delivery. A shard allowlist must include all shards a wildcard can match.
This is a tenant credential service, not per-device ACLs. All cluster members
must receive the same tenant policy. B2B peers are trusted brokers and must
present distinct pinned, CA-verified certificates plus the epoch HMAC key.

Token replay entries survive configured broker restarts and are coordinated
through quorum resources in a cluster. This strengthens replay handling but
does not add the fresh server challenge missing from the paper's handshake.
Secure transport remains required for remote authenticated deployment.
