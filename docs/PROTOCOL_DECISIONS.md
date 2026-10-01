# Protocol decisions and open issues

The source is [README.pdf](../README.pdf). Tables determine bit positions;
illustrative byte counts and example reason numbers are not normative registries.
The original `Protocol.h` contains older constants that disagree with the paper.

## Wire format

- Integers, RLF lengths, PIDs, aliases and key epochs use network byte order.
  Tables explicitly show MSB first for PIDs and aliases; applying it to other
  multibyte fields is an implementation convention.
- RLF is `(length_bytes - 1) << 2`; lengths use the smallest possible width.
  The protocol can represent 32-bit body lengths, but configured limits are
  checked before allocation. String/token length prefixes are one octet.
- CONREQ RES and CONSYN TOK are bit 1 (`0x02`) of the control byte.
  Authentication SID/TOK are `0x80`/`0x40`. PUBREQ PRT occupies the low two
  control bits with values 1/2/3; DUP is `0x80` of its variable flags byte.
- Session flags are SAF/SPF/QPF/SWF/TPF = `80/40/20/10/08` (hex).
  Profile operation mask is `0xC0`; QoS 2 and 3 are rejected.
- The 2026-09-29 request/response extension assigns PUBREQ variable-header flag
  bit 4 (`0x10`, `COR`) to a mandatory nonzero U16 correlation ID on REQ and RES.
  In both QoS levels, the ID follows the optional topic and QoS 1 PID, before
  REQ's response-count byte or RES's reason block. Byte order is big endian.
  MSG must omit COR and the ID; its encoding is unchanged. Other reserved bits
  remain rejected. CONREQ stays at version 1: the COR flag makes legacy codecs
  reject extended RPC packets rather than misinterpret shifted fields. Updated
  codecs reject unmarked legacy RPC. Deploy matched brokers and clients; no
  capability negotiation or mixed-version RPC interoperability is claimed.
- Reason strings appear after a nonzero reason iff IRS is negotiated. This
  includes `0x80`, which the prose's strict inequality accidentally omits.
  The initial code's reason registry is retained; missing codes are explicit
  implementation additions, not numbers asserted by the paper.
- Embedded TPF contains the PROREQ body, including its PID, preceded by its
  own RLF header/length. CONACK reports admission; it does not cause a second
  PROACK for that embedded request.
- All topic paths are canonicalized by removing leading/trailing separators
  and empty levels. UTF-8 must be well formed. Wildcards occupy a complete
  level and `#` is terminal. Reserved `$` topics require explicit matching;
  ordinary root wildcards do not receive status messages.
- Alias maps are directional and scoped to a session. A subscriber's declared
  alias describes its incoming namespace; a publisher's alias describes its
  outgoing namespace. New wildcard aliases use PROSYN in the reverse direction.
  Newly allocated subscriber aliases from clients occupy `1..32767`; broker-assigned concrete
  wildcard aliases occupy `32768..65535`. This prevents allocation races between
  a PROREQ and an in-flight publication. Publisher aliases can use `1..65535`.
  A later PROREQ may reference an already known broker-assigned alias without
  rebinding it.

## Delivery and state

- PIDs are 16-bit on the wire but monotonic within a session and direction.
  Exhaustion is an error requiring a fresh session, never silent wraparound.
  Profile request IDs are independent of publication sequence numbers.
- An unordered receiver rejects a publication beyond its bounded window rather
  than forgetting missing PIDs. Reliable sends must stay within that window.
  QUIC uses a 256-PID window even when some streams individually preserve order.
  Send admission reserves capacity before advancing the PID. Outstanding sends
  cannot advance 256 positions past the oldest unacknowledged PID. If an allocated
  outgoing PID expires on a QUIC session, that session cannot resume: no wire
  mechanism can communicate a skipped position safely.
- A QoS 0 subscription receives only QoS 0 publications, as stated on page 5.
  QoS 1 subscriptions accept both. This differs from MQTT QoS downgrading.
- Request identity is `(requester SID, correlation ID)`, with its canonical
  topic, deadline and admitted responder SIDs recorded separately. Multiple
  exchanges can use the same topic, and different requesters can use equal IDs.
  The broker allocates a distinct responder-facing ID per delivered request and
  persists its mapping to the requester/ID/topic/deadline. Responders echo that
  ID; the broker restores the requester's original ID. Only a matching, unexpired
  exchange and an admitted, not-yet-consumed responder may supply a response.
  No-responder/timeout RES packets also carry the original requester ID.
- IDs range from 1 through 65535 and are never reused in a SID, including after
  rejection, completion or timeout. Fresh request admission claims an unused ID;
  QoS 1 duplicate receipts bypass that claim and bind the CID in their content
  digest. Client allocation accepts unused explicit IDs in any order, or chooses
  the lowest unused ID automatically. Exhaustion requires a fresh SID. A lazy
  8192-byte bitmap records used requester IDs; responders use a nonwrapping
  allocation counter plus bounded live mappings. `0x90` denotes invalid
  correlation. Resource limits can still reject concurrent requests as busy.
- Broker snapshots preserve pending requests, partial response sets, consumed
  requester IDs and responder mappings. Cluster directory keys hash SID + U16
  CID, and the requester's session snapshot is checkpointed before fan-out.
  Reliable REQ/RES admission also checkpoints the original frame. If an owner
  fails before writing its incoming receipt, its successor completes the same
  recipient-inbox writes and stores the receipt before serving resumed traffic.
  This preserves correlation without treating the outstanding retry as a new
  or reused ID. Replay is bounded by session bytes/in-flight limits and the
  original deadline. Expired exchanges release their directory leases even
  when delivery of the timeout notification is backpressured.
  Both requester and responder state follow their SIDs through resumption.
  The new persistent formats are ESS3, EBR3, ERQ1 and backplane manifest schema 3;
  prior state is rejected rather than guessed or silently migrated.
- Wills use the paper's `$bwm/` and `$lwm/` prefixes. A missing will suffix is
  the SID. The paper describes LWM for disconnection generally, so clean and
  unclean disconnections both publish it once.
  With a configured tenant root, an omitted suffix becomes `<tenant>/<SID>` and
  reserved status subscriptions are checked against the tenant part of the path.
- Checkpoints suppress retransmission duplicates within a running or retained
  session. Configured storage persists them before protocol ACK/delivery effects;
  clustered checkpoints require the SID's SSR majority and current ownership.
  They do not promise durable exactly-once business effects across process
  crashes. ACK timing and application transactions are separate.
  Cached receipts bind a PID to canonical publication content; changing that
  content under the same PID is a protocol error. Old PIDs outside the receipt
  cache are rejected rather than treated as new operations.

## Cryptography

- The author explicitly selected NIST SP 800-232 Ascon-AEAD128 on 2026-09-18,
  updating the paper's ASCON-128 choice. These variants are not wire compatible.
  See the vendored reference README for the pinned commit and test provenance.
- Define HKDF-SHA256 explicitly: parent key as IKM, four-byte big-endian epoch
  as salt, exact UTF-8 level bytes as info, 16 output bytes. Derive each level
  recursively. AAD is canonical UTF-8 topic followed by the same epoch bytes.
  The mutable broker-mapped CID is not part of this AAD. Payload authentication
  alone does not authenticate request correlation; its integrity relies on the
  configured secure hops and trusted brokers. No cryptographic format changed.
- Authentication credentials and payload root keys are separate. The broker
  must never be configured with an E2E payload root key.
  A broker can use one default realm or a `tenants` list with distinct identities,
  keys, roots and optional shard allowlists. Multi-realm clients prefix their
  32-byte random authentication token with eight SHA-256 identity bytes; the
  remaining 24 bytes stay random. See [CLUSTERING.md](CLUSTERING.md). This is not
  a per-device credential/ACL service.
- The paper's authentication handshake has no fresh server challenge. Token
  replay entries persist with broker storage and use quorum resources across
  cluster peers, but expire after one hour. This is not permanent transcript
  freshness. Secure transport and an interoperable freshness extension remain
  necessary considerations for Internet deployment.
- Random 64-bit nonce prefixes make collision risk small, not mathematically
  impossible across all devices. Optional cipher storage durably reserves each
  counter before ciphertext is released. Globally allocated publisher identities
  are still needed for a deterministic uniqueness guarantee across devices.
- Topic+epoch AAD does not authenticate PID, publication type or response count;
  encryption alone does not prevent replay on the same topic and epoch.
  Operational replay protection must be specified separately.
  The implemented client authenticates before updating a 256-counter window for
  each topic/epoch/nonce-prefix tuple. A known duplicate is never emitted twice;
  it may be acknowledged under a new broker PID during retained replay. An unseen
  envelope older than the window is rejected. Replay state is bounded and restored
  across restarts when `payload_state_directory` is enabled. Plaintext is released
  after that checkpoint, independently of application-side transactions.

## QUIC binding

- The ALPN bytes are `MMQP`, matching the current protocol alias. No 0-RTT
  application data is accepted. A QUIC build is an explicit dependency option.
- The client opens bidirectional stream ID 0 for control. SAS 1 also carries
  publications there. SAS 4 separates control, QoS 0 and QoS 1; SAS 3 additionally
  permits bounded bulk streams. SAS 0 has no generic interpretation and is rejected.
- Semantic publication streams are unidirectional in each direction. Their first
  octet selects class 1 (QoS 0), 2 (QoS 1) or 3 (bulk), followed by ordinary MMQP
  frames. The two semantic streams are persistent. A bulk stream contains exactly
  one publication and FIN; each direction has a negotiated concurrency bound.
  These stream-ID/class conventions fill a gap in the paper and require peer
  agreement; they do not assert compatibility with an independent QUIC binding.
- Datagrams contain one complete QoS 0 PUBREQ with no extra prefix. The negotiated
  threshold applies to payload bytes, while MsQuic's actual datagram limit applies
  to the entire encoded packet. Oversized datagrams and exhausted bulk concurrency
  fall back to the corresponding reliable stream, preserving the publication QoS.
- Negotiation chooses bounded supported values. Non-control frames that overtake
  CONACK are held within a bounded pre-negotiation queue. Resume preserves the
  negotiated policy. A changed transport/policy requires a fresh session.
- Certificate validation is mandatory. MsQuic/OpenSSL uses its built-in verifier
  and supplied CA; Schannel can use an existing certificate thumbprint. A custom
  Schannel CA is checked through the portable-certificate callback using OpenSSL
  chain, purpose, validity and server-name validation. That Schannel profile still
  needs deployment testing; the verified native suite uses MsQuic/OpenSSL.

## Fabric

- Use a stable, specified hash, never implementation-defined `std::hash`.
- Pin ingress at rank zero, following Figure 20; sort the remaining destinations
  by hash and broker-ID tie break. Sorting ingress together with all nodes as
  Equation 10 literally suggests can leave its predecessors unreachable.
- Hashing a wildcard predicate once does not place it in every matching concrete
  topic shard. With full-topic hashing, wildcard assertions must be visible to
  every logical shard; account for that replication cost explicitly.
- Carry the accepted publication's epoch/identity through retries. Recomputing
  an epoch-dependent UMID after failover changes its identity and breaks dedup.
- Tombstone stability is per origin and replica; unrelated brokers' sequence
  numbers cannot safely be compared as a single global watermark.
- The live binding uses static authenticated membership, durable fencing and
  majority leases with conservative holder deadlines and restart quarantine.
  Mathematical placement and majority counting alone remain insufficient.
- The paper provides logical FORWARD fields, not a B2B wire protocol. Any local
  envelope must be separately versioned and cannot claim external compatibility.
  `FabricCodec` defines a private MHF/1 envelope: magic/version/kind, immutable
  identity, accepted/routing epochs, origin sequence, expiry, session, ingress,
  sender rank, destinations and a full concrete-topic PUBREQ, followed by an
  HMAC-SHA256 tag. All multibyte integers are big endian; expiry is Unix time in
  milliseconds. The key must contain at least 32 bytes and be provisioned for the
  routing epoch by the caller. MAC verification precedes parsing/admission.
- SHA-256's first eight bytes, interpreted big endian, supply the stable 64-bit
  placement hash. Rendezvous scores include the epoch and use broker-ID tie breaks.
  Message identities include a separate nonzero 64-bit origin sequence, covering
  QoS 0 packets that do not have a wire PID. The accepted epoch is immutable;
  changing the routing epoch does not create a new message identity.
- `ObservedProfiles` separates assertion dots from removal-operation dots.
  Acknowledgements represent contiguous per-origin prefixes. Compaction requires
  every configured replica to acknowledge both the removal and its observed adds.
  Stable floors prevent resurrection from old snapshots. The caller authenticates
  delta senders and replica acknowledgements. The executable instead exchanges
  complete fenced profile snapshots under a single session owner, with explicit
  empty replacements for removed shard assertions. It does not send OR-set deltas.
- `EpochFilter` admits the current and previous routing epochs with bounded exact
  sets, refusing capacity overflow. `SessionQuorum` commits a version only after
  unique members acknowledge its digest in the configured epoch. Neither primitive
  elects a primary or advances a cluster epoch on its own.

## Live backplane binding

MHF/2 RPCs travel in MMQP PUBREQ envelopes on a dedicated mutual-TLS/QUIC listener.
The control topic is `$mhf/control`; payload transfer uses `$mhf/data`. An RPC
contains the `MHF2` magic, U64 epoch, U8 kind, U64 request ID, length-prefixed broker
identity, U32-length payload and HMAC-SHA256 tag. The HELLO carries a manifest
digest binding schema, membership and certificate pins. Native forwarding wraps
the MHF/1 record plus concrete recipient broker/SID pairs. QUIC uses the `MMQP`
ALPN; this is a private binding, not independently established interoperability.

Lease operations replicate `(fence, revision, expiry, snapshot)`. Session state
includes aliases, receive windows, outgoing PIDs, pending messages, fabric
receipts and replay queues. Reliable ingress first replicates recipient inbox
records. Expired values and consumed tombstones are collected within configured
budgets; rejected late expired writes cannot restore them. Read
[CLUSTERING.md](CLUSTERING.md) for the failure model, clock assumptions,
acknowledgement boundary, persistent schemas and offline membership changes.

## Monitoring binding

- Monitoring adds no MMQP packet type/field or persistent session-schema change.
  `Broker::Inspect` and `ClusterNode::Inspect` make bounded value copies on their
  owning executors. Packet/byte/publication observations are process-local and do
  not become replicated application state.
- The opt-in executable telemetry listener is loopback HTTP, separate from the
  client and B2B listeners. It serves schema 1 JSON, with an independent sampler,
  bounded cache/requests and optional bearer token. It is read-only, has no CORS
  route and never exposes payloads, topic filters, credentials or private keys.
  Stop it before the broker. Embedders start it explicitly.
- A broker-instance ID and monotonic uptime define the counter baseline. The
  cluster manifest digest scopes broker identities in topology views; configured
  peers are distinct from actual ready authenticated links. Lease samples are
  observations, not an operational quorum guarantee.
- The separate dashboard uses one-second HTTP collection by default. Stale and
  unreachable sources retain historical observations but do not contribute to
  live totals. Broker-observed sessions do not measure device CPU/RSS or MMQP RPC
  latency. Stored bytes count local record values, including replica copies.
- Both dashboard and telemetry bind to loopback. Remote operators/collectors use
  SSH forwarding or separately managed authenticated TLS proxies. Remote HTTP,
  URL credentials and redirects are rejected by the collector; HTTPS verification
  cannot be disabled. This is an operator console, not tenant-scoped access control.

Read [DASHBOARD.md](DASHBOARD.md) for endpoint bounds and metric definitions.

## External references

- [Official Ascon reference implementation and variant notes](https://github.com/ascon/ascon-c)
- [NIST SP 800-232, final standard](https://csrc.nist.gov/pubs/sp/800/232/final)
- [MsQuic credentials and provider differences](https://github.com/microsoft/msquic/blob/main/docs/api/QUIC_CREDENTIAL_CONFIG.md)
- [MsQuic stream and buffer lifetimes](https://github.com/microsoft/msquic/blob/main/docs/api/QUIC_STREAM_EVENT.md)
