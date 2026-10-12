#include <eMQ/Cluster/Node.h>
#include <eMQ/Cluster/Clock.h>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace eMQ
{
    namespace
    {
        enum class Operation : uint8_t { Read = 1, Grant, Write, Release, Put, Scan, Inbox, Consume };

        // Room for the MHF/2 envelope and PUBREQ framing around a reply page.
        constexpr size_t EMPY_CLUSTER_PACKET_RESERVE = 4096;
        constexpr size_t EMPY_CLUSTER_PLACEMENT_CACHE = 4096;
        constexpr size_t EMPY_CLUSTER_TOMBSTONE_SIZE = 8;

        using Targets = std::vector<std::pair<std::string, std::string>>;

        struct Value
        {
            uint64_t fence = 0;
            uint64_t revision = 0;
            Buffer bytes;
            uint64_t expiry = UINT64_MAX;

            auto Stamp() const { return std::pair(fence, revision); }
            void Save(Writer& writer) const { writer.U64(fence); writer.U64(revision); writer.U64(expiry); WriteBuffer(writer, bytes); }
            static Value Load(Reader& reader)
            {
                Value value;
                value.fence = reader.U64();
                value.revision = reader.U64();
                value.expiry = reader.U64();
                value.bytes = ReadBuffer(reader);
                return value;
            }
        };

        struct Lease
        {
            uint64_t fence = 0;
            std::string owner;
            LeaseTimePoint expiry{};
            Value value;
        };

        Buffer EncodeDelivery(const Digest& identity, std::string_view sender, std::string_view session,
            uint64_t expiry, std::span<const uint8_t> publication)
        {
            Writer writer(32 * 1024 * 1024);
            writer.Bytes(identity);
            writer.String(sender);
            writer.String(session);
            writer.U64(expiry);
            WriteBuffer(writer, publication);
            return writer.Take();
        }

        Buffer EncodeDelivery(const ClusterDelivery& delivery)
        {
            return EncodeDelivery(delivery.identity, delivery.sender, delivery.session, delivery.expiry,
                Codec::Encode(delivery.publication, {true, UINT32_MAX}));
        }

        // Reads only the record prefix; the publication is not decoded.
        uint64_t InboxExpiry(std::span<const uint8_t> bytes)
        {
            Reader reader(bytes);
            if (bytes.size() == EMPY_CLUSTER_TOMBSTONE_SIZE) return reader.U64();
            reader.Bytes(32);
            reader.String();
            reader.String();
            return reader.U64();
        }

        ClusterDelivery DecodeDelivery(std::span<const uint8_t> bytes)
        {
            Reader reader(bytes);
            ClusterDelivery result;
            auto identity = reader.Bytes(32);
            std::copy(identity.begin(), identity.end(), result.identity.begin());
            result.sender = reader.String();
            result.session = reader.String();
            result.expiry = reader.U64();
            result.publication = std::get<PublishRequest>(Codec::Decode(ReadBuffer(reader), {true, UINT32_MAX}));
            reader.Finish();
            return result;
        }

        std::string InboxPrefix(std::string_view session) { return "in/" + std::string(session) + '/'; }

        // Inbox keys are the session prefix followed by a 64-character hexadecimal identity.
        std::string InboxSession(std::string_view key)
        {
            if (!key.starts_with("in/") || key.size() < 3 + 1 + 1 + 64 || key[key.size() - 65] != '/')
                throw ProtocolError("Malformed inbox record key");
            return std::string(key.substr(3, key.size() - 3 - 65));
        }
    }

    struct ClusterNode::Impl
    {
        struct Link
        {
            Connection::Pointer connection;
            std::string expected;
            std::string peer;
            bool outgoing = false;
            bool ready = false;
        };
        struct Pending
        {
            std::string peer;
            Buffer packet;
            LeaseTimePoint deadline;
            bool sent = false;
            std::function<void(bool, Buffer)> done;
        };
        struct Owned
        {
            uint64_t fence = 0;
            uint64_t revision = 0;
            LeaseTimePoint expiry{};
            Buffer bytes;
            bool renewing = false;
        };
        // Replies of one quorum operation; callers wake as soon as its outcome is decided.
        struct Round
        {
            std::mutex mutex;
            std::condition_variable ready;
            std::vector<Buffer> replies;
            size_t failures = 0;
            size_t total = 0;
            std::chrono::steady_clock::time_point deadline{};
        };
        struct Usage
        {
            size_t bytes = 0;
            size_t records = 0;
        };
        using Placement = std::shared_ptr<const std::vector<std::string>>;

        ClusterNode& owner;
        ClusterOptions options;
        Fabric fabric;
        FileStore store;
        IoContext io;
        asio::steady_timer timer;
        std::map<std::string, ClusterPeer> members;
        std::map<std::string, std::shared_ptr<Link>> links;
        std::set<std::shared_ptr<Link>> connections;
        std::map<std::string, LeaseTimePoint> dialing;
        std::map<uint64_t, Pending> pending;
        std::map<std::string, Lease> leases;
        std::map<std::string, Owned> owned;
        std::map<std::string, std::set<uint32_t>> profileShards;
        std::unordered_map<std::string, Usage> inboxes;
        std::mutex placementMutex;
        std::unordered_map<std::string, Placement> placements;
        std::string incarnation;
        Digest manifest{};
        LeaseTimePoint bootGuard{};
        uint64_t counter = 0;
        uint64_t profileVersion = 0;
        bool started = false;
        bool stopping = false;
        uint64_t nextSweep = 0;

        Impl(ClusterNode& node, ClusterOptions settings): owner(node), options(std::move(settings)),
            fabric(options.fabric), store(options.directory, options.maxBytes, options.maxRecords, true), timer(io.Executor())
        {
            if (options.identifier.empty() || options.identifier.size() > 100 || options.epochKey.size() < 32 || options.leaseMilliseconds < 300
                || !options.timeoutMilliseconds || options.timeoutMilliseconds >= options.leaseMilliseconds / 2
                || !options.maxOperations || !options.transport.mutual || (!options.transport.tls && !options.transport.quic))
                throw std::invalid_argument("Cluster needs mutual TLS/QUIC, epoch credentials and valid lease/resource bounds");
            options.transport.backplane = true;
            options.transport.quicPolicy = QuicPolicy{MMQP_QUIC_STRATEGY_BULK, BulkPolicy{4, 65536}, 1200};
            if (options.transport.maxPacket < 1024 * 1024) throw std::invalid_argument("Cluster packet budget is too small");
            std::set<std::string> certificates;
            for (auto peer : options.peers)
            {
                if (!peer.port || peer.host.empty() || FromHex(peer.fingerprint).size() != 32
                    || !certificates.insert(peer.fingerprint).second || !members.emplace(peer.identifier, peer).second)
                    throw std::invalid_argument("Cluster identities require distinct pinned certificates and valid endpoints");
            }
            if (members.size() != fabric.Options().brokers.size() || !members.contains(options.identifier))
                throw std::invalid_argument("Cluster endpoints must describe the complete configured membership");
            Writer writer(65536);
            writer.U32(4); // Persistent backplane value schema, with request IDs selected by PRT.
            writer.U64(fabric.Options().epoch);
            writer.U32(fabric.Options().shards);
            writer.U16(fabric.Options().replicas);
            writer.U16(fabric.Options().fanout);
            for (const auto& broker : fabric.Options().brokers)
            {
                if (!members.contains(broker)) throw std::invalid_argument("Missing cluster member endpoint");
                writer.String(broker);
                writer.String(members.at(broker).fingerprint);
            }
            manifest = Sha256(writer.Data());
            if (auto previous = store.Get("manifest"))
            {
                if (!SecureEqual(*previous, manifest)) throw std::invalid_argument("State directory belongs to another schema/membership/epoch; migrate it explicitly");
                // A restarted voter cannot remember a monotonic lease deadline.
                bootGuard = LeaseClock::now() + std::chrono::milliseconds(options.leaseMilliseconds);
            }
            store.Put("manifest", Buffer(manifest.begin(), manifest.end()));
            incarnation = options.identifier + ':' + ToHex(RandomBytes(16));
            for (const auto& key : store.Keys("lease/"))
            {
                auto bytes = store.Find(key);
                Reader reader(*bytes);
                Lease lease;
                lease.fence = reader.U64();
                lease.value = Value::Load(reader);
                reader.Finish();
                leases.emplace(key.substr(6), std::move(lease));
            }
            if (!options.maxInboxBytes)
                options.maxInboxBytes = std::min(options.maxBytes, std::max<size_t>(options.maxBytes / 16, options.transport.maxPacket));
            if (!options.maxInboxRecords) options.maxInboxRecords = std::max<size_t>(options.maxRecords / 16, 1);
            for (const auto& key : store.Keys("in/"))
                if (auto bytes = store.Find(key); bytes->size() != EMPY_CLUSTER_TOMBSTONE_SIZE) this->Track(key, bytes->size(), true);
        }

        ~Impl() { SecureErase(options.epochKey); }

        size_t Majority() const { return fabric.Options().replicas / 2 + 1; }

        // Charges or releases a live inbox record against its session budget.
        void Track(std::string_view key, size_t bytes, bool add)
        {
            auto session = InboxSession(key);
            auto& usage = inboxes[session];
            if (add) { usage.bytes += bytes; ++usage.records; }
            else
            {
                usage.bytes -= std::min(usage.bytes, bytes);
                if (usage.records) --usage.records;
            }
            if (!usage.records) inboxes.erase(session);
        }

        // Session placement hashes every broker; a bounded cache avoids repeating it per operation.
        Placement SessionReplicas(const std::string& key)
        {
            {
                std::lock_guard lock(placementMutex);
                if (auto found = placements.find(key); found != placements.end()) return found->second;
            }
            auto replicas = std::make_shared<const std::vector<std::string>>(fabric.SessionReplicas(key));
            std::lock_guard lock(placementMutex);
            if (placements.size() >= EMPY_CLUSTER_PLACEMENT_CACHE) placements.erase(placements.begin());
            placements.emplace(key, replicas);
            return replicas;
        }

        void Persist(const std::string& key, const Lease& lease)
        {
            Writer writer(options.maxBytes);
            writer.U64(lease.fence);
            lease.value.Save(writer);
            store.Put("lease/" + key, writer.Take());
        }

        Buffer Packet(uint8_t kind, uint64_t id, std::span<const uint8_t> body, bool data = false, uint8_t qos = 1)
        {
            Writer writer(options.transport.maxPacket);
            writer.U32(0x4D484632);
            writer.U64(fabric.Options().epoch);
            writer.U8(kind);
            writer.U64(id);
            writer.String(options.identifier);
            WriteBuffer(writer, body);
            auto tag = Hmac256(options.epochKey, writer.Data());
            writer.Bytes(tag);
            PublishRequest publication;
            publication.topic = data ? "$mhf/data" : "$mhf/control";
            publication.alias = 1;
            publication.qos = qos;
            publication.packetId = qos ? 1 : 0;
            publication.payload = writer.Take();
            return Codec::Encode(publication, {false, options.transport.maxPacket});
        }

        void Bind(Connection::Pointer connection, std::string expected = {})
        {
            auto link = std::make_shared<Link>();
            link->connection = std::move(connection);
            link->expected = std::move(expected);
            link->outgoing = !link->expected.empty();
            connections.insert(link);
            if (link->connection->Quic()) link->connection->Configure(options.transport.quicPolicy, false);
            std::weak_ptr<Link> weak = link;
            link->connection->onReceive = [this, weak] (auto bytes)
            {
                if (auto link = weak.lock())
                {
                    try { this->Receive(link, bytes); }
                    catch (const std::exception& error) { EMPY_WARN("Backplane record rejected: {}", error.what()); link->connection->Close(false); }
                }
            };
            link->connection->onClose = [this, weak] (const auto&)
            {
                auto link = weak.lock();
                if (!link) return;
                if (auto found = links.find(link->peer); found != links.end() && found->second == link) links.erase(found);
                connections.erase(link);
                for (auto& [id, request] : pending) if (request.peer == link->peer) request.sent = false;
            };
            if (link->outgoing) link->connection->Send(this->Packet(0, 0, manifest));
        }

        void Dial(const std::string& peer)
        {
            if (peer == options.identifier || links.contains(peer) || stopping) return;
            auto now = LeaseClock::now();
            if (dialing.contains(peer) && dialing.at(peer) > now) return;
            dialing[peer] = now + std::chrono::milliseconds(500);
            auto endpoint = members.at(peer);
            auto transport = options.transport;
            transport.serverName = endpoint.serverName;
            try
            {
                io.Connect(endpoint.host, endpoint.port, transport,
                    [this, peer] (auto connection) { this->Bind(std::move(connection), peer); },
                    [] ([[maybe_unused]] const auto& error) { EMPY_DEBUG("Backplane connection closed: {}", error); });
            }
            catch (const std::exception& error) { EMPY_WARN("Backplane connect failed: {}", error.what()); }
        }

        void Receive(const std::shared_ptr<Link>& link, std::span<const uint8_t> frame)
        {
            auto publication = std::get<PublishRequest>(Codec::Decode(frame, {false, options.transport.maxPacket}));
            auto& bytes = publication.payload;
            if (bytes.size() < 32) throw ProtocolError("Short backplane record");
            auto body = std::span<const uint8_t>(bytes).first(bytes.size() - 32);
            if (!SecureEqual(Hmac256(options.epochKey, body), std::span<const uint8_t>(bytes).last(32)))
                throw ProtocolError("Backplane MAC failed");
            Reader reader(body);
            if (reader.U32() != 0x4D484632 || reader.U64() != fabric.Options().epoch) throw ProtocolError("Backplane version/epoch mismatch");
            auto kind = reader.U8();
            auto id = reader.U64();
            auto peer = reader.String();
            auto payload = ReadBuffer(reader);
            reader.Finish();
            if (!members.contains(peer) || peer == options.identifier || (!link->expected.empty() && link->expected != peer)
                || link->connection->PeerFingerprint() != members.at(peer).fingerprint)
                throw ProtocolError("Backplane certificate does not bind the claimed broker");
            if (!link->ready)
            {
                if (kind != 0 || !SecureEqual(payload, manifest)) throw ProtocolError("Backplane membership handshake failed");
                link->peer = peer;
                link->ready = true;
                auto old = links.find(peer);
                bool preferred = link->outgoing == (options.identifier < peer);
                if (old != links.end() && old->second != link)
                {
                    if (!preferred) { link->connection->Close(false); return; }
                    auto previous = old->second;
                    links.erase(old);
                    previous->connection->Close(false);
                }
                links[peer] = link;
                if (!link->outgoing) link->connection->Send(this->Packet(0, 0, manifest));
                this->Flush(peer);
                return;
            }
            if (peer != link->peer) throw ProtocolError("Backplane identity changed");
            if (kind == 2 || kind == 3)
            {
                auto found = pending.find(id);
                if (found == pending.end() || found->second.peer != peer) return;
                auto done = std::move(found->second.done);
                pending.erase(found);
                done(kind == 2, std::move(payload));
            }
            else if (kind == 1)
            {
                try { link->connection->Send(this->Packet(2, id, this->Handle(peer, payload))); }
                catch (const std::exception& error)
                {
                    EMPY_DEBUG("Backplane operation rejected: {}", error.what());
                    link->connection->Send(this->Packet(3, id, {}));
                }
            }
            else if (kind == 4) this->Forward(peer, std::move(payload));
            else throw ProtocolError("Unknown backplane record kind");
        }

        void Forward(const std::string& peer, Buffer bytes)
        {
            Reader reader(bytes);
            auto length = reader.U32();
            auto envelope = reader.Bytes(length);
            // Decode verifies the MAC before parsing and binds the identity to the payload.
            auto record = FabricCodec::Decode(envelope, options.epochKey, options.transport.maxPacket, fabric.Options().maxDestinations);
            auto tree = FabricCodec::Validate(record, fabric, peer, options.identifier, UnixMilliseconds());
            auto rank = tree.Rank(options.identifier);
            auto count = reader.U32();
            if (count > options.maxRecords) throw ProtocolError("Forward target budget exceeded");
            std::unordered_set<std::string_view> destinations(record.destinations.begin(), record.destinations.end());
            Targets targets;
            targets.reserve(count);
            for (uint32_t i = 0; i < count; ++i)
            {
                auto broker = reader.String();
                auto session = reader.String();
                if (!destinations.contains(broker)) throw ProtocolError("Forward target is absent from destination tree");
                targets.emplace_back(std::move(broker), std::move(session));
            }
            reader.Finish();
            this->Deliver(record, targets);
            auto children = tree.Children(rank);
            if (children.empty()) return; // Leaves neither re-encode nor re-authenticate the record.
            FabricCodec::Rebind(std::span<uint8_t>(bytes).subspan(4, length), record, static_cast<uint32_t>(rank), options.epochKey);
            this->Send(tree, children, bytes, record.publication.qos);
        }

        void Deliver(const ForwardRecord& record, const Targets& targets)
        {
            auto now = LeaseClock::now();
            for (const auto& [broker, session] : targets)
            {
                if (broker != options.identifier) continue;
                auto found = owned.find(session);
                if (found == owned.end() || found->second.expiry <= now) continue;
                ClusterDelivery delivery{record.identity, record.session, session, record.publication, record.expires};
                if (!record.publication.qos && owner.onTransient) owner.onTransient(std::move(delivery));
                else if (owner.onDelivery) owner.onDelivery(std::move(delivery));
            }
        }

        void Send(const DisseminationTree& tree, const std::vector<size_t>& children, std::span<const uint8_t> bytes, uint8_t qos)
        {
            auto packet = this->Packet(4, 0, bytes, true, qos);
            for (auto child : children)
            {
                const auto& destination = tree.brokers[child];
                this->Dial(destination);
                if (auto link = links.find(destination); link != links.end()) link->second->connection->Send(packet);
                // Reliable destinations recover from their replicated inbox if this branch is unavailable.
            }
        }

        void Flush(const std::string& peer)
        {
            if (!links.contains(peer)) { this->Dial(peer); return; }
            for (auto& [id, request] : pending)
                if (request.peer == peer && !request.sent) request.sent = links.at(peer)->connection->Send(request.packet);
        }

        void Call(const std::string& peer, Operation operation, std::span<const uint8_t> bytes, std::function<void(bool, Buffer)> done)
        {
            Writer writer(options.transport.maxPacket);
            writer.U8(static_cast<uint8_t>(operation));
            writer.Bytes(bytes);
            if (peer == options.identifier)
            {
                try { auto result = this->Handle(peer, writer.Data()); done(true, std::move(result)); }
                catch (...) { done(false, {}); }
                return;
            }
            if (stopping || pending.size() >= options.maxOperations || counter == UINT64_MAX) { done(false, {}); return; }
            auto id = ++counter;
            pending.emplace(id, Pending{peer, this->Packet(1, id, writer.Data(), operation == Operation::Inbox),
                LeaseClock::now() + std::chrono::milliseconds(options.timeoutMilliseconds), false, std::move(done)});
            this->Flush(peer);
        }

        // Starts one operation on every replica from a caller thread; Await collects the outcome.
        std::shared_ptr<Round> Begin(const std::vector<std::string>& replicas, Operation operation, Buffer bytes)
        {
            auto round = std::make_shared<Round>();
            round->total = replicas.size();
            round->deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(options.timeoutMilliseconds + 100);
            io.Post([this, replicas, operation, bytes = std::move(bytes), round]
            {
                for (const auto& replica : replicas)
                    this->Call(replica, operation, bytes, [round] (bool success, Buffer result)
                    {
                        {
                            std::lock_guard lock(round->mutex);
                            if (success) round->replies.push_back(std::move(result));
                            else ++round->failures;
                        }
                        round->ready.notify_all();
                    });
            });
            return round;
        }

        std::vector<Buffer> Await(const std::shared_ptr<Round>& round)
        {
            auto majority = this->Majority();
            std::unique_lock lock(round->mutex);
            round->ready.wait_until(lock, round->deadline, [&]
            { return round->replies.size() >= majority || round->total - round->failures < majority; });
            if (round->replies.size() < majority) throw ProtocolError("Cluster majority unavailable", MMQP_REASON_BUSY);
            return std::move(round->replies);
        }

        std::vector<Buffer> Quorum(const std::vector<std::string>& replicas, Operation operation, Buffer bytes)
        {
            return this->Await(this->Begin(replicas, operation, std::move(bytes)));
        }

        // Issues independent quorum writes concurrently, bounded by the pending-operation budget.
        // Every request must reach its majority; the first failure is reported after issuing its window.
        void Replicate(std::vector<std::pair<std::vector<std::string>, Buffer>> requests, Operation operation)
        {
            auto window = std::max<size_t>(1, options.maxOperations / (2 * fabric.Options().replicas));
            for (size_t first = 0; first < requests.size(); first += window)
            {
                std::vector<std::shared_ptr<Round>> rounds;
                for (size_t i = first; i < std::min(requests.size(), first + window); ++i)
                    rounds.push_back(this->Begin(requests[i].first, operation, std::move(requests[i].second)));
                for (const auto& round : rounds) this->Await(round);
            }
        }

        bool Replica(const std::string& key)
        {
            auto replicas = this->SessionReplicas(key);
            return std::find(replicas->begin(), replicas->end(), options.identifier) != replicas->end();
        }

        Buffer Handle(const std::string& peer, std::span<const uint8_t> bytes)
        {
            Reader reader(bytes);
            auto operation = static_cast<Operation>(reader.U8());
            auto key = reader.String();
            Writer reply(options.transport.maxPacket);
            if (operation == Operation::Read || operation == Operation::Grant || operation == Operation::Write || operation == Operation::Release)
            {
                if (!this->Replica(key) || key.size() > 240) throw ProtocolError("Wrong session replica");
                if (!leases.contains(key) && leases.size() >= options.maxRecords / 2) throw ProtocolError("Lease table budget exhausted");
                auto& lease = leases[key];
                if (operation == Operation::Read)
                {
                    reader.Finish();
                    reply.U64(lease.fence);
                    lease.value.Save(reply);
                    return reply.Take();
                }
                auto fence = reader.U64();
                auto claimant = reader.String();
                if (!claimant.starts_with(peer + ':')) throw ProtocolError("Lease claimant identity mismatch");
                auto now = LeaseClock::now();
                if (now < bootGuard) throw ProtocolError("Restart lease quarantine is active");
                if (operation == Operation::Grant)
                {
                    reader.Finish();
                    if (!fence || fence < lease.fence || (lease.expiry > now && lease.owner != claimant)
                        || (fence == lease.fence && !lease.owner.empty() && lease.owner != claimant)) throw ProtocolError("Lease is fenced");
                    if (fence != lease.fence) { lease.fence = fence; this->Persist(key, lease); }
                    lease.owner = claimant;
                    lease.expiry = now + std::chrono::milliseconds(options.leaseMilliseconds);
                    lease.value.Save(reply);
                    return reply.Take();
                }
                if (lease.fence != fence || lease.owner != claimant || lease.expiry <= now) throw ProtocolError("Lease is no longer valid");
                if (operation == Operation::Release)
                {
                    reader.Finish();
                    lease.owner.clear();
                    lease.expiry = now;
                    return {};
                }
                auto value = Value::Load(reader);
                reader.Finish();
                if (value.expiry <= UnixMilliseconds() || value.fence != fence || !value.revision || value.Stamp() < lease.value.Stamp()
                    || (value.Stamp() == lease.value.Stamp() && value.bytes != lease.value.bytes)) throw ProtocolError("Conflicting snapshot revision");
                auto updated = lease;
                updated.value = std::move(value);
                this->Persist(key, updated);
                lease = std::move(updated);
                return {};
            }
            if (operation == Operation::Scan)
            {
                // A page resumes after the last key of the previous page and stays within the packet budget.
                auto after = reader.String();
                reader.Finish();
                if (!key.starts_with("profile/") && !key.starts_with("in/") && !key.starts_with("ret/")) throw ProtocolError("Invalid cluster scan");
                if (!after.empty() && !after.starts_with(key)) throw ProtocolError("Invalid cluster scan resume key");
                auto keys = store.Keys(key);
                auto item = after.empty() ? keys.begin() : std::upper_bound(keys.begin(), keys.end(), after);
                auto budget = options.transport.maxPacket - EMPY_CLUSTER_PACKET_RESERVE;
                size_t size = 4 + 1;
                std::vector<std::pair<std::string, Buffer>> page;
                for (; item != keys.end(); ++item)
                {
                    auto value = store.Get(*item);
                    auto length = 1 + item->size() + 4 + value->size();
                    if (!page.empty() && (size + length > budget || page.size() >= options.maxRecords)) break;
                    size += length;
                    page.emplace_back(*item, std::move(*value));
                }
                reply.U32(static_cast<uint32_t>(page.size()));
                for (const auto& [name, value] : page) { reply.String(name); WriteBuffer(reply, value); }
                reply.U8(item != keys.end());
                return reply.Take();
            }
            if (operation == Operation::Put)
            {
                if (!key.starts_with("profile/") && !key.starts_with("ret/")) throw ProtocolError("Invalid replica record kind");
                auto value = Value::Load(reader);
                reader.Finish();
                if (value.expiry <= UnixMilliseconds()) return {}; // Delayed expired index updates cannot resurrect compacted state.
                if (auto previous = store.Find(key))
                {
                    Reader input(*previous);
                    auto old = Value::Load(input);
                    if (old.Stamp() > value.Stamp()) return {};
                    if (old.Stamp() == value.Stamp())
                    {
                        if (old.bytes != value.bytes) throw ProtocolError("Conflicting replica record");
                        return {};
                    }
                }
                value.Save(reply);
                store.Put(key, reply.Take());
                return {};
            }
            if (operation == Operation::Inbox)
            {
                auto value = ReadBuffer(reader);
                reader.Finish();
                auto delivery = DecodeDelivery(value);
                if (delivery.expiry <= UnixMilliseconds() || key != InboxPrefix(delivery.session) + ToHex(delivery.identity)
                    || !this->Replica(delivery.session)) throw ProtocolError("Invalid inbox destination or expiry");
                if (auto previous = store.Find(key))
                {
                    if (previous->size() == 8) return {}; // Durable consumed tombstone.
                    auto old = DecodeDelivery(*previous);
                    if (old.sender != delivery.sender || old.session != delivery.session
                        || Codec::Encode(old.publication, {true, UINT32_MAX}) != Codec::Encode(delivery.publication, {true, UINT32_MAX}))
                        throw ProtocolError("Inbox identity conflict");
                }
                else
                {
                    // Q_bytes + b(m) <= B and Q_records + 1 <= N, in addition to the global store budget.
                    auto found = inboxes.find(delivery.session);
                    auto usage = found == inboxes.end() ? Usage{} : found->second;
                    if (usage.records >= options.maxInboxRecords || usage.bytes > options.maxInboxBytes
                        || value.size() > options.maxInboxBytes - usage.bytes)
                        throw ProtocolError("Session inbox budget exhausted", MMQP_REASON_BUSY);
                    auto size = value.size();
                    store.Put(key, std::move(value));
                    this->Track(key, size, true);
                }
                if (owned.contains(delivery.session) && owned.at(delivery.session).expiry > LeaseClock::now() && owner.onDelivery)
                    owner.onDelivery(std::move(delivery));
                return {};
            }
            if (operation == Operation::Consume)
            {
                auto session = reader.String();
                auto fence = reader.U64();
                auto claimant = reader.String();
                auto expiry = reader.U64();
                reader.Finish();
                if (!this->Replica(session) || !key.starts_with(InboxPrefix(session)) || key.size() != InboxPrefix(session).size() + 64)
                    throw ProtocolError("Wrong session replica");
                auto now = LeaseClock::now();
                if (now < bootGuard) throw ProtocolError("Restart lease quarantine is active");
                auto lease = leases.find(session);
                if (lease == leases.end() || lease->second.fence != fence || lease->second.owner != claimant
                    || lease->second.expiry <= now || !claimant.starts_with(peer + ':'))
                    throw ProtocolError("Consumed inbox record is not owned");
                auto previous = store.Find(key);
                auto size = previous ? previous->size() : EMPY_CLUSTER_TOMBSTONE_SIZE;
                reply.U64(expiry);
                store.Put(key, reply.Take());
                if (size != EMPY_CLUSTER_TOMBSTONE_SIZE) this->Track(key, size, false);
                return {};
            }
            throw ProtocolError("Unknown cluster operation");
        }

        void Tick()
        {
            if (stopping) return;
            timer.expires_after(std::chrono::milliseconds(25));
            timer.async_wait([this] (auto error) { if (!error) this->Tick(); });
            auto now = LeaseClock::now();
            std::vector<std::function<void()>> completed;
            std::set<std::string> retry;
            for (auto it = pending.begin(); it != pending.end();)
            {
                if (it->second.deadline <= now)
                {
                    completed.push_back([done = std::move(it->second.done)] { done(false, {}); });
                    it = pending.erase(it);
                }
                else { retry.insert(it->second.peer); ++it; }
            }
            for (auto& done : completed) done();
            for (const auto& peer : retry) this->Flush(peer);
            auto wall = UnixMilliseconds();
            if (wall >= nextSweep)
            {
                nextSweep = wall + 1000;
                for (const auto& key : store.Keys("in/"))
                {
                    auto bytes = store.Find(key);
                    if (InboxExpiry(*bytes) > wall) continue;
                    auto size = bytes->size();
                    store.Erase(key);
                    if (size != EMPY_CLUSTER_TOMBSTONE_SIZE) this->Track(key, size, false);
                }
                for (auto it = leases.begin(); it != leases.end();)
                {
                    bool active = it->second.expiry > now || (owned.contains(it->first) && owned.at(it->first).expiry > now);
                    if (!active && it->second.value.expiry < wall && wall - it->second.value.expiry > options.leaseMilliseconds * 2ULL)
                    {
                        store.Erase("lease/" + it->first);
                        owned.erase(it->first);
                        it = leases.erase(it);
                    }
                    else ++it;
                }
                for (const auto& prefix : {"profile/", "ret/"})
                    for (const auto& key : store.Keys(prefix))
                    {
                        // Values start with fence, revision and expiry; the snapshot itself is not copied.
                        auto bytes = store.Find(key);
                        Reader reader(*bytes);
                        reader.U64();
                        reader.U64();
                        auto expiry = reader.U64();
                        if (expiry < wall && wall - expiry > options.leaseMilliseconds * 2ULL) store.Erase(key);
                    }
            }
            for (auto& [key, lease] : owned)
            {
                if (lease.renewing || lease.expiry <= now || lease.expiry > now + std::chrono::milliseconds(options.leaseMilliseconds / 6)) continue;
                lease.renewing = true;
                auto fence = lease.fence;
                auto deadline = now + std::chrono::milliseconds(options.leaseMilliseconds / 3);
                Writer writer;
                writer.String(key);
                writer.U64(fence);
                writer.String(incarnation);
                auto acknowledgements = std::make_shared<size_t>(0);
                auto remaining = std::make_shared<size_t>(fabric.Options().replicas);
                for (const auto& replica : *this->SessionReplicas(key))
                    this->Call(replica, Operation::Grant, writer.Data(), [this, key, fence, deadline, acknowledgements, remaining] (bool success, Buffer)
                    {
                        if (!owned.contains(key) || owned.at(key).fence != fence) return;
                        auto& value = owned.at(key);
                        if (success && ++*acknowledgements >= this->Majority() && value.expiry > LeaseClock::now()) value.expiry = deadline;
                        if (--*remaining == 0) value.renewing = false;
                    });
            }
        }

        // Each page is a quorum read. Every reply covers its keys up to its last key, so keys up to
        // the smallest truncated boundary are covered by a full majority before the next page.
        std::map<std::string, Buffer> Scan(const std::vector<std::string>& replicas, const std::string& prefix, bool versioned)
        {
            std::map<std::string, Buffer> records;
            std::string after;
            while (true)
            {
                Writer request;
                request.String(prefix);
                request.String(after);
                auto replies = this->Quorum(replicas, Operation::Scan, request.Take());
                std::vector<std::vector<std::pair<std::string, Buffer>>> pages;
                std::optional<std::string> boundary;
                for (const auto& bytes : replies)
                {
                    Reader reader(bytes);
                    auto count = reader.U32();
                    if (count > options.maxRecords) throw ProtocolError("Cluster scan budget exceeded");
                    auto& page = pages.emplace_back();
                    page.reserve(count);
                    for (uint32_t i = 0; i < count; ++i)
                    {
                        auto key = reader.String();
                        if (!key.starts_with(prefix) || key <= (page.empty() ? after : page.back().first))
                            throw ProtocolError("Cluster scan prefix or order mismatch");
                        page.emplace_back(std::move(key), ReadBuffer(reader));
                    }
                    auto more = reader.U8() != 0;
                    reader.Finish();
                    if (!more) continue;
                    if (page.empty()) throw ProtocolError("Cluster scan page made no progress");
                    if (!boundary || page.back().first < *boundary) boundary = page.back().first;
                }
                for (auto& page : pages)
                    for (auto& [key, value] : page)
                    {
                        if (boundary && key > *boundary) break;
                        this->Merge(records, std::move(key), std::move(value), versioned);
                    }
                if (records.size() > options.maxRecords) throw ProtocolError("Cluster scan budget exceeded");
                if (!boundary) return records;
                after = std::move(*boundary);
            }
        }

        void Merge(std::map<std::string, Buffer>& records, std::string key, Buffer value, bool versioned)
        {
            auto found = records.find(key);
            if (found == records.end()) records.emplace(std::move(key), std::move(value));
            else if (versioned)
            {
                Reader before(found->second), after(value);
                auto old = Value::Load(before), incoming = Value::Load(after);
                if (incoming.Stamp() > old.Stamp()) found->second = std::move(value);
                else if (incoming.Stamp() == old.Stamp() && incoming.bytes != old.bytes) throw ProtocolError("Replica equivocation");
            }
            else if (value.size() == EMPY_CLUSTER_TOMBSTONE_SIZE) found->second = std::move(value);
            else if (found->second.size() != EMPY_CLUSTER_TOMBSTONE_SIZE && found->second != value) throw ProtocolError("Inbox replica conflict");
        }
    };

    ClusterNode::ClusterNode(ClusterOptions options): m_Impl(std::make_unique<Impl>(*this, std::move(options))) {}
    ClusterNode::~ClusterNode() { this->Stop(); }

    uint16_t ClusterNode::Start()
    {
        if (m_Impl->started || m_Impl->stopping) throw std::logic_error("Cluster node already started or stopped");
        auto port = m_Impl->io.Listen(m_Impl->options.address, m_Impl->options.port, m_Impl->options.transport,
            [this] (auto connection) { m_Impl->Bind(std::move(connection)); }, m_Impl->members.size() * 4);
        m_Impl->started = true;
        m_Impl->io.Post([this] { m_Impl->Tick(); });
        m_Impl->io.Init();
        return port;
    }

    void ClusterNode::Stop()
    {
        if (!m_Impl || m_Impl->stopping) return;
        if (m_Impl->started) m_Impl->io.Submit([this]
        {
            m_Impl->stopping = true;
            m_Impl->timer.cancel();
            auto pending = std::move(m_Impl->pending);
            for (auto& [id, request] : pending) request.done(false, {});
        }).get();
        else m_Impl->stopping = true;
        m_Impl->io.Stop();
    }

    size_t ClusterNode::Connected() { return m_Impl->io.Submit([this] { return m_Impl->links.size(); }).get(); }

    ClusterSnapshot ClusterNode::Inspect(const std::vector<std::string>& sessions)
    {
        auto collect = [this, &sessions]
        {
            ClusterSnapshot result;
            result.identifier = ToHex(m_Impl->manifest);
            const auto& options = m_Impl->options;
            auto now = LeaseClock::now();
            auto remaining = [now] (auto deadline) -> uint64_t
            {
                return deadline > now ? std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count() : 0;
            };
            result.epoch = options.fabric.epoch;
            result.shards = options.fabric.shards;
            result.replicas = options.fabric.replicas;
            result.fanout = options.fabric.fanout;
            result.pendingOperations = m_Impl->pending.size();
            result.storedBytes = m_Impl->store.Bytes();
            result.quarantineMs = remaining(m_Impl->bootGuard);
            for (const auto& [key, lease] : m_Impl->owned)
                if (lease.expiry > now) ++result.activeLeases;
            for (const auto& id : sessions)
                if (auto found = m_Impl->owned.find(id); found != m_Impl->owned.end() && found->second.expiry > now)
                    result.ownership.emplace(id, LeaseSnapshot{found->second.fence, remaining(found->second.expiry)});
            for (const auto& [id, member] : m_Impl->members)
            {
                if (id == options.identifier) continue;
                auto link = m_Impl->links.find(id);
                bool connected = link != m_Impl->links.end() && link->second->ready;
                result.peers.push_back({id, member.host, member.port, connected,
                    connected ? link->second->connection->Queued() : 0});
            }
            return result;
        };
        return m_Impl->started && !m_Impl->stopping ? m_Impl->io.Submit(collect).get() : collect();
    }

    const Fabric& ClusterNode::Placement() const { return m_Impl->fabric; }
    const std::string& ClusterNode::Identifier() const { return m_Impl->options.identifier; }

    bool ClusterNode::Owns(std::string_view key)
    {
        return m_Impl->io.Submit([this, key = std::string(key)]
        { return m_Impl->owned.contains(key) && m_Impl->owned.at(key).expiry > LeaseClock::now(); }).get();
    }

    Buffer ClusterNode::Acquire(std::string key, uint32_t timeout)
    {
        if (this->Owns(key)) return m_Impl->io.Submit([this, key] { return m_Impl->owned.at(key).bytes; }).get();
        auto deadline = LeaseClock::now() + std::chrono::milliseconds(timeout);
        auto replicas = m_Impl->SessionReplicas(key);
        while (LeaseClock::now() < deadline)
        {
            try
            {
                Writer read;
                read.String(key);
                auto responses = m_Impl->Quorum(*replicas, Operation::Read, read.Take());
                uint64_t maximum = 0;
                for (const auto& bytes : responses) { Reader reader(bytes); maximum = std::max(maximum, reader.U64()); }
                auto count = m_Impl->members.size();
                auto rank = static_cast<size_t>(std::distance(m_Impl->members.begin(), m_Impl->members.find(m_Impl->options.identifier)));
                if (maximum > UINT64_MAX - 2 * count) throw std::overflow_error("Lease fence exhausted");
                auto fence = (maximum / count + 1) * count + rank;
                Writer claim;
                claim.String(key);
                claim.U64(fence);
                claim.String(m_Impl->incarnation);
                auto started = LeaseClock::now();
                auto votes = m_Impl->Quorum(*replicas, Operation::Grant, claim.Take());
                Value newest;
                for (const auto& bytes : votes)
                {
                    Reader reader(bytes);
                    auto value = Value::Load(reader);
                    reader.Finish();
                    if (value.Stamp() > newest.Stamp()) newest = std::move(value);
                    else if (value.Stamp() == newest.Stamp() && value.bytes != newest.bytes) throw ProtocolError("Snapshot replicas disagree");
                }
                auto expiry = started + std::chrono::milliseconds(m_Impl->options.leaseMilliseconds / 3);
                if (expiry <= LeaseClock::now()) continue;
                m_Impl->io.Submit([this, key, fence, expiry, bytes = newest.bytes]
                { m_Impl->owned[key] = Impl::Owned{fence, 0, expiry, bytes, false}; }).get();
                if (newest.expiry <= UnixMilliseconds()) newest.bytes.clear();
                auto retention = newest.bytes.empty() ? UnixMilliseconds() + m_Impl->options.leaseMilliseconds : newest.expiry;
                this->Commit(key, newest.bytes, retention); // Adopt an accepted successor before serving.
                return newest.bytes;
            }
            catch (const ProtocolError&) { std::this_thread::sleep_for(std::chrono::milliseconds(25)); }
        }
        throw ProtocolError("No majority could grant exclusive session ownership", MMQP_REASON_BUSY);
    }

    void ClusterNode::Commit(std::string key, Buffer snapshot, uint64_t expiry)
    {
        auto owned = m_Impl->io.Submit([this, key]
        {
            auto found = m_Impl->owned.find(key);
            if (found == m_Impl->owned.end() || found->second.expiry <= LeaseClock::now() || found->second.revision == UINT64_MAX)
                throw ProtocolError("Session primary is fenced", MMQP_REASON_BUSY);
            ++found->second.revision;
            return found->second;
        }).get();
        Writer writer(m_Impl->options.transport.maxPacket);
        writer.String(key);
        writer.U64(owned.fence);
        writer.String(m_Impl->incarnation);
        Value{owned.fence, owned.revision, snapshot, expiry}.Save(writer);
        try
        {
            m_Impl->Quorum(*m_Impl->SessionReplicas(key), Operation::Write, writer.Take());
            m_Impl->io.Submit([this, key, fence = owned.fence, bytes = std::move(snapshot)] () mutable
            {
                auto& lease = m_Impl->owned.at(key);
                if (lease.fence != fence || lease.expiry <= LeaseClock::now()) throw ProtocolError("Session lease expired during commit", MMQP_REASON_BUSY);
                lease.bytes = std::move(bytes);
            }).get();
        }
        catch (...)
        {
            m_Impl->io.Submit([this, key] { m_Impl->owned.erase(key); }).get();
            throw;
        }
    }

    Buffer ClusterNode::Read(std::string key)
    {
        Writer writer;
        writer.String(key);
        Value newest;
        for (const auto& bytes : m_Impl->Quorum(*m_Impl->SessionReplicas(key), Operation::Read, writer.Take()))
        {
            Reader reader(bytes);
            reader.U64();
            auto value = Value::Load(reader);
            reader.Finish();
            if (value.Stamp() > newest.Stamp()) newest = std::move(value);
        }
        return newest.expiry > UnixMilliseconds() ? newest.bytes : Buffer{};
    }

    void ClusterNode::Release(std::string key)
    {
        auto lease = m_Impl->io.Submit([this, key]
        {
            auto found = m_Impl->owned.find(key);
            auto result = found == m_Impl->owned.end() ? Impl::Owned{} : found->second;
            m_Impl->owned.erase(key);
            return result;
        }).get();
        if (!lease.fence) return;
        Writer writer;
        writer.String(key);
        writer.U64(lease.fence);
        writer.String(m_Impl->incarnation);
        try { m_Impl->Quorum(*m_Impl->SessionReplicas(key), Operation::Release, writer.Take()); }
        catch (const ProtocolError&) {} // Expiry still fences the previous owner.
    }

    void ClusterNode::Profiles(std::string session, const std::vector<ProfileEntry>& profiles, uint64_t expiry, bool online)
    {
        auto lease = m_Impl->io.Submit([this, session]
        {
            if (!m_Impl->owned.contains(session) || m_Impl->owned.at(session).expiry <= LeaseClock::now()) throw ProtocolError("Profile owner is fenced");
            return m_Impl->owned.at(session);
        }).get();
        // Placements are computed once per profile; shards sharing a profile set share one encoding.
        std::map<uint32_t, std::vector<size_t>> placement;
        for (size_t i = 0; i < profiles.size(); ++i)
            if (profiles[i].type == MMQP_PROREQ_TYPE_SUB)
                for (auto shard : m_Impl->fabric.ProfileShards(profiles[i].topic)) placement[shard].push_back(i);
        std::set<uint32_t> shards;
        for (const auto& [shard, members] : placement) shards.insert(shards.end(), shard);
        auto previous = m_Impl->io.Submit([this, session, shards]
        {
            auto old = m_Impl->profileShards[session];
            m_Impl->profileShards[session] = shards;
            return old;
        }).get();
        shards.insert(previous.begin(), previous.end());
        std::map<std::vector<size_t>, Buffer> snapshots;
        std::vector<std::pair<std::vector<std::string>, Buffer>> requests;
        requests.reserve(shards.size());
        for (auto shard : shards)
        {
            Writer body(m_Impl->options.transport.maxPacket);
            body.String(m_Impl->options.identifier);
            body.String(session);
            body.U64(expiry);
            body.U8(online);
            auto found = placement.find(shard);
            body.U8(found != placement.end());
            if (found != placement.end())
            {
                auto& snapshot = snapshots[found->second];
                if (snapshot.empty())
                {
                    std::vector<ProfileEntry> entries;
                    entries.reserve(found->second.size());
                    for (auto index : found->second) entries.push_back(profiles[index]);
                    snapshot = Codec::Encode(ProfileRequest{1, std::move(entries)}, {false, UINT32_MAX, UINT32_MAX});
                }
                WriteBuffer(body, snapshot);
            }
            Writer request(m_Impl->options.transport.maxPacket);
            request.String("profile/" + std::to_string(shard) + '/' + session);
            Value{lease.fence, lease.revision, body.Take(), expiry}.Save(request);
            requests.emplace_back(m_Impl->fabric.Replicas(shard), request.Take());
        }
        m_Impl->Replicate(std::move(requests), Operation::Put);
    }

    std::vector<ClusterProfile> ClusterNode::Resolve(std::string topic, uint8_t qos, bool responders)
    {
        topic = ParseTopic(topic).filter;
        auto shard = m_Impl->fabric.Shard(topic);
        auto records = m_Impl->Scan(m_Impl->fabric.Replicas(shard), "profile/" + std::to_string(shard) + '/', true);
        std::vector<ClusterProfile> result;
        for (const auto& [key, bytes] : records)
        {
            Reader record(bytes);
            auto value = Value::Load(record);
            Reader reader(value.bytes);
            auto broker = reader.String();
            auto session = reader.String();
            auto expiry = reader.U64();
            auto online = reader.U8() != 0;
            if (expiry <= UnixMilliseconds() || (!online && (!qos || responders)) || !reader.U8()) continue;
            auto profiles = std::get<ProfileRequest>(Codec::Decode(ReadBuffer(reader), {false, UINT32_MAX, UINT32_MAX}));
            reader.Finish();
            for (const auto& profile : profiles.entries)
                if (profile.type == MMQP_PROREQ_TYPE_SUB && qos <= profile.qos && (!responders || profile.request) && MatchTopic(profile.topic, topic))
                    result.push_back({broker, session, profile, expiry, online});
        }
        return result;
    }

    void ClusterNode::Publish(const std::string& sender, uint64_t sequence, PublishRequest publication,
        const std::vector<ClusterProfile>& targets, uint64_t expiry)
    {
        publication.alias = 1;
        publication.duplicate = false;
        auto identity = MessageIdentity(sender, sequence, publication.topic, m_Impl->fabric.Options().epoch, publication.payload);
        std::map<std::string, ClusterProfile> distinct;
        for (const auto& target : targets) distinct.emplace(target.session, target);
        if (publication.qos)
        {
            // All recipient inbox writes run concurrently; acceptance still needs every majority.
            auto encoded = Codec::Encode(publication, {true, UINT32_MAX});
            std::vector<std::pair<std::vector<std::string>, Buffer>> requests;
            requests.reserve(distinct.size());
            for (const auto& [session, target] : distinct)
            {
                Writer request(m_Impl->options.transport.maxPacket);
                request.String(InboxPrefix(session) + ToHex(identity));
                WriteBuffer(request, EncodeDelivery(identity, sender, session, expiry, encoded));
                requests.emplace_back(*m_Impl->SessionReplicas(session), request.Take());
            }
            m_Impl->Replicate(std::move(requests), Operation::Inbox);
        }
        ForwardRecord record;
        record.identity = identity;
        record.acceptedEpoch = record.routingEpoch = m_Impl->fabric.Options().epoch;
        record.sequence = sequence;
        record.expires = expiry;
        record.session = sender;
        record.ingress = m_Impl->options.identifier;
        record.publication = std::move(publication);
        std::set<std::string> brokers;
        Targets forwarded;
        for (const auto& [session, target] : distinct)
            if (!target.broker.empty()) { brokers.insert(target.broker); forwarded.emplace_back(target.broker, session); }
        if (brokers.empty()) return;
        record.destinations.assign(brokers.begin(), brokers.end());
        // The ingress already holds the record: it is encoded once, and only when it has children.
        auto tree = m_Impl->fabric.Tree(record.publication.topic, record.ingress, record.destinations);
        auto children = tree.Children(0);
        Buffer bytes;
        if (!children.empty())
        {
            Writer writer(m_Impl->options.transport.maxPacket);
            WriteBuffer(writer, FabricCodec::Encode(record, m_Impl->options.epochKey, m_Impl->options.transport.maxPacket));
            writer.U32(static_cast<uint32_t>(forwarded.size()));
            for (const auto& [broker, session] : forwarded) { writer.String(broker); writer.String(session); }
            bytes = writer.Take();
        }
        m_Impl->io.Submit([this, &record, &forwarded, &tree, &children, &bytes]
        {
            m_Impl->Deliver(record, forwarded);
            if (!children.empty()) m_Impl->Send(tree, children, bytes, record.publication.qos);
        }).get();
    }

    std::vector<ClusterDelivery> ClusterNode::Inbox(std::string session)
    {
        auto records = m_Impl->Scan(*m_Impl->SessionReplicas(session), InboxPrefix(session), false);
        std::vector<ClusterDelivery> result;
        for (const auto& [key, bytes] : records)
        {
            if (bytes.size() == 8) continue;
            auto delivery = DecodeDelivery(bytes);
            if (delivery.expiry > UnixMilliseconds()) result.push_back(std::move(delivery));
        }
        std::sort(result.begin(), result.end(), [] (const auto& left, const auto& right)
        {
            return std::tie(left.sender, left.publication.packetId) < std::tie(right.sender, right.publication.packetId);
        });
        return result;
    }

    void ClusterNode::Acknowledge(const std::vector<ClusterDelivery>& deliveries)
    {
        std::vector<std::shared_ptr<Impl::Round>> rounds;
        for (const auto& delivery : deliveries)
        {
            try
            {
                auto lease = m_Impl->io.Submit([this, session = delivery.session]
                {
                    if (!m_Impl->owned.contains(session)) throw ProtocolError("Inbox owner is fenced");
                    return m_Impl->owned.at(session);
                }).get();
                Writer request;
                request.String(InboxPrefix(delivery.session) + ToHex(delivery.identity));
                request.String(delivery.session);
                request.U64(lease.fence);
                request.String(m_Impl->incarnation);
                request.U64(delivery.expiry);
                rounds.push_back(m_Impl->Begin(*m_Impl->SessionReplicas(delivery.session), Operation::Consume, request.Take()));
            }
            catch (const ProtocolError&) {}
            if (rounds.size() >= std::max<size_t>(1, m_Impl->options.maxOperations / (2 * m_Impl->fabric.Options().replicas)))
            {
                for (const auto& round : rounds) try { m_Impl->Await(round); } catch (const ProtocolError&) {}
                rounds.clear();
            }
        }
        for (const auto& round : rounds) try { m_Impl->Await(round); } catch (const ProtocolError&) {}
    }

    void ClusterNode::Acknowledge(const ClusterDelivery& delivery)
    {
        auto lease = m_Impl->io.Submit([this, session = delivery.session]
        {
            if (!m_Impl->owned.contains(session)) throw ProtocolError("Inbox owner is fenced");
            return m_Impl->owned.at(session);
        }).get();
        Writer request;
        request.String(InboxPrefix(delivery.session) + ToHex(delivery.identity));
        request.String(delivery.session);
        request.U64(lease.fence);
        request.String(m_Impl->incarnation);
        request.U64(delivery.expiry);
        m_Impl->Quorum(*m_Impl->SessionReplicas(delivery.session), Operation::Consume, request.Take());
    }

    void ClusterNode::Retain(const PublishRequest& publication, const ProfileEntry& profile, const Digest& identity)
    {
        auto shard = m_Impl->fabric.Shard(publication.topic);
        auto key = "ret/" + std::to_string(shard) + '/' + ToHex(Sha256({reinterpret_cast<const uint8_t*>(publication.topic.data()), publication.topic.size()}));
        auto lock = "retain:" + ToHex(Sha256({reinterpret_cast<const uint8_t*>(publication.topic.data()), publication.topic.size()}));
        auto previous = this->Acquire(lock);
        std::vector<ClusterDelivery> records;
        bool duplicate = false;
        if (!previous.empty())
        {
            Reader reader(previous);
            auto count = reader.U32();
            if (count > 255) throw ProtocolError("Retained queue exceeds profile limit");
            for (uint32_t i = 0; i < count; ++i)
            {
                auto item = DecodeDelivery(ReadBuffer(reader));
                if (item.identity == identity) duplicate = true;
                if (item.expiry > UnixMilliseconds()) records.push_back(std::move(item));
            }
            reader.Finish();
        }
        auto limit = profile.qos ? profile.queue : 1;
        if (!duplicate)
        {
            while (records.size() >= static_cast<size_t>(limit)) records.erase(records.begin());
            records.push_back({identity, {}, {}, publication, UnixMilliseconds() + DecodeInterval(profile.expiry) * 1000ULL});
        }
        Writer body(m_Impl->options.transport.maxPacket);
        body.U32(static_cast<uint32_t>(records.size()));
        for (const auto& item : records) WriteBuffer(body, EncodeDelivery(item));
        uint64_t expiry = 0;
        for (const auto& record : records) expiry = std::max(expiry, record.expiry);
        // The lock record and the shard index carry the same fenced revision, so both quorum
        // writes run concurrently; the release then proceeds without being awaited.
        auto lease = m_Impl->io.Submit([this, lock]
        {
            auto found = m_Impl->owned.find(lock);
            if (found == m_Impl->owned.end() || found->second.expiry <= LeaseClock::now() || found->second.revision == UINT64_MAX)
                throw ProtocolError("Retained queue lock is fenced", MMQP_REASON_BUSY);
            ++found->second.revision;
            return found->second;
        }).get();
        Value value{lease.fence, lease.revision, body.Take(), expiry};
        Writer commit(m_Impl->options.transport.maxPacket);
        commit.String(lock);
        commit.U64(lease.fence);
        commit.String(m_Impl->incarnation);
        value.Save(commit);
        Writer index(m_Impl->options.transport.maxPacket);
        index.String(key);
        value.Save(index);
        try
        {
            auto first = m_Impl->Begin(*m_Impl->SessionReplicas(lock), Operation::Write, commit.Take());
            auto second = m_Impl->Begin(m_Impl->fabric.Replicas(shard), Operation::Put, index.Take());
            m_Impl->Await(first);
            m_Impl->Await(second);
        }
        catch (...)
        {
            m_Impl->io.Submit([this, lock] { m_Impl->owned.erase(lock); }).get();
            throw;
        }
        m_Impl->io.Submit([this, lock] { m_Impl->owned.erase(lock); }).get();
        Writer release;
        release.String(lock);
        release.U64(lease.fence);
        release.String(m_Impl->incarnation);
        // Lease expiry still fences this owner if the release is lost.
        try { m_Impl->Begin(*m_Impl->SessionReplicas(lock), Operation::Release, release.Take()); }
        catch (const ProtocolError&) {}
    }

    std::vector<ClusterDelivery> ClusterNode::Retained(const std::vector<ProfileEntry>& profiles)
    {
        std::set<uint32_t> shards;
        for (const auto& profile : profiles)
            if (profile.type == MMQP_PROREQ_TYPE_SUB && profile.qos && !profile.request)
                for (auto shard : m_Impl->fabric.ProfileShards(profile.topic)) shards.insert(shard);
        std::set<Digest> seen;
        std::vector<ClusterDelivery> result;
        for (auto shard : shards)
        {
            auto records = m_Impl->Scan(m_Impl->fabric.Replicas(shard), "ret/" + std::to_string(shard) + '/', true);
            for (const auto& [key, bytes] : records)
            {
                Reader record(bytes);
                auto value = Value::Load(record);
                Reader reader(value.bytes);
                auto count = reader.U32();
                if (count > 255) throw ProtocolError("Retained queue exceeds profile limit");
                for (uint32_t i = 0; i < count; ++i)
                {
                    auto delivery = DecodeDelivery(ReadBuffer(reader));
                    if (delivery.expiry <= UnixMilliseconds()) continue;
                    for (const auto& profile : profiles)
                        if (profile.type == MMQP_PROREQ_TYPE_SUB && profile.qos && !profile.request && MatchTopic(profile.topic, delivery.publication.topic))
                        {
                            if (seen.insert(delivery.identity).second) result.push_back(std::move(delivery));
                            break;
                        }
                }
                reader.Finish();
            }
        }
        return result;
    }
}
