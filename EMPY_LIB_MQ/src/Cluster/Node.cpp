#include <eMQ/Cluster/Node.h>
#include <eMQ/Cluster/Clock.h>
#include <thread>

namespace eMQ
{
    namespace
    {
        enum class Operation : uint8_t { Read = 1, Grant, Write, Release, Put, Scan, Inbox, Consume };

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

        Buffer EncodeDelivery(const ClusterDelivery& delivery)
        {
            Writer writer(32 * 1024 * 1024);
            writer.Bytes(delivery.identity);
            writer.String(delivery.sender);
            writer.String(delivery.session);
            writer.U64(delivery.expiry);
            WriteBuffer(writer, Codec::Encode(delivery.publication, {true, UINT32_MAX}));
            return writer.Take();
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
        std::string incarnation;
        Digest manifest{};
        LeaseTimePoint bootGuard{};
        uint64_t counter = 0;
        uint64_t profileVersion = 0;
        bool started = false;
        bool stopping = false;
        uint64_t nextSweep = 0;

        Impl(ClusterNode& node, ClusterOptions settings): owner(node), options(std::move(settings)),
            fabric(options.fabric), store(options.directory, options.maxBytes, options.maxRecords), timer(io.Executor())
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
            writer.U32(3); // Persistent backplane value schema, including correlated RPC sessions.
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
                if (!SecureEqual(*previous, manifest)) throw std::invalid_argument("State directory belongs to another membership/epoch; migrate it explicitly");
                // A restarted voter cannot remember a monotonic lease deadline.
                bootGuard = LeaseClock::now() + std::chrono::milliseconds(options.leaseMilliseconds);
            }
            store.Put("manifest", Buffer(manifest.begin(), manifest.end()));
            incarnation = options.identifier + ':' + ToHex(RandomBytes(16));
            for (const auto& key : store.Keys("lease/"))
            {
                auto bytes = store.Get(key);
                Reader reader(*bytes);
                Lease lease;
                lease.fence = reader.U64();
                lease.value = Value::Load(reader);
                reader.Finish();
                leases.emplace(key.substr(6), std::move(lease));
            }
        }

        ~Impl() { SecureErase(options.epochKey); }

        size_t Majority() const { return fabric.Options().replicas / 2 + 1; }

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
            else if (kind == 4) this->Forward(peer, payload);
            else throw ProtocolError("Unknown backplane record kind");
        }

        void Forward(const std::string& peer, std::span<const uint8_t> bytes)
        {
            Reader reader(bytes);
            auto record = FabricCodec::Decode(ReadBuffer(reader), options.epochKey, options.transport.maxPacket, fabric.Options().maxDestinations);
            if (peer != options.identifier) FabricCodec::Validate(record, fabric, peer, options.identifier, UnixMilliseconds());
            auto tree = fabric.Tree(record.publication.topic, record.ingress, record.destinations);
            auto rank = tree.Rank(options.identifier);
            auto count = reader.U32();
            if (count > options.maxRecords) throw ProtocolError("Forward target budget exceeded");
            std::vector<std::pair<std::string, std::string>> targets;
            for (uint32_t i = 0; i < count; ++i)
            {
                auto broker = reader.String();
                auto session = reader.String();
                if (std::find(record.destinations.begin(), record.destinations.end(), broker) == record.destinations.end())
                    throw ProtocolError("Forward target is absent from destination tree");
                targets.emplace_back(broker, session);
                if (broker == options.identifier && owned.contains(session) && owned.at(session).expiry > LeaseClock::now())
                {
                    ClusterDelivery delivery{record.identity, record.session, session, record.publication, record.expires};
                    if (!record.publication.qos && owner.onTransient) owner.onTransient(std::move(delivery));
                    else if (owner.onDelivery) owner.onDelivery(std::move(delivery));
                }
            }
            reader.Finish();
            record.senderRank = static_cast<uint32_t>(rank);
            Writer writer(options.transport.maxPacket);
            WriteBuffer(writer, FabricCodec::Encode(record, options.epochKey, options.transport.maxPacket));
            writer.U32(static_cast<uint32_t>(targets.size()));
            for (const auto& [broker, session] : targets) { writer.String(broker); writer.String(session); }
            auto packet = this->Packet(4, 0, writer.Data(), true, record.publication.qos);
            for (auto child : tree.Children(rank))
            {
                const auto& destination = tree.brokers[child];
                this->Dial(destination);
                if (links.contains(destination)) links.at(destination)->connection->Send(packet);
                // Reliable destinations recover from their replicated inbox if this branch is unavailable.
            }
        }

        void Flush(const std::string& peer)
        {
            if (!links.contains(peer)) { this->Dial(peer); return; }
            for (auto& [id, request] : pending)
                if (request.peer == peer && !request.sent) request.sent = links.at(peer)->connection->Send(request.packet);
        }

        void Call(std::string peer, Operation operation, Buffer bytes, std::function<void(bool, Buffer)> done)
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

        std::future<Buffer> Call(std::string peer, Operation operation, Buffer bytes)
        {
            auto promise = std::make_shared<std::promise<Buffer>>();
            auto result = promise->get_future();
            io.Post([this, peer = std::move(peer), operation, bytes = std::move(bytes), promise] () mutable
            {
                this->Call(std::move(peer), operation, std::move(bytes), [promise] (bool success, Buffer result)
                {
                    if (success) promise->set_value(std::move(result));
                    else promise->set_exception(std::make_exception_ptr(ProtocolError("Cluster operation unavailable or rejected", MMQP_REASON_BUSY)));
                });
            });
            return result;
        }

        std::vector<Buffer> Quorum(const std::vector<std::string>& replicas, Operation operation, const Buffer& bytes)
        {
            std::vector<std::future<Buffer>> futures;
            for (const auto& replica : replicas) futures.push_back(this->Call(replica, operation, bytes));
            std::vector<Buffer> result;
            auto deadline = LeaseClock::now() + std::chrono::milliseconds(options.timeoutMilliseconds + 100);
            size_t remaining = futures.size();
            while (LeaseClock::now() < deadline)
            {
                for (auto& future : futures)
                {
                    if (!future.valid() || future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) continue;
                    --remaining;
                    try { result.push_back(future.get()); }
                    catch (...) {}
                }
                if (result.size() >= this->Majority()) return result;
                if (result.size() + remaining < this->Majority()) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            throw ProtocolError("Cluster majority unavailable", MMQP_REASON_BUSY);
        }

        bool Replica(const std::string& key) const
        {
            auto replicas = fabric.SessionReplicas(key);
            return std::find(replicas.begin(), replicas.end(), options.identifier) != replicas.end();
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
                reader.Finish();
                if (!key.starts_with("profile/") && !key.starts_with("in/") && !key.starts_with("ret/")) throw ProtocolError("Invalid cluster scan");
                auto keys = store.Keys(key);
                reply.U32(static_cast<uint32_t>(keys.size()));
                for (const auto& item : keys) { reply.String(item); WriteBuffer(reply, *store.Get(item)); }
                return reply.Take();
            }
            if (operation == Operation::Put)
            {
                if (!key.starts_with("profile/") && !key.starts_with("ret/")) throw ProtocolError("Invalid replica record kind");
                auto value = Value::Load(reader);
                reader.Finish();
                if (value.expiry <= UnixMilliseconds()) return {}; // Delayed expired index updates cannot resurrect compacted state.
                if (auto previous = store.Get(key))
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
                if (auto previous = store.Get(key))
                {
                    if (previous->size() == 8) return {}; // Durable consumed tombstone.
                    auto old = DecodeDelivery(*previous);
                    if (old.sender != delivery.sender || old.session != delivery.session
                        || Codec::Encode(old.publication, {true, UINT32_MAX}) != Codec::Encode(delivery.publication, {true, UINT32_MAX}))
                        throw ProtocolError("Inbox identity conflict");
                }
                else store.Put(key, value);
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
                auto lease = leases.find(session);
                if (!key.starts_with(InboxPrefix(session)) || lease == leases.end() || lease->second.fence != fence
                    || lease->second.owner != claimant || lease->second.expiry <= LeaseClock::now() || !claimant.starts_with(peer + ':'))
                    throw ProtocolError("Consumed inbox record is not owned");
                reply.U64(expiry);
                store.Put(key, reply.Take());
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
                    auto bytes = store.Get(key);
                    uint64_t expiry;
                    if (bytes->size() == 8) { Reader reader(*bytes); expiry = reader.U64(); }
                    else expiry = DecodeDelivery(*bytes).expiry;
                    if (expiry <= wall) store.Erase(key);
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
                        auto bytes = store.Get(key);
                        Reader reader(*bytes);
                        auto value = Value::Load(reader);
                        if (value.expiry < wall && wall - value.expiry > options.leaseMilliseconds * 2ULL) store.Erase(key);
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
                for (const auto& replica : fabric.SessionReplicas(key))
                    this->Call(replica, Operation::Grant, writer.Data(), [this, key, fence, deadline, acknowledgements, remaining] (bool success, Buffer)
                    {
                        if (!owned.contains(key) || owned.at(key).fence != fence) return;
                        auto& value = owned.at(key);
                        if (success && ++*acknowledgements >= this->Majority() && value.expiry > LeaseClock::now()) value.expiry = deadline;
                        if (--*remaining == 0) value.renewing = false;
                    });
            }
        }

        std::map<std::string, Buffer> Scan(const std::vector<std::string>& replicas, const std::string& prefix, bool versioned)
        {
            Writer request;
            request.String(prefix);
            auto replies = this->Quorum(replicas, Operation::Scan, request.Data());
            std::map<std::string, Buffer> records;
            for (const auto& bytes : replies)
            {
                Reader reader(bytes);
                auto count = reader.U32();
                if (count > options.maxRecords) throw ProtocolError("Cluster scan budget exceeded");
                for (uint32_t i = 0; i < count; ++i)
                {
                    auto key = reader.String();
                    auto value = ReadBuffer(reader);
                    if (!key.starts_with(prefix)) throw ProtocolError("Cluster scan prefix mismatch");
                    auto found = records.find(key);
                    if (found == records.end()) records.emplace(key, std::move(value));
                    else if (versioned)
                    {
                        Reader before(found->second), after(value);
                        auto old = Value::Load(before), incoming = Value::Load(after);
                        if (incoming.Stamp() > old.Stamp()) found->second = std::move(value);
                        else if (incoming.Stamp() == old.Stamp() && incoming.bytes != old.bytes) throw ProtocolError("Replica equivocation");
                    }
                    else if (value.size() == 8) found->second = std::move(value);
                    else if (found->second.size() != 8 && found->second != value) throw ProtocolError("Inbox replica conflict");
                }
                reader.Finish();
            }
            return records;
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
        auto replicas = m_Impl->fabric.SessionReplicas(key);
        while (LeaseClock::now() < deadline)
        {
            try
            {
                Writer read;
                read.String(key);
                auto responses = m_Impl->Quorum(replicas, Operation::Read, read.Data());
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
                auto votes = m_Impl->Quorum(replicas, Operation::Grant, claim.Data());
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
            m_Impl->Quorum(m_Impl->fabric.SessionReplicas(key), Operation::Write, writer.Data());
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
        for (const auto& bytes : m_Impl->Quorum(m_Impl->fabric.SessionReplicas(key), Operation::Read, writer.Data()))
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
        try { m_Impl->Quorum(m_Impl->fabric.SessionReplicas(key), Operation::Release, writer.Data()); }
        catch (const ProtocolError&) {} // Expiry still fences the previous owner.
    }

    void ClusterNode::Profiles(std::string session, const std::vector<ProfileEntry>& profiles, uint64_t expiry, bool online)
    {
        auto lease = m_Impl->io.Submit([this, session]
        {
            if (!m_Impl->owned.contains(session) || m_Impl->owned.at(session).expiry <= LeaseClock::now()) throw ProtocolError("Profile owner is fenced");
            return m_Impl->owned.at(session);
        }).get();
        std::set<uint32_t> shards;
        for (const auto& profile : profiles)
            if (profile.type == MMQP_PROREQ_TYPE_SUB)
                for (auto shard : m_Impl->fabric.ProfileShards(profile.topic)) shards.insert(shard);
        auto previous = m_Impl->io.Submit([this, session, shards]
        {
            auto old = m_Impl->profileShards[session];
            m_Impl->profileShards[session] = shards;
            return old;
        }).get();
        shards.insert(previous.begin(), previous.end());
        for (auto shard : shards)
        {
            Writer body(m_Impl->options.transport.maxPacket);
            body.String(m_Impl->options.identifier);
            body.String(session);
            body.U64(expiry);
            body.U8(online);
            std::vector<ProfileEntry> entries;
            for (const auto& profile : profiles)
            {
                if (profile.type != MMQP_PROREQ_TYPE_SUB) continue;
                auto placements = m_Impl->fabric.ProfileShards(profile.topic);
                if (std::find(placements.begin(), placements.end(), shard) != placements.end()) entries.push_back(profile);
            }
            body.U8(!entries.empty());
            if (!entries.empty()) WriteBuffer(body, Codec::Encode(ProfileRequest{1, entries}, {false, UINT32_MAX, UINT32_MAX}));
            Writer request(m_Impl->options.transport.maxPacket);
            request.String("profile/" + std::to_string(shard) + '/' + session);
            Value{lease.fence, lease.revision, body.Take(), expiry}.Save(request);
            m_Impl->Quorum(m_Impl->fabric.Replicas(shard), Operation::Put, request.Data());
        }
    }

    std::vector<ClusterProfile> ClusterNode::Resolve(std::string topic, uint8_t qos, bool responders)
    {
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
                if (profile.type == MMQP_PROREQ_TYPE_SUB && qos <= profile.qos && (!responders || profile.request) && TopicMatches(profile.topic, topic))
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
        for (const auto& [session, target] : distinct)
        {
            if (!publication.qos) continue;
            ClusterDelivery delivery{identity, sender, session, publication, expiry};
            Writer request(m_Impl->options.transport.maxPacket);
            request.String(InboxPrefix(session) + ToHex(identity));
            WriteBuffer(request, EncodeDelivery(delivery));
            m_Impl->Quorum(m_Impl->fabric.SessionReplicas(session), Operation::Inbox, request.Data());
        }
        ForwardRecord record;
        record.identity = identity;
        record.acceptedEpoch = record.routingEpoch = m_Impl->fabric.Options().epoch;
        record.sequence = sequence;
        record.expires = expiry;
        record.session = sender;
        record.ingress = m_Impl->options.identifier;
        record.publication = publication;
        std::set<std::string> brokers;
        for (const auto& [session, target] : distinct) if (!target.broker.empty()) brokers.insert(target.broker);
        record.destinations.assign(brokers.begin(), brokers.end());
        if (!record.destinations.empty())
        {
            Writer writer(m_Impl->options.transport.maxPacket);
            WriteBuffer(writer, FabricCodec::Encode(record, m_Impl->options.epochKey, m_Impl->options.transport.maxPacket));
            size_t count = 0;
            for (const auto& [session, target] : distinct) if (!target.broker.empty()) ++count;
            writer.U32(static_cast<uint32_t>(count));
            for (const auto& [session, target] : distinct)
                if (!target.broker.empty()) { writer.String(target.broker); writer.String(session); }
            m_Impl->io.Submit([this, bytes = writer.Take()]
            { m_Impl->Forward(m_Impl->options.identifier, bytes); }).get();
        }
    }

    std::vector<ClusterDelivery> ClusterNode::Inbox(std::string session)
    {
        auto records = m_Impl->Scan(m_Impl->fabric.SessionReplicas(session), InboxPrefix(session), false);
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
        m_Impl->Quorum(m_Impl->fabric.SessionReplicas(delivery.session), Operation::Consume, request.Data());
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
        this->Commit(lock, body.Data(), expiry);
        auto lease = m_Impl->io.Submit([this, lock] { return m_Impl->owned.at(lock); }).get();
        Writer request(m_Impl->options.transport.maxPacket);
        request.String(key);
        Value{lease.fence, lease.revision, body.Take(), expiry}.Save(request);
        m_Impl->Quorum(m_Impl->fabric.Replicas(shard), Operation::Put, request.Data());
        this->Release(lock);
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
                        if (profile.type == MMQP_PROREQ_TYPE_SUB && profile.qos && !profile.request && TopicMatches(profile.topic, delivery.publication.topic))
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
