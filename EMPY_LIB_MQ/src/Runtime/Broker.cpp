#include <eMQ/Runtime/Broker.h>
#include <eMQ/Common/State.h>
#include <eMQ/Network/IOContext.h>
#include <eMQ/Security/Crypto.h>
#include <deque>
#include <map>
#include <set>

namespace eMQ
{
    struct Broker::Impl
    {
        enum class Phase { Request, Challenge, Active, Closing };

        struct Peer
        {
            uint64_t id = 0;
            Connection::Pointer connection;
            Phase phase = Phase::Request;
            CodecOptions codec;
            ConnectionRequest request;
            Buffer expected;
            std::string session;
            std::string realm;
            TimePoint deadline;
        };

        struct ResponseRoute
        {
            std::string requester;
            uint16_t correlationId = 0;
            std::string topic;
            TimePoint expiry;
        };

        struct Request : ResponseRoute
        {
            std::set<std::string> responders;
            uint8_t qos = 0;
            std::string broker;
        };

        struct Session
        {
            static constexpr bool in_place_delete = true;
            std::string identifier;
            SessionProperties properties;
            ProfileTable profiles;
            ReceiveWindow received;
            PacketSequence sequence;
            PendingQueue pending;
            std::map<uint16_t, std::pair<Digest, Reason>> receipts;
            std::map<uint16_t, std::pair<Digest, Reason>> profileReceipts;
            uint16_t lastProfile = 0;
            std::weak_ptr<Peer> peer;
            TimePoint expiry;
            TimePoint activity;
            TimePoint pingDeadline;
            bool ping = false;
            bool authenticated = false;
            bool resumable = true;
            uint64_t originSequence = 0;
            std::deque<ClusterDelivery> replay;
            size_t replayBytes = 0;
            std::map<Digest, uint64_t> fabricReceipts;
            TimePoint nextInbox{};
            TimePoint nextCheckpoint{};
            CorrelationIds requestIds;
            PacketSequence responseIds;
            std::map<uint16_t, Request> requests;
            std::map<uint16_t, ResponseRoute> responseRoutes;
            std::map<uint16_t, PublishRequest> rpcAdmissions;
            std::string realm;
            TrafficMetrics traffic;
            uint64_t observedAt = UnixMilliseconds();
            uint64_t publications = 0;
            uint64_t deliveries = 0;
        };

        struct Retained
        {
            PublishRequest message;
            TimePoint expiry;
            size_t bytes = 0;
        };

        struct Dispatch
        {
            Session* session;
            PublishRequest message;
            Buffer bytes;
            TimePoint expiry;
            std::string topic;
            std::optional<ResponseRoute> response;
        };

        Broker& owner;
        BrokerOptions options;
        IoContext io;
        asio::steady_timer timer;
        eCore::Registry registry;
        std::map<std::string, eCore::Identt> sessions;
        std::map<uint64_t, std::shared_ptr<Peer>> peers;
        std::map<std::string, std::deque<Retained>> retained;
        std::map<std::pair<std::string, std::string>, size_t> sharedCursor;
        std::map<std::string, TimePoint> authTokens;
        BrokerMetrics metrics;
        TimePoint startedAt{};
        std::string instance = ToHex(RandomBytes(16));
        uint64_t nextPeer = 0;
        uint16_t port = 0;
        bool started = false;
        bool stopping = false;
        std::unique_ptr<FileStore> storage;
        std::unique_ptr<ClusterNode> cluster;
        std::set<std::string> dirty;
        std::set<std::string> dirtyProfiles;
        std::vector<std::function<void()>> effects;
        size_t effectBytes = 0;
        bool transaction = false;
        bool stateDirty = false;
        std::atomic<size_t> notificationBytes = 0;

        Impl(Broker& service, BrokerOptions settings):
            owner(service), options(std::move(settings)), timer(io.Executor())
        {
            DecodeInterval(options.sessionExpiry);
            DecodeInterval(options.keepalive);
            if (!options.maxSessions || !options.maxProfiles || !options.maxInflight || options.maxInflight > 256
                || !options.maxSessionBytes || !options.maxRetainedBytes || !options.maxRetainedTopics
                || !options.maxRequests || !options.maxResponders || !options.workers
                || !options.retryInterval || !options.requestTimeout || !options.maxRetries
                || options.transport.maxPacket < 64 || options.transport.maxQueue < options.transport.maxPacket)
                throw std::invalid_argument("Invalid broker resource bounds");
            if (options.requireAuth && options.authKey.size() < 32 && options.tenants.empty())
                throw std::invalid_argument("Required authentication needs a separate key of at least 32 bytes");
            if (!options.authKey.empty() && options.authKey.size() < 32)
                throw std::invalid_argument("Authentication key must contain at least 32 bytes");
            if (!options.tenantRoot.empty()) options.tenantRoot = ParseTopic(options.tenantRoot).filter;
            if (!options.tenants.empty())
            {
                if (!options.authKey.empty() || !options.tenantRoot.empty()) throw std::invalid_argument("Use tenant credentials or the default authentication realm, not both");
                options.requireAuth = true;
                std::set<std::array<uint8_t, 8>> identities;
                std::vector<std::string> roots;
                for (auto& tenant : options.tenants)
                {
                    tenant.root = ParseTopic(tenant.root).filter;
                    if (tenant.authKey.size() < 32 || !identities.insert(AuthenticationIdentity(tenant.identity)).second)
                        throw std::invalid_argument("Invalid tenant credentials or duplicate authentication identity");
                    for (const auto& root : roots)
                        if (root == tenant.root || root.starts_with(tenant.root + '/') || tenant.root.starts_with(root + '/'))
                            throw std::invalid_argument("Tenant namespace roots must be disjoint");
                    roots.push_back(tenant.root);
                    for (auto shard : tenant.shards)
                        if (!options.cluster || shard >= options.cluster->fabric.shards) throw std::invalid_argument("Tenant shard is outside the configured fabric");
                }
            }
            if (!options.transport.tls && !options.transport.quic && !options.allowRemotePlaintext
                && !asio::ip::make_address(options.address).is_loopback())
                throw std::invalid_argument("Remote plaintext listeners require explicit opt-in");
            if (!options.maxStateBytes || !options.maxReplayBytes) throw std::invalid_argument("Invalid state/replay bounds");
            if (options.cluster)
            {
                cluster = std::make_unique<ClusterNode>(*options.cluster);
                cluster->onDelivery = [this] (ClusterDelivery delivery)
                {
                    // The replicated inbox owns the payload; notifications only wake its consumer.
                    if (notificationBytes.fetch_add(64) >= options.maxSessionBytes) { notificationBytes.fetch_sub(64); return; }
                    io.Post([this, session = std::move(delivery.session)]
                    { notificationBytes.fetch_sub(64); if (auto value = this->Find(session)) value->nextInbox = TimePoint{}; });
                };
                cluster->onTransient = [this] (ClusterDelivery delivery)
                {
                    auto bytes = delivery.publication.payload.size() + 512;
                    auto previous = notificationBytes.fetch_add(bytes);
                    if (previous >= options.maxSessionBytes || bytes > options.maxSessionBytes - previous)
                    { notificationBytes.fetch_sub(bytes); return; }
                    io.Post([this, bytes, delivery = std::move(delivery)]
                    {
                        notificationBytes.fetch_sub(bytes);
                        auto session = this->Find(delivery.session);
                        if (!session || session->peer.expired() || !cluster->Owns(delivery.session)
                            || delivery.expiry <= UnixMilliseconds() || stopping) return;
                        transaction = true;
                        try
                        {
                            this->AdmitTopic(delivery.publication.topic, false, session);
                            bool response = delivery.publication.type == MMQP_PUBREQ_TYPE_RES;
                            if ((response && this->AcceptResponse(*session, delivery.publication, delivery.sender))
                                || (!response && !session->profiles.Subscriptions(delivery.publication.topic, 0,
                                    delivery.publication.type == MMQP_PUBREQ_TYPE_REQ).empty()))
                            {
                                this->Deliver(this->Prepare(*session, delivery.publication, LoadExpiry(delivery.expiry), delivery.sender));
                                if (response) this->CompleteResponse(*session, delivery.publication.correlationId, delivery.sender);
                            }
                            this->Finish();
                        }
                        catch (...)
                        {
                            transaction = false;
                            effects.clear();
                            effectBytes = 0;
                        }
                    });
                };
            }
            else if (!options.stateDirectory.empty())
            {
                storage = std::make_unique<FileStore>(options.stateDirectory, options.maxStateBytes);
                if (auto saved = storage->Get("broker")) this->Restore(*saved);
            }
        }

        ~Impl()
        {
            SecureErase(options.authKey);
            for (auto& tenant : options.tenants) SecureErase(tenant.authKey);
        }

        Buffer SaveSession(const Session& session)
        {
            Writer writer(options.maxStateBytes);
            writer.U32(0x45535333);
            writer.String(session.identifier);
            writer.String(session.realm);
            ConnectionRequest request;
            request.properties = session.properties;
            WriteBuffer(writer, Codec::Encode(request, {true, UINT32_MAX}));
            writer.U8(!session.profiles.entries.empty());
            if (!session.profiles.entries.empty())
                WriteBuffer(writer, Codec::Encode(ProfileRequest{1, session.profiles.entries}, {true, UINT32_MAX, UINT32_MAX}));
            session.profiles.publishers.Save(writer);
            session.profiles.subscribers.Save(writer);
            session.received.Save(writer);
            writer.U16(session.sequence.Value());
            writer.U16(session.lastProfile);
            writer.U8(session.authenticated);
            writer.U8(session.resumable);
            auto expiry = session.peer.expired() ? session.expiry
                : Clock::now() + std::chrono::seconds(DecodeInterval(*session.properties.policy->expiry))
                    + std::chrono::milliseconds(options.cluster ? options.cluster->leaseMilliseconds : 0);
            writer.U64(SaveExpiry(expiry));
            writer.U64(session.originSequence);
            auto receipts = [&] (const auto& values)
            {
                writer.U32(static_cast<uint32_t>(values.size()));
                for (const auto& [id, value] : values)
                {
                    writer.U16(id);
                    writer.Bytes(value.first);
                    WriteBuffer(writer, Codec::Encode(PublishAck{id, value.second}, {true, UINT32_MAX}));
                }
            };
            receipts(session.receipts);
            receipts(session.profileReceipts);
            writer.U32(static_cast<uint32_t>(session.pending.entries.size()));
            for (const auto& [id, pending] : session.pending.entries)
            {
                writer.U64(SaveExpiry(pending.expiry));
                WriteBuffer(writer, Codec::Encode(pending.message, {true, UINT32_MAX}));
            }
            writer.U32(static_cast<uint32_t>(session.replay.size()));
            for (const auto& record : session.replay)
            {
                writer.Bytes(record.identity);
                writer.U64(record.expiry);
                WriteBuffer(writer, Codec::Encode(record.publication, {true, UINT32_MAX}));
            }
            writer.U32(static_cast<uint32_t>(session.fabricReceipts.size()));
            for (const auto& [identity, expiry] : session.fabricReceipts) { writer.Bytes(identity); writer.U64(expiry); }
            session.requestIds.Save(writer);
            writer.U16(session.responseIds.Value());
            writer.U32(static_cast<uint32_t>(session.requests.size()));
            for (const auto& [id, request] : session.requests) WriteBuffer(writer, this->SaveRequest(request));
            writer.U32(static_cast<uint32_t>(session.responseRoutes.size()));
            for (const auto& [id, route] : session.responseRoutes)
            {
                writer.U16(id);
                writer.String(route.requester);
                writer.U16(route.correlationId);
                writer.String(route.topic);
                writer.U64(SaveExpiry(route.expiry));
            }
            writer.U32(static_cast<uint32_t>(session.rpcAdmissions.size()));
            for (const auto& [id, message] : session.rpcAdmissions)
                WriteBuffer(writer, Codec::Encode(message, {true, UINT32_MAX}));
            return writer.Take();
        }

        Session LoadSession(std::span<const uint8_t> bytes)
        {
            Reader reader(bytes);
            if (reader.U32() != 0x45535333) throw ProtocolError("Unknown session snapshot version; correlation IDs require ESS3");
            Session session;
            session.identifier = reader.String();
            session.realm = reader.String();
            session.properties = std::get<ConnectionRequest>(Codec::Decode(ReadBuffer(reader), {true, UINT32_MAX})).properties;
            if (!session.properties.policy || !session.properties.policy->expiry || !session.properties.policy->keepalive
                || !session.properties.policy->maxPacket || !session.properties.auth
                || session.properties.auth->identifier != session.identifier) throw ProtocolError("Incomplete session snapshot");
            if (reader.U8()) session.profiles.entries = std::get<ProfileRequest>(
                Codec::Decode(ReadBuffer(reader), {true, UINT32_MAX, static_cast<uint32_t>(options.maxProfiles)})).entries;
            session.profiles.publishers = AliasMap::Load(reader);
            session.profiles.subscribers = AliasMap::Load(reader);
            session.received = ReceiveWindow::Load(reader);
            session.sequence = PacketSequence(reader.U16());
            session.lastProfile = reader.U16();
            session.authenticated = reader.U8() != 0;
            session.resumable = reader.U8() != 0;
            session.expiry = LoadExpiry(reader.U64());
            session.originSequence = reader.U64();
            auto receipts = [&] (auto& values)
            {
                auto count = reader.U32();
                if (count > options.maxInflight) throw ProtocolError("Stored receipt budget exceeded");
                for (uint32_t i = 0; i < count; ++i)
                {
                    auto id = reader.U16();
                    Digest digest{};
                    auto bytes = reader.Bytes(digest.size());
                    std::copy(bytes.begin(), bytes.end(), digest.begin());
                    auto ack = std::get<PublishAck>(Codec::Decode(ReadBuffer(reader), {true, UINT32_MAX}));
                    if (ack.packetId != id || !values.emplace(id, std::make_pair(digest, ack.reason)).second)
                        throw ProtocolError("Invalid stored receipt");
                }
            };
            receipts(session.receipts);
            receipts(session.profileReceipts);
            session.pending.maxBytes = options.maxSessionBytes;
            session.pending.maxCount = options.maxInflight;
            auto count = reader.U32();
            if (count > options.maxInflight) throw ProtocolError("Stored pending budget exceeded");
            for (uint32_t i = 0; i < count; ++i)
            {
                auto expiry = LoadExpiry(reader.U64());
                auto message = std::get<PublishRequest>(Codec::Decode(ReadBuffer(reader), {true, UINT32_MAX}));
                if (message.packetId > session.sequence.Value()) throw ProtocolError("Stored outgoing PID exceeds sequence");
                if (expiry <= Clock::now() && session.properties.quic) session.resumable = false;
                if (expiry > Clock::now())
                {
                    auto size = Codec::Encode(message, {session.properties.policy->reasons, UINT32_MAX}).size();
                    session.pending.Add(std::move(message), size, Clock::now(), expiry);
                }
            }
            count = reader.U32();
            if (count > options.maxReplayBytes / 32) throw ProtocolError("Stored replay count exceeded");
            for (uint32_t i = 0; i < count; ++i)
            {
                ClusterDelivery record;
                auto identity = reader.Bytes(32);
                std::copy(identity.begin(), identity.end(), record.identity.begin());
                record.expiry = reader.U64();
                auto bytes = ReadBuffer(reader);
                record.publication = std::get<PublishRequest>(Codec::Decode(bytes, {true, UINT32_MAX}));
                if (record.expiry > UnixMilliseconds())
                {
                    session.replayBytes += bytes.size();
                    if (session.replayBytes > options.maxReplayBytes) throw ProtocolError("Stored replay byte budget exceeded");
                    session.replay.emplace_back(std::move(record));
                }
            }
            count = reader.U32();
            if (count > 8192) throw ProtocolError("Stored fabric receipt budget exceeded");
            for (uint32_t i = 0; i < count; ++i)
            {
                Digest identity{};
                auto bytes = reader.Bytes(32);
                std::copy(bytes.begin(), bytes.end(), identity.begin());
                auto expiry = reader.U64();
                if (expiry > UnixMilliseconds()) session.fabricReceipts.emplace(identity, expiry);
            }
            session.requestIds = CorrelationIds::Load(reader);
            session.responseIds = PacketSequence(reader.U16());
            count = reader.U32();
            if (count > options.maxRequests) throw ProtocolError("Stored request budget exceeded");
            for (uint32_t i = 0; i < count; ++i)
            {
                auto request = this->ReadRequest(ReadBuffer(reader));
                if (request.requester != session.identifier || !session.requestIds.Contains(request.correlationId)
                    || !session.requests.emplace(request.correlationId, std::move(request)).second)
                    throw ProtocolError("Invalid stored request identity");
            }
            count = reader.U32();
            if (count > options.maxRequests) throw ProtocolError("Stored response-route budget exceeded");
            size_t routeBytes = 0;
            for (uint32_t i = 0; i < count; ++i)
            {
                auto id = reader.U16();
                ResponseRoute route;
                route.requester = reader.String();
                route.correlationId = reader.U16();
                route.topic = ParseTopic(reader.String()).filter;
                route.expiry = LoadExpiry(reader.U64());
                routeBytes += this->RouteSize(route);
                if (!id || id > session.responseIds.Value() || !route.correlationId || route.requester.empty()
                    || routeBytes > options.maxSessionBytes || !session.responseRoutes.emplace(id, std::move(route)).second)
                    throw ProtocolError("Invalid stored response route");
            }
            count = reader.U32();
            if (count > options.maxInflight) throw ProtocolError("Stored RPC-admission count exceeded");
            size_t admissionBytes = 0;
            for (uint32_t i = 0; i < count; ++i)
            {
                auto bytes = ReadBuffer(reader);
                admissionBytes += bytes.size();
                auto message = std::get<PublishRequest>(Codec::Decode(bytes, {true, UINT32_MAX}));
                const ResponseRoute* route = nullptr;
                if (message.type == MMQP_PUBREQ_TYPE_REQ)
                {
                    auto request = session.requests.find(message.correlationId);
                    if (request != session.requests.end()) route = &request->second;
                }
                else if (message.type == MMQP_PUBREQ_TYPE_RES)
                {
                    auto response = session.responseRoutes.find(message.correlationId);
                    if (response != session.responseRoutes.end()) route = &response->second;
                }
                if (admissionBytes > options.maxSessionBytes || !message.qos || !route
                    || session.received.Inspect(message.packetId) != Reception::New || route->topic != message.topic
                    || !session.rpcAdmissions.emplace(message.packetId, std::move(message)).second)
                    throw ProtocolError("Invalid stored RPC admission");
            }
            reader.Finish();
            return session;
        }

        Buffer Snapshot()
        {
            Writer writer(options.maxStateBytes);
            writer.U32(0x45425233);
            writer.U32(static_cast<uint32_t>(sessions.size()));
            for (const auto& [id, entity] : sessions) WriteBuffer(writer, this->SaveSession(registry.get<Session>(entity)));
            writer.U32(static_cast<uint32_t>(retained.size()));
            for (const auto& [topic, records] : retained)
            {
                writer.String(topic);
                writer.U32(static_cast<uint32_t>(records.size()));
                for (const auto& record : records)
                {
                    writer.U64(SaveExpiry(record.expiry));
                    WriteBuffer(writer, Codec::Encode(record.message, {true, UINT32_MAX}));
                }
            }
            writer.U32(static_cast<uint32_t>(authTokens.size()));
            for (const auto& [token, expiry] : authTokens) { writer.String(token); writer.U64(SaveExpiry(expiry)); }
            return writer.Take();
        }

        void Restore(const Buffer& bytes)
        {
            Reader reader(bytes);
            if (reader.U32() != 0x45425233) throw ProtocolError("Unknown broker snapshot version; correlation IDs require EBR3");
            auto count = reader.U32();
            if (count > options.maxSessions) throw ProtocolError("Stored sessions exceed configured budget");
            for (uint32_t i = 0; i < count; ++i)
            {
                auto session = this->LoadSession(ReadBuffer(reader));
                if (session.expiry <= Clock::now()) continue;
                auto entity = registry.create();
                if (!sessions.emplace(session.identifier, entity).second) throw ProtocolError("Duplicate stored session");
                registry.emplace<Session>(entity, std::move(session));
            }
            count = reader.U32();
            if (count > options.maxRetainedTopics) throw ProtocolError("Stored topics exceed configured budget");
            for (uint32_t i = 0; i < count; ++i)
            {
                auto topic = reader.String();
                auto size = reader.U32();
                if (size > 255) throw ProtocolError("Stored retention queue exceeds profile limit");
                for (uint32_t j = 0; j < size; ++j)
                {
                    auto expiry = LoadExpiry(reader.U64());
                    auto record = ReadBuffer(reader);
                    auto publication = std::get<PublishRequest>(Codec::Decode(record, {true, UINT32_MAX}));
                    if (publication.topic != topic) throw ProtocolError("Stored retained topic mismatch");
                    if (expiry > Clock::now())
                    {
                        auto bytes = Codec::Encode(publication, {false, UINT32_MAX}).size();
                        metrics.retainedBytes += bytes;
                        if (metrics.retainedBytes > options.maxRetainedBytes) throw ProtocolError("Stored retention exceeds budget");
                        retained[topic].push_back({std::move(publication), expiry, bytes});
                    }
                }
            }
            if (this->RequestCount() > options.maxRequests) throw ProtocolError("Stored request budget exceeded");
            count = reader.U32();
            if (count > 4096) throw ProtocolError("Stored authentication budget exceeded");
            for (uint32_t i = 0; i < count; ++i)
            {
                auto token = reader.String();
                auto expiry = LoadExpiry(reader.U64());
                if (expiry > Clock::now()) authTokens.emplace(std::move(token), expiry);
            }
            reader.Finish();
        }

        void Changed(Session& session, bool profile = false)
        {
            dirty.insert(session.identifier);
            if (profile) dirtyProfiles.insert(session.identifier);
            stateDirty = true;
        }

        void Checkpoint()
        {
            if (!stateDirty) return;
            if (storage) storage->Put("broker", this->Snapshot());
            if (cluster)
            {
                for (const auto& id : dirty)
                    if (auto session = this->Find(id))
                    {
                        if (cluster->Owns(id))
                        {
                            auto expiry = session->peer.expired() ? SaveExpiry(session->expiry)
                                : UnixMilliseconds() + DecodeInterval(*session->properties.policy->expiry) * 1000ULL + options.cluster->leaseMilliseconds;
                            cluster->Commit(id, this->SaveSession(*session), expiry);
                        }
                        else if (!session->peer.expired()) throw ProtocolError("Session ownership was lost", MMQP_REASON_BUSY);
                    }
                for (const auto& id : dirtyProfiles)
                    if (auto session = this->Find(id); session && cluster->Owns(id))
                    {
                        auto expiry = session->peer.expired() ? SaveExpiry(session->expiry)
                            : UnixMilliseconds() + DecodeInterval(*session->properties.policy->expiry) * 1000ULL;
                        cluster->Profiles(id, session->profiles.entries, expiry, !session->peer.expired());
                    }
            }
            dirty.clear();
            dirtyProfiles.clear();
            stateDirty = false;
        }

        bool Transmit(const std::shared_ptr<Peer>& peer, Buffer bytes)
        {
            auto size = bytes.size();
            if (!peer->connection->Send(std::move(bytes))) return false;
            ++metrics.sentPackets;
            metrics.sentBytes += size;
            if (auto session = this->Find(peer->session))
            {
                ++session->traffic.sentPackets;
                session->traffic.sentBytes += size;
            }
            return true;
        }

        bool Queue(const std::shared_ptr<Peer>& peer, Buffer bytes)
        {
            if (!transaction) return this->Transmit(peer, std::move(bytes));
            if (effectBytes > options.maxStateBytes || bytes.size() > options.maxStateBytes - effectBytes)
                throw ProtocolError("Pending effect budget exhausted", MMQP_REASON_BUSY);
            effectBytes += bytes.size();
            effects.push_back([this, peer, bytes = std::move(bytes)] () mutable
            {
                if (cluster && !peer->session.empty() && !cluster->Owns(peer->session)) { peer->connection->Close(false); return; }
                if (!this->Transmit(peer, std::move(bytes))) peer->connection->Close(false);
            });
            return true;
        }

        void Finish()
        {
            this->Checkpoint();
            transaction = false;
            effectBytes = 0;
            auto ready = std::move(effects);
            effects.clear();
            for (auto& effect : ready) effect();
        }

        void Close(const std::shared_ptr<Peer>& peer, bool drain)
        {
            if (transaction) effects.push_back([peer, drain] { peer->connection->Close(drain); });
            else peer->connection->Close(drain);
        }

        Session* Find(const std::string& identifier)
        {
            auto found = sessions.find(identifier);
            return found == sessions.end() ? nullptr : &registry.get<Session>(found->second);
        }

        const TenantOptions* Realm(const std::string& name) const
        {
            if (name.empty()) return nullptr;
            for (const auto& tenant : options.tenants) if (tenant.identity == name) return &tenant;
            throw ProtocolError("Unknown authentication realm", MMQP_REASON_UNAUTHORIZED);
        }

        std::string TenantRoot(const Session* session) const
        {
            auto realm = this->Realm(session ? session->realm : "");
            return realm ? realm->root : options.tenantRoot;
        }

        void AdmitTopic(std::string_view topic, bool filter, const Session* session = nullptr)
        {
            auto pattern = ParseTopic(topic, filter).filter;
            if (pattern.starts_with(MMQP_BIRTH_STATUS_TOPIC_PREFIX)) pattern.erase(0, std::string_view(MMQP_BIRTH_STATUS_TOPIC_PREFIX).size());
            else if (pattern.starts_with(MMQP_LAST_STATUS_TOPIC_PREFIX)) pattern.erase(0, std::string_view(MMQP_LAST_STATUS_TOPIC_PREFIX).size());
            auto root = this->TenantRoot(session);
            if (!root.empty() && pattern != root && !pattern.starts_with(root + '/'))
                throw ProtocolError("Topic is outside the configured tenant", MMQP_REASON_UNAUTHORIZED);
            auto realm = this->Realm(session ? session->realm : "");
            if (realm && !realm->shards.empty() && cluster)
                for (auto shard : cluster->Placement().ProfileShards(pattern))
                    if (!realm->shards.contains(shard)) throw ProtocolError("Topic exceeds the tenant shard budget", MMQP_REASON_UNAUTHORIZED);
        }

        void AdmitProfiles(const ProfileRequest& profile, const Session* session = nullptr)
        {
            for (const auto& entry : profile.entries)
            {
                if (entry.type == MMQP_PROREQ_TYPE_REG && !entry.topic.empty()
                    && ParseTopic(entry.topic, true).filter.starts_with('$'))
                    throw ProtocolError("Reserved topics cannot be registered by publishers", MMQP_REASON_UNAUTHORIZED);
                if (entry.type == MMQP_PROREQ_TYPE_SUB && entry.alias >= 0x8000)
                {
                    if (!session) throw ProtocolError("New subscriber aliases must be below 0x8000", MMQP_REASON_INVALID_ALIAS);
                    const auto& mapped = session->profiles.subscribers.Resolve(entry.alias);
                    if (!entry.topic.empty() && mapped != CanonicalTopic(entry.topic))
                        throw ProtocolError("Broker-assigned alias cannot be rebound", MMQP_REASON_INVALID_ALIAS);
                }
                if (!entry.topic.empty()) this->AdmitTopic(entry.topic, true, session);
            }
        }

        bool Send(const std::shared_ptr<Peer>& peer, const Message& message)
        {
            if (peer->phase == Phase::Closing) return false;
            return this->Queue(peer, Codec::Encode(message, peer->codec));
        }

        void Fail(const std::shared_ptr<Peer>& peer, Reason reason, bool connecting = false)
        {
            ++metrics.rejected;
            if (peer->phase == Phase::Closing) return;
            if (cluster && connecting && peer->request.resume && peer->request.properties.auth)
            {
                auto id = peer->request.properties.auth->identifier;
                auto session = this->Find(id);
                if (!session || session->peer.expired()) cluster->Release(id);
            }
            try
            {
                if (connecting) this->Send(peer, ConnectionAck{reason, {}});
                else this->Send(peer, ConnectionEnd{reason});
            }
            catch (...) {}
            peer->phase = Phase::Closing;
            this->Close(peer, true);
        }

        void Accepted(Connection::Pointer connection)
        {
            auto peer = std::make_shared<Peer>();
            peer->id = ++nextPeer;
            peer->connection = std::move(connection);
            peer->codec.maxPacket = options.transport.maxPacket;
            peer->codec.maxProfiles = static_cast<uint32_t>(options.maxProfiles);
            peer->deadline = Clock::now() + std::chrono::milliseconds(options.transport.timeout);
            peers.emplace(peer->id, peer);
            peer->connection->onReceive = [this, id = peer->id] (auto bytes) { this->Receive(id, bytes); };
            peer->connection->onClose = [this, id = peer->id] (const auto& reason) { this->Closed(id, reason); };
        }

        void Establish(const std::shared_ptr<Peer>& peer, bool authenticated)
        {
            auto& request = peer->request;
            auto now = Clock::now();
            Session* session = nullptr;
            bool created = false;
            if (cluster && request.resume)
            {
                auto identifier = request.properties.auth->identifier;
                auto existing = this->Find(identifier);
                if (existing && !existing->peer.expired()) throw ProtocolError("Session is already connected", MMQP_REASON_BUSY);
                auto snapshot = cluster->Acquire(identifier);
                if (snapshot.empty()) { cluster->Release(identifier); throw ProtocolError("Unknown replicated session", MMQP_REASON_SESSION_EXPIRED); }
                auto restored = this->LoadSession(snapshot);
                if (restored.identifier != identifier) throw ProtocolError("Snapshot identity differs from requested SID");
                if (existing) *existing = std::move(restored);
                else
                {
                    if (sessions.size() >= options.maxSessions) throw ProtocolError("Session budget exhausted", MMQP_REASON_BUSY);
                    auto entity = registry.create();
                    sessions.emplace(identifier, entity);
                    registry.emplace<Session>(entity, std::move(restored));
                }
            }
            if (request.resume)
            {
                session = this->Find(request.properties.auth->identifier);
                if (!session || !session->resumable || session->expiry <= now)
                    throw ProtocolError("Stored session has expired", MMQP_REASON_SESSION_EXPIRED);
                if (!session->peer.expired())
                    throw ProtocolError("Session is already connected", MMQP_REASON_BUSY);
                if (session->authenticated != authenticated)
                    throw ProtocolError("Session authentication cannot change on resume", MMQP_REASON_AUTH_FAILED);
                if (session->realm != peer->realm) throw ProtocolError("Session cannot change authentication realm", MMQP_REASON_AUTH_FAILED);
                if (request.properties.profile || request.properties.will)
                    throw ProtocolError("Resume preserves stored profiles and wills; update profiles after CONACK",
                        MMQP_REASON_POLICY_VIOLATION);
            }
            else
            {
                if (sessions.size() >= options.maxSessions)
                    throw ProtocolError("Session budget exhausted", MMQP_REASON_BUSY);
                auto entity = registry.create();
                session = &registry.emplace<Session>(entity);
                session->realm = peer->realm;
                session->identifier = ToHex(RandomBytes(16));
                while (sessions.contains(session->identifier)) session->identifier = ToHex(RandomBytes(16));
                sessions.emplace(session->identifier, entity);
                created = true;
            }
            try
            {
                if (cluster && created) cluster->Acquire(session->identifier);
                if (request.properties.quic && !peer->connection->Quic())
                    throw ProtocolError("QUIC policy is not valid on the TCP listener", MMQP_REASON_POLICY_VIOLATION);
                std::optional<QuicPolicy> quic;
                if (peer->connection->Quic())
                    quic = QuicPlan::Negotiate(request.properties.quic.value_or(QuicPolicy{}), options.transport.quicPolicy);
                if (request.resume && quic != session->properties.quic)
                    throw ProtocolError("Resume must preserve the negotiated transport and QUIC policy", MMQP_REASON_POLICY_VIOLATION);
                if (created) session->received = ReceiveWindow(!peer->connection->Quic());
                session->properties.quic = quic;
                auto policy = request.properties.policy.value_or(SessionPolicy{});
                if (!policy.expiry) policy.expiry = options.sessionExpiry;
                if (!policy.keepalive) policy.keepalive = options.keepalive;
                policy.maxPacket = std::min(policy.maxPacket.value_or(options.transport.maxPacket), options.transport.maxPacket);
                if (*policy.maxPacket < 64)
                    throw ProtocolError("Negotiated packet limit must be at least 64", MMQP_REASON_POLICY_VIOLATION);
                peer->codec.reasons = policy.reasons;
                peer->codec.maxPacket = *policy.maxPacket;
                peer->connection->SetLimit(*policy.maxPacket);
                if (request.properties.profile)
                {
                    this->AdmitProfiles(*request.properties.profile, session);
                    session->profiles.Apply(*request.properties.profile, options.maxProfiles);
                    session->lastProfile = request.properties.profile->packetId;
                }
                if (request.properties.will)
                {
                    auto topic = request.properties.will->topic;
                    if (!topic.empty() && ParseTopic(topic).filter.starts_with('$'))
                        throw ProtocolError("Will topic must be an ordinary topic suffix", MMQP_REASON_UNAUTHORIZED);
                    if (!topic.empty()) this->AdmitTopic(topic, false, session);
                    session->properties.will = request.properties.will;
                    auto root = this->TenantRoot(session);
                    if (topic.empty() && !root.empty()) session->properties.will->topic = root + '/' + session->identifier;
                    auto suffix = session->properties.will->topic.empty() ? session->identifier : session->properties.will->topic;
                    ParseTopic(std::string(MMQP_BIRTH_STATUS_TOPIC_PREFIX) + suffix);
                }
                session->properties.policy = policy;
                session->properties.auth = SessionAuth{session->identifier, {}};
                session->pending.maxBytes = options.maxSessionBytes;
                session->pending.maxCount = options.maxInflight;
                session->authenticated = authenticated;
                session->activity = now;
                session->expiry = TimePoint::max();
                session->ping = false;
                session->peer = peer;
                peer->session = session->identifier;
                peer->phase = Phase::Active;
                this->Changed(*session, true);
                if (cluster)
                    for (auto& [id, pending] : session->requests)
                    {
                        if (pending.expiry <= now) continue;
                        auto key = this->RequestKey(session->identifier, id);
                        cluster->Acquire(key);
                        pending.broker = cluster->Identifier();
                        // The session snapshot includes responders already consumed.
                        // Do not replace it with a potentially older directory copy.
                        cluster->Commit(key, this->SaveRequest(pending), SaveExpiry(pending.expiry));
                    }
                this->RecoverExchanges(*session);
                ConnectionAck ack;
                ack.properties.auth = session->properties.auth;
                ack.properties.policy = session->properties.policy;
                ack.properties.quic = session->properties.quic;
                if (!this->Send(peer, ack)) throw ProtocolError("Transport send budget exhausted", MMQP_REASON_BUSY);
                if (quic) peer->connection->Configure(*quic, policy.reasons);
                ++metrics.accepted;
                owner.TryPost<SessionConnected>(session->identifier.size(), session->identifier, request.resume);
                this->Will(*session, true);
                if (created && request.properties.profile) this->Replay(*session, *request.properties.profile);
                for (auto& [id, publication] : session->pending.entries)
                {
                    publication.retry = now;
                    publication.attempts = 0;
                }
            }
            catch (...)
            {
                if (created)
                {
                    auto identifier = session->identifier;
                    auto entity = sessions.at(identifier);
                    sessions.erase(identifier);
                    registry.destroy(entity);
                    if (cluster) cluster->Release(identifier);
                }
                throw;
            }
        }

        void Connect(const std::shared_ptr<Peer>& peer, ConnectionRequest request)
        {
            peer->request = std::move(request);
            if (peer->request.properties.policy) peer->codec.reasons = peer->request.properties.policy->reasons;
            auto auth = peer->request.properties.auth;
            if (auth && !auth->token.empty())
            {
                const Buffer* key = &options.authKey;
                if (auth->token.size() == 32)
                    for (const auto& tenant : options.tenants)
                    {
                        if (SecureEqual(std::span(auth->token).first(8), AuthenticationIdentity(tenant.identity)))
                        { key = &tenant.authKey; peer->realm = tenant.identity; break; }
                    }
                if (key->empty() || auth->token.size() != 32
                    || (!peer->connection->Secure() && !options.allowInsecureAuth))
                    throw ProtocolError("Authentication requires configured credentials and secure transport", MMQP_REASON_AUTH_FAILED);
                auto token = ToHex(auth->token);
                if (authTokens.contains(token) || authTokens.size() >= 4096)
                    throw ProtocolError("Repeated token or authentication budget exhausted", MMQP_REASON_AUTH_FAILED);
                authTokens.emplace(token, Clock::now() + std::chrono::hours(1));
                stateDirty = true;
                if (cluster)
                {
                    auto key = "auth:" + token;
                    auto previous = cluster->Acquire(key);
                    if (!previous.empty())
                    {
                        Reader reader(previous);
                        if (reader.U64() > UnixMilliseconds()) { cluster->Release(key); throw ProtocolError("Repeated cluster authentication token", MMQP_REASON_AUTH_FAILED); }
                    }
                    Writer record;
                    record.U64(UnixMilliseconds() + 3600000);
                    cluster->Commit(key, record.Take(), UnixMilliseconds() + 3600000);
                    cluster->Release(key);
                }
                auto response = Hmac256(*key, auth->token);
                auto expected = Hmac256(*key, response);
                peer->expected.assign(expected.begin(), expected.end());
                SecureErase(expected);
                peer->phase = Phase::Challenge;
                if (!this->Send(peer, ConnectionSync{Buffer(response.begin(), response.end())}))
                    throw ProtocolError("Authentication send failed", MMQP_REASON_BUSY);
            }
            else
            {
                if (options.requireAuth) throw ProtocolError("Authentication is required", MMQP_REASON_AUTH_FAILED);
                this->Establish(peer, false);
            }
        }

        std::vector<Session*> Destinations(const PublishRequest& publication, bool responders)
        {
            std::map<std::string, Session*> selected;
            std::map<std::pair<std::string, std::string>, std::vector<Session*>> shared;
            for (auto& [identifier, entity] : sessions)
            {
                auto& session = registry.get<Session>(entity);
                if ((session.peer.expired() && (!publication.qos || responders)) || session.expiry <= Clock::now()) continue;
                for (const auto& entry : session.profiles.Subscriptions(publication.topic, publication.qos, responders))
                {
                    auto pattern = ParseTopic(entry.topic, true);
                    if (pattern.group.empty()) selected.emplace(identifier, &session);
                    else if (!session.peer.expired()) shared[{pattern.group, pattern.filter}].push_back(&session);
                }
            }
            for (auto& [group, members] : shared)
            {
                auto& cursor = sharedCursor[group];
                auto selectedMember = members[cursor++ % members.size()];
                selected.emplace(selectedMember->identifier, selectedMember);
            }
            std::vector<Session*> result;
            for (auto& [identifier, session] : selected) result.push_back(session);
            return result;
        }

        void PruneShared()
        {
            std::set<std::pair<std::string, std::string>> groups;
            for (const auto& [id, entity] : sessions)
            {
                for (const auto& profile : registry.get<Session>(entity).profiles.entries)
                {
                    if (profile.type != MMQP_PROREQ_TYPE_SUB) continue;
                    auto pattern = ParseTopic(profile.topic, true);
                    if (!pattern.group.empty()) groups.emplace(pattern.group, pattern.filter);
                }
            }
            std::erase_if(sharedCursor, [&] (const auto& cursor) { return !groups.contains(cursor.first); });
        }

        size_t RouteSize(const ResponseRoute& route) const
        {
            return sizeof(ResponseRoute) + 32 + route.requester.size() + route.topic.size();
        }

        Dispatch Prepare(Session& session, const PublishRequest& source, TimePoint expiry, std::string_view sender = {})
        {
            auto message = source;
            std::optional<ResponseRoute> response;
            if (message.type == MMQP_PUBREQ_TYPE_REQ)
            {
                if (sender.empty() || !message.correlationId) throw ProtocolError("Request is missing its origin");
                if (session.responseRoutes.size() >= options.maxRequests)
                    throw ProtocolError("Responder route budget exhausted", MMQP_REASON_BUSY);
                if (session.responseIds.Value() == UINT16_MAX)
                    throw ProtocolError("Responder correlation IDs exhausted; establish a fresh session", MMQP_REASON_INVALID_CORRELATION);
                response = ResponseRoute{std::string(sender), message.correlationId, source.topic, expiry};
                auto size = this->RouteSize(*response);
                for (const auto& [id, route] : session.responseRoutes) size += this->RouteSize(route);
                if (size > options.maxSessionBytes) throw ProtocolError("Responder route byte budget exhausted", MMQP_REASON_BUSY);
                message.correlationId = session.responseIds.Peek();
            }
            message.alias = session.profiles.subscribers.Candidate(source.topic, 0x8000);
            message.topic = session.profiles.subscribers.Confirmed(message.alias) ? "" : source.topic;
            message.duplicate = false;
            message.packetId = message.qos ? session.sequence.Peek() : 0;
            auto peer = session.peer.lock();
            CodecOptions codec;
            codec.reasons = session.properties.policy->reasons;
            codec.maxPacket = *session.properties.policy->maxPacket;
            auto bytes = Codec::Encode(message, codec);
            if ((message.qos && !session.pending.CanAdd(bytes.size(), message.packetId))
                || (peer && bytes.size() > peer->connection->Capacity() - peer->connection->Queued()))
                throw ProtocolError("Subscriber send budget exhausted", MMQP_REASON_BUSY);
            return {&session, std::move(message), std::move(bytes), expiry, source.topic, std::move(response)};
        }

        void Deliver(Dispatch dispatch)
        {
            auto& session = *dispatch.session;
            if (dispatch.response)
            {
                if (session.responseIds.Next() != dispatch.message.correlationId)
                    throw std::logic_error("Response-route admission order changed");
                session.responseRoutes.emplace(dispatch.message.correlationId, std::move(*dispatch.response));
            }
            session.profiles.subscribers.Map(dispatch.message.alias, dispatch.topic);
            if (dispatch.message.qos)
            {
                if (session.sequence.Next() != dispatch.message.packetId) throw std::logic_error("Publication admission order changed");
                session.pending.Add(dispatch.message, dispatch.bytes.size(),
                    Clock::now() + std::chrono::milliseconds(options.retryInterval), dispatch.expiry);
            }
            this->Changed(session);
            if (auto peer = session.peer.lock())
            {
                if (!this->Queue(peer, std::move(dispatch.bytes))) this->Close(peer, false);
            }
            ++metrics.forwarded;
            ++session.deliveries;
        }

        void PurgeRetained(TimePoint now)
        {
            for (auto it = retained.begin(); it != retained.end();)
            {
                std::erase_if(it->second, [&] (const Retained& value)
                {
                    if (value.expiry > now) return false;
                    metrics.retainedBytes -= value.bytes;
                    return true;
                });
                if (it->second.empty()) it = retained.erase(it);
                else ++it;
            }
        }

        void Retain(const PublishRequest& publication, const ProfileEntry& profile)
        {
            stateDirty = true;
            this->PurgeRetained(Clock::now());
            auto previous = retained.find(publication.topic);
            if (previous == retained.end() && retained.size() >= options.maxRetainedTopics)
                throw ProtocolError("Retained topic budget exhausted", MMQP_REASON_BUSY);
            auto queue = previous == retained.end() ? std::deque<Retained>{} : previous->second;
            size_t oldBytes = 0;
            for (const auto& record : queue) oldBytes += record.bytes;
            auto limit = profile.qos ? profile.queue : 1;
            while (queue.size() >= static_cast<size_t>(limit)) queue.pop_front();
            auto bytes = Codec::Encode(publication, {false, options.transport.maxPacket}).size();
            queue.push_back({publication, Clock::now() + std::chrono::seconds(DecodeInterval(profile.expiry)), bytes});
            size_t newBytes = 0;
            for (const auto& record : queue) newBytes += record.bytes;
            if (newBytes > options.maxRetainedBytes - (metrics.retainedBytes - oldBytes))
                throw ProtocolError("Retained payload budget exhausted", MMQP_REASON_BUSY);
            retained[publication.topic] = std::move(queue);
            metrics.retainedBytes = metrics.retainedBytes - oldBytes + newBytes;
        }

        void Replay(Session& session, const ProfileRequest& profile)
        {
            this->PurgeRetained(Clock::now());
            std::vector<ClusterDelivery> records;
            if (cluster) records = cluster->Retained(profile.entries);
            for (const auto& [topic, queue] : retained)
            {
                bool match = false;
                for (const auto& entry : profile.entries)
                {
                    auto filter = entry.topic.empty() ? session.profiles.subscribers.Resolve(entry.alias) : entry.topic;
                    if (entry.type == MMQP_PROREQ_TYPE_SUB && entry.qos && !entry.request && TopicMatches(filter, topic))
                        match = true;
                }
                if (!match) continue;
                for (const auto& record : queue)
                {
                    records.push_back({{}, {}, session.identifier, record.message, SaveExpiry(record.expiry)});
                }
            }
            size_t bytes = session.replayBytes;
            for (const auto& record : records)
            {
                auto size = Codec::Encode(record.publication, {true, UINT32_MAX}).size();
                if (bytes > options.maxReplayBytes || size > options.maxReplayBytes - bytes)
                    throw ProtocolError("Retained replay budget exceeded", MMQP_REASON_BUSY);
                bytes += size;
            }
            session.replayBytes = bytes;
            for (auto& record : records) session.replay.push_back(std::move(record));
            this->Changed(session);
            this->DrainReplay(session);
        }

        void DrainReplay(Session& session)
        {
            if (session.peer.expired()) return;
            while (!session.replay.empty())
            {
                const auto& record = session.replay.front();
                if (record.expiry > UnixMilliseconds())
                {
                    try { this->Deliver(this->Prepare(session, record.publication, LoadExpiry(record.expiry))); }
                    catch (const ProtocolError& error)
                    {
                        if (error.reason == MMQP_REASON_BUSY) return;
                        throw;
                    }
                }
                session.replayBytes -= Codec::Encode(record.publication, {true, UINT32_MAX}).size();
                session.replay.pop_front();
                this->Changed(session);
            }
        }

        size_t RequestCount() const
        {
            size_t count = 0;
            for (const auto& [id, entity] : sessions)
                if (!cluster || cluster->Owns(id)) count += registry.get<Session>(entity).requests.size();
            return count;
        }

        std::string RequestKey(std::string_view requester, uint16_t correlationId)
        {
            Writer writer;
            writer.String(requester);
            writer.U16(correlationId);
            return "rpc:" + ToHex(Sha256(writer.Data()));
        }

        Buffer SaveRequest(const Request& request)
        {
            Writer writer(options.maxStateBytes);
            writer.U32(0x45525131);
            writer.String(request.requester);
            writer.U16(request.correlationId);
            writer.String(request.topic);
            writer.String(request.broker);
            writer.U64(SaveExpiry(request.expiry));
            writer.U8(request.qos);
            writer.U32(static_cast<uint32_t>(request.responders.size()));
            for (const auto& responder : request.responders) writer.String(responder);
            return writer.Take();
        }

        Request ReadRequest(std::span<const uint8_t> bytes)
        {
            Reader reader(bytes);
            if (reader.U32() != 0x45525131) throw ProtocolError("Unknown request snapshot version");
            Request result;
            result.requester = reader.String();
            result.correlationId = reader.U16();
            result.topic = ParseTopic(reader.String()).filter;
            result.broker = reader.String();
            result.expiry = LoadExpiry(reader.U64());
            result.qos = reader.U8();
            if (result.requester.empty() || !result.correlationId || result.qos > 1)
                throw ProtocolError("Invalid stored request");
            auto count = reader.U32();
            if (count > options.maxResponders) throw ProtocolError("Cluster responder limit exceeded");
            for (uint32_t i = 0; i < count; ++i)
            {
                auto responder = reader.String();
                if (responder.empty() || !result.responders.insert(responder).second)
                    throw ProtocolError("Invalid stored responder");
            }
            reader.Finish();
            return result;
        }

        bool AcceptResponse(const Session& session, const PublishRequest& publication, const std::string& sender) const
        {
            auto found = session.requests.find(publication.correlationId);
            return found != session.requests.end() && found->second.topic == publication.topic
                && found->second.expiry > Clock::now() && found->second.responders.contains(sender);
        }

        void UpdateRequest(const Request& request)
        {
            if (!cluster) return;
            effects.push_back([this, request]
            {
                auto key = this->RequestKey(request.requester, request.correlationId);
                try
                {
                    if (request.expiry > Clock::now()) cluster->Commit(key, this->SaveRequest(request), SaveExpiry(request.expiry));
                    if (request.responders.empty()) cluster->Release(key);
                }
                catch (const ProtocolError&) {}
            });
        }

        void CompleteResponse(Session& session, uint16_t correlationId, const std::string& responder)
        {
            auto found = session.requests.find(correlationId);
            if (found == session.requests.end() || !found->second.responders.erase(responder)) return;
            this->UpdateRequest(found->second);
            if (found->second.responders.empty()) session.requests.erase(found);
            this->Changed(session);
        }

        bool AdmissionPending(const Session& session, uint8_t type, uint16_t correlationId) const
        {
            return std::any_of(session.rpcAdmissions.begin(), session.rpcAdmissions.end(), [&] (const auto& value)
            { return value.second.type == type && value.second.correlationId == correlationId; });
        }

        void StageExchange(Session& session, const PublishRequest& publication)
        {
            auto bytes = Codec::Encode(publication, {true, UINT32_MAX}).size();
            for (const auto& [id, message] : session.rpcAdmissions) bytes += Codec::Encode(message, {true, UINT32_MAX}).size();
            if (session.rpcAdmissions.size() >= options.maxInflight || bytes > options.maxSessionBytes)
                throw ProtocolError("RPC-admission recovery budget exhausted", MMQP_REASON_BUSY);
            session.rpcAdmissions.emplace(publication.packetId, publication);
            this->Changed(session);
        }

        void RecoverExchanges(Session& session)
        {
            if (!cluster) return;
            for (auto it = session.rpcAdmissions.begin(); it != session.rpcAdmissions.end();)
            {
                const auto& message = it->second;
                auto publication = message;
                TimePoint expiry;
                std::vector<ClusterProfile> targets;
                if (message.type == MMQP_PUBREQ_TYPE_REQ)
                {
                    const auto& request = session.requests.at(message.correlationId);
                    expiry = request.expiry;
                    for (const auto& responder : request.responders) targets.push_back({{}, responder, {}, SaveExpiry(expiry), true});
                }
                else
                {
                    const auto& route = session.responseRoutes.at(message.correlationId);
                    expiry = route.expiry;
                    publication.correlationId = route.correlationId;
                    targets.push_back({{}, route.requester, {}, SaveExpiry(expiry), true});
                }
                Reason reason;
                if (expiry <= Clock::now()) reason = {MMQP_REASON_TIMEOUT, "Exchange expired during admission recovery"};
                else
                {
                    // Repeat the original inbox writes with the same identity.
                    // Current SID owners discover them through quorum polling;
                    // stale broker locations need not be restored or guessed.
                    cluster->Publish(session.identifier, message.packetId, publication, targets, SaveExpiry(expiry));
                }
                auto canonical = message;
                canonical.alias = 1;
                canonical.duplicate = false;
                session.received.Commit(message.packetId);
                session.receipts[message.packetId] = {Sha256(Codec::Encode(canonical, {true, UINT32_MAX})), reason};
                while (session.receipts.size() > options.maxInflight) session.receipts.erase(session.receipts.begin());
                if (message.type == MMQP_PUBREQ_TYPE_RES) session.responseRoutes.erase(message.correlationId);
                it = session.rpcAdmissions.erase(it);
                this->Changed(session);
            }
        }

        Reason RouteCluster(Session* publisher, PublishRequest publication, std::optional<ProfileEntry> registration,
            const ResponseRoute* response, uint16_t correlationId)
        {
            auto sender = publisher ? publisher->identifier : "status:" + cluster->Identifier();
            uint64_t sequence = publication.packetId;
            if (!publisher)
            {
                auto random = RandomBytes(8);
                Reader reader(random);
                sequence = reader.U64() | (1ULL << 63);
            }
            else if (!publication.qos)
            {
                if (publisher->originSequence >= (1ULL << 63) - 1) throw ProtocolError("Publication origin sequence exhausted");
                sequence = ++publisher->originSequence | (1ULL << 63);
            }
            auto expiry = Clock::now() + std::chrono::milliseconds(options.requestTimeout);
            if (registration && publication.type == MMQP_PUBREQ_TYPE_MSG)
                expiry = Clock::now() + std::chrono::seconds(DecodeInterval(registration->expiry));
            std::vector<ClusterProfile> targets;
            if (publication.type == MMQP_PUBREQ_TYPE_RES)
            {
                auto bytes = cluster->Read(this->RequestKey(response->requester, publication.correlationId));
                if (bytes.empty()) throw ProtocolError("Response has no pending request", MMQP_REASON_INVALID_CORRELATION);
                auto request = this->ReadRequest(bytes);
                if (request.requester != response->requester || request.correlationId != publication.correlationId
                    || request.topic != publication.topic || request.expiry <= Clock::now() || !request.responders.contains(sender))
                    throw ProtocolError("Response is not authorized by the pending request", MMQP_REASON_UNAUTHORIZED);
                expiry = request.expiry;
                targets.push_back({request.broker, request.requester, {}, SaveExpiry(expiry), true});
            }
            else
            {
                auto candidates = cluster->Resolve(publication.topic, publication.qos, publication.type == MMQP_PUBREQ_TYPE_REQ);
                std::map<std::string, ClusterProfile> selected;
                std::map<std::pair<std::string, std::string>, std::vector<ClusterProfile>> groups;
                for (const auto& candidate : candidates)
                {
                    auto pattern = ParseTopic(candidate.profile.topic, true);
                    if (pattern.group.empty()) selected.emplace(candidate.session, candidate);
                    else if (candidate.online) groups[{pattern.group, pattern.filter}].push_back(candidate);
                }
                for (auto& [group, members] : groups)
                {
                    auto& cursor = sharedCursor[group];
                    auto member = members[cursor++ % members.size()];
                    selected.emplace(member.session, std::move(member));
                }
                for (auto& [id, candidate] : selected) targets.push_back(std::move(candidate));
            }
            if (publication.type == MMQP_PUBREQ_TYPE_REQ)
            {
                auto maximum = publication.responses ? std::min<size_t>(publication.responses, options.maxResponders) : options.maxResponders;
                if (targets.size() > maximum) targets.resize(maximum);
                if (targets.empty())
                {
                    auto response = publication;
                    response.type = MMQP_PUBREQ_TYPE_RES;
                    response.payload.clear();
                    response.encryption.reset();
                    response.reason = {MMQP_REASON_NO_RESPONDER, "No eligible responder"};
                    this->Deliver(this->Prepare(*publisher, response, expiry));
                    return response.reason;
                }
                if (publication.qos) this->StageExchange(*publisher, publication);
                Request request;
                request.requester = sender;
                request.correlationId = publication.correlationId;
                request.topic = publication.topic;
                request.expiry = expiry;
                request.qos = publication.qos;
                request.broker = cluster->Identifier();
                for (const auto& target : targets) request.responders.insert(target.session);
                auto key = this->RequestKey(sender, publication.correlationId);
                if (!cluster->Acquire(key).empty())
                {
                    cluster->Release(key);
                    throw ProtocolError("Request correlation ID already exists", MMQP_REASON_INVALID_CORRELATION);
                }
                cluster->Commit(key, this->SaveRequest(request), SaveExpiry(request.expiry));
                publisher->requests.emplace(publication.correlationId, std::move(request));
                this->Changed(*publisher);
                // Recovery must know the request before any responder can see it.
                this->Checkpoint();
            }
            if (publication.type == MMQP_PUBREQ_TYPE_RES && publication.qos)
            {
                auto original = publication;
                original.correlationId = correlationId;
                this->StageExchange(*publisher, original);
                this->Checkpoint();
            }
            if (publication.type == MMQP_PUBREQ_TYPE_MSG && publication.qos && registration)
            {
                auto identity = MessageIdentity(sender, sequence, publication.topic, cluster->Placement().Options().epoch, publication.payload);
                cluster->Retain(publication, *registration, identity);
            }
            cluster->Publish(sender, sequence, publication, targets, SaveExpiry(expiry));
            ++metrics.publications;
            if (publisher) this->Changed(*publisher);
            return {targets.empty() ? MMQP_REASON_NOSUBSCRIBER : MMQP_REASON_SUCCESS, {}};
        }

        void DrainInbox(Session& session)
        {
            if (!cluster || session.peer.expired() || session.nextInbox > Clock::now() || !cluster->Owns(session.identifier)) return;
            session.nextInbox = Clock::now() + std::chrono::milliseconds(150);
            auto records = cluster->Inbox(session.identifier);
            std::erase_if(session.fabricReceipts, [] (const auto& entry) { return entry.second <= UnixMilliseconds(); });
            for (const auto& record : records)
            {
                if (!session.fabricReceipts.contains(record.identity))
                {
                    if (session.fabricReceipts.size() >= 8192) break;
                    const auto& message = record.publication;
                    this->AdmitTopic(message.topic, false, &session);
                    bool allowed = message.type == MMQP_PUBREQ_TYPE_RES ? this->AcceptResponse(session, message, record.sender)
                        : !session.profiles.Subscriptions(message.topic, message.qos, message.type == MMQP_PUBREQ_TYPE_REQ).empty();
                    if (allowed)
                    {
                        try { this->Deliver(this->Prepare(session, message, LoadExpiry(record.expiry), record.sender)); }
                        catch (const ProtocolError& error) { if (error.reason == MMQP_REASON_BUSY) break; throw; }
                        if (message.type == MMQP_PUBREQ_TYPE_RES) this->CompleteResponse(session, message.correlationId, record.sender);
                    }
                    session.fabricReceipts[record.identity] = record.expiry;
                    this->Changed(session);
                }
                effects.push_back([this, record]
                { try { cluster->Acknowledge(record); } catch (const ProtocolError&) {} });
            }
        }

        Reason Route(Session* publisher, PublishRequest publication, std::optional<ProfileEntry> registration = {})
        {
            const ResponseRoute* response = nullptr;
            auto correlationId = publication.correlationId;
            if (publication.type == MMQP_PUBREQ_TYPE_REQ)
            {
                if (!publisher) throw ProtocolError("Internal messages cannot issue requests");
                publisher->requestIds.Claim(publication.correlationId);
                this->Changed(*publisher);
                if (this->RequestCount() >= options.maxRequests)
                    throw ProtocolError("Outstanding request budget exhausted", MMQP_REASON_BUSY);
            }
            else if (publication.type == MMQP_PUBREQ_TYPE_RES)
            {
                if (!publisher) throw ProtocolError("Response is missing its responder");
                auto found = publisher->responseRoutes.find(publication.correlationId);
                if (found == publisher->responseRoutes.end() || found->second.expiry <= Clock::now()
                    || found->second.topic != publication.topic)
                    throw ProtocolError("Response has no matching correlation route", MMQP_REASON_INVALID_CORRELATION);
                response = &found->second;
                publication.correlationId = response->correlationId;
            }
            if (cluster) return this->RouteCluster(publisher, std::move(publication), registration, response, correlationId);
            auto now = Clock::now();
            auto expiry = now + std::chrono::milliseconds(options.requestTimeout);
            if (publication.type == MMQP_PUBREQ_TYPE_MSG && registration)
                expiry = now + std::chrono::seconds(DecodeInterval(registration->expiry));
            std::vector<Session*> targets;
            if (publication.type == MMQP_PUBREQ_TYPE_RES)
            {
                auto target = this->Find(response->requester);
                if (!target || !this->AcceptResponse(*target, publication, publisher->identifier))
                    throw ProtocolError("Response has no matching outstanding request", MMQP_REASON_INVALID_CORRELATION);
                targets.push_back(target);
                expiry = target->requests.at(publication.correlationId).expiry;
            }
            else targets = this->Destinations(publication, publication.type == MMQP_PUBREQ_TYPE_REQ);

            if (publication.type == MMQP_PUBREQ_TYPE_REQ)
            {
                auto count = publication.responses ? std::min<size_t>(publication.responses, options.maxResponders) : options.maxResponders;
                if (targets.size() > count) targets.resize(count);
                if (targets.empty())
                {
                    auto response = publication;
                    response.type = MMQP_PUBREQ_TYPE_RES;
                    response.reason = {MMQP_REASON_NO_RESPONDER, "No eligible responder"};
                    response.payload.clear();
                    response.encryption.reset();
                    this->Deliver(this->Prepare(*publisher, response, expiry));
                    return {MMQP_REASON_NO_RESPONDER, "No eligible responder"};
                }
            }
            std::vector<Dispatch> deliveries;
            for (auto target : targets)
            {
                try { deliveries.emplace_back(this->Prepare(*target, publication, expiry, publisher ? publisher->identifier : "")); }
                catch (const ProtocolError&)
                {
                    if (publication.qos) throw;
                    ++metrics.rejected;
                }
            }
            if (publication.type == MMQP_PUBREQ_TYPE_MSG && publication.qos && registration)
                this->Retain(publication, *registration);
            if (publication.type == MMQP_PUBREQ_TYPE_REQ)
            {
                Request request;
                request.requester = publisher->identifier;
                request.correlationId = publication.correlationId;
                request.topic = publication.topic;
                request.expiry = expiry;
                request.qos = publication.qos;
                for (const auto& dispatch : deliveries) request.responders.insert(dispatch.session->identifier);
                publisher->requests.emplace(publication.correlationId, std::move(request));
                this->Changed(*publisher);
            }
            for (auto& dispatch : deliveries) this->Deliver(std::move(dispatch));
            if (publication.type == MMQP_PUBREQ_TYPE_RES)
                this->CompleteResponse(*targets.front(), publication.correlationId, publisher->identifier);
            ++metrics.publications;
            owner.TryPost<PublicationRouted>(publication.topic.size(), publication.topic, deliveries.size(), publication.payload.size());
            return {deliveries.empty() ? MMQP_REASON_NOSUBSCRIBER : MMQP_REASON_SUCCESS, {}};
        }

        void Will(Session& session, bool birth)
        {
            if (!session.properties.will || stopping) return;
            const auto& will = *session.properties.will;
            const auto& payload = birth ? will.birth : will.last;
            if (!payload) return;
            PublishRequest message;
            message.topic = std::string(birth ? MMQP_BIRTH_STATUS_TOPIC_PREFIX : MMQP_LAST_STATUS_TOPIC_PREFIX)
                + (will.topic.empty() ? session.identifier : will.topic);
            message.alias = 1;
            message.qos = will.qos;
            message.packetId = will.qos ? 1 : 0;
            message.payload = *payload;
            try { this->Route(nullptr, std::move(message)); }
            catch (const ProtocolError&) { ++metrics.rejected; }
        }

        void Publish(const std::shared_ptr<Peer>& peer, Session& session, PublishRequest publication)
        {
            Digest digest{};
            if (publication.qos)
            {
                auto canonical = publication;
                if (canonical.topic.empty()) canonical.topic = session.profiles.publishers.Resolve(canonical.alias);
                canonical.alias = 1;
                canonical.duplicate = false;
                digest = Sha256(Codec::Encode(canonical, {true, UINT32_MAX}));
            }
            if (publication.qos && session.received.Inspect(publication.packetId) == Reception::OutsideWindow)
                throw ProtocolError("Publication exceeds the receive window", MMQP_REASON_INVALID_PID);
            if (publication.qos && session.received.Inspect(publication.packetId) == Reception::Duplicate)
            {
                auto found = session.receipts.find(publication.packetId);
                if (found == session.receipts.end())
                    throw ProtocolError("PID is older than the receipt window", MMQP_REASON_INVALID_PID);
                if (!SecureEqual(found->second.first, digest))
                    throw ProtocolError("Publication PID was reused for different content", MMQP_REASON_INVALID_PID);
                ++metrics.duplicates;
                this->Send(peer, PublishAck{publication.packetId, found->second.second});
                return;
            }
            Reason reason;
            auto packetId = publication.packetId;
            auto qos = publication.qos;
            auto type = publication.type;
            auto correlationId = publication.correlationId;
            try
            {
                bool mapping = !publication.topic.empty();
                if (!mapping) publication.topic = session.profiles.publishers.Resolve(publication.alias);
                this->AdmitTopic(publication.topic, false, &session);
                if (publication.topic.front() == '$')
                    throw ProtocolError("Reserved topics cannot be published by clients", MMQP_REASON_UNAUTHORIZED);
                auto registration = session.profiles.Registration(publication.topic, publication.type);
                if (!registration) throw ProtocolError("Publication has no authorized profile", MMQP_REASON_UNAUTHORIZED);
                session.profiles.publishers.Map(publication.alias, publication.topic, true);
                if (mapping) this->Send(peer, ProfileSync{publication.alias, publication.topic, {}});
                reason = this->Route(&session, std::move(publication), registration);
                if (reason.code < 0x80) ++session.publications;
                if (type == MMQP_PUBREQ_TYPE_RES && reason.code < 0x80)
                    session.responseRoutes.erase(correlationId);
            }
            catch (const ProtocolError& error)
            {
                ++metrics.rejected;
                if (!qos) throw;
                reason = {error.reason, error.what()};
            }
            if (qos)
            {
                session.rpcAdmissions.erase(packetId);
                session.received.Commit(packetId);
                session.receipts[packetId] = std::make_pair(digest, reason);
                while (session.receipts.size() > options.maxInflight) session.receipts.erase(session.receipts.begin());
                this->Send(peer, PublishAck{packetId, reason});
            }
        }

        void Profile(const std::shared_ptr<Peer>& peer, Session& session, const ProfileRequest& request)
        {
            auto digest = Sha256(Codec::Encode(request, peer->codec));
            if (auto previous = session.profileReceipts.find(request.packetId); previous != session.profileReceipts.end())
            {
                if (!SecureEqual(previous->second.first, digest))
                    throw ProtocolError("Profile PID was reused for different content", MMQP_REASON_INVALID_PID);
                this->Send(peer, ProfileAck{request.packetId, previous->second.second});
                return;
            }
            if (request.packetId <= session.lastProfile)
                throw ProtocolError("Profile PID is older than the receipt window", MMQP_REASON_INVALID_PID);
            Reason reason;
            try
            {
                this->AdmitProfiles(request, &session);
                session.profiles.Apply(request, options.maxProfiles);
            }
            catch (const ProtocolError& error) { reason = {error.reason, error.what()}; }
            session.profileReceipts.emplace(request.packetId, std::make_pair(digest, reason));
            session.lastProfile = request.packetId;
            this->Changed(session, true);
            this->PruneShared();
            while (session.profileReceipts.size() > options.maxInflight) session.profileReceipts.erase(session.profileReceipts.begin());
            this->Send(peer, ProfileAck{request.packetId, reason});
            if (reason.code < 0x80) this->Replay(session, request);
        }

        void Receive(uint64_t id, std::span<const uint8_t> bytes)
        {
            transaction = true;
            try
            {
                this->ReceivePacket(id, bytes);
                this->Finish();
            }
            catch (const std::exception& error)
            {
                transaction = false;
                effects.clear();
                effectBytes = 0;
                EMPY_ERROR("Broker checkpoint failed: {}", error.what());
                if (auto peer = peers.find(id); peer != peers.end()) this->Fail(peer->second, {MMQP_REASON_BUSY, "Durable majority unavailable"});
            }
        }

        void ReceivePacket(uint64_t id, std::span<const uint8_t> bytes)
        {
            auto found = peers.find(id);
            if (found == peers.end()) return;
            auto peer = found->second;
            ++metrics.receivedPackets;
            metrics.receivedBytes += bytes.size();
            if (auto session = this->Find(peer->session))
            {
                ++session->traffic.receivedPackets;
                session->traffic.receivedBytes += bytes.size();
            }
            try
            {
                auto message = Codec::Decode(bytes, peer->codec);
                if (peer->phase == Phase::Request)
                {
                    auto request = std::get_if<ConnectionRequest>(&message);
                    if (!request) throw ProtocolError("CONREQ must be the first packet");
                    this->Connect(peer, std::move(*request));
                    return;
                }
                if (peer->phase == Phase::Challenge)
                {
                    auto sync = std::get_if<ConnectionSync>(&message);
                    if (!sync || !sync->token || !SecureEqual(*sync->token, peer->expected))
                        throw ProtocolError("Authentication proof failed", MMQP_REASON_AUTH_FAILED);
                    SecureErase(peer->expected);
                    peer->expected.clear();
                    this->Establish(peer, true);
                    return;
                }
                if (peer->phase != Phase::Active) return;
                auto session = this->Find(peer->session);
                if (!session) throw ProtocolError("Missing active session", MMQP_REASON_SESSION_EXPIRED);
                if (cluster && !cluster->Owns(session->identifier)) throw ProtocolError("Session primary lost its majority", MMQP_REASON_BUSY);
                this->Changed(*session);
                session->activity = Clock::now();
                std::visit([&] (auto& value)
                {
                    using Type = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<Type, PublishRequest>) this->Publish(peer, *session, std::move(value));
                    else if constexpr (std::is_same_v<Type, ProfileRequest>) this->Profile(peer, *session, value);
                    else if constexpr (std::is_same_v<Type, ProfileSync>)
                    {
                        if (value.reason.code >= 0x80) throw ProtocolError("Peer rejected alias mapping", value.reason.code);
                        session->profiles.subscribers.Confirm(value.alias, value.topic);
                    }
                    else if constexpr (std::is_same_v<Type, PublishAck>)
                    {
                        if (!session->pending.Remove(value.packetId) && value.packetId > session->sequence.Value())
                            throw ProtocolError("Acknowledgement references an unsent PID", MMQP_REASON_INVALID_PID);
                        this->DrainReplay(*session);
                    }
                    else if constexpr (std::is_same_v<Type, ConnectionSync>)
                    {
                        if (value.token || !session->ping) throw ProtocolError("Unexpected session synchronization");
                        session->ping = false;
                        dirtyProfiles.insert(session->identifier);
                    }
                    else if constexpr (std::is_same_v<Type, ConnectionEnd>)
                    {
                        peer->phase = Phase::Closing;
                        this->Close(peer, true);
                    }
                    else throw ProtocolError("Packet is invalid in the active broker state");
                }, message);
            }
            catch (const ProtocolError& error)
            {
                this->Fail(peer, {error.reason, error.what()}, peer->session.empty());
            }
            catch (const std::exception& error)
            {
                EMPY_ERROR("Broker processing failed: {}", error.what());
                this->Fail(peer, {MMQP_REASON_INTERNAL_ERROR, "Internal processing failure"}, peer->session.empty());
            }
        }

        void Closed(uint64_t id, const std::string& reason)
        {
            auto found = peers.find(id);
            if (found == peers.end()) return;
            auto peer = found->second;
            transaction = true;
            peer->phase = Phase::Closing;
            if (auto session = this->Find(peer->session))
            {
                session->peer.reset();
                session->ping = false;
                session->expiry = session->resumable
                    ? Clock::now() + std::chrono::seconds(DecodeInterval(*session->properties.policy->expiry)) : Clock::now();
                if (!cluster || cluster->Owns(session->identifier))
                {
                    this->Changed(*session, true);
                    this->Will(*session, false);
                    if (cluster) effects.push_back([this, id = session->identifier] { cluster->Release(id); });
                }
                owner.TryPost<SessionDisconnected>(session->identifier.size() + reason.size(), session->identifier, reason);
            }
            SecureErase(peer->expected);
            peers.erase(found);
            try { this->Finish(); }
            catch (...)
            {
                transaction = false;
                effects.clear();
                effectBytes = 0;
            }
        }

        void Tick()
        {
            if (stopping) return;
            timer.expires_after(std::chrono::milliseconds(50));
            timer.async_wait([this] (auto error)
            {
                if (!error)
                {
                    try { this->Tick(); }
                    catch (const std::exception& failure)
                    {
                        transaction = false;
                        effects.clear();
                        effectBytes = 0;
                        EMPY_ERROR("Broker timer failed: {}", failure.what());
                    }
                }
            });
            transaction = true;
            auto now = Clock::now();
            std::vector<std::shared_ptr<Peer>> timedOut;
            for (const auto& [id, peer] : peers)
                if (peer->phase != Phase::Active && peer->phase != Phase::Closing && peer->deadline <= now)
                    timedOut.push_back(peer);
            for (const auto& peer : timedOut) this->Fail(peer, {MMQP_REASON_TIMEOUT, "Handshake timeout"}, true);
            for (auto it = sessions.begin(); it != sessions.end();)
            {
                auto& session = registry.get<Session>(it->second);
                if (cluster && !session.peer.expired() && !cluster->Owns(session.identifier))
                {
                    this->Fail(session.peer.lock(), {MMQP_REASON_BUSY, "Session primary is fenced"});
                    ++it;
                    continue;
                }
                if (session.expiry <= now)
                {
                    for (auto& [id, request] : session.requests)
                    {
                        request.responders.clear();
                        this->UpdateRequest(request);
                    }
                    registry.destroy(it->second);
                    it = sessions.erase(it);
                    this->PruneShared();
                    stateDirty = true;
                    continue;
                }
                ++it;
                if (std::erase_if(session.responseRoutes, [&] (const auto& value)
                    { return value.second.expiry <= now && !this->AdmissionPending(session, MMQP_PUBREQ_TYPE_RES, value.first); }))
                    this->Changed(session);
                if ((storage || cluster) && !session.peer.expired() && session.nextCheckpoint <= now)
                {
                    auto interval = std::min<uint32_t>(30000, DecodeInterval(*session.properties.policy->expiry) * 1000U / 3);
                    session.nextCheckpoint = now + std::chrono::milliseconds(interval);
                    this->Changed(session, true);
                }
                this->DrainReplay(session);
                this->DrainInbox(session);
                std::vector<uint16_t> expired;
                for (const auto& [packetId, pending] : session.pending.entries)
                    if (pending.expiry <= now) expired.push_back(packetId);
                for (auto packetId : expired) session.pending.Remove(packetId);
                if (!expired.empty()) this->Changed(session);
                if (!expired.empty() && session.properties.quic)
                {
                    // An expired hole cannot be skipped by the unordered checkpoint on the wire.
                    session.resumable = false;
                    if (auto peer = session.peer.lock()) this->Fail(peer, {MMQP_REASON_TIMEOUT, "Reliable publication expired"});
                    else session.expiry = now;
                    continue;
                }
                if (auto peer = session.peer.lock())
                {
                    auto keepalive = std::chrono::seconds(DecodeInterval(*session.properties.policy->keepalive));
                    if (session.ping && session.pingDeadline <= now)
                    {
                        this->Fail(peer, {MMQP_REASON_KEEPALIVE_TIMEOUT, "Keepalive response timed out"});
                        continue;
                    }
                    if (!session.ping && session.activity + keepalive <= now)
                    {
                        session.ping = true;
                        session.pingDeadline = now + keepalive;
                        this->Send(peer, ConnectionSync{});
                    }
                    bool failed = false;
                    for (auto& [packetId, pending] : session.pending.entries)
                    {
                        if (pending.retry > now) continue;
                        if (pending.attempts >= options.maxRetries)
                        {
                            failed = true;
                            break;
                        }
                        auto message = pending.message;
                        message.duplicate = true;
                        if (!this->Send(peer, message)) { failed = true; break; }
                        pending.retry = now + std::chrono::milliseconds(options.retryInterval);
                        ++pending.attempts;
                        ++metrics.retransmitted;
                    }
                    if (failed) this->Fail(peer, {MMQP_REASON_TIMEOUT, "Reliable delivery timed out"});
                }
            }
            std::erase_if(authTokens, [&] (const auto& value) { return value.second <= now; });
            this->PurgeRetained(now);
            for (const auto& [id, entity] : sessions)
            {
                if (cluster && !cluster->Owns(id)) continue;
                auto& session = registry.get<Session>(entity);
                for (auto it = session.requests.begin(); it != session.requests.end();)
                {
                    if (it->second.expiry > now || this->AdmissionPending(session, MMQP_PUBREQ_TYPE_REQ, it->first)) { ++it; continue; }
                    if (!it->second.responders.empty())
                    {
                        it->second.responders.clear();
                        this->UpdateRequest(it->second);
                        this->Changed(session);
                    }
                    PublishRequest response;
                    response.topic = it->second.topic;
                    response.type = MMQP_PUBREQ_TYPE_RES;
                    response.correlationId = it->first;
                    response.qos = it->second.qos;
                    response.reason = {MMQP_REASON_TIMEOUT, "Response timeout"};
                    try { this->Deliver(this->Prepare(session, response, now + std::chrono::seconds(5))); }
                    catch (const ProtocolError& error)
                    {
                        ++metrics.rejected;
                        // Keep the timed-out exchange until its result can be
                        // queued. Another request must not consume its identity.
                        if (error.reason == MMQP_REASON_BUSY) { ++it; continue; }
                    }
                    it = session.requests.erase(it);
                    this->Changed(session);
                }
            }
            this->Finish();
        }
    };

    Broker::Broker(BrokerOptions options): m_Impl(std::make_unique<Impl>(*this, std::move(options))) {}
    Broker::Broker(const char* filename): Broker(LoadBrokerOptions(filename)) {}
    Broker::~Broker() { this->WaitTasks(); this->Stop(); }

    uint16_t Broker::Listen()
    {
        if (m_Impl->started || m_Impl->stopping) throw std::logic_error("Broker already started or stopped");
        if (m_Impl->cluster) m_Impl->cluster->Start();
        m_Impl->port = m_Impl->io.Listen(m_Impl->options.address, m_Impl->options.port, m_Impl->options.transport,
            [this] (auto connection) { m_Impl->Accepted(std::move(connection)); }, m_Impl->options.maxSessions);
        m_Impl->started = true;
        m_Impl->startedAt = Clock::now();
        m_Impl->io.Post([this] { m_Impl->Tick(); });
        m_Impl->io.Init(m_Impl->options.workers);
        return m_Impl->port;
    }

    uint16_t Broker::Port() const { return m_Impl->port; }

    BrokerMetrics Broker::Metrics()
    {
        auto collect = [this]
        {
            auto metrics = m_Impl->metrics;
            metrics.sessions = m_Impl->sessions.size();
            metrics.online = 0;
            metrics.pending = 0;
            for (const auto& [id, entity] : m_Impl->sessions)
            {
                const auto& session = m_Impl->registry.get<Impl::Session>(entity);
                if (!session.peer.expired()) ++metrics.online;
                metrics.pending += session.pending.entries.size();
            }
            return metrics;
        };
        return m_Impl->started && !m_Impl->stopping ? m_Impl->io.Submit(collect).get() : collect();
    }

    BrokerSnapshot Broker::Inspect(size_t maxSessions)
    {
        if (!maxSessions || maxSessions > 4096) throw std::invalid_argument("Snapshot session limit must be in 1..4096");
        auto collect = [this, maxSessions]
        {
            BrokerSnapshot result;
            const auto& options = m_Impl->options;
            auto now = Clock::now();
            auto milliseconds = [] (auto duration) -> uint64_t
            {
                return std::max<int64_t>(0, std::chrono::duration_cast<std::chrono::milliseconds>(duration).count());
            };
            result.identifier = m_Impl->cluster ? m_Impl->cluster->Identifier()
                : options.address + ':' + std::to_string(m_Impl->port);
            result.instance = m_Impl->instance;
            result.address = options.address;
            result.port = m_Impl->port;
            result.transport = options.transport.quic ? "QUIC" : options.transport.tls ? "TLS" : "TCP";
            result.collectedAt = UnixMilliseconds();
            result.uptimeMs = m_Impl->startedAt == TimePoint{} ? 0 : milliseconds(now - m_Impl->startedAt);
            result.durable = m_Impl->cluster || m_Impl->storage;
            result.authenticationRequired = options.requireAuth;
            result.metrics = m_Impl->metrics;
            result.metrics.sessions = m_Impl->sessions.size();
            result.maxSessions = options.maxSessions;
            result.maxInflight = options.maxInflight;
            result.maxRetainedBytes = options.maxRetainedBytes;
            result.maxStateBytes = options.cluster ? options.cluster->maxBytes : options.maxStateBytes;
            if (m_Impl->storage) result.storedBytes = m_Impl->storage->Bytes();
            std::vector<std::string> identifiers;
            for (const auto& [id, entity] : m_Impl->sessions)
            {
                const auto& session = m_Impl->registry.get<Impl::Session>(entity);
                auto peer = session.peer.lock();
                if (peer) ++result.metrics.online;
                result.metrics.pending += session.pending.entries.size();
                if (result.sessions.size() == maxSessions) { ++result.omittedSessions; continue; }
                SessionSnapshot value;
                static_cast<TrafficMetrics&>(value) = session.traffic;
                value.identifier = id;
                value.realm = session.realm;
                value.transport = peer ? (peer->connection->Quic() ? "QUIC" : peer->connection->Secure() ? "TLS" : "TCP") : "";
                value.online = peer != nullptr;
                value.authenticated = session.authenticated;
                value.resumable = session.resumable;
                value.owner = !m_Impl->cluster;
                value.observedAt = session.observedAt;
                value.publications = session.publications;
                value.deliveries = session.deliveries;
                if (session.activity != TimePoint{}) value.idleMs = milliseconds(now - session.activity);
                if (!peer) value.expiresInMs = milliseconds(session.expiry - now);
                if (session.properties.policy && session.properties.policy->keepalive)
                    value.keepaliveSeconds = DecodeInterval(*session.properties.policy->keepalive);
                for (const auto& profile : session.profiles.entries)
                {
                    if (profile.type == MMQP_PROREQ_TYPE_SUB)
                    {
                        ++value.subscriptions;
                        if (profile.request) ++value.responders;
                    }
                    else if (profile.type == MMQP_PROREQ_TYPE_REG)
                    {
                        ++value.registrations;
                        if (profile.request) ++value.requesters;
                    }
                }
                value.pending = session.pending.entries.size();
                value.pendingBytes = session.pending.Bytes();
                value.queuedBytes = peer ? peer->connection->Queued() : 0;
                value.replay = session.replay.size();
                value.replayBytes = session.replayBytes;
                value.requests = session.requests.size();
                value.responseRoutes = session.responseRoutes.size();
                identifiers.push_back(id);
                result.sessions.push_back(std::move(value));
            }
            if (m_Impl->cluster)
            {
                result.cluster = m_Impl->cluster->Inspect(identifiers);
                result.storedBytes = result.cluster->storedBytes;
                for (auto& session : result.sessions)
                    if (auto found = result.cluster->ownership.find(session.identifier); found != result.cluster->ownership.end())
                    {
                        session.owner = true;
                        session.lease = found->second;
                    }
            }
            return result;
        };
        return m_Impl->started && !m_Impl->stopping ? m_Impl->io.Submit(collect).get() : collect();
    }

    void Broker::Stop()
    {
        if (m_Impl->stopping) return;
        if (m_Impl->started)
        {
            m_Impl->io.Submit([this]
            {
                m_Impl->stopping = true;
                m_Impl->timer.cancel();
            }).get();
        }
        else m_Impl->stopping = true;
        m_Impl->io.Stop();
        if (m_Impl->cluster) m_Impl->cluster->Stop();
        this->Terminate();
    }
}
