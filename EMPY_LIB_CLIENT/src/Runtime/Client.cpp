#include <eClient/Runtime/Client.h>
#include <eMQ/Common/State.h>
#include <eMQ/Network/IOContext.h>
#include <deque>

namespace eMQ
{
    struct Client::Impl
    {
        enum class Phase { Offline, Transport, Challenge, Ack, Active };
        struct ProfileTask
        {
            ProfileRequest request;
            ProfileTable accepted;
            std::shared_ptr<std::promise<Reason>> result;
            TimePoint deadline;
            TimePoint retry;
            uint32_t attempts = 0;
        };

        Client& owner;
        ClientOptions options;
        IoContext io;
        asio::steady_timer timer;
        Connection::Pointer connection;
        Phase phase = Phase::Offline;
        CodecOptions codec;
        ProfileTable profiles;
        PacketSequence publications;
        PacketSequence profileIds;
        CorrelationIds requestIds;
        ReceiveWindow received;
        PendingQueue pending;
        std::map<uint16_t, std::shared_ptr<std::promise<Reason>>> results;
        std::deque<ProfileTask> profileTasks;
        std::shared_ptr<std::promise<std::string>> connecting;
        std::shared_ptr<std::promise<void>> disconnecting;
        std::string identifier;
        Buffer expected;
        std::optional<QuicPolicy> negotiated;
        TimePoint deadline;
        Reason closeReason{MMQP_REASON_UNSPECIFIED, "Transport disconnected"};
        std::atomic_bool active = false;
        bool stopping = false;
        bool resume = false;
        bool resumable = true;
        bool manualDisconnect = false;
        TimePoint reconnectAt{};
        size_t reconnectIndex = 0;

        Impl(Client& service, ClientOptions settings):
            owner(service), options(std::move(settings)), timer(io.Executor())
        {
            if (!options.maxInflight || options.maxInflight > 256 || !options.maxProfiles || !options.maxPendingBytes
                || !options.retryInterval || !options.operationTimeout || !options.maxRetries
                || options.transport.maxPacket < 64 || options.transport.maxQueue < options.transport.maxPacket)
                throw std::invalid_argument("Invalid client resource bounds");
            if (!options.authKey.empty() && options.authKey.size() < 32)
                throw std::invalid_argument("Authentication key must contain at least 32 bytes");
            if (options.autoReconnect && (options.failoverEndpoints.empty() || !options.reconnectInterval))
                throw std::invalid_argument("Automatic reconnect needs endpoints and a positive retry interval");
            for (const auto& endpoint : options.failoverEndpoints)
                if (endpoint.host.empty() || !endpoint.port) throw std::invalid_argument("Invalid failover endpoint");
            if (!options.properties.policy)
                options.properties.policy = SessionPolicy{true, EncodeInterval(300), EncodeInterval(30), options.transport.maxPacket};
            if (options.transport.quic)
            {
                if (!options.properties.quic)
                    options.properties.quic = QuicPolicy{MMQP_QUIC_STRATEGY_SEMANTIC, {}, {}};
                options.transport.quicPolicy = QuicPlan(*options.properties.quic).Policy();
                options.properties.quic = options.transport.quicPolicy;
            }
            else if (options.properties.quic) throw std::invalid_argument("QUIC properties require a QUIC transport");
            options.properties.policy->maxPacket = std::min(options.transport.maxPacket,
                options.properties.policy->maxPacket.value_or(options.transport.maxPacket));
            codec = {options.properties.policy->reasons, *options.properties.policy->maxPacket,
                static_cast<uint32_t>(options.maxProfiles)};
            pending.maxCount = options.maxInflight;
            pending.maxBytes = options.maxPendingBytes;
        }

        ~Impl() { SecureErase(options.authKey); SecureErase(expected); }

        bool Write(const Message& message)
        {
            return connection && connection->Send(Codec::Encode(message, codec));
        }

        void Closed(const std::string& text)
        {
            active = false;
            phase = Phase::Offline;
            connection.reset();
            auto failure = std::make_exception_ptr(std::runtime_error(text));
            if (connecting) { connecting->set_exception(failure); connecting.reset(); }
            if (disconnecting) { disconnecting->set_value(); disconnecting.reset(); }
            bool recover = options.autoReconnect && !manualDisconnect && !stopping && resumable && !identifier.empty();
            if (!recover)
            {
                for (auto& [id, result] : results) result->set_exception(failure);
                results.clear();
                if (!profileTasks.empty()) resumable = false;
                for (auto& task : profileTasks) task.result->set_exception(failure);
                profileTasks.clear();
            }
            reconnectAt = Clock::now() + std::chrono::milliseconds(options.reconnectInterval);
            SecureErase(expected);
            expected.clear();
            if (closeReason.code == MMQP_REASON_UNSPECIFIED) closeReason.text = text;
            owner.TryPost<eMQ::Disconnect>(closeReason.text.size(), closeReason, recover);
        }

        void Fail(const ProtocolError& error)
        {
            closeReason = {error.reason, error.what()};
            if (!(options.autoReconnect && phase != Phase::Active
                && (error.reason == MMQP_REASON_BUSY || error.reason == MMQP_REASON_TIMEOUT))) resumable = false;
            if (connection)
            {
                try { this->Write(ConnectionEnd{closeReason}); }
                catch (...) {}
                connection->Close(true);
            }
            else this->Closed(error.what());
        }

        void Ready(Connection::Pointer transport)
        {
            connection = std::move(transport);
            connection->onReceive = [this] (auto bytes) { this->Receive(bytes); };
            ConnectionRequest request;
            request.properties = options.properties;
            request.properties.auth.reset();
            request.resume = resume;
            if (resume)
            {
                request.properties.profile.reset();
                request.properties.will.reset();
                request.properties.auth = SessionAuth{identifier, {}};
            }
            if (!options.authKey.empty())
            {
                if (!connection->Secure() && !options.allowInsecureAuth)
                    throw ProtocolError("Authentication requires secure transport", MMQP_REASON_AUTH_FAILED);
                if (!request.properties.auth) request.properties.auth = SessionAuth{};
                request.properties.auth->token = RandomBytes(32);
                if (!options.authIdentity.empty())
                {
                    auto prefix = AuthenticationIdentity(options.authIdentity);
                    std::copy(prefix.begin(), prefix.end(), request.properties.auth->token.begin());
                }
                auto proof = Hmac256(options.authKey, request.properties.auth->token);
                expected.assign(proof.begin(), proof.end());
                SecureErase(proof);
                phase = Phase::Challenge;
            }
            else phase = Phase::Ack;
            if (!this->Write(request)) throw ProtocolError("Connection request exceeds send budget", MMQP_REASON_BUSY);
        }

        void Begin(std::string host, uint16_t port, bool restore, std::shared_ptr<std::promise<std::string>> result)
        {
            if (phase != Phase::Offline || stopping) throw std::logic_error("Client is already connecting, connected or stopped");
            if (restore && (identifier.empty() || !resumable))
                throw std::logic_error("No safely resumable session is available");
            if (restore)
                for (const auto& [id, item] : pending.entries)
                    if (item.expiry <= Clock::now())
                        throw std::logic_error("A reliable publication expired; establish a fresh session");
            resume = restore;
            manualDisconnect = false;
            if (!resume)
            {
                identifier.clear();
                profiles = {};
                publications = PacketSequence{};
                requestIds = CorrelationIds{};
                profileIds = PacketSequence(options.properties.profile ? options.properties.profile->packetId : 0);
                received = ReceiveWindow(!options.transport.quic);
                negotiated.reset();
                pending = PendingQueue{};
                pending.maxCount = options.maxInflight;
                pending.maxBytes = options.maxPendingBytes;
                resumable = true;
            }
            codec = {options.properties.policy->reasons, *options.properties.policy->maxPacket,
                static_cast<uint32_t>(options.maxProfiles)};
            closeReason = {MMQP_REASON_UNSPECIFIED, "Transport disconnected"};
            connecting = std::move(result);
            deadline = Clock::now() + std::chrono::milliseconds(options.transport.timeout);
            phase = Phase::Transport;
            try
            {
                connection = io.Connect(std::move(host), port, options.transport,
                    [this] (auto transport) { this->Ready(std::move(transport)); },
                    [this] (const auto& text) { this->Closed(text); });
            }
            catch (...)
            {
                phase = Phase::Offline;
                connecting.reset();
                throw;
            }
        }

        void Connected(const ConnectionAck& ack)
        {
            if (ack.reason.code >= 0x80)
                throw ProtocolError(ack.reason.text.empty() ? "Connection rejected" : ack.reason.text.c_str(), ack.reason.code);
            if (phase != Phase::Ack) throw ProtocolError("Missing broker authentication proof", MMQP_REASON_AUTH_FAILED);
            if (!ack.properties.auth || ack.properties.auth->identifier.empty() || !ack.properties.policy
                || !ack.properties.policy->maxPacket || !ack.properties.policy->keepalive || !ack.properties.policy->expiry
                || ack.properties.quic.has_value() != connection->Quic())
                throw ProtocolError("Incomplete or unsupported negotiated session properties");
            if (resume && negotiated != ack.properties.quic)
                throw ProtocolError("Broker changed the resumed QUIC policy");
            if (ack.properties.quic)
            {
                auto allowed = QuicPlan::Negotiate(*ack.properties.quic, options.transport.quicPolicy);
                if (allowed != *ack.properties.quic) throw ProtocolError("Broker exceeded the requested QUIC resources");
            }
            if (resume && ack.properties.auth->identifier != identifier)
                throw ProtocolError("Broker changed the resumed session identity", MMQP_REASON_AUTH_FAILED);
            identifier = ack.properties.auth->identifier;
            codec.reasons = ack.properties.policy->reasons;
            codec.maxPacket = std::min(codec.maxPacket, *ack.properties.policy->maxPacket);
            connection->SetLimit(codec.maxPacket);
            if (!resume && options.properties.profile) profiles.Apply(*options.properties.profile, options.maxProfiles);
            phase = Phase::Active;
            active = true;
            negotiated = ack.properties.quic;
            if (negotiated) connection->Configure(*negotiated, codec.reasons);
            for (auto& [id, publication] : pending.entries)
            {
                publication.retry = Clock::now();
                publication.attempts = 0;
            }
            auto result = std::move(connecting);
            for (auto& task : profileTasks)
            {
                task.deadline = Clock::now() + std::chrono::milliseconds(options.operationTimeout);
                task.retry = Clock::now();
                task.attempts = 0;
            }
            this->PumpProfile();
            result->set_value(identifier);
            owner.TryPost<eMQ::Connect>(identifier.size(), identifier, resume);
        }

        void PumpProfile()
        {
            while (!profileTasks.empty() && phase == Phase::Active && !profileTasks.front().request.packetId)
            {
                auto& task = profileTasks.front();
                try
                {
                    task.accepted = profiles;
                    for (auto& entry : task.request.entries)
                    {
                        if (!entry.topic.empty())
                        {
                            entry.topic = CanonicalTopic(entry.topic);
                            if (!entry.alias && !HasWildcard(entry.topic))
                            {
                                auto& aliases = entry.type == MMQP_PROREQ_TYPE_REG
                                    ? task.accepted.publishers : task.accepted.subscribers;
                                if (entry.type == MMQP_PROREQ_TYPE_UNS)
                                {
                                    auto alias = aliases.Find(entry.topic);
                                    if (!alias) throw ProtocolError("No subscription alias to remove", MMQP_REASON_INVALID_ALIAS);
                                    entry.alias = *alias;
                                }
                                else entry.alias = aliases.Assign(entry.topic, 1,
                                    entry.type == MMQP_PROREQ_TYPE_SUB ? 0x7FFF : UINT16_MAX);
                            }
                        }
                    }
                    task.request.packetId = profileIds.Next();
                    task.accepted.Apply(task.request, options.maxProfiles);
                    // Reserve outgoing choices while the profile is in flight; live alias maps
                    // must also retain mappings learned from interleaved publications.
                    for (const auto& entry : task.request.entries)
                    {
                        if (!entry.alias || entry.type == MMQP_PROREQ_TYPE_UNS || entry.topic.empty()) continue;
                        auto& aliases = entry.type == MMQP_PROREQ_TYPE_REG ? profiles.publishers : profiles.subscribers;
                        aliases.Map(entry.alias, entry.topic);
                    }
                    task.deadline = Clock::now() + std::chrono::milliseconds(options.operationTimeout);
                    task.retry = Clock::now() + std::chrono::milliseconds(options.retryInterval);
                    if (!this->Write(task.request)) throw ProtocolError("Profile send budget exhausted", MMQP_REASON_BUSY);
                }
                catch (...)
                {
                    task.result->set_exception(std::current_exception());
                    profileTasks.pop_front();
                }
            }
        }

        void ProfileAcked(const ProfileAck& ack)
        {
            if (profileTasks.empty() || profileTasks.front().request.packetId != ack.packetId)
            {
                if (ack.packetId <= profileIds.Value()) return;
                throw ProtocolError("Unknown profile acknowledgement", MMQP_REASON_INVALID_PID);
            }
            auto task = std::move(profileTasks.front());
            profileTasks.pop_front();
            if (ack.reason.code < 0x80) profiles.Apply(task.request, options.maxProfiles);
            task.result->set_value(ack.reason);
            for (const auto& entry : task.request.entries)
                owner.TryPost<eMQ::Profile>(entry.topic.size() + ack.reason.text.size(), entry.topic, ack.packetId, ack.reason);
            this->PumpProfile();
        }

        void Publish(PublishRequest publication, const std::shared_ptr<std::promise<Reason>>& result)
        {
            if (phase != Phase::Active) throw std::logic_error("Client is not connected");
            publication.topic = ParseTopic(publication.topic).filter;
            if (!profiles.Registration(publication.topic, publication.type))
                throw ProtocolError("Register the topic or responder profile before publishing", MMQP_REASON_UNAUTHORIZED);
            auto topic = publication.topic;
            publication.alias = profiles.publishers.Candidate(topic);
            publication.packetId = publication.qos ? publications.Peek() : 0;
            publication.duplicate = false;
            if (publication.type == MMQP_PUBREQ_TYPE_REQ)
            {
                if (!publication.correlationId) publication.correlationId = requestIds.Peek();
                if (requestIds.Contains(publication.correlationId))
                    throw ProtocolError("Correlation ID already used in this session", MMQP_REASON_INVALID_CORRELATION);
            }
            if (options.cipher) options.cipher->Encrypt(publication, options.keyEpoch);
            if (profiles.publishers.Confirmed(publication.alias)) publication.topic.clear();
            auto bytes = Codec::Encode(publication, codec);
            if (bytes.size() > connection->Capacity() - connection->Queued()
                || (publication.qos && !pending.CanAdd(bytes.size(), publication.packetId)))
                throw ProtocolError("Client send budget exhausted", MMQP_REASON_BUSY);
            auto packetId = publication.packetId;
            auto size = bytes.size();
            if (publication.type == MMQP_PUBREQ_TYPE_REQ) requestIds.Claim(publication.correlationId);
            if (!connection->Send(std::move(bytes)))
                throw ProtocolError("Transport rejected publication", MMQP_REASON_BUSY);
            profiles.publishers.Map(publication.alias, topic);
            if (publication.qos)
            {
                publications.Next();
                pending.Add(publication, size, Clock::now() + std::chrono::milliseconds(options.retryInterval),
                    Clock::now() + std::chrono::milliseconds(options.operationTimeout));
                results.emplace(packetId, result);
            }
            if (!publication.qos) result->set_value({});
        }

        void Publication(PublishRequest publication)
        {
            if (publication.qos && received.Inspect(publication.packetId) == Reception::OutsideWindow)
                throw ProtocolError("Publication exceeds the receive window", MMQP_REASON_INVALID_PID);
            if (publication.qos && received.Inspect(publication.packetId) == Reception::Duplicate)
            {
                this->Write(PublishAck{publication.packetId, {}});
                return;
            }
            bool mapping = !publication.topic.empty();
            if (mapping) profiles.subscribers.Map(publication.alias, publication.topic, true);
            else publication.topic = profiles.subscribers.Resolve(publication.alias);
            bool authorized = publication.type == MMQP_PUBREQ_TYPE_RES
                ? profiles.Registration(publication.topic, MMQP_PUBREQ_TYPE_REQ).has_value()
                : !profiles.Subscriptions(publication.topic, publication.qos, publication.type == MMQP_PUBREQ_TYPE_REQ).empty();
            if (!authorized) throw ProtocolError("Received publication is outside the client profile", MMQP_REASON_UNAUTHORIZED);
            if (publication.type == MMQP_PUBREQ_TYPE_RES && !requestIds.Contains(publication.correlationId))
                throw ProtocolError("Response names an unknown request", MMQP_REASON_INVALID_CORRELATION);
            bool decrypted = false;
            if (publication.encryption)
            {
                if (!options.cipher) throw ProtocolError("No key provider for encrypted publication", MMQP_REASON_AUTH_FAILED);
                try { publication.payload = options.cipher->Decrypt(publication, publication.topic); }
                catch (const PayloadReplay&)
                {
                    // Retention can redeliver an authenticated envelope under a new broker PID.
                    if (mapping) this->Write(ProfileSync{publication.alias, publication.topic, {}});
                    if (publication.qos)
                    {
                        received.Commit(publication.packetId);
                        this->Write(PublishAck{publication.packetId, {}});
                    }
                    return;
                }
                decrypted = true;
            }
            else if (options.cipher && options.requireEncryption
                && !(publication.type == MMQP_PUBREQ_TYPE_RES && publication.reason.code >= 0x80 && publication.payload.empty()))
                throw ProtocolError("Unencrypted application publication rejected", MMQP_REASON_AUTH_FAILED);
            if (mapping) this->Write(ProfileSync{publication.alias, publication.topic, {}});
            auto packetId = publication.packetId;
            auto qos = publication.qos;
            auto bytes = publication.topic.size() + publication.payload.size();
            if (!owner.TryPost<eMQ::Receive>(bytes, std::move(publication), decrypted))
                throw ProtocolError("Application receive queue is full; poll events before accepting more data", MMQP_REASON_BUSY);
            if (qos)
            {
                received.Commit(packetId);
                this->Write(PublishAck{packetId, {}});
            }
        }

        void Receive(std::span<const uint8_t> bytes)
        {
            try
            {
                auto message = Codec::Decode(bytes, codec);
                if (auto ack = std::get_if<ConnectionAck>(&message); ack && phase != Phase::Active)
                {
                    this->Connected(*ack);
                    return;
                }
                if (phase == Phase::Challenge)
                {
                    auto sync = std::get_if<ConnectionSync>(&message);
                    if (!sync || !sync->token || !SecureEqual(expected, *sync->token))
                        throw ProtocolError("Broker authentication failed", MMQP_REASON_AUTH_FAILED);
                    auto response = Hmac256(options.authKey, *sync->token);
                    SecureErase(expected);
                    expected.clear();
                    phase = Phase::Ack;
                    if (!this->Write(ConnectionSync{Buffer(response.begin(), response.end())}))
                        throw ProtocolError("Authentication response send failed", MMQP_REASON_BUSY);
                    return;
                }
                if (phase != Phase::Active) throw ProtocolError("Unexpected packet during connection establishment");
                std::visit([&] (auto& value)
                {
                    using Type = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<Type, PublishRequest>) this->Publication(std::move(value));
                    else if constexpr (std::is_same_v<Type, ProfileAck>) this->ProfileAcked(value);
                    else if constexpr (std::is_same_v<Type, PublishAck>)
                    {
                        if (!pending.Remove(value.packetId) && value.packetId > publications.Value())
                            throw ProtocolError("Unknown publication acknowledgement", MMQP_REASON_INVALID_PID);
                        if (auto result = results.find(value.packetId); result != results.end())
                        {
                            result->second->set_value(value.reason);
                            results.erase(result);
                        }
                        owner.TryPost<eMQ::Delivery>(value.reason.text.size(), value.packetId, value.reason);
                    }
                    else if constexpr (std::is_same_v<Type, ProfileSync>)
                    {
                        if (value.reason.code >= 0x80) throw ProtocolError("Broker rejected alias mapping", value.reason.code);
                        profiles.publishers.Confirm(value.alias, value.topic);
                    }
                    else if constexpr (std::is_same_v<Type, ConnectionSync>)
                    {
                        if (value.token)
                        {
                            if (options.authKey.empty()) throw ProtocolError("No reauthentication key", MMQP_REASON_AUTH_FAILED);
                            auto response = Hmac256(options.authKey, *value.token);
                            this->Write(ConnectionSync{Buffer(response.begin(), response.end())});
                        }
                        else this->Write(ConnectionSync{});
                    }
                    else if constexpr (std::is_same_v<Type, ConnectionEnd>)
                    {
                        closeReason = value.reason;
                        connection->Close(true);
                    }
                    else throw ProtocolError("Packet is invalid in the active client state");
                }, message);
            }
            catch (const ProtocolError& error) { this->Fail(error); }
            catch (const std::exception&) { this->Fail(ProtocolError("Client processing failed", MMQP_REASON_INTERNAL_ERROR)); }
        }

        void Tick()
        {
            if (stopping) return;
            auto now = Clock::now();
            if (phase == Phase::Offline && options.autoReconnect && !manualDisconnect && resumable
                && !identifier.empty() && now >= reconnectAt)
            {
                const auto& endpoint = options.failoverEndpoints[reconnectIndex++ % options.failoverEndpoints.size()];
                try { this->Begin(endpoint.host, endpoint.port, true, std::make_shared<std::promise<std::string>>()); }
                catch (const std::logic_error&)
                {
                    resumable = false;
                    this->Closed("Session cannot safely resume after the outage");
                }
                catch (...) { reconnectAt = now + std::chrono::milliseconds(options.reconnectInterval); }
            }
            if (phase != Phase::Offline && phase != Phase::Active && now >= deadline)
                this->Fail(ProtocolError("Connection handshake timed out", MMQP_REASON_TIMEOUT));
            if (phase == Phase::Active)
            {
                try
                {
                    if (!profileTasks.empty())
                    {
                        auto& task = profileTasks.front();
                        if (task.deadline <= now) throw ProtocolError("Profile acknowledgement timed out", MMQP_REASON_TIMEOUT);
                        if (task.retry <= now)
                        {
                            if (++task.attempts > options.maxRetries || !this->Write(task.request))
                                throw ProtocolError("Profile retry budget exhausted", MMQP_REASON_TIMEOUT);
                            task.retry = now + std::chrono::milliseconds(options.retryInterval);
                        }
                    }
                    for (auto& [id, publication] : pending.entries)
                    {
                        if (publication.expiry <= now) throw ProtocolError("Publication expired before acknowledgement", MMQP_REASON_TIMEOUT);
                        if (publication.retry > now) continue;
                        if (++publication.attempts > options.maxRetries)
                            throw ProtocolError("Publication retry budget exhausted", MMQP_REASON_TIMEOUT);
                        auto retry = publication.message;
                        retry.duplicate = true;
                        if (!this->Write(retry)) throw ProtocolError("Publication retry send budget exhausted", MMQP_REASON_BUSY);
                        publication.retry = now + std::chrono::milliseconds(options.retryInterval);
                    }
                }
                catch (const ProtocolError& error) { this->Fail(error); }
            }
            timer.expires_after(std::chrono::milliseconds(50));
            timer.async_wait([this] (auto error) { if (!error) this->Tick(); });
        }
    };

    Client::Client(ClientOptions options): m_Impl(std::make_unique<Impl>(*this, std::move(options)))
    {
        m_Impl->io.Post([this] { m_Impl->Tick(); });
        m_Impl->io.Init();
    }

    Client::Client(const char* filename): Client(LoadClientOptions(filename)) {}
    Client::~Client() { this->WaitTasks(); this->Stop(); }

    std::future<std::string> Client::AsyncConnect(std::string host, uint16_t port, bool resume)
    {
        auto result = std::make_shared<std::promise<std::string>>();
        auto future = result->get_future();
        m_Impl->io.Post([this, host = std::move(host), port, resume, result]
        {
            try { m_Impl->Begin(host, port, resume, result); }
            catch (...) { result->set_exception(std::current_exception()); }
        });
        return future;
    }

    std::string Client::Connect(std::string host, uint16_t port, bool resume)
    {
        return this->AsyncConnect(std::move(host), port, resume).get();
    }

    void Client::Disconnect(bool graceful)
    {
        if (m_Impl->stopping) return;
        auto result = std::make_shared<std::promise<void>>();
        auto future = result->get_future();
        m_Impl->io.Post([this, graceful, result]
        {
            try
            {
                if (m_Impl->disconnecting) throw std::logic_error("Disconnect is already pending");
                m_Impl->closeReason = {};
                m_Impl->manualDisconnect = true;
                if (m_Impl->connection)
                {
                    m_Impl->disconnecting = result;
                    if (graceful && m_Impl->phase == Impl::Phase::Active) m_Impl->Write(ConnectionEnd{});
                    m_Impl->connection->Close(graceful);
                }
                else result->set_value();
            }
            catch (...) { result->set_exception(std::current_exception()); }
        });
        future.get();
    }

    void Client::Stop()
    {
        if (m_Impl->stopping) return;
        m_Impl->io.Submit([this]
        {
            m_Impl->stopping = true;
            m_Impl->timer.cancel();
            if (m_Impl->connection) m_Impl->connection->Close(false);
        }).get();
        m_Impl->io.Stop();
        this->Terminate();
    }

    bool Client::Connected() const { return m_Impl->active.load(); }
    std::string Client::Identifier()
    {
        if (m_Impl->stopping) return m_Impl->identifier;
        return m_Impl->io.Submit([this] { return m_Impl->identifier; }).get();
    }

    std::future<Reason> Client::UpdateProfile(std::vector<ProfileEntry> entries)
    {
        auto result = std::make_shared<std::promise<Reason>>();
        auto future = result->get_future();
        m_Impl->io.Post([this, entries = std::move(entries), result]() mutable
        {
            try
            {
                if (m_Impl->phase != Impl::Phase::Active) throw std::logic_error("Client is not connected");
                if (m_Impl->profileTasks.size() >= m_Impl->options.maxInflight)
                    throw ProtocolError("Profile request budget exhausted", MMQP_REASON_BUSY);
                m_Impl->profileTasks.push_back({ProfileRequest{0, std::move(entries)}, {}, result, {}, {}, 0});
                m_Impl->PumpProfile();
            }
            catch (...) { result->set_exception(std::current_exception()); }
        });
        return future;
    }

    std::future<Reason> Client::Register(std::string topic, uint8_t retain, uint32_t expiry, uint8_t queue, bool requests)
    {
        return this->UpdateProfile({ProfileEntry{MMQP_PROREQ_TYPE_REG, 0, std::move(topic), retain, requests, EncodeInterval(expiry), queue}});
    }

    std::future<Reason> Client::Subscribe(std::string topic, uint8_t qos, bool responder)
    {
        return this->UpdateProfile({ProfileEntry{MMQP_PROREQ_TYPE_SUB, 0, std::move(topic), qos, responder}});
    }

    std::future<Reason> Client::Unsubscribe(std::string topic)
    {
        return this->UpdateProfile({ProfileEntry{MMQP_PROREQ_TYPE_UNS, 0, std::move(topic)}});
    }

    std::future<Reason> Client::Send(PublishRequest publication)
    {
        auto result = std::make_shared<std::promise<Reason>>();
        auto future = result->get_future();
        m_Impl->io.Post([this, publication = std::move(publication), result]() mutable
        {
            try { m_Impl->Publish(std::move(publication), result); }
            catch (...) { result->set_exception(std::current_exception()); }
        });
        return future;
    }

    std::future<Reason> Client::Publish(std::string topic, Buffer payload, uint8_t qos)
    {
        PublishRequest publication;
        publication.topic = std::move(topic);
        publication.payload = std::move(payload);
        publication.qos = qos;
        return this->Send(std::move(publication));
    }

    std::future<Reason> Client::Request(std::string topic, Buffer payload, uint8_t responses, uint8_t qos, uint16_t correlationId)
    {
        PublishRequest publication;
        publication.type = MMQP_PUBREQ_TYPE_REQ;
        publication.topic = std::move(topic);
        publication.payload = std::move(payload);
        publication.responses = responses;
        publication.correlationId = correlationId;
        publication.qos = qos;
        return this->Send(std::move(publication));
    }

    std::future<Reason> Client::Respond(std::string topic, uint16_t correlationId, Buffer payload, Reason reason, uint8_t qos)
    {
        PublishRequest publication;
        publication.type = MMQP_PUBREQ_TYPE_RES;
        publication.correlationId = correlationId;
        publication.topic = std::move(topic);
        publication.payload = std::move(payload);
        publication.reason = std::move(reason);
        publication.qos = qos;
        return this->Send(std::move(publication));
    }

    std::future<Reason> Client::Respond(const PublishRequest& request, Buffer payload, Reason reason)
    {
        if (request.type != MMQP_PUBREQ_TYPE_REQ || !request.correlationId)
            throw std::invalid_argument("Respond requires a correlated request");
        return this->Respond(request.topic, request.correlationId, std::move(payload), std::move(reason), request.qos);
    }
}
