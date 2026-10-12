#include <eMQ/Network/QContext.h>
#include <eMQ/Security/Crypto.h>

#ifdef EMPY_ENABLE_QUIC
#include <msquic.h>
#include <openssl/pem.h>
#include <openssl/pkcs7.h>
#include <openssl/x509v3.h>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <cstring>
#include <map>
#include <set>

namespace eMQ
{
    namespace
    {
        void CheckQuic(QUIC_STATUS status, const char* operation)
        {
            if (QUIC_FAILED(status))
                throw std::runtime_error(std::string(operation) + " failed (MsQuic status "
                    + std::to_string(static_cast<uint32_t>(status)) + ')');
        }

        struct QuicConfiguration
        {
            const QUIC_API_TABLE* api;
            HQUIC handle = nullptr;
            std::unique_ptr<X509_STORE, decltype(&X509_STORE_free)> trust{nullptr, X509_STORE_free};

            explicit QuicConfiguration(const QUIC_API_TABLE* table): api(table) {}
            ~QuicConfiguration()
            {
                if (handle) api->ConfigurationClose(handle);
            }

            bool Verify(const QUIC_CONNECTION_EVENT& event, const std::string& name, bool server)
            {
                if (!trust) return QUIC_SUCCEEDED(event.PEER_CERTIFICATE_RECEIVED.DeferredStatus);
                auto leaf = reinterpret_cast<const QUIC_BUFFER*>(event.PEER_CERTIFICATE_RECEIVED.Certificate);
                auto chain = reinterpret_cast<const QUIC_BUFFER*>(event.PEER_CERTIFICATE_RECEIVED.Chain);
                if (!leaf || !leaf->Buffer || !leaf->Length) return false;
                const unsigned char* cursor = leaf->Buffer;
                std::unique_ptr<X509, decltype(&X509_free)> cert(d2i_X509(nullptr, &cursor, leaf->Length), X509_free);
                if (!cert || cursor != leaf->Buffer + leaf->Length) return false;
                std::unique_ptr<PKCS7, decltype(&PKCS7_free)> bundle(nullptr, PKCS7_free);
                STACK_OF(X509)* intermediates = nullptr;
                if (chain && chain->Length)
                {
                    cursor = chain->Buffer;
                    bundle.reset(d2i_PKCS7(nullptr, &cursor, chain->Length));
                    if (!bundle || cursor != chain->Buffer + chain->Length || !PKCS7_type_is_signed(bundle.get())) return false;
                    intermediates = bundle->d.sign->cert;
                }
                std::unique_ptr<X509_STORE_CTX, decltype(&X509_STORE_CTX_free)> context(X509_STORE_CTX_new(), X509_STORE_CTX_free);
                if (!context || X509_STORE_CTX_init(context.get(), trust.get(), cert.get(), intermediates) != 1) return false;
                if (X509_STORE_CTX_set_purpose(context.get(), server ? X509_PURPOSE_SSL_CLIENT : X509_PURPOSE_SSL_SERVER) != 1)
                    return false;
                if (!server)
                {
                    auto parameters = X509_STORE_CTX_get0_param(context.get());
                    asio::error_code error;
                    asio::ip::make_address(name, error);
                    int success = error ? X509_VERIFY_PARAM_set1_host(parameters, name.c_str(), name.size())
                        : X509_VERIFY_PARAM_set1_ip_asc(parameters, name.c_str());
                    if (success != 1) return false;
                    X509_VERIFY_PARAM_set_hostflags(parameters, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
                }
                return X509_verify_cert(context.get()) == 1;
            }

        };
    }

    struct QcContext::Impl : std::enable_shared_from_this<Impl>
    {
        struct QuicConnection;
        struct Stream : std::enable_shared_from_this<Stream>
        {
            std::weak_ptr<QuicConnection> connection;
            HQUIC handle = nullptr;
            FrameStream decoder;
            std::optional<QuicChannel> channel;
            bool outgoing = false;
            bool prefixed = false;
            bool fin = false;
            bool sent = false;
            size_t frames = 0;

            explicit Stream(uint32_t limit): decoder(limit) {}
        };

        struct Write
        {
            Buffer bytes;
            QUIC_BUFFER buffer;
            std::shared_ptr<QuicConnection> connection;

            Write(Buffer data, std::shared_ptr<QuicConnection> owner):
                bytes(std::move(data)), buffer{static_cast<uint32_t>(bytes.size()), bytes.data()}, connection(std::move(owner)) {}
        };

        struct QuicConnection : Connection
        {
            std::shared_ptr<Impl> context;
            std::shared_ptr<QuicConfiguration> configuration;
            TransportOptions options;
            HQUIC handle = nullptr;
            asio::steady_timer timeout;
            IoContext::Accept ready;
            std::string name;
            std::string failure = "QUIC connection closed";
            std::map<HQUIC, std::shared_ptr<Stream>> streams;
            std::map<QuicChannel, std::shared_ptr<Stream>> persistent;
            std::set<QuicChannel> incoming;
            std::deque<std::pair<QuicChannel, Buffer>> deferred;
            std::optional<QuicPlan> plan;
            std::atomic_size_t posted = 0;
            size_t deferredBytes = 0;
            size_t queued = 0;
            size_t bulk = 0;
            size_t incomingBulk = 0;
            uint16_t datagramLimit = 0;
            uint32_t limit;
            bool server;
            bool reasons = false;
            bool draining = false;
            bool closing = false;
            bool closed = false;

            QuicConnection(std::shared_ptr<Impl> owner, TransportOptions settings, bool listener):
                context(std::move(owner)), options(std::move(settings)), timeout(context->executor),
                limit(options.maxPacket), server(listener) {}

            std::shared_ptr<QuicConnection> Self()
            {
                return std::static_pointer_cast<QuicConnection>(shared_from_this());
            }

            void Post(std::function<void()> task)
            {
                asio::post(context->executor, [self = Self(), task = std::move(task)]
                {
                    try { task(); }
                    catch (const std::exception& error) { self->Fail(error.what()); }
                });
            }

            void Deadline()
            {
                timeout.expires_after(std::chrono::milliseconds(options.timeout));
                timeout.async_wait([self = Self()] (auto error) { if (!error) self->Fail("QUIC connection timed out"); });
            }

            void Ready()
            {
                if (closed || closing) return;
                timeout.cancel();
                if (!server) persistent[QuicChannel::Control] = this->Open(QuicChannel::Control);
                ready(Self());
            }

            std::shared_ptr<Stream> Open(QuicChannel channel)
            {
                auto stream = std::make_shared<Stream>(limit);
                stream->connection = Self();
                stream->channel = channel;
                stream->outgoing = true;
                CheckQuic(context->api->StreamOpen(handle, channel == QuicChannel::Control
                    ? QUIC_STREAM_OPEN_FLAG_NONE : QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL,
                    OnStream, stream.get(), &stream->handle), "StreamOpen");
                streams.emplace(stream->handle, stream);
                auto status = context->api->StreamStart(stream->handle, QUIC_STREAM_START_FLAG_NONE);
                if (QUIC_FAILED(status))
                {
                    streams.erase(stream->handle);
                    context->api->StreamClose(stream->handle);
                    stream->handle = nullptr;
                    CheckQuic(status, "StreamStart");
                }
                if (channel == QuicChannel::Bulk) ++bulk;
                return stream;
            }

            void PeerStream(std::shared_ptr<Stream> stream, bool unidirectional)
            {
                if (closed || closing)
                {
                    context->api->StreamShutdown(stream->handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 1);
                    streams.emplace(stream->handle, std::move(stream));
                    return;
                }
                streams.emplace(stream->handle, stream);
                stream->decoder.SetLimit(limit);
                if (!unidirectional)
                {
                    uint64_t id = 0;
                    uint32_t size = sizeof(id);
                    CheckQuic(context->api->GetParam(stream->handle, QUIC_PARAM_STREAM_ID, &size, &id), "Stream ID");
                    if (!server || id != 0 || persistent.contains(QuicChannel::Control))
                        throw ProtocolError("QUIC requires exactly one client-initiated bidirectional control stream");
                    stream->channel = QuicChannel::Control;
                    persistent.emplace(QuicChannel::Control, stream);
                }
                if (streams.size() > 2 * (static_cast<size_t>(options.quicPolicy.bulk ? options.quicPolicy.bulk->streams : 0) + 2) + 1)
                    throw ProtocolError("QUIC stream budget exhausted", MMQP_REASON_BUSY);
            }

            size_t Buffered() const
            {
                size_t total = deferredBytes;
                for (const auto& [id, stream] : streams) total += stream->decoder.Buffered();
                return total;
            }

            void Receive(const std::shared_ptr<Stream>& stream, std::span<const uint8_t> bytes)
            {
                if (closed || closing || draining) return;
                if (bytes.size() > options.maxQueue - std::min(options.maxQueue, this->Buffered()))
                    throw ProtocolError("QUIC receive budget exhausted", MMQP_REASON_BUSY);
                if (!stream->channel && !bytes.empty())
                {
                    auto lane = static_cast<QuicChannel>(bytes.front());
                    bytes = bytes.subspan(1);
                    if (lane != QuicChannel::BestEffort && lane != QuicChannel::Reliable && lane != QuicChannel::Bulk)
                        throw ProtocolError("Unknown QUIC stream class");
                    if (lane == QuicChannel::Bulk)
                    {
                        const auto& policy = plan ? plan->Policy() : options.quicPolicy;
                        size_t maximum = policy.bulk ? policy.bulk->streams : 0;
                        if (++incomingBulk > maximum) throw ProtocolError("QUIC bulk stream budget exhausted");
                    }
                    else if (!incoming.insert(lane).second) throw ProtocolError("Duplicate persistent QUIC stream");
                    stream->channel = lane;
                }
                stream->decoder.Feed(bytes, [&] (auto frame)
                {
                    if (!stream->channel) throw ProtocolError("QUIC stream is missing its class byte");
                    if (*stream->channel == QuicChannel::Bulk && stream->frames != 0)
                        throw ProtocolError("A bulk stream carries exactly one publication");
                    ++stream->frames;
                    this->Frame(*stream->channel, frame);
                });
                this->Flush();
            }

            void Flush()
            {
                if (!closed && onFlush) onFlush();
            }

            void Frame(QuicChannel channel, std::span<const uint8_t> bytes)
            {
                if (closed || closing || draining) return;
                if (!plan && channel != QuicChannel::Control)
                {
                    if (bytes.size() > options.maxQueue - std::min(options.maxQueue, this->Buffered()))
                        throw ProtocolError("Pre-negotiation QUIC receive budget exhausted");
                    deferred.emplace_back(channel, Buffer(bytes.begin(), bytes.end()));
                    deferredBytes += bytes.size();
                    return;
                }
                if (plan)
                {
                    // The consumer decodes and validates the body; routing needs only the headers.
                    auto packet = Codec::Peek(bytes, {reasons, limit, UINT32_MAX});
                    if (options.backplane && packet.Publication() && packet.topic == "$mhf/control")
                    {
                        if (channel != QuicChannel::Control) throw ProtocolError("Backplane control record on data stream");
                    }
                    else plan->Validate(channel, packet, bytes.size());
                }
                if (onReceive) onReceive(bytes);
            }

            bool Send(Buffer bytes) override
            {
                if (closed || closing || draining || queued >= options.maxQueue || bytes.size() >= options.maxQueue - queued) return false;
                QuicChannel channel = QuicChannel::Control;
                if (plan)
                {
                    auto packet = Codec::Peek(bytes, {reasons, limit, UINT32_MAX});
                    if (!(options.backplane && packet.Publication() && packet.topic == "$mhf/control"))
                        channel = plan->Select(packet, bytes.size(), datagramLimit, bulk);
                }
                std::shared_ptr<Stream> stream;
                if (channel != QuicChannel::Datagram)
                {
                    if (channel == QuicChannel::Bulk) stream = this->Open(channel);
                    else
                    {
                        if (!persistent.contains(channel))
                        {
                            if (channel == QuicChannel::Control) return false;
                            persistent[channel] = this->Open(channel);
                        }
                        stream = persistent.at(channel);
                    }
                    if (!stream->handle) return false;
                    if (channel != QuicChannel::Control && !stream->prefixed)
                    {
                        bytes.insert(bytes.begin(), static_cast<uint8_t>(channel));
                        stream->prefixed = true;
                    }
                }
                if (bytes.size() > options.maxQueue - queued) return false;
                auto write = std::make_unique<Write>(std::move(bytes), Self());
                auto size = write->bytes.size();
                queued += size;
                QUIC_STATUS status;
                if (channel == QuicChannel::Datagram)
                    status = context->api->DatagramSend(handle, &write->buffer, 1, QUIC_SEND_FLAG_NONE, write.get());
                else
                {
                    status = context->api->StreamSend(stream->handle, &write->buffer, 1,
                        channel == QuicChannel::Bulk ? QUIC_SEND_FLAG_FIN : QUIC_SEND_FLAG_NONE, write.get());
                    if (channel == QuicChannel::Bulk) stream->fin = true;
                }
                if (QUIC_FAILED(status))
                {
                    queued -= size;
                    this->Fail("QUIC send failed");
                    return false;
                }
                write.release(); // Released only by SEND_COMPLETE or the final datagram event.
                return true;
            }

            void Sent(std::unique_ptr<Write> write) { queued -= write->bytes.size(); }

            void PeerFinished(const std::shared_ptr<Stream>& stream)
            {
                if (closed || closing || draining) return;
                if (stream->channel != QuicChannel::Bulk)
                {
                    this->Fail("Peer closed a persistent QUIC stream");
                    return;
                }
                if (stream->frames != 1 || stream->decoder.Buffered())
                    throw ProtocolError("Truncated QUIC bulk publication");
            }

            void StreamClosed(const std::shared_ptr<Stream>& stream)
            {
                if (!stream->handle) return;
                auto native = stream->handle;
                stream->handle = nullptr;
                context->api->StreamClose(native);
                streams.erase(native);
                if (stream->channel == QuicChannel::Bulk)
                {
                    if (stream->outgoing) --bulk;
                    else --incomingBulk;
                }
            }

            void FinishedSending(const std::shared_ptr<Stream>& stream)
            {
                stream->sent = true;
                if (draining)
                {
                    bool complete = true;
                    for (const auto& [id, item] : streams)
                        if ((item->outgoing || item->channel == QuicChannel::Control) && !item->sent) complete = false;
                    if (complete) this->Abort();
                }
            }

            void Abort()
            {
                if (closing || closed) return;
                closing = true;
                timeout.cancel();
                context->api->ConnectionShutdown(handle, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
            }

            void Fail(const std::string& text)
            {
                if (closed || closing) return;
                failure = text;
                this->Abort();
            }

            void Close(bool drain) override
            {
                if (closed || closing || draining) return;
                if (!drain) { this->Abort(); return; }
                draining = true;
                bool waiting = false;
                for (auto& [id, stream] : streams)
                {
                    if ((!stream->outgoing && stream->channel != QuicChannel::Control) || stream->sent) continue;
                    waiting = true;
                    if (!stream->fin)
                    {
                        stream->fin = true;
                        context->api->StreamShutdown(stream->handle, QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, 0);
                    }
                }
                if (!waiting) this->Abort();
                else this->Deadline();
            }

            void Finish()
            {
                if (closed) return;
                closed = true;
                timeout.cancel();
                auto remaining = streams;
                for (const auto& [id, stream] : remaining) this->StreamClosed(stream);
                context->api->ConnectionClose(handle);
                handle = nullptr;
                if (onClose)
                {
                    try { onClose(failure); }
                    catch (const std::exception& error) { EMPY_ERROR("QUIC close callback failed: {}", error.what()); }
                }
                std::lock_guard lock(context->mutex);
                context->connections.erase(this);
                context->complete.notify_all();
            }

            void SetLimit(uint32_t maximum) override
            {
                for (auto& [id, stream] : streams) stream->decoder.SetLimit(maximum);
                limit = maximum;
            }

            void Configure(const QuicPolicy& policy, bool strings) override
            {
                if (plan) throw std::logic_error("QUIC policy is immutable within a transport connection");
                plan.emplace(policy);
                reasons = strings;
                if (incomingBulk > (plan->Policy().bulk ? plan->Policy().bulk->streams : 0u))
                    throw ProtocolError("Peer exceeded the negotiated bulk stream budget");
                for (const auto& [channel, bytes] : deferred) this->Frame(channel, bytes);
                deferred.clear();
                deferredBytes = 0;
            }

            size_t Queued() const override { return queued; }
            size_t Capacity() const override { return options.maxQueue; }
            bool Secure() const override { return true; }
            std::string fingerprint;
            std::string PeerFingerprint() const override { return fingerprint; }
            bool Quic() const override { return true; }
        };

        asio::any_io_executor executor;
        const QUIC_API_TABLE* api = nullptr;
        QUIC_TLS_PROVIDER provider = QUIC_TLS_PROVIDER_SCHANNEL;
        HQUIC registration = nullptr;
        HQUIC listener = nullptr;
        QUIC_BUFFER alpn{4, reinterpret_cast<uint8_t*>(const_cast<char*>("MMQP"))};
        std::shared_ptr<QuicConfiguration> serverConfiguration;
        TransportOptions serverOptions;
        IoContext::Accept accepted;
        size_t maxConnections = 1024;
        std::mutex mutex;
        std::condition_variable complete;
        std::map<QuicConnection*, std::shared_ptr<QuicConnection>> connections;
        bool stopping = false;

        explicit Impl(asio::any_io_executor actor): executor(std::move(actor))
        {
            CheckQuic(MsQuicOpen2(&api), "MsQuicOpen2");
            QUIC_REGISTRATION_CONFIG config{"Empy/MMQP", QUIC_EXECUTION_PROFILE_LOW_LATENCY};
            auto status = api->RegistrationOpen(&config, &registration);
            if (QUIC_FAILED(status)) { MsQuicClose(api); api = nullptr; CheckQuic(status, "RegistrationOpen"); }
            uint32_t size = sizeof(provider);
            auto providerStatus = api->GetParam(nullptr, QUIC_PARAM_GLOBAL_TLS_PROVIDER, &size, &provider);
            if (QUIC_FAILED(providerStatus))
            {
                api->RegistrationClose(registration);
                MsQuicClose(api);
                registration = nullptr;
                api = nullptr;
                CheckQuic(providerStatus, "QUIC TLS provider");
            }
        }

        ~Impl()
        {
            serverConfiguration.reset();
            if (listener) api->ListenerClose(listener);
            if (registration) api->RegistrationClose(registration);
            if (api) MsQuicClose(api);
        }

        std::shared_ptr<QuicConfiguration> Configuration(const TransportOptions& options, bool server)
        {
            if (!options.timeout || options.maxPacket < 64 || options.maxQueue < options.maxPacket
                || options.maxQueue > UINT32_MAX)
                throw std::invalid_argument("Invalid QUIC packet, queue or timeout bounds");
            auto plan = QuicPlan(options.quicPolicy);
            auto config = std::make_shared<QuicConfiguration>(api);
            QUIC_SETTINGS settings{};
            settings.IsSet.PeerBidiStreamCount = true;
            settings.PeerBidiStreamCount = server ? 1 : 0;
            settings.IsSet.PeerUnidiStreamCount = true;
            settings.PeerUnidiStreamCount = static_cast<uint16_t>(2 + (plan.Policy().bulk ? plan.Policy().bulk->streams : 0));
            settings.IsSet.DatagramReceiveEnabled = true;
            settings.DatagramReceiveEnabled = plan.Policy().datagram.has_value();
            settings.IsSet.HandshakeIdleTimeoutMs = true;
            settings.HandshakeIdleTimeoutMs = options.timeout;
            settings.IsSet.IdleTimeoutMs = true;
            settings.IdleTimeoutMs = 0; // MMQP keepalive owns the negotiated idle deadline.
            settings.IsSet.ConnFlowControlWindow = true;
            settings.ConnFlowControlWindow = static_cast<uint32_t>(options.maxQueue);
            CheckQuic(api->ConfigurationOpen(registration, &alpn, 1, &settings, sizeof(settings), nullptr, &config->handle),
                "ConfigurationOpen");
            QUIC_CREDENTIAL_CONFIG credential{};
            credential.Type = QUIC_CREDENTIAL_TYPE_NONE;
            credential.Flags = server ? QUIC_CREDENTIAL_FLAG_NONE : QUIC_CREDENTIAL_FLAG_CLIENT;
            if (server && options.mutual) credential.Flags |= QUIC_CREDENTIAL_FLAG_REQUIRE_CLIENT_AUTHENTICATION;
            QUIC_CERTIFICATE_FILE file{};
            QUIC_CERTIFICATE_HASH hash{};
            if (server || !options.certificate.empty() || !options.privateKey.empty() || !options.certificateHash.empty())
            {
#ifdef _WIN32
                if (provider == QUIC_TLS_PROVIDER_SCHANNEL)
                {
                    if (options.certificateHash.empty())
                        throw std::invalid_argument("Schannel QUIC requires certificate_hash for an existing Windows credential; use the MsQuic OpenSSL build for PEM files");
                    auto bytes = FromHex(options.certificateHash);
                    if (bytes.size() != sizeof(hash.ShaHash)) throw std::invalid_argument("certificate_hash requires exactly 40 hexadecimal digits");
                    std::copy(bytes.begin(), bytes.end(), hash.ShaHash);
                    credential.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_HASH;
                    credential.CertificateHash = &hash;
                }
                else
#endif
                {
                    if (options.certificate.empty() || options.privateKey.empty() || !options.certificateHash.empty())
                        throw std::invalid_argument("OpenSSL QUIC requires a PEM certificate and private key");
                    file.CertificateFile = options.certificate.c_str();
                    file.PrivateKeyFile = options.privateKey.c_str();
                    credential.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE;
                    credential.CertificateFile = &file;
                }
            }
            if (provider == QUIC_TLS_PROVIDER_OPENSSL)
            {
                credential.Flags |= QUIC_CREDENTIAL_FLAG_USE_TLS_BUILTIN_CERTIFICATE_VALIDATION;
                if (!options.authority.empty())
                {
                    credential.Flags |= QUIC_CREDENTIAL_FLAG_SET_CA_CERTIFICATE_FILE;
                    credential.CaCertificateFile = options.authority.c_str();
                }
            }
            else if (!options.authority.empty() && (!server || options.mutual))
            {
                config->trust.reset(X509_STORE_new());
                if (!config->trust || X509_STORE_load_file(config->trust.get(), options.authority.c_str()) != 1)
                    throw std::runtime_error("Cannot load the QUIC certificate authority");
                credential.Flags |= QUIC_CREDENTIAL_FLAG_INDICATE_CERTIFICATE_RECEIVED
                    | QUIC_CREDENTIAL_FLAG_DEFER_CERTIFICATE_VALIDATION | QUIC_CREDENTIAL_FLAG_USE_PORTABLE_CERTIFICATES;
            }
            if (options.backplane)
                credential.Flags |= QUIC_CREDENTIAL_FLAG_INDICATE_CERTIFICATE_RECEIVED | QUIC_CREDENTIAL_FLAG_USE_PORTABLE_CERTIFICATES;
            CheckQuic(api->ConfigurationLoadCredential(config->handle, &credential), "ConfigurationLoadCredential");
            return config;
        }

        static QUIC_STATUS QUIC_API OnListen(HQUIC, void* raw, QUIC_LISTENER_EVENT* event)
        {
            auto self = static_cast<Impl*>(raw);
            if (event->Type != QUIC_LISTENER_EVENT_NEW_CONNECTION) return QUIC_STATUS_SUCCESS;
            try
            {
                std::lock_guard lock(self->mutex);
                if (self->stopping || self->connections.size() >= self->maxConnections) return QUIC_STATUS_CONNECTION_REFUSED;
                auto connection = std::make_shared<QuicConnection>(self->shared_from_this(), self->serverOptions, true);
                connection->configuration = self->serverConfiguration;
                connection->ready = self->accepted;
                connection->handle = event->NEW_CONNECTION.Connection;
                self->connections.emplace(connection.get(), connection);
                self->api->SetCallbackHandler(connection->handle, reinterpret_cast<void*>(OnConnection), connection.get());
                auto status = self->api->ConnectionSetConfiguration(connection->handle, self->serverConfiguration->handle);
                if (QUIC_FAILED(status)) self->connections.erase(connection.get());
                return status;
            }
            catch (...) { return QUIC_STATUS_INTERNAL_ERROR; }
        }

        static QUIC_STATUS QUIC_API OnConnection(HQUIC, void* raw, QUIC_CONNECTION_EVENT* event)
        {
            auto connection = static_cast<QuicConnection*>(raw)->Self();
            try
            {
                switch (event->Type)
                {
                case QUIC_CONNECTION_EVENT_CONNECTED:
                    connection->Post([connection] { connection->Ready(); });
                    break;
                case QUIC_CONNECTION_EVENT_PEER_CERTIFICATE_RECEIVED:
                    if (connection->options.backplane)
                    {
                        auto leaf = reinterpret_cast<const QUIC_BUFFER*>(event->PEER_CERTIFICATE_RECEIVED.Certificate);
                        if (!leaf || !leaf->Buffer || !leaf->Length) return QUIC_STATUS_BAD_CERTIFICATE;
                        auto fingerprint = ToHex(Sha256({leaf->Buffer, leaf->Length}));
                        connection->Post([connection, fingerprint] { connection->fingerprint = fingerprint; });
                    }
                    return connection->configuration->Verify(*event, connection->name, connection->server)
                        ? QUIC_STATUS_SUCCESS : QUIC_STATUS_BAD_CERTIFICATE;
                case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT:
                    connection->Post([connection, status = event->SHUTDOWN_INITIATED_BY_TRANSPORT.Status]
                    {
                        connection->failure = "QUIC transport closed (status " + std::to_string(static_cast<uint32_t>(status)) + ')';
                    });
                    break;
                case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE:
                    connection->Post([connection] { connection->Finish(); });
                    break;
                case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED:
                {
                    auto stream = std::make_shared<Stream>(connection->options.maxPacket);
                    stream->handle = event->PEER_STREAM_STARTED.Stream;
                    stream->connection = connection;
                    // Keep the object alive until the actor has installed it, before setting its native callback.
                    connection->Post([connection, stream, flags = event->PEER_STREAM_STARTED.Flags]
                    {
                        connection->PeerStream(stream, (flags & QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL) != 0);
                    });
                    connection->context->api->SetCallbackHandler(stream->handle, reinterpret_cast<void*>(OnStream), stream.get());
                    break;
                }
                case QUIC_CONNECTION_EVENT_DATAGRAM_STATE_CHANGED:
                    connection->Post([connection, enabled = event->DATAGRAM_STATE_CHANGED.SendEnabled,
                        limit = event->DATAGRAM_STATE_CHANGED.MaxSendLength] { connection->datagramLimit = enabled ? limit : 0; });
                    break;
                case QUIC_CONNECTION_EVENT_DATAGRAM_RECEIVED:
                {
                    auto& datagram = *event->DATAGRAM_RECEIVED.Buffer;
                    if (event->DATAGRAM_RECEIVED.Flags & QUIC_RECEIVE_FLAG_0_RTT) return QUIC_STATUS_ABORTED;
                    if (connection->posted.fetch_add(datagram.Length) + datagram.Length > connection->options.maxQueue)
                    {
                        connection->posted.fetch_sub(datagram.Length);
                        break; // QoS 0 overload is a drop.
                    }
                    Buffer bytes(datagram.Buffer, datagram.Buffer + datagram.Length);
                    connection->Post([connection, bytes = std::move(bytes)]
                    {
                        connection->posted.fetch_sub(bytes.size());
                        connection->Frame(QuicChannel::Datagram, bytes);
                        connection->Flush();
                    });
                    break;
                }
                case QUIC_CONNECTION_EVENT_DATAGRAM_SEND_STATE_CHANGED:
                    if (QUIC_DATAGRAM_SEND_STATE_IS_FINAL(event->DATAGRAM_SEND_STATE_CHANGED.State))
                    {
                        auto write = static_cast<Write*>(event->DATAGRAM_SEND_STATE_CHANGED.ClientContext);
                        event->DATAGRAM_SEND_STATE_CHANGED.ClientContext = nullptr;
                        connection->Post([connection, write] { connection->Sent(std::unique_ptr<Write>(write)); });
                    }
                    break;
                default: break;
                }
                return QUIC_STATUS_SUCCESS;
            }
            catch (...) { return QUIC_STATUS_INTERNAL_ERROR; }
        }

        static QUIC_STATUS QUIC_API OnStream(HQUIC, void* raw, QUIC_STREAM_EVENT* event)
        {
            auto stream = static_cast<Stream*>(raw)->shared_from_this();
            auto connection = stream->connection.lock();
            if (!connection) return QUIC_STATUS_ABORTED;
            try
            {
                switch (event->Type)
                {
                case QUIC_STREAM_EVENT_RECEIVE:
                {
                    if (event->RECEIVE.Flags & QUIC_RECEIVE_FLAG_0_RTT) return QUIC_STATUS_ABORTED;
                    auto size = event->RECEIVE.TotalBufferLength;
                    if (size > connection->options.maxQueue
                        || connection->posted.fetch_add(static_cast<size_t>(size)) + size > connection->options.maxQueue)
                    {
                        if (size <= connection->options.maxQueue) connection->posted.fetch_sub(static_cast<size_t>(size));
                        connection->Post([connection] { connection->Fail("QUIC callback queue exhausted"); });
                        return QUIC_STATUS_ABORTED;
                    }
                    Buffer bytes;
                    bytes.reserve(static_cast<size_t>(size));
                    for (uint32_t i = 0; i < event->RECEIVE.BufferCount; ++i)
                    {
                        const auto& buffer = event->RECEIVE.Buffers[i];
                        bytes.insert(bytes.end(), buffer.Buffer, buffer.Buffer + buffer.Length);
                    }
                    connection->Post([connection, stream, bytes = std::move(bytes)]
                    {
                        connection->posted.fetch_sub(bytes.size());
                        connection->Receive(stream, bytes);
                    });
                    break;
                }
                case QUIC_STREAM_EVENT_SEND_COMPLETE:
                {
                    auto write = static_cast<Write*>(event->SEND_COMPLETE.ClientContext);
                    connection->Post([connection, write] { connection->Sent(std::unique_ptr<Write>(write)); });
                    break;
                }
                case QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN:
                    connection->Post([connection, stream] { connection->PeerFinished(stream); });
                    break;
                case QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
                case QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED:
                    connection->Post([connection] { connection->Fail("Peer aborted a QUIC stream"); });
                    break;
                case QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE:
                    connection->Post([connection, stream] { connection->FinishedSending(stream); });
                    break;
                case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
                    connection->Post([connection, stream] { connection->StreamClosed(stream); });
                    break;
                default: break;
                }
                return QUIC_STATUS_SUCCESS;
            }
            catch (...) { return QUIC_STATUS_INTERNAL_ERROR; }
        }
    };

    QcContext::QcContext(asio::any_io_executor executor): m_Impl(std::make_shared<Impl>(std::move(executor))) {}
    QcContext::~QcContext() { this->Stop(); }
    bool QcContext::Available() { return true; }

    uint16_t QcContext::Listen(std::string_view host, uint16_t port, TransportOptions options,
        IoContext::Accept accept, size_t maxConnections)
    {
        if (m_Impl->listener || m_Impl->stopping || !maxConnections) throw std::logic_error("QUIC listener already configured or stopped");
        m_Impl->serverConfiguration = m_Impl->Configuration(options, true);
        m_Impl->serverOptions = std::move(options);
        m_Impl->maxConnections = maxConnections;
        m_Impl->accepted = std::move(accept);
        CheckQuic(m_Impl->api->ListenerOpen(m_Impl->registration, Impl::OnListen, m_Impl.get(), &m_Impl->listener), "ListenerOpen");
        auto endpoint = asio::ip::udp::endpoint(asio::ip::make_address(host), port);
        QUIC_ADDR address{};
        if (endpoint.size() > sizeof(address)) throw std::runtime_error("Unsupported QUIC address size");
        std::memcpy(&address, endpoint.data(), endpoint.size());
        CheckQuic(m_Impl->api->ListenerStart(m_Impl->listener, &m_Impl->alpn, 1, &address), "ListenerStart");
        uint32_t length = sizeof(address);
        CheckQuic(m_Impl->api->GetParam(m_Impl->listener, QUIC_PARAM_LISTENER_LOCAL_ADDRESS, &length, &address), "Listener address");
        return QuicAddrGetPort(&address);
    }

    Connection::Pointer QcContext::Connect(std::string host, uint16_t port, TransportOptions options,
        IoContext::Accept ready, Connection::Closed closed)
    {
        if (m_Impl->stopping) throw std::logic_error("QUIC context is stopped");
        auto connection = std::make_shared<Impl::QuicConnection>(m_Impl, options, false);
        connection->configuration = m_Impl->Configuration(options, false);
        connection->name = options.serverName.empty() ? host : options.serverName;
        connection->ready = std::move(ready);
        connection->onClose = std::move(closed);
        CheckQuic(m_Impl->api->ConnectionOpen(m_Impl->registration, Impl::OnConnection, connection.get(), &connection->handle),
            "ConnectionOpen");
        {
            std::lock_guard lock(m_Impl->mutex);
            m_Impl->connections.emplace(connection.get(), connection);
        }
        try
        {
            if (connection->name != host)
            {
                asio::ip::udp::resolver resolver(m_Impl->executor);
                auto addresses = resolver.resolve(host, std::to_string(port));
                if (addresses.empty()) throw std::runtime_error("No QUIC endpoint resolved");
                auto endpoint = addresses.begin()->endpoint();
                QUIC_ADDR address{};
                std::memcpy(&address, endpoint.data(), endpoint.size());
                CheckQuic(m_Impl->api->SetParam(connection->handle, QUIC_PARAM_CONN_REMOTE_ADDRESS, sizeof(address), &address),
                    "QUIC remote address");
            }
            connection->Deadline();
            CheckQuic(m_Impl->api->ConnectionStart(connection->handle, connection->configuration->handle,
                QUIC_ADDRESS_FAMILY_UNSPEC, connection->name.c_str(), port), "ConnectionStart");
        }
        catch (...)
        {
            connection->timeout.cancel();
            m_Impl->api->ConnectionClose(connection->handle);
            connection->handle = nullptr;
            std::lock_guard lock(m_Impl->mutex);
            m_Impl->connections.erase(connection.get());
            throw;
        }
        return connection;
    }

    void QcContext::Stop()
    {
        {
            std::lock_guard lock(m_Impl->mutex);
            if (m_Impl->stopping) return;
            m_Impl->stopping = true;
        }
        if (m_Impl->listener)
        {
            m_Impl->api->ListenerClose(m_Impl->listener);
            m_Impl->listener = nullptr;
        }
        m_Impl->api->RegistrationShutdown(m_Impl->registration, QUIC_CONNECTION_SHUTDOWN_FLAG_SILENT, 0);
        std::unique_lock lock(m_Impl->mutex);
        m_Impl->complete.wait(lock, [this] { return m_Impl->connections.empty(); });
    }
}

#else
namespace eMQ
{
    struct QcContext::Impl {};
    QcContext::QcContext(asio::any_io_executor): m_Impl(std::make_shared<Impl>()) {}
    QcContext::~QcContext() = default;
    bool QcContext::Available() { return false; }
    void QcContext::Stop() {}
    uint16_t QcContext::Listen(std::string_view, uint16_t, TransportOptions, IoContext::Accept, size_t)
    {
        throw std::runtime_error("QUIC requires a build with EMPY_ENABLE_QUIC=ON and MsQuic");
    }
    Connection::Pointer QcContext::Connect(std::string, uint16_t, TransportOptions, IoContext::Accept, Connection::Closed)
    {
        throw std::runtime_error("QUIC requires a build with EMPY_ENABLE_QUIC=ON and MsQuic");
    }
}
#endif
