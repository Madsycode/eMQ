#include <eMQ/Network/IOContext.h>
#include <eMQ/Network/QContext.h>
#include <eMQ/Security/Crypto.h>
#include <asio/ssl.hpp>
#include <deque>
#include <thread>

namespace eMQ
{
    namespace
    {
        using TlsContext = asio::ssl::context;

        std::shared_ptr<TlsContext> MakeTls(const TransportOptions& options, bool server)
        {
            auto context = std::make_shared<TlsContext>(server ? TlsContext::tls_server : TlsContext::tls_client);
            if (!options.tls) return context;
            if (SSL_CTX_set_min_proto_version(context->native_handle(), TLS1_3_VERSION) != 1)
                throw std::runtime_error("TLS 1.3 is unavailable");
            if (!options.authority.empty()) context->load_verify_file(options.authority);
            else context->set_default_verify_paths();
            if (!options.certificate.empty()) context->use_certificate_chain_file(options.certificate);
            if (!options.privateKey.empty()) context->use_private_key_file(options.privateKey, TlsContext::pem);
            if (server && options.tls && (options.certificate.empty() || options.privateKey.empty()))
                throw std::invalid_argument("TLS listeners require a certificate and private key");
            if (!server || options.mutual)
                context->set_verify_mode(asio::ssl::verify_peer
                    | (options.mutual ? asio::ssl::verify_fail_if_no_peer_cert : 0));
            return context;
        }

        struct TcpConnection : Connection
        {
            std::shared_ptr<TlsContext> context;
            asio::ssl::stream<asio::ip::tcp::socket> stream;
            asio::ip::tcp::resolver resolver;
            asio::steady_timer timeout;
            TransportOptions options;
            FrameStream decoder;
            std::array<uint8_t, 8192> input{};
            std::deque<Buffer> output;
            size_t queued = 0;
            bool closed = false;
            bool draining = false;

            TcpConnection(asio::ip::tcp::socket socket, std::shared_ptr<TlsContext> tls, TransportOptions settings):
                context(std::move(tls)), stream(std::move(socket), *context),
                resolver(stream.get_executor()), timeout(stream.get_executor()),
                options(std::move(settings)), decoder(options.maxPacket)
            {
                if (!options.timeout || options.maxQueue < options.maxPacket)
                    throw std::invalid_argument("Invalid transport queue or timeout bounds");
            }

            std::shared_ptr<TcpConnection> Self()
            {
                return std::static_pointer_cast<TcpConnection>(shared_from_this());
            }

            void Deadline()
            {
                timeout.expires_after(std::chrono::milliseconds(options.timeout));
                timeout.async_wait([self = Self()] (auto error)
                {
                    if (!error) self->Fail("Transport connection timed out");
                });
            }

            void Dial(std::string host, uint16_t port, IoContext::Accept ready)
            {
                this->Deadline();
                resolver.async_resolve(host, std::to_string(port),
                    [self = Self(), host, ready = std::move(ready)] (auto error, auto endpoints) mutable
                {
                    if (error) { self->Fail(error.message()); return; }
                    asio::async_connect(self->stream.next_layer(), endpoints,
                        [self, host, ready = std::move(ready)] (auto failure, auto) mutable
                    {
                        if (failure) { self->Fail(failure.message()); return; }
                        self->Handshake(false, host, std::move(ready));
                    });
                });
            }

            void Handshake(bool server, const std::string& host, IoContext::Accept ready)
            {
                if (closed) return;
                asio::error_code error;
                stream.next_layer().set_option(asio::ip::tcp::no_delay(true), error);
                if (error) { this->Fail(error.message()); return; }
                if (!options.tls) { this->Ready(std::move(ready)); return; }
                if (!server)
                {
                    auto name = options.serverName.empty() ? host : options.serverName;
                    if (SSL_set_tlsext_host_name(stream.native_handle(), name.c_str()) != 1)
                    {
                        this->Fail("TLS server name configuration failed");
                        return;
                    }
                    stream.set_verify_callback(asio::ssl::host_name_verification(name));
                }
                stream.async_handshake(server ? asio::ssl::stream_base::server : asio::ssl::stream_base::client,
                    [self = Self(), ready = std::move(ready)] (auto failure) mutable
                {
                    if (failure) { self->Fail(failure.message()); return; }
                    self->Ready(std::move(ready));
                });
            }

            void Ready(IoContext::Accept ready)
            {
                if (closed) return;
                timeout.cancel();
                try { ready(Self()); }
                catch (const std::exception& error) { this->Fail(error.what()); return; }
                this->Read();
            }

            void Read()
            {
                if (closed || draining) return;
                auto receive = [self = Self()] (asio::error_code error, size_t count)
                {
                    if (error) { self->Fail(error.message()); return; }
                    try
                    {
                        self->decoder.Feed(std::span(self->input).first(count), [self] (auto frame)
                        {
                            if (!self->closed && !self->draining && self->onReceive) self->onReceive(frame);
                        });
                    }
                    catch (const std::exception& failure) { self->Fail(failure.what()); return; }
                    self->Read();
                };
                if (options.tls) stream.async_read_some(asio::buffer(input), std::move(receive));
                else stream.next_layer().async_read_some(asio::buffer(input), std::move(receive));
            }

            bool Send(Buffer bytes) override
            {
                if (closed || draining || bytes.size() > options.maxQueue - queued) return false;
                bool idle = output.empty();
                queued += bytes.size();
                output.emplace_back(std::move(bytes));
                if (idle) this->Write();
                return true;
            }

            void Write()
            {
                auto complete = [self = Self()] (asio::error_code error, size_t)
                {
                    if (self->closed) return;
                    if (error) { self->Fail(error.message()); return; }
                    self->queued -= self->output.front().size();
                    self->output.pop_front();
                    if (!self->output.empty()) self->Write();
                    else if (self->draining) self->Fail("Connection closed");
                };
                if (options.tls) asio::async_write(stream, asio::buffer(output.front()), std::move(complete));
                else asio::async_write(stream.next_layer(), asio::buffer(output.front()), std::move(complete));
            }

            void Close(bool drain) override
            {
                if (closed) return;
                draining = drain;
                if (!drain || output.empty()) this->Fail("Connection closed");
                else
                {
                    timeout.expires_after(std::chrono::milliseconds(options.timeout));
                    timeout.async_wait([self = Self()] (auto error)
                    {
                        if (!error) self->Fail("Connection drain timed out");
                    });
                }
            }

            void Fail(const std::string& message)
            {
                if (closed) return;
                closed = true;
                resolver.cancel();
                timeout.cancel();
                asio::error_code ignored;
                stream.next_layer().cancel(ignored);
                stream.next_layer().shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
                stream.next_layer().close(ignored);
                // Pending writes own their buffers until their completion handlers run.
                if (onClose)
                {
                    try { onClose(message); }
                    catch (const std::exception& error) { EMPY_ERROR("Connection close callback failed: {}", error.what()); }
                }
            }

            void SetLimit(uint32_t limit) override { decoder.SetLimit(limit); }
            size_t Queued() const override { return queued; }
            size_t Capacity() const override { return options.maxQueue; }
            bool Secure() const override { return options.tls; }
            std::string PeerFingerprint() const override
            {
                if (!options.tls) return {};
                auto ssl = const_cast<TcpConnection*>(this)->stream.native_handle();
                std::unique_ptr<X509, decltype(&X509_free)> certificate(SSL_get1_peer_certificate(ssl), X509_free);
                Digest digest{};
                unsigned int size = 0;
                if (!certificate || X509_digest(certificate.get(), EVP_sha256(), digest.data(), &size) != 1 || size != digest.size()) return {};
                return ToHex(digest);
            }
        };
    }

    struct IoContext::Impl
    {
        asio::io_context context;
        asio::strand<asio::io_context::executor_type> strand{context.get_executor()};
        std::optional<asio::executor_work_guard<asio::io_context::executor_type>> guard;
        std::optional<asio::ip::tcp::acceptor> acceptor;
        std::unique_ptr<QcContext> quic;
        std::vector<std::thread> workers;
        std::vector<std::weak_ptr<TcpConnection>> connections;
        std::shared_ptr<TlsContext> tls;
        TransportOptions options;
        Accept accepted;
        size_t maxConnections = 1024;
        std::atomic_bool stopped = false;

        void AcceptNext()
        {
            if (!acceptor || !acceptor->is_open()) return;
            acceptor->async_accept(strand, [this] (auto error, auto socket)
            {
                if (!error)
                {
                    std::erase_if(connections, [] (const auto& weak) { return weak.expired(); });
                    if (connections.size() < maxConnections)
                    {
                        auto connection = std::make_shared<TcpConnection>(std::move(socket), tls, options);
                        connections.emplace_back(connection);
                        connection->Deadline();
                        connection->Handshake(true, {}, accepted);
                    }
                }
                if (error != asio::error::operation_aborted) this->AcceptNext();
            });
        }
    };

    IoContext::IoContext(): m_Impl(std::make_unique<Impl>()) {}
    IoContext::~IoContext() { this->Stop(); }

    uint16_t IoContext::Listen(std::string_view host, uint16_t port, TransportOptions options,
        Accept accept, size_t maxConnections)
    {
        if (!m_Impl->workers.empty() || m_Impl->acceptor || m_Impl->stopped || !maxConnections)
            throw std::logic_error("Configure a listener before starting I/O workers");
        if (options.quic)
        {
            if (!m_Impl->quic) m_Impl->quic = std::make_unique<QcContext>(m_Impl->strand);
            return m_Impl->quic->Listen(host, port, std::move(options), std::move(accept), maxConnections);
        }
        auto address = asio::ip::make_address(host);
        m_Impl->tls = MakeTls(options, true);
        m_Impl->options = std::move(options);
        m_Impl->accepted = std::move(accept);
        m_Impl->maxConnections = maxConnections;
        auto& acceptor = m_Impl->acceptor.emplace(m_Impl->strand);
        asio::ip::tcp::endpoint endpoint(address, port);
        acceptor.open(endpoint.protocol());
        acceptor.set_option(asio::socket_base::reuse_address(true));
        if (address.is_v6()) acceptor.set_option(asio::ip::v6_only(false));
        acceptor.bind(endpoint);
        acceptor.listen(asio::socket_base::max_listen_connections);
        m_Impl->AcceptNext();
        return acceptor.local_endpoint().port();
    }

    Connection::Pointer IoContext::Connect(std::string host, uint16_t port, TransportOptions options,
        Accept connected, Connection::Closed closed)
    {
        if (m_Impl->stopped) throw std::logic_error("I/O context is stopped");
        if (options.quic)
        {
            if (!m_Impl->quic) m_Impl->quic = std::make_unique<QcContext>(m_Impl->strand);
            return m_Impl->quic->Connect(std::move(host), port, std::move(options), std::move(connected), std::move(closed));
        }
        auto tls = MakeTls(options, false);
        auto connection = std::make_shared<TcpConnection>(asio::ip::tcp::socket(m_Impl->strand), tls, options);
        connection->onClose = std::move(closed);
        m_Impl->connections.emplace_back(connection);
        connection->Dial(std::move(host), port, std::move(connected));
        return connection;
    }

    asio::any_io_executor IoContext::Executor() { return m_Impl->strand; }

    void IoContext::Init(uint32_t workers)
    {
        if (!workers || !m_Impl->workers.empty() || m_Impl->stopped)
            throw std::logic_error("I/O workers already started or invalid worker count");
        m_Impl->guard.emplace(m_Impl->context.get_executor());
        for (uint32_t i = 0; i < workers; ++i)
            m_Impl->workers.emplace_back([this] { m_Impl->context.run(); });
    }

    void IoContext::Post(std::function<void()> task)
    {
        if (m_Impl->stopped) throw std::logic_error("I/O context is stopped");
        asio::post(m_Impl->strand, std::move(task));
    }

    void IoContext::Stop()
    {
        if (m_Impl->stopped) return;
        for (const auto& worker : m_Impl->workers)
            if (worker.get_id() == std::this_thread::get_id())
                throw std::logic_error("Stop I/O from the owner thread, not an I/O callback");
        if (m_Impl->quic)
        {
            // Native completion callbacks must drain on the actor before its executor stops.
            if (m_Impl->workers.empty()) this->Init();
            m_Impl->quic->Stop();
        }
        auto close = [this]
        {
            if (m_Impl->acceptor)
            {
                asio::error_code ignored;
                m_Impl->acceptor->close(ignored);
            }
            auto connections = m_Impl->connections;
            for (const auto& weak : connections)
                if (auto connection = weak.lock()) connection->Close(false);
        };
        if (!m_Impl->workers.empty()) this->Submit(close).get();
        else close();
        m_Impl->stopped = true;
        m_Impl->guard.reset();
        m_Impl->context.stop();
        for (auto& worker : m_Impl->workers) worker.join();
        m_Impl->workers.clear();
    }
}
