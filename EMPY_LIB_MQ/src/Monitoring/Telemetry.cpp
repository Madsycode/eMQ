#include <eMQ/Monitoring/Telemetry.h>
#include <eMQ/Runtime/Broker.h>
#include <eMQ/Security/Crypto.h>
#include <condition_variable>
#include <sstream>
#include <thread>

namespace eMQ
{
    namespace
    {
        std::shared_ptr<const std::string> Response(std::string_view status, std::string_view body)
        {
            return std::make_shared<const std::string>("HTTP/1.1 " + std::string(status)
                + "\r\nContent-Type: application/json; charset=utf-8\r\nCache-Control: no-store\r\n"
                "X-Content-Type-Options: nosniff\r\nConnection: close\r\nContent-Length: "
                + std::to_string(body.size()) + "\r\n\r\n" + std::string(body));
        }

        std::string Lower(std::string text)
        {
            for (auto& c : text) if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
            return text;
        }
    }

    struct TelemetryServer::Impl
    {
        struct Request : std::enable_shared_from_this<Request>
        {
            Impl& owner;
            asio::ip::tcp::socket socket;
            asio::steady_timer deadline;
            std::string input;
            std::shared_ptr<const std::string> response;
            bool closed = false;

            Request(Impl& server, asio::ip::tcp::socket stream):
                owner(server), socket(std::move(stream)), deadline(socket.get_executor()) {}

            void Start()
            {
                deadline.expires_after(std::chrono::seconds(3));
                deadline.async_wait([self = shared_from_this()] (auto error) { if (!error) self->Close(); });
                asio::async_read_until(socket, asio::dynamic_buffer(input, 8192), "\r\n\r\n",
                    [self = shared_from_this()] (auto error, size_t size)
                {
                    if (error) { self->Close(); return; }
                    self->Reply(self->owner.Handle(std::string_view(self->input).substr(0, size)));
                });
            }

            void Reply(std::shared_ptr<const std::string> bytes)
            {
                if (closed) return;
                response = std::move(bytes);
                asio::async_write(socket, asio::buffer(*response), [self = shared_from_this()] (auto, size_t) { self->Close(); });
            }

            void Close()
            {
                if (closed) return;
                closed = true;
                asio::error_code ignored;
                deadline.cancel();
                socket.close(ignored);
                owner.requests.erase(shared_from_this());
            }
        };

        Broker& broker;
        TelemetryOptions options;
        asio::io_context io;
        asio::executor_work_guard<asio::io_context::executor_type> work{io.get_executor()};
        asio::ip::tcp::acceptor acceptor{io};
        std::set<std::shared_ptr<Request>> requests;
        std::set<std::string> hosts;
        std::thread worker;
        std::thread sampler;
        std::mutex mutex;
        std::condition_variable wake;
        std::atomic<bool> stopping = false;
        std::shared_ptr<const std::string> cached = Response("503 Service Unavailable", "{\"error\":\"Awaiting first snapshot\"}");

        Impl(Broker& service, TelemetryOptions settings): broker(service), options(std::move(settings))
        {
            if (!asio::ip::make_address(options.address).is_loopback())
                throw std::invalid_argument("Telemetry must bind to loopback; use an authenticated TLS proxy or SSH tunnel for remote collection");
            if (options.intervalMs < 250 || options.intervalMs > 10000 || !options.maxSessions || options.maxSessions > 4096)
                throw std::invalid_argument("Invalid telemetry sampling bounds");
            if (!options.token.empty() && (options.token.size() < 32 || options.token.size() > 256
                || std::any_of(options.token.begin(), options.token.end(), [] (auto c) { return c < 33 || c > 126; })))
                throw std::invalid_argument("Telemetry token must contain 32..256 printable non-space ASCII characters");
        }

        ~Impl() { std::fill(options.token.begin(), options.token.end(), '\0'); }

        std::shared_ptr<const std::string> Handle(std::string_view input)
        {
            std::istringstream stream{std::string(input)};
            std::string line, method, path, version, extra;
            std::getline(stream, line);
            std::istringstream first(line);
            if (!(first >> method >> path >> version) || (first >> extra) || (version != "HTTP/1.1" && version != "HTTP/1.0"))
                return Response("400 Bad Request", "{\"error\":\"Invalid request\"}");
            std::map<std::string, std::string> headers;
            while (std::getline(stream, line) && line != "\r")
            {
                if (line.empty() || line.back() != '\r') return Response("400 Bad Request", "{}");
                line.pop_back();
                auto colon = line.find(':');
                if (colon == std::string::npos || !colon) return Response("400 Bad Request", "{}");
                auto key = Lower(line.substr(0, colon));
                if (std::any_of(key.begin(), key.end(), [] (auto c) { return !(c >= 'a' && c <= 'z') && c != '-'; }))
                    return Response("400 Bad Request", "{}");
                auto begin = line.find_first_not_of(" \t", colon + 1);
                auto end = line.find_last_not_of(" \t");
                auto value = begin == std::string::npos ? std::string{} : line.substr(begin, end - begin + 1);
                if (!headers.emplace(key, value).second) return Response("400 Bad Request", "{}");
            }
            if (!hosts.contains(Lower(headers["host"]))) return Response("403 Forbidden", "{\"error\":\"Invalid host\"}");
            if (auto origin = headers.find("origin"); origin != headers.end()
                && !hosts.contains(Lower(origin->second).starts_with("http://") ? Lower(origin->second).substr(7) : ""))
                return Response("403 Forbidden", "{\"error\":\"Cross-origin requests are disabled\"}");
            if (!options.token.empty())
            {
                auto expected = "Bearer " + options.token;
                const auto& provided = headers["authorization"];
                if (!SecureEqual(std::span(reinterpret_cast<const uint8_t*>(provided.data()), provided.size()),
                    std::span(reinterpret_cast<const uint8_t*>(expected.data()), expected.size())))
                    return Response("401 Unauthorized", "{\"error\":\"Telemetry token required\"}");
            }
            if (method != "GET") return Response("405 Method Not Allowed", "{\"error\":\"Read-only endpoint\"}");
            if (headers.contains("transfer-encoding") || (headers.contains("content-length") && headers["content-length"] != "0"))
                return Response("400 Bad Request", "{}");
            if (path == "/health") return Response("200 OK", "{\"status\":\"ok\",\"schema\":1}");
            if (path != "/api/v1/snapshot") return Response("404 Not Found", "{\"error\":\"Unknown endpoint\"}");
            std::lock_guard lock(mutex);
            return cached;
        }

        void Accept()
        {
            acceptor.async_accept([this] (auto error, auto socket)
            {
                if (!error && !stopping && requests.size() < 32)
                {
                    auto request = std::make_shared<Request>(*this, std::move(socket));
                    requests.insert(request);
                    request->Start();
                }
                if (!stopping) this->Accept();
            });
        }

        void Sample()
        {
            while (!stopping)
            {
                try
                {
                    auto result = Response("200 OK", SnapshotJson(broker.Inspect(options.maxSessions)));
                    std::lock_guard lock(mutex);
                    cached = std::move(result);
                }
                catch (const std::exception&)
                {
                    // Preserve the last observation. Its unchanged uptime lets
                    // collectors detect stale state without exposing internals.
                }
                std::unique_lock lock(mutex);
                wake.wait_for(lock, std::chrono::milliseconds(options.intervalMs), [this] { return stopping.load(); });
            }
        }
    };

    TelemetryServer::TelemetryServer(Broker& broker, TelemetryOptions options): m_Impl(std::make_unique<Impl>(broker, std::move(options))) {}
    TelemetryServer::~TelemetryServer() { this->Stop(); }

    uint16_t TelemetryServer::Start()
    {
        if (m_Impl->worker.joinable() || m_Impl->stopping) throw std::logic_error("Telemetry already started or stopped");
        asio::ip::tcp::endpoint endpoint(asio::ip::make_address(m_Impl->options.address), m_Impl->options.port);
        m_Impl->acceptor.open(endpoint.protocol());
        m_Impl->acceptor.bind(endpoint);
        m_Impl->acceptor.listen(32);
        auto port = m_Impl->acceptor.local_endpoint().port();
        auto suffix = ':' + std::to_string(port);
        auto address = endpoint.address().is_v6() ? '[' + m_Impl->options.address + ']' : m_Impl->options.address;
        m_Impl->hosts = {Lower(address) + suffix, "localhost" + suffix};
        m_Impl->Accept();
        m_Impl->worker = std::thread([this] { m_Impl->io.run(); });
        m_Impl->sampler = std::thread([this] { m_Impl->Sample(); });
        return port;
    }

    void TelemetryServer::Stop()
    {
        if (m_Impl->stopping.exchange(true)) return;
        m_Impl->wake.notify_all();
        if (m_Impl->sampler.joinable()) m_Impl->sampler.join();
        if (m_Impl->worker.joinable())
        {
            asio::post(m_Impl->io, [this]
            {
                asio::error_code ignored;
                m_Impl->acceptor.close(ignored);
                auto requests = m_Impl->requests;
                for (const auto& request : requests) request->Close();
            });
            m_Impl->work.reset();
            m_Impl->worker.join();
        }
    }
}
