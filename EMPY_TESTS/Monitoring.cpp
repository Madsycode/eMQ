#include "Checks.h"
#include <eClient/API.h>
#include <eMQ/Runtime/Broker.h>
#include <eMQ/Monitoring/Telemetry.h>
#include <thread>

using namespace eMQ;

namespace
{
    std::string Http(uint16_t port, std::string path, std::string headers = {}, std::string method = "GET", std::string host = {})
    {
        asio::io_context io;
        asio::ip::tcp::socket socket(io);
        socket.connect({asio::ip::make_address("127.0.0.1"), port});
        if (host.empty()) host = "127.0.0.1:" + std::to_string(port);
        auto request = method + ' ' + path + " HTTP/1.1\r\nHost: " + host + "\r\n" + headers + "\r\n";
        asio::write(socket, asio::buffer(request));
        std::string reply;
        asio::error_code error;
        asio::read(socket, asio::dynamic_buffer(reply, 1024 * 1024), error);
        EMPY_CHECK(error == asio::error::eof || error == asio::error::connection_reset);
        return reply;
    }
}

void TestMonitoring()
{
    BrokerOptions settings;
    settings.port = 0;
    settings.transport.maxPacket = 65536;
    Broker broker(settings);
    auto port = broker.Listen();
    Client publisher, subscriber;
    publisher.Connect("127.0.0.1", port);
    auto sid = subscriber.Connect("127.0.0.1", port);
    EMPY_CHECK(subscriber.Subscribe("monitor/#").get().code < 0x80);
    EMPY_CHECK(publisher.Register("monitor/value").get().code < 0x80);
    auto initial = broker.Inspect();
    EMPY_CHECK(initial.metrics.online == 2 && initial.sessions.size() == 2 && !initial.cluster);
    auto one = broker.Inspect(1);
    EMPY_CHECK(one.sessions.size() == 1 && one.omittedSessions == 1 && one.metrics.sessions == 2);
    Throws([&] { broker.Inspect(0); });
    Throws([&] { broker.Inspect(4097); });
    EMPY_CHECK(publisher.Publish("monitor/value", {'S', 'E', 'C', 'R', 'E', 'T'}, 1).get().code < 0x80);
    auto snapshot = broker.Inspect();
    EMPY_CHECK(snapshot.metrics.publications == 1 && snapshot.metrics.forwarded == 1);
    EMPY_CHECK(snapshot.metrics.receivedBytes > initial.metrics.receivedBytes && snapshot.metrics.sentBytes > initial.metrics.sentBytes);
    auto session = std::find_if(snapshot.sessions.begin(), snapshot.sessions.end(), [&] (const auto& item) { return item.identifier == sid; });
    EMPY_CHECK(session != snapshot.sessions.end() && session->online && session->owner && session->subscriptions == 1);
    EMPY_CHECK(session->deliveries == 1 && session->sentPackets > 0 && session->transport == "TCP");
    auto json = SnapshotJson(snapshot);
    EMPY_CHECK(json.find("SECRET") == std::string::npos && json.find("monitor/value") == std::string::npos);
    snapshot.identifier = "quoted\"\\\n\t";
    EMPY_CHECK(SnapshotJson(snapshot).find("quoted\\\"\\\\\\u000a\\u0009") != std::string::npos);

    TelemetryOptions options;
    options.port = 0;
    options.intervalMs = 250;
    options.token = std::string(40, 't');
    auto invalid = options;
    invalid.address = "0.0.0.0";
    Throws([&] { TelemetryServer server(broker, invalid); });
    invalid = options;
    invalid.token = "short";
    Throws([&] { TelemetryServer server(broker, invalid); });
    invalid = options;
    invalid.intervalMs = 0;
    Throws([&] { TelemetryServer server(broker, invalid); });

    TelemetryServer server(broker, options);
    auto httpPort = server.Start();
    auto authorization = "Authorization: Bearer " + options.token + "\r\n";
    EMPY_CHECK(Http(httpPort, "/api/v1/snapshot").starts_with("HTTP/1.1 401"));
    EMPY_CHECK(Http(httpPort, "/health", authorization).starts_with("HTTP/1.1 200"));
    EMPY_CHECK(Http(httpPort, "/health", authorization, "POST").starts_with("HTTP/1.1 405"));
    EMPY_CHECK(Http(httpPort, "/unknown", authorization).starts_with("HTTP/1.1 404"));
    EMPY_CHECK(Http(httpPort, "/health", authorization, "GET", "attacker.example").starts_with("HTTP/1.1 403"));
    EMPY_CHECK(Http(httpPort, "/health", authorization + "Origin: https://attacker.example\r\n").starts_with("HTTP/1.1 403"));
    EMPY_CHECK(Http(httpPort, "/health", authorization + authorization).starts_with("HTTP/1.1 400"));
    EMPY_CHECK(Http(httpPort, "/health", authorization + "Transfer-Encoding: chunked\r\n").starts_with("HTTP/1.1 400"));
    EMPY_CHECK(Http(httpPort, "/health", "X-Large: " + std::string(9000, 'a') + "\r\n").empty());
    auto deadline = Clock::now() + std::chrono::seconds(3);
    do
    {
        json = Http(httpPort, "/api/v1/snapshot", authorization);
        if (json.starts_with("HTTP/1.1 200")) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (Clock::now() < deadline);
    EMPY_CHECK(json.starts_with("HTTP/1.1 200") && json.find("\"schema\":1") != std::string::npos);
    EMPY_CHECK(json.find(options.token) == std::string::npos && json.find("SECRET") == std::string::npos);
    subscriber.Disconnect();
    deadline = Clock::now() + std::chrono::seconds(3);
    do
    {
        snapshot = broker.Inspect();
        if (snapshot.metrics.online == 1) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    } while (Clock::now() < deadline);
    EMPY_CHECK(snapshot.metrics.online == 1 && snapshot.metrics.sessions == 2);
    session = std::find_if(snapshot.sessions.begin(), snapshot.sessions.end(), [&] (const auto& item) { return item.identifier == sid; });
    EMPY_CHECK(session != snapshot.sessions.end() && !session->online && session->expiresInMs.has_value());
    // Stop must cancel slow HTTP readers immediately, before the broker shuts down.
    asio::io_context io;
    asio::ip::tcp::socket slow(io);
    slow.connect({asio::ip::make_address("127.0.0.1"), httpPort});
    asio::write(slow, asio::buffer(std::string("GET /")));
    auto started = Clock::now();
    server.Stop();
    EMPY_CHECK(Clock::now() - started < std::chrono::seconds(2));
    publisher.Disconnect();
}
