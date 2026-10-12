#include "Bench.h"
#include <eClient/API.h>
#include <charconv>
#include <csignal>
#include <iostream>

namespace
{
    volatile std::sig_atomic_t g_Interrupted = 0;
}

int32_t main(int32_t argc, char** argv)
{
    if (argc > 1 && std::string_view(argv[1]) == "bench") return RunBench(argc, argv);
    try
    {
        eMQ::ClientOptions options;
        std::string host = "127.0.0.1";
        uint16_t port = 5600;
        uint32_t duration = 0;
        uint32_t count = 0;
        uint32_t interval = 0;
        uint8_t qos = 1;
        bool verbose = false;
        std::vector<std::string> positional;
        auto number = [] (std::string_view text, uint32_t maximum)
        {
            uint32_t value = 0;
            auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
            if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() || value > maximum)
                throw std::invalid_argument("Invalid numeric argument");
            return value;
        };
        for (int32_t i = 1; i < argc; ++i)
        {
            std::string_view argument(argv[i]);
            if (argument == "--help")
            {
                std::cout << "EmpyKlient publish|subscribe|request|respond TOPIC [PAYLOAD]\n"
                    << "  [--config FILE] [--host HOST] [--port PORT] [--qos 0|1] [--seconds N] [--count N] [--verbose]\n"
                    << "  [--interval-ms N]  Pace repeated publications or requests\n"
                    << "EmpyKlient bench PREFIX --endpoints HOST:PORT[,HOST:PORT...] [--config FILE]\n"
                    << "  --publishers P --subscribers S [--topics T] [--rate R] [--payload B] [--qos 0|1]\n"
                    << "  [--seconds N] [--drain-ms D] [--offset K] [--verbose]\n"
                    << "  Load generator; defaults P=1 S=1 T=1 R=10/s per publisher (0 = window-limited),\n"
                    << "  B=64 (16..1048576), qos=1, N=30, D=3000, K=0; prints one JSON line on stdout\n";
                return EXIT_SUCCESS;
            }
            if (!argument.starts_with("--")) { positional.emplace_back(argument); continue; }
            if (argument == "--verbose") { verbose = true; continue; }
            if (++i == argc) throw std::invalid_argument("Missing option value");
            if (argument == "--config") options = eMQ::LoadClientOptions(argv[i]);
            else if (argument == "--host") host = argv[i];
            else if (argument == "--port") port = static_cast<uint16_t>(number(argv[i], UINT16_MAX));
            else if (argument == "--qos") qos = static_cast<uint8_t>(number(argv[i], 1));
            else if (argument == "--seconds") duration = number(argv[i], 86400);
            else if (argument == "--count") count = number(argv[i], 1000000);
            else if (argument == "--interval-ms") interval = number(argv[i], 60000);
            else throw std::invalid_argument("Unknown client option");
        }
        if (positional.size() < 2 || positional.size() > 3)
            throw std::invalid_argument("Provide a mode, topic and optional payload; use --help");
        auto mode = positional[0];
        if (mode != "publish" && mode != "subscribe" && mode != "request" && mode != "respond")
            throw std::invalid_argument("Unknown client mode");
        auto topic = positional[1];
        std::string text = positional.size() == 3 ? positional[2] : "";
        eMQ::Buffer payload(text.begin(), text.end());
        std::signal(SIGINT, [] (int) { g_Interrupted = 1; });
        std::signal(SIGTERM, [] (int) { g_Interrupted = 1; });
        eMQ::Client client(std::move(options));
        size_t received = 0;
        auto success = [] (std::future<eMQ::Reason> result)
        {
            auto reason = result.get();
            if (reason.code >= 0x80)
                throw std::runtime_error("MMQP rejected the operation: " + std::to_string(reason.code) + " " + reason.text);
        };
        client.On<eMQ::Receive>(topic, [&] (const eMQ::Receive& event)
        {
            if (event.message.reason.code >= 0x80)
                throw std::runtime_error("MMQP response failed: " + std::to_string(event.message.reason.code)
                    + " " + event.message.reason.text);
            std::cout << event.message.topic << ' ';
            std::cout.write(reinterpret_cast<const char*>(event.message.payload.data()),
                static_cast<std::streamsize>(event.message.payload.size()));
            std::cout << std::endl;
            ++received;
            if (mode == "respond" && event.message.type == MMQP_PUBREQ_TYPE_REQ)
                success(client.Respond(event.message.topic, event.message.correlationId, payload, {}, qos));
        });
        client.On<eMQ::Disconnect>([] (const auto& event) { if (!event.reconnecting) g_Interrupted = 1; });
        client.Connect(host, port);
        auto deadline = duration ? std::chrono::steady_clock::now() + std::chrono::seconds(duration)
            : std::chrono::steady_clock::time_point::max();
        if (mode == "publish" || mode == "request")
        {
            success(client.Register(topic, 0, 60, 1, mode == "request"));
            for (uint32_t i = 0; i < (count ? count : 1) && !g_Interrupted && std::chrono::steady_clock::now() < deadline; ++i)
            {
                if (mode == "publish") success(client.Publish(topic, payload, qos));
                else
                {
                    success(client.Request(topic, payload, 1, qos));
                    while (!g_Interrupted && received <= i)
                    {
                        if (std::chrono::steady_clock::now() >= deadline)
                            throw std::runtime_error("Request did not finish before the requested deadline");
                        client.Poll();
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    }
                    if (g_Interrupted) break;
                }
                if (interval && i + 1 < count)
                {
                    auto next = std::min(deadline, std::chrono::steady_clock::now() + std::chrono::milliseconds(interval));
                    while (!g_Interrupted && std::chrono::steady_clock::now() < next)
                    {
                        client.Poll();
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    }
                }
            }
            if (mode == "publish") { client.Disconnect(); return EXIT_SUCCESS; }
            if (!count) count = 1;
        }
        else success(client.Subscribe(topic, qos, mode == "respond"));
        if (verbose) std::cerr << "Profile ready: " << topic << std::endl;
        while (!g_Interrupted && std::chrono::steady_clock::now() < deadline && (!count || received < count))
        {
            client.Poll();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        client.Disconnect();
        return EXIT_SUCCESS;
    }
    catch (const std::exception& error)
    {
        std::cerr << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
