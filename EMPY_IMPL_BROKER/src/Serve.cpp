#include "Modules/Monitoring.h"
#include <charconv>
#include <iostream>

int32_t main(int32_t argc, char** argv)
{
    try
    {
        eMQ::BrokerOptions options;
        uint32_t duration = 0;
        bool monitor = false;
        auto number = [] (const char* text, uint32_t maximum)
        {
            std::string_view input(text);
            uint32_t value = 0;
            auto parsed = std::from_chars(input.data(), input.data() + input.size(), value);
            if (parsed.ec != std::errc{} || parsed.ptr != input.data() + input.size() || value > maximum)
                throw std::invalid_argument("Invalid numeric argument");
            return value;
        };
        for (int32_t i = 1; i < argc; ++i)
        {
            std::string_view argument(argv[i]);
            if (argument == "--help")
            {
                std::cout << "EmpyBroker [--config FILE] [--address IP] [--port PORT] [--seconds N] [--monitor]\n"
                    << "  [--telemetry-port PORT]  Enable the loopback HTTP monitoring endpoint\n";
                return EXIT_SUCCESS;
            }
            if (argument == "--monitor") { monitor = true; continue; }
            if (++i == argc) throw std::invalid_argument("Missing option value");
            if (argument == "--config") options = eMQ::LoadBrokerOptions(argv[i]);
            else if (argument == "--address") options.address = argv[i];
            else if (argument == "--port") options.port = static_cast<uint16_t>(number(argv[i], UINT16_MAX));
            else if (argument == "--seconds") duration = number(argv[i], 86400);
            else if (argument == "--telemetry-port")
            {
                if (!options.telemetry) options.telemetry.emplace();
                options.telemetry->port = static_cast<uint16_t>(number(argv[i], UINT16_MAX));
            }
            else throw std::invalid_argument("Unknown broker option");
        }
        std::signal(SIGINT, [] (int) { eMQ::g_Interrupted = 1; });
        std::signal(SIGTERM, [] (int) { eMQ::g_Interrupted = 1; });
        auto telemetryOptions = std::move(options.telemetry);
        options.telemetry.reset();
        eMQ::Runtime runtime(std::move(options), duration);
        auto port = runtime.Listen();
        std::unique_ptr<eMQ::TelemetryServer> telemetry;
        if (telemetryOptions)
        {
            telemetry = std::make_unique<eMQ::TelemetryServer>(runtime, std::move(*telemetryOptions));
            std::cout << "Telemetry listening on port " << telemetry->Start() << std::endl;
        }
        if (monitor) runtime.Attach<eMQ::Monitoring>();
        std::cout << "MMQP broker listening on port " << port << std::endl;
        runtime.StartService(10);
        if (telemetry) telemetry->Stop();
        runtime.Stop();
        return EXIT_SUCCESS;
    }
    catch (const std::exception& error)
    {
        std::cerr << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
