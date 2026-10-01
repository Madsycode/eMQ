#pragma once
#include "Snapshot.h"

namespace eMQ
{
    struct Broker;

    struct TelemetryOptions
    {
        std::string address = "127.0.0.1";
        uint16_t port = 5601;
        std::string token;
        uint32_t intervalMs = 1000;
        size_t maxSessions = 1024;
    };

    // A loopback-only, read-only HTTP endpoint with a bounded cached snapshot.
    // Start after Broker::Listen and stop before Broker::Stop. The broker must
    // outlive this object; sampling never runs on its protocol executor.
    struct EMPY_MQ_API TelemetryServer
    {
        explicit TelemetryServer(Broker& broker, TelemetryOptions options = {});
        ~TelemetryServer();
        TelemetryServer(const TelemetryServer&) = delete;
        TelemetryServer& operator=(const TelemetryServer&) = delete;

        uint16_t Start();
        void Stop();

    private:
        struct Impl;
        std::unique_ptr<Impl> m_Impl;
    };
}
