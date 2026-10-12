#pragma once
#include <eMQ/Core/Runtime/Runtime.h>
#include "../Common/Events.h"
#include "../Network/Connection.h"
#include "../Cluster/Node.h"
#include "../Monitoring/Telemetry.h"

namespace eMQ
{
    struct TenantOptions
    {
        std::string identity;
        std::string root;
        Buffer authKey;
        std::set<uint32_t> shards;
    };

    struct BrokerOptions
    {
        std::string address = "127.0.0.1";
        uint16_t port = 5600;
        uint32_t workers = 1;
        TransportOptions transport;
        size_t maxSessions = 1024;
        size_t maxProfiles = 256;
        size_t maxInflight = 256;
        size_t maxSessionBytes = 4 * 1024 * 1024;
        size_t maxRetainedBytes = 16 * 1024 * 1024;
        size_t maxRetainedTopics = 4096;
        size_t maxRequests = 256;
        size_t maxResponders = 64;
        uint32_t retryInterval = 500;
        uint32_t maxRetries = 5;
        uint32_t requestTimeout = 5000;
        uint8_t sessionExpiry = 0x45;
        uint8_t keepalive = 30;
        std::string tenantRoot;
        Buffer authKey;
        bool requireAuth = false;
        bool allowRemotePlaintext = false;
        bool allowInsecureAuth = false;
        std::string stateDirectory;
        size_t maxStateBytes = 256 * 1024 * 1024;
        size_t maxReplayBytes = 16 * 1024 * 1024;
        std::optional<ClusterOptions> cluster;
        std::vector<TenantOptions> tenants;
        std::optional<TelemetryOptions> telemetry;
    };

    EMPY_MQ_API BrokerOptions LoadBrokerOptions(const char* filename);

    struct EMPY_MQ_API Broker : eMQ::ImplService
    {
        explicit Broker(BrokerOptions options = {});
        explicit Broker(const char* filename);
        ~Broker() override;

        uint16_t Listen();
        uint16_t Port() const;
        BrokerMetrics Metrics();
        BrokerSnapshot Inspect(size_t maxSessions = 1024);
        void Stop();

    private:
        struct Impl;
        std::unique_ptr<Impl> m_Impl;
    };
}
