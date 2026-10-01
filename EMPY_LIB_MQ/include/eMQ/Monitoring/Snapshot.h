#pragma once
#include "../Common/State.h"

namespace eMQ
{
    // Process-local observations. These counters do not change persisted MMQP state.
    struct TrafficMetrics
    {
        uint64_t receivedPackets = 0;
        uint64_t receivedBytes = 0;
        uint64_t sentPackets = 0;
        uint64_t sentBytes = 0;
    };

    struct BrokerMetrics : TrafficMetrics
    {
        uint64_t accepted = 0;
        uint64_t publications = 0;
        uint64_t duplicates = 0;
        uint64_t rejected = 0;
        uint64_t forwarded = 0;
        uint64_t retransmitted = 0;
        size_t sessions = 0;
        size_t online = 0;
        size_t retainedBytes = 0;
        size_t pending = 0;
    };

    struct PeerSnapshot
    {
        std::string identifier;
        std::string host;
        uint16_t port = 0;
        bool connected = false;
        size_t queuedBytes = 0;
    };

    struct LeaseSnapshot
    {
        uint64_t fence = 0;
        uint64_t remainingMs = 0;
    };

    struct ClusterSnapshot
    {
        std::string identifier;
        uint64_t epoch = 0;
        uint32_t shards = 0;
        uint16_t replicas = 0;
        uint16_t fanout = 0;
        size_t pendingOperations = 0;
        size_t activeLeases = 0;
        size_t storedBytes = 0;
        uint64_t quarantineMs = 0;
        std::vector<PeerSnapshot> peers;
        // Only the keys requested by Inspect; never serialized as a lease directory.
        std::map<std::string, LeaseSnapshot> ownership;
    };

    struct SessionSnapshot : TrafficMetrics
    {
        std::string identifier;
        std::string realm;
        std::string transport;
        bool online = false;
        bool authenticated = false;
        bool resumable = false;
        bool owner = false;
        uint64_t observedAt = 0;
        uint64_t publications = 0;
        uint64_t deliveries = 0;
        std::optional<uint64_t> idleMs;
        std::optional<uint64_t> expiresInMs;
        std::optional<LeaseSnapshot> lease;
        uint32_t keepaliveSeconds = 0;
        size_t subscriptions = 0;
        size_t registrations = 0;
        size_t responders = 0;
        size_t requesters = 0;
        size_t pending = 0;
        size_t pendingBytes = 0;
        size_t queuedBytes = 0;
        size_t replay = 0;
        size_t replayBytes = 0;
        size_t requests = 0;
        size_t responseRoutes = 0;
    };

    struct BrokerSnapshot
    {
        std::string identifier;
        std::string instance;
        std::string address;
        std::string transport;
        uint16_t port = 0;
        uint64_t collectedAt = 0;
        uint64_t uptimeMs = 0;
        bool durable = false;
        bool authenticationRequired = false;
        BrokerMetrics metrics;
        size_t maxSessions = 0;
        size_t maxInflight = 0;
        size_t maxRetainedBytes = 0;
        size_t maxStateBytes = 0;
        std::optional<uint64_t> storedBytes;
        size_t omittedSessions = 0;
        std::vector<SessionSnapshot> sessions;
        std::optional<ClusterSnapshot> cluster;
    };

    EMPY_MQ_API std::string SnapshotJson(const BrokerSnapshot& snapshot);
}
