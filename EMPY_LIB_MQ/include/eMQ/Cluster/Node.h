#pragma once
#include "Storage.h"
#include "../Network/IOContext.h"
#include "../Monitoring/Snapshot.h"

namespace eMQ
{
    struct ClusterPeer
    {
        std::string identifier;
        std::string host = "127.0.0.1";
        uint16_t port = 0;
        std::string serverName;
        std::string fingerprint;
    };

    struct ClusterOptions
    {
        std::string identifier;
        std::string address = "127.0.0.1";
        uint16_t port = 0;
        std::string directory;
        FabricOptions fabric;
        std::vector<ClusterPeer> peers;
        Buffer epochKey;
        TransportOptions transport;
        uint32_t leaseMilliseconds = 3000;
        uint32_t timeoutMilliseconds = 700;
        size_t maxBytes = 256 * 1024 * 1024;
        size_t maxRecords = 65536;
        size_t maxOperations = 1024;
        // Per-session inbox admission budget on each replica (paper section IV-J).
        // Zero derives max(maxBytes / 16, packet budget) bytes and maxRecords / 16 records.
        size_t maxInboxBytes = 0;
        size_t maxInboxRecords = 0;
    };

    struct ClusterProfile
    {
        std::string broker;
        std::string session;
        ProfileEntry profile;
        uint64_t expiry = 0;
        bool online = false;
    };

    struct ClusterDelivery
    {
        Digest identity{};
        std::string sender;
        std::string session;
        PublishRequest publication;
        uint64_t expiry = 0;
    };

    // Calls below block the caller, never the node's own I/O executor.
    // Broker protocol state and backplane state therefore use distinct executors.
    struct EMPY_MQ_API ClusterNode
    {
        explicit ClusterNode(ClusterOptions options);
        ~ClusterNode();
        ClusterNode(const ClusterNode&) = delete;
        ClusterNode& operator=(const ClusterNode&) = delete;

        uint16_t Start();
        void Stop();
        size_t Connected();
        ClusterSnapshot Inspect(const std::vector<std::string>& sessions = {});
        Buffer Acquire(std::string key, uint32_t timeout = 5000);
        bool Owns(std::string_view key);
        void Commit(std::string key, Buffer snapshot, uint64_t expiry = UINT64_MAX);
        Buffer Read(std::string key);
        void Release(std::string key);

        void Profiles(std::string session, const std::vector<ProfileEntry>& profiles, uint64_t expiry, bool online);
        std::vector<ClusterProfile> Resolve(std::string topic, uint8_t qos, bool responders = false);
        void Publish(const std::string& sender, uint64_t sequence, PublishRequest publication,
            const std::vector<ClusterProfile>& targets, uint64_t expiry);
        std::vector<ClusterDelivery> Inbox(std::string session);
        void Acknowledge(const ClusterDelivery& delivery);
        // Consumes several inbox records with concurrent quorum rounds; failures are ignored
        // because an unconsumed record is only redelivered and deduplicated by its receipt.
        void Acknowledge(const std::vector<ClusterDelivery>& deliveries);
        void Retain(const PublishRequest& publication, const ProfileEntry& profile, const Digest& identity);
        std::vector<ClusterDelivery> Retained(const std::vector<ProfileEntry>& profiles);
        const Fabric& Placement() const;
        const std::string& Identifier() const;
        std::function<void(ClusterDelivery)> onDelivery;
        std::function<void(ClusterDelivery)> onTransient;

    private:
        struct Impl;
        std::unique_ptr<Impl> m_Impl;
    };
}
