#pragma once
#include "Fabric.h"

namespace eMQ
{
    using VersionVector = std::map<std::string, uint64_t>;

    struct VersionDot
    {
        std::string origin;
        uint64_t version = 0;
        auto operator<=>(const VersionDot&) const = default;
    };

    struct ProfileAssertion
    {
        VersionDot dot;
        std::string broker;
        std::string client;
        ProfileEntry profile;
    };

    struct ProfileRemoval
    {
        VersionDot dot;
        std::set<VersionDot> observed;
    };

    struct EMPY_MQ_API ObservedProfiles
    {
        ObservedProfiles(std::string replica, std::set<std::string> replicas, size_t budget = 65536);
        void Add(const ProfileAssertion& assertion);
        void Remove(const ProfileRemoval& removal);
        void Merge(const ObservedProfiles& other);
        std::set<VersionDot> Observe(std::string_view broker, std::string_view client, std::string_view topic) const;
        std::vector<std::string> Destinations(std::string_view topic, uint8_t qos, bool responders = false) const;
        void Acknowledge(std::string replica, VersionVector vector);
        void Compact();
        const VersionVector& Versions() const;
        size_t Tombstones() const;

    private:
        void Seen(const VersionDot& dot);
        bool Retired(const VersionDot& dot) const;
        bool Removed(const VersionDot& dot) const;
        std::string m_Replica;
        std::set<std::string> m_Replicas;
        size_t m_Budget;
        size_t m_Entries = 0;
        std::map<VersionDot, ProfileAssertion> m_Adds;
        std::map<VersionDot, ProfileRemoval> m_Removes;
        VersionVector m_Versions;
        VersionVector m_Stable;
        std::map<std::string, std::set<uint64_t>> m_Gaps;
        std::map<std::string, VersionVector> m_Acknowledged;
    };

    struct EMPY_MQ_API SessionQuorum
    {
        SessionQuorum(uint64_t epoch, std::set<std::string> replicas, size_t limit = 1024 * 1024);
        Digest Propose(uint64_t version, Buffer snapshot);
        bool Acknowledge(std::string_view replica, uint64_t epoch, uint64_t version, const Digest& digest);
        uint64_t CommittedVersion() const;
        const Buffer& Snapshot() const;
        size_t Required() const;

    private:
        uint64_t m_Epoch;
        std::set<std::string> m_Replicas;
        size_t m_Limit;
        uint64_t m_Proposed = 0;
        uint64_t m_Committed = 0;
        Digest m_Digest{};
        Buffer m_Pending;
        Buffer m_Snapshot;
        std::set<std::string> m_Acks;
    };

    struct RetainedDescriptor
    {
        Digest identity{};
        std::string topic;
        std::string location;
        uint64_t expiry = 0;
        uint8_t strategy = 0;
        Digest payloadHash{};
    };

    struct DurabilityStore
    {
        virtual ~DurabilityStore() = default;
        virtual RetainedDescriptor Append(const ForwardRecord& record) = 0;
        virtual Buffer Read(const RetainedDescriptor& descriptor) = 0;
    };
}
