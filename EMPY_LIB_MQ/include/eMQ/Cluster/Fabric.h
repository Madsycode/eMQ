#pragma once
#include "../Common/State.h"
#include "../Security/Crypto.h"

namespace eMQ
{
    struct FabricOptions
    {
        uint64_t epoch = 1;
        uint32_t shards = 64;
        uint16_t replicas = 3;
        uint16_t fanout = 2;
        std::vector<std::string> brokers;
        size_t maxDestinations = 4096;
    };

    struct EMPY_MQ_API DisseminationTree
    {
        std::vector<std::string> brokers;
        uint16_t fanout = 2;

        size_t Rank(std::string_view broker) const;
        std::optional<size_t> Parent(size_t rank) const;
        std::vector<size_t> Children(size_t rank) const;
        std::vector<size_t> Subtree(size_t rank) const;
        bool Edge(std::string_view sender, std::string_view receiver) const;
    };

    struct EMPY_MQ_API Fabric
    {
        explicit Fabric(FabricOptions options);
        uint32_t Shard(std::string_view topic) const;
        std::vector<uint32_t> ProfileShards(std::string_view filter) const;
        std::vector<std::string> Replicas(uint32_t shard) const;
        std::vector<std::string> SessionReplicas(std::string_view identifier) const;
        DisseminationTree Tree(std::string_view topic, std::string_view ingress,
            const std::vector<std::string>& destinations) const;
        const FabricOptions& Options() const;

    private:
        std::vector<std::string> Select(std::span<const uint8_t> key) const;
        FabricOptions m_Options;
    };

    EMPY_MQ_API Digest MessageIdentity(std::string_view session, uint64_t sequence,
        std::string_view topic, uint64_t acceptedEpoch, std::span<const uint8_t> payload);

    struct ForwardRecord
    {
        Digest identity{};
        uint64_t acceptedEpoch = 0;
        uint64_t routingEpoch = 0;
        uint64_t sequence = 0;
        uint64_t expires = 0;
        std::string session;
        std::string ingress;
        uint32_t senderRank = 0;
        std::vector<std::string> destinations;
        PublishRequest publication;
    };

    // Private MHF/1 envelope. This is separate from the nine client packet types.
    struct EMPY_MQ_API FabricCodec
    {
        static Buffer Encode(const ForwardRecord& record, std::span<const uint8_t> epochKey,
            size_t limit = 2 * 1024 * 1024);
        static ForwardRecord Decode(std::span<const uint8_t> bytes, std::span<const uint8_t> epochKey,
            size_t limit = 2 * 1024 * 1024, size_t maxDestinations = 4096);
        static void Validate(const ForwardRecord& record, const Fabric& fabric,
            std::string_view authenticatedPeer, std::string_view receiver, uint64_t now);
    };

    struct EMPY_MQ_API EpochFilter
    {
        explicit EpochFilter(uint64_t epoch, size_t capacity = 65536);
        void Advance(uint64_t epoch);
        bool Accept(uint64_t epoch, const Digest& identity);
        size_t Size() const;

    private:
        uint64_t m_Current;
        size_t m_Capacity;
        std::map<uint64_t, std::set<Digest>> m_Epochs;
    };

    struct TenantPolicy
    {
        std::string root;
        std::set<uint32_t> shards;

        bool Allows(const Fabric& fabric, std::string_view topic) const
        {
            auto canonical = ParseTopic(topic).filter;
            auto name = ParseTopic(root).filter;
            return (canonical == name || canonical.starts_with(name + '/')) && shards.contains(fabric.Shard(canonical));
        }
    };

    enum class Admission : uint8_t { Accept, Drop, Defer, Reject };

    struct AdmissionBudget
    {
        size_t maxBytes = 4 * 1024 * 1024;
        size_t maxRecords = 256;

        Admission Reserve(uint8_t qos, size_t bytes, bool unexpired, bool defer = false)
        {
            if (qos > 1 || !unexpired || bytes > maxBytes) return qos ? Admission::Reject : Admission::Drop;
            if (m_Records >= maxRecords || m_Bytes > maxBytes || bytes > maxBytes - m_Bytes)
                return !qos ? Admission::Drop : defer ? Admission::Defer : Admission::Reject;
            m_Bytes += bytes;
            ++m_Records;
            return Admission::Accept;
        }

        void Release(size_t bytes)
        {
            if (!m_Records || bytes > m_Bytes) throw std::logic_error("Admission budget release mismatch");
            m_Bytes -= bytes;
            --m_Records;
        }

        EMPY_INLINE size_t Bytes() const { return m_Bytes; }
        EMPY_INLINE size_t Records() const { return m_Records; }

    private:
        size_t m_Bytes = 0;
        size_t m_Records = 0;
    };
}
