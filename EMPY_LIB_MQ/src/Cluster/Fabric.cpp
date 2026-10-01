#include <eMQ/Cluster/Fabric.h>

namespace eMQ
{
    namespace
    {
        std::span<const uint8_t> Bytes(std::string_view value)
        {
            return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
        }

        void BrokerName(std::string_view name)
        {
            if (name.empty() || name.size() > 255 || !ValidUtf8(name))
                throw std::invalid_argument("Broker identity must be 1..255 UTF-8 bytes");
        }
    }

    size_t DisseminationTree::Rank(std::string_view broker) const
    {
        auto found = std::find(brokers.begin(), brokers.end(), broker);
        if (found == brokers.end()) throw ProtocolError("Broker is not in the forwarding tree", MMQP_REASON_UNAUTHORIZED);
        return static_cast<size_t>(found - brokers.begin());
    }

    std::optional<size_t> DisseminationTree::Parent(size_t rank) const
    {
        if (rank >= brokers.size() || !fanout) throw std::out_of_range("Invalid tree rank or fanout");
        return rank ? std::optional((rank - 1) / fanout) : std::nullopt;
    }

    std::vector<size_t> DisseminationTree::Children(size_t rank) const
    {
        if (rank >= brokers.size() || !fanout) throw std::out_of_range("Invalid tree rank or fanout");
        std::vector<size_t> children;
        for (size_t child = rank * fanout + 1; child < brokers.size() && children.size() < fanout; ++child)
            children.push_back(child);
        return children;
    }

    std::vector<size_t> DisseminationTree::Subtree(size_t rank) const
    {
        if (rank >= brokers.size()) throw std::out_of_range("Invalid tree rank");
        std::vector<size_t> nodes{rank};
        for (size_t offset = 0; offset < nodes.size(); ++offset)
        {
            auto children = this->Children(nodes[offset]);
            nodes.insert(nodes.end(), children.begin(), children.end());
        }
        return nodes;
    }

    bool DisseminationTree::Edge(std::string_view sender, std::string_view receiver) const
    {
        auto parent = this->Parent(this->Rank(receiver));
        return parent && *parent == this->Rank(sender);
    }

    Fabric::Fabric(FabricOptions options): m_Options(std::move(options))
    {
        if (!m_Options.epoch || !m_Options.shards || m_Options.shards > 65536 || !m_Options.replicas
            || m_Options.replicas > m_Options.brokers.size() || !m_Options.fanout || m_Options.fanout > 64
            || !m_Options.maxDestinations || m_Options.maxDestinations > UINT16_MAX)
            throw std::invalid_argument("Invalid fabric epoch, shards, replica count or fanout");
        for (const auto& broker : m_Options.brokers) BrokerName(broker);
        std::sort(m_Options.brokers.begin(), m_Options.brokers.end());
        if (std::adjacent_find(m_Options.brokers.begin(), m_Options.brokers.end()) != m_Options.brokers.end())
            throw std::invalid_argument("Duplicate broker identity");
    }

    uint32_t Fabric::Shard(std::string_view topic) const
    {
        auto canonical = ParseTopic(topic).filter;
        return static_cast<uint32_t>(Hash64(Bytes(canonical)) % m_Options.shards);
    }

    std::vector<uint32_t> Fabric::ProfileShards(std::string_view filter) const
    {
        auto canonical = ParseTopic(filter, true).filter;
        if (!HasWildcard(canonical)) return {this->Shard(canonical)};
        std::vector<uint32_t> shards;
        shards.reserve(m_Options.shards);
        for (uint32_t shard = 0; shard < m_Options.shards; ++shard) shards.push_back(shard);
        return shards;
    }

    std::vector<std::string> Fabric::Select(std::span<const uint8_t> key) const
    {
        std::vector<std::pair<uint64_t, std::string>> ranked;
        for (const auto& broker : m_Options.brokers)
        {
            Writer writer(1024);
            writer.Bytes(key);
            writer.U64(m_Options.epoch);
            writer.String(broker);
            ranked.emplace_back(Hash64(writer.Data()), broker);
        }
        std::sort(ranked.begin(), ranked.end(), [] (const auto& left, const auto& right)
        {
            return left.first != right.first ? left.first > right.first : left.second < right.second;
        });
        std::vector<std::string> selected;
        for (uint16_t i = 0; i < m_Options.replicas; ++i) selected.push_back(ranked[i].second);
        return selected;
    }

    std::vector<std::string> Fabric::Replicas(uint32_t shard) const
    {
        if (shard >= m_Options.shards) throw std::out_of_range("Unknown profile shard");
        Writer writer(8);
        writer.U8(1);
        writer.U32(shard);
        return this->Select(writer.Data());
    }

    std::vector<std::string> Fabric::SessionReplicas(std::string_view identifier) const
    {
        if (identifier.empty() || !ValidUtf8(identifier)) throw std::invalid_argument("Invalid session identity");
        Writer writer(257);
        writer.U8(2);
        writer.String(identifier);
        return this->Select(writer.Data());
    }

    DisseminationTree Fabric::Tree(std::string_view topic, std::string_view ingress,
        const std::vector<std::string>& destinations) const
    {
        auto canonical = ParseTopic(topic).filter;
        if (destinations.size() > m_Options.maxDestinations)
            throw ProtocolError("Tree exceeds destination budget", MMQP_REASON_POLICY_VIOLATION);
        if (!std::binary_search(m_Options.brokers.begin(), m_Options.brokers.end(), std::string(ingress)))
            throw ProtocolError("Ingress is outside this epoch's membership", MMQP_REASON_UNAUTHORIZED);
        std::set<std::string> distinct(destinations.begin(), destinations.end());
        distinct.erase(std::string(ingress));
        std::vector<std::pair<uint64_t, std::string>> ranked;
        for (const auto& broker : distinct)
        {
            if (!std::binary_search(m_Options.brokers.begin(), m_Options.brokers.end(), broker))
                throw ProtocolError("Destination is outside this epoch's membership", MMQP_REASON_UNAUTHORIZED);
            Writer writer(1024);
            writer.String(canonical);
            writer.U64(m_Options.epoch);
            writer.String(broker);
            ranked.emplace_back(Hash64(writer.Data()), broker);
        }
        std::sort(ranked.begin(), ranked.end());
        DisseminationTree tree{{std::string(ingress)}, m_Options.fanout};
        for (const auto& [hash, broker] : ranked) tree.brokers.push_back(broker);
        return tree;
    }

    const FabricOptions& Fabric::Options() const { return m_Options; }

    Digest MessageIdentity(std::string_view session, uint64_t sequence,
        std::string_view topic, uint64_t acceptedEpoch, std::span<const uint8_t> payload)
    {
        if (!sequence || !acceptedEpoch || session.empty() || !ValidUtf8(session))
            throw std::invalid_argument("An origin sequence, accepted epoch and session are required");
        Writer writer(1024);
        writer.String("MHF/1/message");
        writer.String(session);
        writer.U64(sequence);
        writer.Bytes(Sha256(payload));
        writer.String(ParseTopic(topic).filter);
        writer.U64(acceptedEpoch);
        return Sha256(writer.Data());
    }

    Buffer FabricCodec::Encode(const ForwardRecord& record, std::span<const uint8_t> epochKey, size_t limit)
    {
        if (epochKey.size() < 32 || record.destinations.size() > UINT16_MAX || !record.routingEpoch || !record.expires)
            throw std::invalid_argument("Invalid forwarding key, epoch or destination count");
        BrokerName(record.ingress);
        if (std::set<std::string>(record.destinations.begin(), record.destinations.end()).size() != record.destinations.size())
            throw ProtocolError("Duplicate forwarding destination");
        if (!SecureEqual(record.identity, MessageIdentity(record.session, record.sequence,
            record.publication.topic, record.acceptedEpoch, record.publication.payload)))
            throw ProtocolError("Forward identity does not match the accepted publication");
        Writer writer(limit);
        writer.String("MHF");
        writer.U8(1);
        writer.U8(1);
        writer.Bytes(record.identity);
        writer.U64(record.acceptedEpoch);
        writer.U64(record.routingEpoch);
        writer.U64(record.sequence);
        writer.U64(record.expires);
        writer.String(record.session);
        writer.String(record.ingress);
        writer.U32(record.senderRank);
        writer.U16(static_cast<uint16_t>(record.destinations.size()));
        for (const auto& broker : record.destinations) { BrokerName(broker); writer.String(broker); }
        auto message = Codec::Encode(record.publication, {true});
        writer.U32(static_cast<uint32_t>(message.size()));
        writer.Bytes(message);
        auto mac = Hmac256(epochKey, writer.Data());
        writer.Bytes(mac);
        return writer.Take();
    }

    ForwardRecord FabricCodec::Decode(std::span<const uint8_t> bytes, std::span<const uint8_t> epochKey,
        size_t limit, size_t maxDestinations)
    {
        if (epochKey.size() < 32 || bytes.size() > limit || bytes.size() < 32)
            throw ProtocolError("Invalid authenticated forwarding envelope", MMQP_REASON_AUTH_FAILED);
        auto body = bytes.first(bytes.size() - 32);
        if (!SecureEqual(Hmac256(epochKey, body), bytes.last(32)))
            throw ProtocolError("Forwarding record MAC failed", MMQP_REASON_AUTH_FAILED);
        Reader reader(body);
        if (reader.String() != "MHF" || reader.U8() != 1 || reader.U8() != 1)
            throw ProtocolError("Unsupported fabric envelope version", MMQP_REASON_UNSUPPORTED_VERSION);
        ForwardRecord record;
        auto identity = reader.Bytes(32);
        std::copy(identity.begin(), identity.end(), record.identity.begin());
        record.acceptedEpoch = reader.U64();
        record.routingEpoch = reader.U64();
        record.sequence = reader.U64();
        record.expires = reader.U64();
        record.session = reader.String();
        record.ingress = reader.String();
        BrokerName(record.ingress);
        record.senderRank = reader.U32();
        auto count = reader.U16();
        if (count > maxDestinations) throw ProtocolError("Forwarding destination budget exceeded");
        std::set<std::string> unique;
        for (uint16_t i = 0; i < count; ++i)
        {
            auto broker = reader.String();
            BrokerName(broker);
            if (!unique.insert(broker).second) throw ProtocolError("Duplicate forwarding destination");
            record.destinations.emplace_back(std::move(broker));
        }
        auto length = reader.U32();
        auto message = Codec::Decode(reader.Bytes(length), {true});
        auto publication = std::get_if<PublishRequest>(&message);
        if (!publication || publication->topic.empty()) throw ProtocolError("Forwarding requires a concrete topic");
        record.publication = std::move(*publication);
        reader.Finish();
        if (!SecureEqual(record.identity, MessageIdentity(record.session, record.sequence,
            record.publication.topic, record.acceptedEpoch, record.publication.payload)))
            throw ProtocolError("Forwarding identity mismatch", MMQP_REASON_AUTH_FAILED);
        return record;
    }

    void FabricCodec::Validate(const ForwardRecord& record, const Fabric& fabric,
        std::string_view authenticatedPeer, std::string_view receiver, uint64_t now)
    {
        if (!SecureEqual(record.identity, MessageIdentity(record.session, record.sequence,
            record.publication.topic, record.acceptedEpoch, record.publication.payload)))
            throw ProtocolError("Forwarding identity mismatch", MMQP_REASON_AUTH_FAILED);
        if (record.routingEpoch != fabric.Options().epoch || record.acceptedEpoch > record.routingEpoch || record.expires <= now)
            throw ProtocolError("Stale, future or expired forwarding record", MMQP_REASON_UNAUTHORIZED);
        auto tree = fabric.Tree(record.publication.topic, record.ingress, record.destinations);
        if (record.senderRank >= tree.brokers.size() || tree.brokers[record.senderRank] != authenticatedPeer
            || !tree.Edge(authenticatedPeer, receiver))
            throw ProtocolError("Forwarding edge does not match the authenticated tree parent", MMQP_REASON_UNAUTHORIZED);
    }

    EpochFilter::EpochFilter(uint64_t epoch, size_t capacity): m_Current(epoch), m_Capacity(capacity)
    {
        if (!epoch || !capacity) throw std::invalid_argument("Invalid epoch filter bounds");
        m_Epochs.emplace(epoch, std::set<Digest>{});
    }

    void EpochFilter::Advance(uint64_t epoch)
    {
        if (epoch <= m_Current) throw std::invalid_argument("Cluster epoch must advance");
        m_Current = epoch;
        m_Epochs.emplace(epoch, std::set<Digest>{});
        while (m_Epochs.size() > 2) m_Epochs.erase(m_Epochs.begin());
    }

    bool EpochFilter::Accept(uint64_t epoch, const Digest& identity)
    {
        auto found = m_Epochs.find(epoch);
        if (found == m_Epochs.end()) throw ProtocolError("Epoch is outside the deduplication grace window");
        // Identity remains stable when the routing epoch changes.
        for (const auto& [period, identities] : m_Epochs)
            if (identities.contains(identity)) return false;
        if (found->second.size() >= m_Capacity)
            throw ProtocolError("Epoch duplicate filter is full", MMQP_REASON_BUSY);
        found->second.insert(identity);
        return true;
    }

    size_t EpochFilter::Size() const
    {
        size_t size = 0;
        for (const auto& [epoch, identities] : m_Epochs) size += identities.size();
        return size;
    }
}
