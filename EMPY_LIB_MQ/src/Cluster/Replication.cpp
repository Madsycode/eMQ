#include <eMQ/Cluster/Replication.h>

namespace eMQ
{
    namespace
    {
        uint64_t Version(const VersionVector& vector, const std::string& origin)
        {
            auto found = vector.find(origin);
            return found == vector.end() ? 0 : found->second;
        }

        bool Same(const ProfileAssertion& left, const ProfileAssertion& right)
        {
            return left.broker == right.broker && left.client == right.client
                && Codec::Encode(ProfileRequest{1, {left.profile}}) == Codec::Encode(ProfileRequest{1, {right.profile}});
        }

        void CheckName(const std::string& name)
        {
            if (name.empty() || name.size() > 255 || !ValidUtf8(name))
                throw std::invalid_argument("Profile identities must be 1..255 UTF-8 bytes");
        }

        void CheckDot(const VersionDot& dot)
        {
            CheckName(dot.origin);
            if (!dot.version) throw std::invalid_argument("Profile operation versions start at one");
        }
    }

    ObservedProfiles::ObservedProfiles(std::string replica, std::set<std::string> replicas, size_t budget):
        m_Replica(std::move(replica)), m_Replicas(std::move(replicas)), m_Budget(budget)
    {
        if (!budget || !m_Replicas.contains(m_Replica)) throw std::invalid_argument("Invalid profile replica set");
        for (const auto& member : m_Replicas) CheckName(member);
        m_Acknowledged.emplace(m_Replica, VersionVector{});
    }

    void ObservedProfiles::Seen(const VersionDot& dot)
    {
        CheckDot(dot);
        if (!m_Versions.contains(dot.origin) && m_Versions.size() >= m_Budget)
            throw ProtocolError("Profile origin budget exhausted", MMQP_REASON_BUSY);
        auto& version = m_Versions[dot.origin];
        if (dot.version > version)
        {
            auto& gaps = m_Gaps[dot.origin];
            if (gaps.size() >= m_Budget && !gaps.contains(dot.version))
                throw ProtocolError("Out-of-order profile budget exhausted", MMQP_REASON_BUSY);
            gaps.insert(dot.version);
            while (version != UINT64_MAX && gaps.erase(version + 1)) ++version;
        }
        m_Acknowledged[m_Replica] = m_Versions;
    }

    bool ObservedProfiles::Retired(const VersionDot& dot) const
    {
        return dot.version <= Version(m_Stable, dot.origin);
    }

    bool ObservedProfiles::Removed(const VersionDot& dot) const
    {
        return std::any_of(m_Removes.begin(), m_Removes.end(), [&] (const auto& removal)
        {
            return removal.second.observed.contains(dot);
        });
    }

    void ObservedProfiles::Add(const ProfileAssertion& assertion)
    {
        CheckDot(assertion.dot);
        CheckName(assertion.broker);
        CheckName(assertion.client);
        if (assertion.broker.empty() || assertion.client.empty() || assertion.profile.topic.empty()
            || assertion.profile.type == MMQP_PROREQ_TYPE_UNS)
            throw std::invalid_argument("Invalid profile assertion");
        Codec::Encode(ProfileRequest{1, {assertion.profile}});
        if (m_Removes.contains(assertion.dot)) throw ProtocolError("An operation dot cannot identify both an add and a removal");
        if (auto found = m_Adds.find(assertion.dot); found != m_Adds.end())
        {
            if (!Same(found->second, assertion)) throw ProtocolError("Conflicting profile operation identity");
            return;
        }
        if (this->Retired(assertion.dot)) return;
        if (m_Entries >= m_Budget)
            throw ProtocolError("Profile shard operation budget exhausted", MMQP_REASON_BUSY);
        this->Seen(assertion.dot);
        m_Adds.emplace(assertion.dot, assertion);
        ++m_Entries;
    }

    void ObservedProfiles::Remove(const ProfileRemoval& removal)
    {
        CheckDot(removal.dot);
        if (removal.observed.empty() || removal.observed.size() > m_Budget)
            throw std::invalid_argument("Removal must name its observed assertion dots");
        for (const auto& dot : removal.observed) CheckDot(dot);
        if (removal.observed.contains(removal.dot) || m_Adds.contains(removal.dot))
            throw ProtocolError("An operation dot cannot identify both an add and a removal");
        if (auto found = m_Removes.find(removal.dot); found != m_Removes.end())
        {
            if (found->second.observed != removal.observed) throw ProtocolError("Conflicting removal identity");
            return;
        }
        if (this->Retired(removal.dot)) return;
        if (m_Entries >= m_Budget || removal.observed.size() >= m_Budget - m_Entries)
            throw ProtocolError("Profile shard operation budget exhausted", MMQP_REASON_BUSY);
        this->Seen(removal.dot);
        m_Removes.emplace(removal.dot, removal);
        m_Entries += removal.observed.size() + 1;
    }

    void ObservedProfiles::Merge(const ObservedProfiles& other)
    {
        if (m_Replicas != other.m_Replicas) throw std::invalid_argument("Profile epochs must share a replica set");
        auto merged = *this;
        for (auto it = merged.m_Adds.begin(); it != merged.m_Adds.end();)
        {
            if (other.Retired(it->first) && !other.m_Adds.contains(it->first))
            {
                it = merged.m_Adds.erase(it);
                --merged.m_Entries;
            }
            else ++it;
        }
        for (const auto& [dot, assertion] : other.m_Adds) merged.Add(assertion);
        for (const auto& [dot, removal] : other.m_Removes) merged.Remove(removal);
        for (const auto& [origin, version] : other.m_Stable)
            merged.m_Stable[origin] = std::max(merged.m_Stable[origin], version);
        for (const auto& [origin, version] : other.m_Versions)
            merged.m_Versions[origin] = std::max(merged.m_Versions[origin], version);
        for (auto& [origin, gaps] : merged.m_Gaps)
        {
            std::erase_if(gaps, [&] (auto value) { return value <= merged.m_Versions[origin]; });
            auto& version = merged.m_Versions[origin];
            while (version != UINT64_MAX && gaps.erase(version + 1)) ++version;
        }
        if (merged.m_Versions.size() > merged.m_Budget)
            throw ProtocolError("Profile origin budget exhausted", MMQP_REASON_BUSY);
        merged.m_Acknowledged[merged.m_Replica] = merged.m_Versions;
        *this = std::move(merged);
    }

    std::set<VersionDot> ObservedProfiles::Observe(std::string_view broker, std::string_view client, std::string_view topic) const
    {
        std::set<VersionDot> observed;
        auto canonical = CanonicalTopic(topic);
        for (const auto& [dot, assertion] : m_Adds)
            if (!this->Removed(dot) && assertion.broker == broker && assertion.client == client
                && CanonicalTopic(assertion.profile.topic) == canonical)
                observed.insert(dot);
        return observed;
    }

    std::vector<std::string> ObservedProfiles::Destinations(std::string_view topic, uint8_t qos, bool responders) const
    {
        if (qos > 1) throw std::invalid_argument("Unsupported publication QoS");
        ParseTopic(topic);
        std::set<std::string> brokers;
        for (const auto& [dot, assertion] : m_Adds)
        {
            const auto& profile = assertion.profile;
            if (!this->Removed(dot) && profile.type == MMQP_PROREQ_TYPE_SUB && profile.qos >= qos
                && (!responders || profile.request) && TopicMatches(profile.topic, topic))
                brokers.insert(assertion.broker);
        }
        return {brokers.begin(), brokers.end()};
    }

    void ObservedProfiles::Acknowledge(std::string replica, VersionVector vector)
    {
        if (!m_Replicas.contains(replica) || replica == m_Replica)
            throw std::invalid_argument("Only a remote member can acknowledge profile versions");
        if (vector.size() > m_Budget) throw std::invalid_argument("Replica vector exceeds the origin budget");
        for (const auto& [origin, version] : vector) CheckName(origin);
        auto& acknowledged = m_Acknowledged[replica];
        for (const auto& [origin, version] : acknowledged)
            if (Version(vector, origin) < version) throw std::invalid_argument("Replica acknowledgement regressed");
        acknowledged = std::move(vector);
    }

    void ObservedProfiles::Compact()
    {
        if (m_Acknowledged.size() != m_Replicas.size()) return;
        VersionVector stable;
        for (const auto& [origin, version] : m_Versions)
        {
            auto floor = version;
            for (const auto& [replica, vector] : m_Acknowledged) floor = std::min(floor, Version(vector, origin));
            stable[origin] = floor;
        }
        for (auto it = m_Removes.begin(); it != m_Removes.end();)
        {
            bool safe = it->first.version <= Version(stable, it->first.origin);
            for (const auto& dot : it->second.observed)
                safe = safe && dot.version <= Version(stable, dot.origin);
            if (!safe) { ++it; continue; }
            for (const auto& dot : it->second.observed) m_Entries -= m_Adds.erase(dot);
            m_Entries -= it->second.observed.size() + 1;
            it = m_Removes.erase(it);
        }
        for (const auto& [origin, version] : stable) m_Stable[origin] = std::max(m_Stable[origin], version);
    }

    const VersionVector& ObservedProfiles::Versions() const { return m_Versions; }
    size_t ObservedProfiles::Tombstones() const { return m_Removes.size(); }

    SessionQuorum::SessionQuorum(uint64_t epoch, std::set<std::string> replicas, size_t limit):
        m_Epoch(epoch), m_Replicas(std::move(replicas)), m_Limit(limit)
    {
        if (!epoch || m_Replicas.empty() || !limit) throw std::invalid_argument("Invalid session replica unit");
    }

    Digest SessionQuorum::Propose(uint64_t version, Buffer snapshot)
    {
        if (!version || snapshot.size() > m_Limit || m_Committed == UINT64_MAX || version != m_Committed + 1)
            throw ProtocolError("Invalid session snapshot version or size");
        auto digest = Sha256(snapshot);
        if (m_Proposed > m_Committed)
        {
            if (version != m_Proposed || !SecureEqual(m_Digest, digest))
                throw ProtocolError("An uncommitted snapshot is already pending");
            return digest;
        }
        m_Proposed = version;
        m_Pending = std::move(snapshot);
        m_Digest = digest;
        m_Acks.clear();
        return digest;
    }

    bool SessionQuorum::Acknowledge(std::string_view replica, uint64_t epoch, uint64_t version, const Digest& digest)
    {
        if (epoch != m_Epoch || version != m_Proposed || !version || !m_Replicas.contains(std::string(replica))
            || !SecureEqual(digest, m_Digest))
            throw ProtocolError("Invalid session replica acknowledgement", MMQP_REASON_UNAUTHORIZED);
        m_Acks.emplace(replica);
        if (m_Acks.size() < this->Required()) return false;
        if (m_Committed != m_Proposed)
        {
            m_Committed = m_Proposed;
            m_Snapshot = m_Pending;
        }
        return true;
    }

    uint64_t SessionQuorum::CommittedVersion() const { return m_Committed; }
    size_t SessionQuorum::Required() const { return m_Replicas.size() / 2 + 1; }
    const Buffer& SessionQuorum::Snapshot() const
    {
        if (!m_Committed) throw std::logic_error("No session snapshot has committed");
        return m_Snapshot;
    }
}
