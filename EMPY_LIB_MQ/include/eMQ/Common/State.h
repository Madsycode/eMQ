#pragma once
#include "Message.h"
#include <bitset>
#include <map>
#include <set>

namespace eMQ
{
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    enum class Reception : uint8_t { New, Duplicate, OutsideWindow };

    // Publication PIDs cycle through 1..65535 using serial-number arithmetic
    // (RFC 1982). Both endpoints keep a 64-bit sequence n and transmit
    // WirePid(n), so WirePid(n) == n (mod 65535). At most MMQP_PID_WINDOW
    // publications are outstanding, far below half the space, so a receiver
    // always distinguishes a new PID from a retry and no session renewal is needed.
    inline constexpr uint32_t MMQP_PID_SPACE = 65535;
    inline constexpr uint32_t MMQP_PID_HORIZON = MMQP_PID_SPACE / 2;
    inline constexpr uint32_t MMQP_PID_WINDOW = 256;

    EMPY_INLINE uint16_t WirePid(uint64_t sequence)
    {
        return sequence ? static_cast<uint16_t>((sequence - 1) % MMQP_PID_SPACE + 1) : 0;
    }

    // Forward distance from sequence position n to the wire PID, modulo 65535.
    EMPY_INLINE uint32_t PidAhead(uint64_t sequence, uint16_t packetId)
    {
        return static_cast<uint32_t>((packetId + MMQP_PID_SPACE - sequence % MMQP_PID_SPACE) % MMQP_PID_SPACE);
    }

    struct ReceiveWindow
    {
        explicit ReceiveWindow(bool ordered = true, uint16_t size = MMQP_PID_WINDOW):
            m_Ordered(ordered), m_Size(size)
        {
            if (!size || size > m_Bits.size()) throw std::invalid_argument("Receive window must be in 1..256");
        }

        // Positions at or up to half the space behind the checkpoint are earlier
        // PIDs: the caller acknowledges them only against a matching receipt.
        Reception Inspect(uint16_t packetId) const
        {
            if (!packetId) throw ProtocolError("Zero publication PID", MMQP_REASON_INVALID_PID);
            auto ahead = PidAhead(m_Checkpoint, packetId);
            if (!ahead || ahead > MMQP_PID_HORIZON) return Reception::Duplicate;
            if (!m_Ordered && ahead > m_Size) return Reception::OutsideWindow;
            return !m_Ordered && m_Bits[ahead - 1] ? Reception::Duplicate : Reception::New;
        }

        // The 64-bit sequence of a wire PID relative to the current checkpoint.
        uint64_t Sequence(uint16_t packetId) const
        {
            auto ahead = PidAhead(m_Checkpoint, packetId);
            if (ahead && ahead <= MMQP_PID_HORIZON) return m_Checkpoint + ahead;
            auto behind = (MMQP_PID_SPACE - ahead) % MMQP_PID_SPACE;
            if (behind > m_Checkpoint) throw ProtocolError("PID precedes the session", MMQP_REASON_INVALID_PID);
            return m_Checkpoint - behind;
        }

        void Commit(uint16_t packetId)
        {
            auto state = this->Inspect(packetId);
            if (state == Reception::Duplicate) return;
            if (state == Reception::OutsideWindow)
                throw ProtocolError("Publication is beyond the receive window", MMQP_REASON_INVALID_PID);
            auto ahead = PidAhead(m_Checkpoint, packetId);
            if (m_Ordered) m_Checkpoint += ahead;
            else
            {
                m_Bits.set(ahead - 1);
                while (m_Bits[0])
                {
                    ++m_Checkpoint;
                    m_Bits >>= 1;
                }
            }
        }

        // The 64-bit sequence of the highest contiguous accepted publication.
        EMPY_INLINE uint64_t Checkpoint() const { return m_Checkpoint; }

        void Save(Writer& writer) const
        {
            writer.U8(m_Ordered);
            writer.U16(m_Size);
            writer.U64(m_Checkpoint);
            for (size_t i = 0; i < 256; i += 8)
            {
                uint8_t byte = 0;
                for (size_t bit = 0; bit < 8; ++bit) if (m_Bits[i + bit]) byte |= static_cast<uint8_t>(1U << bit);
                writer.U8(byte);
            }
        }

        // Legacy (ESS4) windows stored a 16-bit checkpoint, which never wrapped.
        static ReceiveWindow Load(Reader& reader, bool legacy = false)
        {
            auto ordered = reader.U8();
            if (ordered > 1) throw ProtocolError("Invalid stored receive window");
            ReceiveWindow result(ordered != 0, reader.U16());
            result.m_Checkpoint = legacy ? reader.U16() : reader.U64();
            for (size_t i = 0; i < 256; i += 8)
            {
                auto byte = reader.U8();
                for (size_t bit = 0; bit < 8; ++bit) result.m_Bits[i + bit] = (byte & (1U << bit)) != 0;
            }
            if (result.m_Ordered && result.m_Bits.any()) throw ProtocolError("Invalid ordered receive bitmap");
            return result;
        }

    private:
        bool m_Ordered;
        uint16_t m_Size;
        uint64_t m_Checkpoint = 0;
        std::bitset<256> m_Bits;
    };

    // Nonwrapping 16-bit identifiers (profile requests, responder correlation IDs).
    struct PacketSequence
    {
        explicit PacketSequence(uint16_t initial = 0): m_Value(initial) {}

        uint16_t Next()
        {
            m_Value = this->Peek();
            return m_Value;
        }

        uint16_t Peek() const
        {
            if (m_Value == UINT16_MAX)
                throw ProtocolError("Identifier space exhausted; establish a fresh session", MMQP_REASON_INVALID_PID);
            return static_cast<uint16_t>(m_Value + 1);
        }

        EMPY_INLINE uint16_t Value() const { return m_Value; }

    private:
        uint16_t m_Value = 0;
    };

    // Outgoing publication PIDs: a 64-bit sequence whose wire form wraps after 65535.
    struct PublicationSequence
    {
        explicit PublicationSequence(uint64_t issued = 0): m_Issued(issued) {}

        EMPY_INLINE uint16_t Peek() const { return WirePid(m_Issued + 1); }
        EMPY_INLINE uint16_t Next() { return WirePid(++m_Issued); }
        EMPY_INLINE uint64_t Issued() const { return m_Issued; }

        // True when the PID names one of the last half-space publications sent.
        bool Sent(uint16_t packetId) const
        {
            if (!packetId || !m_Issued) return false;
            auto behind = PidAhead(packetId, WirePid(m_Issued));
            return behind < MMQP_PID_HORIZON && behind < m_Issued;
        }

        uint64_t Sequence(uint16_t packetId) const
        {
            if (!this->Sent(packetId)) throw ProtocolError("PID was not sent in this session", MMQP_REASON_INVALID_PID);
            return m_Issued - PidAhead(packetId, WirePid(m_Issued));
        }

    private:
        uint64_t m_Issued = 0;
    };

    struct CorrelationIds
    {
        // Allocate the bitmap only for sessions using request/response. IDs are
        // never reused within a session, including after completion or timeout.
        bool Contains(uint16_t identifier) const
        {
            return identifier && !m_Bits.empty() && (m_Bits[identifier / 8] & (1U << (identifier % 8)));
        }

        uint16_t Peek() const
        {
            if (m_Next > UINT16_MAX)
                throw ProtocolError("Correlation IDs exhausted; establish a fresh session", MMQP_REASON_INVALID_CORRELATION);
            return static_cast<uint16_t>(m_Next);
        }

        void Claim(uint16_t identifier)
        {
            if (!identifier || this->Contains(identifier))
                throw ProtocolError("Correlation ID is zero or already used in this session", MMQP_REASON_INVALID_CORRELATION);
            if (m_Bits.empty()) m_Bits.resize(8192);
            m_Bits[identifier / 8] |= static_cast<uint8_t>(1U << (identifier % 8));
            while (m_Next <= UINT16_MAX && this->Contains(static_cast<uint16_t>(m_Next))) ++m_Next;
        }

        void Save(Writer& writer) const
        {
            writer.U8(!m_Bits.empty());
            writer.Bytes(m_Bits);
        }

        static CorrelationIds Load(Reader& reader)
        {
            CorrelationIds result;
            auto present = reader.U8();
            if (present > 1) throw ProtocolError("Invalid stored correlation bitmap");
            if (present)
            {
                auto bytes = reader.Bytes(8192);
                if (bytes[0] & 1) throw ProtocolError("Stored correlation ID zero is reserved");
                result.m_Bits.assign(bytes.begin(), bytes.end());
                while (result.m_Next <= UINT16_MAX && result.Contains(static_cast<uint16_t>(result.m_Next))) ++result.m_Next;
            }
            return result;
        }

    private:
        Buffer m_Bits;
        uint32_t m_Next = 1;
    };

    struct AliasMap
    {
        bool Map(uint16_t alias, std::string_view topic, bool confirmed = false)
        {
            if (!alias) throw ProtocolError("Alias zero is reserved", MMQP_REASON_INVALID_ALIAS);
            auto name = ParseTopic(topic, true).filter;
            if (HasWildcard(name)) throw ProtocolError("Wildcard aliases must be zero", MMQP_REASON_INVALID_ALIAS);
            auto canonical = CanonicalTopic(topic);
            if (auto it = m_Topics.find(alias); it != m_Topics.end() && it->second != canonical)
                throw ProtocolError("Alias already names another topic", MMQP_REASON_INVALID_ALIAS);
            if (auto it = m_Aliases.find(canonical); it != m_Aliases.end() && it->second != alias)
                throw ProtocolError("Topic already has another alias", MMQP_REASON_INVALID_ALIAS);
            bool added = m_Topics.emplace(alias, canonical).second;
            m_Aliases.emplace(canonical, alias);
            if (confirmed) m_Confirmed.insert(alias);
            return added;
        }

        uint16_t Assign(std::string_view topic, uint16_t first = 1, uint16_t last = UINT16_MAX)
        {
            auto alias = this->Candidate(topic, first, last);
            this->Map(alias, topic);
            m_Next = std::max<uint32_t>(m_Next, static_cast<uint32_t>(alias) + 1);
            return alias;
        }

        uint16_t Candidate(std::string_view topic, uint16_t first = 1, uint16_t last = UINT16_MAX) const
        {
            auto pattern = ParseTopic(topic, true);
            if (HasWildcard(pattern.filter))
                throw ProtocolError("Cannot allocate an alias for a wildcard", MMQP_REASON_INVALID_ALIAS);
            auto canonical = CanonicalTopic(topic);
            if (auto found = m_Aliases.find(canonical); found != m_Aliases.end()) return found->second;
            if (!first || first > last) throw std::invalid_argument("Invalid alias allocation range");
            auto next = std::max<uint32_t>(m_Next, first);
            while (next <= last && m_Topics.contains(static_cast<uint16_t>(next))) ++next;
            if (next > last)
                throw ProtocolError("Session alias namespace exhausted", MMQP_REASON_INVALID_ALIAS);
            return static_cast<uint16_t>(next);
        }

        const std::string& Resolve(uint16_t alias) const
        {
            auto found = m_Topics.find(alias);
            if (found == m_Topics.end()) throw ProtocolError("Unknown topic alias", MMQP_REASON_INVALID_ALIAS);
            return found->second;
        }

        std::optional<uint16_t> Find(std::string_view topic) const
        {
            auto found = m_Aliases.find(CanonicalTopic(topic));
            return found == m_Aliases.end() ? std::nullopt : std::optional(found->second);
        }

        void Confirm(uint16_t alias, std::string_view topic)
        {
            if (this->Resolve(alias) != CanonicalTopic(topic))
                throw ProtocolError("Alias acknowledgement does not match", MMQP_REASON_INVALID_ALIAS);
            m_Confirmed.insert(alias);
        }

        EMPY_INLINE bool Confirmed(uint16_t alias) const { return m_Confirmed.contains(alias); }
        EMPY_INLINE size_t Size() const { return m_Topics.size(); }

        void Save(Writer& writer) const
        {
            writer.U32(m_Next);
            writer.U32(static_cast<uint32_t>(m_Topics.size()));
            for (const auto& [alias, topic] : m_Topics)
            {
                writer.U16(alias);
                writer.String(topic);
                writer.U8(m_Confirmed.contains(alias));
            }
        }

        static AliasMap Load(Reader& reader)
        {
            AliasMap result;
            auto next = reader.U32();
            auto count = reader.U32();
            if (!next || next > 65536 || count > UINT16_MAX) throw ProtocolError("Invalid stored alias map");
            for (uint32_t i = 0; i < count; ++i)
            {
                auto alias = reader.U16();
                auto topic = reader.String();
                auto confirmed = reader.U8();
                if (confirmed > 1 || !result.Map(alias, topic, confirmed != 0)) throw ProtocolError("Invalid stored alias entry");
            }
            result.m_Next = next;
            return result;
        }

    private:
        std::map<uint16_t, std::string> m_Topics;
        std::map<std::string, uint16_t> m_Aliases;
        std::set<uint16_t> m_Confirmed;
        uint32_t m_Next = 1;
    };

    struct EMPY_MQ_API ProfileTable
    {
        AliasMap publishers;
        AliasMap subscribers;
        std::vector<ProfileEntry> entries;

        void Apply(const ProfileRequest& request, size_t limit = 256);
        std::optional<ProfileEntry> Registration(std::string_view topic, uint8_t type) const;
        std::vector<ProfileEntry> Subscriptions(std::string_view topic, uint8_t qos, bool responders = false) const;
        bool Subscribed(std::string_view topic, uint8_t qos, bool responders = false) const;

        // Allocation-free visit of the subscriptions accepting a canonical topic.
        template<typename _Visit>
        void ForEachSubscription(std::string_view topic, uint8_t qos, bool responders, _Visit&& visit) const
        {
            for (const auto& entry : entries)
                if (entry.type == MMQP_PROREQ_TYPE_SUB && qos <= entry.qos
                    && (!responders || entry.request) && MatchTopic(entry.topic, topic))
                    visit(entry);
        }
    };

    struct PendingPublication
    {
        PublishRequest message;
        TimePoint retry;
        TimePoint expiry;
        uint32_t attempts = 0;
        size_t bytes = 0;
    };

    struct PendingQueue
    {
        // Ordered by the 64-bit publication sequence, so retries keep their send
        // order when wire PIDs wrap. Wire PIDs remain unique among these entries.
        std::map<uint64_t, PendingPublication> entries;
        size_t maxCount = MMQP_PID_WINDOW;
        size_t maxBytes = 4 * 1024 * 1024;

        bool CanAdd(size_t bytes, uint64_t sequence = 0) const
        {
            return entries.size() < maxCount && m_Bytes <= maxBytes && bytes <= maxBytes - m_Bytes
                && (!sequence || entries.empty() || sequence < entries.begin()->first + MMQP_PID_WINDOW);
        }

        void Add(PublishRequest message, uint64_t sequence, size_t bytes, TimePoint retry, TimePoint expiry)
        {
            if (!message.qos || !message.packetId || message.packetId != WirePid(sequence) || entries.contains(sequence))
                throw ProtocolError("Invalid pending publication", MMQP_REASON_INVALID_PID);
            if (!this->CanAdd(bytes, sequence))
                throw ProtocolError("Reliable send budget exhausted", MMQP_REASON_BUSY);
            m_Index.emplace(message.packetId, sequence);
            entries.emplace(sequence, PendingPublication{std::move(message), retry, expiry, 0, bytes});
            m_Bytes += bytes;
        }

        bool Remove(uint16_t packetId)
        {
            auto index = m_Index.find(packetId);
            if (index == m_Index.end()) return false;
            auto found = entries.find(index->second);
            m_Bytes -= found->second.bytes;
            entries.erase(found);
            m_Index.erase(index);
            return true;
        }

        EMPY_INLINE size_t Bytes() const { return m_Bytes; }

    private:
        std::map<uint16_t, uint64_t> m_Index;
        size_t m_Bytes = 0;
    };
}
