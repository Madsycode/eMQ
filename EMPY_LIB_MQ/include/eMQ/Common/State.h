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

    struct ReceiveWindow
    {
        explicit ReceiveWindow(bool ordered = true, uint16_t size = 256):
            m_Ordered(ordered), m_Size(size)
        {
            if (!size || size > m_Bits.size()) throw std::invalid_argument("Receive window must be in 1..256");
        }

        Reception Inspect(uint16_t packetId) const
        {
            if (!packetId) throw ProtocolError("Zero publication PID", MMQP_REASON_INVALID_PID);
            if (packetId <= m_Checkpoint) return Reception::Duplicate;
            auto offset = static_cast<size_t>(packetId - m_Checkpoint - 1);
            if (!m_Ordered && offset >= m_Size) return Reception::OutsideWindow;
            return !m_Ordered && m_Bits[offset] ? Reception::Duplicate : Reception::New;
        }

        void Commit(uint16_t packetId)
        {
            auto state = this->Inspect(packetId);
            if (state == Reception::Duplicate) return;
            if (state == Reception::OutsideWindow)
                throw ProtocolError("Publication is beyond the receive window", MMQP_REASON_INVALID_PID);
            if (m_Ordered) m_Checkpoint = packetId;
            else
            {
                m_Bits.set(packetId - m_Checkpoint - 1);
                while (m_Bits[0])
                {
                    ++m_Checkpoint;
                    m_Bits >>= 1;
                }
            }
        }

        EMPY_INLINE uint16_t Checkpoint() const { return m_Checkpoint; }

        void Save(Writer& writer) const
        {
            writer.U8(m_Ordered);
            writer.U16(m_Size);
            writer.U16(m_Checkpoint);
            for (size_t i = 0; i < 256; i += 8)
            {
                uint8_t byte = 0;
                for (size_t bit = 0; bit < 8; ++bit) if (m_Bits[i + bit]) byte |= static_cast<uint8_t>(1U << bit);
                writer.U8(byte);
            }
        }

        static ReceiveWindow Load(Reader& reader)
        {
            auto ordered = reader.U8();
            if (ordered > 1) throw ProtocolError("Invalid stored receive window");
            ReceiveWindow result(ordered != 0, reader.U16());
            result.m_Checkpoint = reader.U16();
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
        uint16_t m_Checkpoint = 0;
        std::bitset<256> m_Bits;
    };

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
                throw ProtocolError("PID exhausted; establish a fresh session", MMQP_REASON_INVALID_PID);
            return static_cast<uint16_t>(m_Value + 1);
        }

        EMPY_INLINE uint16_t Value() const { return m_Value; }

    private:
        uint16_t m_Value = 0;
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
        std::map<uint16_t, PendingPublication> entries;
        size_t maxCount = 256;
        size_t maxBytes = 4 * 1024 * 1024;

        bool CanAdd(size_t bytes, uint16_t packetId = 0) const
        {
            return entries.size() < maxCount && m_Bytes <= maxBytes && bytes <= maxBytes - m_Bytes
                && (!packetId || entries.empty() || static_cast<uint32_t>(packetId) < static_cast<uint32_t>(entries.begin()->first) + 256);
        }

        void Add(PublishRequest message, size_t bytes, TimePoint retry, TimePoint expiry)
        {
            if (!message.qos || !message.packetId || entries.contains(message.packetId))
                throw ProtocolError("Invalid pending publication", MMQP_REASON_INVALID_PID);
            if (!this->CanAdd(bytes, message.packetId))
                throw ProtocolError("Reliable send budget exhausted", MMQP_REASON_BUSY);
            auto packetId = message.packetId;
            entries.emplace(packetId, PendingPublication{std::move(message), retry, expiry, 0, bytes});
            m_Bytes += bytes;
        }

        bool Remove(uint16_t packetId)
        {
            auto found = entries.find(packetId);
            if (found == entries.end()) return false;
            m_Bytes -= found->second.bytes;
            entries.erase(found);
            return true;
        }

        EMPY_INLINE size_t Bytes() const { return m_Bytes; }

    private:
        size_t m_Bytes = 0;
    };
}
