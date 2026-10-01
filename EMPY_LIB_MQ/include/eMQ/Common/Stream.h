#pragma once
#include <eCore/Common/Core.h>
#include "Protocol.h"

namespace eMQ
{
    using Buffer = std::vector<uint8_t>;

    struct ProtocolError : std::runtime_error
    {
        uint8_t reason;

        ProtocolError(const char* text, uint8_t code = MMQP_REASON_MALFORMED_PACKET):
            std::runtime_error(text), reason(code) {}
    };

    struct Reader
    {
        EMPY_INLINE explicit Reader(std::span<const uint8_t> data): m_Data(data) {}

        EMPY_INLINE size_t Remaining() const { return m_Data.size() - m_Offset; }
        EMPY_INLINE bool Empty() const { return this->Remaining() == 0; }

        std::span<const uint8_t> Bytes(size_t count)
        {
            if (count > this->Remaining())
                throw ProtocolError("Truncated packet body");
            auto bytes = m_Data.subspan(m_Offset, count);
            m_Offset += count;
            return bytes;
        }

        uint64_t Number(uint8_t count)
        {
            if (!count || count > 8)
                throw ProtocolError("Invalid integer width");
            uint64_t value = 0;
            for (auto byte : this->Bytes(count))
                value = (value << 8) | byte;
            return value;
        }

        EMPY_INLINE uint8_t U8() { return static_cast<uint8_t>(this->Number(1)); }
        EMPY_INLINE uint16_t U16() { return static_cast<uint16_t>(this->Number(2)); }
        EMPY_INLINE uint32_t U32() { return static_cast<uint32_t>(this->Number(4)); }
        EMPY_INLINE uint64_t U64() { return this->Number(8); }

        std::string String()
        {
            auto bytes = this->Bytes(this->U8());
            return std::string(bytes.begin(), bytes.end());
        }

        Buffer Blob()
        {
            auto bytes = this->Bytes(this->U8());
            return Buffer(bytes.begin(), bytes.end());
        }

        void Finish() const
        {
            if (!this->Empty())
                throw ProtocolError("Unexpected trailing packet data");
        }

    private:
        std::span<const uint8_t> m_Data;
        size_t m_Offset = 0;
    };

    struct Writer
    {
        EMPY_INLINE explicit Writer(size_t limit = 1024 * 1024): m_Limit(limit) {}

        void Bytes(std::span<const uint8_t> bytes)
        {
            if (bytes.size() > m_Limit - m_Data.size())
                throw ProtocolError("Packet exceeds configured size", MMQP_REASON_POLICY_VIOLATION);
            m_Data.insert(m_Data.end(), bytes.begin(), bytes.end());
        }

        void Number(uint64_t value, uint8_t count)
        {
            if (!count || count > 8 || (count < 8 && value >= (uint64_t{1} << (count * 8))))
                throw ProtocolError("Integer does not fit its wire field");
            std::array<uint8_t, 8> bytes{};
            for (uint8_t i = 0; i < count; ++i)
                bytes[count - i - 1] = static_cast<uint8_t>(value >> (i * 8));
            this->Bytes(std::span(bytes).first(count));
        }

        EMPY_INLINE void U8(uint8_t value) { this->Number(value, 1); }
        EMPY_INLINE void U16(uint16_t value) { this->Number(value, 2); }
        EMPY_INLINE void U32(uint32_t value) { this->Number(value, 4); }
        EMPY_INLINE void U64(uint64_t value) { this->Number(value, 8); }

        void Blob(std::span<const uint8_t> bytes)
        {
            if (bytes.size() > UINT8_MAX)
                throw ProtocolError("One-octet string length exceeded");
            this->U8(static_cast<uint8_t>(bytes.size()));
            this->Bytes(bytes);
        }

        void String(std::string_view text)
        {
            this->Blob({reinterpret_cast<const uint8_t*>(text.data()), text.size()});
        }

        EMPY_INLINE const Buffer& Data() const { return m_Data; }
        EMPY_INLINE Buffer Take() { return std::move(m_Data); }

    private:
        Buffer m_Data;
        size_t m_Limit;
    };

    struct PacketHeader
    {
        uint8_t type = 0;
        uint8_t flags = 0;
        uint32_t remaining = 0;
        uint8_t size = 0;
    };

    EMPY_MQ_API uint8_t LengthWidth(uint32_t length);
    EMPY_MQ_API std::optional<PacketHeader> ReadHeader(std::span<const uint8_t> bytes,
        uint32_t limit = 1024 * 1024);
    EMPY_MQ_API Buffer Frame(uint8_t type, uint8_t flags, std::span<const uint8_t> body,
        uint32_t limit = 1024 * 1024);

    struct EMPY_MQ_API FrameStream
    {
        explicit FrameStream(uint32_t limit = 1024 * 1024);
        void Feed(std::span<const uint8_t> bytes,
            const std::function<void(std::span<const uint8_t>)>& receive);
        void Reset();
        void SetLimit(uint32_t limit);
        size_t Buffered() const;

    private:
        Buffer m_Buffer;
        uint32_t m_Limit;
        size_t m_Target = 1;
        bool m_Body = false;
    };
}
