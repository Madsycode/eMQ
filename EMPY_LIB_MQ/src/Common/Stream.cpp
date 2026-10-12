#include <eMQ/Common/Stream.h>

namespace eMQ
{
    namespace
    {
        void ValidateControl(uint8_t type, uint8_t flags)
        {
            if (type < MMQP_PACKET_CONREQ || type > MMQP_PACKET_PUBACK || (type & 0x0F))
                throw ProtocolError("Unknown control packet type");
            if (type == MMQP_PACKET_PUBREQ)
            {
                if (!flags || flags > MMQP_PUBREQ_TYPE_RES)
                    throw ProtocolError("Invalid publication type");
            }
            else if (type == MMQP_PACKET_CONREQ || type == MMQP_PACKET_CONSYN)
            {
                if (flags & ~uint8_t{0x02})
                    throw ProtocolError("Reserved control flags are set");
            }
            else if (flags)
                throw ProtocolError("Reserved control flags are set");
        }
    }

    uint8_t LengthWidth(uint32_t length)
    {
        if (length <= UINT8_MAX) return 1;
        if (length <= UINT16_MAX) return 2;
        if (length <= UINT24_MAX) return 3;
        return 4;
    }

    std::optional<PacketHeader> ReadHeader(std::span<const uint8_t> bytes, uint32_t limit)
    {
        if (bytes.empty()) return std::nullopt;
        PacketHeader header;
        header.type = bytes[0] & MMQP_PACKET_TYPE_MASK;
        header.flags = bytes[0] & MMQP_CTRL_FLAGS_MASK;
        ValidateControl(header.type, header.flags);
        auto width = static_cast<uint8_t>(((bytes[0] & MMQP_RLEN_FLAGS_MASK) >> 2) + 1);
        header.size = width + 1;
        if (bytes.size() < header.size) return std::nullopt;
        Reader reader(bytes.subspan(1, width));
        header.remaining = static_cast<uint32_t>(reader.Number(width));
        if (LengthWidth(header.remaining) != width)
            throw ProtocolError("Noncanonical remaining length");
        if (uint64_t{header.remaining} + header.size > limit)
            throw ProtocolError("Packet exceeds configured size", MMQP_REASON_POLICY_VIOLATION);
        return header;
    }

    Buffer Frame(uint8_t type, uint8_t flags, std::span<const uint8_t> body, uint32_t limit)
    {
        ValidateControl(type, flags);
        if (body.size() > UINT32_MAX)
            throw ProtocolError("Remaining length exceeds the protocol range");
        auto width = LengthWidth(static_cast<uint32_t>(body.size()));
        if (body.size() + 1 + width > limit)
            throw ProtocolError("Packet exceeds configured size", MMQP_REASON_POLICY_VIOLATION);
        // One exact allocation: control octet, big-endian remaining length, body.
        Buffer frame(1 + width + body.size());
        frame[0] = type | static_cast<uint8_t>((width - 1) << 2) | flags;
        for (uint8_t i = 0; i < width; ++i) frame[width - i] = static_cast<uint8_t>(body.size() >> (i * 8));
        std::copy(body.begin(), body.end(), frame.begin() + 1 + width);
        return frame;
    }

    FrameStream::FrameStream(uint32_t limit): m_Limit(limit)
    {
        this->SetLimit(limit);
    }

    void FrameStream::Feed(std::span<const uint8_t> bytes,
        const std::function<void(std::span<const uint8_t>)>& receive)
    {
        while (!bytes.empty())
        {
            if (m_Buffer.empty())
            {
                // Fast path: complete frames are delivered from the read buffer without copying.
                auto header = ReadHeader(bytes, m_Limit);
                auto size = header ? size_t{header->size} + header->remaining : 0;
                if (header && bytes.size() >= size)
                {
                    auto frame = bytes.first(size);
                    bytes = bytes.subspan(size);
                    receive(frame);
                    continue;
                }
            }
            auto count = std::min(bytes.size(), m_Target - m_Buffer.size());
            m_Buffer.insert(m_Buffer.end(), bytes.begin(), bytes.begin() + count);
            bytes = bytes.subspan(count);
            if (m_Buffer.size() < m_Target) continue;
            if (m_Target == 1 && !m_Body)
            {
                // Validate the type immediately, before accepting a length or payload.
                ReadHeader(m_Buffer, m_Limit);
                m_Target = 2 + ((m_Buffer[0] & MMQP_RLEN_FLAGS_MASK) >> 2);
                continue;
            }
            if (!m_Body)
            {
                auto header = ReadHeader(m_Buffer, m_Limit).value();
                m_Target = size_t{header.size} + header.remaining;
                m_Body = true;
                if (m_Buffer.size() < m_Target) continue;
            }
            // Move the complete frame out so callbacks may reset or retune this decoder.
            auto frame = std::move(m_Buffer);
            this->Reset();
            receive(frame);
        }
    }

    void FrameStream::Reset()
    {
        m_Buffer.clear();
        m_Target = 1;
        m_Body = false;
    }

    void FrameStream::SetLimit(uint32_t limit)
    {
        if (limit < 2 || m_Buffer.size() > limit || (m_Body && m_Target > limit))
            throw ProtocolError("Invalid receive packet limit", MMQP_REASON_POLICY_VIOLATION);
        m_Limit = limit;
    }

    size_t FrameStream::Buffered() const { return m_Buffer.size(); }
}
