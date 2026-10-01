#pragma once
#include "QuicPolicy.h"

namespace eMQ
{
    struct TransportOptions
    {
        uint32_t maxPacket = 1024 * 1024;
        size_t maxQueue = 4 * 1024 * 1024;
        uint32_t timeout = 5000;
        bool tls = false;
        bool quic = false;
        QuicPolicy quicPolicy{MMQP_QUIC_STRATEGY_BULK, BulkPolicy{2, 65536}, 1200};
        bool mutual = false;
        bool backplane = false;
        std::string certificate;
        std::string certificateHash;
        std::string privateKey;
        std::string authority;
        std::string serverName;
    };

    struct EMPY_MQ_API Connection : std::enable_shared_from_this<Connection>
    {
        using Pointer = std::shared_ptr<Connection>;
        using Receive = std::function<void(std::span<const uint8_t>)>;
        using Closed = std::function<void(const std::string&)>;

        Receive onReceive;
        Closed onClose;
        virtual ~Connection() = default;
        virtual bool Send(Buffer bytes) = 0;
        virtual void Close(bool drain = false) = 0;
        virtual void SetLimit(uint32_t limit) = 0;
        virtual size_t Queued() const = 0;
        virtual size_t Capacity() const = 0;
        virtual bool Secure() const = 0;
        virtual std::string PeerFingerprint() const { return {}; }
        virtual bool Quic() const { return false; }
        virtual void Configure(const QuicPolicy&, bool)
        {
            throw ProtocolError("QUIC policy is not supported by this transport", MMQP_REASON_POLICY_VIOLATION);
        }
    };
}
