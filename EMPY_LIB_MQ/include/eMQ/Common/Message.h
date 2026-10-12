#pragma once
#include "Topic.h"
#include <variant>

namespace eMQ
{
    struct Reason
    {
        uint8_t code = MMQP_REASON_SUCCESS;
        std::string text;
    };

    struct ProfileEntry
    {
        uint8_t type = MMQP_PROREQ_TYPE_SUB;
        uint16_t alias = 0;
        std::string topic;
        uint8_t qos = 0;
        bool request = false;
        uint8_t expiry = 10;
        uint8_t queue = 10;
    };

    struct ProfileRequest
    {
        uint16_t packetId = 0;
        std::vector<ProfileEntry> entries;
    };

    struct SessionAuth
    {
        std::string identifier;
        Buffer token;
    };

    struct SessionPolicy
    {
        bool reasons = false;
        std::optional<uint8_t> expiry;
        std::optional<uint8_t> keepalive;
        std::optional<uint32_t> maxPacket;
    };

    struct BulkPolicy
    {
        uint8_t streams = 1;
        uint32_t threshold = 65536;
        bool operator==(const BulkPolicy&) const = default;
    };

    struct QuicPolicy
    {
        std::optional<uint8_t> strategy;
        std::optional<BulkPolicy> bulk;
        std::optional<uint32_t> datagram;
        bool operator==(const QuicPolicy&) const = default;
    };

    struct SessionWill
    {
        std::optional<Buffer> birth;
        std::optional<Buffer> last;
        std::string topic;
        uint8_t qos = 0;
    };

    struct SessionProperties
    {
        std::optional<SessionAuth> auth;
        std::optional<SessionPolicy> policy;
        std::optional<QuicPolicy> quic;
        std::optional<SessionWill> will;
        std::optional<ProfileRequest> profile;
    };

    struct ConnectionRequest
    {
        bool resume = false;
        uint8_t version = MMQPv1;
        SessionProperties properties;
    };

    struct ConnectionAck
    {
        Reason reason;
        SessionProperties properties;
    };

    struct ConnectionSync
    {
        std::optional<Buffer> token;
    };

    struct ConnectionEnd
    {
        Reason reason;
    };

    struct ProfileAck
    {
        uint16_t packetId = 0;
        Reason reason;
    };

    struct ProfileSync
    {
        uint16_t alias = 0;
        std::string topic;
        Reason reason;
    };

    struct EncryptionHeader
    {
        uint32_t epoch = 0;
        std::array<uint8_t, 16> nonce{};
        std::array<uint8_t, 16> tag{};
    };

    struct PublishRequest
    {
        uint8_t type = MMQP_PUBREQ_TYPE_MSG;
        uint16_t alias = 0;
        std::string topic;
        uint8_t qos = 0;
        uint16_t packetId = 0;
        bool duplicate = false;
        uint16_t correlationId = 0; // Mandatory for PRT REQ/RES, independent of the QoS packet ID.
        uint8_t responses = 1;
        Reason reason;
        std::optional<EncryptionHeader> encryption;
        Buffer payload;
    };

    struct PublishAck
    {
        uint16_t packetId = 0;
        Reason reason;
    };

    using Message = std::variant<ConnectionRequest, ConnectionAck, ConnectionSync,
        ConnectionEnd, ProfileRequest, ProfileAck, ProfileSync, PublishRequest, PublishAck>;

    struct CodecOptions
    {
        uint32_t maxPacket = 1024 * 1024;
        uint32_t maxProfiles = 256;
        bool reasons = false;

        CodecOptions(bool includeReasons = false, uint32_t packetLimit = 1024 * 1024, uint32_t profileLimit = 256):
            maxPacket(packetLimit), maxProfiles(profileLimit), reasons(includeReasons) {}
    };

    struct EMPY_MQ_API Codec
    {
        static Message Decode(std::span<const uint8_t> bytes, CodecOptions options = {});
        static Buffer Encode(const Message& message, CodecOptions options = {});
        static uint8_t Type(const Message& message);
    };
}
