#include <eMQ/Common/Message.h>

namespace eMQ
{
    namespace
    {
        void Require(bool valid, const char* text, uint8_t reason = MMQP_REASON_MALFORMED_PACKET)
        {
            if (!valid) throw ProtocolError(text, reason);
        }

        void Flags(uint8_t value, uint8_t allowed)
        {
            Require((value & ~allowed) == 0, "Reserved variable flags are set");
        }

        void Pid(uint16_t value)
        {
            Require(value != 0, "Packet identifier must be nonzero", MMQP_REASON_INVALID_PID);
        }

        void Qos(uint8_t value)
        {
            Require(value <= 1, "Only QoS 0 and 1 are supported");
        }

        std::string Text(Reader& reader)
        {
            auto text = reader.String();
            Require(ValidUtf8(text), "Malformed UTF-8 text");
            return text;
        }

        void WriteReason(Writer& writer, const Reason& reason, CodecOptions options)
        {
            writer.U8(reason.code);
            if (options.reasons && reason.code)
            {
                Require(ValidUtf8(reason.text), "Malformed reason text");
                writer.String(reason.text);
            }
        }

        Reason ReadReason(Reader& reader, CodecOptions options)
        {
            Reason reason{reader.U8(), {}};
            if (options.reasons && reason.code) reason.text = Text(reader);
            return reason;
        }

        void ValidateProfile(const ProfileEntry& entry)
        {
            Require(entry.type == MMQP_PROREQ_TYPE_SUB || entry.type == MMQP_PROREQ_TYPE_REG
                || entry.type == MMQP_PROREQ_TYPE_UNS, "Unknown profile operation");
            Qos(entry.qos);
            if (entry.type == MMQP_PROREQ_TYPE_UNS)
                Require(!entry.qos && !entry.request, "Unsubscribe has reserved flags");
            if (!entry.topic.empty())
            {
                auto pattern = ParseTopic(entry.topic, true);
                Require(entry.type != MMQP_PROREQ_TYPE_REG || pattern.group.empty(),
                    "Publish registrations cannot be shared", MMQP_REASON_INVALID_TOPIC);
                Require(HasWildcard(pattern.filter) ? entry.alias == 0 : entry.alias != 0,
                    "Only wildcard profiles use alias zero", MMQP_REASON_INVALID_ALIAS);
            }
            else
                Require(entry.alias != 0, "A profile requires a topic or a mapped alias", MMQP_REASON_INVALID_ALIAS);
            if (entry.type == MMQP_PROREQ_TYPE_REG)
            {
                DecodeInterval(entry.expiry);
                Require(!entry.qos || entry.queue, "Retained queue length must be positive");
            }
        }

        void WriteProfiles(Writer& writer, const ProfileRequest& request, CodecOptions options)
        {
            Pid(request.packetId);
            Require(!request.entries.empty() && request.entries.size() <= options.maxProfiles,
                "Profile count exceeds configured bounds", MMQP_REASON_POLICY_VIOLATION);
            writer.U16(request.packetId);
            for (const auto& entry : request.entries)
            {
                ValidateProfile(entry);
                writer.U8(entry.type | (entry.topic.empty() ? 0 : MMQP_PROREQ_FLAGS_TOP)
                    | (entry.request ? MMQP_PROREQ_FLAGS_REQ : 0) | entry.qos);
                writer.U16(entry.alias);
                if (!entry.topic.empty()) writer.String(CanonicalTopic(entry.topic));
                if (entry.type == MMQP_PROREQ_TYPE_REG)
                {
                    writer.U8(entry.expiry);
                    if (entry.qos) writer.U8(entry.queue);
                }
            }
        }

        ProfileRequest ReadProfiles(Reader& reader, CodecOptions options)
        {
            ProfileRequest request;
            request.packetId = reader.U16();
            Pid(request.packetId);
            while (!reader.Empty())
            {
                Require(request.entries.size() < options.maxProfiles, "Too many profile entries",
                    MMQP_REASON_POLICY_VIOLATION);
                auto flags = reader.U8();
                Flags(flags, 0xF3);
                ProfileEntry entry;
                entry.type = flags & MMQP_PROREQ_TYPE_MASK;
                entry.qos = flags & MMQP_QOS_LEVEL_MASK;
                entry.request = flags & MMQP_PROREQ_FLAGS_REQ;
                entry.alias = reader.U16();
                if (flags & MMQP_PROREQ_FLAGS_TOP) entry.topic = CanonicalTopic(Text(reader));
                if (entry.type == MMQP_PROREQ_TYPE_REG)
                {
                    entry.expiry = reader.U8();
                    if (entry.qos) entry.queue = reader.U8();
                }
                ValidateProfile(entry);
                request.entries.emplace_back(std::move(entry));
            }
            Require(!request.entries.empty(), "Empty profile request");
            return request;
        }

        void ValidateQuic(const QuicPolicy& policy)
        {
            if (policy.strategy)
            {
                auto value = *policy.strategy;
                Require(value == MMQP_QUIC_STRATEGY_CUSTOM || value == MMQP_QUIC_STRATEGY_SINGLE
                    || value == MMQP_QUIC_STRATEGY_SEMANTIC || value == MMQP_QUIC_STRATEGY_BULK,
                    "Unknown QUIC allocation strategy", MMQP_REASON_POLICY_VIOLATION);
            }
            if (policy.bulk)
                Require(policy.bulk->streams && policy.bulk->threshold, "Invalid QUIC bulk policy");
            if (policy.datagram)
                Require(*policy.datagram != 0, "Invalid QUIC datagram threshold");
        }

        void WriteProperties(Writer& writer, const SessionProperties& properties, CodecOptions options, bool ack)
        {
            writer.U8((properties.auth ? MMQP_SESSION_FLAGS_SAF : 0)
                | (properties.policy ? MMQP_SESSION_FLAGS_SPF : 0)
                | (properties.quic ? MMQP_SESSION_FLAGS_QPF : 0)
                | (properties.will ? MMQP_SESSION_FLAGS_SWF : 0)
                | (properties.profile ? MMQP_SESSION_FLAGS_TPF : 0));
            if (properties.auth)
            {
                const auto& auth = *properties.auth;
                Require(!ack || auth.token.empty(), "CONACK cannot contain an authentication token");
                Require(ValidUtf8(auth.identifier), "Invalid session identifier");
                writer.U8((auth.identifier.empty() ? 0 : MMQP_AUTHEN_FLAGS_SID)
                    | (auth.token.empty() ? 0 : MMQP_AUTHEN_FLAGS_TOK));
                if (!auth.identifier.empty()) writer.String(auth.identifier);
                if (!auth.token.empty()) writer.Blob(auth.token);
            }
            if (properties.policy)
            {
                const auto& policy = *properties.policy;
                writer.U8((policy.reasons ? MMQP_POLICY_FLAGS_IRS : 0)
                    | (policy.expiry ? MMQP_POLICY_FLAGS_SEI : 0)
                    | (policy.keepalive ? MMQP_POLICY_FLAGS_KAI : 0)
                    | (policy.maxPacket ? MMQP_POLICY_FLAGS_MPS : 0));
                if (policy.expiry) { DecodeInterval(*policy.expiry); writer.U8(*policy.expiry); }
                if (policy.keepalive) { DecodeInterval(*policy.keepalive); writer.U8(*policy.keepalive); }
                if (policy.maxPacket)
                {
                    Require(*policy.maxPacket >= 2, "Invalid maximum packet size");
                    writer.U32(*policy.maxPacket);
                }
            }
            if (properties.quic)
            {
                const auto& policy = *properties.quic;
                ValidateQuic(policy);
                writer.U8((policy.strategy ? MMQP_QUIC_FLAGS_SAS : 0)
                    | (policy.bulk ? MMQP_QUIC_FLAGS_BTS : 0) | (policy.datagram ? MMQP_QUIC_FLAGS_QDG : 0));
                if (policy.strategy) writer.U8(*policy.strategy);
                if (policy.bulk) { writer.U8(policy.bulk->streams); writer.U32(policy.bulk->threshold); }
                if (policy.datagram) writer.U32(*policy.datagram);
            }
            if (properties.will)
            {
                const auto& will = *properties.will;
                Qos(will.qos);
                Require(will.birth || will.last, "Empty will block");
                writer.U8((will.birth ? MMQP_STATUS_FLAGS_BWM : 0)
                    | (will.last ? MMQP_STATUS_FLAGS_LWM : 0)
                    | (will.topic.empty() ? 0 : MMQP_STATUS_FLAGS_TOP) | will.qos);
                if (will.birth) writer.Blob(*will.birth);
                if (will.last) writer.Blob(*will.last);
                if (!will.topic.empty()) writer.String(ParseTopic(will.topic).filter);
            }
            if (properties.profile)
            {
                Writer profile(options.maxPacket);
                WriteProfiles(profile, *properties.profile, options);
                auto length = static_cast<uint32_t>(profile.Data().size());
                auto width = LengthWidth(length);
                writer.U8(static_cast<uint8_t>((width - 1) << 2));
                writer.Number(length, width);
                writer.Bytes(profile.Data());
            }
        }

        SessionProperties ReadProperties(Reader& reader, CodecOptions options, bool ack)
        {
            SessionProperties properties;
            auto flags = reader.U8();
            Flags(flags, 0xF8);
            if (flags & MMQP_SESSION_FLAGS_SAF)
            {
                auto attrs = reader.U8();
                Flags(attrs, ack ? 0x80 : 0xC0);
                SessionAuth auth;
                if (attrs & MMQP_AUTHEN_FLAGS_SID)
                {
                    auth.identifier = Text(reader);
                    Require(!auth.identifier.empty(), "Empty session identifier");
                }
                if (attrs & MMQP_AUTHEN_FLAGS_TOK)
                {
                    auth.token = reader.Blob();
                    Require(!auth.token.empty(), "Empty authentication token");
                }
                properties.auth = std::move(auth);
            }
            if (flags & MMQP_SESSION_FLAGS_SPF)
            {
                auto attrs = reader.U8();
                Flags(attrs, 0xF0);
                SessionPolicy policy;
                policy.reasons = attrs & MMQP_POLICY_FLAGS_IRS;
                if (attrs & MMQP_POLICY_FLAGS_SEI) { policy.expiry = reader.U8(); DecodeInterval(*policy.expiry); }
                if (attrs & MMQP_POLICY_FLAGS_KAI) { policy.keepalive = reader.U8(); DecodeInterval(*policy.keepalive); }
                if (attrs & MMQP_POLICY_FLAGS_MPS)
                {
                    policy.maxPacket = reader.U32();
                    Require(*policy.maxPacket >= 2, "Invalid maximum packet size");
                }
                properties.policy = policy;
            }
            if (flags & MMQP_SESSION_FLAGS_QPF)
            {
                auto attrs = reader.U8();
                Flags(attrs, 0xE0);
                QuicPolicy policy;
                if (attrs & MMQP_QUIC_FLAGS_SAS) policy.strategy = reader.U8();
                if (attrs & MMQP_QUIC_FLAGS_BTS)
                {
                    BulkPolicy bulk;
                    bulk.streams = reader.U8();
                    bulk.threshold = reader.U32();
                    policy.bulk = bulk;
                }
                if (attrs & MMQP_QUIC_FLAGS_QDG) policy.datagram = reader.U32();
                ValidateQuic(policy);
                properties.quic = policy;
            }
            if (flags & MMQP_SESSION_FLAGS_SWF)
            {
                auto attrs = reader.U8();
                Flags(attrs, 0xE3);
                SessionWill will;
                will.qos = attrs & MMQP_QOS_LEVEL_MASK;
                Qos(will.qos);
                if (attrs & MMQP_STATUS_FLAGS_BWM) will.birth = reader.Blob();
                if (attrs & MMQP_STATUS_FLAGS_LWM) will.last = reader.Blob();
                if (attrs & MMQP_STATUS_FLAGS_TOP) will.topic = ParseTopic(Text(reader)).filter;
                Require(will.birth || will.last, "Empty will block");
                properties.will = std::move(will);
            }
            if (flags & MMQP_SESSION_FLAGS_TPF)
            {
                auto attrs = reader.U8();
                Flags(attrs, MMQP_RLEN_FLAGS_MASK);
                auto width = static_cast<uint8_t>((attrs >> 2) + 1);
                auto length = static_cast<uint32_t>(reader.Number(width));
                Require(LengthWidth(length) == width, "Noncanonical embedded profile length");
                Reader profile(reader.Bytes(length));
                properties.profile = ReadProfiles(profile, options);
            }
            return properties;
        }

        void WritePublication(Writer& writer, const PublishRequest& request, CodecOptions options)
        {
            Require(request.alias != 0, "Publication alias must be nonzero", MMQP_REASON_INVALID_ALIAS);
            Qos(request.qos);
            Require(request.qos || (!request.duplicate && !request.packetId), "QoS 0 cannot carry a PID or DUP");
            if (request.qos) Pid(request.packetId);
            bool correlated = request.type == MMQP_PUBREQ_TYPE_REQ || request.type == MMQP_PUBREQ_TYPE_RES;
            Require(correlated == (request.correlationId != 0), "Requests and responses require a nonzero correlation ID; messages cannot carry one",
                MMQP_REASON_INVALID_CORRELATION);
            writer.U16(request.alias);
            writer.U8((request.duplicate ? MMQP_PUBREQ_FLAGS_DUP : 0)
                | (request.encryption ? MMQP_PUBREQ_FLAGS_E2E : 0)
                | (request.topic.empty() ? 0 : MMQP_PUBREQ_FLAGS_TOP) | request.qos);
            if (!request.topic.empty()) writer.String(ParseTopic(request.topic).filter);
            if (request.qos) writer.U16(request.packetId);
            if (correlated) writer.U16(request.correlationId);
            if (request.type == MMQP_PUBREQ_TYPE_REQ) writer.U8(request.responses);
            if (request.type == MMQP_PUBREQ_TYPE_RES) WriteReason(writer, request.reason, options);
            if (request.encryption)
            {
                writer.U32(request.encryption->epoch);
                writer.Bytes(request.encryption->nonce);
                writer.Bytes(request.encryption->tag);
            }
            writer.Bytes(request.payload);
        }

        PublishRequest ReadPublication(Reader& reader, uint8_t type, CodecOptions options)
        {
            PublishRequest request;
            request.type = type;
            request.alias = reader.U16();
            Require(request.alias != 0, "Publication alias must be nonzero", MMQP_REASON_INVALID_ALIAS);
            auto flags = reader.U8();
            Flags(flags, 0xE3);
            bool correlated = type == MMQP_PUBREQ_TYPE_REQ || type == MMQP_PUBREQ_TYPE_RES;
            request.qos = flags & MMQP_QOS_LEVEL_MASK;
            request.duplicate = flags & MMQP_PUBREQ_FLAGS_DUP;
            Qos(request.qos);
            Require(request.qos || !request.duplicate, "QoS 0 cannot set DUP");
            if (flags & MMQP_PUBREQ_FLAGS_TOP) request.topic = ParseTopic(Text(reader)).filter;
            if (request.qos) { request.packetId = reader.U16(); Pid(request.packetId); }
            if (correlated)
            {
                request.correlationId = reader.U16();
                Require(request.correlationId != 0, "Zero correlation ID", MMQP_REASON_INVALID_CORRELATION);
            }
            if (type == MMQP_PUBREQ_TYPE_REQ) request.responses = reader.U8();
            if (type == MMQP_PUBREQ_TYPE_RES) request.reason = ReadReason(reader, options);
            if (flags & MMQP_PUBREQ_FLAGS_E2E)
            {
                EncryptionHeader encryption;
                encryption.epoch = reader.U32();
                auto nonce = reader.Bytes(encryption.nonce.size());
                std::copy(nonce.begin(), nonce.end(), encryption.nonce.begin());
                auto tag = reader.Bytes(encryption.tag.size());
                std::copy(tag.begin(), tag.end(), encryption.tag.begin());
                request.encryption = encryption;
            }
            auto payload = reader.Bytes(reader.Remaining());
            request.payload.assign(payload.begin(), payload.end());
            return request;
        }
    }

    uint8_t Codec::Type(const Message& message)
    {
        return static_cast<uint8_t>((message.index() + 1) << 4);
    }

    Buffer Codec::Encode(const Message& message, CodecOptions options)
    {
        Writer writer(options.maxPacket);
        uint8_t flags = 0;
        std::visit([&] (const auto& value)
        {
            using Type = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<Type, ConnectionRequest>)
            {
                Require(value.version == MMQPv1, "Unsupported MMQP version", MMQP_REASON_UNSUPPORTED_VERSION);
                Require(!value.resume || (value.properties.auth && !value.properties.auth->identifier.empty()),
                    "Resume requires a session identifier");
                flags = value.resume ? MMQP_CONREQ_FLAGS_RES : 0;
                writer.String(MMQP_ALIAS);
                writer.U8(value.version);
                WriteProperties(writer, value.properties, options, false);
            }
            else if constexpr (std::is_same_v<Type, ConnectionAck>)
            {
                WriteReason(writer, value.reason, options);
                WriteProperties(writer, value.properties, options, true);
            }
            else if constexpr (std::is_same_v<Type, ConnectionSync>)
            {
                if (value.token)
                {
                    Require(!value.token->empty(), "Empty authentication token");
                    flags = MMQP_CONSYN_FLAGS_TOK;
                    writer.Blob(*value.token);
                }
            }
            else if constexpr (std::is_same_v<Type, ConnectionEnd>) WriteReason(writer, value.reason, options);
            else if constexpr (std::is_same_v<Type, ProfileRequest>) WriteProfiles(writer, value, options);
            else if constexpr (std::is_same_v<Type, ProfileSync>)
            {
                Require(value.alias != 0, "Alias mapping cannot use zero", MMQP_REASON_INVALID_ALIAS);
                writer.U16(value.alias);
                writer.String(ParseTopic(value.topic).filter);
                WriteReason(writer, value.reason, options);
            }
            else if constexpr (std::is_same_v<Type, PublishRequest>)
            {
                flags = value.type;
                WritePublication(writer, value, options);
            }
            else
            {
                Pid(value.packetId);
                writer.U16(value.packetId);
                WriteReason(writer, value.reason, options);
            }
        }, message);
        return Frame(Codec::Type(message), flags, writer.Data(), options.maxPacket);
    }

    Message Codec::Decode(std::span<const uint8_t> bytes, CodecOptions options)
    {
        auto header = ReadHeader(bytes, options.maxPacket);
        Require(header && bytes.size() == uint64_t{header->size} + header->remaining,
            "Incomplete packet or trailing frame bytes");
        Reader reader(bytes.subspan(header->size));
        Message message;
        switch (header->type)
        {
        case MMQP_PACKET_CONREQ:
        {
            ConnectionRequest request;
            Require(Text(reader) == MMQP_ALIAS, "Invalid protocol alias");
            request.version = reader.U8();
            Require(request.version == MMQPv1, "Unsupported MMQP version", MMQP_REASON_UNSUPPORTED_VERSION);
            request.resume = header->flags & MMQP_CONREQ_FLAGS_RES;
            request.properties = ReadProperties(reader, options, false);
            Require(!request.resume || (request.properties.auth && !request.properties.auth->identifier.empty()),
                "Resume requires a session identifier");
            message = std::move(request);
            break;
        }
        case MMQP_PACKET_CONACK:
        {
            ConnectionAck ack;
            ack.reason = ReadReason(reader, options);
            ack.properties = ReadProperties(reader, options, true);
            message = std::move(ack);
            break;
        }
        case MMQP_PACKET_CONSYN:
        {
            ConnectionSync sync;
            if (header->flags & MMQP_CONSYN_FLAGS_TOK)
            {
                sync.token = reader.Blob();
                Require(!sync.token->empty(), "Empty authentication token");
            }
            message = std::move(sync);
            break;
        }
        case MMQP_PACKET_CONEND: message = ConnectionEnd{ReadReason(reader, options)}; break;
        case MMQP_PACKET_PROREQ: message = ReadProfiles(reader, options); break;
        case MMQP_PACKET_PROACK:
        case MMQP_PACKET_PUBACK:
        {
            auto packetId = reader.U16();
            Pid(packetId);
            auto reason = ReadReason(reader, options);
            if (header->type == MMQP_PACKET_PROACK) message = ProfileAck{packetId, std::move(reason)};
            else message = PublishAck{packetId, std::move(reason)};
            break;
        }
        case MMQP_PACKET_PROSYN:
        {
            ProfileSync sync;
            sync.alias = reader.U16();
            Require(sync.alias != 0, "Alias mapping cannot use zero", MMQP_REASON_INVALID_ALIAS);
            sync.topic = ParseTopic(Text(reader)).filter;
            sync.reason = ReadReason(reader, options);
            message = std::move(sync);
            break;
        }
        case MMQP_PACKET_PUBREQ: message = ReadPublication(reader, header->flags, options); break;
        default: throw ProtocolError("Unknown packet type");
        }
        reader.Finish();
        return message;
    }
}
