#include "Checks.h"
#include <eMQ/Network/QuicPolicy.h>

using namespace eMQ;

void TestProtocol()
{
    // Independent golden frames follow the paper's field order and PRT-based request IDs.
    std::vector<std::pair<Message, Buffer>> golden{
        {ConnectionRequest{}, {0x10, 0x07, 0x04, 'M', 'M', 'Q', 'P', 0x01, 0x00}},
        {ConnectionAck{}, {0x20, 0x02, 0x00, 0x00}},
        {ConnectionSync{}, {0x30, 0x00}},
        {ConnectionEnd{}, {0x40, 0x01, 0x00}},
        {ProfileRequest{0x1234, {{MMQP_PROREQ_TYPE_SUB, 1, "a", 1}}},
            {0x50, 0x07, 0x12, 0x34, 0x61, 0x00, 0x01, 0x01, 'a'}},
        {ProfileAck{7, {}}, {0x60, 0x03, 0x00, 0x07, 0x00}},
        {ProfileSync{2, "x", {}}, {0x70, 0x05, 0x00, 0x02, 0x01, 'x', 0x00}},
        {PublishAck{7, {}}, {0x90, 0x03, 0x00, 0x07, 0x00}}
    };
    PublishRequest publication;
    publication.alias = 2;
    publication.topic = "x";
    publication.qos = 1;
    publication.packetId = 7;
    publication.payload = {0xAA};
    golden.emplace_back(publication, Buffer{0x81, 0x08, 0x00, 0x02, 0x21, 0x01, 'x', 0x00, 0x07, 0xAA});
    publication.type = MMQP_PUBREQ_TYPE_REQ;
    publication.correlationId = 0x1234;
    publication.responses = 2;
    golden.emplace_back(publication, Buffer{0x82, 0x0B, 0x00, 0x02, 0x21, 0x01, 'x', 0x00, 0x07, 0x12, 0x34, 0x02, 0xAA});
    publication.type = MMQP_PUBREQ_TYPE_RES;
    golden.emplace_back(publication, Buffer{0x83, 0x0B, 0x00, 0x02, 0x21, 0x01, 'x', 0x00, 0x07, 0x12, 0x34, 0x00, 0xAA});
    publication.qos = 0;
    publication.packetId = 0;
    publication.topic.clear();
    golden.emplace_back(publication, Buffer{0x83, 0x07, 0x00, 0x02, 0x00, 0x12, 0x34, 0x00, 0xAA});
    publication.type = MMQP_PUBREQ_TYPE_REQ;
    golden.emplace_back(publication, Buffer{0x82, 0x07, 0x00, 0x02, 0x00, 0x12, 0x34, 0x02, 0xAA});
    for (const auto& [message, bytes] : golden)
    {
        EMPY_CHECK(Codec::Encode(message) == bytes);
        EMPY_CHECK(Codec::Encode(Codec::Decode(bytes)) == bytes);
        if (auto expected = std::get_if<PublishRequest>(&message))
        {
            auto decoded = std::get<PublishRequest>(Codec::Decode(bytes));
            EMPY_CHECK(decoded.type == expected->type && decoded.qos == expected->qos);
            EMPY_CHECK(decoded.packetId == expected->packetId && decoded.correlationId == expected->correlationId);
            EMPY_CHECK(decoded.payload == expected->payload);
            for (auto reserved : {0x10, 0x08, 0x04})
            {
                auto invalid = bytes;
                invalid[4] |= reserved;
                Throws([&] { Codec::Decode(invalid); });
            }
        }
        for (size_t count = 0; count < bytes.size(); ++count)
            Throws([&] { Codec::Decode(std::span(bytes).first(count)); });
        auto extra = bytes;
        extra.push_back(0);
        Throws([&] { Codec::Decode(extra); });
    }

    for (auto length : {0u, 255u, 256u, 65535u, 65536u, 16777215u, 16777216u})
    {
        auto width = LengthWidth(length);
        Writer writer;
        writer.U8(0x80 | ((width - 1) << 2) | 1);
        writer.Number(length, width);
        auto header = ReadHeader(writer.Data(), UINT32_MAX).value();
        EMPY_CHECK(header.remaining == length && header.size == width + 1);
    }
    Throws([] { ReadHeader(Buffer{0x35, 0, 1}); });
    Throws([] { ReadHeader(Buffer{0x34, 0, 1}); });
    Throws([] { ReadHeader(Buffer{0xA0, 0}); });
    FrameStream limited(64);
    Throws([&] { limited.Feed(Buffer{0x8D, 0x7F, 0xFF, 0xFF, 0xFF}, [] (auto) {}); });
    EMPY_CHECK(limited.Buffered() == 5);
    FrameStream stream;
    size_t frames = 0;
    auto receive = [&] (auto bytes) { Codec::Decode(bytes); ++frames; };
    for (const auto& [message, bytes] : golden)
        for (auto byte : bytes) stream.Feed(std::span(&byte, 1), receive);
    EMPY_CHECK(frames == golden.size() && stream.Buffered() == 0);
    Buffer batch;
    for (const auto& [message, bytes] : golden) batch.insert(batch.end(), bytes.begin(), bytes.end());
    stream.Feed(batch, receive);
    EMPY_CHECK(frames == golden.size() * 2);

    for (uint8_t type : {MMQP_PUBREQ_TYPE_REQ, MMQP_PUBREQ_TYPE_RES})
    {
        auto control = static_cast<uint8_t>(0x80 | type);
        // Complete frames with missing/truncated IDs, missing type-specific data, or zero IDs.
        for (const auto& invalid : {
            Buffer{control, 3, 0, 1, 0}, Buffer{control, 4, 0, 1, 0, 1},
            Buffer{control, 5, 0, 1, 0, 0, 1}, Buffer{control, 6, 0, 1, 0, 0, 0, 0},
            Buffer{control, 5, 0, 1, 1, 0, 7}, Buffer{control, 6, 0, 1, 1, 0, 7, 1},
            Buffer{control, 7, 0, 1, 1, 0, 7, 0, 1}, Buffer{control, 8, 0, 1, 1, 0, 7, 0, 0, 0}})
            Throws([&] { Codec::Decode(invalid); });
        publication.type = type;
        publication.correlationId = 0;
        Throws([&] { Codec::Encode(publication); });
    }
    publication.type = MMQP_PUBREQ_TYPE_MSG;
    publication.correlationId = 1;
    Throws([&] { Codec::Encode(publication); });

    ConnectionRequest connect;
    connect.resume = true;
    connect.properties.auth = SessionAuth{"session", {0, 1, 2, 0xFF}};
    connect.properties.policy = SessionPolicy{true, EncodeInterval(3600), EncodeInterval(120), 65536};
    connect.properties.quic = QuicPolicy{MMQP_QUIC_STRATEGY_BULK, BulkPolicy{2, 4096}, 1200};
    connect.properties.will = SessionWill{Buffer{'o', 'n'}, Buffer{'o', 'f', 'f'}, "device/status", 1};
    connect.properties.profile = ProfileRequest{1, {{MMQP_PROREQ_TYPE_REG, 0, "room/#", 1, true, 10, 2}}};
    auto connectBytes = Codec::Encode(connect);
    EMPY_CHECK((connectBytes[0] & 0x03) == 0x02);
    EMPY_CHECK(Codec::Encode(Codec::Decode(connectBytes)) == connectBytes);
    for (auto type : {MMQP_PUBREQ_TYPE_MSG, MMQP_PUBREQ_TYPE_REQ, MMQP_PUBREQ_TYPE_RES})
    {
        publication.type = type;
        publication.correlationId = type == MMQP_PUBREQ_TYPE_MSG ? 0 : UINT16_MAX;
        publication.qos = 1;
        publication.packetId = 7;
        publication.encryption = EncryptionHeader{10, {}, {}};
        publication.reason = {MMQP_REASON_NO_RESPONDER, "none"};
        publication.duplicate = true;
        auto encoded = Codec::Encode(publication, {true});
        EMPY_CHECK((encoded[0] & 3) == type);
        EMPY_CHECK(Codec::Encode(Codec::Decode(encoded, {true}), {true}) == encoded);
    }
    EMPY_CHECK(Codec::Encode(ConnectionSync{Buffer{0x11, 0x22}}) == Buffer({0x32, 3, 2, 0x11, 0x22}));
    auto failure = Codec::Encode(ConnectionEnd{{0x80, "failure"}}, {true});
    EMPY_CHECK(std::get<ConnectionEnd>(Codec::Decode(failure, {true})).reason.text == "failure");

    EMPY_CHECK(CanonicalTopic("//home///room//") == "home/room");
    EMPY_CHECK(TopicMatches("room/#", "room"));
    EMPY_CHECK(TopicMatches("room/+", "room/temp"));
    EMPY_CHECK(!TopicMatches("room/+", "room"));
    EMPY_CHECK(!TopicMatches("room/+", "room/temp/value"));
    EMPY_CHECK(TopicMatches("$share/workers/room/#", "room/temp"));
    EMPY_CHECK(!TopicMatches("#", "$lwm/device"));
    EMPY_CHECK(TopicMatches("$lwm/#", "$lwm/device"));
    for (auto invalid : {"a/#/b", "a/x+", "$share/g", "$share/+/a"})
        Throws([&] { ParseTopic(invalid, true); });
    EMPY_CHECK(!ValidUtf8(std::string("\xC0\xAF", 2)));
    EMPY_CHECK(!ValidUtf8(std::string("\xED\xA0\x80", 3)));
    EMPY_CHECK(!ValidUtf8(std::string("\xF4\x90\x80\x80", 4)));
    EMPY_CHECK(ValidUtf8("temp\xC3\xA9rature"));
    Throws([] { ParseTopic(std::string(256, 'x')); });
    for (auto value : {1u, 35u, 63u, 60u, 3780u, 3600u, 226800u, 86400u, 5443200u})
        EMPY_CHECK(DecodeInterval(EncodeInterval(value)) == value);
    Throws([] { EncodeInterval(64); });
    Throws([] { EncodeInterval(0); });
    for (uint8_t value : {0x00, 0x40, 0x80, 0xC0}) Throws([&] { DecodeInterval(value); });

    ReceiveWindow ordered;
    ordered.Commit(2);
    EMPY_CHECK(ordered.Inspect(1) == Reception::Duplicate);
    ReceiveWindow unordered(false, 64);
    unordered.Commit(3);
    unordered.Commit(1);
    EMPY_CHECK(unordered.Checkpoint() == 1 && unordered.Inspect(3) == Reception::Duplicate);
    unordered.Commit(2);
    EMPY_CHECK(unordered.Checkpoint() == 3);
    EMPY_CHECK(unordered.Inspect(68) == Reception::OutsideWindow);
    Throws([&] { unordered.Commit(68); });
    PacketSequence sequence;
    for (uint32_t i = 1; i <= UINT16_MAX; ++i) EMPY_CHECK(sequence.Next() == i);
    Throws([&] { sequence.Next(); });

    CorrelationIds correlations;
    correlations.Claim(UINT16_MAX);
    correlations.Claim(2);
    EMPY_CHECK(correlations.Peek() == 1 && correlations.Contains(2));
    correlations.Claim(1);
    EMPY_CHECK(correlations.Peek() == 3);
    Throws([&] { correlations.Claim(0); });
    Throws([&] { correlations.Claim(2); });
    Writer stored;
    correlations.Save(stored);
    Reader restored(stored.Data());
    correlations = CorrelationIds::Load(restored);
    restored.Finish();
    EMPY_CHECK(correlations.Peek() == 3 && correlations.Contains(UINT16_MAX));
    for (uint32_t i = 3; i < UINT16_MAX; ++i) correlations.Claim(static_cast<uint16_t>(i));
    Throws([&] { correlations.Peek(); });
    Throws([&] { correlations.Claim(UINT16_MAX); });

    ProfileTable profiles;
    profiles.Apply({1, {{MMQP_PROREQ_TYPE_REG, 1, "room/temp", 0},
        {MMQP_PROREQ_TYPE_SUB, 0, "room/#", 1}}});
    EMPY_CHECK(profiles.Registration("room/temp", MMQP_PUBREQ_TYPE_MSG));
    EMPY_CHECK(!profiles.Registration("room/temp", MMQP_PUBREQ_TYPE_REQ));
    EMPY_CHECK(profiles.Subscriptions("room/temp", 1).size() == 1);
    Throws([&] { profiles.Apply({2, {{MMQP_PROREQ_TYPE_SUB, 2, "valid", 1},
        {MMQP_PROREQ_TYPE_REG, 1, "collision", 0}}}); });
    EMPY_CHECK(profiles.entries.size() == 2 && !profiles.subscribers.Find("valid"));
    profiles.Apply({3, {{MMQP_PROREQ_TYPE_UNS, 0, "room/#", 0}}});
    EMPY_CHECK(profiles.Subscriptions("room/temp", 1).empty());

    QuicPlan plan({MMQP_QUIC_STRATEGY_BULK, BulkPolicy{2, 1024}, 1200});
    PublishRequest telemetry;
    telemetry.alias = 1;
    telemetry.payload.resize(100);
    EMPY_CHECK(plan.Select(telemetry, 110, 1200, 0) == QuicChannel::Datagram);
    EMPY_CHECK(plan.Select(telemetry, 110, 50, 0) == QuicChannel::BestEffort);
    telemetry.qos = 1;
    telemetry.packetId = 1;
    EMPY_CHECK(plan.Select(telemetry, 112, 1200, 0) == QuicChannel::Reliable);
    Throws([&] { plan.Validate(QuicChannel::Datagram, telemetry, 112); });
    telemetry.payload.resize(2048);
    EMPY_CHECK(plan.Select(telemetry, 2060, 1200, 0) == QuicChannel::Bulk);
    EMPY_CHECK(plan.Select(telemetry, 2060, 1200, 2) == QuicChannel::Reliable);
    EMPY_CHECK(plan.Select(PublishAck{1, {}}, 5, 1200, 0) == QuicChannel::Control);
    Throws([&] { plan.Validate(QuicChannel::Bulk, PublishAck{1, {}}, 5); });
    Throws([&] { QuicPlan invalid({MMQP_QUIC_STRATEGY_CUSTOM, {}, {}}); });
    Throws([&] { QuicPlan invalid({MMQP_QUIC_STRATEGY_SEMANTIC, BulkPolicy{}, {}}); });
    auto negotiated = QuicPlan::Negotiate(plan.Policy(), {MMQP_QUIC_STRATEGY_BULK, BulkPolicy{1, 4096}, 512});
    EMPY_CHECK(negotiated.bulk->streams == 1 && negotiated.bulk->threshold == 4096 && *negotiated.datagram == 512);
    negotiated = QuicPlan::Negotiate(plan.Policy(), {MMQP_QUIC_STRATEGY_SINGLE, {}, {}});
    EMPY_CHECK(*negotiated.strategy == MMQP_QUIC_STRATEGY_SINGLE && !negotiated.bulk && !negotiated.datagram);
}
