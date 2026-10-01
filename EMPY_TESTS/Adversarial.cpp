#include "Checks.h"
#include <eClient/API.h>
#include <eMQ/Network/IOContext.h>
#include <eMQ/Runtime/Broker.h>
#include <deque>
#include <thread>

using namespace eMQ;

namespace
{
    struct RawPeer
    {
        asio::io_context io;
        asio::ip::tcp::socket socket{io};
        FrameStream decoder;
        std::deque<Message> messages;

        explicit RawPeer(uint16_t port)
        {
            socket.connect({asio::ip::make_address("127.0.0.1"), port});
            socket.non_blocking(true);
        }

        void Bytes(const Buffer& bytes) { asio::write(socket, asio::buffer(bytes)); }
        void Send(const Message& message) { this->Bytes(Codec::Encode(message)); }
        void Close() { socket.close(); }

        std::optional<Message> Next(uint32_t timeout = 3000)
        {
            auto deadline = Clock::now() + std::chrono::milliseconds(timeout);
            std::array<uint8_t, 8192> bytes;
            while (messages.empty())
            {
                asio::error_code error;
                auto length = socket.read_some(asio::buffer(bytes), error);
                if (!error) decoder.Feed(std::span(bytes).first(length), [&] (auto frame) { messages.push_back(Codec::Decode(frame)); });
                else if (error != asio::error::would_block && error != asio::error::try_again) return {};
                if (Clock::now() >= deadline) throw std::runtime_error("Raw peer receive timeout");
                if (messages.empty()) std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            auto message = std::move(messages.front());
            messages.pop_front();
            return message;
        }

        template <typename _Message>
        _Message Read()
        {
            for (size_t i = 0; i < 16; ++i)
            {
                auto message = this->Next();
                if (!message) throw std::runtime_error("Raw peer closed before expected packet");
                if (auto result = std::get_if<_Message>(&*message)) return *result;
            }
            throw std::runtime_error("Unexpected raw peer packet sequence");
        }

        ConnectionAck Connect(SessionPolicy policy = {})
        {
            ConnectionRequest request;
            request.properties.policy = policy;
            this->Send(request);
            return this->Read<ConnectionAck>();
        }
    };

    template <typename _Predicate>
    void Until(_Predicate&& predicate, Client* client = nullptr, uint32_t timeout = 3000)
    {
        auto deadline = Clock::now() + std::chrono::milliseconds(timeout);
        do
        {
            if (client) client->Poll();
            if (predicate()) return;
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        } while (Clock::now() < deadline);
        throw std::runtime_error("Adversarial condition timed out");
    }
}

void TestAdversarial()
{
    BrokerOptions options;
    options.port = 0;
    options.retryInterval = 50;
    Broker broker(options);
    auto port = broker.Listen();
    {
        RawPeer wrongOrder(port);
        wrongOrder.Send(PublishAck{1, {}});
        EMPY_CHECK(wrongOrder.Read<ConnectionAck>().reason.code == MMQP_REASON_MALFORMED_PACKET);
        EMPY_CHECK(!wrongOrder.Next());
    }
    {
        RawPeer oversized(port);
        oversized.Bytes({0x89, 0x20, 0x00, 0x00}); // Advertise 2 MiB without sending its body.
        EMPY_CHECK(!oversized.Next());
    }
    Client subscriber;
    subscriber.Connect("127.0.0.1", port);
    EMPY_CHECK(subscriber.Subscribe("raw/value").get().code < 0x80);
    size_t received = 0;
    subscriber.On<Receive>("raw/value", [&] (const Receive&) { ++received; });
    {
        RawPeer publisher(port);
        EMPY_CHECK(publisher.Connect().reason.code == 0);
        publisher.Send(ProfileRequest{1, {{MMQP_PROREQ_TYPE_REG, 1, "raw/value", 0}}});
        EMPY_CHECK(publisher.Read<ProfileAck>().reason.code == 0);
        PublishRequest publication;
        publication.alias = 1;
        publication.qos = 1;
        publication.packetId = 1;
        publication.payload = {1, 2, 3};
        publisher.Send(publication);
        EMPY_CHECK(publisher.Read<PublishAck>().reason.code == 0);
        publication.duplicate = true;
        publisher.Send(publication); // Simulate an acknowledgement lost to the sender.
        EMPY_CHECK(publisher.Read<PublishAck>().reason.code == 0);
        Until([&] { return received == 1; }, &subscriber);
        publication.payload[0] ^= 1;
        publisher.Send(publication);
        EMPY_CHECK(publisher.Read<ConnectionEnd>().reason.code == MMQP_REASON_INVALID_PID);
    }
    {
        RawPeer silentSubscriber(port);
        silentSubscriber.Connect();
        silentSubscriber.Send(ProfileRequest{1, {{MMQP_PROREQ_TYPE_SUB, 1, "raw/retry", 1}}});
        EMPY_CHECK(silentSubscriber.Read<ProfileAck>().reason.code == 0);
        Client publisher;
        publisher.Connect("127.0.0.1", port);
        EMPY_CHECK(publisher.Register("raw/retry").get().code < 0x80);
        EMPY_CHECK(publisher.Publish("raw/retry", {7}, 1).get().code < 0x80);
        auto first = silentSubscriber.Read<PublishRequest>();
        auto retry = silentSubscriber.Read<PublishRequest>();
        EMPY_CHECK(first.packetId == retry.packetId && retry.duplicate && first.payload == retry.payload);
        silentSubscriber.Send(PublishAck{retry.packetId, {}});
        Until([&] { return broker.Metrics().pending == 0; });
    }
    {
        Client responder;
        responder.Connect("127.0.0.1", port);
        EMPY_CHECK(responder.Subscribe("raw/rpc", 1, true).get().code == 0);
        std::vector<PublishRequest> requests;
        responder.On<Receive>("raw/rpc", [&] (const auto& event) { requests.push_back(event.message); });
        RawPeer requester(port);
        requester.Connect();
        requester.Send(ProfileRequest{1, {{MMQP_PROREQ_TYPE_REG, 1, "raw/rpc", 0, true}}});
        EMPY_CHECK(requester.Read<ProfileAck>().reason.code == 0);
        PublishRequest request;
        request.type = MMQP_PUBREQ_TYPE_REQ;
        request.alias = 1;
        request.qos = 1;
        request.packetId = 1;
        request.correlationId = 100;
        request.payload = {'Q'};
        requester.Send(request);
        EMPY_CHECK(requester.Read<PublishAck>().reason.code == 0);
        request.duplicate = true;
        requester.Send(request);
        EMPY_CHECK(requester.Read<PublishAck>().reason.code == 0);
        Until([&] { return requests.size() == 1; }, &responder);
        request.packetId = 2;
        request.duplicate = false;
        requester.Send(request); // A new hop PID must not permit CID reuse.
        EMPY_CHECK(requester.Read<PublishAck>().reason.code == MMQP_REASON_INVALID_CORRELATION);
        request.packetId = 3;
        request.correlationId = 101;
        requester.Send(request);
        EMPY_CHECK(requester.Read<PublishAck>().reason.code == 0);
        Until([&] { return requests.size() == 2; }, &responder);
        EMPY_CHECK(responder.Respond(requests[1], {'B'}).get().code == 0);
        auto response = requester.Read<PublishRequest>();
        EMPY_CHECK(response.correlationId == 101 && response.payload == Buffer{'B'});
        requester.Send(PublishAck{response.packetId, {}});
        EMPY_CHECK(responder.Respond(requests[0], {'A'}).get().code == 0);
        response = requester.Read<PublishRequest>();
        EMPY_CHECK(response.correlationId == 100 && response.payload == Buffer{'A'});
        requester.Send(PublishAck{response.packetId, {}});
        request.duplicate = true;
        request.correlationId = 102;
        requester.Send(request); // The reliable receipt digest binds the CID.
        EMPY_CHECK(requester.Read<ConnectionEnd>().reason.code == MMQP_REASON_INVALID_PID);
    }
    {
        BrokerOptions bounded = options;
        bounded.maxSessionBytes = 64;
        Broker limited(bounded);
        auto limitedPort = limited.Listen();
        Client publisher, receiver;
        publisher.Connect("127.0.0.1", limitedPort);
        receiver.Connect("127.0.0.1", limitedPort);
        publisher.Register("budget/value").get();
        receiver.Subscribe("budget/value").get();
        uint16_t delivered = 0;
        receiver.On<Receive>("budget/value", [&] (const Receive& event) { delivered = event.message.packetId; });
        for (size_t i = 0; i < 3; ++i)
            EMPY_CHECK(publisher.Publish("budget/value", Buffer(128), 1).get().code == MMQP_REASON_BUSY);
        EMPY_CHECK(publisher.Publish("budget/value", {1}, 1).get().code < 0x80);
        Until([&] { return delivered != 0; }, &receiver);
        EMPY_CHECK(delivered == 1); // Failed fan-out admission must not consume the subscriber's PID.
    }
    {
        RawPeer responder(port);
        responder.Connect();
        responder.Send(ProfileRequest{1, {{MMQP_PROREQ_TYPE_SUB, 1, "raw/rpc-retry", 1, true}}});
        EMPY_CHECK(responder.Read<ProfileAck>().reason.code == 0);
        Client requester;
        requester.Connect("127.0.0.1", port);
        EMPY_CHECK(requester.Register("raw/rpc-retry", 0, 60, 1, true).get().code == 0);
        size_t responses = 0;
        requester.On<Receive>("raw/rpc-retry", [&] (const auto& event)
        { EMPY_CHECK(event.message.correlationId == 0x2345); ++responses; });
        EMPY_CHECK(requester.Request("raw/rpc-retry", {'Q'}, 1, 1, 0x2345).get().code == 0);
        auto request = responder.Read<PublishRequest>();
        auto retry = responder.Read<PublishRequest>();
        EMPY_CHECK(request.correlationId == retry.correlationId && request.packetId == retry.packetId && retry.duplicate);
        responder.Send(PublishAck{retry.packetId, {}});
        PublishRequest response;
        response.type = MMQP_PUBREQ_TYPE_RES;
        response.alias = 1;
        response.topic = "raw/rpc-retry";
        response.qos = 1;
        response.packetId = 1;
        response.correlationId = request.correlationId;
        response.payload = {'R'};
        responder.Send(response);
        EMPY_CHECK(responder.Read<PublishAck>().reason.code == 0);
        response.duplicate = true;
        responder.Send(response); // Retry after the response mapping was consumed.
        EMPY_CHECK(responder.Read<PublishAck>().reason.code == 0);
        Until([&] { return responses == 1 && broker.Metrics().pending == 0; }, &requester);
    }
    {
        RawPeer idle(port);
        auto ack = idle.Connect(SessionPolicy{false, EncodeInterval(1), EncodeInterval(1), {}});
        EMPY_CHECK(ack.reason.code == 0);
        EMPY_CHECK(!idle.Read<ConnectionSync>().token);
        EMPY_CHECK(idle.Read<ConnectionEnd>().reason.code == MMQP_REASON_KEEPALIVE_TIMEOUT);
        idle.Close();
        std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        RawPeer expired(port);
        ConnectionRequest resume;
        resume.resume = true;
        resume.properties.auth = SessionAuth{ack.properties.auth->identifier, {}};
        expired.Send(resume);
        EMPY_CHECK(expired.Read<ConnectionAck>().reason.code == MMQP_REASON_SESSION_EXPIRED);
    }
    {
        ClientOptions crypto;
        crypto.cipher = std::make_shared<PayloadCipher>(KeyGrant{Buffer(16, 0x11)}, 1, 1);
        Client publisher(crypto);
        crypto.cipher = std::make_shared<PayloadCipher>(KeyGrant{Buffer(16, 0x11)}, 1, 1);
        Client receiver(crypto);
        publisher.Connect("127.0.0.1", port);
        receiver.Connect("127.0.0.1", port);
        publisher.Register("encrypted/retained").get();
        receiver.Subscribe("encrypted/retained").get();
        size_t count = 0;
        receiver.On<Receive>("encrypted/#", [&] (const Receive&) { ++count; });
        publisher.Publish("encrypted/retained", {1, 2, 3}, 1).get();
        Until([&] { return count == 1; }, &receiver);
        receiver.Unsubscribe("encrypted/retained").get();
        receiver.Subscribe("encrypted/retained").get();
        Until([&] { return broker.Metrics().pending == 0; }, &receiver);
        EMPY_CHECK(count == 1 && receiver.Connected());
    }
    EMPY_CHECK(broker.Metrics().duplicates >= 1 && broker.Metrics().retransmitted >= 1);
    {
        Client publisher, wildcard;
        publisher.Connect("127.0.0.1", port);
        wildcard.Connect("127.0.0.1", port);
        publisher.Register("aliases/value").get();
        wildcard.Subscribe("aliases/#").get();
        uint16_t alias = 0;
        wildcard.On<Receive>("aliases/#", [&] (const Receive& event) { alias = event.message.alias; });
        publisher.Publish("aliases/value", {1}, 1).get();
        Until([&] { return alias != 0; }, &wildcard);
        EMPY_CHECK(alias >= 0x8000);
        EMPY_CHECK(wildcard.Subscribe("aliases/value").get().code == 0);
    }
    {
        BrokerOptions tenant = options;
        tenant.tenantRoot = "tenant";
        Broker isolated(tenant);
        auto isolatedPort = isolated.Listen();
        Client monitor;
        monitor.Connect("127.0.0.1", isolatedPort);
        EMPY_CHECK(monitor.Subscribe("other/#").get().code == MMQP_REASON_UNAUTHORIZED);
        EMPY_CHECK(monitor.Subscribe("$bwm/other/#").get().code == MMQP_REASON_UNAUTHORIZED);
        EMPY_CHECK(monitor.Subscribe("$bwm/tenant/#").get().code == 0);
        std::string birth;
        monitor.On<Receive>("$bwm/tenant/#", [&] (const Receive& event) { birth = event.message.topic; });
        ClientOptions status;
        status.properties.will = SessionWill{Buffer{1}, {}, {}, 0};
        Client device(status);
        auto sid = device.Connect("127.0.0.1", isolatedPort);
        Until([&] { return !birth.empty(); }, &monitor);
        EMPY_CHECK(birth == "$bwm/tenant/" + sid);
    }
}
