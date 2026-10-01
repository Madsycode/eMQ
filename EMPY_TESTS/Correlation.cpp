#include "Checks.h"
#include <eClient/API.h>
#include <eMQ/Runtime/Broker.h>
#include <filesystem>
#include <thread>

using namespace eMQ;

namespace
{
    template <typename _Predicate>
    void Await(_Predicate&& predicate, std::initializer_list<Client*> clients = {}, uint32_t timeout = 4000)
    {
        auto deadline = Clock::now() + std::chrono::milliseconds(timeout);
        do
        {
            for (auto client : clients) client->Poll();
            if (predicate()) return;
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        } while (Clock::now() < deadline);
        throw std::runtime_error("Correlation exchange timed out");
    }

    void Accepted(std::future<Reason> result)
    {
        EMPY_CHECK(result.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        auto reason = result.get();
        if (reason.code >= 0x80) throw std::runtime_error("Correlation exchange rejected: " + reason.text);
    }

    void Concurrent(bool encrypted)
    {
        BrokerOptions settings;
        settings.port = 0;
        Broker broker(settings);
        auto port = broker.Listen();
        auto options = [encrypted]
        {
            ClientOptions result;
            if (encrypted) result.cipher = std::make_shared<PayloadCipher>(KeyGrant{Buffer(16, 0x51)}, 1, 1);
            return result;
        };
        Client first(options()), second(options()), responder(options()), intruder(options());
        first.Connect("127.0.0.1", port);
        second.Connect("127.0.0.1", port);
        responder.Connect("127.0.0.1", port);
        intruder.Connect("127.0.0.1", port);
        Accepted(first.Register("rpc/#", 0, 60, 1, true));
        Accepted(second.Register("rpc/#", 0, 60, 1, true));
        Accepted(responder.Subscribe("rpc/#", 1, true));
        std::vector<PublishRequest> incoming, firstReplies, secondReplies;
        responder.On<Receive>("rpc/#", [&] (const Receive& event)
        { EMPY_CHECK(event.decrypted == encrypted); incoming.push_back(event.message); });
        first.On<Receive>("rpc/#", [&] (const Receive& event)
        { EMPY_CHECK(event.decrypted == encrypted); firstReplies.push_back(event.message); });
        second.On<Receive>("rpc/#", [&] (const Receive& event)
        { EMPY_CHECK(event.decrypted == encrypted); secondReplies.push_back(event.message); });

        for (uint8_t qos : {uint8_t{1}, uint8_t{0}})
        {
            incoming.clear();
            firstReplies.clear();
            secondReplies.clear();
            auto id = static_cast<uint16_t>(qos ? 4001 : 2001);
            // Concurrent callers, nonmonotonic explicit IDs, and equal IDs in
            // different requester sessions must remain independent.
            auto a = std::async(std::launch::async, [&] { return first.Request("rpc/value", {'A'}, 1, qos, id + 1).get(); });
            auto b = std::async(std::launch::async, [&] { return first.Request("rpc/value", {'B'}, 1, qos, id).get(); });
            auto c = std::async(std::launch::async, [&] { return second.Request("rpc/value", {'C'}, 1, qos, id).get(); });
            Accepted(std::move(a));
            Accepted(std::move(b));
            Accepted(std::move(c));
            Await([&] { return incoming.size() == 3; }, {&responder});
            std::set<uint16_t> routes;
            for (const auto& request : incoming) EMPY_CHECK(routes.insert(request.correlationId).second);

            auto wrong = incoming.front();
            wrong.topic = "rpc/wrong";
            EMPY_CHECK(responder.Respond(wrong.topic, wrong.correlationId, {'X'}).get().code == MMQP_REASON_INVALID_CORRELATION);
            EMPY_CHECK(responder.Respond("rpc/value", UINT16_MAX, {'X'}).get().code == MMQP_REASON_INVALID_CORRELATION);
            Throws([&] { responder.Respond("rpc/value", 0, {'X'}).get(); });
            Accepted(intruder.Subscribe("rpc/#", 1, true));
            EMPY_CHECK(intruder.Respond("rpc/value", incoming.front().correlationId, {'X'}).get().code == MMQP_REASON_INVALID_CORRELATION);
            Accepted(intruder.Unsubscribe("rpc/#"));
            for (auto it = incoming.rbegin(); it != incoming.rend(); ++it) Accepted(responder.Respond(*it, it->payload));
            Await([&] { return firstReplies.size() == 2 && secondReplies.size() == 1; }, {&first, &second});
            for (const auto& reply : firstReplies)
            {
                EMPY_CHECK(reply.correlationId == (reply.payload == Buffer{'A'} ? id + 1 : id));
                EMPY_CHECK(reply.payload == Buffer{'A'} || reply.payload == Buffer{'B'});
            }
            EMPY_CHECK(secondReplies.front().correlationId == id && secondReplies.front().payload == Buffer{'C'});
            EMPY_CHECK(responder.Respond("rpc/value", incoming.front().correlationId, {'X'}).get().code == MMQP_REASON_INVALID_CORRELATION);
            Throws([&] { first.Request("rpc/value", {}, 1, 1, id).get(); });
        }

        incoming.clear();
        firstReplies.clear();
        Accepted(first.Request("rpc/value", {'D'}));
        Accepted(first.Request("rpc/value", {'E'}));
        Await([&] { return incoming.size() == 2; }, {&responder});
        Accepted(responder.Respond(incoming[1], incoming[1].payload));
        Accepted(responder.Respond(incoming[0], incoming[0].payload));
        Await([&] { return firstReplies.size() == 2; }, {&first});
        EMPY_CHECK(firstReplies[0].payload == Buffer{'E'} && firstReplies[1].payload == Buffer{'D'});
        EMPY_CHECK(firstReplies[0].correlationId == 2 && firstReplies[1].correlationId == 1);
    }

    void Deadlines()
    {
        BrokerOptions settings;
        settings.port = 0;
        settings.requestTimeout = 350;
        settings.maxRequests = 2;
        Broker broker(settings);
        auto port = broker.Listen();
        Client requester, responder;
        requester.Connect("127.0.0.1", port);
        responder.Connect("127.0.0.1", port);
        Accepted(requester.Register("rpc/#", 0, 60, 1, true));
        Accepted(responder.Subscribe("rpc/slow", 1, true));
        std::vector<PublishRequest> incoming, replies;
        responder.On<Receive>("rpc/#", [&] (const auto& event) { incoming.push_back(event.message); });
        requester.On<Receive>("rpc/#", [&] (const auto& event) { replies.push_back(event.message); });
        EMPY_CHECK(requester.Request("rpc/missing", {}, 1, 1, 40).get().code == MMQP_REASON_NO_RESPONDER);
        Await([&] { return replies.size() == 1; }, {&requester});
        EMPY_CHECK(replies.back().correlationId == 40 && replies.back().reason.code == MMQP_REASON_NO_RESPONDER);
        Accepted(requester.Request("rpc/slow", {}, 1, 1, 41));
        Accepted(requester.Request("rpc/slow", {}, 1, 1, 42));
        EMPY_CHECK(requester.Request("rpc/slow", {}, 1, 1, 43).get().code == MMQP_REASON_BUSY);
        Await([&] { return incoming.size() == 2 && replies.size() == 3; }, {&requester, &responder});
        std::set<uint16_t> timedOut;
        for (size_t i = 1; i < replies.size(); ++i)
        { EMPY_CHECK(replies[i].reason.code == MMQP_REASON_TIMEOUT); timedOut.insert(replies[i].correlationId); }
        EMPY_CHECK(timedOut == std::set<uint16_t>({41, 42}));
        Accepted(requester.Request("rpc/slow", {'N'}, 1, 1, 44));
        Await([&] { return incoming.size() == 3; }, {&responder});
        EMPY_CHECK(responder.Respond(incoming[0], {'L'}).get().code == MMQP_REASON_INVALID_CORRELATION);
        Accepted(responder.Respond(incoming[2], {'N'}));
        Await([&] { return replies.size() == 4; }, {&requester});
        EMPY_CHECK(replies.back().correlationId == 44 && replies.back().payload == Buffer{'N'});
    }

    struct Directory
    {
        std::filesystem::path path = std::filesystem::temp_directory_path() / ("EmpyCorrelation-" + ToHex(RandomBytes(8)));
        ~Directory() { std::error_code error; std::filesystem::remove_all(path, error); }
    };

    void Recovery()
    {
        Directory directory;
        BrokerOptions settings;
        settings.port = 0;
        settings.requestTimeout = 10000;
        settings.stateDirectory = directory.path.string();
        auto broker = std::make_unique<Broker>(settings);
        auto port = broker->Listen();
        Client requester, responder, other;
        auto requesterId = requester.Connect("127.0.0.1", port);
        auto responderId = responder.Connect("127.0.0.1", port);
        auto otherId = other.Connect("127.0.0.1", port);
        Accepted(requester.Register("rpc/restart", 0, 60, 1, true));
        Accepted(responder.Subscribe("rpc/restart", 1, true));
        Accepted(other.Subscribe("rpc/restart", 1, true));
        std::vector<PublishRequest> first, second, replies;
        responder.On<Receive>("rpc/restart", [&] (const auto& event) { first.push_back(event.message); });
        other.On<Receive>("rpc/restart", [&] (const auto& event) { second.push_back(event.message); });
        requester.On<Receive>("rpc/restart", [&] (const auto& event) { replies.push_back(event.message); });
        Accepted(requester.Request("rpc/restart", {'A'}, 0, 1, 51));
        Accepted(requester.Request("rpc/restart", {'B'}, 2, 1, 52));
        Await([&] { return first.size() == 2 && second.size() == 2; }, {&responder, &other});
        Accepted(responder.Respond(first[0], {'A', '1'}));
        Await([&] { return replies.size() == 1 && broker->Metrics().pending == 0; }, {&requester});
        requester.Disconnect();
        responder.Disconnect();
        other.Disconnect();
        Await([&] { return broker->Metrics().online == 0; });
        broker.reset();
        broker = std::make_unique<Broker>(settings);
        port = broker->Listen();
        EMPY_CHECK(requester.Connect("127.0.0.1", port, true) == requesterId);
        EMPY_CHECK(responder.Connect("127.0.0.1", port, true) == responderId);
        EMPY_CHECK(other.Connect("127.0.0.1", port, true) == otherId);
        EMPY_CHECK(responder.Respond(first[0], {'X'}).get().code == MMQP_REASON_INVALID_CORRELATION);
        Accepted(other.Respond(second[1], {'B', '2'}));
        Accepted(responder.Respond(first[1], {'B', '1'}));
        Accepted(other.Respond(second[0], {'A', '2'}));
        Await([&] { return replies.size() == 4; }, {&requester});
        std::map<uint16_t, size_t> counts;
        for (const auto& reply : replies)
        {
            ++counts[reply.correlationId];
            EMPY_CHECK(reply.correlationId == (reply.payload.front() == 'A' ? 51 : 52));
        }
        EMPY_CHECK(counts[51] == 2 && counts[52] == 2);
        Throws([&] { requester.Request("rpc/restart", {}, 1, 1, 51).get(); });
        Accepted(requester.Request("rpc/restart", {'C'}, 0));
        Await([&] { return first.size() == 3 && second.size() == 3; }, {&responder, &other});
        EMPY_CHECK(first[2].correlationId != first[0].correlationId && first[2].correlationId != first[1].correlationId);
        Accepted(responder.Respond(first[2], {'C', '1'}));
        Accepted(other.Respond(second[2], {'C', '2'}));
        Await([&] { return replies.size() == 6; }, {&requester});
        EMPY_CHECK(replies[4].correlationId == 1 && replies[5].correlationId == 1);
    }
}

void TestCorrelation()
{
    Concurrent(false);
    Concurrent(true);
    Deadlines();
    Recovery();
}
