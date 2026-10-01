#include "Checks.h"
#include <eClient/API.h>
#include <eMQ/Runtime/Broker.h>
#include <eMQ/Network/QContext.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <filesystem>
#include <thread>

using namespace eMQ;

namespace
{
    template <typename _Predicate>
    void Until(_Predicate&& predicate, std::initializer_list<Client*> clients = {}, uint32_t timeout = 3000)
    {
        auto deadline = Clock::now() + std::chrono::milliseconds(timeout);
        do
        {
            for (auto client : clients) client->Poll();
            if (predicate()) return;
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        } while (Clock::now() < deadline);
        throw std::runtime_error("Integration condition timed out");
    }

    void Accepted(std::future<Reason> result)
    {
        EMPY_CHECK(result.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        auto reason = result.get();
        if (reason.code >= 0x80) throw std::runtime_error("Operation rejected: " + reason.text);
    }

    struct Certificate
    {
        std::filesystem::path folder;
        std::string certificate;
        std::string privateKey;

        Certificate()
        {
            folder = std::filesystem::temp_directory_path() / ("EmpyTlsTest-" + ToHex(RandomBytes(8)));
            std::filesystem::create_directory(folder);
            certificate = (folder / "certificate.pem").string();
            privateKey = (folder / "private-key.pem").string();
            std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(
                EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "prime256v1"), EVP_PKEY_free);
            std::unique_ptr<X509, decltype(&X509_free)> cert(X509_new(), X509_free);
            EMPY_CHECK(key && cert);
            EMPY_CHECK(X509_set_version(cert.get(), 2) == 1);
            EMPY_CHECK(ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), 1) == 1);
            EMPY_CHECK(X509_gmtime_adj(X509_getm_notBefore(cert.get()), -60));
            EMPY_CHECK(X509_gmtime_adj(X509_getm_notAfter(cert.get()), 3600));
            EMPY_CHECK(X509_set_pubkey(cert.get(), key.get()) == 1);
            auto subject = X509_get_subject_name(cert.get());
            EMPY_CHECK(X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC,
                reinterpret_cast<const unsigned char*>("localhost"), -1, -1, 0) == 1);
            EMPY_CHECK(X509_set_issuer_name(cert.get(), subject) == 1);
            std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)> san(
                X509V3_EXT_conf_nid(nullptr, nullptr, NID_subject_alt_name,
                    const_cast<char*>("DNS:localhost,IP:127.0.0.1")), X509_EXTENSION_free);
            EMPY_CHECK(san && X509_add_ext(cert.get(), san.get(), -1) == 1);
            EMPY_CHECK(X509_sign(cert.get(), key.get(), EVP_sha256()) > 0);
            auto certFile = std::fopen(certificate.c_str(), "wb");
            EMPY_CHECK(certFile);
            EMPY_CHECK(PEM_write_X509(certFile, cert.get()) == 1);
            std::fclose(certFile);
            auto keyFile = std::fopen(privateKey.c_str(), "wb");
            EMPY_CHECK(keyFile);
            EMPY_CHECK(PEM_write_PrivateKey(keyFile, key.get(), nullptr, nullptr, 0, nullptr, nullptr) == 1);
            std::fclose(keyFile);
        }

        ~Certificate()
        {
            std::error_code ignored;
            std::filesystem::remove(certificate, ignored);
            std::filesystem::remove(privateKey, ignored);
            std::filesystem::remove(folder, ignored);
        }
    };
}

void TestIntegration()
{
    BrokerOptions settings;
    settings.port = 0;
    settings.requestTimeout = 800;
    settings.retryInterval = 100;
    Broker broker(settings);
    auto port = broker.Listen();
    EMPY_CHECK(port != 0);

    Client publisher;
    Client subscriber;
    std::vector<PublishRequest> received;
    subscriber.On<Receive>("room/#", [&] (const Receive& event) { received.push_back(event.message); });
    publisher.Connect("127.0.0.1", port);
    auto session = subscriber.Connect("127.0.0.1", port);
    Accepted(subscriber.Subscribe("room/#"));
    Accepted(publisher.Register("room/#", 1, 60, 2));
    Accepted(publisher.Publish("room/temp", {'0'}, 0));
    Until([&] { return received.size() == 1; }, {&subscriber});
    EMPY_CHECK(received.back().payload == Buffer{'0'});
    for (char value : {'A', 'B', 'C'}) Accepted(publisher.Publish("room/temp", Buffer{static_cast<uint8_t>(value)}, 1));
    Until([&] { return received.size() == 4; }, {&subscriber});
    EMPY_CHECK(received.back().payload == Buffer{'C'});

    Client late;
    std::vector<Buffer> replay;
    late.On<Receive>("room/#", [&] (const Receive& event) { replay.push_back(event.message.payload); });
    late.Connect("127.0.0.1", port);
    Accepted(late.Subscribe("room/#"));
    Until([&] { return replay.size() == 2; }, {&late});
    EMPY_CHECK(replay[0] == Buffer{'B'} && replay[1] == Buffer{'C'});

    subscriber.Disconnect(false);
    Until([&] { return broker.Metrics().online == 2; });
    Accepted(publisher.Publish("room/temp", {'D'}, 1));
    EMPY_CHECK(subscriber.Connect("127.0.0.1", port, true) == session);
    Until([&] { return received.size() == 5; }, {&subscriber});
    EMPY_CHECK(received.back().payload == Buffer{'D'});
    Accepted(subscriber.Unsubscribe("room/#"));
    Accepted(publisher.Publish("room/temp", {'E'}, 1));
    Until([&] { return broker.Metrics().pending == 0; }, {&subscriber, &late});
    EMPY_CHECK(received.size() == 5);

    Client sharedA, sharedB, qosZero;
    size_t first = 0, second = 0, zero = 0;
    sharedA.On<Receive>("work/#", [&] (const auto&) { ++first; });
    sharedB.On<Receive>("work/#", [&] (const auto&) { ++second; });
    qosZero.On<Receive>("work/#", [&] (const auto&) { ++zero; });
    sharedA.Connect("127.0.0.1", port);
    sharedB.Connect("127.0.0.1", port);
    qosZero.Connect("127.0.0.1", port);
    Accepted(sharedA.Subscribe("$share/workers/work/#"));
    Accepted(sharedB.Subscribe("$share/workers/work/#"));
    Accepted(qosZero.Subscribe("work/#", 0));
    Accepted(publisher.Register("work/#"));
    for (int i = 0; i < 4; ++i) Accepted(publisher.Publish("work/task", {'x'}, 0));
    Until([&] { return first + second == 4 && zero == 4; }, {&sharedA, &sharedB, &qosZero});
    EMPY_CHECK(first == 2 && second == 2);
    Accepted(publisher.Publish("work/task", {'y'}, 1));
    Until([&] { return first + second == 5; }, {&sharedA, &sharedB, &qosZero});
    EMPY_CHECK(zero == 4);

    Client responder;
    responder.Connect("127.0.0.1", port);
    responder.On<Receive>("rpc/temp", [&] (const Receive& event)
    {
        EMPY_CHECK(event.message.type == MMQP_PUBREQ_TYPE_REQ);
        Accepted(responder.Respond(event.message, {'4', '2'}));
    });
    std::vector<PublishRequest> responses;
    publisher.On<Receive>("rpc/#", [&] (const Receive& event) { responses.push_back(event.message); });
    Accepted(responder.Subscribe("rpc/temp", 1, true));
    Accepted(responder.Subscribe("rpc/slow", 1, true));
    Accepted(publisher.Register("rpc/#", 0, 60, 1, true));
    Accepted(publisher.Request("rpc/temp", {'?'}, 1));
    Until([&] { return responses.size() == 1; }, {&responder, &publisher});
    EMPY_CHECK(responses.back().payload == Buffer({'4', '2'}));
    EMPY_CHECK(publisher.Request("rpc/missing", {}).get().code == MMQP_REASON_NO_RESPONDER);
    Until([&] { return responses.size() == 2; }, {&publisher});
    EMPY_CHECK(responses.back().reason.code == MMQP_REASON_NO_RESPONDER && responses.back().payload.empty());
    Accepted(publisher.Request("rpc/slow", {}));
    Accepted(publisher.Request("rpc/slow", {}));
    Until([&] { return responses.size() == 4; }, {&publisher, &responder});
    EMPY_CHECK(responses[2].reason.code == MMQP_REASON_TIMEOUT && responses[3].reason.code == MMQP_REASON_TIMEOUT);
    EMPY_CHECK(responses[2].correlationId != responses[3].correlationId);

    Client monitor;
    monitor.Connect("127.0.0.1", port);
    std::vector<std::string> wills;
    monitor.On<Receive>("$bwm/#", [&] (const auto& event) { wills.push_back(event.message.topic); });
    monitor.On<Receive>("$lwm/#", [&] (const auto& event) { wills.push_back(event.message.topic); });
    Accepted(monitor.Subscribe("$bwm/#"));
    Accepted(monitor.Subscribe("$lwm/#"));
    ClientOptions willOptions;
    willOptions.properties.will = SessionWill{Buffer{'1'}, Buffer{'0'}, "device/1", 0};
    Client status(willOptions);
    status.Connect("127.0.0.1", port);
    Until([&] { return wills.size() == 1; }, {&monitor});
    status.Disconnect();
    Until([&] { return wills.size() == 2; }, {&monitor});
    EMPY_CHECK(wills[0] == "$bwm/device/1" && wills[1] == "$lwm/device/1");

    auto secret = FromHex("000102030405060708090a0b0c0d0e0f");
    ClientOptions producerOptions, consumerOptions;
    producerOptions.cipher = std::make_shared<PayloadCipher>(KeyGrant{secret}, 1, 1);
    consumerOptions.cipher = std::make_shared<PayloadCipher>(KeyGrant{secret}, 1, 1);
    Client encryptedPublisher(producerOptions), encryptedSubscriber(consumerOptions);
    encryptedPublisher.Connect("127.0.0.1", port);
    encryptedSubscriber.Connect("127.0.0.1", port);
    bool decrypted = false;
    encryptedSubscriber.On<Receive>("secret/#", [&] (const Receive& event)
    {
        EMPY_CHECK(event.decrypted && event.message.payload == Buffer({'h', 'i'}));
        decrypted = true;
    });
    Accepted(encryptedPublisher.Register("secret/#"));
    Accepted(encryptedSubscriber.Subscribe("secret/#"));
    Accepted(encryptedPublisher.Publish("secret/value", {'h', 'i'}, 1));
    Until([&] { return decrypted; }, {&encryptedSubscriber});

    Certificate certificate;
    BrokerOptions secured;
    secured.port = 0;
    secured.transport.tls = true;
    secured.transport.certificate = certificate.certificate;
    secured.transport.privateKey = certificate.privateKey;
    secured.requireAuth = true;
    secured.authKey = Buffer(32, 0x42);
    Broker secureBroker(secured);
    auto securePort = secureBroker.Listen();
    ClientOptions trusted;
    trusted.transport.tls = true;
    trusted.transport.authority = certificate.certificate;
    trusted.transport.serverName = "localhost";
    trusted.authKey = secured.authKey;
    Client trustedClient(trusted);
    EMPY_CHECK(!trustedClient.Connect("127.0.0.1", securePort).empty());
    ClientOptions anonymous = trusted;
    anonymous.authKey.clear();
    Client anonymousClient(anonymous);
    Throws([&] { anonymousClient.Connect("127.0.0.1", securePort); });
    ClientOptions wrongKey = trusted;
    wrongKey.authKey.assign(32, 0x43);
    Client wrongClient(wrongKey);
    Throws([&] { wrongClient.Connect("127.0.0.1", securePort); });
    ClientOptions wrongName = trusted;
    wrongName.transport.serverName = "untrusted.example";
    Client wrongHost(wrongName);
    Throws([&] { wrongHost.Connect("127.0.0.1", securePort); });
    EMPY_CHECK(broker.Metrics().publications >= 10);
}

void TestQuic()
{
    EMPY_CHECK(QcContext::Available());
    Certificate certificate;
    BrokerOptions settings;
    settings.port = 0;
    settings.transport.quic = true;
    settings.transport.certificate = certificate.certificate;
    settings.transport.privateKey = certificate.privateKey;
    settings.transport.quicPolicy = {MMQP_QUIC_STRATEGY_BULK, BulkPolicy{2, 1024}, 1200};
    settings.requireAuth = true;
    settings.authKey = Buffer(32, 0x42);
    Broker broker(settings);
    auto port = broker.Listen();
    ClientOptions trusted;
    trusted.transport.quic = true;
    trusted.transport.authority = certificate.certificate;
    trusted.transport.serverName = "localhost";
    trusted.authKey = settings.authKey;
    for (auto strategy : {MMQP_QUIC_STRATEGY_SINGLE, MMQP_QUIC_STRATEGY_SEMANTIC, MMQP_QUIC_STRATEGY_BULK})
    {
        auto options = trusted;
        options.properties.quic = QuicPolicy{strategy, {}, 1200};
        if (strategy == MMQP_QUIC_STRATEGY_BULK) options.properties.quic->bulk = BulkPolicy{2, 1024};
        Client subscriber(options), publisher(options);
        auto identifier = subscriber.Connect("127.0.0.1", port);
        publisher.Connect("127.0.0.1", port);
        auto topic = "quic/" + std::to_string(strategy) + "/value";
        Accepted(subscriber.Subscribe(topic));
        Accepted(publisher.Register(topic));
        std::map<size_t, size_t> received;
        subscriber.On<Receive>(topic, [&] (const Receive& event)
        {
            EMPY_CHECK(!event.message.payload.empty() && event.message.payload.front() == event.message.payload.back());
            ++received[event.message.payload.size()];
        });
        Accepted(publisher.Publish(topic, Buffer(32, 1), 0));
        Accepted(publisher.Publish(topic, Buffer(2048, 2), 0)); // Too large for a datagram; uses a stream.
        Accepted(publisher.Publish(topic, Buffer(128 * 1024, 3), 1));
        Until([&] { return received[32] == 1 && received[2048] == 1 && received[128 * 1024] == 1; }, {&subscriber}, 5000);
        std::vector<std::future<Reason>> pending;
        for (size_t i = 0; i < 8; ++i)
            pending.push_back(publisher.Publish(topic, Buffer(i % 2 ? 4096 : 128 * 1024, static_cast<uint8_t>(i)), 1));
        for (auto& result : pending) Accepted(std::move(result));
        Until([&] { return received[4096] == 4 && received[128 * 1024] == 5; }, {&subscriber}, 5000);
        auto rpc = topic + "/rpc";
        Accepted(subscriber.Subscribe(rpc, 1, true));
        Accepted(publisher.Register(rpc, 0, 60, 1, true));
        std::vector<PublishRequest> requests, responses;
        subscriber.On<Receive>(rpc, [&] (const auto& event) { requests.push_back(event.message); });
        publisher.On<Receive>(rpc, [&] (const auto& event) { responses.push_back(event.message); });
        pending.clear();
        for (uint16_t id = 1; id <= 3; ++id)
            pending.push_back(publisher.Request(rpc, Buffer(id == 1 ? 32 : id == 2 ? 2048 : 128 * 1024,
                static_cast<uint8_t>(id)), 1, id == 3 ? 1 : 0, id));
        for (auto& result : pending) Accepted(std::move(result));
        Until([&] { return requests.size() == 3; }, {&subscriber}, 5000);
        for (auto it = requests.rbegin(); it != requests.rend(); ++it) Accepted(subscriber.Respond(*it, it->payload));
        Until([&] { return responses.size() == 3; }, {&publisher}, 5000);
        std::set<uint16_t> correlations;
        for (const auto& response : responses)
        {
            EMPY_CHECK(response.correlationId == response.payload.front());
            correlations.insert(response.correlationId);
        }
        EMPY_CHECK(correlations == std::set<uint16_t>({1, 2, 3}));
        subscriber.Disconnect();
        EMPY_CHECK(subscriber.Connect("127.0.0.1", port, true) == identifier);
        Accepted(publisher.Publish(topic, Buffer(64, 4), 1));
        Until([&] { return received[64] == 1; }, {&subscriber});
        std::vector<std::future<Reason>> canceled;
        for (size_t i = 0; i < 8; ++i) canceled.push_back(publisher.Publish(topic, Buffer(256 * 1024, 5), 1));
        publisher.Disconnect(false);
        for (auto& operation : canceled)
        {
            EMPY_CHECK(operation.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
            try { operation.get(); } catch (const std::exception&) {} // Delivery may finish or be canceled.
        }
        subscriber.Disconnect();
    }
    auto wrongName = trusted;
    wrongName.transport.serverName = "untrusted.example";
    Client wrongHost(wrongName);
    Throws([&] { wrongHost.Connect("127.0.0.1", port); });
    auto wrongKey = trusted;
    wrongKey.authKey.assign(32, 0x43);
    Client unauthorized(wrongKey);
    Throws([&] { unauthorized.Connect("127.0.0.1", port); });
    auto mutual = settings;
    mutual.transport.mutual = true;
    mutual.transport.authority = certificate.certificate;
    Broker mutualBroker(mutual);
    auto mutualPort = mutualBroker.Listen();
    Client missingCertificate(trusted);
    Throws([&] { missingCertificate.Connect("127.0.0.1", mutualPort); });
    auto identified = trusted;
    identified.transport.certificate = certificate.certificate;
    identified.transport.privateKey = certificate.privateKey;
    Client mutualClient(identified);
    EMPY_CHECK(!mutualClient.Connect("127.0.0.1", mutualPort).empty());
    mutualClient.Disconnect();
}
