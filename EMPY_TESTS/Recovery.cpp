#include "Checks.h"
#include <eClient/API.h>
#include <eMQ/Runtime/Broker.h>
#include <eMQ/Network/QContext.h>
#include <eMQ/Cluster/Clock.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <fstream>
#include <iostream>
#include <thread>

using namespace eMQ;

namespace
{
    template <typename _Predicate>
    void Await(_Predicate&& predicate, std::initializer_list<Client*> clients = {}, uint32_t timeout = 8000)
    {
        auto deadline = Clock::now() + std::chrono::milliseconds(timeout);
        do
        {
            for (auto client : clients) client->Poll();
            if (predicate()) return;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        } while (Clock::now() < deadline);
        throw std::runtime_error("Recovery test condition timed out");
    }

    void Okay(std::future<Reason> result)
    {
        EMPY_CHECK(result.wait_for(std::chrono::seconds(12)) == std::future_status::ready);
        auto reason = result.get();
        if (reason.code >= 0x80) throw std::runtime_error("Recovery operation rejected: " + reason.text);
    }

    struct Fixture
    {
        std::filesystem::path directory;
        std::vector<std::string> certificates;
        std::vector<std::string> keys;
        std::vector<std::string> fingerprints;
        std::string authority;

        Fixture()
        {
            directory = std::filesystem::temp_directory_path() / ("EmpyRecovery-" + ToHex(RandomBytes(8)));
            std::filesystem::create_directories(directory);
            authority = (directory / "authority.pem").string();
            std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> root(EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "prime256v1"), EVP_PKEY_free);
            std::unique_ptr<X509, decltype(&X509_free)> ca(X509_new(), X509_free);
            auto setup = [] (X509* cert, EVP_PKEY* key, int serial, const char* name)
            {
                EMPY_CHECK(cert && key && X509_set_version(cert, 2) == 1 && ASN1_INTEGER_set(X509_get_serialNumber(cert), serial) == 1);
                EMPY_CHECK(X509_gmtime_adj(X509_getm_notBefore(cert), -60));
                EMPY_CHECK(X509_gmtime_adj(X509_getm_notAfter(cert), 3600));
                EMPY_CHECK(X509_set_pubkey(cert, key) == 1);
                EMPY_CHECK(X509_NAME_add_entry_by_txt(X509_get_subject_name(cert), "CN", MBSTRING_ASC,
                    reinterpret_cast<const unsigned char*>(name), -1, -1, 0) == 1);
            };
            auto extension = [] (X509* cert, int nid, const char* value)
            {
                std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)> entry(
                    X509V3_EXT_conf_nid(nullptr, nullptr, nid, const_cast<char*>(value)), X509_EXTENSION_free);
                EMPY_CHECK(entry && X509_add_ext(cert, entry.get(), -1) == 1);
            };
            auto writeCertificate = [] (const std::string& path, X509* cert)
            {
                auto file = std::fopen(path.c_str(), "wb");
                EMPY_CHECK(file);
                auto result = PEM_write_X509(file, cert);
                std::fclose(file);
                EMPY_CHECK(result == 1);
            };
            setup(ca.get(), root.get(), 1, "Empy recovery test CA");
            X509_set_issuer_name(ca.get(), X509_get_subject_name(ca.get()));
            extension(ca.get(), NID_basic_constraints, "critical,CA:TRUE");
            extension(ca.get(), NID_key_usage, "critical,keyCertSign,cRLSign");
            EMPY_CHECK(X509_sign(ca.get(), root.get(), EVP_sha256()) > 0);
            writeCertificate(authority, ca.get());
            for (int i = 0; i < 3; ++i)
            {
                std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "prime256v1"), EVP_PKEY_free);
                std::unique_ptr<X509, decltype(&X509_free)> certificate(X509_new(), X509_free);
                setup(certificate.get(), key.get(), i + 2, "localhost");
                X509_set_issuer_name(certificate.get(), X509_get_subject_name(ca.get()));
                extension(certificate.get(), NID_basic_constraints, "critical,CA:FALSE");
                extension(certificate.get(), NID_key_usage, "critical,digitalSignature");
                extension(certificate.get(), NID_ext_key_usage, "serverAuth,clientAuth");
                extension(certificate.get(), NID_subject_alt_name, "DNS:localhost,IP:127.0.0.1");
                EMPY_CHECK(X509_sign(certificate.get(), root.get(), EVP_sha256()) > 0);
                certificates.push_back((directory / ("broker-" + std::to_string(i) + ".pem")).string());
                keys.push_back((directory / ("broker-" + std::to_string(i) + ".key")).string());
                writeCertificate(certificates.back(), certificate.get());
                auto file = std::fopen(keys.back().c_str(), "wb");
                EMPY_CHECK(file);
                auto success = PEM_write_PrivateKey(file, key.get(), nullptr, nullptr, 0, nullptr, nullptr);
                std::fclose(file);
                EMPY_CHECK(success == 1);
                Digest fingerprint{};
                unsigned int size = 0;
                EMPY_CHECK(X509_digest(certificate.get(), EVP_sha256(), fingerprint.data(), &size) == 1 && size == 32);
                fingerprints.push_back(ToHex(fingerprint));
            }
        }

        ~Fixture()
        {
            std::error_code error;
            std::filesystem::remove_all(directory, error);
        }

        std::array<ClusterOptions, 3> Options(bool quic)
        {
            asio::io_context context;
            std::vector<std::unique_ptr<asio::ip::tcp::acceptor>> reservations;
            std::vector<std::unique_ptr<asio::ip::udp::socket>> datagrams;
            std::vector<ClusterPeer> peers;
            for (size_t i = 0; i < 3; ++i)
            {
                auto socket = std::make_unique<asio::ip::tcp::acceptor>(context, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
                auto datagram = std::make_unique<asio::ip::udp::socket>(context);
                datagram->open(asio::ip::udp::v4());
                asio::error_code error;
                datagram->bind(asio::ip::udp::endpoint(asio::ip::make_address("127.0.0.1"), socket->local_endpoint().port()), error);
                if (error) { --i; continue; }
                peers.push_back({"broker" + std::to_string(i), "127.0.0.1", socket->local_endpoint().port(), "localhost", fingerprints[i]});
                reservations.push_back(std::move(socket));
                datagrams.push_back(std::move(datagram));
            }
            std::array<ClusterOptions, 3> settings;
            for (size_t i = 0; i < settings.size(); ++i)
            {
                auto& option = settings[i];
                option.identifier = peers[i].identifier;
                option.port = peers[i].port;
                option.directory = (directory / ("state-" + std::to_string(i))).string();
                option.peers = peers;
                option.fabric.brokers = {"broker0", "broker1", "broker2"};
                option.fabric.shards = 4;
                option.fabric.replicas = 3;
                option.fabric.fanout = 2;
                option.epochKey = Buffer(32, 0x39);
                option.transport.tls = true;
                option.transport.quic = quic;
                option.transport.mutual = true;
                option.transport.certificate = certificates[i];
                option.transport.privateKey = keys[i];
                option.transport.authority = authority;
                option.transport.maxPacket = 8 * 1024 * 1024;
                option.transport.maxQueue = 32 * 1024 * 1024;
                option.leaseMilliseconds = 3000;
                option.timeoutMilliseconds = 500;
            }
            return settings;
        }
    };

    void Storage()
    {
        Fixture fixture;
        auto path = fixture.directory / "store";
        {
            FileStore store(path, 4096, 4);
            store.Put("checkpoint", {1, 2, 3});
            Throws([&] { FileStore duplicate(path); });
            Throws([&] { store.Put("oversized", Buffer(4097)); });
            store.Put("checkpoint", {4, 5});
        }
        {
            std::ofstream abandoned(path / "incomplete.rec.tmp", std::ios::binary);
            abandoned << "partial";
        }
        {
            FileStore store(path, 4096, 4);
            EMPY_CHECK(store.Get("checkpoint") == std::optional(Buffer{4, 5}));
        }
        for (const auto& file : std::filesystem::directory_iterator(path))
            if (file.path().extension() == ".rec")
            {
                std::fstream corrupt(file.path(), std::ios::in | std::ios::out | std::ios::binary);
                corrupt.seekp(7);
                corrupt.put('\x7F');
            }
        Throws([&] { FileStore corrupt(path, 4096, 4); });
        ForwardRecord record;
        record.session = "durable-origin";
        record.sequence = 1;
        record.acceptedEpoch = record.routingEpoch = 1;
        record.expires = UnixMilliseconds() + 60000;
        record.publication.topic = "durable/descriptor";
        record.publication.alias = 1;
        record.publication.qos = 1;
        record.publication.packetId = 1;
        record.publication.payload = {1, 2, 3};
        record.identity = MessageIdentity(record.session, record.sequence, record.publication.topic, record.acceptedEpoch, record.publication.payload);
        RetainedDescriptor descriptor;
        {
            FileDurabilityStore store(fixture.directory / "descriptors");
            descriptor = store.Append(record);
            EMPY_CHECK(store.Append(record).identity == descriptor.identity);
        }
        {
            FileDurabilityStore store(fixture.directory / "descriptors");
            EMPY_CHECK(store.Read(descriptor) == Codec::Encode(record.publication, {true, UINT32_MAX}));
            auto forged = descriptor;
            forged.payloadHash[0] ^= 1;
            Throws([&] { store.Read(forged); });
            store.Compact(record.expires);
            Throws([&] { store.Read(descriptor); });
        }
    }

    void SnapshotSchemas()
    {
        Fixture fixture;
        BrokerOptions settings;
        settings.port = 0;
        settings.stateDirectory = (fixture.directory / "legacy-broker").string();
        // A complete, empty schema-3 snapshot must fail before loading any state.
        Buffer snapshot{'E', 'B', 'R', '3', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        {
            FileStore store(settings.stateDirectory);
            store.Put("broker", snapshot);
        }
        Throws([&] { Broker legacy(settings); });
        {
            FileStore store(settings.stateDirectory);
            EMPY_CHECK(store.Get("broker") == std::optional(snapshot));
        }

        auto options = fixture.Options(false).front();
        Writer writer;
        writer.U32(3);
        writer.U64(options.fabric.epoch);
        writer.U32(options.fabric.shards);
        writer.U16(options.fabric.replicas);
        writer.U16(options.fabric.fanout);
        for (const auto& peer : options.peers)
        {
            writer.String(peer.identifier);
            writer.String(peer.fingerprint);
        }
        auto digest = Sha256(writer.Data());
        Buffer manifest(digest.begin(), digest.end());
        { ClusterNode current(options); }
        {
            FileStore store(options.directory);
            EMPY_CHECK(store.Get("manifest") != std::optional(manifest));
            store.Put("manifest", manifest);
        }
        Throws([&] { ClusterNode legacy(options); });
        {
            FileStore store(options.directory);
            EMPY_CHECK(store.Get("manifest") == std::optional(manifest));
        }
    }

    void PayloadState()
    {
        Fixture fixture;
        auto sender = (fixture.directory / "cipher-sender").string();
        auto receiver = (fixture.directory / "cipher-receiver").string();
        Buffer root(16, 0x63);
        PublishRequest first;
        first.topic = "durable/encrypted";
        first.payload = {'S'};
        {
            PayloadCipher cipher(KeyGrant{root}, 1, 2, 2, 8, sender);
            Throws([&] { PayloadCipher duplicate(KeyGrant{root}, 1, 2, 2, 8, sender); });
            cipher.Encrypt(first, 1);
        }
        {
            PayloadCipher cipher(KeyGrant{root}, 1, 2, 2, 8, receiver);
            EMPY_CHECK(cipher.Decrypt(first, first.topic) == Buffer{'S'});
        }
        PublishRequest second;
        second.topic = first.topic;
        second.payload = {'T'};
        {
            PayloadCipher cipher(KeyGrant{root}, 1, 2, 2, 8, sender);
            cipher.Encrypt(second, 1);
            EMPY_CHECK(first.encryption->nonce != second.encryption->nonce);
            EMPY_CHECK(std::equal(first.encryption->nonce.begin(), first.encryption->nonce.begin() + 8,
                second.encryption->nonce.begin()));
        }
        {
            PayloadCipher cipher(KeyGrant{root}, 1, 2, 2, 8, receiver);
            Throws([&] { cipher.Decrypt(first, first.topic); });
            auto altered = second;
            altered.encryption->tag[0] ^= 1;
            Throws([&] { cipher.Decrypt(altered, altered.topic); });
            EMPY_CHECK(cipher.Decrypt(second, second.topic) == Buffer{'T'});
            cipher.Rotate(2, 3);
        }
        Throws([&] { PayloadCipher stale(KeyGrant{root}, 1, 2, 2, 8, receiver); });
        Throws([&] { PayloadCipher wrong(KeyGrant{Buffer(16, 0x64)}, 2, 3, 2, 8, receiver); });
        PayloadCipher restored(KeyGrant{root}, 2, 3, 2, 8, receiver);
        Throws([&] { restored.Decrypt(second, second.topic); });
    }

    void Standalone()
    {
        Fixture fixture;
        BrokerOptions options;
        options.port = 0;
        options.stateDirectory = (fixture.directory / "broker").string();
        options.maxInflight = 2;
        options.retryInterval = 100;
        auto broker = std::make_unique<Broker>(options);
        auto port = broker->Listen();
        Client publisher, subscriber;
        publisher.Connect("127.0.0.1", port);
        auto sid = subscriber.Connect("127.0.0.1", port);
        Okay(publisher.Register("durable/value", 1, 60, 8));
        for (uint8_t i = 0; i < 8; ++i) Okay(publisher.Publish("durable/value", {i}, 1));
        std::vector<uint8_t> values;
        subscriber.On<Receive>("durable/#", [&] (const auto& event) { values.push_back(event.message.payload.at(0)); });
        Okay(subscriber.Subscribe("durable/#"));
        Await([&] { return values.size() == 8; }, {&subscriber});
        for (size_t i = 0; i < values.size(); ++i) EMPY_CHECK(values[i] == i);
        subscriber.Disconnect();
        publisher.Disconnect();
        broker.reset();
        broker = std::make_unique<Broker>(options);
        port = broker->Listen();
        EMPY_CHECK(subscriber.Connect("127.0.0.1", port, true) == sid);
        Client late;
        size_t replayed = 0;
        late.On<Receive>("durable/#", [&] (const auto&) { ++replayed; });
        late.Connect("127.0.0.1", port);
        Okay(late.Subscribe("durable/#"));
        Await([&] { return replayed == 8; }, {&subscriber, &late});
        late.Disconnect();
        subscriber.Disconnect();
    }

    void Tenants()
    {
        Fixture fixture;
        BrokerOptions options;
        options.port = 0;
        options.transport.tls = true;
        options.transport.certificate = fixture.certificates[0];
        options.transport.privateKey = fixture.keys[0];
        options.tenants = {{"alpha", "alpha", Buffer(32, 0xA1), {}}, {"beta", "beta", Buffer(32, 0xB2), {}}};
        Broker broker(options);
        auto port = broker.Listen();
        ClientOptions alpha;
        alpha.transport.tls = true;
        alpha.transport.authority = fixture.authority;
        alpha.transport.serverName = "localhost";
        alpha.authIdentity = "alpha";
        alpha.authKey = Buffer(32, 0xA1);
        auto beta = alpha;
        beta.authIdentity = "beta";
        beta.authKey = Buffer(32, 0xB2);
        Client a(alpha), b(beta);
        a.Connect("127.0.0.1", port);
        b.Connect("127.0.0.1", port);
        Okay(a.Register("alpha/value"));
        Okay(b.Subscribe("beta/#"));
        EMPY_CHECK(a.Subscribe("beta/#").get().code == MMQP_REASON_UNAUTHORIZED);
        EMPY_CHECK(b.Register("alpha/value").get().code == MMQP_REASON_UNAUTHORIZED);
        auto wrong = alpha;
        wrong.authIdentity = "beta";
        Client rejected(wrong);
        Throws([&] { rejected.Connect("127.0.0.1", port); });
        a.Disconnect();
        b.Disconnect();
    }

    void Quorum(bool quic)
    {
        Fixture fixture;
        auto options = fixture.Options(quic);
        std::array<std::unique_ptr<ClusterNode>, 3> nodes;
        for (size_t i = 0; i < nodes.size(); ++i) { nodes[i] = std::make_unique<ClusterNode>(options[i]); nodes[i]->Start(); }
        EMPY_CHECK(nodes[0]->Acquire("session-test").empty());
        nodes[0]->Commit("session-test", {'a'});
        Throws([&] { nodes[1]->Acquire("session-test", 200); });
        EMPY_CHECK(nodes[0]->Owns("session-test"));
        auto observation = nodes[0]->Inspect({"session-test", "missing"});
        EMPY_CHECK(observation.peers.size() == 2 && observation.activeLeases >= 1);
        EMPY_CHECK(observation.ownership.contains("session-test") && !observation.ownership.contains("missing"));
        EMPY_CHECK(observation.ownership.at("session-test").remainingMs > 0);
        EMPY_CHECK(nodes[2]->Read("session-test") == Buffer{'a'});
        nodes[0].reset();
        EMPY_CHECK(nodes[1]->Acquire("session-test", 6000) == Buffer{'a'});
        nodes[1]->Commit("session-test", {'b'});
        nodes[0] = std::make_unique<ClusterNode>(options[0]);
        nodes[0]->Start();
        EMPY_CHECK(nodes[0]->Read("session-test") == Buffer{'b'});
        nodes[0].reset();
        nodes[2].reset();
        Throws([&] { nodes[1]->Commit("session-test", {'c'}); });
        EMPY_CHECK(!nodes[1]->Owns("session-test"));
        EMPY_CHECK(!nodes[1]->Inspect({"session-test"}).ownership.contains("session-test"));
    }

    void Brokers(bool quic)
    {
        Fixture fixture;
        auto settings = fixture.Options(quic);
        std::array<BrokerOptions, 3> options;
        std::array<std::unique_ptr<Broker>, 3> brokers;
        std::array<uint16_t, 3> ports{};
        for (size_t i = 0; i < 3; ++i)
        {
            options[i].port = 0;
            options[i].cluster = settings[i];
            options[i].retryInterval = 100;
            options[i].requestTimeout = 15000;
            brokers[i] = std::make_unique<Broker>(options[i]);
            ports[i] = brokers[i]->Listen();
        }
        Client publisher;
        ClientOptions client;
        client.autoReconnect = true;
        client.failoverEndpoints = {{"127.0.0.1", ports[1]}, {"127.0.0.1", ports[2]}};
        Client subscriber(client);
        std::vector<Buffer> received;
        subscriber.On<Receive>("cluster/#", [&] (const auto& event) { received.push_back(event.message.payload); });
        auto sid = subscriber.Connect("127.0.0.1", ports[0]);
        publisher.Connect("127.0.0.1", ports[2]);
        Okay(subscriber.Subscribe("cluster/#"));
        Okay(publisher.Register("cluster/value", 1, 60, 4));
        Okay(publisher.Publish("cluster/value", {'A'}, 1));
        Await([&] { return received.size() == 1; }, {&subscriber});
        EMPY_CHECK(received.back() == Buffer{'A'});
        auto telemetry = brokers[0]->Inspect();
        EMPY_CHECK(telemetry.cluster && telemetry.durable && telemetry.cluster->replicas == 3);
        EMPY_CHECK(telemetry.storedBytes && *telemetry.storedBytes > 0 && *telemetry.storedBytes <= telemetry.maxStateBytes);
        EMPY_CHECK(telemetry.sessions.size() == 1 && telemetry.sessions.front().owner && telemetry.sessions.front().lease);
        Client sharedA, sharedB, responder;
        size_t first = 0, second = 0, responses = 0;
        sharedA.On<Receive>("work/#", [&] (const auto&) { ++first; });
        sharedB.On<Receive>("work/#", [&] (const auto&) { ++second; });
        publisher.On<Receive>("rpc/cluster", [&] (const auto& event)
        { EMPY_CHECK(event.message.payload == Buffer{'R'}); ++responses; });
        sharedA.Connect("127.0.0.1", ports[0]);
        sharedB.Connect("127.0.0.1", ports[1]);
        responder.Connect("127.0.0.1", ports[1]);
        responder.On<Receive>("rpc/cluster", [&] (const auto& event)
        { Okay(responder.Respond(event.message, {'R'})); });
        Okay(sharedA.Subscribe("$share/workers/work/#"));
        Okay(sharedB.Subscribe("$share/workers/work/#"));
        Okay(publisher.Register("work/task"));
        for (int i = 0; i < 4; ++i) Okay(publisher.Publish("work/task", {'W'}, 1));
        Await([&] { return first + second == 4; }, {&sharedA, &sharedB});
        EMPY_CHECK(first == 2 && second == 2);
        Okay(responder.Subscribe("rpc/cluster", 1, true));
        Okay(publisher.Register("rpc/cluster", 0, 60, 1, true));
        for (uint8_t qos : {uint8_t{1}, uint8_t{0}})
        {
            auto before = responses;
            Okay(publisher.Request("rpc/cluster", {'?'}, 1, qos));
            Await([&] { return responses > before; }, {&publisher, &responder});
        }

        Client requester(client), movingResponder(client);
        auto requesterId = requester.Connect("127.0.0.1", ports[0]);
        auto responderId = movingResponder.Connect("127.0.0.1", ports[0]);
        size_t requesterDisconnects = 0, responderDisconnects = 0;
        requester.On<Disconnect>([&] (const auto&) { ++requesterDisconnects; });
        movingResponder.On<Disconnect>([&] (const auto&) { ++responderDisconnects; });
        std::vector<PublishRequest> fixedRequests, movingRequests, publisherReplies, requesterReplies;
        responder.On<Receive>("rpc/concurrent", [&] (const auto& event) { fixedRequests.push_back(event.message); });
        movingResponder.On<Receive>("rpc/concurrent", [&] (const auto& event) { movingRequests.push_back(event.message); });
        publisher.On<Receive>("rpc/concurrent", [&] (const auto& event) { publisherReplies.push_back(event.message); });
        requester.On<Receive>("rpc/concurrent", [&] (const auto& event) { requesterReplies.push_back(event.message); });
        Okay(responder.Subscribe("rpc/concurrent", 1, true));
        Okay(movingResponder.Subscribe("rpc/concurrent", 1, true));
        Okay(publisher.Register("rpc/concurrent", 0, 60, 1, true));
        Okay(requester.Register("rpc/concurrent", 0, 60, 1, true));
        for (uint16_t id : {uint16_t{350}, uint16_t{351}})
        {
            Okay(publisher.Request("rpc/concurrent", {'P', static_cast<uint8_t>(id - 350)}, 2, 1, id));
            Okay(requester.Request("rpc/concurrent", {'Q', static_cast<uint8_t>(id - 350)}, 2, 1, id));
        }
        Await([&] { return fixedRequests.size() == 4 && movingRequests.size() == 4; }, {&responder, &movingResponder});
        std::set<uint16_t> routes;
        for (const auto& request : fixedRequests)
        {
            EMPY_CHECK(routes.insert(request.correlationId).second);
            Okay(responder.Respond(request, request.payload));
        }
        Await([&] { return publisherReplies.size() == 2 && requesterReplies.size() == 2; }, {&publisher, &requester});
        sharedA.Disconnect();
        sharedB.Disconnect();
        brokers[0].reset();
        Await([&] { return requesterDisconnects && responderDisconnects && requester.Connected() && movingResponder.Connected(); },
            {&requester, &movingResponder, &subscriber}, 12000);
        EMPY_CHECK(requester.Identifier() == requesterId && movingResponder.Identifier() == responderId);
        EMPY_CHECK(responder.Respond(fixedRequests[0], {'X'}).get().code == MMQP_REASON_INVALID_CORRELATION);
        for (auto it = movingRequests.rbegin(); it != movingRequests.rend(); ++it) Okay(movingResponder.Respond(*it, it->payload));
        Await([&] { return publisherReplies.size() == 4 && requesterReplies.size() == 4; }, {&publisher, &requester});
        for (auto replies : {&publisherReplies, &requesterReplies})
        {
            std::map<uint16_t, size_t> counts;
            for (const auto& reply : *replies)
            {
                EMPY_CHECK(reply.payload[0] == (replies == &publisherReplies ? 'P' : 'Q'));
                EMPY_CHECK(reply.correlationId == 350 + reply.payload[1]);
                ++counts[reply.correlationId];
            }
            EMPY_CHECK(counts[350] == 2 && counts[351] == 2);
        }
        requester.Disconnect();
        movingResponder.Disconnect();
        responder.Disconnect();
        Await([&] { return subscriber.Connected(); }, {&subscriber}, 9000);
        EMPY_CHECK(subscriber.Identifier() == sid);
        Okay(publisher.Publish("cluster/value", {'B'}, 1));
        Await([&] { return received.size() == 2; }, {&subscriber});
        EMPY_CHECK(received.back() == Buffer{'B'});
        Client late;
        std::vector<Buffer> retained;
        late.On<Receive>("cluster/#", [&] (const auto& event) { retained.push_back(event.message.payload); });
        late.Connect("127.0.0.1", ports[1]);
        Okay(late.Subscribe("cluster/#"));
        Await([&] { return retained.size() == 2; }, {&subscriber, &late});
        EMPY_CHECK(retained[0] == Buffer{'A'} && retained[1] == Buffer{'B'});
        late.Disconnect();
        subscriber.Disconnect();
        publisher.Disconnect();
        for (auto& broker : brokers) broker.reset();
        for (size_t i = 0; i < 3; ++i)
        {
            brokers[i] = std::make_unique<Broker>(options[i]);
            ports[i] = brokers[i]->Listen();
        }
        Client restarted;
        size_t replay = 0;
        restarted.On<Receive>("cluster/#", [&] (const auto&) { ++replay; });
        restarted.Connect("127.0.0.1", ports[1]);
        Okay(restarted.Subscribe("cluster/#"));
        Await([&] { return replay == 2; }, {&restarted});
        restarted.Disconnect();
    }
}

void TestRecovery()
{
    Storage();
    SnapshotSchemas();
    PayloadState();
    Standalone();
    Tenants();
    Quorum(false);
    Brokers(false);
}

void TestFabricQuic()
{
    EMPY_CHECK(QcContext::Available());
    Quorum(true);
    Brokers(true);
}
