#include "Checks.h"
#include <eClient/Security/Payload.h>
#include <fstream>
#include <map>

using namespace eMQ;

void TestSecurity()
{
    auto ikm = FromHex("0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b");
    EMPY_CHECK(ToHex(Hkdf256(ikm, FromHex("000102030405060708090a0b0c"),
        FromHex("f0f1f2f3f4f5f6f7f8f9"), 42)) ==
        "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865");
    EMPY_CHECK(ToHex(Sha256(Buffer{'a', 'b', 'c'})) ==
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    EMPY_CHECK(!SecureEqual(Buffer{1, 2}, Buffer{1, 3}));
    Throws([] { FromHex("0x"); });

    std::ifstream vectors(std::string(EMPY_TEST_SOURCE_DIR) + "/Vectors/Ascon-AEAD128.txt");
    EMPY_CHECK(vectors.good());
    std::map<std::string, std::string> fields;
    size_t checked = 0;
    auto verify = [&]
    {
        if (fields.empty()) return;
        auto secret = FromHex(fields.at("Key"));
        auto publicNonce = FromHex(fields.at("Nonce"));
        auto plaintext = FromHex(fields.at("PT"));
        auto associated = FromHex(fields.at("AD"));
        auto expected = FromHex(fields.at("CT"));
        EMPY_CHECK(secret.size() == 16 && publicNonce.size() == 16);
        std::span<const uint8_t, 16> key(secret.data(), 16);
        std::span<const uint8_t, 16> nonce(publicNonce.data(), 16);
        EMPY_CHECK(Ascon::Encrypt(key, nonce, plaintext, associated) == expected);
        EMPY_CHECK(Ascon::Decrypt(key, nonce, expected, associated) == plaintext);
        expected.back() ^= 1;
        Throws([&] { Ascon::Decrypt(key, nonce, expected, associated); });
        ++checked;
        fields.clear();
    };
    std::string line;
    while (std::getline(vectors, line))
    {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty()) { verify(); continue; }
        auto separator = line.find(" = ");
        if (separator != std::string::npos) fields[line.substr(0, separator)] = line.substr(separator + 3);
    }
    verify();
    EMPY_CHECK(checked == 1089);

    auto root = FromHex("000102030405060708090a0b0c0d0e0f");
    PayloadCipher publisher(KeyGrant{root}, 1, 3, 2);
    PayloadCipher subscriber(KeyGrant{root}, 1, 3, 2);
    auto branch = publisher.Derive("home/room", 2);
    PayloadCipher delegated(KeyGrant{Buffer(branch.begin(), branch.end()), "home/room", 2}, 1, 3);
    EMPY_CHECK(delegated.Derive("home/room/temp", 2) == publisher.Derive("home/room/temp", 2));
    EMPY_CHECK(publisher.Derive("home/room/temp", 1) != publisher.Derive("home/room/temp", 2));
    Throws([&] { delegated.Derive("home/other/temp", 2); });
    Throws([&] { delegated.Derive("home/room/temp", 3); });
    PublishRequest message;
    message.topic = "//home//room/temp/";
    message.payload = Buffer{'s', 'e', 'c', 'r', 'e', 't'};
    auto plaintext = message.payload;
    publisher.Encrypt(message, 2);
    EMPY_CHECK(message.payload != plaintext && message.topic == "home/room/temp");
    auto modified = message;
    modified.encryption->tag[0] ^= 1;
    Throws([&] { subscriber.Decrypt(modified, message.topic); });
    Throws([&] { subscriber.Decrypt(message, "home/room/other"); });
    modified = message;
    modified.encryption->epoch = 3;
    Throws([&] { subscriber.Decrypt(modified, message.topic); });
    EMPY_CHECK(subscriber.Decrypt(message, message.topic) == plaintext);
    Throws([&] { subscriber.Decrypt(message, message.topic); });
    EMPY_CHECK(delegated.Decrypt(message, message.topic) == plaintext);
    subscriber.Rotate(3, 4);
    Throws([&] { subscriber.Decrypt(message, message.topic); });
    NonceSequence nonces({}, UINT64_MAX);
    EMPY_CHECK(nonces.Next()[15] == 0xFF);
    Throws([&] { nonces.Next(); });
}
