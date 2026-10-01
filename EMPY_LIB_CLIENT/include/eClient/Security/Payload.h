#pragma once
#include <eMQ/Common/Message.h>
#include <eMQ/Security/Crypto.h>

namespace eMQ
{
    struct PayloadReplay : ProtocolError
    {
        PayloadReplay(): ProtocolError("Replayed encrypted publication", MMQP_REASON_AUTH_FAILED) {}
    };

    struct EMPY_CLIENT_API Ascon
    {
        // NIST SP 800-232 Ascon-AEAD128; output is ciphertext followed by its tag.
        static Buffer Encrypt(std::span<const uint8_t, 16> key, std::span<const uint8_t, 16> nonce,
            std::span<const uint8_t> plaintext, std::span<const uint8_t> associated);
        static Buffer Decrypt(std::span<const uint8_t, 16> key, std::span<const uint8_t, 16> nonce,
            std::span<const uint8_t> ciphertext, std::span<const uint8_t> associated);
    };

    struct EMPY_CLIENT_API KeyGrant
    {
        Buffer key;
        std::string root;
        std::optional<uint32_t> epoch;

        explicit KeyGrant(Buffer secret, std::string topic = {}, std::optional<uint32_t> period = {});
        KeyGrant(KeyGrant&&) noexcept;
        KeyGrant& operator=(KeyGrant&&) noexcept;
        KeyGrant(const KeyGrant&) = delete;
        KeyGrant& operator=(const KeyGrant&) = delete;
        ~KeyGrant();
    };

    struct EMPY_CLIENT_API NonceSequence
    {
        NonceSequence();
        NonceSequence(std::array<uint8_t, 8> prefix, uint64_t counter);
        std::array<uint8_t, 16> Next();
        void Save(Writer& writer) const;
        void Load(Reader& reader);

    private:
        std::array<uint8_t, 8> m_Prefix{};
        uint64_t m_Counter = 0;
        bool m_Exhausted = false;
    };

    struct EMPY_CLIENT_API PayloadCipher
    {
        PayloadCipher(KeyGrant grant, uint32_t minimumEpoch, uint32_t maximumEpoch,
            size_t cacheLimit = 128, size_t replayLimit = 4096, std::string stateDirectory = {});
        ~PayloadCipher();
        PayloadCipher(const PayloadCipher&) = delete;
        PayloadCipher& operator=(const PayloadCipher&) = delete;

        Key128 Derive(std::string_view topic, uint32_t epoch);
        void Encrypt(PublishRequest& publication, uint32_t epoch);
        Buffer Decrypt(const PublishRequest& publication, std::string_view topic);
        void Rotate(uint32_t minimumEpoch, uint32_t maximumEpoch);

    private:
        struct Impl;
        std::unique_ptr<Impl> m_Impl;
    };
}
