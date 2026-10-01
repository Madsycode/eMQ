#include <eClient/Security/Payload.h>
#include <eMQ/Cluster/Storage.h>
#include <deque>
#include <map>
#include <mutex>
#include <tuple>
#include <bitset>

extern "C"
{
    #include "crypto_aead.h"
}

namespace eMQ
{
    namespace
    {
        const uint8_t* Data(std::span<const uint8_t> data)
        {
            static constexpr uint8_t empty = 0;
            return data.empty() ? &empty : data.data();
        }

        Buffer Associated(std::string_view topic, uint32_t epoch)
        {
            Writer writer(259);
            writer.Bytes({reinterpret_cast<const uint8_t*>(topic.data()), topic.size()});
            writer.U32(epoch);
            return writer.Take();
        }

        struct EraseKey
        {
            Key128& key;
            ~EraseKey() { SecureErase(key); }
        };
    }

    Buffer Ascon::Encrypt(std::span<const uint8_t, 16> key, std::span<const uint8_t, 16> nonce,
        std::span<const uint8_t> plaintext, std::span<const uint8_t> associated)
    {
        if (plaintext.size() > SIZE_MAX - 16) throw std::length_error("Plaintext too large");
        Buffer encrypted(plaintext.size() + 16);
        unsigned long long size = encrypted.size();
        if (crypto_aead_encrypt(encrypted.data(), &size, Data(plaintext), plaintext.size(),
            Data(associated), associated.size(), nullptr, nonce.data(), key.data()) != 0 || size != encrypted.size())
            throw std::runtime_error("Ascon encryption failed");
        return encrypted;
    }

    Buffer Ascon::Decrypt(std::span<const uint8_t, 16> key, std::span<const uint8_t, 16> nonce,
        std::span<const uint8_t> ciphertext, std::span<const uint8_t> associated)
    {
        if (ciphertext.size() < 16)
            throw ProtocolError("Missing authentication tag", MMQP_REASON_AUTH_FAILED);
        Buffer plaintext(std::max<size_t>(1, ciphertext.size() - 16));
        unsigned long long size = 0;
        if (crypto_aead_decrypt(plaintext.data(), &size, nullptr, ciphertext.data(), ciphertext.size(),
            Data(associated), associated.size(), nonce.data(), key.data()) != 0 || size != ciphertext.size() - 16)
        {
            SecureErase(plaintext);
            throw ProtocolError("Payload authentication failed", MMQP_REASON_AUTH_FAILED);
        }
        plaintext.resize(static_cast<size_t>(size));
        return plaintext;
    }

    KeyGrant::KeyGrant(Buffer secret, std::string topic, std::optional<uint32_t> period):
        key(std::move(secret)), root(std::move(topic)), epoch(period)
    {
        if ((key.size() != 16 && key.size() != 32) || (!root.empty() && (!epoch || key.size() != 16)))
        {
            SecureErase(key);
            throw std::invalid_argument("Root keys require 16/32 bytes; subtree keys require 16 bytes and an epoch");
        }
        try { if (!root.empty()) root = ParseTopic(root).filter; }
        catch (...) { SecureErase(key); throw; }
    }

    KeyGrant::KeyGrant(KeyGrant&&) noexcept = default;
    KeyGrant& KeyGrant::operator=(KeyGrant&& other) noexcept
    {
        if (this != &other)
        {
            SecureErase(key);
            key = std::move(other.key);
            root = std::move(other.root);
            epoch = other.epoch;
        }
        return *this;
    }
    KeyGrant::~KeyGrant() { SecureErase(key); }

    NonceSequence::NonceSequence()
    {
        auto random = RandomBytes(m_Prefix.size());
        std::copy(random.begin(), random.end(), m_Prefix.begin());
    }

    NonceSequence::NonceSequence(std::array<uint8_t, 8> prefix, uint64_t counter):
        m_Prefix(prefix), m_Counter(counter) {}

    std::array<uint8_t, 16> NonceSequence::Next()
    {
        if (m_Exhausted) throw std::overflow_error("Nonce counter exhausted");
        std::array<uint8_t, 16> nonce{};
        std::copy(m_Prefix.begin(), m_Prefix.end(), nonce.begin());
        for (size_t i = 0; i < 8; ++i)
            nonce[15 - i] = static_cast<uint8_t>(m_Counter >> (i * 8));
        if (m_Counter == UINT64_MAX) m_Exhausted = true;
        else ++m_Counter;
        return nonce;
    }

    void NonceSequence::Save(Writer& writer) const
    {
        writer.Bytes(m_Prefix);
        writer.U64(m_Counter);
        writer.U8(m_Exhausted);
    }

    void NonceSequence::Load(Reader& reader)
    {
        auto prefix = reader.Bytes(m_Prefix.size());
        std::copy(prefix.begin(), prefix.end(), m_Prefix.begin());
        m_Counter = reader.U64();
        auto exhausted = reader.U8();
        if (exhausted > 1 || (exhausted && m_Counter != UINT64_MAX))
            throw ProtocolError("Invalid persistent nonce sequence");
        m_Exhausted = exhausted != 0;
    }

    struct PayloadCipher::Impl
    {
        using CacheKey = std::pair<std::string, uint32_t>;
        using ReplayKey = std::tuple<std::string, uint32_t, uint64_t>;
        struct ReplayState { uint64_t highest = 0; std::bitset<256> bits; };

        KeyGrant grant;
        uint32_t minimum;
        uint32_t maximum;
        size_t cacheLimit;
        size_t replayLimit;
        std::map<CacheKey, Key128> cache;
        std::deque<CacheKey> order;
        std::map<ReplayKey, ReplayState> replays;
        NonceSequence nonces;
        std::mutex mutex;
        std::unique_ptr<FileStore> storage;
        Digest identity{};
        bool failed = false;

        Impl(KeyGrant secret, uint32_t first, uint32_t last, size_t keys, size_t replay, const std::string& directory):
            grant(std::move(secret)), minimum(first), maximum(last), cacheLimit(keys), replayLimit(replay)
        {
            if (first > last || !keys || !replay || replay > UINT32_MAX || replay > (SIZE_MAX - 1024) / 320)
                throw std::invalid_argument("Invalid epoch or cryptographic cache bounds");
            if (!directory.empty())
            {
                Writer binding;
                binding.String("MMQP/PAYLOAD/STATE/1");
                binding.String(grant.root);
                binding.U8(grant.epoch.has_value());
                if (grant.epoch) binding.U32(*grant.epoch);
                identity = Hmac256(grant.key, binding.Data());
                storage = std::make_unique<FileStore>(directory, 1024 + replay * 320, 1);
                if (auto saved = storage->Get("payload")) this->Load(*saved);
                this->Save();
            }
        }

        ~Impl()
        {
            for (auto& [name, key] : cache) SecureErase(key);
        }

        void Check() const
        {
            if (failed) throw ProtocolError("Payload state storage failed; reopen the cipher before continuing", MMQP_REASON_BUSY);
        }

        void Save()
        {
            if (!storage) return;
            try
            {
                Writer writer(1024 + replayLimit * 320);
                writer.U32(0x45504331);
                writer.Bytes(identity);
                writer.U32(minimum);
                writer.U32(maximum);
                nonces.Save(writer);
                writer.U32(static_cast<uint32_t>(replays.size()));
                for (const auto& [key, state] : replays)
                {
                    writer.String(std::get<0>(key));
                    writer.U32(std::get<1>(key));
                    writer.U64(std::get<2>(key));
                    writer.U64(state.highest);
                    for (size_t i = 0; i < state.bits.size(); i += 8)
                    {
                        uint8_t bits = 0;
                        for (size_t j = 0; j < 8; ++j) if (state.bits[i + j]) bits |= uint8_t(1U << j);
                        writer.U8(bits);
                    }
                }
                storage->Put("payload", writer.Take());
            }
            catch (...) { failed = true; throw; }
        }

        void Load(const Buffer& bytes)
        {
            Reader reader(bytes);
            if (reader.U32() != 0x45504331 || !SecureEqual(reader.Bytes(32), identity))
                throw ProtocolError("Payload state belongs to a different key grant or format");
            auto first = reader.U32();
            auto last = reader.U32();
            if (first > last || minimum < first || maximum < last)
                throw ProtocolError("Persistent payload epoch policy cannot move backwards");
            nonces.Load(reader);
            auto count = reader.U32();
            if (count > replayLimit) throw ProtocolError("Persistent payload replay budget exceeded");
            for (uint32_t i = 0; i < count; ++i)
            {
                auto topic = ParseTopic(reader.String()).filter;
                auto epoch = reader.U32();
                auto prefix = reader.U64();
                ReplayState state;
                state.highest = reader.U64();
                for (size_t j = 0; j < state.bits.size(); j += 8)
                {
                    auto bits = reader.U8();
                    for (size_t k = 0; k < 8; ++k) state.bits[j + k] = (bits & (1U << k)) != 0;
                }
                if (!state.bits[0] || epoch < first || epoch > last) throw ProtocolError("Invalid persistent payload replay window");
                if (epoch >= minimum && epoch <= maximum
                    && !replays.emplace(ReplayKey{topic, epoch, prefix}, state).second)
                    throw ProtocolError("Duplicate persistent replay entry");
            }
            reader.Finish();
        }

        Key128 Derive(const std::string& topic, uint32_t epoch)
        {
            if (epoch < minimum || epoch > maximum || (grant.epoch && *grant.epoch != epoch))
                throw ProtocolError("Key epoch is not authorized", MMQP_REASON_UNAUTHORIZED);
            auto name = CacheKey{topic, epoch};
            if (auto it = cache.find(name); it != cache.end()) return it->second;
            size_t start = 0;
            if (!grant.root.empty())
            {
                if (topic != grant.root && !topic.starts_with(grant.root + '/'))
                    throw ProtocolError("Topic is outside the delegated key subtree", MMQP_REASON_UNAUTHORIZED);
                start = grant.root.size() + 1;
            }
            Buffer secret = grant.key;
            try
            {
                Writer salt(4);
                salt.U32(epoch);
                while (start < topic.size())
                {
                    auto end = topic.find('/', start);
                    if (end == std::string::npos) end = topic.size();
                    auto level = std::string_view(topic).substr(start, end - start);
                    auto derived = Hkdf256(secret, salt.Data(),
                        {reinterpret_cast<const uint8_t*>(level.data()), level.size()}, 16);
                    SecureErase(secret);
                    secret = std::move(derived);
                    start = end + 1;
                }
                Key128 key{};
                EraseKey erase{key};
                std::copy_n(secret.begin(), key.size(), key.begin());
                SecureErase(secret);
                if (cache.size() >= cacheLimit)
                {
                    auto oldest = cache.find(order.front());
                    SecureErase(oldest->second);
                    cache.erase(oldest);
                    order.pop_front();
                }
                cache.emplace(name, key);
                order.emplace_back(std::move(name));
                return Key128(key);
            }
            catch (...)
            {
                SecureErase(secret);
                throw;
            }
        }

        void Replay(const std::string& topic, const EncryptionHeader& encryption)
        {
            Reader reader(encryption.nonce);
            auto prefix = reader.U64();
            auto counter = reader.U64();
            ReplayKey key{topic, encryption.epoch, prefix};
            auto found = replays.find(key);
            if (found == replays.end())
            {
                if (replays.size() >= replayLimit)
                    throw ProtocolError("Replay state budget exhausted", MMQP_REASON_POLICY_VIOLATION);
                replays.emplace(std::move(key), ReplayState{counter, std::bitset<256>{1}});
                return;
            }
            auto& state = found->second;
            if (counter > state.highest)
            {
                auto shift = counter - state.highest;
                if (shift >= state.bits.size()) state.bits.reset();
                else state.bits <<= static_cast<size_t>(shift);
                state.bits.set(0);
                state.highest = counter;
            }
            else
            {
                auto shift = state.highest - counter;
                if (shift >= state.bits.size())
                    throw ProtocolError("Encrypted nonce is outside the replay window", MMQP_REASON_POLICY_VIOLATION);
                if (state.bits[static_cast<size_t>(shift)]) throw PayloadReplay();
                state.bits.set(static_cast<size_t>(shift));
            }
        }
    };

    PayloadCipher::PayloadCipher(KeyGrant grant, uint32_t minimumEpoch, uint32_t maximumEpoch,
        size_t cacheLimit, size_t replayLimit, std::string stateDirectory):
        m_Impl(std::make_unique<Impl>(std::move(grant), minimumEpoch, maximumEpoch, cacheLimit, replayLimit, stateDirectory)) {}

    PayloadCipher::~PayloadCipher() = default;

    Key128 PayloadCipher::Derive(std::string_view topic, uint32_t epoch)
    {
        std::scoped_lock lock(m_Impl->mutex);
        return m_Impl->Derive(ParseTopic(topic).filter, epoch);
    }

    void PayloadCipher::Encrypt(PublishRequest& publication, uint32_t epoch)
    {
        std::scoped_lock lock(m_Impl->mutex);
        m_Impl->Check();
        if (publication.encryption) throw std::invalid_argument("Publication is already encrypted");
        auto topic = ParseTopic(publication.topic).filter;
        auto key = m_Impl->Derive(topic, epoch);
        EraseKey erase{key};
        EncryptionHeader encryption;
        encryption.epoch = epoch;
        encryption.nonce = m_Impl->nonces.Next();
        m_Impl->Save(); // Reserve the nonce durably before releasing any ciphertext.
        auto bytes = Ascon::Encrypt(key, encryption.nonce, publication.payload, Associated(topic, epoch));
        std::copy(bytes.end() - 16, bytes.end(), encryption.tag.begin());
        bytes.resize(bytes.size() - 16);
        SecureErase(publication.payload);
        publication.payload = std::move(bytes);
        publication.topic = std::move(topic);
        publication.encryption = encryption;
    }

    Buffer PayloadCipher::Decrypt(const PublishRequest& publication, std::string_view topic)
    {
        std::scoped_lock lock(m_Impl->mutex);
        m_Impl->Check();
        if (!publication.encryption)
            throw ProtocolError("An encrypted payload is required", MMQP_REASON_AUTH_FAILED);
        auto name = ParseTopic(topic).filter;
        const auto& encryption = *publication.encryption;
        auto key = m_Impl->Derive(name, encryption.epoch);
        EraseKey erase{key};
        auto bytes = publication.payload;
        bytes.insert(bytes.end(), encryption.tag.begin(), encryption.tag.end());
        auto plaintext = Ascon::Decrypt(key, encryption.nonce, bytes, Associated(name, encryption.epoch));
        try { m_Impl->Replay(name, encryption); m_Impl->Save(); }
        catch (...) { SecureErase(plaintext); throw; }
        return plaintext;
    }

    void PayloadCipher::Rotate(uint32_t minimumEpoch, uint32_t maximumEpoch)
    {
        std::scoped_lock lock(m_Impl->mutex);
        m_Impl->Check();
        if (minimumEpoch > maximumEpoch || minimumEpoch < m_Impl->minimum || maximumEpoch < m_Impl->maximum)
            throw std::invalid_argument("Epoch policy cannot move backwards");
        m_Impl->minimum = minimumEpoch;
        m_Impl->maximum = maximumEpoch;
        for (auto& [name, key] : m_Impl->cache) SecureErase(key);
        m_Impl->cache.clear();
        m_Impl->order.clear();
        std::erase_if(m_Impl->replays, [&] (const auto& entry)
        {
            return std::get<1>(entry.first) < minimumEpoch || std::get<1>(entry.first) > maximumEpoch;
        });
        m_Impl->Save();
    }
}
