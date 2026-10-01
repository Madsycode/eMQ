#include <eMQ/Security/Crypto.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/kdf.h>
#include <openssl/rand.h>

namespace eMQ
{
    std::array<uint8_t, 8> AuthenticationIdentity(std::string_view name)
    {
        if (name.empty() || name.size() > 128) throw std::invalid_argument("Authentication identity must contain 1..128 bytes");
        auto context = "MMQP/AUTH/" + std::string(name);
        auto digest = Sha256({reinterpret_cast<const uint8_t*>(context.data()), context.size()});
        std::array<uint8_t, 8> identity{};
        std::copy_n(digest.begin(), identity.size(), identity.begin());
        return identity;
    }

    Buffer RandomBytes(size_t count)
    {
        if (count > INT_MAX) throw std::length_error("Random byte request is too large");
        Buffer bytes(count);
        if (count && RAND_bytes(bytes.data(), static_cast<int>(count)) != 1)
            throw std::runtime_error("Operating-system random generator failed");
        return bytes;
    }

    Digest Sha256(std::span<const uint8_t> bytes)
    {
        Digest digest{};
        size_t length = digest.size();
        if (EVP_Q_digest(nullptr, "SHA256", nullptr, bytes.data(), bytes.size(), digest.data(), &length) != 1
            || length != digest.size())
            throw std::runtime_error("SHA-256 provider failed");
        return digest;
    }

    Digest Hmac256(std::span<const uint8_t> key, std::span<const uint8_t> bytes)
    {
        Digest digest{};
        size_t length = digest.size();
        if (!EVP_Q_mac(nullptr, "HMAC", nullptr, "SHA256", nullptr, key.data(), key.size(),
            bytes.data(), bytes.size(), digest.data(), digest.size(), &length) || length != digest.size())
            throw std::runtime_error("HMAC-SHA256 provider failed");
        return digest;
    }

    Buffer Hkdf256(std::span<const uint8_t> key, std::span<const uint8_t> salt,
        std::span<const uint8_t> info, size_t count)
    {
        if (!count || count > 255 * 32 || key.size() > INT_MAX || salt.size() > INT_MAX || info.size() > INT_MAX)
            throw std::invalid_argument("Invalid HKDF bounds");
        std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> context(
            EVP_PKEY_CTX_new_id(EVP_PKEY_HKDF, nullptr), EVP_PKEY_CTX_free);
        Buffer output(count);
        auto length = count;
        if (!context || EVP_PKEY_derive_init(context.get()) <= 0
            || EVP_PKEY_CTX_set_hkdf_md(context.get(), EVP_sha256()) <= 0
            || EVP_PKEY_CTX_set1_hkdf_salt(context.get(), salt.data(), static_cast<int>(salt.size())) <= 0
            || EVP_PKEY_CTX_set1_hkdf_key(context.get(), key.data(), static_cast<int>(key.size())) <= 0
            || EVP_PKEY_CTX_add1_hkdf_info(context.get(), info.data(), static_cast<int>(info.size())) <= 0
            || EVP_PKEY_derive(context.get(), output.data(), &length) <= 0 || length != count)
        {
            SecureErase(output);
            throw std::runtime_error("HKDF-SHA256 provider failed");
        }
        return output;
    }

    bool SecureEqual(std::span<const uint8_t> left, std::span<const uint8_t> right)
    {
        return left.size() == right.size() && (left.empty()
            || CRYPTO_memcmp(left.data(), right.data(), left.size()) == 0);
    }

    void SecureErase(std::span<uint8_t> bytes) noexcept
    {
        if (!bytes.empty()) OPENSSL_cleanse(bytes.data(), bytes.size());
    }

    std::string ToHex(std::span<const uint8_t> bytes)
    {
        constexpr char digits[] = "0123456789abcdef";
        std::string text;
        text.reserve(bytes.size() * 2);
        for (auto byte : bytes)
        {
            text.push_back(digits[byte >> 4]);
            text.push_back(digits[byte & 0x0F]);
        }
        return text;
    }

    Buffer FromHex(std::string_view text)
    {
        if (text.size() % 2) throw std::invalid_argument("Hexadecimal data has an odd length");
        auto digit = [] (char value) -> uint8_t
        {
            if (value >= '0' && value <= '9') return static_cast<uint8_t>(value - '0');
            if (value >= 'a' && value <= 'f') return static_cast<uint8_t>(value - 'a' + 10);
            if (value >= 'A' && value <= 'F') return static_cast<uint8_t>(value - 'A' + 10);
            throw std::invalid_argument("Invalid hexadecimal digit");
        };
        Buffer bytes;
        bytes.reserve(text.size() / 2);
        for (size_t i = 0; i < text.size(); i += 2)
            bytes.push_back(static_cast<uint8_t>((digit(text[i]) << 4) | digit(text[i + 1])));
        return bytes;
    }

    uint64_t Hash64(std::span<const uint8_t> bytes)
    {
        auto digest = Sha256(bytes);
        Reader reader(digest);
        return reader.U64();
    }
}
