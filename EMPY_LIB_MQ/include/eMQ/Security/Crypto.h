#pragma once
#include "../Common/Stream.h"

namespace eMQ
{
    using Digest = std::array<uint8_t, 32>;
    using Key128 = std::array<uint8_t, 16>;

    EMPY_MQ_API Buffer RandomBytes(size_t count);
    EMPY_MQ_API Digest Sha256(std::span<const uint8_t> bytes);
    EMPY_MQ_API Digest Hmac256(std::span<const uint8_t> key, std::span<const uint8_t> bytes);
    EMPY_MQ_API Buffer Hkdf256(std::span<const uint8_t> key, std::span<const uint8_t> salt,
        std::span<const uint8_t> info, size_t count);
    EMPY_MQ_API bool SecureEqual(std::span<const uint8_t> left, std::span<const uint8_t> right);
    EMPY_MQ_API void SecureErase(std::span<uint8_t> bytes) noexcept;
    EMPY_MQ_API std::string ToHex(std::span<const uint8_t> bytes);
    EMPY_MQ_API Buffer FromHex(std::string_view text);
    EMPY_MQ_API uint64_t Hash64(std::span<const uint8_t> bytes);
    EMPY_MQ_API std::array<uint8_t, 8> AuthenticationIdentity(std::string_view name);
}
