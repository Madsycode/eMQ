#pragma once
#include "Replication.h"
#include <filesystem>

namespace eMQ
{
    EMPY_INLINE void WriteBuffer(Writer& writer, std::span<const uint8_t> bytes)
    {
        if (bytes.size() > UINT32_MAX) throw ProtocolError("State record is too large");
        writer.U32(static_cast<uint32_t>(bytes.size()));
        writer.Bytes(bytes);
    }

    EMPY_INLINE Buffer ReadBuffer(Reader& reader)
    {
        auto bytes = reader.Bytes(reader.U32());
        return {bytes.begin(), bytes.end()};
    }

    EMPY_INLINE uint64_t UnixMilliseconds()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    EMPY_INLINE uint64_t SaveExpiry(TimePoint expiry)
    {
        if (expiry == TimePoint::max()) return UINT64_MAX;
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(expiry - Clock::now()).count();
        return UnixMilliseconds() + std::max<int64_t>(0, remaining);
    }

    EMPY_INLINE TimePoint LoadExpiry(uint64_t expiry)
    {
        if (expiry == UINT64_MAX) return TimePoint::max();
        auto now = UnixMilliseconds();
        return Clock::now() + std::chrono::milliseconds(expiry > now ? std::min<uint64_t>(expiry - now, 366ULL * 86400000) : 0);
    }

    // One owner, bounded checksummed records, atomic replacement and OS flush.
    // A corrupt committed record fails closed; abandoned temporary files are ignored.
    struct EMPY_MQ_API FileStore
    {
        explicit FileStore(std::filesystem::path directory, size_t maxBytes = 256 * 1024 * 1024,
            size_t maxRecords = 65536);
        ~FileStore();
        FileStore(const FileStore&) = delete;
        FileStore& operator=(const FileStore&) = delete;

        void Put(std::string key, Buffer value);
        std::optional<Buffer> Get(std::string_view key) const;
        void Erase(std::string_view key);
        std::vector<std::string> Keys(std::string_view prefix = {}) const;
        size_t Bytes() const;

    private:
        struct Impl;
        std::unique_ptr<Impl> m_Impl;
    };

    struct EMPY_MQ_API FileDurabilityStore : DurabilityStore
    {
        explicit FileDurabilityStore(std::filesystem::path directory, size_t maxBytes = 256 * 1024 * 1024);
        RetainedDescriptor Append(const ForwardRecord& record) override;
        Buffer Read(const RetainedDescriptor& descriptor) override;
        void Compact(uint64_t now);

    private:
        FileStore m_Store;
    };
}
