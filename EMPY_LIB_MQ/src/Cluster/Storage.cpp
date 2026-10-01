#include <eMQ/Cluster/Storage.h>
#include <fstream>
#ifdef _WIN32
    #include <windows.h>
#else
    #include <fcntl.h>
    #include <sys/file.h>
    #include <unistd.h>
#endif

namespace eMQ
{
    namespace
    {
        std::string RecordName(std::string_view key)
        {
            return ToHex(Sha256({reinterpret_cast<const uint8_t*>(key.data()), key.size()})) + ".rec";
        }

        void FlushReplace(const std::filesystem::path& target, std::span<const uint8_t> bytes)
        {
            auto temporary = target;
            temporary += ".tmp";
#ifdef _WIN32
            auto file = CreateFileW(temporary.c_str(), GENERIC_WRITE, 0, nullptr, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (file == INVALID_HANDLE_VALUE) throw std::runtime_error("Cannot create durable temporary record");
            size_t offset = 0;
            bool okay = true;
            while (offset < bytes.size())
            {
                DWORD written = 0;
                auto count = static_cast<DWORD>(std::min<size_t>(bytes.size() - offset, 1024 * 1024));
                if (!WriteFile(file, bytes.data() + offset, count, &written, nullptr) || !written) { okay = false; break; }
                offset += written;
            }
            okay = okay && FlushFileBuffers(file);
            CloseHandle(file);
            if (!okay || !MoveFileExW(temporary.c_str(), target.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH))
                throw std::runtime_error("Durable record flush or replacement failed");
#else
            int file = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
            if (file < 0) throw std::runtime_error("Cannot create durable temporary record");
            size_t offset = 0;
            while (offset < bytes.size())
            {
                auto count = ::write(file, bytes.data() + offset, bytes.size() - offset);
                if (count < 0 && errno == EINTR) continue;
                if (count <= 0) { ::close(file); throw std::runtime_error("Durable record write failed"); }
                offset += static_cast<size_t>(count);
            }
            auto status = ::fsync(file);
            ::close(file);
            if (status || ::rename(temporary.c_str(), target.c_str())) throw std::runtime_error("Durable replacement failed");
            int directory = ::open(target.parent_path().c_str(), O_RDONLY | O_DIRECTORY);
            if (directory < 0) throw std::runtime_error("Cannot open durable directory");
            status = ::fsync(directory);
            ::close(directory);
            if (status) throw std::runtime_error("Durable directory flush failed");
#endif
        }
    }

    struct FileStore::Impl
    {
        std::filesystem::path directory;
        std::map<std::string, Buffer> records;
        size_t bytes = 0;
        size_t maximum;
        size_t count;
#ifdef _WIN32
        HANDLE lock = INVALID_HANDLE_VALUE;
#else
        int lock = -1;
#endif
        Impl(std::filesystem::path path, size_t limit, size_t capacity):
            directory(std::move(path)), maximum(limit), count(capacity)
        {
            if (directory.empty() || !maximum || !count) throw std::invalid_argument("Invalid storage directory or budget");
            std::filesystem::create_directories(directory);
            auto name = directory / ".lock";
#ifdef _WIN32
            lock = CreateFileW(name.c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (lock == INVALID_HANDLE_VALUE) throw std::runtime_error("State directory already in use or inaccessible");
#else
            lock = ::open(name.c_str(), O_RDWR | O_CREAT, 0600);
            if (lock < 0) throw std::runtime_error("State directory inaccessible");
            if (::flock(lock, LOCK_EX | LOCK_NB)) { ::close(lock); lock = -1; throw std::runtime_error("State directory already in use"); }
#endif
        }

        ~Impl()
        {
#ifdef _WIN32
            if (lock != INVALID_HANDLE_VALUE) CloseHandle(lock);
#else
            if (lock >= 0) ::close(lock);
#endif
        }
    };

    FileStore::FileStore(std::filesystem::path path, size_t maxBytes, size_t maxRecords):
        m_Impl(std::make_unique<Impl>(std::move(path), maxBytes, maxRecords))
    {
        for (const auto& entry : std::filesystem::directory_iterator(m_Impl->directory))
        {
            if (entry.path().extension() != ".rec") continue;
            if (!entry.is_regular_file() || entry.is_symlink()) throw std::runtime_error("Invalid storage entry");
            auto size = entry.file_size();
            if (size < 41 || size > maxBytes + 300 || m_Impl->records.size() >= maxRecords)
                throw std::runtime_error("Stored record exceeds bounds");
            Buffer bytes(static_cast<size_t>(size));
            std::ifstream file(entry.path(), std::ios::binary);
            if (!file.read(reinterpret_cast<char*>(bytes.data()), bytes.size())) throw std::runtime_error("Cannot read durable record");
            auto body = std::span<const uint8_t>(bytes).first(bytes.size() - 32);
            if (!SecureEqual(Sha256(body), std::span<const uint8_t>(bytes).last(32))) throw std::runtime_error("Durable record checksum failed");
            Reader reader(body);
            if (reader.U32() != 0x454D5331) throw std::runtime_error("Unknown storage record version");
            auto key = reader.String();
            auto value = ReadBuffer(reader);
            reader.Finish();
            if (key.empty() || entry.path().filename() != RecordName(key) || m_Impl->records.contains(key)
                || value.size() > maxBytes - m_Impl->bytes) throw std::runtime_error("Invalid stored key or storage budget");
            m_Impl->bytes += value.size();
            m_Impl->records.emplace(std::move(key), std::move(value));
        }
    }

    FileStore::~FileStore() = default;

    void FileStore::Put(std::string key, Buffer value)
    {
        if (key.empty() || key.size() > 255 || !ValidUtf8(key)) throw std::invalid_argument("Invalid storage key");
        auto previous = m_Impl->records.find(key);
        auto old = previous == m_Impl->records.end() ? 0 : previous->second.size();
        if ((previous == m_Impl->records.end() && m_Impl->records.size() >= m_Impl->count)
            || value.size() > m_Impl->maximum - (m_Impl->bytes - old)) throw ProtocolError("Storage budget exhausted", MMQP_REASON_BUSY);
        if (previous != m_Impl->records.end() && previous->second == value) return;
        Writer writer(m_Impl->maximum + 300);
        writer.U32(0x454D5331);
        writer.String(key);
        WriteBuffer(writer, value);
        auto digest = Sha256(writer.Data());
        writer.Bytes(digest);
        FlushReplace(m_Impl->directory / RecordName(key), writer.Data());
        m_Impl->bytes = m_Impl->bytes - old + value.size();
        m_Impl->records[std::move(key)] = std::move(value);
    }

    std::optional<Buffer> FileStore::Get(std::string_view key) const
    {
        auto found = m_Impl->records.find(std::string(key));
        return found == m_Impl->records.end() ? std::nullopt : std::optional(found->second);
    }

    void FileStore::Erase(std::string_view key)
    {
        auto found = m_Impl->records.find(std::string(key));
        if (found == m_Impl->records.end()) return;
        std::filesystem::remove(m_Impl->directory / RecordName(key));
        m_Impl->bytes -= found->second.size();
        m_Impl->records.erase(found);
    }

    std::vector<std::string> FileStore::Keys(std::string_view prefix) const
    {
        std::vector<std::string> keys;
        for (const auto& [key, value] : m_Impl->records) if (key.starts_with(prefix)) keys.push_back(key);
        return keys;
    }

    size_t FileStore::Bytes() const { return m_Impl->bytes; }

    FileDurabilityStore::FileDurabilityStore(std::filesystem::path path, size_t limit): m_Store(std::move(path), limit) {}

    RetainedDescriptor FileDurabilityStore::Append(const ForwardRecord& record)
    {
        auto identity = MessageIdentity(record.session, record.sequence, record.publication.topic, record.acceptedEpoch, record.publication.payload);
        if (identity != record.identity || record.expires <= UnixMilliseconds()) throw ProtocolError("Invalid durable publication");
        auto payload = Codec::Encode(record.publication, {true, UINT32_MAX});
        auto location = ToHex(identity);
        Writer writer(payload.size() + 16);
        writer.U64(record.expires);
        WriteBuffer(writer, payload);
        if (auto previous = m_Store.Get(location); previous && *previous != writer.Data())
            throw ProtocolError("Durability identity conflicts with existing content");
        m_Store.Put(location, writer.Take());
        return {identity, record.publication.topic, location, record.expires, 0, Sha256(payload)};
    }

    Buffer FileDurabilityStore::Read(const RetainedDescriptor& descriptor)
    {
        if (descriptor.location != ToHex(descriptor.identity)) throw ProtocolError("Invalid durable location");
        auto record = m_Store.Get(descriptor.location);
        if (!record) throw ProtocolError("Durable publication is unavailable");
        Reader reader(*record);
        auto expiry = reader.U64();
        auto payload = ReadBuffer(reader);
        reader.Finish();
        if (expiry <= UnixMilliseconds() || expiry != descriptor.expiry || Sha256(payload) != descriptor.payloadHash)
            throw ProtocolError("Durable publication expired or descriptor differs");
        return payload;
    }

    void FileDurabilityStore::Compact(uint64_t now)
    {
        for (const auto& key : m_Store.Keys())
        {
            auto value = m_Store.Get(key);
            Reader reader(*value);
            if (reader.U64() <= now) m_Store.Erase(key);
        }
    }
}
