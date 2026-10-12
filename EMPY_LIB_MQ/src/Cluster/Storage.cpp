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
        std::string RecordName(std::string_view key, std::string_view extension = ".rec")
        {
            return ToHex(Sha256({reinterpret_cast<const uint8_t*>(key.data()), key.size()})) + std::string(extension);
        }

        constexpr uint32_t MMQP_JOURNAL_MAGIC = 0x454A4E31; // "EJN1"
        constexpr uint32_t MMQP_SNAPSHOT_MAGIC = 0x454D5332; // "EMS2"
        constexpr std::string_view MMQP_STORE_JOURNAL = "__store";
        constexpr size_t MMQP_COMPACT_BYTES = 8 * 1024 * 1024;

#ifdef _WIN32
        using FileHandle = HANDLE;
        const FileHandle MMQP_NO_FILE = INVALID_HANDLE_VALUE;
#else
        using FileHandle = int;
        constexpr FileHandle MMQP_NO_FILE = -1;
#endif

        void CloseFile(FileHandle& file)
        {
            if (file == MMQP_NO_FILE) return;
#ifdef _WIN32
            CloseHandle(file);
#else
            ::close(file);
#endif
            file = MMQP_NO_FILE;
        }

        FileHandle OpenAppend(const std::filesystem::path& path)
        {
            bool created = !std::filesystem::exists(path);
#ifdef _WIN32
            auto file = CreateFileW(path.c_str(), FILE_APPEND_DATA, FILE_SHARE_READ, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (file == INVALID_HANDLE_VALUE) throw std::runtime_error("Cannot open durable journal");
            (void)created;
#else
            int file = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0600);
            if (file < 0) throw std::runtime_error("Cannot open durable journal");
            if (created)
            {
                int directory = ::open(path.parent_path().c_str(), O_RDONLY | O_DIRECTORY);
                auto status = directory < 0 ? -1 : ::fsync(directory);
                if (directory >= 0) ::close(directory);
                if (status) { ::close(file); throw std::runtime_error("Durable directory flush failed"); }
            }
#endif
            return file;
        }

        void AppendFlush(FileHandle file, std::span<const uint8_t> bytes)
        {
#ifdef _WIN32
            size_t offset = 0;
            while (offset < bytes.size())
            {
                DWORD written = 0;
                auto count = static_cast<DWORD>(std::min<size_t>(bytes.size() - offset, 1024 * 1024));
                if (!WriteFile(file, bytes.data() + offset, count, &written, nullptr) || !written)
                    throw std::runtime_error("Durable journal write failed");
                offset += written;
            }
            if (!FlushFileBuffers(file)) throw std::runtime_error("Durable journal flush failed");
#else
            size_t offset = 0;
            while (offset < bytes.size())
            {
                auto count = ::write(file, bytes.data() + offset, bytes.size() - offset);
                if (count < 0 && errno == EINTR) continue;
                if (count <= 0) throw std::runtime_error("Durable journal write failed");
                offset += static_cast<size_t>(count);
            }
            if (::fsync(file)) throw std::runtime_error("Durable journal flush failed");
#endif
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
        std::map<std::string, Buffer, std::less<>> records;
        size_t bytes = 0;
        size_t maximum;
        size_t count;
        bool journaled = false;
        struct Log
        {
            FileHandle file = MMQP_NO_FILE;
            size_t bytes = 0;
        };
        std::map<std::string, Log, std::less<>> journals;
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
            for (auto& [name, log] : journals) CloseFile(log.file);
#ifdef _WIN32
            if (lock != INVALID_HANDLE_VALUE) CloseHandle(lock);
#else
            if (lock >= 0) ::close(lock);
#endif
        }
    };

    FileStore::FileStore(std::filesystem::path path, size_t maxBytes, size_t maxRecords, bool journaled):
        m_Impl(std::make_unique<Impl>(std::move(path), maxBytes, maxRecords))
    {
        m_Impl->journaled = journaled;
        auto snapshot = m_Impl->directory / "store.snapshot";
        if (journaled && std::filesystem::exists(snapshot))
        {
            // A snapshot supersedes any per-record files left from before journaling.
            Buffer bytes(static_cast<size_t>(std::filesystem::file_size(snapshot)));
            std::ifstream file(snapshot, std::ios::binary);
            if (bytes.size() < 40 || !file.read(reinterpret_cast<char*>(bytes.data()), bytes.size()))
                throw std::runtime_error("Cannot read durable snapshot");
            auto body = std::span<const uint8_t>(bytes).first(bytes.size() - 32);
            if (!SecureEqual(Sha256(body), std::span<const uint8_t>(bytes).last(32))) throw std::runtime_error("Durable snapshot checksum failed");
            Reader reader(body);
            if (reader.U32() != MMQP_SNAPSHOT_MAGIC) throw std::runtime_error("Unknown durable snapshot version");
            for (auto records = reader.U32(); records; --records)
            {
                auto key = reader.String();
                this->Load(std::move(key), ReadBuffer(reader));
            }
            reader.Finish();
        }
        else this->LoadRecords();
        if (!journaled) return;
        for (const auto& bytes : this->Journal(MMQP_STORE_JOURNAL))
        {
            Reader reader(bytes);
            auto operation = reader.U8();
            auto key = reader.String();
            if (operation == 1) this->Load(std::move(key), ReadBuffer(reader), true);
            else if (operation == 2) { if (auto found = m_Impl->records.find(key); found != m_Impl->records.end()) { m_Impl->bytes -= found->second.size(); m_Impl->records.erase(found); } }
            else throw std::runtime_error("Unknown durable journal operation");
            reader.Finish();
        }
    }

    void FileStore::Load(std::string key, Buffer value, bool replace)
    {
        auto found = m_Impl->records.find(key);
        auto old = found == m_Impl->records.end() ? 0 : found->second.size();
        if (key.empty() || (found != m_Impl->records.end() && !replace)
            || (found == m_Impl->records.end() && m_Impl->records.size() >= m_Impl->count)
            || value.size() > m_Impl->maximum - (m_Impl->bytes - old)) throw std::runtime_error("Invalid stored key or storage budget");
        m_Impl->bytes = m_Impl->bytes - old + value.size();
        m_Impl->records[std::move(key)] = std::move(value);
    }

    void FileStore::LoadRecords()
    {
        auto maxBytes = m_Impl->maximum;
        auto maxRecords = m_Impl->count;
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
            if (entry.path().filename() != RecordName(key)) throw std::runtime_error("Invalid stored key or storage budget");
            this->Load(std::move(key), std::move(value));
        }
    }

    void FileStore::Compact()
    {
        if (this->JournalBytes(MMQP_STORE_JOURNAL) < std::max(MMQP_COMPACT_BYTES, m_Impl->bytes)) return;
        Writer writer(SIZE_MAX);
        writer.U32(MMQP_SNAPSHOT_MAGIC);
        writer.U32(static_cast<uint32_t>(m_Impl->records.size()));
        for (const auto& [key, value] : m_Impl->records) { writer.String(key); WriteBuffer(writer, value); }
        auto digest = Sha256(writer.Data());
        writer.Bytes(digest);
        FlushReplace(m_Impl->directory / "store.snapshot", writer.Data());
        // The snapshot now holds every record: legacy files and the journal are superseded.
        for (const auto& entry : std::filesystem::directory_iterator(m_Impl->directory))
            if (entry.path().extension() == ".rec") std::filesystem::remove(entry.path());
        this->Reset(MMQP_STORE_JOURNAL);
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
        if (m_Impl->journaled)
        {
            Writer entry(value.size() + 300);
            entry.U8(1);
            entry.String(key);
            WriteBuffer(entry, value);
            this->Append(MMQP_STORE_JOURNAL, entry.Data());
            m_Impl->bytes = m_Impl->bytes - old + value.size();
            m_Impl->records[std::move(key)] = std::move(value);
            this->Compact();
            return;
        }
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
        auto value = this->Find(key);
        return value ? std::optional(*value) : std::nullopt;
    }

    const Buffer* FileStore::Find(std::string_view key) const
    {
        auto found = m_Impl->records.find(key);
        return found == m_Impl->records.end() ? nullptr : &found->second;
    }

    void FileStore::Erase(std::string_view key)
    {
        auto found = m_Impl->records.find(key);
        if (found == m_Impl->records.end()) return;
        if (m_Impl->journaled)
        {
            Writer entry(300);
            entry.U8(2);
            entry.String(key);
            this->Append(MMQP_STORE_JOURNAL, entry.Data());
            m_Impl->bytes -= found->second.size();
            m_Impl->records.erase(found);
            this->Compact();
            return;
        }
        std::filesystem::remove(m_Impl->directory / RecordName(key));
        m_Impl->bytes -= found->second.size();
        m_Impl->records.erase(found);
    }

    std::vector<std::string> FileStore::Keys(std::string_view prefix) const
    {
        std::vector<std::string> keys;
        for (auto it = m_Impl->records.lower_bound(prefix); it != m_Impl->records.end() && it->first.starts_with(prefix); ++it)
            keys.push_back(it->first);
        return keys;
    }

    size_t FileStore::Bytes() const { return m_Impl->bytes; }

    void FileStore::Append(std::string_view journal, std::span<const uint8_t> entry)
    {
        if (entry.size() > m_Impl->maximum) throw ProtocolError("Journal entry exceeds the storage budget", MMQP_REASON_BUSY);
        auto& log = m_Impl->journals[std::string(journal)];
        if (log.file == MMQP_NO_FILE) log.file = OpenAppend(m_Impl->directory / RecordName(journal, ".log"));
        Writer writer(entry.size() + 40);
        writer.U32(MMQP_JOURNAL_MAGIC);
        WriteBuffer(writer, entry);
        auto digest = Sha256(writer.Data());
        writer.Bytes(digest);
        try { AppendFlush(log.file, writer.Data()); }
        catch (...)
        {
            // A partial append may remain; only a reopened journal can drop it safely.
            CloseFile(log.file);
            throw;
        }
        log.bytes += writer.Data().size();
    }

    std::vector<Buffer> FileStore::Journal(std::string_view journal)
    {
        auto& log = m_Impl->journals[std::string(journal)];
        CloseFile(log.file);
        auto path = m_Impl->directory / RecordName(journal, ".log");
        std::vector<Buffer> entries;
        log.bytes = 0;
        if (!std::filesystem::exists(path)) return entries;
        auto size = std::filesystem::file_size(path);
        if (size > m_Impl->maximum * 2 + MMQP_COMPACT_BYTES * 4) throw std::runtime_error("Durable journal exceeds its byte budget");
        Buffer bytes(static_cast<size_t>(size));
        std::ifstream file(path, std::ios::binary);
        if (size && !file.read(reinterpret_cast<char*>(bytes.data()), bytes.size())) throw std::runtime_error("Cannot read durable journal");
        file.close();
        size_t offset = 0;
        while (offset < bytes.size())
        {
            auto rest = std::span<const uint8_t>(bytes).subspan(offset);
            Reader header(rest.first(std::min<size_t>(rest.size(), 8)));
            bool torn = rest.size() < 8;
            uint64_t length = 0;
            if (!torn)
            {
                if (header.U32() != MMQP_JOURNAL_MAGIC) throw std::runtime_error("Unknown durable journal entry");
                length = header.U32();
                torn = rest.size() < 8 + length + 32;
            }
            if (torn) break;
            auto body = rest.first(static_cast<size_t>(8 + length));
            bool valid = SecureEqual(Sha256(body), rest.subspan(body.size(), 32));
            if (!valid && body.size() + 32 == rest.size()) break; // torn final flush
            if (!valid) throw std::runtime_error("Durable journal checksum failed");
            entries.emplace_back(body.begin() + 8, body.end());
            offset += body.size() + 32;
        }
        if (offset != bytes.size()) std::filesystem::resize_file(path, offset);
        log.bytes = offset;
        return entries;
    }

    void FileStore::Reset(std::string_view journal)
    {
        auto& log = m_Impl->journals[std::string(journal)];
        CloseFile(log.file);
        std::filesystem::remove(m_Impl->directory / RecordName(journal, ".log"));
        log.bytes = 0;
    }

    size_t FileStore::JournalBytes(std::string_view journal) const
    {
        auto found = m_Impl->journals.find(journal);
        return found == m_Impl->journals.end() ? 0 : found->second.bytes;
    }

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
