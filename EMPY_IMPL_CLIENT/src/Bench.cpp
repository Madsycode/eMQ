#include "Bench.h"
#include <eClient/API.h>
#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <csignal>
#include <iomanip>
#include <iostream>
#include <sstream>
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace
{
    using Clock = std::chrono::steady_clock;
    constexpr size_t BENCH_WINDOW = 256;
    constexpr size_t BENCH_BURST = 64;
    constexpr size_t BENCH_BUCKETS = 800; // 100 per decade from 1 us to 100 s
    constexpr size_t BENCH_HEADER = 16;
    constexpr uint64_t BENCH_OVERFLOW = UINT64_MAX;

    volatile std::sig_atomic_t g_Stop = 0;

    uint64_t NowUs()
    {
        auto now = std::chrono::system_clock::now().time_since_epoch();
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(now).count());
    }

    void Store(uint8_t* data, uint64_t value, size_t bytes)
    {
        for (size_t i = 0; i < bytes; ++i)
            data[i] = static_cast<uint8_t>(value >> (8 * (bytes - 1 - i)));
    }

    uint64_t Load(const uint8_t* data, size_t bytes)
    {
        uint64_t value = 0;
        for (size_t i = 0; i < bytes; ++i)
            value = (value << 8) | data[i];
        return value;
    }

    struct Histogram
    {
        Histogram(): m_Counts(BENCH_BUCKETS + 1, 0)
        {
            // Integer bounds from a fixed formula keep merged histograms aligned.
            m_Bounds.resize(BENCH_BUCKETS);
            for (size_t i = 0; i < BENCH_BUCKETS; ++i)
                m_Bounds[i] = static_cast<uint64_t>(std::llround(std::pow(10.0, static_cast<double>(i + 1) / 100.0)));
        }

        void Record(uint64_t value)
        {
            ++m_Counts[std::lower_bound(m_Bounds.begin(), m_Bounds.end(), value) - m_Bounds.begin()];
            m_Min = m_Count ? std::min(m_Min, value) : value;
            m_Max = std::max(m_Max, value);
            m_Sum += value;
            ++m_Count;
        }

        uint64_t Bound(size_t index) const { return index < BENCH_BUCKETS ? m_Bounds[index] : BENCH_OVERFLOW; }

        uint64_t Percentile(double quantile) const
        {
            if (!m_Count) return 0;
            auto rank = std::max<uint64_t>(1, static_cast<uint64_t>(std::ceil(quantile * static_cast<double>(m_Count))));
            uint64_t seen = 0;
            for (size_t i = 0; i < m_Counts.size(); ++i)
                if ((seen += m_Counts[i]) >= rank) return i < BENCH_BUCKETS ? m_Bounds[i] : m_Max;
            return m_Max;
        }

        std::vector<uint64_t> m_Bounds;
        std::vector<uint64_t> m_Counts;
        uint64_t m_Count = 0;
        uint64_t m_Min = 0;
        uint64_t m_Max = 0;
        uint64_t m_Sum = 0;
    };

    struct Track
    {
        std::vector<uint64_t> m_Seen;
        uint32_t m_Next = 0; // one past the highest sequence seen
    };

    struct Publisher
    {
        eMQ::Client* m_Client = nullptr;
        std::string m_Topic;
        std::array<std::future<eMQ::Reason>, BENCH_WINDOW> m_Window;
        size_t m_Head = 0;
        size_t m_Pending = 0;
        uint64_t m_Sent = 0;
        uint64_t m_Accepted = 0;
        uint64_t m_Rejected = 0;
        uint64_t m_Failed = 0;
    };

    struct Endpoint
    {
        std::string host;
        uint16_t port = 0;
    };

    uint32_t Number(std::string_view text, uint32_t minimum, uint32_t maximum, const char* name)
    {
        uint32_t value = 0;
        auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
        if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() || value < minimum || value > maximum)
            throw std::invalid_argument(std::string("Invalid value for ") + name);
        return value;
    }

    std::vector<Endpoint> Endpoints(std::string_view list)
    {
        std::vector<Endpoint> endpoints;
        while (!list.empty())
        {
            auto item = list.substr(0, list.find(','));
            list.remove_prefix(std::min(list.size(), item.size() + 1));
            auto colon = item.rfind(':');
            if (colon == std::string_view::npos || colon == 0)
                throw std::invalid_argument("Endpoints must be HOST:PORT[,HOST:PORT...]");
            auto host = item.substr(0, colon);
            if (host.size() > 2 && host.front() == '[' && host.back() == ']') host = host.substr(1, host.size() - 2);
            endpoints.push_back({std::string(host), static_cast<uint16_t>(Number(item.substr(colon + 1), 1, UINT16_MAX, "--endpoints port"))});
        }
        if (endpoints.empty()) throw std::invalid_argument("Provide at least one endpoint");
        return endpoints;
    }

    bool Harvest(Publisher& publisher)
    {
        bool progress = false;
        while (publisher.m_Pending)
        {
            auto& future = publisher.m_Window[publisher.m_Head];
            if (future.wait_for(std::chrono::seconds(0)) != std::future_status::ready) break;
            try
            {
                if (future.get().code >= 0x80) ++publisher.m_Rejected;
                else ++publisher.m_Accepted;
            }
            catch (...) { ++publisher.m_Failed; }
            publisher.m_Head = (publisher.m_Head + 1) % BENCH_WINDOW;
            --publisher.m_Pending;
            progress = true;
        }
        return progress;
    }

    void SleepIdle(bool busy, Clock::time_point& active)
    {
        // Spin briefly after activity so sleeping does not skew measured latency.
        auto now = Clock::now();
        if (busy) { active = now; return; }
        if (now - active < std::chrono::milliseconds(2)) { std::this_thread::yield(); return; }
#ifdef _WIN32
        // Plain sleeps round up to the 15.6 ms system tick; a high resolution timer keeps ~1 ms.
        static thread_local HANDLE timer = CreateWaitableTimerExW(nullptr, nullptr, 0x2, TIMER_ALL_ACCESS);
        LARGE_INTEGER due;
        due.QuadPart = -10000;
        if (timer && SetWaitableTimer(timer, &due, 0, nullptr, nullptr, FALSE))
        {
            WaitForSingleObject(timer, INFINITE);
            return;
        }
#endif
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

int32_t RunBench(int32_t argc, char** argv)
{
    try
    {
        eMQ::ClientOptions options;
        std::string prefix;
        std::string host = "127.0.0.1";
        uint16_t port = 5600;
        std::vector<Endpoint> endpoints;
        uint32_t publishers = 1, subscribers = 1, topics = 1, rate = 10, size = 64, seconds = 30, drain = 3000, offset = 0;
        uint8_t qos = 1;
        bool verbose = false;
        for (int32_t i = 2; i < argc; ++i)
        {
            std::string_view argument(argv[i]);
            if (!argument.starts_with("--"))
            {
                if (!prefix.empty()) throw std::invalid_argument("Bench takes a single topic prefix");
                prefix = argument;
                continue;
            }
            if (argument == "--verbose") { verbose = true; continue; }
            if (argument == "--help")
            {
                std::cout << "EmpyKlient bench PREFIX --endpoints HOST:PORT[,HOST:PORT...] [--config FILE]\n"
                    << "  --publishers P --subscribers S [--topics T] [--rate R] [--payload B] [--qos 0|1]\n"
                    << "  [--seconds N] [--drain-ms D] [--offset K] [--verbose]\n";
                return EXIT_SUCCESS;
            }
            if (++i == argc) throw std::invalid_argument("Missing option value");
            std::string_view value(argv[i]);
            if (argument == "--config") options = eMQ::LoadClientOptions(argv[i]);
            else if (argument == "--endpoints") endpoints = Endpoints(value);
            else if (argument == "--host") host = argv[i];
            else if (argument == "--port") port = static_cast<uint16_t>(Number(value, 1, UINT16_MAX, "--port"));
            else if (argument == "--publishers") publishers = Number(value, 0, 2000, "--publishers");
            else if (argument == "--subscribers") subscribers = Number(value, 0, 2000, "--subscribers");
            else if (argument == "--topics") topics = Number(value, 1, 2000, "--topics");
            else if (argument == "--rate") rate = Number(value, 0, 100000, "--rate");
            else if (argument == "--payload") size = Number(value, BENCH_HEADER, 1048576, "--payload");
            else if (argument == "--qos") qos = static_cast<uint8_t>(Number(value, 0, 1, "--qos"));
            else if (argument == "--seconds") seconds = Number(value, 1, 86400, "--seconds");
            else if (argument == "--drain-ms") drain = Number(value, 0, 600000, "--drain-ms");
            else if (argument == "--offset") offset = Number(value, 0, UINT32_MAX, "--offset");
            else throw std::invalid_argument("Unknown bench option");
        }
        if (prefix.empty()) throw std::invalid_argument("Provide a bench topic prefix; use --help");
        if (!publishers && !subscribers) throw std::invalid_argument("Bench needs at least one publisher or subscriber");
        if (topics > std::max<uint32_t>(publishers, 1)) throw std::invalid_argument("--topics must not exceed --publishers");
        if (endpoints.empty()) endpoints.push_back({host, port});
        std::signal(SIGINT, [] (int) { g_Stop = 1; });
        std::signal(SIGTERM, [] (int) { g_Stop = 1; });

        // Clients: publishers first, then subscribers, spread over the endpoints.
        auto total = static_cast<size_t>(publishers) + subscribers;
        std::vector<std::unique_ptr<eMQ::Client>> clients;
        clients.reserve(total);
        for (size_t c = 0; c < total; ++c)
            clients.push_back(std::make_unique<eMQ::Client>(options));
        std::vector<Publisher> pubs(publishers);
        for (uint32_t i = 0; i < publishers; ++i)
        {
            pubs[i].m_Client = clients[i].get();
            pubs[i].m_Topic = prefix + "/t" + std::to_string(i % topics);
        }

        // Subscriber j tracks publishers i with i % T == j % T at slot i / T.
        Histogram latency;
        uint64_t received = 0, duplicates = 0, reordered = 0;
        std::vector<std::vector<Track>> tracks(subscribers);
        for (uint32_t j = 0; j < subscribers; ++j)
        {
            auto topic = j % topics;
            tracks[j].resize(publishers > topic ? (publishers - topic + topics - 1) / topics : 0);
            clients[publishers + j]->On<eMQ::Receive>(prefix + "/t" + std::to_string(topic),
                [&, j, topic] (const eMQ::Receive& event)
            {
                const auto& data = event.message.payload;
                if (data.size() < BENCH_HEADER) return;
                auto now = NowUs();
                auto sent = Load(data.data(), 8);
                auto index = Load(data.data() + 8, 4);
                auto sequence = static_cast<uint32_t>(Load(data.data() + 12, 4));
                if (index >= publishers || index % topics != topic || sequence >= pubs[index].m_Sent) return;
                auto& track = tracks[j][index / topics];
                auto word = sequence / 64;
                auto bit = uint64_t(1) << (sequence % 64);
                if (word >= track.m_Seen.size()) track.m_Seen.resize(std::max<size_t>(word + 1, track.m_Seen.size() * 2), 0);
                if (track.m_Seen[word] & bit) { ++duplicates; return; }
                track.m_Seen[word] |= bit;
                ++received;
                if (sequence < track.m_Next) ++reordered;
                else track.m_Next = sequence + 1;
                latency.Record(now > sent ? now - sent : 0);
            });
        }

        // Publisher i and subscriber j start at the same rotation but subscribers are shifted
        // by half the endpoint list, so matching pairs sit on different brokers.
        auto Place = [&] (size_t c) -> const auto&
        {
            auto shift = c < publishers ? c : c - publishers + std::max<size_t>(1, endpoints.size() / 2);
            return endpoints[(offset + shift) % endpoints.size()];
        };
        auto connectStart = Clock::now();
        {
            std::vector<std::future<std::string>> connects;
            connects.reserve(total);
            for (size_t c = 0; c < total; ++c)
            {
                const auto& endpoint = Place(c);
                connects.push_back(clients[c]->AsyncConnect(endpoint.host, endpoint.port));
            }
            for (size_t c = 0; c < total; ++c)
            {
                try { connects[c].get(); }
                catch (const std::exception& error)
                {
                    const auto& endpoint = Place(c);
                    throw std::runtime_error("Bench client " + std::to_string(c) + " failed to connect to "
                        + endpoint.host + ":" + std::to_string(endpoint.port) + ": " + error.what());
                }
            }
        }
        auto connectSeconds = std::chrono::duration<double>(Clock::now() - connectStart).count();
        {
            std::vector<std::future<eMQ::Reason>> profiles;
            profiles.reserve(total);
            for (uint32_t i = 0; i < publishers; ++i)
                profiles.push_back(clients[i]->Register(pubs[i].m_Topic, 0, 60, 1, false));
            for (uint32_t j = 0; j < subscribers; ++j)
                profiles.push_back(clients[publishers + j]->Subscribe(prefix + "/t" + std::to_string(j % topics), qos));
            for (size_t c = 0; c < total; ++c)
            {
                auto reason = profiles[c].get();
                if (reason.code >= 0x80)
                    throw std::runtime_error("Bench client " + std::to_string(c) + " profile rejected: "
                        + std::to_string(reason.code) + " " + reason.text);
            }
        }
        std::cerr << "Bench ready" << std::endl;

        auto pollAll = [&]
        {
            auto before = received + duplicates;
            for (auto& client : clients)
                client->Poll();
            return received + duplicates != before;
        };
        auto counter = [&] (uint64_t Publisher::* field)
        {
            uint64_t sum = 0;
            for (const auto& publisher : pubs)
                sum += publisher.*field;
            return sum;
        };

        // Send window: schedule-paced publications, single owner thread.
        auto start = Clock::now();
        auto end = start + std::chrono::seconds(seconds);
        auto report = start + std::chrono::seconds(1);
        uint64_t reported = 0;
        auto now = start;
        auto active = start;
        while ((now = Clock::now()) < end && !g_Stop)
        {
            bool busy = false;
            auto elapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(now - start).count());
            auto due = rate ? elapsed * rate / 1000000 + 1 : UINT64_MAX;
            for (uint32_t i = 0; i < publishers; ++i)
            {
                auto& publisher = pubs[i];
                busy |= Harvest(publisher);
                for (size_t burst = 0; burst < BENCH_BURST && publisher.m_Sent < due && publisher.m_Pending < BENCH_WINDOW; ++burst)
                {
                    eMQ::Buffer payload(size, 0);
                    Store(payload.data(), NowUs(), 8);
                    Store(payload.data() + 8, i, 4);
                    Store(payload.data() + 12, publisher.m_Sent, 4);
                    ++publisher.m_Sent;
                    try
                    {
                        auto slot = (publisher.m_Head + publisher.m_Pending) % BENCH_WINDOW;
                        publisher.m_Window[slot] = publisher.m_Client->Publish(publisher.m_Topic, std::move(payload), qos);
                        ++publisher.m_Pending;
                    }
                    catch (...) { ++publisher.m_Failed; }
                    busy = true;
                }
            }
            busy |= pollAll();
            if (verbose && now >= report)
            {
                auto t = std::chrono::duration<double>(now - start).count();
                std::cerr << "bench t=" << std::fixed << std::setprecision(1) << t << " sent=" << counter(&Publisher::m_Sent)
                    << " accepted=" << counter(&Publisher::m_Accepted) << " received=" << received
                    << " rate=" << (received - reported) << std::endl;
                reported = received;
                report += std::chrono::seconds(1);
            }
            SleepIdle(busy, active);
        }
        auto window = std::chrono::duration<double>(now - start).count();

        // Drain: settle outstanding futures and deliveries, stop once complete.
        auto expected = [&]
        {
            std::vector<uint64_t> perTopic(topics, 0);
            for (uint32_t i = 0; i < publishers; ++i)
                perTopic[i % topics] += pubs[i].m_Accepted; // QoS 0: admitted by the local transport
            uint64_t sum = 0;
            for (uint32_t j = 0; j < subscribers; ++j)
                sum += perTopic[j % topics];
            return sum;
        };
        auto drainEnd = Clock::now() + std::chrono::milliseconds(drain);
        while (Clock::now() < drainEnd && !g_Stop)
        {
            bool busy = false;
            for (auto& publisher : pubs)
                busy |= Harvest(publisher);
            busy |= pollAll();
            if (!counter(&Publisher::m_Pending) && received >= expected()) break;
            SleepIdle(busy, active);
        }
        for (auto& publisher : pubs)
        {
            // Unsettled publications after the drain count as failed.
            publisher.m_Failed += publisher.m_Pending;
            publisher.m_Pending = 0;
        }
        auto wanted = expected();

        // Graceful disconnects block on the close handshake; run them in parallel.
        {
            std::vector<std::thread> workers;
            auto count = std::min<size_t>(16, total);
            for (size_t w = 0; w < count; ++w)
                workers.emplace_back([&, w]
                {
                    for (size_t c = w; c < total; c += count)
                    {
                        try { clients[c]->Disconnect(); }
                        catch (...) {}
                    }
                });
            for (auto& worker : workers)
                worker.join();
        }

        std::ostringstream json;
        json << std::fixed << std::setprecision(3)
            << "{\"bench\":1,\"publishers\":" << publishers << ",\"subscribers\":" << subscribers
            << ",\"topics\":" << topics << ",\"qos\":" << uint32_t(qos) << ",\"payload\":" << size
            << ",\"rate\":" << rate << ",\"seconds\":" << window << ",\"connectSeconds\":" << connectSeconds
            << ",\"sent\":" << counter(&Publisher::m_Sent) << ",\"accepted\":" << counter(&Publisher::m_Accepted)
            << ",\"rejected\":" << counter(&Publisher::m_Rejected) << ",\"failed\":" << counter(&Publisher::m_Failed)
            << ",\"expected\":" << wanted << ",\"received\":" << received << ",\"duplicates\":" << duplicates
            << ",\"reordered\":" << reordered << ",\"lost\":" << (wanted > received ? wanted - received : 0)
            << ",\"latencyUs\":{\"count\":" << latency.m_Count << ",\"min\":" << latency.m_Min
            << ",\"mean\":" << (latency.m_Count ? static_cast<double>(latency.m_Sum) / static_cast<double>(latency.m_Count) : 0.0)
            << ",\"p50\":" << latency.Percentile(0.50) << ",\"p90\":" << latency.Percentile(0.90)
            << ",\"p99\":" << latency.Percentile(0.99) << ",\"p999\":" << latency.Percentile(0.999)
            << ",\"max\":" << latency.m_Max << "},\"histogram\":[";
        bool first = true;
        for (size_t b = 0; b < latency.m_Counts.size(); ++b)
        {
            if (!latency.m_Counts[b]) continue;
            json << (first ? "" : ",") << '[' << latency.Bound(b) << ',' << latency.m_Counts[b] << ']';
            first = false;
        }
        json << "]}\n";
        std::cout << json.str() << std::flush;
        return EXIT_SUCCESS;
    }
    catch (const std::exception& error)
    {
        std::cerr << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
