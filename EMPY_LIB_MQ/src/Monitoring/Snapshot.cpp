#include <eMQ/Monitoring/Snapshot.h>

namespace eMQ
{
    namespace
    {
        // This writer only produces JSON; it is not a general parser or dependency.
        struct Json
        {
            std::string value;
            std::vector<bool> first;

            void Separator()
            {
                if (!first.back()) value += ',';
                first.back() = false;
            }

            void Quote(std::string_view text)
            {
                constexpr char hex[] = "0123456789abcdef";
                value += '"';
                for (auto byte : text)
                {
                    auto c = static_cast<uint8_t>(byte);
                    if (c == '"' || c == '\\') { value += '\\'; value += byte; }
                    else if (c < 32) { value += "\\u00"; value += hex[c >> 4]; value += hex[c & 15]; }
                    else value += byte;
                }
                value += '"';
            }

            void Key(std::string_view key) { this->Separator(); this->Quote(key); value += ':'; }
            void Begin(char bracket) { value += bracket; first.push_back(true); }
            void End(char bracket) { value += bracket; first.pop_back(); }
            void Object() { this->Separator(); this->Begin('{'); }
            void Object(std::string_view key) { this->Key(key); this->Begin('{'); }
            void Array(std::string_view key) { this->Key(key); this->Begin('['); }
            void Number(std::string_view key, uint64_t number) { this->Key(key); value += std::to_string(number); }
            void String(std::string_view key, std::string_view text) { this->Key(key); this->Quote(text); }
            void Boolean(std::string_view key, bool boolean) { this->Key(key); value += boolean ? "true" : "false"; }
            void Optional(std::string_view key, const std::optional<uint64_t>& number)
            {
                this->Key(key);
                value += number ? std::to_string(*number) : "null";
            }

            void Traffic(const TrafficMetrics& metrics)
            {
                this->Number("receivedPackets", metrics.receivedPackets);
                this->Number("receivedBytes", metrics.receivedBytes);
                this->Number("sentPackets", metrics.sentPackets);
                this->Number("sentBytes", metrics.sentBytes);
            }
        };
    }

    std::string SnapshotJson(const BrokerSnapshot& snapshot)
    {
        Json json;
        json.Begin('{');
        json.Number("schema", 1);
        json.String("identifier", snapshot.identifier);
        json.String("instance", snapshot.instance);
        json.String("address", snapshot.address);
        json.Number("port", snapshot.port);
        json.String("transport", snapshot.transport);
        json.Number("collectedAt", snapshot.collectedAt);
        json.Number("uptimeMs", snapshot.uptimeMs);
        json.Boolean("durable", snapshot.durable);
        json.Boolean("authenticationRequired", snapshot.authenticationRequired);
        json.Optional("storedBytes", snapshot.storedBytes);
        json.Object("metrics");
        json.Traffic(snapshot.metrics);
        json.Number("accepted", snapshot.metrics.accepted);
        json.Number("publications", snapshot.metrics.publications);
        json.Number("duplicates", snapshot.metrics.duplicates);
        json.Number("rejected", snapshot.metrics.rejected);
        json.Number("forwarded", snapshot.metrics.forwarded);
        json.Number("retransmitted", snapshot.metrics.retransmitted);
        json.Number("sessions", snapshot.metrics.sessions);
        json.Number("online", snapshot.metrics.online);
        json.Number("retainedBytes", snapshot.metrics.retainedBytes);
        json.Number("pending", snapshot.metrics.pending);
        json.End('}');
        json.Object("limits");
        json.Number("sessions", snapshot.maxSessions);
        json.Number("inflightPerSession", snapshot.maxInflight);
        json.Number("retainedBytes", snapshot.maxRetainedBytes);
        json.Number("storedBytes", snapshot.maxStateBytes);
        json.End('}');
        json.Number("omittedSessions", snapshot.omittedSessions);
        json.Array("sessions");
        for (const auto& session : snapshot.sessions)
        {
            json.Object();
            json.String("identifier", session.identifier);
            json.String("realm", session.realm);
            json.String("transport", session.transport);
            json.Boolean("online", session.online);
            json.Boolean("authenticated", session.authenticated);
            json.Boolean("resumable", session.resumable);
            json.Boolean("owner", session.owner);
            json.Number("observedAt", session.observedAt);
            json.Number("publications", session.publications);
            json.Number("deliveries", session.deliveries);
            json.Optional("idleMs", session.idleMs);
            json.Optional("expiresInMs", session.expiresInMs);
            json.Number("keepaliveSeconds", session.keepaliveSeconds);
            json.Number("subscriptions", session.subscriptions);
            json.Number("registrations", session.registrations);
            json.Number("responders", session.responders);
            json.Number("requesters", session.requesters);
            json.Number("pending", session.pending);
            json.Number("pendingBytes", session.pendingBytes);
            json.Number("queuedBytes", session.queuedBytes);
            json.Number("replay", session.replay);
            json.Number("replayBytes", session.replayBytes);
            json.Number("requests", session.requests);
            json.Number("responseRoutes", session.responseRoutes);
            json.Traffic(session);
            if (session.lease)
            {
                json.Object("lease");
                json.String("fence", std::to_string(session.lease->fence));
                json.Number("remainingMs", session.lease->remainingMs);
                json.End('}');
            }
            else { json.Key("lease"); json.value += "null"; }
            json.End('}');
        }
        json.End(']');
        if (snapshot.cluster)
        {
            const auto& cluster = *snapshot.cluster;
            json.Object("cluster");
            json.String("identifier", cluster.identifier);
            json.String("epoch", std::to_string(cluster.epoch));
            json.Number("shards", cluster.shards);
            json.Number("replicas", cluster.replicas);
            json.Number("fanout", cluster.fanout);
            json.Number("pendingOperations", cluster.pendingOperations);
            json.Number("activeLeases", cluster.activeLeases);
            json.Number("quarantineMs", cluster.quarantineMs);
            json.Array("peers");
            for (const auto& peer : cluster.peers)
            {
                json.Object();
                json.String("identifier", peer.identifier);
                json.String("host", peer.host);
                json.Number("port", peer.port);
                json.Boolean("connected", peer.connected);
                json.Number("queuedBytes", peer.queuedBytes);
                json.End('}');
            }
            json.End(']');
            json.End('}');
        }
        else { json.Key("cluster"); json.value += "null"; }
        json.End('}');
        if (json.value.size() > 16 * 1024 * 1024) throw std::length_error("Telemetry snapshot exceeds 16 MiB");
        return std::move(json.value);
    }
}
