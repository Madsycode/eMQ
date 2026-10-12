#include <eMQ/Runtime/Broker.h>
#include <eMQ/Security/Crypto.h>
#include <eMQ/Core/Auxiliaries/YAML.h>
#include <cstdlib>

namespace eMQ
{
    BrokerOptions LoadBrokerOptions(const char* filename)
    {
        auto root = eMQ::LoadYAML(filename);
        BrokerOptions options;
        options.address = eMQ::ReadYAML(root, "address", options.address);
        options.port = eMQ::ReadYAML(root, "port", options.port);
        options.workers = eMQ::ReadYAML(root, "workers", options.workers);
        options.maxSessions = eMQ::ReadYAML(root, "max_sessions", options.maxSessions);
        options.maxProfiles = eMQ::ReadYAML(root, "max_profiles", options.maxProfiles);
        options.maxInflight = eMQ::ReadYAML(root, "max_inflight", options.maxInflight);
        options.maxSessionBytes = eMQ::ReadYAML(root, "max_session_bytes", options.maxSessionBytes);
        options.maxRetainedBytes = eMQ::ReadYAML(root, "max_retained_bytes", options.maxRetainedBytes);
        options.maxRetainedTopics = eMQ::ReadYAML(root, "max_retained_topics", options.maxRetainedTopics);
        options.maxRequests = eMQ::ReadYAML(root, "max_requests", options.maxRequests);
        options.maxResponders = eMQ::ReadYAML(root, "max_responders", options.maxResponders);
        options.retryInterval = eMQ::ReadYAML(root, "retry_interval_ms", options.retryInterval);
        options.maxRetries = eMQ::ReadYAML(root, "max_retries", options.maxRetries);
        options.requestTimeout = eMQ::ReadYAML(root, "request_timeout_ms", options.requestTimeout);
        options.sessionExpiry = EncodeInterval(eMQ::ReadYAML<uint32_t>(root, "session_expiry_seconds", 300));
        options.keepalive = EncodeInterval(eMQ::ReadYAML<uint32_t>(root, "keepalive_seconds", 30));
        options.tenantRoot = eMQ::ReadYAML(root, "tenant_root", options.tenantRoot);
        options.requireAuth = eMQ::ReadYAML(root, "require_auth", options.requireAuth);
        options.allowRemotePlaintext = eMQ::ReadYAML(root, "allow_remote_plaintext", options.allowRemotePlaintext);
        options.stateDirectory = eMQ::ConfigPath(filename, eMQ::ReadYAML<std::string>(root, "state_directory", {}));
        options.maxStateBytes = eMQ::ReadYAML(root, "max_state_bytes", options.maxStateBytes);
        options.maxReplayBytes = eMQ::ReadYAML(root, "max_replay_bytes", options.maxReplayBytes);
        options.transport.maxPacket = eMQ::ReadYAML(root, "max_packet_bytes", options.transport.maxPacket);
        options.transport.maxQueue = eMQ::ReadYAML(root, "max_send_bytes", options.transport.maxQueue);
        options.transport.timeout = eMQ::ReadYAML(root, "handshake_timeout_ms", options.transport.timeout);
        options.transport.tls = eMQ::ReadYAML(root, "tls", false);
        options.transport.quic = eMQ::ReadYAML(root, "quic", false);
        if (options.transport.quic)
        {
            options.transport.quicPolicy.strategy = eMQ::ReadYAML<uint8_t>(root, "quic_strategy", MMQP_QUIC_STRATEGY_BULK);
            if (*options.transport.quicPolicy.strategy == MMQP_QUIC_STRATEGY_BULK)
                options.transport.quicPolicy.bulk = BulkPolicy{eMQ::ReadYAML<uint8_t>(root, "bulk_streams", 2),
                    eMQ::ReadYAML<uint32_t>(root, "bulk_threshold_bytes", 65536)};
            else options.transport.quicPolicy.bulk.reset();
            auto datagram = eMQ::ReadYAML<uint32_t>(root, "datagram_threshold_bytes", 1200);
            options.transport.quicPolicy.datagram = datagram ? std::optional(datagram) : std::nullopt;
            QuicPlan(options.transport.quicPolicy);
        }
        options.transport.mutual = eMQ::ReadYAML(root, "mutual_tls", false);
        options.transport.certificate = eMQ::ConfigPath(filename, eMQ::ReadYAML<std::string>(root, "certificate", {}));
        options.transport.certificateHash = eMQ::ReadYAML<std::string>(root, "certificate_hash", {});
        options.transport.privateKey = eMQ::ConfigPath(filename, eMQ::ReadYAML<std::string>(root, "private_key", {}));
        options.transport.authority = eMQ::ConfigPath(filename, eMQ::ReadYAML<std::string>(root, "authority", {}));
        auto variable = eMQ::ReadYAML<std::string>(root, "auth_key_env", {});
        if (!variable.empty())
        {
            auto secret = std::getenv(variable.c_str());
            if (!secret) throw std::invalid_argument("Authentication key environment variable is missing");
            options.authKey = FromHex(secret);
        }
        if (root.contains("telemetry"))
        {
            const auto& node = root["telemetry"];
            if (!node.is_mapping()) throw std::invalid_argument("telemetry must be a mapping");
            TelemetryOptions telemetry;
            telemetry.address = eMQ::ReadYAML(node, "address", telemetry.address);
            telemetry.port = eMQ::ReadYAML(node, "port", telemetry.port);
            telemetry.intervalMs = eMQ::ReadYAML(node, "interval_ms", telemetry.intervalMs);
            telemetry.maxSessions = eMQ::ReadYAML(node, "max_sessions", telemetry.maxSessions);
            auto variable = eMQ::ReadYAML<std::string>(node, "token_env", {});
            if (!variable.empty())
            {
                auto token = std::getenv(variable.c_str());
                if (!token || !*token) throw std::invalid_argument("Telemetry token environment variable is missing or empty");
                telemetry.token = token;
            }
            options.telemetry = std::move(telemetry);
        }
        if (root.contains("cluster"))
        {
            const auto& node = root["cluster"];
            if (!node.is_mapping()) throw std::invalid_argument("cluster must be a mapping");
            ClusterOptions cluster;
            cluster.identifier = eMQ::ReadYAML<std::string>(node, "identifier", {});
            cluster.address = eMQ::ReadYAML(node, "address", cluster.address);
            cluster.port = eMQ::ReadYAML(node, "port", cluster.port);
            cluster.directory = eMQ::ConfigPath(filename, eMQ::ReadYAML<std::string>(node, "state_directory", {}));
            cluster.fabric.epoch = eMQ::ReadYAML(node, "epoch", cluster.fabric.epoch);
            cluster.fabric.shards = eMQ::ReadYAML(node, "shards", cluster.fabric.shards);
            cluster.fabric.replicas = eMQ::ReadYAML(node, "replicas", cluster.fabric.replicas);
            cluster.fabric.fanout = eMQ::ReadYAML(node, "fanout", cluster.fabric.fanout);
            cluster.leaseMilliseconds = eMQ::ReadYAML(node, "lease_ms", cluster.leaseMilliseconds);
            cluster.timeoutMilliseconds = eMQ::ReadYAML(node, "rpc_timeout_ms", cluster.timeoutMilliseconds);
            cluster.maxBytes = eMQ::ReadYAML(node, "max_state_bytes", cluster.maxBytes);
            cluster.maxRecords = eMQ::ReadYAML(node, "max_records", cluster.maxRecords);
            cluster.maxOperations = eMQ::ReadYAML(node, "max_operations", cluster.maxOperations);
            cluster.maxInboxBytes = eMQ::ReadYAML(node, "max_inbox_bytes", cluster.maxInboxBytes);
            cluster.maxInboxRecords = eMQ::ReadYAML(node, "max_inbox_records", cluster.maxInboxRecords);
            cluster.transport.quic = eMQ::ReadYAML(node, "quic", true);
            cluster.transport.tls = true;
            cluster.transport.mutual = true;
            cluster.transport.maxPacket = eMQ::ReadYAML<uint32_t>(node, "max_packet_bytes", 16 * 1024 * 1024);
            cluster.transport.maxQueue = eMQ::ReadYAML<size_t>(node, "max_send_bytes", 64 * 1024 * 1024);
            cluster.transport.certificate = eMQ::ConfigPath(filename, eMQ::ReadYAML<std::string>(node, "certificate", {}));
            cluster.transport.privateKey = eMQ::ConfigPath(filename, eMQ::ReadYAML<std::string>(node, "private_key", {}));
            cluster.transport.authority = eMQ::ConfigPath(filename, eMQ::ReadYAML<std::string>(node, "authority", {}));
            cluster.transport.certificateHash = eMQ::ReadYAML<std::string>(node, "certificate_hash", {});
            auto variable = eMQ::ReadYAML<std::string>(node, "epoch_key_env", {});
            auto secret = variable.empty() ? nullptr : std::getenv(variable.c_str());
            if (!secret) throw std::invalid_argument("Cluster epoch key environment variable is missing");
            cluster.epochKey = FromHex(secret);
            if (!node.contains("peers") || !node["peers"].is_sequence()) throw std::invalid_argument("Cluster peers must be a sequence");
            for (const auto& peer : node["peers"])
            {
                if (!peer.is_mapping()) throw std::invalid_argument("Cluster peer must be a mapping");
                ClusterPeer member;
                member.identifier = eMQ::ReadYAML<std::string>(peer, "identifier", {});
                member.host = eMQ::ReadYAML(peer, "host", member.host);
                member.port = eMQ::ReadYAML<uint16_t>(peer, "port", 0);
                member.serverName = eMQ::ReadYAML<std::string>(peer, "server_name", {});
                member.fingerprint = ToHex(FromHex(eMQ::ReadYAML<std::string>(peer, "fingerprint", {})));
                cluster.fabric.brokers.push_back(member.identifier);
                cluster.peers.push_back(std::move(member));
            }
            options.cluster = std::move(cluster);
        }
        if (root.contains("tenants"))
        {
            if (!root["tenants"].is_sequence()) throw std::invalid_argument("tenants must be a sequence");
            for (const auto& node : root["tenants"])
            {
                if (!node.is_mapping()) throw std::invalid_argument("Tenant must be a mapping");
                TenantOptions tenant;
                tenant.identity = eMQ::ReadYAML<std::string>(node, "identity", {});
                tenant.root = eMQ::ReadYAML<std::string>(node, "root", {});
                auto variable = eMQ::ReadYAML<std::string>(node, "auth_key_env", {});
                auto secret = variable.empty() ? nullptr : std::getenv(variable.c_str());
                if (!secret) throw std::invalid_argument("Tenant authentication key environment variable is missing");
                tenant.authKey = FromHex(secret);
                if (node.contains("shards"))
                {
                    if (!node["shards"].is_sequence()) throw std::invalid_argument("Tenant shards must be a sequence");
                    for (const auto& shard : node["shards"])
                    {
                        auto value = shard.get_value<int64_t>();
                        if (value < 0 || value > UINT32_MAX) throw std::invalid_argument("Invalid tenant shard number");
                        tenant.shards.insert(static_cast<uint32_t>(value));
                    }
                }
                options.tenants.push_back(std::move(tenant));
            }
        }
        return options;
    }
}
