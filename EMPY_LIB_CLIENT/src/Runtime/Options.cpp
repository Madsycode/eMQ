#include <eClient/Runtime/Client.h>
#include <eMQ/Core/Auxiliaries/YAML.h>
#include <cstdlib>

namespace eMQ
{
    ClientOptions LoadClientOptions(const char* filename)
    {
        auto root = eMQ::LoadYAML(filename);
        ClientOptions options;
        options.transport.maxPacket = eMQ::ReadYAML(root, "max_packet_bytes", options.transport.maxPacket);
        options.transport.maxQueue = eMQ::ReadYAML(root, "max_send_bytes", options.transport.maxQueue);
        options.transport.timeout = eMQ::ReadYAML(root, "handshake_timeout_ms", options.transport.timeout);
        options.transport.tls = eMQ::ReadYAML(root, "tls", false);
        options.transport.quic = eMQ::ReadYAML(root, "quic", false);
        if (options.transport.quic)
        {
            QuicPolicy quic;
            quic.strategy = eMQ::ReadYAML<uint8_t>(root, "quic_strategy", MMQP_QUIC_STRATEGY_SEMANTIC);
            if (*quic.strategy == MMQP_QUIC_STRATEGY_BULK)
                quic.bulk = BulkPolicy{eMQ::ReadYAML<uint8_t>(root, "bulk_streams", 2),
                    eMQ::ReadYAML<uint32_t>(root, "bulk_threshold_bytes", 65536)};
            auto datagram = eMQ::ReadYAML<uint32_t>(root, "datagram_threshold_bytes", 0);
            if (datagram) quic.datagram = datagram;
            options.properties.quic = QuicPlan(quic).Policy();
        }
        options.transport.certificate = eMQ::ConfigPath(filename, eMQ::ReadYAML<std::string>(root, "certificate", {}));
        options.transport.certificateHash = eMQ::ReadYAML<std::string>(root, "certificate_hash", {});
        options.transport.privateKey = eMQ::ConfigPath(filename, eMQ::ReadYAML<std::string>(root, "private_key", {}));
        options.transport.authority = eMQ::ConfigPath(filename, eMQ::ReadYAML<std::string>(root, "authority", {}));
        options.transport.serverName = eMQ::ReadYAML<std::string>(root, "server_name", {});
        options.maxInflight = eMQ::ReadYAML(root, "max_inflight", options.maxInflight);
        options.maxProfiles = eMQ::ReadYAML(root, "max_profiles", options.maxProfiles);
        options.maxPendingBytes = eMQ::ReadYAML(root, "max_pending_bytes", options.maxPendingBytes);
        options.retryInterval = eMQ::ReadYAML(root, "retry_interval_ms", options.retryInterval);
        options.maxRetries = eMQ::ReadYAML(root, "max_retries", options.maxRetries);
        options.operationTimeout = eMQ::ReadYAML(root, "operation_timeout_ms", options.operationTimeout);
        options.autoReconnect = eMQ::ReadYAML(root, "auto_reconnect", options.autoReconnect);
        options.authIdentity = eMQ::ReadYAML<std::string>(root, "auth_identity", {});
        options.allowInsecureAuth = eMQ::ReadYAML(root, "allow_insecure_auth", options.allowInsecureAuth);
        options.reconnectInterval = eMQ::ReadYAML(root, "reconnect_interval_ms", options.reconnectInterval);
        if (root.contains("failover_endpoints"))
        {
            if (!root["failover_endpoints"].is_sequence()) throw std::invalid_argument("failover_endpoints must be a sequence");
            for (const auto& node : root["failover_endpoints"])
            {
                if (!node.is_mapping()) throw std::invalid_argument("Failover endpoint must be a mapping");
                options.failoverEndpoints.push_back({eMQ::ReadYAML<std::string>(node, "host", "127.0.0.1"),
                    eMQ::ReadYAML<uint16_t>(node, "port", 5600)});
            }
        }
        SessionPolicy policy;
        policy.reasons = eMQ::ReadYAML(root, "reason_strings", true);
        policy.expiry = EncodeInterval(eMQ::ReadYAML<uint32_t>(root, "session_expiry_seconds", 300));
        policy.keepalive = EncodeInterval(eMQ::ReadYAML<uint32_t>(root, "keepalive_seconds", 30));
        policy.maxPacket = options.transport.maxPacket;
        options.properties.policy = policy;
        auto variable = eMQ::ReadYAML<std::string>(root, "auth_key_env", {});
        if (!variable.empty())
        {
            auto secret = std::getenv(variable.c_str());
            if (!secret) throw std::invalid_argument("Authentication key environment variable is missing");
            options.authKey = FromHex(secret);
        }
        auto payloadVariable = eMQ::ReadYAML<std::string>(root, "payload_key_env", {});
        if (!payloadVariable.empty())
        {
            if (payloadVariable == variable)
                throw std::invalid_argument("Authentication and payload keys must use separate provisioning");
            auto secret = std::getenv(payloadVariable.c_str());
            if (!secret) throw std::invalid_argument("Payload key environment variable is missing");
            auto key = FromHex(secret);
            if (!options.authKey.empty() && SecureEqual(options.authKey, key))
            {
                SecureErase(key);
                throw std::invalid_argument("Authentication and payload keys must be different");
            }
            options.keyEpoch = eMQ::ReadYAML<uint32_t>(root, "key_epoch", 1);
            options.requireEncryption = eMQ::ReadYAML(root, "require_encryption", options.requireEncryption);
            auto minimum = eMQ::ReadYAML(root, "minimum_key_epoch", options.keyEpoch);
            auto maximum = eMQ::ReadYAML(root, "maximum_key_epoch", options.keyEpoch);
            auto topic = eMQ::ReadYAML<std::string>(root, "key_subtree", {});
            auto epoch = topic.empty() ? std::optional<uint32_t>{} : std::optional(options.keyEpoch);
            auto directory = eMQ::ConfigPath(filename, eMQ::ReadYAML<std::string>(root, "payload_state_directory", {}));
            auto keys = eMQ::ReadYAML<size_t>(root, "payload_key_cache", 128);
            auto replays = eMQ::ReadYAML<size_t>(root, "payload_replay_limit", 4096);
            auto reservation = eMQ::ReadYAML<uint64_t>(root, "payload_nonce_reservation", 1024);
            options.cipher = std::make_shared<PayloadCipher>(KeyGrant{std::move(key), topic, epoch}, minimum, maximum,
                keys, replays, directory, reservation);
        }
        return options;
    }
}
