#include <eClient/Runtime/Client.h>
#include <eCore/Auxiliaries/YAML.h>
#include <cstdlib>

namespace eMQ
{
    ClientOptions LoadClientOptions(const char* filename)
    {
        auto root = eCore::LoadYAML(filename);
        ClientOptions options;
        options.transport.maxPacket = eCore::ReadYAML(root, "max_packet_bytes", options.transport.maxPacket);
        options.transport.maxQueue = eCore::ReadYAML(root, "max_send_bytes", options.transport.maxQueue);
        options.transport.timeout = eCore::ReadYAML(root, "handshake_timeout_ms", options.transport.timeout);
        options.transport.tls = eCore::ReadYAML(root, "tls", false);
        options.transport.quic = eCore::ReadYAML(root, "quic", false);
        if (options.transport.quic)
        {
            QuicPolicy quic;
            quic.strategy = eCore::ReadYAML<uint8_t>(root, "quic_strategy", MMQP_QUIC_STRATEGY_SEMANTIC);
            if (*quic.strategy == MMQP_QUIC_STRATEGY_BULK)
                quic.bulk = BulkPolicy{eCore::ReadYAML<uint8_t>(root, "bulk_streams", 2),
                    eCore::ReadYAML<uint32_t>(root, "bulk_threshold_bytes", 65536)};
            auto datagram = eCore::ReadYAML<uint32_t>(root, "datagram_threshold_bytes", 0);
            if (datagram) quic.datagram = datagram;
            options.properties.quic = QuicPlan(quic).Policy();
        }
        options.transport.certificate = eCore::ConfigPath(filename, eCore::ReadYAML<std::string>(root, "certificate", {}));
        options.transport.certificateHash = eCore::ReadYAML<std::string>(root, "certificate_hash", {});
        options.transport.privateKey = eCore::ConfigPath(filename, eCore::ReadYAML<std::string>(root, "private_key", {}));
        options.transport.authority = eCore::ConfigPath(filename, eCore::ReadYAML<std::string>(root, "authority", {}));
        options.transport.serverName = eCore::ReadYAML<std::string>(root, "server_name", {});
        options.maxInflight = eCore::ReadYAML(root, "max_inflight", options.maxInflight);
        options.maxProfiles = eCore::ReadYAML(root, "max_profiles", options.maxProfiles);
        options.maxPendingBytes = eCore::ReadYAML(root, "max_pending_bytes", options.maxPendingBytes);
        options.retryInterval = eCore::ReadYAML(root, "retry_interval_ms", options.retryInterval);
        options.operationTimeout = eCore::ReadYAML(root, "operation_timeout_ms", options.operationTimeout);
        options.autoReconnect = eCore::ReadYAML(root, "auto_reconnect", options.autoReconnect);
        options.authIdentity = eCore::ReadYAML<std::string>(root, "auth_identity", {});
        options.reconnectInterval = eCore::ReadYAML(root, "reconnect_interval_ms", options.reconnectInterval);
        if (root.contains("failover_endpoints"))
        {
            if (!root["failover_endpoints"].is_sequence()) throw std::invalid_argument("failover_endpoints must be a sequence");
            for (const auto& node : root["failover_endpoints"])
            {
                if (!node.is_mapping()) throw std::invalid_argument("Failover endpoint must be a mapping");
                options.failoverEndpoints.push_back({eCore::ReadYAML<std::string>(node, "host", "127.0.0.1"),
                    eCore::ReadYAML<uint16_t>(node, "port", 5600)});
            }
        }
        SessionPolicy policy;
        policy.reasons = eCore::ReadYAML(root, "reason_strings", true);
        policy.expiry = EncodeInterval(eCore::ReadYAML<uint32_t>(root, "session_expiry_seconds", 300));
        policy.keepalive = EncodeInterval(eCore::ReadYAML<uint32_t>(root, "keepalive_seconds", 30));
        policy.maxPacket = options.transport.maxPacket;
        options.properties.policy = policy;
        auto variable = eCore::ReadYAML<std::string>(root, "auth_key_env", {});
        if (!variable.empty())
        {
            auto secret = std::getenv(variable.c_str());
            if (!secret) throw std::invalid_argument("Authentication key environment variable is missing");
            options.authKey = FromHex(secret);
        }
        auto payloadVariable = eCore::ReadYAML<std::string>(root, "payload_key_env", {});
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
            options.keyEpoch = eCore::ReadYAML<uint32_t>(root, "key_epoch", 1);
            auto minimum = eCore::ReadYAML(root, "minimum_key_epoch", options.keyEpoch);
            auto maximum = eCore::ReadYAML(root, "maximum_key_epoch", options.keyEpoch);
            auto topic = eCore::ReadYAML<std::string>(root, "key_subtree", {});
            auto epoch = topic.empty() ? std::optional<uint32_t>{} : std::optional(options.keyEpoch);
            auto directory = eCore::ConfigPath(filename, eCore::ReadYAML<std::string>(root, "payload_state_directory", {}));
            options.cipher = std::make_shared<PayloadCipher>(KeyGrant{std::move(key), topic, epoch}, minimum, maximum, 128, 4096, directory);
        }
        return options;
    }
}
