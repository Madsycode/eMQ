#pragma once
#include <eCore/API.h>
#include <eMQ/Common/Events.h>
#include <eMQ/Network/Connection.h>
#include "../Security/Payload.h"

namespace eMQ
{
    struct ClientEndpoint
    {
        std::string host = "127.0.0.1";
        uint16_t port = 5600;
    };

    struct ClientOptions
    {
        TransportOptions transport;
        SessionProperties properties;
        Buffer authKey;
        std::string authIdentity;
        size_t maxInflight = 256;
        size_t maxProfiles = 256;
        size_t maxPendingBytes = 4 * 1024 * 1024;
        uint32_t retryInterval = 500;
        uint32_t maxRetries = 5;
        uint32_t operationTimeout = 5000;
        bool allowInsecureAuth = false;
        std::shared_ptr<PayloadCipher> cipher;
        uint32_t keyEpoch = 1;
        bool requireEncryption = true;
        std::vector<ClientEndpoint> failoverEndpoints;
        bool autoReconnect = false;
        uint32_t reconnectInterval = 250;
    };

    EMPY_CLIENT_API ClientOptions LoadClientOptions(const char* filename);

    struct EMPY_CLIENT_API Client : eCore::ImplService
    {
        explicit Client(ClientOptions options = {});
        explicit Client(const char* filename);
        ~Client() override;

        std::future<std::string> AsyncConnect(std::string host, uint16_t port, bool resume = false);
        std::string Connect(std::string host, uint16_t port, bool resume = false);
        void Disconnect(bool graceful = true);
        void Stop();
        bool Connected() const;
        std::string Identifier();

        std::future<Reason> UpdateProfile(std::vector<ProfileEntry> entries);
        std::future<Reason> Register(std::string topic, uint8_t retain = 0, uint32_t expiry = 60,
            uint8_t queue = 10, bool requests = false);
        std::future<Reason> Subscribe(std::string topic, uint8_t qos = 1, bool responder = false);
        std::future<Reason> Unsubscribe(std::string topic);

        std::future<Reason> Publish(std::string topic, Buffer payload, uint8_t qos = 0);
        // Zero selects an unused correlation ID. Supply an explicit ID to match
        // concurrent calls before their Receive events arrive. At QoS 1 the
        // future reports PUBACK; at QoS 0 it reports local send admission.
        // Application responses arrive through Receive.
        std::future<Reason> Request(std::string topic, Buffer payload, uint8_t responses = 1, uint8_t qos = 1,
            uint16_t correlationId = 0);
        std::future<Reason> Respond(std::string topic, uint16_t correlationId, Buffer payload,
            Reason reason = {}, uint8_t qos = 1);
        std::future<Reason> Respond(const PublishRequest& request, Buffer payload, Reason reason = {});
        
        std::future<Reason> Send(PublishRequest publication);

        using eCore::ImplService::On;
        using eCore::ImplService::Off;

        template <typename _Event, typename _Callback>
        eCore::Identt On(std::string topic, _Callback&& callback)
        {
            EMPY_STATIC((std::is_same_v<_Event, Receive> || std::is_same_v<_Event, Profile>));
            auto filter = CanonicalTopic(topic);
            ParseTopic(filter, true);
            auto entity = this->CreateEntt();
            this->template OnEvent<_Event>([filter = std::move(filter), fn = std::forward<_Callback>(callback)]
                (const _Event& event)
            {
                if constexpr (std::is_same_v<_Event, Receive>)
                {
                    if (TopicMatches(filter, event.message.topic)) fn(event);
                }
                else
                {
                    if (filter == event.topic || (!HasWildcard(event.topic) && TopicMatches(filter, event.topic))) fn(event);
                }
            }, entity);
            return entity;
        }

        template <typename _Event>
        void Off(eCore::Identt identt)
        {
            this->template OffEvent<_Event>(identt);
            auto entity = this->ToEntt(identt);
            if (entity) entity.Release();
        }

    private:
        struct Impl;
        std::unique_ptr<Impl> m_Impl;
    };
}
