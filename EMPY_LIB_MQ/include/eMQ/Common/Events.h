#pragma once
#include "Message.h"

namespace eMQ
{
    struct Connect
    {
        std::string identifier;
        bool resumed = false;
    };

    struct Disconnect { Reason reason; bool reconnecting = false; };
    struct Receive { PublishRequest message; bool decrypted = false; };
    struct Profile { std::string topic; uint16_t packetId = 0; Reason reason; };
    struct Delivery { uint16_t packetId = 0; Reason reason; };
    struct SessionConnected { std::string identifier; bool resumed = false; };
    struct SessionDisconnected { std::string identifier; std::string reason; };
    struct PublicationRouted { std::string topic; size_t recipients = 0; size_t bytes = 0; };
}
