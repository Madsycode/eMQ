#pragma once
#include "Stream.h"

namespace eMQ
{
    struct TopicPattern
    {
        std::string group;
        std::string filter;
    };

    EMPY_MQ_API bool ValidUtf8(std::string_view text);
    EMPY_MQ_API std::string CanonicalTopic(std::string_view topic);
    EMPY_MQ_API TopicPattern ParseTopic(std::string_view topic, bool filter = false);
    EMPY_MQ_API bool TopicMatches(std::string_view filter, std::string_view topic);
    EMPY_MQ_API bool HasWildcard(std::string_view topic);
    EMPY_MQ_API uint32_t DecodeInterval(uint8_t value);
    EMPY_MQ_API uint8_t EncodeInterval(uint32_t seconds);
}
