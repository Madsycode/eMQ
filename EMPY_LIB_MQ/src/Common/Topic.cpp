#include <eMQ/Common/Topic.h>

namespace eMQ
{
    bool ValidUtf8(std::string_view text)
    {
        for (size_t i = 0; i < text.size();)
        {
            auto first = static_cast<uint8_t>(text[i++]);
            if (first == 0) return false;
            if (first < 0x80) continue;
            uint32_t code = 0;
            uint8_t count = 0;
            uint32_t minimum = 0;
            if (first >= 0xC2 && first <= 0xDF) { code = first & 0x1F; count = 1; minimum = 0x80; }
            else if (first >= 0xE0 && first <= 0xEF) { code = first & 0x0F; count = 2; minimum = 0x800; }
            else if (first >= 0xF0 && first <= 0xF4) { code = first & 0x07; count = 3; minimum = 0x10000; }
            else return false;
            if (count > text.size() - i) return false;
            for (uint8_t j = 0; j < count; ++j)
            {
                auto byte = static_cast<uint8_t>(text[i++]);
                if ((byte & 0xC0) != 0x80) return false;
                code = (code << 6) | (byte & 0x3F);
            }
            if (code < minimum || code > 0x10FFFF || (code >= 0xD800 && code <= 0xDFFF))
                return false;
        }
        return true;
    }

    std::string CanonicalTopic(std::string_view topic)
    {
        if (!ValidUtf8(topic))
            throw ProtocolError("Topic is not valid UTF-8", MMQP_REASON_INVALID_TOPIC);
        std::string result;
        for (char value : topic)
        {
            if (value == '/' && (result.empty() || result.back() == '/')) continue;
            result.push_back(value);
        }
        if (!result.empty() && result.back() == '/') result.pop_back();
        if (result.empty() || result.size() > UINT8_MAX)
            throw ProtocolError("Topic length is outside 1..255 bytes", MMQP_REASON_INVALID_TOPIC);
        return result;
    }

    bool HasWildcard(std::string_view topic)
    {
        return topic.find_first_of("+#") != std::string_view::npos;
    }

    TopicPattern ParseTopic(std::string_view topic, bool filter)
    {
        TopicPattern pattern{{}, CanonicalTopic(topic)};
        if (pattern.filter.starts_with(MMQP_SHARED_TOPIC_PREFIX))
        {
            if (!filter)
                throw ProtocolError("Shared prefix is only valid for subscriptions", MMQP_REASON_INVALID_TOPIC);
            auto end = pattern.filter.find('/', 7);
            if (end == std::string::npos || end == 7)
                throw ProtocolError("Missing shared subscription group or filter", MMQP_REASON_INVALID_TOPIC);
            pattern.group = pattern.filter.substr(7, end - 7);
            if (HasWildcard(pattern.group) || pattern.group[0] == '$')
                throw ProtocolError("Invalid shared subscription group", MMQP_REASON_INVALID_TOPIC);
            pattern.filter.erase(0, end + 1);
            if (pattern.filter.starts_with(MMQP_SHARED_TOPIC_PREFIX))
                throw ProtocolError("Nested shared subscription", MMQP_REASON_INVALID_TOPIC);
        }
        for (size_t start = 0; start < pattern.filter.size();)
        {
            auto end = pattern.filter.find('/', start);
            if (end == std::string::npos) end = pattern.filter.size();
            auto level = std::string_view(pattern.filter).substr(start, end - start);
            if (HasWildcard(level) && (!filter || (level != "+" && level != "#")
                || (level == "#" && end != pattern.filter.size())))
                throw ProtocolError("Invalid topic wildcard", MMQP_REASON_INVALID_TOPIC);
            start = end + 1;
        }
        return pattern;
    }

    std::pair<std::string_view, std::string_view> SplitShared(std::string_view filter) noexcept
    {
        constexpr std::string_view prefix = MMQP_SHARED_TOPIC_PREFIX;
        if (!filter.starts_with(prefix)) return {{}, filter};
        auto end = filter.find(MMQP_TOPIC_DELIMETER, prefix.size());
        if (end == std::string_view::npos) return {{}, {}};
        return {filter.substr(prefix.size(), end - prefix.size()), filter.substr(end + 1)};
    }

    bool MatchTopic(std::string_view filter, std::string_view topic) noexcept
    {
        filter = SplitShared(filter).second;
        if (filter.empty() || topic.empty()) return false;
        // Root wildcards never match reserved '$' status topics.
        if (topic.front() == '$' && filter.front() != '$') return false;
        while (true)
        {
            auto filterEnd = filter.find(MMQP_TOPIC_DELIMETER);
            auto level = filter.substr(0, filterEnd);
            if (level == "#") return true;
            auto topicEnd = topic.find(MMQP_TOPIC_DELIMETER);
            if (level != "+" && level != topic.substr(0, topicEnd)) return false;
            if (filterEnd == std::string_view::npos) return topicEnd == std::string_view::npos;
            filter.remove_prefix(filterEnd + 1);
            if (topicEnd == std::string_view::npos) return filter == "#";
            topic.remove_prefix(topicEnd + 1);
        }
    }

    bool TopicMatches(std::string_view filter, std::string_view topic)
    {
        auto pattern = ParseTopic(filter, true).filter;
        return MatchTopic(pattern, ParseTopic(topic).filter);
    }

    uint32_t DecodeInterval(uint8_t value)
    {
        constexpr std::array<uint32_t, 4> bases{1, 60, 3600, 86400};
        auto multiplier = value & 0x3F;
        if (!multiplier) throw ProtocolError("Zero interval multiplier");
        return bases[value >> 6] * multiplier;
    }

    uint8_t EncodeInterval(uint32_t seconds)
    {
        constexpr std::array<uint32_t, 4> bases{1, 60, 3600, 86400};
        for (uint8_t tier = 0; tier < bases.size(); ++tier)
        {
            auto multiplier = seconds / bases[tier];
            if (seconds % bases[tier] == 0 && multiplier && multiplier <= 63)
                return static_cast<uint8_t>((tier << 6) | multiplier);
        }
        throw std::invalid_argument("Interval cannot be represented exactly by TIE");
    }
}
