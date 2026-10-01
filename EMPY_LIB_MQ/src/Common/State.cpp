#include <eMQ/Common/State.h>

namespace eMQ
{
    void ProfileTable::Apply(const ProfileRequest& request, size_t limit)
    {
        // Encoding validates the same field rules as the wire decoder.
        Codec::Encode(request, {false, 1024 * 1024, static_cast<uint32_t>(limit)});
        auto next = *this;
        for (auto entry : request.entries)
        {
            auto& aliases = entry.type == MMQP_PROREQ_TYPE_REG ? next.publishers : next.subscribers;
            if (entry.topic.empty()) entry.topic = aliases.Resolve(entry.alias);
            entry.topic = CanonicalTopic(entry.topic);
            auto pattern = ParseTopic(entry.topic, true);
            if (entry.type == MMQP_PROREQ_TYPE_UNS)
            {
                auto removed = std::erase_if(next.entries, [&] (const ProfileEntry& existing)
                {
                    if (existing.type != MMQP_PROREQ_TYPE_SUB) return false;
                    auto previous = ParseTopic(existing.topic, true);
                    return previous.group == pattern.group && (previous.filter == pattern.filter
                        || (!HasWildcard(previous.filter) && TopicMatches(pattern.filter, previous.filter)));
                });
                if (!removed) throw ProtocolError("No matching subscription to remove", MMQP_REASON_INVALID_TOPIC);
                continue;
            }
            if (entry.alias) aliases.Map(entry.alias, entry.topic, true);
            auto existing = std::find_if(next.entries.begin(), next.entries.end(), [&] (const auto& value)
            {
                return value.type == entry.type && value.topic == entry.topic;
            });
            if (existing != next.entries.end()) *existing = std::move(entry);
            else
            {
                if (next.entries.size() >= limit)
                    throw ProtocolError("Session profile budget exhausted", MMQP_REASON_POLICY_VIOLATION);
                next.entries.emplace_back(std::move(entry));
            }
        }
        *this = std::move(next);
    }

    std::optional<ProfileEntry> ProfileTable::Registration(std::string_view topic, uint8_t type) const
    {
        std::optional<ProfileEntry> best;
        size_t specificity = 0;
        for (const auto& entry : entries)
        {
            bool allowed = type == MMQP_PUBREQ_TYPE_RES
                ? entry.type == MMQP_PROREQ_TYPE_SUB && entry.request
                : entry.type == MMQP_PROREQ_TYPE_REG && (type != MMQP_PUBREQ_TYPE_REQ || entry.request);
            if (!allowed || !TopicMatches(entry.topic, topic)) continue;
            auto score = entry.topic.find_first_of("+#");
            if (score == std::string::npos) score = entry.topic.size() + 256;
            if (!best || score > specificity || (score == specificity && entry.topic < best->topic))
            {
                best = entry;
                specificity = score;
            }
        }
        return best;
    }

    std::vector<ProfileEntry> ProfileTable::Subscriptions(std::string_view topic, uint8_t qos, bool responders) const
    {
        std::vector<ProfileEntry> matches;
        for (const auto& entry : entries)
        {
            if (entry.type == MMQP_PROREQ_TYPE_SUB && qos <= entry.qos
                && (!responders || entry.request) && TopicMatches(entry.topic, topic))
                matches.emplace_back(entry);
        }
        return matches;
    }
}
