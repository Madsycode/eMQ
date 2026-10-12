#pragma once
#include "Stream.h"
#include <unordered_map>

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
    // Hot-path helpers for already canonical topics: no validation or allocation.
    EMPY_MQ_API bool MatchTopic(std::string_view filter, std::string_view topic) noexcept;
    EMPY_MQ_API std::pair<std::string_view, std::string_view> SplitShared(std::string_view filter) noexcept;
    EMPY_MQ_API bool HasWildcard(std::string_view topic);
    EMPY_MQ_API uint32_t DecodeInterval(uint8_t value);
    EMPY_MQ_API uint8_t EncodeInterval(uint32_t seconds);

    // Level trie over canonical filters ($share prefix already removed). A
    // lookup costs O(levels) instead of a scan over every stored filter. It may
    // report one value several times when several of its filters match.
    template<typename _Type>
    struct TopicTree
    {
        void Insert(std::string_view filter, _Type value)
        {
            auto* node = &m_Root;
            while (true)
            {
                auto end = filter.find(MMQP_TOPIC_DELIMETER);
                auto level = filter.substr(0, end);
                if (level == "#") { node->rest.push_back(std::move(value)); ++m_Count; return; }
                auto& next = level == "+" ? node->plus : node->children[std::string(level)];
                if (!next) next = std::make_unique<Node>();
                node = next.get();
                if (end == std::string_view::npos) break;
                filter.remove_prefix(end + 1);
            }
            node->values.push_back(std::move(value));
            ++m_Count;
        }

        // Removes one occurrence and prunes emptied branches.
        bool Remove(std::string_view filter, const _Type& value)
        {
            bool removed = Erase(m_Root, filter, value);
            m_Count -= removed;
            return removed;
        }

        template<typename _Visit>
        void Match(std::string_view topic, _Visit&& visit) const
        {
            if (topic.empty()) return;
            this->Walk(m_Root, topic, topic.front() == '$', visit);
        }

        void Clear() { m_Root = {}; m_Count = 0; }
        EMPY_INLINE size_t Size() const { return m_Count; }

    private:
        struct Hash
        {
            using is_transparent = void;
            size_t operator()(std::string_view text) const noexcept { return std::hash<std::string_view>{}(text); }
        };

        struct Node
        {
            std::unordered_map<std::string, std::unique_ptr<Node>, Hash, std::equal_to<>> children;
            std::unique_ptr<Node> plus;
            std::vector<_Type> rest;
            std::vector<_Type> values;
        };

        static bool Empty(const Node& node)
        {
            return node.children.empty() && !node.plus && node.rest.empty() && node.values.empty();
        }

        static bool Take(std::vector<_Type>& values, const _Type& value)
        {
            auto found = std::find(values.begin(), values.end(), value);
            if (found == values.end()) return false;
            *found = std::move(values.back());
            values.pop_back();
            return true;
        }

        static bool Erase(Node& node, std::string_view filter, const _Type& value)
        {
            auto end = filter.find(MMQP_TOPIC_DELIMETER);
            auto level = filter.substr(0, end);
            if (level == "#") return Take(node.rest, value);
            std::unique_ptr<Node>* next = nullptr;
            if (level == "+") next = node.plus ? &node.plus : nullptr;
            else if (auto found = node.children.find(level); found != node.children.end()) next = &found->second;
            if (!next) return false;
            bool removed = end == std::string_view::npos ? Take((*next)->values, value)
                : Erase(**next, filter.substr(end + 1), value);
            if (removed && Empty(**next))
            {
                if (level == "+") node.plus.reset();
                else node.children.erase(node.children.find(level));
            }
            return removed;
        }

        template<typename _Visit>
        static void Walk(const Node& node, std::string_view topic, bool reserved, _Visit& visit)
        {
            // '#' also matches its parent level, so "a/#" matches "a".
            if (!reserved) for (const auto& value : node.rest) visit(value);
            auto end = topic.find(MMQP_TOPIC_DELIMETER);
            auto level = topic.substr(0, end);
            auto last = end == std::string_view::npos;
            auto tail = last ? std::string_view{} : topic.substr(end + 1);
            auto Step = [&] (const Node& next)
            {
                if (!last) { Walk(next, tail, false, visit); return; }
                for (const auto& value : next.values) visit(value);
                for (const auto& value : next.rest) visit(value);
            };
            if (auto found = node.children.find(level); found != node.children.end()) Step(*found->second);
            if (node.plus && !reserved) Step(*node.plus);
        }

        Node m_Root;
        size_t m_Count = 0;
    };
}
