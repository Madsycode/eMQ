#pragma once
#include "../Vendors/Yaml.h"
#include <filesystem>
#include <fstream>
#include <limits>
#include <type_traits>

namespace eCore
{
    inline fkyaml::node LoadYAML(const char* filename)
    {
        std::ifstream input(filename);
        if (!input) throw std::runtime_error(std::string("Cannot open configuration: ") + filename);
        auto node = fkyaml::node::deserialize(input);
        if (!node.is_mapping()) throw std::invalid_argument("Configuration must be a YAML mapping");
        return node;
    }

    template <typename _Value>
    inline _Value ReadYAML(const fkyaml::node& node, const char* name, _Value fallback)
    {
        if (!node.contains(name)) return fallback;
        if constexpr (std::is_integral_v<_Value> && !std::is_same_v<_Value, bool>)
        {
            if (!node[name].is_integer()) throw std::invalid_argument(std::string(name) + " must be an integer");
            auto value = node[name].template get_value<int64_t>();
            if constexpr (std::is_unsigned_v<_Value>)
            {
                if (value < 0 || static_cast<uint64_t>(value) > std::numeric_limits<_Value>::max())
                    throw std::out_of_range(std::string(name) + " is outside its supported range");
            }
            else if (value < std::numeric_limits<_Value>::min() || value > std::numeric_limits<_Value>::max())
                throw std::out_of_range(std::string(name) + " is outside its supported range");
            return static_cast<_Value>(value);
        }
        else return node[name].template get_value<_Value>();
    }

    inline std::string ConfigPath(const char* filename, const std::string& value)
    {
        if (value.empty()) return {};
        auto path = std::filesystem::path(value);
        if (path.is_relative()) path = std::filesystem::path(filename).parent_path() / path;
        return std::filesystem::absolute(path).string();
    }
}
