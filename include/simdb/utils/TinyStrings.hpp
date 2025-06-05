// <TinyStrings.hpp> -*- C++ -*-

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

namespace simdb
{

/// To keep SimDB collection as fast and small as possible, we serialize strings
/// not as actual strings, but as ints. This class is used to map strings to ints,
/// while the SimDB compression/sqlite pipeline will serialize the map to the database
/// throughout simulation.
class TinyStrings
{
public:
    using string_map_t = std::shared_ptr<std::unordered_map<std::string, uint32_t>>;
    using unserialized_string_map_t = std::unordered_map<uint32_t, std::string>;

    static TinyStrings* instance()
    {
        static TinyStrings map;
        return &map;
    }

    uint32_t getStringId(const std::string& s)
    {
        auto iter = map_->find(s);
        if (iter == map_->end())
        {
            uint32_t id = map_->size();
            map_->insert({s, id});
            unserialized_map_.insert({id, s});
            return id;
        }
        else
        {
            return iter->second;
        }
    }

    const unserialized_string_map_t& getUnserializedMap() const
    {
        return unserialized_map_;
    }

    void clearUnserializedMap()
    {
        unserialized_map_.clear();
    }

private:
    TinyStrings() = default;
    string_map_t map_ = std::make_shared<std::unordered_map<std::string, uint32_t>>();
    unserialized_string_map_t unserialized_map_;
};

} // namespace simdb
