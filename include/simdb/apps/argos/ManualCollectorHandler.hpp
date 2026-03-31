// <ManualCollectorHandler.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/PipelineStager.hpp"
#include <vector>

namespace simdb::collection {

class ManualCollectorHandler
{
public:
    ManualCollectorHandler(size_t heartbeat, std::vector<char>&& bytes)
        : heartbeat_(heartbeat)
        , bytes_(std::move(bytes))
    {}

    void setBytes(std::vector<char>&& bytes)
    {
        bytes_ = std::move(bytes);
        counter_ = 0;
    }

    void appendToAutoCollection(const TimePointBase* time_point, std::vector<char>& auto_collected)
    {
        // TODO cnyce: figure out the logic for time_point
        (void)time_point;

        if (counter_++ % heartbeat_ == 0)
        {
            auto_collected.insert(auto_collected.end(), bytes_.begin(), bytes_.end());
        }
    }

    void collectableEnabledAt(std::shared_ptr<TimePointBase> time_point, bool enabled)
    {
        // TODO cnyce: we need a data structure to figure out when to dump in
        // appendToAutoCollection() and when to skip it
        (void)time_point;
        (void)enabled;
    }

private:
    const size_t heartbeat_;
    size_t counter_ = 0;
    std::vector<char> bytes_;
};

} // namespace simdb::collection
