// <PipelineStager.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/Timestamps.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"

namespace simdb::collection {

struct Payload
{
    std::shared_ptr<TimePointBase> time_point;
    std::vector<char> bytes;
    bool auto_collected;
};

class PipelineStagerBase
{
public:
    virtual ~PipelineStagerBase() = default;
    virtual void stage(std::vector<char>&& bytes, bool auto_collected) = 0;
};

template <typename TimeT>
class PipelineStager final : public PipelineStagerBase
{
public:
    PipelineStager(Timestamp<TimeT>* timestamp,
                   ConcurrentQueue<Payload>* pipeline_head)
        : timestamp_(timestamp)
        , pipeline_head_(pipeline_head)
    {}

    void stage(std::vector<char>&& bytes, bool auto_collected) override
    {
        Payload payload{timestamp_->snapshot(), std::move(bytes), auto_collected};
        pipeline_head_->emplace(std::move(payload));
    }

private:
    Timestamp<TimeT> *const timestamp_;
    ConcurrentQueue<Payload> *const pipeline_head_;
};

} // namespace simdb::collection
