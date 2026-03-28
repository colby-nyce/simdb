// <PipelineStager.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/Timestamps.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"

namespace simdb::collection {

struct Payload
{
    std::unique_ptr<TimePointBase> time_point;
    std::vector<char> bytes;
};

class PipelineStagerBase
{
public:
    virtual ~PipelineStagerBase() = default;
    virtual void stage(std::vector<char>&& bytes) = 0;
    virtual void flush() = 0;
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

    void stage(std::vector<char>&& bytes) override
    {
        // TODO cnyce
        (void)bytes;
    }

    void flush() override
    {
        // TODO cnyce
    }

private:
    Timestamp<TimeT> *const timestamp_;
    ConcurrentQueue<Payload> *const pipeline_head_;
};

} // namespace simdb::collection
