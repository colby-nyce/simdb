// <PipelineStager.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/Timestamps.hpp"
#include "simdb/apps/argos/CollectedData.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include <queue>

namespace simdb::collection {

// TODO cnyce: does this need a redesign?
struct Payload
{
    std::shared_ptr<TimePointBase> time_point;
    std::vector<char> bytes;
};

class PipelineStagerBase
{
public:
    virtual ~PipelineStagerBase() = default;
    virtual void stage(CollectedData&& data) = 0;
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

    void stage(CollectedData&& data) override
    {
        assert(data.getCID() != 0);

        auto current_time = timestamp_->snapshot();
        if (waiting_queue_.empty())
        {
            QueueCollectionData entry;
            entry.first = current_time;
            entry.second.emplace_back(std::make_unique<CollectedData>(std::move(data)));
            waiting_queue_.emplace(std::move(entry));
            return;
        }

        if (waiting_queue_.front().first->equals(current_time.get(), true))
        {
            //append existing
            //CollectionDataAtTimePoint& collection = waiting_queue_.front().second;
            //collection.emplace_back(std::make_unique<CollectedData>(std::move(data)));
        }
        else
        {
            //new one
            //CollectionDataAtTimePoint collection({std::make_unique<CollectedData>(std::move(data))});
            //QueueCollectionData entry = std::make_pair(current_time, std::move(collection));
            //waiting_queue_.emplace(std::move(entry));
        }

        //Payload payload{timestamp_->snapshot(), std::move(bytes), auto_collected, false};
        //pipeline_head_->emplace(std::move(payload));
    }

private:
    Timestamp<TimeT> *const timestamp_;
    ConcurrentQueue<Payload> *const pipeline_head_;

    using CollectionDataAtTimePoint = std::vector<std::unique_ptr<CollectedData>>;
    using CollectionTime = std::shared_ptr<TimePointBase>;
    using QueueCollectionData = std::pair<CollectionTime, CollectionDataAtTimePoint>;
    std::queue<QueueCollectionData> waiting_queue_;
};

} // namespace simdb::collection
