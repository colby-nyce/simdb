// <PipelineStager.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/Timestamps.hpp"
#include "simdb/apps/argos/CollectedData.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include <queue>

namespace simdb::collection {

using CollectionDataAtTimePoint = std::vector<std::unique_ptr<CollectedData>>;
using CollectionTime = std::shared_ptr<TimePointBase>;
using QueueCollectionData = std::pair<CollectionTime, CollectionDataAtTimePoint>;

class PipelineStagerBase
{
public:
    virtual ~PipelineStagerBase() = default;
    virtual void stage(CollectedData&& data) = 0;
    virtual void sendCollectedDataToPipeline() = 0;
};

template <typename TimeT>
class PipelineStager final : public PipelineStagerBase
{
public:
    PipelineStager(size_t heartbeat,
                   Timestamp<TimeT>* timestamp,
                   ConcurrentQueue<QueueCollectionData>* pipeline_head)
        : heartbeat_(heartbeat)
        , timestamp_(timestamp)
        , pipeline_head_(pipeline_head)
    {}

    void stage(CollectedData&& data) override
    {
        auto cid = data.getCID();
        assert(cid != 0);
        all_known_cids_.insert(cid);

        std::cout << "Stager received data from cid " << cid << " at time " << timestamp_->snapshot()->getTimeAsString() << std::endl;

        auto current_time = timestamp_->snapshot();
        if (!last_stage_time_)
        {
            last_stage_time_ = current_time;
        }
        else if (!last_stage_time_->equals(current_time.get()) && !last_stage_time_->lessThan(current_time.get()))
        {
            throw DBException("Time must be monotonically increasing");
        }

        if (!waiting_queue_.empty() && waiting_queue_.back().first->equals(current_time.get(), true))
        {
            std::cout << "...at the same time step as the back of our queue; appending" << std::endl;
            CollectionDataAtTimePoint& collection = waiting_queue_.back().second;
            collection.emplace_back(std::make_unique<CollectedData>(std::move(data)));
        }
        else
        {
            std::cout << "...at a new time step; creating new entry" << std::endl;
            QueueCollectionData entry;
            entry.first = current_time;
            entry.second.emplace_back(std::make_unique<CollectedData>(std::move(data)));
            waiting_queue_.emplace(std::move(entry));
        }

        std::cout << std::endl;
    }

    void sendCollectedDataToPipeline() override
    {
        while (!waiting_queue_.empty())
        {
            sendToPipeline_(waiting_queue_.front());
            waiting_queue_.pop();
        }
    }

private:
    void sendToPipeline_(QueueCollectionData& collection_at_time)
    {
        std::cout << "\n**********\nSending collection at time " << collection_at_time.first->getTimeAsString() << std::endl;

        QueueCollectionData to_send;
        to_send.first = collection_at_time.first;
        for (auto& data : collection_at_time.second)
        {
            auto cid = data->getCID();
            std::cout << "...looking at the last sent bytes for cid " << cid << std::endl;
            if (auto it = last_sent_bytes_.find(cid); it != last_sent_bytes_.end())
            {
                if (it->second == data->getData())
                {
                    std::cout << "......data is the same as before; skipping" << std::endl;
                    continue;
                }
                else
                {
                    std::cout << "......data is different than before; processing" << std::endl;
                }
            }
            else
            {
                std::cout << "......never sent this one; processing" << std::endl;
            }
            to_send.second.emplace_back(std::move(data));
        }
        std::cout << std::endl;

        auto missing_cids = all_known_cids_;
        std::cout << "We are going to send the following cids (just refreshed counter): ";
        for (auto& data : to_send.second)
        {
            auto cid = data->getCID();
            missing_cids.erase(cid);
            countdowns_to_refresh_[cid] = heartbeat_;
            last_sent_bytes_[cid] = data->getData();
            std::cout << cid << (&data != &to_send.second.back() ? ", " : "");
        }
        std::cout << std::endl;

        std::vector<uint16_t> injected_cids;
        for (auto cid : missing_cids)
        {
            auto it = countdowns_to_refresh_.find(cid);
            if (it == countdowns_to_refresh_.end())
            {
                continue;
            }

            if (--it->second == 0)
            {
                // The CollectedData object will immediately add the uint16_t cid
                // to the underlying buffer. Our last_sent_bytes_ also has the
                // cid at the head of the bytes. That's why we are using the
                // StreamBuffer::append() api below with a uint16_t offset.
                auto injected_data = std::make_unique<CollectedData>(cid);
                const auto& last_sent_bytes = last_sent_bytes_.at(cid);
                const auto src = last_sent_bytes.data() + sizeof(uint16_t);
                const auto src_bytes = last_sent_bytes.size() - sizeof(uint16_t);
                auto& buffer = injected_data->getBuffer();
                buffer.append(src, src_bytes);
                to_send.second.emplace_back(std::move(injected_data));
                it->second = heartbeat_;

                injected_cids.push_back(cid);
            }
        }

        if (!injected_cids.empty())
        {
            std::cout << "The heartbeat counter ran out for the following cids (dumped): ";
            for (size_t i = 0; i < injected_cids.size() - 1; ++i)
            {
                std::cout << injected_cids[i] << ", ";
            }
            std::cout << injected_cids.back() << std::endl;
        }

        if (!to_send.second.empty())
        {
            pipeline_head_->emplace(std::move(to_send));
        }
    }

    const size_t heartbeat_;
    Timestamp<TimeT> *const timestamp_;
    ConcurrentQueue<QueueCollectionData> *const pipeline_head_;
    std::queue<QueueCollectionData> waiting_queue_;
    CollectionTime last_stage_time_;
    CollectionTime last_sent_time_;
    std::set<uint16_t> all_known_cids_;
    std::unordered_map<uint16_t, size_t> countdowns_to_refresh_;
    std::unordered_map<uint16_t, std::vector<char>> last_sent_bytes_;
};

} // namespace simdb::collection
