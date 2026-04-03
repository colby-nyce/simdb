// <PipelineStager.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/Timestamps.hpp"
#include "simdb/apps/argos/CollectedData.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include <queue>

namespace simdb::collection {

using CollectionDataAtTimePoint = std::vector<std::unique_ptr<CollectedData>>;
using EnabledChangedAtTimePoint = std::vector<std::pair<uint16_t, bool>>;
using CollectionTime = std::shared_ptr<TimePointBase>;

struct QueueCollectionData
{
    CollectionTime time_point;
    CollectionDataAtTimePoint collection_data;
    EnabledChangedAtTimePoint enabled_changes;
};

class PipelineStagerBase
{
public:
    virtual ~PipelineStagerBase() = default;
    virtual void stage(CollectedData&& data) = 0;
    virtual void sendCollectedDataToPipeline() = 0;
    virtual void onEnabledChanged(uint16_t cid, bool enabled) = 0;
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

        auto current_time = timestamp_->snapshot();
        if (!last_stage_time_)
        {
            last_stage_time_ = current_time;
        }
        else if (!current_time->lessThan(last_stage_time_.get()))
        {
            last_stage_time_ = current_time;
        }
        else
        {
            throw DBException("Time must be monotonically increasing");
        }

        if (!waiting_queue_.empty() && waiting_queue_.back().time_point->equals(current_time.get(), true))
        {
            CollectionDataAtTimePoint& collection = waiting_queue_.back().collection_data;
            collection.emplace_back(std::make_unique<CollectedData>(std::move(data)));
        }
        else
        {
            QueueCollectionData entry;
            entry.time_point = current_time;
            entry.collection_data.emplace_back(std::make_unique<CollectedData>(std::move(data)));
            waiting_queue_.emplace(std::move(entry));
        }
    }

    void sendCollectedDataToPipeline() override
    {
        while (!waiting_queue_.empty())
        {
            sendToPipeline_(waiting_queue_.front());
            waiting_queue_.pop();
        }
    }

    void onEnabledChanged(uint16_t cid, bool enabled) override
    {
        all_known_cids_.insert(cid);

        auto current_time = timestamp_->snapshot();
        if (!last_stage_time_)
        {
            last_stage_time_ = current_time;
        }
        else if (!current_time->lessThan(last_stage_time_.get()))
        {
            last_stage_time_ = current_time;
        }
        else
        {
            throw DBException("Time must be monotonically increasing");
        }

        if (!waiting_queue_.empty() && waiting_queue_.back().time_point->equals(current_time.get(), true))
        {
            EnabledChangedAtTimePoint& changes = waiting_queue_.back().enabled_changes;
            changes.emplace_back(std::make_pair(cid, enabled));
        }
        else
        {
            QueueCollectionData entry;
            entry.time_point = current_time;
            entry.enabled_changes.emplace_back(std::make_pair(cid, enabled));
            waiting_queue_.emplace(std::move(entry));
        }
    }

private:
    void sendToPipeline_(QueueCollectionData& collection_at_time)
    {
        std::map<uint16_t, std::unique_ptr<CollectedData>> collected_data_by_cid;
        for (auto rit = collection_at_time.collection_data.rbegin();
             rit != collection_at_time.collection_data.rend(); ++rit)
        {
            auto cid = (*rit)->getCID();
            auto& collected_data = collected_data_by_cid[cid];
            if (!collected_data)
            {
                collected_data = std::move(*rit);
            }
        }

        collection_at_time.collection_data.clear();
        for (auto& [cid, collected_data] : collected_data_by_cid)
        {
            collection_at_time.collection_data.emplace_back(std::move(collected_data));
        }

        QueueCollectionData to_send;

        // Take into account whether the collected data has changed
        to_send.time_point = collection_at_time.time_point;
        for (auto& data : collection_at_time.collection_data)
        {
            auto cid = data->getCID();
            if (auto it = last_sent_bytes_.find(cid); it != last_sent_bytes_.end())
            {
                if (it->second == data->getData())
                {
                    continue;
                }
            }
            to_send.collection_data.emplace_back(std::move(data));
        }

        auto missing_cids = all_known_cids_;
        for (auto& data : to_send.collection_data)
        {
            auto cid = data->getCID();
            missing_cids.erase(cid);
            countdowns_to_refresh_[cid] = heartbeat_;
            last_sent_bytes_[cid] = data->getData();
        }

        std::unordered_set<uint16_t> cids_requiring_dump;
        std::unordered_set<uint16_t> cids_requiring_decrement;
        for (auto cid : missing_cids)
        {
            auto it = countdowns_to_refresh_.find(cid);
            if (it == countdowns_to_refresh_.end())
            {
                continue;
            }

            if (it->second == 1 && disabled_cids_.count(cid) == 0)
            {
                cids_requiring_dump.insert(cid);
            }
            else
            {
                cids_requiring_decrement.insert(cid);
            }
        }

        for (auto cid : cids_requiring_decrement)
        {
            auto& count = countdowns_to_refresh_.at(cid);
            assert(count > 0 || disabled_cids_.count(cid) > 0);
            if (count > 0)
            {
                --count;
            }
        }

        for (auto [cid, enabled] : collection_at_time.enabled_changes)
        {
            if (enabled)
            {
                disabled_cids_.erase(cid);
            }
            else
            {
                disabled_cids_.insert(cid);
            }
        }

        if (to_send.collection_data.empty() && cids_requiring_dump.empty())
        {
            return;
        }

        for (auto cid : cids_requiring_dump)
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
            to_send.collection_data.emplace_back(std::move(injected_data));
            countdowns_to_refresh_[cid] = heartbeat_;
        }

        if (!to_send.collection_data.empty())
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
    std::unordered_set<uint16_t> all_known_cids_;
    std::unordered_set<uint16_t> disabled_cids_;
    std::unordered_map<uint16_t, size_t> countdowns_to_refresh_;
    std::unordered_map<uint16_t, std::vector<char>> last_sent_bytes_;
};

} // namespace simdb::collection
