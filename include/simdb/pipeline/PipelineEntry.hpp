#pragma once

#include "simdb/pipeline/PipelineConfig.hpp"
#include "simdb/utils/MetaStructs.hpp"
#include "simdb/utils/Compress.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/schema/Blob.hpp"
#include <iostream>

namespace simdb
{

class App;
class DatabaseManager;
class PipelineStage;
class Pipeline;

class PipelineStageObserver
{
public:
    virtual void onEnterStage(const PipelineEntry& entry, size_t stage_idx) = 0;
    virtual void onLeaveStage(const PipelineEntry& entry, size_t stage_idx) = 0;
};

/// Represents a single entry in a processing pipeline.
class PipelineEntry
{
public:
    PipelineEntry(uint64_t tick, std::vector<char>&& bytes, const int app_id)
        : tick_(tick)
        , bytes_(std::move(bytes))
        , app_id_(app_id)
    {
    }

    PipelineEntry() = default;

    PipelineEntry(const PipelineEntry&) = delete;

    PipelineEntry(PipelineEntry&&) = default;

    PipelineEntry& operator=(const PipelineEntry&) = delete;

    PipelineEntry& operator=(PipelineEntry&&) = default;

    const void* getDataPtr() const
    {
        return bytes_.data();
    }

    size_t getNumBytes() const
    {
        return bytes_.size();
    }

    DatabaseManager* getDatabaseManager() const
    {
        return db_mgr_;
    }

    void setDatabaseManager(DatabaseManager* db_mgr)
    {
        db_mgr_ = db_mgr;
    }

    void setStageObservers(const std::vector<PipelineStageObserver*>* stage_observers)
    {
        if (stage_observers_)
        {
            throw DBException("Stage observers are already set for this PipelineEntry.");
        }
        stage_observers_ = stage_observers;
    }

    void setStageFunctions(const std::vector<std::vector<PipelineFunc>>* stage_functions)
    {
        if (stage_functions_)
        {
            throw DBException("Stage functions are already set for this PipelineEntry.");
        }
        stage_functions_ = stage_functions;
    }

    void appendStageFunc(size_t stage_idx, PipelineFunc func)
    {
        extra_stage_functions_.resize(std::max(extra_stage_functions_.size(), stage_idx));
        extra_stage_functions_[stage_idx - 1].push_back(func);
    }

    void runStage(size_t stage_idx)
    {
        auto observer = stage_observers_ ? (*stage_observers_)[stage_idx - 1] : nullptr;
        if (observer)
        {
            observer->onEnterStage(*this, stage_idx);
        }

        for (const auto& func : stage_functions_->at(stage_idx - 1))
        {
            func(*this);
        }

        if (!extra_stage_functions_.empty() && stage_idx - 1 < extra_stage_functions_.size())
        {
            for (const auto& func : extra_stage_functions_[stage_idx - 1])
            {
                func(*this);
            }
        }

        if (observer)
        {
            observer->onLeaveStage(*this, stage_idx);
        }
    }

    void setUserData(const void* user_data)
    {
        user_data_ = user_data;
    }

    const void* getUserData() const
    {
        return user_data_;
    }

    uint64_t getTick() const
    {
        return tick_;
    }

    SqlBlob getBlob() const
    {
        SqlBlob blob;
        blob.data_ptr = getDataPtr();
        blob.num_bytes = getNumBytes();
        return blob;
    }

    bool compressed() const
    {
        return compressed_;
    }

    void compress(CompressionLevel level = CompressionLevel::DEFAULT)
    {
        if (compressed())
        {
            return;
        }

        auto data_ptr = getDataPtr();
        auto num_bytes = getNumBytes();
        if (!data_ptr || num_bytes == 0)
        {
            bytes_.clear();
            return;
        }

        std::vector<char> compressed_data;
        compressData(data_ptr, num_bytes, compressed_data, level);
        std::swap(bytes_, compressed_data);
        compressed_ = true;
    }

    int getAppId() const
    {
        return app_id_;
    }

    int getCommittedDbID() const
    {
        return committed_db_id_;
    }

    void setCommittedDbId(int db_id)
    {
        if (committed_db_id_ != 0)
        {
            throw DBException("Committed database ID is already set");
        }
        committed_db_id_ = db_id;
    }

private:
    uint64_t tick_;
    DatabaseManager* db_mgr_ = nullptr;
    const void* user_data_ = nullptr;
    int committed_db_id_ = 0;
    std::vector<char> bytes_;
    bool compressed_ = false;
    int app_id_ = -1;
    const std::vector<PipelineStageObserver*>* stage_observers_ = nullptr;
    const std::vector<std::vector<PipelineFunc>>* stage_functions_ = nullptr;
    std::vector<std::vector<PipelineFunc>> extra_stage_functions_;
};

} // namespace simdb
