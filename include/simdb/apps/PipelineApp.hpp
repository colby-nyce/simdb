#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/PipelineConfig.hpp"
#include "simdb/pipeline/PipelineEntry.hpp"
#include "simdb/utils/VectorSerializer.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

namespace simdb
{

class AppPipeline;

/// Base class for SimDB applications that use a pipeline for processing.
class PipelineApp : public App
{
public:
    PipelineApp() = default;

    virtual void configPipeline(PipelineConfig& config) = 0;

    void setPipeline(std::unique_ptr<AppPipeline> pipeline)
    {
        app_pipeline_ = std::move(pipeline);
    }

    void setDatabaseManager(DatabaseManager* db_mgr)
    {
        db_mgr_ = db_mgr;
    }

    DatabaseManager* getDatabaseManager() const
    {
        return db_mgr_;
    }

    void processEntry(PipelineEntry&& entry, bool strict_fifo = true)
    {
        auto retire_stage_idx = app_pipeline_->numStages();
        entry.appendStageFunc(retire_stage_idx, RetireEntry);
        app_pipeline_->processEntry(std::move(entry), strict_fifo);
    }

    PipelineEntry prepareEntry(uint64_t tick, std::vector<char>&& data)
    {
        return PipelineEntry(tick, std::move(data), getAppID_());
    }

    template <typename T>
    PipelineEntry prepareEntry(uint64_t tick, const std::vector<T>& data)
    {
        VectorSerializer<T> serializer = createVectorSerializer<T>(&data);
        return prepareEntry(tick, std::move(serializer));
    }

    template <typename T>
    PipelineEntry prepareEntry(uint64_t tick, VectorSerializer<T>&& serializer)
    {
        std::vector<char> data = serializer.release();
        return prepareEntry(tick, std::move(data));
    }

    template <typename T>
    VectorSerializer<T> createVectorSerializer(const std::vector<T>* initial_data = nullptr, const void* user_data = nullptr)
    {
        std::vector<char> serialized_data;
        reusable_buffers_.try_pop(serialized_data);
        serialized_data.clear();
        return VectorSerializer<T>(std::move(serialized_data), initial_data);
    }

    void teardown() override final
    {
        onPreTeardown_();
        app_pipeline_->teardown();
        onPostTeardown_();
    }

private:
    virtual void onPreTeardown_() {}
    virtual void onPostTeardown_() {}

    static void RetireEntry(PipelineEntry& entry)
    {
        entry.setReusableBuffers(&reusable_buffers_);
        entry.retire();
    }

    static inline ConcurrentQueue<std::vector<char>> reusable_buffers_;
    std::unique_ptr<simdb::AppPipeline> app_pipeline_;
    DatabaseManager* db_mgr_ = nullptr;
};

} // namespace simdb
