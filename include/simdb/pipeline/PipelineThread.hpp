#pragma once

#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/utils/Thread.hpp"

namespace simdb {

class PipelineThread : public Thread
{
public:
    PipelineThread(const size_t interval_milliseconds = 100)
        : Thread(interval_milliseconds)
    {}

    void addStage(std::unique_ptr<PipelineStageBase> stage)
    {
        stages_.emplace_back(std::move(stage));
    }

    std::vector<PipelineTransformBase*> getTransforms()
    {
        std::vector<PipelineTransformBase*> transforms;
        for (auto& stage : stages_)
        {
            auto stage_transforms = stage->getTransforms();
            transforms.insert(transforms.end(), stage_transforms.begin(), stage_transforms.end());
        }
        return transforms;
    }

    void setDatabaseManager(DatabaseManager* db_mgr)
    {
        db_mgr_ = db_mgr;
    }

    void flush()
    {
        if (db_mgr_)
        {
            db_mgr_->safeTransaction([&]() { flushImpl_(); });
        }
        else
        {
            flushImpl_();
        }
    }

private:
    void onInterval_() override
    {
        flush();
    }

    void flushImpl_()
    {
        for (auto& stage : stages_)
        {
            stage->flush();
        }
    }

    std::vector<std::unique_ptr<PipelineStageBase>> stages_;
    DatabaseManager* db_mgr_ = nullptr;
};

} // namespace simdb
