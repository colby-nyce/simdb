#pragma once

#include "simdb/pipeline/PipelineStage.hpp"

namespace simdb
{

/// Generic pipeline for SimDB applications.
class Pipeline
{
public:
    PipelineStage* addStage()
    {
        auto stage_idx = stages_.size() + 1;
        auto stage = std::make_unique<PipelineStage>(stage_idx);
        stages_.push_back(std::move(stage));
        return stages_.back().get();
    }

    void finalize()
    {
        if (stages_.empty())
        {
            throw DBException("No stages in the pipeline. Add stages before finalizing.");
        }

        // queues_ are all the inputs to each stage
        for (size_t i = 0; i < stages_.size(); ++i)
        {
            auto queue = std::make_unique<ConcurrentQueue<PipelineEntry>>();
            stages_[i]->setInputQueue(queue.get());
            queues_.push_back(std::move(queue));
        }

        // Set the output queues for each stage (last stage has no output queue)
        for (size_t i = 0; i < stages_.size() - 1; ++i)
        {
            stages_[i]->setOutputQueue(queues_[i + 1].get());
        }

        for (auto& stage : stages_)
        {
            stage->start();
        }
    }

    void processEntry(PipelineEntry&& entry, size_t stage_idx = 1)
    {
        if (!queues_.at(stage_idx - 1))
        {
            throw DBException("Pipeline not finalized. Call finalize() before processing entries.");
        }
        queues_[stage_idx - 1]->emplace(std::move(entry));
    }

    void teardown()
    {
        for (auto& stage : stages_)
        {
            stage->teardown();
        }
        stages_.clear();
        queues_.clear();
    }

private:
    std::vector<std::unique_ptr<PipelineStage>> stages_;
    std::vector<std::unique_ptr<ConcurrentQueue<PipelineEntry>>> queues_;
};

class AppPipeline
{
public:
    AppPipeline(std::shared_ptr<Pipeline> pipeline,
                const std::vector<std::vector<PipelineFunc>>& stage_functions,
                const std::vector<PipelineStageObserver*>& stage_observers)
        : pipeline_(pipeline)
        , stage_functions_(stage_functions)
        , stage_observers_(stage_observers)
    {
        if (!pipeline_)
        {
            throw DBException("Pipeline cannot be null.");
        }

        if (stage_functions_.size() != stage_observers_.size())
        {
            throw DBException("Stage functions and observers must have the same size.");
        }

        for (size_t i = 0; i < stage_functions_.size(); ++i)
        {
            if (stage_functions_[i].empty() && stage_observers_[i])
            {
                throw DBException("Cannot observe a stage that has no functions.");
            }
        }

        for (size_t i = 0; i < stage_functions_.size(); ++i)
        {
            if (!stage_functions_[i].empty())
            {
                first_stage_idx_ = i + 1;
                break;
            }
        }
    }

    size_t numStages() const
    {
        return stage_functions_.size();
    }

    void processEntry(PipelineEntry&& entry, bool strict_fifo = true)
    {
        entry.setStageFunctions(&stage_functions_);
        entry.setStageObservers(&stage_observers_);
        pipeline_->processEntry(std::move(entry), strict_fifo ? 1 : first_stage_idx_);
    }

    void teardown()
    {
        pipeline_->teardown();
        pipeline_.reset();
    }

private:
    std::shared_ptr<Pipeline> pipeline_;
    std::vector<std::vector<PipelineFunc>> stage_functions_;
    std::vector<PipelineStageObserver*> stage_observers_;
    size_t first_stage_idx_ = 0;
};

} // namespace simdb
