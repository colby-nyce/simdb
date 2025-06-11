#pragma once

#include "simdb/pipeline/PipelineStage.hpp"

namespace simdb
{

/// Generic pipeline for SimDB applications.
class Pipeline
{
public:
    PipelineStage* addStage(const PipelineChain& stage_chain = nullptr)
    {
        auto stage = std::make_unique<PipelineStage>(stage_chain);
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
            auto queue = std::make_unique<ConcurrentQueue<PipelineEntryBase>>();
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

    void processEntry(PipelineEntryBase&& entry)
    {
        if (!queues_.at(0))
        {
            throw DBException("Pipeline not finalized. Call finalize() before processing entries.");
        }
        queues_[0]->push(std::move(entry));
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
    std::vector<std::unique_ptr<ConcurrentQueue<PipelineEntryBase>>> queues_;
};

} // namespace simdb
