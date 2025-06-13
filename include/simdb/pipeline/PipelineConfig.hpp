#pragma once

#include <vector>
#include <cstddef>
#include "simdb/Exceptions.hpp"

namespace simdb
{

class PipelineEntry;
using PipelineFunc = void(*)(PipelineEntry&);

class PipelineStageObserver;

/// This class is used by SimDB applications to define app-specific pipelines:
///   - How many async stages you need
///   - What functions to call in each stage
///   - What functions should be called in the main thread
///   - Whether the last stage should have access to the database
class PipelineConfig
{
public:
    class StageConfig
    {
    public:
        StageConfig& operator>>(const PipelineFunc& func)
        {
            functions_.push_back(func);
            return *this;
        }

        StageConfig& operator<<(const PipelineFunc& func)
        {
            functions_.insert(functions_.begin(), func);
            return *this;
        }

        void observe(PipelineStageObserver* observer)
        {
            observer_ = observer;
        }

        PipelineStageObserver* getObserver() const
        {
            return observer_;
        }

        const std::vector<PipelineFunc>& getFunctions() const
        {
            return functions_;
        }

    private:
        std::vector<PipelineFunc> functions_;
        PipelineStageObserver* observer_ = nullptr;
        friend class PipelineConfig;
    };

    StageConfig& asyncStage(size_t stage_idx)
    {
        if (stage_idx == 0)
        {
            throw DBException("Async stage indexes start at 1");
        }
        async_stage_configs_.resize(std::max(async_stage_configs_.size(), stage_idx));
        return async_stage_configs_[stage_idx - 1];
    }

    size_t numAsyncStages() const
    {
        size_t count = 0;
        for (const auto& stage : async_stage_configs_)
        {
            if (!stage.getFunctions().empty())
            {
                ++count;
            }
        }
        return count;
    }

private:
    std::vector<StageConfig> async_stage_configs_;
    friend class StageConfig;
};

} // namespace simdb
