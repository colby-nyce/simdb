// <PipelineSnooper.hpp> -*- C++ -*-

#pragma once

#include <functional>
#include <vector>
#include <set>

#include "simdb/Exceptions.hpp"
#include "simdb/pipeline/PipelineSnooper.hpp"

namespace simdb::pipeline {

class Stage;

template <typename KeyType, typename SnoopedType>
class PipelineSnooper
{
public:
    PipelineSnooper(PipelineManager* pipeline_mgr)
        : pipeline_mgr_(pipeline_mgr)
    {
    }

    template <typename StageType>
    void addStage(StageType* stage)
    {
        static_assert(std::is_base_of<Stage, StageType>::value);
        if (!snooped_stages_.insert(stage).second)
        {
            throw DBException("Already snooping stage!");
        }

        auto cb = std::bind(&StageType::snoop, stage, std::placeholders::_1, std::placeholders::_2);
        callbacks_.push_back(cb);
    }

    bool snoopAllStages(const KeyType& key, SnoopedType& snooped_obj, bool disable_pipeline = true)
    {
        std::unique_ptr<ScopedRunnableDisabler> disabler = disable_pipeline ?
            pipeline_mgr_->scopedDisableAll() : nullptr;

        for (auto& cb : callbacks_)
        {
            if (cb(key, snooped_obj))
            {
                return true;
            }
        }
        return false;
    }

private:
    using Callback = std::function<bool(const KeyType&, SnoopedType&)>;
    std::vector<Callback> callbacks_;
    std::set<Stage*> snooped_stages_;
    PipelineManager* pipeline_mgr_ = nullptr;
};

} // namespace simdb::pipeline
