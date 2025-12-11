// <Pipeline.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/Stage.hpp"
#include "simdb/pipeline/QueueRepo.hpp"
#include "simdb/pipeline/Flusher.hpp"

namespace simdb {
    class DatabaseManager;
}

namespace simdb::pipeline {

/// SimDB pipelines are used to create high-performance multi-stage
/// data processors en route to the database. Unlike other pipeline
/// libraries, SimDB enforces move-only semantics for performance.
/// There are no limitations regarding I/O data type changes from
/// one stage/filter/transform to the next.
class Pipeline
{
public:
    Pipeline(DatabaseManager* db_mgr, const std::string& name)
        : db_mgr_(db_mgr)
        , pipeline_name_(name)
    {}

    DatabaseManager* getDatabaseManager() const
    {
        return db_mgr_;
    }

    std::string getName() const
    {
        return pipeline_name_;
    }

    template <typename StageType, typename... Args>
    void addStage(const std::string& name, Args&&... args)
    {
        if (state_ != State::ACCEPTING_STAGES)
        {
            throw DBException("Cannot add stage '" + name + "' to pipeline '" + pipeline_name_ + "'; not accepting stages.");
        }

        auto& stage = stages_[name];
        if (stage)
        {
            throw DBException("Stage '" + name + "' already exists in pipeline '" + pipeline_name_ + "'.");
        }
        stage = std::make_unique<StageType>(name, queue_repo_, std::forward<Args>(args)...);
    }

    void noMoreStages()
    {
        if (state_ != State::ACCEPTING_STAGES)
        {
            throw DBException("Cannot finalize stages for pipeline '" + pipeline_name_ + "'; stage changes already finalized.");
        }
        state_ = State::ACCEPTING_BINDINGS;
    }

    void bind(const std::string& output_port_full_name, const std::string& input_port_full_name)
    {
        if (state_ != State::ACCEPTING_BINDINGS)
        {
            throw DBException("Cannot bind ports for pipeline '" + pipeline_name_ + "'; not accepting bindings.");
        }
        queue_repo_.bind(output_port_full_name, input_port_full_name);
    }

    void noMoreBindings()
    {
        if (state_ != State::ACCEPTING_BINDINGS)
        {
            throw DBException("Cannot finalize bindings for pipeline '" + pipeline_name_ + "'; binding changes already finalized.");
        }
        queue_repo_.finalizeBindings();
        state_ = State::BINDINGS_COMPLETE;
    }

    template <typename T>
    simdb::ConcurrentQueue<T>* getInPortQueue(const std::string& port_full_name)
    {
        return queue_repo_.getInPortQueue<T>(port_full_name);
    }

    void assignStageThreads(std::vector<std::unique_ptr<PollingThread>>& threads)
    {
        if (state_ != State::BINDINGS_COMPLETE)
        {
            throw DBException("Cannot assign stage threads for pipeline '" + pipeline_name_ + "; noMoreBindings() never called");
        }

        queue_repo_.validateQueues();

        for (auto& [stage_name, stage] : stages_)
        {
            stage->assignThread(db_mgr_, threads, async_db_accessor_);
        }

        if (async_db_accessor_)
        {
            for (auto& [stage_name, stage] : stages_)
            {
                // Only give the accessor to non-DB stages
                if (!dynamic_cast<DatabaseStageBase*>(stage.get()))
                {
                    stage->setAsyncDatabaseAccessor_(async_db_accessor_);
                }
            }
        }

        state_ = State::FINALIZED;
    }

    std::unique_ptr<Flusher> createFlusher(const std::vector<std::string>& stage_names)
    {
        std::vector<Stage*> stages;
        for (const auto & name : stage_names)
        {
            auto it = stages_.find(name);
            if (it == stages_.end())
            {
                throw DBException("Stage does not exist: ") << name;
            }
            stages.emplace_back(it->second.get());
        }

        bool has_db_stage = false;
        for (auto stage : stages)
        {
            if (dynamic_cast<DatabaseStageBase*>(stage))
            {
                has_db_stage = true;
            }
        }

        return has_db_stage ?
            std::make_unique<FlusherWithTransaction>(stages, db_mgr_) :
            std::make_unique<Flusher>(stages);
    }

    AsyncDatabaseAccessor* getAsyncDatabaseAccessor() const
    {
        return async_db_accessor_;
    }

private:
    DatabaseManager* db_mgr_ = nullptr;
    std::string pipeline_name_;
    std::unordered_map<std::string, std::unique_ptr<Stage>> stages_;
    QueueRepo queue_repo_;
    AsyncDatabaseAccessor* async_db_accessor_ = nullptr;

    enum class State {
        ACCEPTING_STAGES,
        ACCEPTING_BINDINGS,
        BINDINGS_COMPLETE,
        FINALIZED
    };

    State state_ = State::ACCEPTING_STAGES;
};

} // namespace simdb::pipeline
