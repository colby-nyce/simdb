// <PipelineManager.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/PollingThread.hpp"
#include "simdb/pipeline/DatabaseThread.hpp"
#include "simdb/utils/MTLogger.hpp"

#include <iostream>

namespace simdb::pipeline {

/// This class manages all pipelines and their threads for an AppManager (or unit test).
class PipelineManager
{
public:
    PipelineManager(DatabaseManager* db_mgr, const std::string& pipeline_log_file = "")
        : db_mgr_(db_mgr)
        , pipeline_logger_(pipeline_log_file)
    {
    }

    AsyncDatabaseAccessor* getAsyncDatabaseAccessor()
    {
        checkOpen_();
        for (auto& thread : polling_threads_)
        {
            if (auto db_thread = dynamic_cast<DatabaseThread*>(thread.get()))
            {
                return db_thread->getAsyncDatabaseAccessor();
            }
        }
        return nullptr;
    }

    utils::MTLogger* getPipelineLogger()
    {
        return &pipeline_logger_;
    }

    Pipeline* createPipeline(const std::string& name)
    {
        checkOpen_();
        auto pipeline = std::make_unique<Pipeline>(db_mgr_, name);
        pipelines_.emplace_back(std::move(pipeline));
        return pipelines_.back().get();
    }

    void addPipeline(std::unique_ptr<Pipeline> pipeline)
    {
        checkOpen_();
        pipelines_.emplace_back(std::move(pipeline));
    }

    std::vector<const Pipeline*> getPipelines() const
    {
        checkOpen_();

        std::vector<const Pipeline*> pipelines;
        for (const auto& pipeline : pipelines_)
        {
            pipelines.push_back(pipeline.get());
        }
        return pipelines;
    }

    void openPipelines()
    {
        checkOpen_();
        for (auto& thread : polling_threads_)
        {
            thread->open();
        }
    }

    /// Finalize the given pipeline, assigning its stages to the appropriate threads.
    void finalize(Pipeline* pipeline)
    {
        checkOpen_();
        pipeline->assignStageThreads(polling_threads_);
    }

    /// Use this API to temporarily disable all pipeline tasks.
    /// The pipelines will be re-enabled when the returned object
    /// goes out of scope.
    ///
    /// Note that recursive calls to this method are no-ops. Only
    /// the first call will disable the runnables; nested calls
    /// will return a nullptr.
    std::unique_ptr<ScopedRunnableDisabler> scopedDisableAll(bool disable_threads_too = true)
    {
        if (disabler_active_)
        {
            return nullptr;
        }

        getDisablerRunnables_();
        getDisablerThreads_();

        std::unique_ptr<ScopedRunnableDisabler> disabler;
        if (disable_threads_too)
        {
            disabler.reset(new ScopedRunnableDisabler(this, disabler_runnables_, disabler_threads_));
        }
        else
        {
            disabler.reset(new ScopedRunnableDisabler(this, disabler_runnables_));
        }

        disabler_active_ = true;
        return disabler;
    }

    void postSimLoopTeardown(std::ostream* perf_report = &std::cout)
    {
        checkOpen_();

        auto close_thread = [&](PollingThread* thread)
        {
            thread->close();

            if (perf_report)
            {
                std::ostringstream oss;
                thread->printPerfReport(oss);
                *perf_report << oss.str() << "\n\n";
            }
        };

        auto it = polling_threads_.begin();
        while (it != polling_threads_.end())
        {
            close_thread(it->get());
            ++it;
        }

        bool continue_while;
        do
        {
            continue_while = false;

            it = polling_threads_.begin();
            while (it != polling_threads_.end())
            {
                continue_while |= (*it)->flushRunnables();
                ++it;
            }
        } while (continue_while);

        closed_ = true;
    }

private:
    /// Associated DatabaseManager.
    DatabaseManager* db_mgr_ = nullptr;

    /// Instantiated pipelines.
    std::vector<std::unique_ptr<Pipeline>> pipelines_;

    /// Instantiated threads.
    std::vector<std::unique_ptr<PollingThread>> polling_threads_;

    /// Threads that we give to the ScopedRunnableDisabler.
    std::vector<PollingThread*> disabler_threads_;

    /// Runnables that we give to the ScopedRunnableDisabler.
    std::vector<Runnable*> disabler_runnables_;

    /// Flag saying whether a ScopedRunnableDisabler is active.
    /// Used in order to short-circuit nested disablers.
    bool disabler_active_ = false;

    /// Multi-threaded pipeline logger.
    utils::MTLogger pipeline_logger_;

    void getDisablerThreads_()
    {
        if (!disabler_threads_.empty())
        {
            return;
        }

        for (auto& thread : polling_threads_)
        {
            disabler_threads_.push_back(thread.get());
        }

        // Ensure unique
        auto it = std::unique(disabler_threads_.begin(), disabler_threads_.end());
        if (it != disabler_threads_.end())
        {
            throw DBException("Internal error: duplicate threads found in disabler_threads_");
        }
    }

    void getDisablerRunnables_()
    {
        if (!disabler_runnables_.empty())
        {
            return;
        }

        for (auto& thread : polling_threads_)
        {
            const auto& runnables = thread->getRunnables();
            disabler_runnables_.insert(disabler_runnables_.end(), runnables.begin(), runnables.end());
        }

        // Ensure unique
        auto it = std::unique(disabler_runnables_.begin(), disabler_runnables_.end());
        if (it != disabler_runnables_.end())
        {
            throw DBException("Internal error: duplicate runnables found in disabler_runnables_");
        }
    }

    /// Get a notification when a disabler goes out of scope.
    friend class ScopedRunnableDisabler;
    void onDisablerDestruction_()
    {
        if (!disabler_active_)
        {
            throw DBException(
                "Internal error: no disabler active in onDisablerDestruction_()");
        }
        disabler_active_ = false;
    }

    /// Flag saying whether postSimLoopTeardown() was called.
    bool closed_ = false;

    /// Validate that no APIs are called after closing the pipelines
    void checkOpen_() const
    {
        if (closed_)
        {
            throw DBException("PipelineManager has been closed");
        }
    }
};

/// Defined here so we can avoid circular includes
inline void ScopedRunnableDisabler::notifyPipelineMgrReenabled_()
{
    pipeline_mgr_->onDisablerDestruction_();
}

} // namespace simdb::pipeline
