// <PipelineManager.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/DatabaseThread.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/PipelineSnooper.hpp"
#include "simdb/pipeline/PollingThread.hpp"
#include "simdb/pipeline/ThreadMerger.hpp"
#include "simdb/utils/MTLogger.hpp"

#include <iostream>

namespace simdb {
class App;
}

namespace simdb::pipeline {

/// This class manages all pipelines and their threads for an AppManager (or
/// unit test).
class PipelineManager
{
public:
    PipelineManager(DatabaseManager* db_mgr, const std::string& pipeline_log_file = "")
        : db_mgr_(db_mgr), pipeline_logger_(pipeline_log_file)
    {
    }

    AsyncDatabaseAccessor* getAsyncDatabaseAccessor()
    {
        checkOpen_();
        if (!threads_opened_)
        {
            throw DBException("Cannot access the AsyncDatabaseAccessor before "
                              "calling openPipelines()");
        }
        return async_db_accessor_;
    }

    utils::MTLogger* getPipelineLogger() { return &pipeline_logger_; }

    Pipeline* createPipeline(const std::string& name, const App* app)
    {
        checkOpen_();
        auto pipeline = std::make_unique<Pipeline>(db_mgr_, name, app);
        pipelines_.emplace_back(std::move(pipeline));
        return pipelines_.back().get();
    }

    std::vector<Pipeline*> getPipelines()
    {
        checkOpen_();

        std::vector<Pipeline*> pipelines;
        for (auto& pipeline : pipelines_)
        {
            pipelines.push_back(pipeline.get());
        }
        return pipelines;
    }

    template <typename KeyType, typename SnoopedType>
    std::unique_ptr<PipelineSnooper<KeyType, SnoopedType>> createSnooper()
    {
        return std::make_unique<PipelineSnooper<KeyType, SnoopedType>>(this);
    }

    void minimizeThreads()
    {
        if (thread_merger_)
        {
            throw DBException("You can only call minimizeThreads() method once.");
        }

        thread_merger_ = std::make_unique<ThreadMerger>(pipelines_);
        thread_merger_->mergeAllAppThreads();
    }

    void minimizeThreads(const App* app)
    {
        if (!thread_merger_)
        {
            throw DBException("Cannot merge a single app's pipeline threads");
        }
        thread_merger_->addAppForMerging(app);
    }

    template <typename... Apps> void minimizeThreads(const App* app, Apps&&... rest)
    {
        if (!thread_merger_)
        {
            thread_merger_ = std::make_unique<ThreadMerger>(pipelines_);
        }
        thread_merger_->addAppForMerging(app);
        minimizeThreads(std::forward<Apps>(rest)...);
    }

    void openPipelines()
    {
        checkOpen_();

        if (!thread_merger_)
        {
            thread_merger_ = std::make_unique<ThreadMerger>(pipelines_);
        }
        thread_merger_->performMerge(polling_threads_);

        // Now that all threads are created, give the async DB accessor to all
        // non-DB stages in all pipelines.
        for (auto& thread : polling_threads_)
        {
            if (auto db_thread = dynamic_cast<DatabaseThread*>(thread.get()))
            {
                async_db_accessor_ = db_thread->getAsyncDatabaseAccessor();
                break;
            }
        }

        if (async_db_accessor_)
        {
            for (auto& thread : polling_threads_)
            {
                if (!dynamic_cast<DatabaseThread*>(thread.get()))
                {
                    for (auto runnable : thread->getRunnables())
                    {
                        if (auto stage = dynamic_cast<Stage*>(runnable))
                        {
                            stage->setAsyncDatabaseAccessor_(async_db_accessor_);
                        }
                    }
                }
            }
        }

        // Now open all threads for simulation
        for (auto& thread : polling_threads_)
        {
            thread->open();
        }
        threads_opened_ = true;
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
        } else
        {
            disabler.reset(new ScopedRunnableDisabler(this, disabler_runnables_));
        }

        disabler_active_ = true;
        return disabler;
    }

    void postSimLoopTeardown(std::ostream* perf_report = &std::cout)
    {
        checkOpen_();

        auto close_thread = [&](PollingThread* thread) {
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

    /// Flag used to prevent AsyncDatabaseAccessor from being
    /// accessed until threads are opened/finalized.
    bool threads_opened_ = false;

    /// Cached AsyncDatabaseAccessor for async DB queries.
    AsyncDatabaseAccessor* async_db_accessor_ = nullptr;

    /// Used to perform minimizeThread() to share threads
    /// between concurrently running apps.
    std::unique_ptr<ThreadMerger> thread_merger_;

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
            throw DBException("Internal error: duplicate runnables found in "
                              "disabler_runnables_");
        }
    }

    /// Get a notification when a disabler goes out of scope.
    friend class ScopedRunnableDisabler;
    void onDisablerDestruction_()
    {
        if (!disabler_active_)
        {
            throw DBException("Internal error: no disabler active in "
                              "onDisablerDestruction_()");
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

/// Defined here so we can avoid circular includes
template <typename KeyType, typename SnoopedType>
bool PipelineSnooper<KeyType, SnoopedType>::snoopAllStages(const KeyType& key, SnoopedType& snooped_obj,
                                                           bool disable_pipeline)
{
    std::unique_ptr<ScopedRunnableDisabler> disabler = disable_pipeline ? pipeline_mgr_->scopedDisableAll() : nullptr;

    for (auto& cb : callbacks_)
    {
        if (cb(key, snooped_obj))
        {
            return true;
        }
    }
    return false;
}

} // namespace simdb::pipeline
