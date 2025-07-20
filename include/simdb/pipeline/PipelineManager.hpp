// <PipelineManager.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/PollingThread.hpp"
#include "simdb/pipeline/DatabaseThread.hpp"

#include <iostream>

namespace simdb::pipeline {

/// This class manages all pipelines and their threads for an AppManager (or unit test).
class PipelineManager
{
public:
    PipelineManager(DatabaseManager* db_mgr)
    {
        polling_threads_.emplace_back(std::make_unique<pipeline::DatabaseThread>(db_mgr));
    }

    AsyncDatabaseAccessor* getAsyncDatabaseAccessor()
    {
        checkOpen_();
        auto db_thread = dynamic_cast<pipeline::DatabaseThread*>(polling_threads_.front().get());
        return db_thread->getAsyncDatabaseAccessor();
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

        // The number of non-database processing threads we need is equal to
        // the max number of TaskGroups across all our pipelines.
        size_t num_proc_threads = 0;
        for (auto& pipeline : pipelines_)
        {
            num_proc_threads = std::max(num_proc_threads, pipeline->getTaskGroups().size());
        }

        for (size_t i = 0; i < num_proc_threads; ++i)
        {
            polling_threads_.emplace_back(std::make_unique<pipeline::PollingThread>());
        }

        for (auto& pipeline : pipelines_)
        {
            auto thread_it = polling_threads_.begin() + 1; // Advance over database thread
            for (auto group : pipeline->getTaskGroups())
            {
                if (thread_it == polling_threads_.end())
                {
                    throw DBException("Internal logic error while connecting threads and runnables");
                }

                (*thread_it)->addRunnable(group);
                ++thread_it;
            }
        }

        for (auto& thread : polling_threads_)
        {
            thread->open();
        }
    }

    void postSimLoopTeardown(std::ostream* perf_report = &std::cout)
    {
        checkOpen_();

        for (auto& thread : polling_threads_)
        {
            thread->close();

            if (perf_report)
            {
                std::ostringstream oss;
                thread->printPerfReport(oss);
                *perf_report << oss.str() << "\n\n";
            }
        }

        bool continue_while;
        do
        {
            continue_while = false;
            for (auto& thread : polling_threads_)
            {
                continue_while |= thread->flushRunnablesToPipelines();
            }
        } while (continue_while);

        pipelines_.clear();
        polling_threads_.clear();
        closed_ = true;
    }

private:
    /// Instantiated pipelines.
    std::vector<std::unique_ptr<pipeline::Pipeline>> pipelines_;

    /// Instantiated threads.
    std::vector<std::unique_ptr<pipeline::PollingThread>> polling_threads_;

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

} // namespace simdb::pipeline
