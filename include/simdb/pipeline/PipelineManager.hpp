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
        for (auto& thread : polling_threads_)
        {
            if (auto db_thread = dynamic_cast<pipeline::DatabaseThread*>(thread.get()))
            {
                return db_thread->getAsyncDatabaseAccessor();
            }
        }
        return nullptr;
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

        // Move the DB thread from the front to the back
        std::rotate(polling_threads_.begin(), polling_threads_.begin() + 1, polling_threads_.end());

        for (auto& pipeline : pipelines_)
        {
            auto it = polling_threads_.begin();
            for (auto group : pipeline->getTaskGroups())
            {
                if (it == polling_threads_.end() - 1)
                {
                    throw DBException("Internal logic error while connecting threads and runnables");
                }

                (*it)->addRunnable(group);
                ++it;
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
