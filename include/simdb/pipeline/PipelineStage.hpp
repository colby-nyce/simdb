#pragma once

#include "simdb/pipeline/PipelineEntry.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/Thread.hpp"

namespace simdb
{

class PipelineStage
{
public:
    explicit PipelineStage(size_t stage_idx)
        : processor_(std::make_unique<Processor>(stage_idx))
    {
    }

    void setInputQueue(ConcurrentQueue<PipelineEntry>* input_queue)
    {
        processor_->setInputQueue(input_queue);
    }

    void setOutputQueue(ConcurrentQueue<PipelineEntry>* output_queue)
    {
        processor_->setOutputQueue(output_queue);
    }

    ConcurrentQueue<PipelineEntry>* getInputQueue() const
    {
        return processor_->getInputQueue();
    }

    ConcurrentQueue<PipelineEntry>* getOutputQueue() const
    {
        return processor_->getOutputQueue();
    }

    void setDatabaseManager(DatabaseManager* db_mgr)
    {
        processor_->setDatabaseManager(db_mgr);
    }

    void start()
    {
        processor_->start();
    }

    void teardown()
    {
        processor_->teardown();
        processor_.reset();
    }

private:
    class Processor : public Thread
    {
    public:
        Processor(size_t stage_idx)
            : Thread(500)
            , stage_idx_(stage_idx)
        {
        }

        void setInputQueue(ConcurrentQueue<PipelineEntry>* input_queue)
        {
            input_queue_ = input_queue;
        }

        void setOutputQueue(ConcurrentQueue<PipelineEntry>* output_queue)
        {
            output_queue_ = output_queue;
        }

        ConcurrentQueue<PipelineEntry>* getInputQueue() const
        {
            return input_queue_;
        }

        ConcurrentQueue<PipelineEntry>* getOutputQueue() const
        {
            return output_queue_;
        }

        void setDatabaseManager(DatabaseManager* db_mgr)
        {
            db_mgr_ = db_mgr;
        }

        void start()
        {
            if (!input_queue_)
            {
                throw DBException("Input queue is not set for the PipelineStage.");
            }

            if (!isRunning())
            {
                startThreadLoop();
            }
        }

        void flush()
        {
            if (db_mgr_)
            {
                db_mgr_->safeTransaction([this]() { flushImpl_(); });
            }
            else
            {
                flushImpl_();
            }
        }

        void teardown()
        {
            flush();
            stopThreadLoop();
        }

    private:
        void onInterval_() override
        {
            flush();
        }

        void flushImpl_()
        {
            PipelineEntry entry;
            while (input_queue_->try_pop(entry))
            {
                entry.setDatabaseManager(db_mgr_);
                entry.runStage(stage_idx_);
                if (output_queue_)
                {
                    output_queue_->emplace(std::move(entry));
                }
            }
        }

        ConcurrentQueue<PipelineEntry>* input_queue_ = nullptr;
        ConcurrentQueue<PipelineEntry>* output_queue_ = nullptr;
        DatabaseManager* db_mgr_ = nullptr;
        size_t stage_idx_ = 0;
    };

    std::unique_ptr<Processor> processor_;
};

} // namespace simdb
