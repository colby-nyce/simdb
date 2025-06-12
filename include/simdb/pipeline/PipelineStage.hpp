#pragma once

#include "simdb/pipeline/PipelineChain.hpp"
#include "simdb/pipeline/PipelineEntry.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/Thread.hpp"

namespace simdb
{

class PipelineStage
{
public:
    PipelineStage(const PipelineChain& stage_chain)
        : stage_chain_(stage_chain)
    {
        processor_ = std::make_unique<Processor>(this, stage_chain_);
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
        Processor(PipelineStage* owning_stage,
                  const PipelineChain& stage_chain)
            : Thread(500)
            , owning_stage_(owning_stage)
            , stage_chain_(stage_chain)
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
                // First run our default chain.
                stage_chain_(entry);

                // Now run any stage-specific chains
                // that were added to this entry.
                entry.runStageChain(owning_stage_);

                // We may or may not have a downstream stage.
                if (output_queue_)
                {
                    output_queue_->emplace(std::move(entry));
                }
            }
        }

        PipelineStage* owning_stage_;
        ConcurrentQueue<PipelineEntry>* input_queue_ = nullptr;
        ConcurrentQueue<PipelineEntry>* output_queue_ = nullptr;
        PipelineChain stage_chain_;
        DatabaseManager* db_mgr_ = nullptr;
    };

    PipelineChain stage_chain_;
    std::unique_ptr<Processor> processor_;
};

} // namespace simdb
