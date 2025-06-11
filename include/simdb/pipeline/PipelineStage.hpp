#pragma once

#include "simdb/pipeline/PipelineChain.hpp"
#include "simdb/pipeline/PipelineEntry.hpp"
#include "simdb/utils/Thread.hpp"

namespace simdb
{

class PipelineStage
{
public:
    PipelineStage(const PipelineChain& stage_chain)
        : stage_chain_(stage_chain)
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

    void start(double interval_seconds = 0.5)
    {
        if (!processor_)
        {
            processor_ = std::make_unique<Processor>(
                this, input_queue_, output_queue_, stage_chain_, interval_seconds);
        }
    }

    void teardown()
    {
        if (processor_)
        {
            processor_->teardown();
            processor_.reset();
        }
    }

private:
    class Processor : public Thread
    {
    public:
        Processor(PipelineStage* owning_stage,
                  ConcurrentQueue<PipelineEntry>* input_queue,
                  ConcurrentQueue<PipelineEntry>* output_queue,
                  const PipelineChain& stage_chain,
                  double interval_seconds)
            : Thread(interval_seconds * 1000) // Convert seconds to milliseconds
            , owning_stage_(owning_stage)
            , input_queue_(input_queue)
            , output_queue_(output_queue)
            , stage_chain_(stage_chain)
        {
            if (!input_queue_)
            {
                throw DBException("Input queue must be set before starting the processor.");
            }

            startThreadLoop();
        }

        void flush()
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
                    output_queue_->push(std::move(entry));
                }
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

        PipelineStage* owning_stage_;
        ConcurrentQueue<PipelineEntry>* input_queue_;
        ConcurrentQueue<PipelineEntry>* output_queue_;
        PipelineChain stage_chain_;
    };

    ConcurrentQueue<PipelineEntry>* input_queue_ = nullptr;
    ConcurrentQueue<PipelineEntry>* output_queue_ = nullptr;
    PipelineChain stage_chain_;
    std::unique_ptr<Processor> processor_;
};

} // namespace simdb
