#pragma once

#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/Exceptions.hpp"

#include <functional>
#include <memory>
#include <set>
#include <vector>

namespace simdb {

class DatabaseManager;

/// Base class for input/output concurrent queues for pipeline elements.
class TransformQueueBase
{
public:
    virtual ~TransformQueueBase() = default;
};

/// Wrapper around a concurrent queue for stage transform I/O.
template <typename T>
class TransformQueue : public TransformQueueBase
{
public:
    simdb::ConcurrentQueue<T>* getQueue() { return &queue_; }
    const simdb::ConcurrentQueue<T>* getQueue() const { return &queue_; }
private:
    simdb::ConcurrentQueue<T> queue_;
};

/// Base class for pipeline stage transforms. Each stage uses its
/// own thread to process one or more of its transforms.
class PipelineTransformBase
{
public:
    virtual ~PipelineTransformBase() = default;
    virtual TransformQueueBase* getInputQueue() = 0;
    virtual void setOutputQueue(TransformQueueBase* output_queue) = 0;
    virtual bool flush() = 0;
};

/// This class lets users implement their own stage functions and
/// have them executed on background threads. This class is templated
/// on input and output data types in order to be generic for pipeline
/// chaining. This also helps performance since SimDB won't have to
/// force the one-size-fits-all use of std::vector<char> etc. which
/// is unnecessary overhead in a lot of cases.
template <typename In, typename Out>
class PipelineTransform : public PipelineTransformBase
{
public:
    using Func = std::function<void(In&, ConcurrentQueue<Out>&)>;
    PipelineTransform(Func func) : func_(func) {}

    TransformQueueBase* getInputQueue() override
    {
        return &input_queue_;
    }

    void setOutputQueue(TransformQueueBase* queue) override
    {
        auto q = dynamic_cast<TransformQueue<Out>*>(queue);
        if (!q)
        {
            throw simdb::DBException("Invalid data type");
        }
        output_queue_ = q;
    }

    bool flush() override
    {
        bool any_flushed = false;
        In in;
        while (input_queue_.getQueue()->try_pop(in))
        {
            func_(in, *output_queue_->getQueue());
            any_flushed = true;
        }
        return any_flushed;
    }

private:
    Func func_;
    TransformQueue<In> input_queue_;
    TransformQueue<Out>* output_queue_ = nullptr;
};

/// Partial specialization for end-of-stage transforms.
template <typename In>
class PipelineTransform<In, void> : public PipelineTransformBase
{
public:
    using Func = std::function<void(In&)>;
    PipelineTransform(Func func) : func_(func) {}

    TransformQueueBase* getInputQueue() override
    {
        return &input_queue_;
    }

    void setOutputQueue(TransformQueueBase*) override
    {
        throw simdb::DBException("Cannot set output queue on the last transform");
    }

    bool flush() override
    {
        bool any_flushed = false;
        In in;
        while (input_queue_.getQueue()->try_pop(in))
        {
            func_(in);
            any_flushed = true;
        }
        return any_flushed;
    }

private:
    Func func_;
    TransformQueue<In> input_queue_;
};

/// Base class for pipeline stages. A "stage" runs on its own thread,
/// may or may not access the database, has a known input data type
/// and (if applicable) an output data type.
class PipelineStageBase
{
public:
    virtual ~PipelineStageBase() = default;
    virtual simdb::DatabaseManager* getDatabaseManager() const = 0;
    virtual std::vector<PipelineTransformBase*> getTransforms() = 0;
    virtual TransformQueueBase* getInputQueue() = 0;
    virtual bool flush() = 0;
};

/// Pipeline stage with known I/O data types. Runs on its own thread
/// and executes the transforms added to it.
template <typename In, typename Out>
class PipelineStage : public PipelineStageBase
{
public:
    PipelineStage(simdb::DatabaseManager* db_mgr = nullptr)
        : db_mgr_(db_mgr)
        , accepting_transforms_{AcceptingTransforms::FIRST, AcceptingTransforms::LAST}
    {}

    template <typename TransformOut>
    void first(std::unique_ptr<PipelineTransform<In, TransformOut>> transform)
    {
        if (!accepting_transforms_.count(AcceptingTransforms::FIRST))
        {
            throw simdb::DBException("Only accepting the first stage transform");
        }
        transforms_.emplace_back(std::move(transform));
        accepting_transforms_ = {AcceptingTransforms::MIDDLE, AcceptingTransforms::LAST};
    }

    template <typename TransformIn, typename TransformOut>
    void then(std::unique_ptr<PipelineTransform<TransformIn, TransformOut>> transform)
    {
        if (!accepting_transforms_.count(AcceptingTransforms::MIDDLE))
        {
            throw simdb::DBException("Only accepting intermediate or final stage transforms");
        }
        transforms_.emplace_back(std::move(transform));
    }

    template <typename TransformIn>
    void last(std::unique_ptr<PipelineTransform<TransformIn, Out>> transform)
    {
        if (!accepting_transforms_.count(AcceptingTransforms::LAST))
        {
            throw simdb::DBException("Only accepting intermediate or final stage transforms");
        }
        transforms_.emplace_back(std::move(transform));
        accepting_transforms_ = {AcceptingTransforms::NONE};
    }

    simdb::DatabaseManager* getDatabaseManager() const override
    {
        return db_mgr_;
    }

    std::vector<PipelineTransformBase*> getTransforms()
    {
        if (!accepting_transforms_.count(AcceptingTransforms::NONE))
        {
            throw simdb::DBException("Cannot access transforms until final");
        }

        std::vector<PipelineTransformBase*> transforms;
        for (auto& transform : transforms_)
        {
            transforms.push_back(transform.get());
        }
        return transforms;
    }

    TransformQueueBase* getInputQueue() override
    {
        return !transforms_.empty() ? transforms_[0]->getInputQueue() : nullptr;
    }

    bool flush() override
    {
        bool any_flushed = false;
        for (auto& transform : transforms_)
        {
            any_flushed |= transform->flush();
        }
        return any_flushed;
    }

private:
    enum class AcceptingTransforms
    {
        FIRST,
        MIDDLE,
        LAST,
        NONE
    };

    simdb::DatabaseManager *const db_mgr_;
    std::set<AcceptingTransforms> accepting_transforms_;
    std::vector<std::unique_ptr<PipelineTransformBase>> transforms_;
};

/// TODO cnyce
class PipelineConfig
{
public:
    void addStage(std::unique_ptr<PipelineStageBase> stage)
    {
        stages_.emplace_back(std::move(stage));
    }

    struct Config {
        size_t num_stages = 0;
        bool needs_db = false;
    };

    Config getConfig()
    {
        Config cfg;
        cfg.num_stages = stages_.size();
        cfg.needs_db = !stages_.empty() ? stages_.back()->getDatabaseManager() != nullptr : false;
        return cfg;
    }

    TransformQueueBase* getInputQueue() const
    {
        return !stages_.empty() ? stages_[0]->getInputQueue() : nullptr;
    }

    std::vector<std::unique_ptr<PipelineStageBase>> releaseStages()
    {
        return std::move(stages_);
    }

private:
    std::vector<std::unique_ptr<PipelineStageBase>> stages_;
}; 

} // namespace simdb
