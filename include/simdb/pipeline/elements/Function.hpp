#pragma once

#include "simdb/pipeline/Task.hpp"
#include <functional>

namespace simdb::pipeline {

/// Function task element.
template <typename FunctionIn, typename FunctionOut>
class Function {};

/// Specialization for non-terminating functions.
template <typename FunctionIn, typename FunctionOut>
class Task<Function<FunctionIn, FunctionOut>> : public NonTerminalTask<FunctionIn, FunctionOut>
{
public:
    using Func = std::function<RunnableOutcome(FunctionIn&&, ConcurrentQueue<FunctionOut>&, bool)>;
    Task(Func func) : func_(func) {}

    using TaskBase::getTypedInputQueue;

private:
    /// Process one item from the queue.
    RunnableOutcome processOne(bool force) override
    {
        if (!this->output_queue_)
        {
            throw DBException("Output queue not set!");
        }

        FunctionIn in;
        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        if (this->input_queue_->get().try_pop(in))
        {
            auto o = func_(std::move(in), this->output_queue_->get(), force);
            if (o == RunnableOutcome::ABORT_FLUSH && !force)
            {
                throw DBException("Cannot issue ABORT_FLUSH when we are not flushing!");
            }
            else if (o == RunnableOutcome::ABORT_FLUSH)
            {
                outcome = RunnableOutcome::ABORT_FLUSH;
            }
            else if (o == RunnableOutcome::DID_WORK)
            {
                outcome = RunnableOutcome::DID_WORK;
            }
        }
        return outcome;
    }

    /// Process all items from the queue.
    RunnableOutcome processAll(bool force) override
    {
        if (!this->output_queue_)
        {
            throw DBException("Output queue not set!");
        }

        FunctionIn in;
        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        while (this->input_queue_->get().try_pop(in))
        {
            auto o = func_(std::move(in), this->output_queue_->get(), force);
            if (o == RunnableOutcome::ABORT_FLUSH && !force)
            {
                throw DBException("Cannot issue ABORT_FLUSH when we are not flushing!");
            }
            else if (o == RunnableOutcome::ABORT_FLUSH)
            {
                outcome = RunnableOutcome::ABORT_FLUSH;
                break;
            }
            else if (o == RunnableOutcome::DID_WORK)
            {
                outcome = RunnableOutcome::DID_WORK;
            }
        }
        return outcome;
    }

    std::string getDescription_() const override
    {
        return "Function<" + demangle_type<FunctionIn>() + ", " + demangle_type<FunctionOut>() + ">";
    }

    Func func_;
};

/// Specialization for terminating functions.
template <typename FunctionIn>
class Task<Function<FunctionIn, void>> : public TerminalTask<FunctionIn>
{
public:
    using Func = std::function<RunnableOutcome(FunctionIn&&, bool)>;
    Task(Func func) : func_(func) {}

private:
    /// Process one item from the queue.
    RunnableOutcome processOne(bool force) override
    {
        FunctionIn in;
        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        if (this->input_queue_->get().try_pop(in))
        {
            auto o = func_(std::move(in), force);
            if (o == RunnableOutcome::ABORT_FLUSH && !force)
            {
                throw DBException("Cannot issue ABORT_FLUSH when we are not flushing!");
            }
            else if (o == RunnableOutcome::ABORT_FLUSH)
            {
                outcome = RunnableOutcome::ABORT_FLUSH;
            }
            else if (o == RunnableOutcome::DID_WORK)
            {
                outcome = RunnableOutcome::DID_WORK;
            }
        }
        return outcome;
    }

    /// Process all items from the queue.
    RunnableOutcome processAll(bool force) override
    {
        FunctionIn in;
        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        while (this->input_queue_->get().try_pop(in))
        {
            auto o = func_(std::move(in), force);
            if (o == RunnableOutcome::ABORT_FLUSH && !force)
            {
                throw DBException("Cannot issue ABORT_FLUSH when we are not flushing!");
            }
            else if (o == RunnableOutcome::ABORT_FLUSH)
            {
                outcome = RunnableOutcome::ABORT_FLUSH;
                break;
            }
            else if (o == RunnableOutcome::DID_WORK)
            {
                outcome = RunnableOutcome::DID_WORK;
            }
        }
        return outcome;
    }

    std::string getDescription_() const override
    {
        return "Function<" + demangle_type<FunctionIn>() + ", void>";
    }

    Func func_;
};

/// Specialization for no-input, non-terminating functions.
template <typename FunctionOut>
class Task<Function<void, FunctionOut>> : public TaskBase
{
public:
    /// Return DID_WORK if your function pushed at least one item to the queue
    using Func = std::function<RunnableOutcome(ConcurrentQueue<FunctionOut>&, bool)>;
    Task(Func func) : func_(func) {}

    RunnableOutcome processOne(bool force) override
    {
        auto outcome = func_(this->output_queue_->get(), force);
        if (outcome == RunnableOutcome::ABORT_FLUSH && !force)
        {
            throw DBException("Cannot issue ABORT_FLUSH when we are not flushing!");
        }
        return outcome;
    }

    RunnableOutcome processAll(bool force) override
    {
        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        while (true)
        {
            auto o = processOne(force);
            if (o == RunnableOutcome::ABORT_FLUSH && !force)
            {
                throw DBException("Cannot issue ABORT_FLUSH when we are not flushing!");
            }
            else if (o == RunnableOutcome::ABORT_FLUSH)
            {
                outcome = RunnableOutcome::ABORT_FLUSH;
                break;
            }
            else if (o == RunnableOutcome::DID_WORK)
            {
                outcome = RunnableOutcome::DID_WORK;
            }
            else
            {
                break;
            }
        }
        return outcome;
    }

protected:
    Queue<FunctionOut>* output_queue_ = nullptr;

private:
    QueueBase* getInputQueue() override final
    {
        return nullptr;
    }

    void setOutputQueue(QueueBase* queue) override final
    {
        if (auto q = dynamic_cast<Queue<FunctionOut>*>(queue))
        {
            this->output_queue_ = q;
        }
        else
        {
            throw DBException("Invalid data type");
        }
    }

    std::string getDescription_() const override
    {
        return "Function<void, " + demangle_type<FunctionOut>() + ">";
    }

    Func func_;
};

} // namespace simdb::pipeline
