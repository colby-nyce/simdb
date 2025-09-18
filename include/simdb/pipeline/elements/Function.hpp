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
    using Func = std::function<void(FunctionIn&&, ConcurrentQueue<FunctionOut>&, bool)>;
    Task(Func func) : func_(func) {}

    using TaskBase::getTypedInputQueue;

private:
    /// Process one item from the queue.
    bool run(bool force_flush) override
    {
        if (!this->output_queue_)
        {
            throw DBException("Output queue not set!");
        }

        FunctionIn in;
        bool ran = false;
        if (this->input_queue_->get().try_pop(in))
        {
            func_(std::move(in), this->output_queue_->get(), force_flush);
            ran = true;
        }
        return ran;
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
    using Func = std::function<void(FunctionIn&&, bool)>;
    Task(Func func) : func_(func) {}

private:
    /// Process one item from the queue.
    bool run(bool force_flush) override
    {
        FunctionIn in;
        bool ran = false;
        if (this->input_queue_->get().try_pop(in))
        {
            func_(std::move(in), force_flush);
            ran = true;
        }
        return ran;
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
    /// Return true if your function pushed at least one item to the queue
    using Func = std::function<bool(ConcurrentQueue<FunctionOut>&, bool)>;
    Task(Func func) : func_(func) {}

    bool run(bool force_flush) override
    {
        return func_(this->output_queue_->get(), force_flush);
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
