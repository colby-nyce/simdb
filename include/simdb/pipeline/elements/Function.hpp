#pragma once

#include "simdb/utils/Demangle.hpp"
#include <functional>

namespace simdb::pipeline {

/// Function task element.
template <typename FunctionIn, typename FunctionOut>
class Function {};

/// Specialization for non-terminating functions.
template <typename FunctionIn, typename FunctionOut>
class Task<Function<FunctionIn, FunctionOut>> : public NonTerminalNonDatabaseTask<FunctionIn>
{
public:
    using Func = std::function<void(FunctionIn&&, ConcurrentQueue<FunctionOut>&)>;
    Task(Func func) : func_(func) {}

    QueueBase* getOutputQueue() override
    {
        return output_queue_;
    }

    void setOutputQueue(QueueBase* queue) override
    {
        if (auto q = dynamic_cast<Queue<FunctionOut>*>(queue))
        {
            output_queue_ = q;
        }
        else
        {
            throw DBException("Invalid data type");
        }
    }

    bool run() override
    {
        FunctionIn in;
        bool ran = false;
        while (this->input_queue_.get().try_pop(in))
        {
            func_(std::move(in), output_queue_->get());
            ran = true;
        }
        return ran;
    }

private:
    std::string getName_() const override
    {
        return "Function<" + demangle_type<FunctionIn>() + ", " + demangle_type<FunctionOut>() + ">";
    }

    Func func_;
    Queue<FunctionOut>* output_queue_ = nullptr;
};

/// Specialization for terminating functions.
template <typename FunctionIn>
class Task<Function<FunctionIn, void>> : public TerminalNonDatabaseTask<FunctionIn>
{
public:
    using Func = std::function<void(FunctionIn&&)>;
    Task(Func func) : func_(func) {}

    bool run() override
    {
        FunctionIn in;
        bool ran = false;
        while (this->input_queue_.get().try_pop(in))
        {
            func_(std::move(in));
            ran = true;
        }
        return ran;
    }

private:
    std::string getName_() const override
    {
        return "Function<" + demangle_type<FunctionIn>() + ", void>";
    }

    Func func_;
};

} // namespace simdb::pipeline
