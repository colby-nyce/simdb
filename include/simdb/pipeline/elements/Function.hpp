#pragma once

#include "simdb/utils/Demangle.hpp"
#include <functional>

namespace simdb::pipeline {

/// Function task element.
template <typename FunctionIn, typename FunctionOut>
class Function {};

/// Specialization for non-terminating functions.
template <typename FunctionIn, typename FunctionOut>
class Task<Function<FunctionIn, FunctionOut>> : public NonTerminalNonDatabaseTask<FunctionIn, FunctionOut>
{
public:
    using Func = std::function<void(FunctionIn&&, ConcurrentQueue<FunctionOut>&)>;
    Task(Func func) : func_(func) {}

    using TaskBase::getTypedInputQueue;

    bool run() override
    {
        FunctionIn in;
        bool ran = false;
        while (this->input_queue_.get().try_pop(in))
        {
            func_(std::move(in), this->output_queue_->get());
            ran = true;
        }
        return ran;
    }

private:
    std::string getDescription_() const override
    {
        return "Function<" + demangle_type<FunctionIn>() + ", " + demangle_type<FunctionOut>() + ">";
    }

    Func func_;
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
    std::string getDescription_() const override
    {
        return "Function<" + demangle_type<FunctionIn>() + ", void>";
    }

    Func func_;
};

} // namespace simdb::pipeline
