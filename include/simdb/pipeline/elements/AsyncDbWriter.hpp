#pragma once

#include "simdb/pipeline/Task.hpp"

namespace simdb::pipeline {

/// Async DB writer task element.
template <typename Input, typename Output>
class AsyncDatabaseWriter {};

/// This class is used by pipeline elements to send data to
/// a concurrent queue that is processed asynchronously by
/// the DB thread.
template <typename Input, typename Output>
class Task<AsyncDatabaseWriter<Input, Output>> : public NonTerminalTask<Input, Output>
{
public:
    using Func = std::function<void(Input&&, ConcurrentQueue<Output>&, PreparedINSERT*)>;

private:
    Task(std::unique_ptr<PreparedINSERT> inserter, Func func)
        : inserter_(std::move(inserter))
        , func_(func)
    {}

    /// Not meant to be publicly constructible.
    friend class AsyncDatabaseAccessor;

    /// Processes one item from the queue and returns. Always invoked on
    /// the database thread. 
    bool run() override
    {
        if (!this->output_queue_)
        {
            throw DBException("Output queue not set!");
        }

        bool ran = false;
        Input in;
        if (this->input_queue_->get().try_pop(in))
        {
            func_(std::move(in), this->output_queue_->get(), inserter_.get());
            ran = true;
        }
        return ran;
    }

    std::string getDescription_() const override
    {
        return "AsyncDatabaseWriter<" + demangle_type<Input>() + ", " + demangle_type<Output>() + ">";
    }

    std::unique_ptr<PreparedINSERT> inserter_;
    Func func_;
};

/// Specialization for terminal database writers.
template <typename Input>
class Task<AsyncDatabaseWriter<Input, void>> : public TerminalTask<Input>
{
public:
    using Func = std::function<void(Input&&, PreparedINSERT*)>;

private:
    Task(std::unique_ptr<PreparedINSERT> inserter, Func func)
        : inserter_(std::move(inserter))
        , func_(func)
    {}

    /// Not meant to be publicly constructible.
    friend class AsyncDatabaseAccessor;

    /// Process one item from the queue. Always invoked on the database thread. 
    bool run() override
    {
        bool ran = false;
        Input in;
        if (this->input_queue_->get().try_pop(in))
        {
            func_(std::move(in), inserter_.get());
            ran = true;
        }
        return ran;
    }

    std::string getDescription_() const override
    {
        return "AsyncDatabaseWriter<" + demangle_type<Input>() + ", void>";
    }

    std::unique_ptr<PreparedINSERT> inserter_;
    Func func_;
};

} // namespace simdb::pipeline
