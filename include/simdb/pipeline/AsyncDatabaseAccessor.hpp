// <AsyncDatabaseAccessor.hpp> -*- C++ -*-

#pragma once

#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/Exceptions.hpp"

#include <functional>
#include <future>

namespace simdb {
    class DatabaseManager;
}

namespace simdb::pipeline {

class DatabaseThread;
using AsyncDbAccessFunc = std::function<void(DatabaseManager*)>;

struct AsyncDatabaseTask
{
    AsyncDbAccessFunc func;
    std::promise<std::string> exception_reason;

    AsyncDatabaseTask(AsyncDbAccessFunc func) : func(func) {}
    AsyncDatabaseTask() = default;
};

using AsyncDatabaseTaskPtr = std::shared_ptr<AsyncDatabaseTask>;

class AsyncDatabaseAccessQueue
{
public:
    virtual ~AsyncDatabaseAccessQueue() = default;
    virtual void queue(AsyncDatabaseTaskPtr&& func) = 0;
};

class AsyncDatabaseAccessor
{
public:
    void evalAsync(const AsyncDbAccessFunc& func)
    {
        auto task = std::make_shared<AsyncDatabaseTask>(func);
        db_access_queue_->queue(std::move(task));
    }

private:
    AsyncDatabaseAccessor(AsyncDatabaseAccessQueue* db_access_queue)
        : db_access_queue_(db_access_queue)
    {}

    AsyncDatabaseAccessQueue* db_access_queue_ = nullptr;
    friend class DatabaseThread;
};

} // namespace simdb::pipeline
