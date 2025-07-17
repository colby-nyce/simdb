// <AsyncDatabaseTask.hpp> -*- C++ -*-

#pragma once

#include <functional>
#include <future>
#include <memory>
#include <string>

namespace simdb {
    class DatabaseManager;
}

namespace simdb::pipeline {

/// Function that is queued by any pipeline thread (or the main
/// thread) and invoked on the dedicated database thread.
using AsyncDbAccessFunc = std::function<void(DatabaseManager*)>;

/// This struct is used to wrap a DB access function and
/// an exception that is suitable for std::future/promise.
struct AsyncDatabaseTask
{
    AsyncDbAccessFunc func;
    std::promise<std::string> exception_reason;

    AsyncDatabaseTask(AsyncDbAccessFunc func) : func(func) {}
    AsyncDatabaseTask() = default;
};

using AsyncDatabaseTaskPtr = std::shared_ptr<AsyncDatabaseTask>;

} // namespace simdb::pipeline
