// <Task.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/Thread.hpp"
#include "simdb/pipeline/Queue.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Demangle.hpp"
#include <memory>

namespace simdb::pipeline {

class QueueBase;

/// Base class for all pipeline tasks.
class TaskBase : public Runnable
{
public:
    virtual QueueBase* getInputQueue() = 0;
    virtual QueueBase* getOutputQueue() = 0;
    virtual void setOutputQueue(QueueBase* q) = 0;
    virtual bool requiresDatabase() const = 0;
    virtual void setDatabaseManager(DatabaseManager*) = 0;
};

/// Base class for all terminal non-database tasks.
class TerminalNonDatabaseTask : public TaskBase
{
public:
    QueueBase* getOutputQueue() override final
    {
        return nullptr;
    }

private:
    void setOutputQueue(QueueBase*) override final
    {
        throw DBException("Cannot set output queue on terminal task");
    }

    bool requiresDatabase() const override final
    {
        return false;
    }

    void setDatabaseManager(DatabaseManager*) override final
    {
        throw DBException("Cannot give database to a non-database task");
    }
};

/// Base class for all non-terminal, non-database tasks.
class NonTerminalNonDatabaseTask : public TaskBase
{
private:
    bool requiresDatabase() const override final
    {
        return false;
    }

    void setDatabaseManager(DatabaseManager*) override final
    {
        throw DBException("Cannot give database to a non-database task");
    }
};

/// Base class for all terminal database tasks.
class TerminalDatabaseTask : public TaskBase
{
public:
    QueueBase* getOutputQueue() override final
    {
        return nullptr;
    }

protected:
    DatabaseManager* getDatabaseManager_() const
    {
        return db_mgr_;
    }

private:
    void setOutputQueue(QueueBase*) override final
    {
        throw DBException("Cannot set output queue on terminal task");
    }

    bool requiresDatabase() const override final
    {
        return true;
    }

    void setDatabaseManager(DatabaseManager* db_mgr) override final
    {
        db_mgr_ = db_mgr;
    }

    DatabaseManager* db_mgr_ = nullptr;
};

/// Base class for all non-terminal database tasks.
class NonTerminalDatabaseTask : public TaskBase
{
protected:
    DatabaseManager* getDatabaseManager_() const
    {
        return db_mgr_;
    }

private:
    bool requiresDatabase() const override final
    {
        return true;
    }

    void setDatabaseManager(DatabaseManager* db_mgr) override final
    {
        db_mgr_ = db_mgr;
    }

    DatabaseManager* db_mgr_ = nullptr;
};

template <typename Element>
class Task;

/// Template the concrete tasks on things like Buffer, Function, Hub,
/// or DatabaseQueue. You can also create your own pipeline element by
/// specializing the Task class similar to Function.hpp, Buffer.hpp,
/// or test/pipeline/elements/CircularBuffer.hpp
///
template <typename Element, typename... Args>
inline std::unique_ptr<Task<Element>> createTask(Args&&... args)
{
    return std::make_unique<Task<Element>>(std::forward<Args>(args)...);
}

} // namespace simdb::pipeline
