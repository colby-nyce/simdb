// <Runnable.hpp> -*- C++ -*-

#pragma once

#include "simdb/Exceptions.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

namespace simdb::pipeline {

class PipelineManager;
class PollingThread;

/// Various outcomes for each processOne/processAll calls to a runnable:
enum PipelineAction {
    // Return if the runnable (task) pushed any data to its output queue,
    // or otherwise should leave the pipeline tasks greedily executing
    // as normal.
    PROCEED,

    // Return if the runnable had no data to consume or otherwise should
    // tell the pipeline thread to go back to sleep for a bit. Note that
    // ALL runnables on a PollingThread need to return SLEEP in order for
    // the thread to go back to sleep. If any runnable returns PROCEED,
    // the thread will continue processing as normal without sleeping.
    SLEEP
};

/// Base class for all things that can be run on a pipeline thread.
class Runnable {
  public:
    virtual ~Runnable() = default;

    /// Get this runnable's description.
    std::string getDescription() const {
        return !description_.empty() ? description_ : getDescription_();
    }

    /// Set/overwrite the this runnable's description.
    void setDescription(const std::string &desc) { description_ = desc; }

    /// Process one item from the input queue, returning true
    /// if this runnable did anything.
    virtual PipelineAction processOne(bool force) = 0;

    /// Flush and process everything from the input queue,
    /// returning true if this runnable did anything.
    virtual PipelineAction processAll(bool force) = 0;

    /// Print info about this runnable for reporting purposes.
    virtual void print(std::ostream &os, int indent) const {
        os << std::string(indent, ' ') << getDescription() << "\n";
    }

    /// Check if this runnable is enabled.
    bool enabled() const { return enabled_; }

    /// Disable/re-enable this runnable.
    void enable(bool enable = true) { enabled_ = enable; }

  private:
    virtual std::string getDescription_() const = 0;
    std::string description_;
    bool enabled_ = true;
};

/// RAII utility used to disable all runnables while in scope,
/// re-enabling them when going out of scope.
class ScopedRunnableDisabler {
  public:
    ~ScopedRunnableDisabler();

  private:
    ScopedRunnableDisabler(PipelineManager *pipeline_mgr, const std::vector<Runnable *> &runnables,
                           const std::vector<PollingThread *> &polling_threads);

    ScopedRunnableDisabler(PipelineManager *pipeline_mgr, const std::vector<Runnable *> &runnables);

    void notifyPipelineMgrReenabled_();

    PipelineManager *pipeline_mgr_ = nullptr;
    std::vector<Runnable *> disabled_runnables_;
    std::vector<PollingThread *> paused_threads_;
    friend class PipelineManager;
};

} // namespace simdb::pipeline
