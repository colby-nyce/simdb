// <Runnable.hpp> -*- C++ -*-

#pragma once

#include <iostream>
#include <string>

namespace simdb::pipeline {

/// Base class for all things that can be run on a pipeline thread.
class Runnable
{
public:
    virtual ~Runnable() = default;

    /// Get this runnable's description.
    std::string getDescription() const
    {
        return !description_.empty() ? description_ : getDescription_();
    }

    /// Set/overwrite the this runnable's description.
    void setDescription(const std::string& desc)
    {
        description_ = desc;
    }

    /// Flush and process everything. Return true if the runnable
    /// actually did anything, false if there was no input data.
    virtual bool run() = 0;

    /// Perform end-of-simulation flush to pipeline. The pipeline
    /// threads have been stopped, so you don't have to worry about
    /// thread safety of your subclass objects.
    virtual bool flushToPipeline()
    {
        bool did_work = false;
        while (run())
        {
            did_work = true;
        }
        return did_work;
    }

    /// Print info about this runnable for reporting purposes.
    virtual void print(std::ostream& os, int indent) const
    {
        os << std::string(indent, ' ') << getDescription() << "\n";
    }

private:
    virtual std::string getDescription_() const = 0;
    std::string description_;
};

} // namespace simdb::pipeline
