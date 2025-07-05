#pragma once

#include <iostream>
#include <string>

namespace simdb::pipeline {

/// Base class for all things that can be run on a pipeline thread.
class Runnable
{
public:
    virtual ~Runnable() = default;

    /// Get the name of this runnable. Used for reporting purposes,
    /// e.g. the pipeline self-profiler report.
    std::string getName() const
    {
        return !name_.empty() ? name_ : getName_();
    }

    /// Set/overwrite the name of this runnable. See getName().
    void setName(const std::string& name)
    {
        name_ = name;
    }

    /// Flush and process everything. Return true if the runnable
    /// actually did anything, false if there was no input data.
    virtual bool run() = 0;

    /// Print info about this runnable for reporting purposes.
    virtual void print(std::ostream& os, int indent) const
    {
        os << std::string(indent, ' ') << getName() << "\n";
    }

private:
    virtual std::string getName_() const = 0;
    std::string name_;
};

} // namespace simdb::pipeline
