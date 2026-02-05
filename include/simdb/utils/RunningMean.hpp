// <RunningMean.hpp> -*- C++ -*-

#pragma once

#include <stdint.h>

namespace simdb {

/// This classes uses Welford's method to compute a running
/// mean of a series of values given one at a time (without
/// requiring all values to be stored in memory).
class RunningMean
{
public:
    /// Update the running average with a new value
    void add(double value)
    {
        // Welford's method to update the mean
        count_++;
        mean_ += (value - mean_) / count_;
    }

    /// Get the current running average
    double mean() const { return mean_; }

    /// Get the number of values added
    uint64_t count() const { return count_; }

private:
    double mean_ = 0.0;
    uint64_t count_ = 0;
};

} // namespace simdb
