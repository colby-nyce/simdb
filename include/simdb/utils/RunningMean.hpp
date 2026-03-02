// <RunningMean.hpp> -*- C++ -*-

#pragma once

#include <stdint.h>

namespace simdb {

/*!
 * \class RunningMean
 *
 * \brief Computes a running mean of a stream of values using Welford's method.
 *        Values are supplied one at a time via add(); no need to store all
 *        values in memory.
 */
class RunningMean
{
public:
    /// \brief Update the running average with a new value (Welford's method).
    /// \param value Next value to include in the mean.
    void add(double value)
    {
        count_++;
        mean_ += (value - mean_) / count_;
    }

    /// \brief Return the current running average.
    double mean() const { return mean_; }

    /// \brief Return the number of values added so far.
    uint64_t count() const { return count_; }

private:
    double mean_ = 0.0;
    uint64_t count_ = 0;
};

} // namespace simdb
