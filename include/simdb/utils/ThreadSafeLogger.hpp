#pragma once

#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>

#include "simdb/Exceptions.hpp"

namespace simdb {

class ThreadSafeLogger
{
public:
    // Construct from existing ostream (cout, cerr, custom stream)
    explicit ThreadSafeLogger(std::ostream& os, bool prefix = false) :
        out_(&os),
        prefix_(prefix ? "[log] " : "")
    {
    }

    // Construct from file name (logger owns the file)
    explicit ThreadSafeLogger(const std::string& filename, bool prefix = false) :
        owned_file_(std::make_unique<std::ofstream>(filename)),
        out_(owned_file_.get()),
        prefix_(prefix ? "[log] " : "")
    {
        if (!*owned_file_)
        {
            throw std::runtime_error("Failed to open log file");
        }
    }

    class Guard
    {
    public:
        explicit Guard(const ThreadSafeLogger& logger) :
            logger_(logger)
        {
        }

        ~Guard()
        {
            std::lock_guard<std::mutex> lock(logger_.mutex_);
            (*logger_.out_) << logger_.prefix_ << buffer_.str();
            logger_.out_->flush();
        }

        template <typename T> Guard& operator<<(T&& value)
        {
            buffer_ << std::forward<T>(value);
            return *this;
        }

        Guard& operator<<(std::ostream& (*manip)(std::ostream&))
        {
            buffer_ << manip;
            return *this;
        }

    private:
        const ThreadSafeLogger& logger_;
        std::ostringstream buffer_;
    };

    Guard protect() const { return Guard(*this); }

private:
    mutable std::mutex mutex_;
    mutable std::unique_ptr<std::ofstream> owned_file_; // only used if file logger
    mutable std::ostream* out_;                         // non-owning or owned via unique_ptr
    const std::string prefix_;
};

} // namespace simdb
