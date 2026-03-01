#pragma once

#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>

#include "simdb/Exceptions.hpp"

namespace simdb {

/*!
 * \class ThreadSafeLogger
 *
 * \brief Thread-safe logger that serializes writes to an ostream or file. Use
 *        protect() to obtain a Guard; stream into the Guard, then on destruction
 *        the line is written under a lock. Optional "[log] " prefix per line.
 */
class ThreadSafeLogger
{
public:
    /// \brief Construct from an existing ostream (e.g. std::cout, std::cerr).
    /// \param os Output stream to write to (caller keeps ownership).
    /// \param prefix If true, prepend "[log] " to each line.
    explicit ThreadSafeLogger(std::ostream& os, bool prefix = false) :
        out_(&os),
        prefix_(prefix ? "[log] " : "")
    {
    }

    /// \brief Construct from a file path; the logger owns and opens the file.
    /// \param filename Path to the log file.
    /// \param prefix If true, prepend "[log] " to each line.
    /// \throws std::runtime_error if the file cannot be opened.
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

    /*!
     * \class Guard
     *
     * \brief RAII handle that buffers streamed output and flushes it to the
     *        logger (under lock) on destruction. Use via ThreadSafeLogger::protect().
     */
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

        /// \brief Stream a value into this line's buffer.
        template <typename T> Guard& operator<<(T&& value)
        {
            buffer_ << std::forward<T>(value);
            return *this;
        }

        /// \brief Stream an ostream manipulator (e.g. std::endl) into this line's buffer.
        Guard& operator<<(std::ostream& (*manip)(std::ostream&))
        {
            buffer_ << manip;
            return *this;
        }

    private:
        const ThreadSafeLogger& logger_;
        std::ostringstream buffer_;
    };

    /// \brief Return a Guard that buffers one line; when it is destroyed, the line is written under lock.
    Guard protect() const { return Guard(*this); }

private:
    mutable std::mutex mutex_;
    mutable std::unique_ptr<std::ofstream> owned_file_; // only used if file logger
    mutable std::ostream* out_;                         // non-owning or owned via unique_ptr
    const std::string prefix_;
};

} // namespace simdb
