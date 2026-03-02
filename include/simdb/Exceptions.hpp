// <Exceptions.hpp> -*- C++ -*-

#pragma once

#include <cassert>
#include <exception>
#include <sstream>
#include <string>

namespace simdb {

/*!
 * \class DBException
 *
 * \brief SimDB exception type; stream additional context with operator<< before
 *        throwing. what() returns the full message built from the initial
 *        reason and any appended values.
 */
class DBException : public std::exception
{
public:
    DBException() = default;

    /// \brief Construct with an initial reason string.
    explicit DBException(const std::string& reason) { reason_ << reason; }

    /// \brief Copy constructor; copies the accumulated message.
    DBException(const DBException& rhs) { reason_ << rhs.reason_.str(); }

    virtual ~DBException() noexcept override {}

    /// \brief Override from std::exception; returns the full exception message.
    virtual const char* what() const noexcept override
    {
        reason_str_ = reason_.str();
        return reason_str_.c_str();
    }

    /// \brief Append a value to the exception message (stream-style).
    template <typename T> DBException& operator<<(const T& msg)
    {
        reason_ << msg;
        return *this;
    }

private:
    // The reason/explanation for the exception
    std::stringstream reason_;

    // Need to keep a local copy of the string formed in the
    // string stream for the 'what' call
    mutable std::string reason_str_;
};

/*!
 * \class SafeTransactionSilentException
 *
 * \brief Internal exception signaling that safeTransaction() should retry (e.g.
 *        SQLITE_BUSY, SQLITE_LOCKED). Not propagated out of safeTransaction();
 *        the transaction is retried instead.
 */
class SafeTransactionSilentException : public std::exception
{
public:
    /// \brief Construct with the SQLite return code (used in the message).
    explicit SafeTransactionSilentException(int rc) :
        msg_("The database is locked (return code " + std::to_string(rc) + ")")
    {
    }

    const char* what() const noexcept override { return msg_.c_str(); }

private:
    const std::string msg_;
};

/*!
 * \class InterruptException
 *
 * \brief This exception is used in order to break out of
 *        the worker thread's infinite consumer loop.
 */
class InterruptException : public std::exception
{
public:
    const char* what() const noexcept override { return "Infinite consumer loop has been interrupted"; }

private:
    /// Private constructor. Not to be created by anyone but the
    /// WorkerInterrupt.
    InterruptException() = default;
    friend class WorkerInterrupt;
};

} // namespace simdb
