// <Exceptions.hpp> -*- C++ -*-

#pragma once

#include <cassert>
#include <exception>
#include <sstream>
#include <string>

namespace simdb {

/// Used to construct and throw a standard C++ exception
class DBException : public std::exception {
  public:
    DBException() = default;

    /// Construct a DBException object
    DBException(const std::string &reason) { reason_ << reason; }

    /// Copy construct a DBException object
    DBException(const DBException &rhs) { reason_ << rhs.reason_.str(); }

    /// Destroy!
    virtual ~DBException() noexcept override {}

    /**
     * \brief Overload from std::exception
     * \return Const char * of the exception reason
     */
    virtual const char *what() const noexcept override {
        reason_str_ = reason_.str();
        return reason_str_.c_str();
    }

    /**
     * \brief Append additional information to the message.
     */
    template <typename T> DBException &operator<<(const T &msg) {
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

/// Used in order to signal to safeTransaction() that the transaction
/// must be retried. Since SimDB is multi-threaded, we expect the database
/// to encounter locked tables etc. which should not be thrown out of
/// calls to safeTransaction().
class SafeTransactionSilentException : public std::exception {
  public:
    explicit SafeTransactionSilentException(int rc)
        : msg_("The database is locked (return code " + std::to_string(rc) + ")") {}

    const char *what() const noexcept override { return msg_.c_str(); }

  private:
    const std::string msg_;
};

/*!
 * \class InterruptException
 *
 * \brief This exception is used in order to break out of
 *        the worker thread's infinite consumer loop.
 */
class InterruptException : public std::exception {
  public:
    const char *what() const noexcept override {
        return "Infinite consumer loop has been interrupted";
    }

  private:
    /// Private constructor. Not to be created by anyone but the WorkerInterrupt.
    InterruptException() = default;
    friend class WorkerInterrupt;
};

} // namespace simdb
