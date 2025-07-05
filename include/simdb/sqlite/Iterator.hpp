// <Iterator.hpp> -*- C++ -*-

#pragma once

#include "simdb/sqlite/Transaction.hpp"

#include <sqlite3.h>
#include <string.h>
#include <memory>
#include <string>
#include <vector>

namespace simdb
{

/*!
 * \class ResultWriterBase
 *
 * \brief Base class for all SELECT objects that are responsible for
 *        writing record values to the user's local variables whenever
 *        a query's result set iterator is advanced.
 */
class ResultWriterBase
{
public:
    /// Destroy
    virtual ~ResultWriterBase() = default;

    /// Read the value for the prepared statement at the given column index
    /// and copy it to the user's local variable.
    virtual void writeToUserVar(sqlite3_stmt* stmt, const int idx) const = 0;

    /// Return a new copy of this writer.
    virtual ResultWriterBase* clone() const = 0;

    /// \brief Return the column name for this writer.
    ///
    /// Used when creating the query's prepared statement:
    /// SELECT ColA,ColB FROM Table WHERE Id=44 AND Name='foo'
    const std::string& getColName() const
    {
        return col_name_;
    }

protected:
    ResultWriterBase(const char* col_name)
        : col_name_(col_name)
    {
    }

private:
    std::string col_name_;
};

/*!
 * \class ResultWriterInt32
 *
 * \brief Responsible for writing int32 record values to the user's local 
 *        variables whenever a query's result set iterator is advanced.
 */
class ResultWriterInt32 : public ResultWriterBase
{
public:
    /// \brief Construction
    /// \param col_name Name of the selected column
    /// \param user_var Pointer to the local variable where result values are written to
    ResultWriterInt32(const char* col_name, int32_t* user_var)
        : ResultWriterBase(col_name)
        , user_var_(user_var)
    {
    }

    /// Read the value for the prepared statement at the given column index
    /// and copy it to the user's local variable.
    void writeToUserVar(sqlite3_stmt* stmt, const int idx) const override
    {
        *user_var_ = sqlite3_column_int(stmt, idx);
    }

    /// Return a new copy of this writer.
    ResultWriterBase* clone() const override
    {
        return new ResultWriterInt32(getColName().c_str(), user_var_);
    }

private:
    int32_t* user_var_;
};

/*!
 * \class ResultWriterInt64
 *
 * \brief Responsible for writing int64 record values to the user's local 
 *        variables whenever a query's result set iterator is advanced.
 */
class ResultWriterInt64 : public ResultWriterBase
{
public:
    /// \brief Construction
    /// \param col_name Name of the selected column
    /// \param user_var Pointer to the local variable where result values are written to
    ResultWriterInt64(const char* col_name, int64_t* user_var)
        : ResultWriterBase(col_name)
        , user_var_(user_var)
    {
    }

    /// Read the value for the prepared statement at the given column index
    /// and copy it to the user's local variable.
    void writeToUserVar(sqlite3_stmt* stmt, const int idx) const override
    {
        *user_var_ = sqlite3_column_int64(stmt, idx);
    }

    /// Return a new copy of this writer.
    ResultWriterBase* clone() const override
    {
        return new ResultWriterInt64(getColName().c_str(), user_var_);
    }

private:
    int64_t* user_var_;
};

/*!
 * \class ResultWriterDouble
 *
 * \brief Responsible for writing double record values to the user's local 
 *        variables whenever a query's result set iterator is advanced.
 */
class ResultWriterDouble : public ResultWriterBase
{
public:
    /// \brief Construction
    /// \param col_name Name of the selected column
    /// \param user_var Pointer to the local variable where result values are written to
    ResultWriterDouble(const char* col_name, double* user_var)
        : ResultWriterBase(col_name)
        , user_var_(user_var)
    {
    }

    /// Read the value for the prepared statement at the given column index
    /// and copy it to the user's local variable.
    void writeToUserVar(sqlite3_stmt* stmt, const int idx) const override
    {
        *user_var_ = sqlite3_column_double(stmt, idx);
    }

    /// Return a new copy of this writer.
    ResultWriterBase* clone() const override
    {
        return new ResultWriterDouble(getColName().c_str(), user_var_);
    }

private:
    double* user_var_;
};

/*!
 * \class ResultWriterString
 *
 * \brief Responsible for writing text record values to the user's local 
 *        variables whenever a query's result set iterator is advanced.
 */
class ResultWriterString : public ResultWriterBase
{
public:
    /// \brief Construction
    /// \param col_name Name of the selected column
    /// \param user_var Pointer to the local variable where result values are written to
    ResultWriterString(const char* col_name, std::string* user_var)
        : ResultWriterBase(col_name)
        , user_var_(user_var)
    {
    }

    /// Read the value for the prepared statement at the given column index
    /// and copy it to the user's local variable.
    void writeToUserVar(sqlite3_stmt* stmt, const int idx) const override
    {
        *user_var_ = (const char*)sqlite3_column_text(stmt, idx);
    }

    /// Return a new copy of this writer.
    ResultWriterBase* clone() const override
    {
        return new ResultWriterString(getColName().c_str(), user_var_);
    }

private:
    std::string* user_var_;
};

/*!
 * \class ResultWriterBlob<T>
 *
 * \brief Responsible for writing blob record values to the user's local 
 *        variables whenever a query's result set iterator is advanced.
 */
template <typename T> class ResultWriterBlob : public ResultWriterBase
{
public:
    /// \brief Construction
    /// \param col_name Name of the selected column
    /// \param user_var Pointer to the local variable where result values are written to
    ResultWriterBlob(const char* col_name, std::vector<T>* user_var)
        : ResultWriterBase(col_name)
        , user_var_(user_var)
    {
    }

    /// Read the value for the prepared statement at the given column index
    /// and copy it to the user's local variable.
    void writeToUserVar(sqlite3_stmt* stmt, const int idx) const override
    {
        const void* data = sqlite3_column_blob(stmt, idx);
        const int bytes = sqlite3_column_bytes(stmt, idx);
        user_var_->resize(bytes / sizeof(T));
        memcpy(user_var_->data(), data, bytes);
    }

    /// Return a new copy of this writer.
    ResultWriterBase* clone() const override
    {
        return new ResultWriterBlob(getColName().c_str(), user_var_);
    }

private:
    std::vector<T>* user_var_;
};

/*!
 * \class SqlResultIterator
 *
 * \brief This class is returned by SqlQuery::getResultSet() and
 *        is used to iterate over a query result set.
 */
class SqlResultIterator
{
public:
    /// Construct with a prepared statement and the result writers that read column
    /// values and write them into the user's local variables.
    SqlResultIterator(sqlite3_stmt* stmt, std::vector<std::shared_ptr<ResultWriterBase>>&& result_writers)
        : stmt_(stmt)
        , result_writers_(std::move(result_writers))
    {
    }

    /// Finalize the prepared statement on destruction.
    ~SqlResultIterator()
    {
        if (stmt_)
        {
            sqlite3_finalize(stmt_);
        }
    }

    /// Get the next record, populate the user's local variables,
    /// and return TRUE if the record was found. FALSE is returned
    /// when the entire result set has been iterated over.
    bool getNextRecord()
    {
        auto rc = SQLiteReturnCode(sqlite3_step(stmt_));
        if (rc == SQLITE_ROW)
        {
            for (size_t idx = 0; idx < result_writers_.size(); ++idx)
            {
                result_writers_[idx]->writeToUserVar(stmt_, (int)idx);
            }
            return true;
        }
        else if (rc != SQLITE_DONE)
        {
            throw DBException(sqlite3_errmsg(sqlite3_db_handle(stmt_)));
        }

        return false;
    }

    /// Go back to the beginning of the result set if you need
    /// to iterate over it again.
    void reset()
    {
        if (SQLiteReturnCode(sqlite3_reset(stmt_)))
        {
            throw DBException(sqlite3_errmsg(sqlite3_db_handle(stmt_)));
        }
    }

private:
    /// Prepared statement
    sqlite3_stmt* const stmt_;

    /// Writers that read column values and write them into the user's local variables
    std::vector<std::shared_ptr<ResultWriterBase>> result_writers_;
};

} // namespace simdb
