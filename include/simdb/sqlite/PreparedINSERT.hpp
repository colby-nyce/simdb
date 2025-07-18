#pragma once

#include "simdb/sqlite/Transaction.hpp"
#include "simdb/sqlite/Connection.hpp"
#include "simdb/schema/SchemaDef.hpp"

namespace simdb {

/// This class is used for high-volume table inserts with reusable prepared statements.
class PreparedINSERT
{
public:
    PreparedINSERT(SQLitePreparedStatement&& stmt, const std::vector<SqlDataType>& col_dtypes, std::shared_ptr<Connection> db_conn)
        : prepared_stmt_(std::move(stmt))
        , stmt_(prepared_stmt_)
        , col_dtypes_(col_dtypes)
        , db_conn_(db_conn)
    {}

    template <typename ColumnT>
    typename std::enable_if<std::is_integral_v<ColumnT> && sizeof(ColumnT) == 64, void>::type
    setColumnValue(uint32_t col_idx, const ColumnT ival)
    {
        if (col_dtypes_.at(col_idx) != SqlDataType::int64_t)
        {
            throw DBException("Column ") << col_idx+1 << " is not an INT64";
        }

        sqlite3_bind_int64(stmt_, (int32_t)col_idx+1, ival);
    }

    template <typename ColumnT>
    typename std::enable_if<std::is_integral_v<ColumnT> && sizeof(ColumnT) < 64, void>::type
    setColumnValue(uint32_t col_idx, const ColumnT ival)
    {
        const auto dtype = col_dtypes_.at(col_idx);
        if (dtype != SqlDataType::int64_t && dtype != SqlDataType::int32_t)
        {
            throw DBException("Column ") << col_idx+1 << " is not an INT";
        }

        sqlite3_bind_int(stmt_, (int32_t)col_idx+1, (int32_t)ival);
    }

    template <typename ColumnT>
    typename std::enable_if<std::is_floating_point_v<ColumnT>, void>::type
    setColumnValue(uint32_t col_idx, const ColumnT dval)
    {
        if (col_dtypes_.at(col_idx) != SqlDataType::double_t)
        {
            throw DBException("Column ") << col_idx+1 << " is not a REAL";
        }

        sqlite3_bind_double(stmt_, (int32_t)col_idx+1, dval);
    }

    template <typename T>
    void setColumnValue(uint32_t col_idx, const std::vector<T>& blobval)
    {
        if (col_dtypes_.at(col_idx) != SqlDataType::blob_t)
        {
            throw DBException("Column ") << col_idx+1 << " is not a BLOB";
        }

        sqlite3_bind_blob(stmt_, (int32_t)col_idx+1, blobval.data(), blobval.size() * sizeof(T), 0);
    }

    void setColumnValue(uint32_t col_idx, const SqlBlob& blobval)
    {
        if (col_dtypes_.at(col_idx) != SqlDataType::blob_t)
        {
            throw DBException("Column ") << col_idx << " is not a BLOB";
        }

        sqlite3_bind_blob(stmt_, (int32_t)col_idx+1, blobval.data_ptr, (int32_t)blobval.num_bytes, 0);
    }

    void setColumnValue(uint32_t col_idx, const std::string& strval)
    {
        setColumnValue(col_idx, strval.c_str());
    }

    void setColumnValue(uint32_t col_idx, const char* strval)
    {
        if (col_dtypes_.at(col_idx) != SqlDataType::string_t)
        {
            throw DBException("Column ") << col_idx << " is not a TEXT";
        }

        sqlite3_bind_text(stmt_, (int32_t)col_idx+1, strval, -1, SQLITE_TRANSIENT);
    }

    int createRecord()
    {
        auto rc = SQLiteReturnCode(sqlite3_step(stmt_));
        if (rc != SQLITE_DONE)
        {
            throw DBException("Could not perform INSERT. Error: ")
                << sqlite3_errmsg(db_conn_->getDatabase());
        }

        sqlite3_reset(stmt_);
        sqlite3_clear_bindings(stmt_);
        return db_conn_->getLastInsertRowId();
    }

private:
    SQLitePreparedStatement prepared_stmt_;
    sqlite3_stmt* stmt_ = nullptr;
    std::vector<SqlDataType> col_dtypes_;
    std::shared_ptr<Connection> db_conn_;
};

} // namespace simdb
