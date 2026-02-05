#pragma once

#include "simdb/schema/Blob.hpp"
#include "simdb/schema/SchemaDef.hpp"
#include "simdb/sqlite/Connection.hpp"
#include "simdb/sqlite/Transaction.hpp"
#include "simdb/utils/utf16.hpp"

namespace simdb {

/// This class is used for high-volume table inserts with reusable prepared statements.
class PreparedINSERT {
  public:
    PreparedINSERT(SQLitePreparedStatement &&stmt, const std::vector<SqlDataType> &col_dtypes,
                   std::shared_ptr<Connection> db_conn)
        : prepared_stmt_(std::move(stmt)), stmt_(prepared_stmt_), col_dtypes_(col_dtypes),
          db_conn_(db_conn) {}

    /// @brief Set the value for the given column index.
    /// @param col_idx 0-based column index.
    /// @param ival Value to set for the column.
    void setColumnValue(uint32_t col_idx, const int32_t ival) {
        if (col_dtypes_.at(col_idx) != SqlDataType::int32_t) {
            throw DBException("Column ") << col_idx + 1 << " is not an int32_t";
        }

        sqlite3_bind_int(stmt_, (int32_t)col_idx + 1, ival);
    }

    /// @brief Set the value for the given column index.
    /// @param col_idx 0-based column index.
    /// @param ival Value to set for the column.
    void setColumnValue(uint32_t col_idx, const uint32_t ival) {
        if (col_dtypes_.at(col_idx) != SqlDataType::uint32_t) {
            throw DBException("Column ") << col_idx + 1 << " is not a uint32_t";
        }

        sqlite3_bind_int64(stmt_, (int32_t)col_idx + 1, static_cast<sqlite3_int64>(ival));
    }

    /// @brief Set the value for the given column index.
    /// @param col_idx 0-based column index.
    /// @param ival Value to set for the column.
    void setColumnValue(uint32_t col_idx, const int64_t ival) {
        if (col_dtypes_.at(col_idx) != SqlDataType::int64_t) {
            throw DBException("Column ") << col_idx + 1 << " is not an int64_t";
        }

        sqlite3_bind_int64(stmt_, (int32_t)col_idx + 1, ival);
    }

    /// @brief Set the value for the given column index.
    /// @param col_idx 0-based column index.
    /// @param ival Value to set for the column.
    void setColumnValue(uint32_t col_idx, const uint64_t ival) {
        if (col_dtypes_.at(col_idx) != SqlDataType::uint64_t) {
            throw DBException("Column ") << col_idx + 1 << " is not a uint64_t";
        }

        auto u16 = utils::uint64_to_utf16(ival);
        sqlite3_bind_text16(stmt_, (int32_t)col_idx + 1, u16.data(), 40, SQLITE_TRANSIENT);
    }

    /// @brief Set the value for the given column index.
    /// @tparam ColumnT double or float
    /// @param col_idx 0-based column index.
    /// @param dval Value to set for the column.
    template <typename ColumnT>
    typename std::enable_if<std::is_floating_point_v<ColumnT>, void>::type
    setColumnValue(uint32_t col_idx, const ColumnT dval) {
        if (col_dtypes_.at(col_idx) != SqlDataType::double_t) {
            throw DBException("Column ") << col_idx + 1 << " is not a double";
        }

        sqlite3_bind_double(stmt_, (int32_t)col_idx + 1, dval);
    }

    /// @brief Set the value for the given column index.
    /// @tparam T blob element type
    /// @param col_idx 0-based column index.
    /// @param blobval Value to set for the column.
    template <typename T> void setColumnValue(uint32_t col_idx, const std::vector<T> &blobval) {
        if (col_dtypes_.at(col_idx) != SqlDataType::blob_t) {
            throw DBException("Column ") << col_idx + 1 << " is not a BLOB";
        }

        sqlite3_bind_blob(stmt_, (int32_t)col_idx + 1, blobval.data(), blobval.size() * sizeof(T),
                          0);
    }

    /// @brief Set the value for the given column index.
    /// @param col_idx 0-based column index.
    /// @param blobval Value to set for the column.
    void setColumnValue(uint32_t col_idx, const SqlBlob &blobval) {
        if (col_dtypes_.at(col_idx) != SqlDataType::blob_t) {
            throw DBException("Column ") << col_idx << " is not a BLOB";
        }

        sqlite3_bind_blob(stmt_, (int32_t)col_idx + 1, blobval.data_ptr, (int32_t)blobval.num_bytes,
                          0);
    }

    /// @brief Set the value for the given column index.
    /// @param col_idx 0-based column index.
    /// @param strval Value to set for the column.
    void setColumnValue(uint32_t col_idx, const std::string &strval) {
        if (col_dtypes_.at(col_idx) != SqlDataType::string_t) {
            throw DBException("Column ") << col_idx << " is not a TEXT";
        }

        sqlite3_bind_text(stmt_, (int32_t)col_idx + 1, strval.c_str(), -1, SQLITE_TRANSIENT);
    }

    /// @brief Set the value for the given column index.
    /// @param col_idx 0-based column index.
    /// @param strval Value to set for the column.
    void setColumnValue(uint32_t col_idx, const char *strval) {
        if (col_dtypes_.at(col_idx) != SqlDataType::string_t) {
            throw DBException("Column ") << col_idx << " is not a TEXT";
        }

        sqlite3_bind_text(stmt_, (int32_t)col_idx + 1, strval, -1, SQLITE_STATIC);
    }

    /// @brief Execute the prepared INSERT statement and create a new record.
    /// @param clear_bindings If true, clears all bound values after executing the INSERT.
    /// If false, the bound values remain for the next INSERT unless explicitly changed
    /// by calling setColumnValue() again. For any previously bound values that are not
    /// changed, the same values will be used for the next INSERT, which requires that
    /// the user is careful to ensure that the bound values are valid for each INSERT.
    /// If they went out of scope or were otherwise invalidated, the INSERT will likely
    /// fail if not cause a crash.
    /// @return The row ID of the newly inserted record.
    int createRecord(bool clear_bindings = true) {
        auto rc = SQLiteReturnCode(sqlite3_step(stmt_));
        if (rc != SQLITE_DONE) {
            throw DBException("Could not perform INSERT. Error: ")
                << sqlite3_errmsg(db_conn_->getDatabase());
        }

        sqlite3_reset(stmt_);
        if (clear_bindings) {
            sqlite3_clear_bindings(stmt_);
        }
        return db_conn_->getLastInsertRowId();
    }

  private:
    SQLitePreparedStatement prepared_stmt_;
    sqlite3_stmt *stmt_ = nullptr;
    std::vector<SqlDataType> col_dtypes_;
    std::shared_ptr<Connection> db_conn_;
};

} // namespace simdb
