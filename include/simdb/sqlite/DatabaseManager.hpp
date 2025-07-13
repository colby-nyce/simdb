// <DatabaseManager.hpp> -*- C++ -*-

#pragma once

#include "simdb/schema/SchemaDef.hpp"
#include "simdb/sqlite/Connection.hpp"
#include "simdb/sqlite/Query.hpp"
#include "simdb/sqlite/Table.hpp"
#include "simdb/sqlite/PreparedINSERT.hpp"
#include <filesystem>
#include <set>

namespace simdb
{

enum class JournalMode
{
    //PRAGMA journal_mode = OFF
    //PRAGMA synchronous = OFF
    FASTEST,

    //PRAGMA journal_mode = DELETE;
    //PRAGMA synchronous = FULL;
    SAFEST,

    //PRAGMA journal_mode = WAL;
    //PRAGMA synchronous = NORMAL;
    BALANCED
};

using PragmaPairs = std::vector<std::pair<std::string, std::string>>;

inline PragmaPairs getPragmas(JournalMode mode)
{
    switch (mode)
    {
        case JournalMode::FASTEST:
            return {{"journal_mode", "OFF"}, {"synchronous", "OFF"}};
        case JournalMode::SAFEST:
            return {{"journal_mode", "DELETE"}, {"synchronous", "FULL"}};
        case JournalMode::BALANCED:
            return {{"journal_mode", "WAL"}, {"synchronous", "NORMAL"}};
    }
}

/*!
 * \class DatabaseManager
 *
 * \brief This class is the primary "entry point" into SimDB connections,
 *        including instantiating schemas, creating records, querying
 *        records, and accessing the underlying Connection.
 */
class DatabaseManager
{
public:
    /// \brief Construct a DatabaseManager
    /// \param db_file Name of the database file, typically with .db extension
    /// \param force_new_file Force the <db_file> to be overwritten if it exists.
    ///                       If the file already existed and this flag is false,
    ///                       then you will not be able to call appendSchema()
    ///                       on this DatabaseManager as the schema is fixed.
    /// \param pragmas Pragmas to set on the database as soon as it is opened.
    DatabaseManager(const std::string& db_file = "sim.db", const bool force_new_file = false, const PragmaPairs& pragmas = {})
        : db_file_(db_file)
    {
        if (std::filesystem::exists(db_file))
        {
            if (force_new_file)
            {
                const auto cmd = "rm -f " + db_file;
                auto rc = system(cmd.c_str());
                (void)rc;
            }
            else
            {
                if (!connectToExistingDatabase_(db_file))
                {
                    throw DBException("Unable to connect to database file: ") << db_file;
                }
                append_schema_allowed_ = false;
            }
        }

        db_conn_.reset(new Connection);
        createDatabaseFile_(pragmas);
    }

    /// \brief   Add one or more tables to the existing schema.
    ///
    /// \throws  This will throw an exception for DatabaseManager's
    ///          whose connection was initialized with a previously
    ///          existing file.
    ///
    /// \return  Returns true if successful, false otherwise.
    bool appendSchema(const Schema& schema)
    {
        if (!append_schema_allowed_)
        {
            throw DBException(
                "Cannot alter schema if you created a DatabaseManager with an existing file.");
        }

        db_conn_->realizeSchema(schema);
        schema_.appendSchema(schema);
        return true;
    }

    /// Get the schema for this database.
    const Schema& getSchema() const
    {
        return schema_;
    }

    /// Get the full database file path.
    const std::string& getDatabaseFilePath() const
    {
        return db_filepath_;
    }

    /// Execute the functor inside BEGIN/COMMIT TRANSACTION.
    void safeTransaction(const TransactionFunc& func) const
    {
        db_conn_->safeTransaction(func);
    }

    /// \brief  Perform INSERT operation on this database.
    ///
    /// \note   The way to call this method is:
    ///         db_mgr.INSERT(SQL_TABLE("TableName"),
    ///                       SQL_COLUMNS("ColA", "ColB"),
    ///                       SQL_VALUES(3.14, "foo"));
    ///
    /// \note   You may also provide ValueContainerBase subclasses in the SQL_VALUES.
    ///
    /// \return SqlRecord which wraps the table and the ID of its record.
    std::unique_ptr<SqlRecord> INSERT(SqlTable&& table, SqlColumns&& cols, SqlValues&& vals)
    {
        std::unique_ptr<SqlRecord> record;

        db_conn_->safeTransaction(
            [&]()
            {
                std::ostringstream oss;
                oss << "INSERT INTO " << table.getName();
                cols.writeColsForINSERT(oss);
                vals.writeValsForINSERT(oss);

                std::string cmd = oss.str();
                auto stmt = db_conn_->prepareStatement(cmd);
                vals.bindValsForINSERT(stmt);

                auto rc = SQLiteReturnCode(sqlite3_step(stmt));
                if (rc != SQLITE_DONE)
                {
                    throw DBException("Could not perform INSERT. Error: ")
                        << sqlite3_errmsg(db_conn_->getDatabase());
                }

                auto db_id = db_conn_->getLastInsertRowId();
                record.reset(
                    new SqlRecord(table.getName(), db_id, db_conn_->getDatabase(), db_conn_.get()));
            });

        return record;
    }

    /// This INSERT() overload is to be used for tables that were defined with
    /// at least one default value for its column(s).
    std::unique_ptr<SqlRecord> INSERT(SqlTable&& table)
    {
        std::unique_ptr<SqlRecord> record;

        db_conn_->safeTransaction(
            [&]()
            {
                const std::string cmd = "INSERT INTO " + table.getName() + " DEFAULT VALUES";
                auto stmt = db_conn_->prepareStatement(cmd);

                auto rc = SQLiteReturnCode(sqlite3_step(stmt));
                if (rc != SQLITE_DONE)
                {
                    throw DBException("Could not perform INSERT. Error: ")
                        << sqlite3_errmsg(db_conn_->getDatabase());
                }

                auto db_id = db_conn_->getLastInsertRowId();
                record.reset(
                    new SqlRecord(table.getName(), db_id, db_conn_->getDatabase(), db_conn_.get()));
            });

        return record;
    }

    /// \brief Create a prepared statement for faster record creation
    ///        when you are creating many of the same table records.
    std::unique_ptr<PreparedINSERT> prepareINSERT(SqlTable&& table, SqlColumns&& cols)
    {
        const std::set<std::string> col_names(cols.getColNames().begin(), cols.getColNames().end());

        std::vector<SqlDataType> col_dtypes;
        for (const auto& col : schema_.getTable(table.getName()).getColumns())
        {
            if (col_names.count(col->getName()))
            {
                col_dtypes.push_back(col->getDataType());
            }
        }

        std::ostringstream oss;
        oss << "INSERT INTO " << table.getName() << " (";

        auto it = cols.getColNames().begin();
        while (true)
        {
            oss << *it;
            if (++it != cols.getColNames().end())
            {
                oss << ",";
            }
            else
            {
                break;
            }
        }

        oss << ") VALUES (";
        for (size_t i = 0; i < col_dtypes.size(); ++i)
        {
            oss << "?" << (i != col_dtypes.size()-1 ? "," : "");
        }
        oss << ")";

        std::string cmd = oss.str();
        auto stmt = db_conn_->prepareStatement(cmd);

        return std::make_unique<PreparedINSERT>(std::move(stmt), col_dtypes, db_conn_);
    }

    /// \brief Execute an arbitrary SQL command on this database.
    void EXECUTE(const std::string& sql_cmd, bool in_transaction = true)
    {
        if (in_transaction)
        {
            db_conn_->safeTransaction(
                [&]()
                {
                    auto rc = SQLiteReturnCode(sqlite3_exec(
                        db_conn_->getDatabase(), sql_cmd.c_str(), nullptr, nullptr, nullptr));
                    if (rc)
                    {
                        throw DBException(sqlite3_errmsg(db_conn_->getDatabase()));
                    }
                });
        }
        else
        {
            auto rc = SQLiteReturnCode(sqlite3_exec(
                db_conn_->getDatabase(), sql_cmd.c_str(), nullptr, nullptr, nullptr));
            if (rc)
            {
                throw DBException(sqlite3_errmsg(db_conn_->getDatabase()));
            }
        }
    }

    /// \brief  Get a SqlRecord from a database ID for the given table.
    ///
    /// \return Returns the record wrapper if found, or nullptr if not.
    std::unique_ptr<SqlRecord> findRecord(const char* table_name, const int db_id) const
    {
        return findRecord_(table_name, db_id, false);
    }

    /// \brief  Get a SqlRecord from a database ID for the given table.
    ///
    /// \throws Throws an exception if this database ID is not found in the given table.
    std::unique_ptr<SqlRecord> getRecord(const char* table_name, const int db_id) const
    {
        return findRecord_(table_name, db_id, true);
    }

    /// \brief  Delete one record from the given table with the given ID.
    ///
    /// \return Returns true if successful, false otherwise.
    bool removeRecordFromTable(const char* table_name, const int db_id)
    {
        db_conn_->safeTransaction(
            [&]()
            {
                std::ostringstream oss;
                oss << "DELETE FROM " << table_name << " WHERE Id=" << db_id;
                const auto cmd = oss.str();

                auto rc = SQLiteReturnCode(
                    sqlite3_exec(db_conn_->getDatabase(), cmd.c_str(), nullptr, nullptr, nullptr));
                if (rc)
                {
                    throw DBException(sqlite3_errmsg(db_conn_->getDatabase()));
                }
            });

        return sqlite3_changes(db_conn_->getDatabase()) == 1;
    }

    /// \brief  Delete every record from the given table.
    ///
    /// \return Returns the total number of deleted records.
    uint32_t removeAllRecordsFromTable(const char* table_name)
    {
        db_conn_->safeTransaction(
            [&]()
            {
                std::ostringstream oss;
                oss << "DELETE FROM " << table_name;
                const auto cmd = oss.str();

                auto rc = SQLiteReturnCode(
                    sqlite3_exec(db_conn_->getDatabase(), cmd.c_str(), nullptr, nullptr, nullptr));
                if (rc)
                {
                    throw DBException(sqlite3_errmsg(db_conn_->getDatabase()));
                }
            });

        return sqlite3_changes(db_conn_->getDatabase());
    }

    /// \brief  Issue "DELETE FROM TableName" to clear out the given table.
    ///
    /// \return Returns the total number of deleted records across all tables.
    uint32_t removeAllRecordsFromAllTables()
    {
        uint32_t count = 0;

        db_conn_->safeTransaction(
            [&]()
            {
                const char* cmd = "SELECT name FROM sqlite_master WHERE type='table'";
                auto stmt = db_conn_->prepareStatement(cmd);

                while (true)
                {
                    auto rc = SQLiteReturnCode(sqlite3_step(stmt));
                    if (rc != SQLITE_ROW)
                    {
                        break;
                    }

                    auto table_name = sqlite3_column_text(stmt, 0);
                    count += removeAllRecordsFromTable((const char*)table_name);
                }
            });

        return count;
    }

    /// Get a query object to issue SELECT statements with constraints.
    std::unique_ptr<SqlQuery> createQuery(const char* table_name)
    {
        return std::unique_ptr<SqlQuery>(new SqlQuery(table_name, db_conn_->getDatabase()));
    }

private:
    /// \brief  Open a database connection to an existing database file.
    ///
    /// \param db_fpath Full path to the database including the ".db"
    ///                 extension, e.g. "/path/to/my/dir/statistics.db"
    ///
    /// \note   The 'db_fpath' is typically one that was given to us
    ///         from a previous call to getDatabaseFilePath()
    ///
    /// \return Returns true if successful, false otherwise.
    bool connectToExistingDatabase_(const std::string& db_fpath)
    {
        db_conn_.reset(new Connection);

        if (db_conn_->openDbFile_(db_fpath).empty())
        {
            db_conn_.reset();
            db_filepath_.clear();
            return false;
        }

        // TODO cnyce: Reconstitute the Schema member variable
        // and remove the append_schema_allowed_ flag.

        db_filepath_ = db_conn_->getDatabaseFilePath();
        append_schema_allowed_ = false;
        return true;
    }

    /// Open the given database file.
    bool createDatabaseFile_(const PragmaPairs& pragmas)
    {
        if (!db_conn_)
        {
            return false;
        }

        auto db_filename = db_conn_->openDbFile_(db_file_);
        if (!db_filename.empty())
        {
            //File opened without issues. Store the full DB filename.
            db_filepath_ = db_filename;

            //Issue PRAGMA's
            for (const auto& [name, val] : pragmas)
            {
                EXECUTE("PRAGMA " + name + " = " + val, false);
            }

            return true;
        }

        return false;
    }

    /// Get a SqlRecord from a database ID for the given table.
    std::unique_ptr<SqlRecord>
    findRecord_(const char* table_name, const int db_id, const bool must_exist) const
    {
        std::ostringstream oss;
        oss << "SELECT * FROM " << table_name << " WHERE Id=" << db_id;
        const auto cmd = oss.str();

        auto stmt = SQLitePreparedStatement(db_conn_->getDatabase(), cmd);
        auto rc = SQLiteReturnCode(sqlite3_step(stmt));

        if (must_exist && rc == SQLITE_DONE)
        {
            throw DBException("Record not found with ID ") << db_id << " in table " << table_name;
        }
        else if (rc == SQLITE_DONE)
        {
            return nullptr;
        }
        else if (rc == SQLITE_ROW)
        {
            return std::unique_ptr<SqlRecord>(
                new SqlRecord(table_name, db_id, db_conn_->getDatabase(), db_conn_.get()));
        }
        else
        {
            throw DBException("Internal error has occured: ")
                << sqlite3_errmsg(db_conn_->getDatabase());
        }
    }

    /// Database connection.
    std::shared_ptr<Connection> db_conn_;

    /// Schema for this database.
    Schema schema_;

    /// Name of the database file.
    const std::string db_file_;

    /// Full database file name, including the database path
    /// and file extension
    std::string db_filepath_;

    /// Flag saying whether or not the schema can be altered.
    /// We do not allow schemas to be altered for DatabaseManager's
    /// that were initialized with a previously existing file.
    bool append_schema_allowed_ = true;
};

} // namespace simdb
