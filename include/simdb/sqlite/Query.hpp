// <Query.hpp> -*- C++ -*-

#pragma once

#include <iomanip>
#include <iostream>
#include <limits>
#include "simdb/sqlite/Constraints.hpp"
#include "simdb/sqlite/Iterator.hpp"

namespace simdb
{

/// Used in query->orderBy("ColA", ASC|DESC)
enum class QueryOrder
{
    ASC,
    DESC
};

/// Stringify QueryOrder enums for SELECT commands
inline std::ostream& operator<<(std::ostream& os, const QueryOrder order)
{
    switch (order)
    {
        case QueryOrder::ASC: os << "ASC"; break;
        case QueryOrder::DESC: os << "DESC"; break;
    }

    return os;
}

/*!
 * \class SqlQuery
 *
 * \brief This class issues SELECT statements with Constraints, and is used
 *        in order to iterate over the result set and automatically write
 *        record values into users' local variables.
 */
class SqlQuery
{
public:
    SqlQuery(const char* table_name, sqlite3* db_conn)
        : table_name_(table_name)
        , db_conn_(db_conn)
    {
    }

    /// Query for at most N matching records.
    void setLimit(uint32_t limit)
    {
        limit_ = limit;
    }

    /// Remove the LIMIT clause.
    void resetLimit()
    {
        limit_ = 0;
    }

    /// Order the query result set by the given column.
    ///
    ///     // SELECT ... ORDER BY foo DESC
    ///     query->orderBy("foo", QueryOrder::DESC);
    ///
    ///     // SELECT ... ORDER BY foo DESC, bar ASC
    ///     query->orderBy("bar", QueryOrder::ASC);
    ///
    ///     // Remove the ORDER BY clauses
    ///     query->resetOrderBy();
    ///
    ///     // SELECT ... ORDER BY bar ASC
    ///     query->orderBy("bar", QueryOrder::ASC);
    void orderBy(const char* col_name, const QueryOrder order)
    {
        order_clauses_.emplace_back(col_name, order);
    }

    /// Remove the ORDER BY clauses.
    void resetOrderBy()
    {
        order_clauses_.clear();
    }

    /// Add a constraint to this query specific to integer types and
    /// scalar target values.
    template <typename T> void addConstraintForInt(const char* col_name, const Constraints constraint, const T target)
    {
        static_assert(std::is_integral<T>::value && std::is_scalar<T>::value, "Wrong addConstraint*() API");

        std::ostringstream oss;
        oss << col_name << stringify(constraint) << target;
        constraint_clauses_.emplace_back(oss.str());
    }

    /// Add a constraint to this query specific to floating-point types
    /// and scalar target values.
    ///
    /// Pass in fuzzy=TRUE to tell SQLite to look for matches that are
    /// within EPS of the target value.
    template <typename T>
    void addConstraintForDouble(const char* col_name, const Constraints constraint, const T target, const bool fuzzy = false)
    {
        static_assert((std::is_floating_point<T>::value || std::is_integral<T>::value) && std::is_scalar<T>::value,
                      "Wrong addConstraint*() API");

        std::ostringstream oss;
        if (fuzzy)
        {
            oss << "fuzzyMatch(" << col_name << ",";
            oss << std::setprecision(std::numeric_limits<T>::max_digits10);
            oss << target << ",";
            oss << static_cast<int>(constraint) << ")";
        }
        else
        {
            oss << col_name << stringify(constraint);
            oss << std::setprecision(std::numeric_limits<T>::max_digits10) << target;
        }

        constraint_clauses_.emplace_back(oss.str());
    }

    /// Add a constraint to this query specific to string types and
    /// scalar target values.
    void addConstraintForString(const char* col_name, const Constraints constraint, const std::string& target)
    {
        addConstraintForString(col_name, constraint, target.c_str());
    }

    /// Add a constraint to this query specific to string types and
    /// scalar target values.
    void addConstraintForString(const char* col_name, const Constraints constraint, const char* target)
    {
        std::ostringstream oss;
        oss << col_name << stringify(constraint) << "'" << target << "'";
        constraint_clauses_.emplace_back(oss.str());
    }

    /// Add a constraint to this query specific to integer types and
    /// multiple target values.
    template <typename T>
    void addConstraintForInt(const char* col_name, const SetConstraints constraint, const std::initializer_list<T>& targets)
    {
        addConstraintForInt(col_name, constraint, std::vector<T>{targets.begin(), targets.end()});
    }

    /// Add a constraint to this query specific to integer types and
    /// multiple target values.
    template <typename T> void addConstraintForInt(const char* col_name, const SetConstraints constraint, const std::vector<T>& targets)
    {
        static_assert(std::is_integral<T>::value && std::is_scalar<T>::value, "Wrong addConstraint*() API");

        std::ostringstream oss;
        oss << col_name << stringify(constraint) << "(";

        for (size_t idx = 0; idx < targets.size(); ++idx)
        {
            oss << targets[idx];
            if (idx != targets.size() - 1)
            {
                oss << ",";
            }
        }

        oss << ")";
        constraint_clauses_.emplace_back(oss.str());
    }

    /// Add a constraint to this query specific to floating-point types
    /// and multiple target values.
    ///
    /// Pass in fuzzy=TRUE to tell SQLite to look for matches that are
    /// within EPS of the target values.
    template <typename T>
    void addConstraintForDouble(const char* col_name,
                                const SetConstraints constraint,
                                const std::initializer_list<T>& targets,
                                const bool fuzzy = false)
    {
        addConstraintForDouble(col_name, constraint, std::vector<T>{targets.begin(), targets.end()}, fuzzy);
    }

    /// Add a constraint to this query specific to floating-point types
    /// and multiple target values.
    ///
    /// Pass in fuzzy=TRUE to tell SQLite to look for matches that are
    /// within EPS of the target values.
    template <typename T>
    void
    addConstraintForDouble(const char* col_name, const SetConstraints constraint, const std::vector<T>& targets, const bool fuzzy = false)
    {
        static_assert(std::is_floating_point<T>::value && std::is_scalar<T>::value, "Wrong addConstraint*() API");

        std::ostringstream oss;
        if (fuzzy)
        {
            oss << "(";

            for (size_t idx = 0; idx < targets.size(); ++idx)
            {
                std::ostringstream target_oss;
                target_oss << "fuzzyMatch(" << col_name << ",";
                target_oss << std::setprecision(std::numeric_limits<T>::max_digits10);
                target_oss << targets[idx] << ",";

                if (constraint == SetConstraints::IN_SET)
                {
                    target_oss << static_cast<int>(Constraints::EQUAL);
                }
                else
                {
                    target_oss << static_cast<int>(Constraints::NOT_EQUAL);
                }

                target_oss << ")";
                oss << target_oss.str();

                if (idx != targets.size() - 1)
                {
                    if (constraint == SetConstraints::IN_SET)
                    {
                        oss << " OR ";
                    }
                    else
                    {
                        oss << " AND ";
                    }
                }
            }

            oss << ")";
        }
        else
        {
            oss << col_name << stringify(constraint) << " (";
            for (size_t idx = 0; idx < targets.size(); ++idx)
            {
                oss << std::setprecision(std::numeric_limits<T>::max_digits10) << targets[idx];
                if (idx != targets.size() - 1)
                {
                    oss << ",";
                }
            }
            oss << ")";
        }

        constraint_clauses_.emplace_back(oss.str());
    }

    /// Add a constraint to this query specific to string types and
    /// multiple target values.
    void addConstraintForString(const char* col_name, const SetConstraints constraint, const std::initializer_list<const char*>& targets)
    {
        addConstraintForString(col_name, constraint, std::vector<std::string>{targets.begin(), targets.end()});
    }

    /// Add a constraint to this query specific to string types and
    /// multiple target values.
    void addConstraintForString(const char* col_name, const SetConstraints constraint, const std::vector<std::string>& targets)
    {
        std::ostringstream oss;
        oss << col_name << stringify(constraint) << "(";

        for (size_t idx = 0; idx < targets.size(); ++idx)
        {
            oss << "'" << targets[idx] << "'";
            if (idx != targets.size() - 1)
            {
                oss << ",";
            }
        }

        oss << ")";
        constraint_clauses_.emplace_back(oss.str());
    }

    /// After one or more calls to addConstraint*(), this function can be called to
    /// create a compound constraint using AND or OR.
    ///
    ///     // SELECT ... (WHERE ColA = 1 AND ColB = 2) OR (ColC = 3)
    ///     query->addConstraintForInt("ColA", Constraints::EQUAL, 1);
    ///     query->addConstraintForInt("ColB", Constraints::EQUAL, 2);
    ///     auto clause1 = query->releaseConstraintClauses();
    ///
    ///     query->addConstraintForInt("ColC", Constraints::EQUAL, 3);
    ///     auto clause2 = query->releaseConstraintClauses();
    ///
    ///     query->addCompoundConstraint(clause1, QueryOperator::OR, clause2);
    void addCompoundConstraint(const std::vector<std::string>& clause1,
                               const QueryOperator compound_constraint,
                               const std::vector<std::string>& clause2)
    {
        std::ostringstream oss;
        oss << "(";
        for (size_t idx = 0; idx < clause1.size(); ++idx)
        {
            oss << clause1[idx];
            if (idx != clause1.size() - 1)
            {
                oss << " AND ";
            }
        }

        if (compound_constraint == QueryOperator::AND)
        {
            oss << ") AND (";
        }
        else
        {
            oss << ") OR (";
        }

        for (size_t idx = 0; idx < clause2.size(); ++idx)
        {
            oss << clause2[idx];
            if (idx != clause2.size() - 1)
            {
                oss << " AND ";
            }
        }

        oss << ")";
        constraint_clauses_.emplace_back(oss.str());
    }

    /// Release the current constraint clauses.
    std::vector<std::string> releaseConstraintClauses()
    {
        return std::move(constraint_clauses_);
    }

    /// Reset the query constraints.
    void resetConstraints()
    {
        constraint_clauses_.clear();
    }

    /// SELECT column values and write to the local variable on each iteration (int32).
    ///
    ///     int32_t val;
    ///     query->select("ColA", val);
    void select(const char* col_name, int32_t& user_var)
    {
        result_writers_.emplace_back(new ResultWriterInt32(col_name, &user_var));
    }

    /// SELECT column values and write to the local variable on each iteration (int64).
    ///
    ///     int64_t val;
    ///     query->select("ColB", val);
    void select(const char* col_name, int64_t& user_var)
    {
        result_writers_.emplace_back(new ResultWriterInt64(col_name, &user_var));
    }

    /// SELECT column values and write to the local variable on each iteration (double).
    ///
    ///     double val;
    ///     query->select("ColE", val);
    void select(const char* col_name, double& user_var)
    {
        result_writers_.emplace_back(new ResultWriterDouble(col_name, &user_var));
    }

    /// SELECT column values and write to the local variable on each iteration (string).
    ///
    ///     std::string val;
    ///     query->select("ColF", val);
    void select(const char* col_name, std::string& user_var)
    {
        result_writers_.emplace_back(new ResultWriterString(col_name, &user_var));
    }

    /// SELECT column values and write to the local variable on each iteration (blob).
    ///
    ///     std::vector<int> val;
    ///     query->select("ColG", val);
    template <typename T> void select(const char* col_name, std::vector<T>& user_var)
    {
        result_writers_.emplace_back(new ResultWriterBlob<T>(col_name, &user_var));
    }

    /// Deselect all record property values.
    void resetSelections()
    {
        result_writers_.clear();
    }

    /// Count the number of records matching this query's constraints (WHERE)
    /// and limit (LIMIT).
    uint64_t count()
    {
        std::ostringstream oss;
        oss << "SELECT COUNT(Id) FROM " << table_name_ << " ";
        appendConstraintClauses_(oss);
        appendLimitClause_(oss);

        const auto cmd = oss.str();
        auto stmt = SQLitePreparedStatement(db_conn_, cmd);
        auto rc = SQLiteReturnCode(sqlite3_step(stmt));

        if (rc == SQLITE_ROW)
        {
            return sqlite3_column_int64(stmt, 0);
        }

        if (rc == SQLITE_DONE)
        {
            return 0;
        }

        throw DBException(sqlite3_errmsg(db_conn_));
    }

    /// Execute the query.
    SqlResultIterator getResultSet()
    {
        std::ostringstream oss;
        oss << "SELECT ";
        for (size_t idx = 0; idx < result_writers_.size(); ++idx)
        {
            oss << result_writers_[idx]->getColName();
            if (idx != result_writers_.size() - 1)
            {
                oss << ",";
            }
        }

        oss << " FROM " << table_name_ << " ";

        appendConstraintClauses_(oss);
        appendOrderByClauses_(oss);
        appendLimitClause_(oss);

        const auto cmd = oss.str();
        auto stmt = SQLitePreparedStatement(db_conn_, cmd);

        std::vector<std::shared_ptr<ResultWriterBase>> result_writers;
        for (const auto& writer : result_writers_)
        {
            result_writers.emplace_back(writer->clone());
        }

        return SqlResultIterator(stmt.release(), std::move(result_writers));
    }

private:
    /// Append WHERE clause(s).
    void appendConstraintClauses_(std::ostringstream& oss) const
    {
        if (!constraint_clauses_.empty())
        {
            oss << " WHERE ";
            for (size_t idx = 0; idx < constraint_clauses_.size(); ++idx)
            {
                oss << constraint_clauses_[idx] << " ";
                if (idx != constraint_clauses_.size() - 1)
                {
                    oss << " AND ";
                }
            }
            oss << " ";
        }
    }

    /// Append ORDER BY clause(s).
    void appendOrderByClauses_(std::ostringstream& oss) const
    {
        if (!order_clauses_.empty())
        {
            oss << " ORDER BY ";
            for (size_t idx = 0; idx < order_clauses_.size(); ++idx)
            {
                oss << order_clauses_[idx].col_name << " " << order_clauses_[idx].order;
                if (idx != order_clauses_.size() - 1)
                {
                    oss << ",";
                }
            }
            oss << " ";
        }
        else
        {
            // Default order by Id if no other order clauses are specified
            oss << " ORDER BY Id ";
        }
    }

    /// Append LIMIT clause.
    void appendLimitClause_(std::ostringstream& oss) const
    {
        if (limit_)
        {
            oss << " LIMIT " << limit_;
        }
    }

    /// \struct QueryOrderClause
    /// \brief  Used to build clauses like "ORDER BY ColA ASC, ColB DESC"
    struct QueryOrderClause
    {
        std::string col_name;
        QueryOrder order;

        QueryOrderClause(const char* col_name, const QueryOrder order)
            : col_name(col_name)
            , order(order)
        {
        }
    };

    /// SELECT ColA,ColB FROM <table_name_> WHERE ...
    const std::string table_name_;

    /// Underlying sqlite3 database
    sqlite3* const db_conn_;

    /// SELECT ColA,ColB FROM Table WHERE ... LIMIT <limit_>
    uint32_t limit_ = 0;

    /// SELECT ColA,ColB FROM Table WHERE ... ORDER BY <order_clauses_>
    std::vector<QueryOrderClause> order_clauses_;

    /// SELECT ColA,ColB FROM Table WHERE <constraint_clauses_>
    std::vector<std::string> constraint_clauses_;

    /// SELECT <result_writers_> FROM Table WHERE ...
    std::vector<std::shared_ptr<ResultWriterBase>> result_writers_;
};

} // namespace simdb
