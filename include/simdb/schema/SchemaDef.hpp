// <SchemaDef.hpp> -*- C++ -*-

#pragma once

#include "simdb/Exceptions.hpp"

#include <algorithm>
#include <deque>
#include <iostream>
#include <limits>
#include <memory>
#include <unordered_map>
#include <vector>

namespace simdb
{

/// Data types supported by SimDB schemas
enum class SqlDataType
{
    int32_t,
    int64_t,
    double_t,
    string_t,
    blob_t
};

/// Stream operator used when creating various SQL commands.
inline std::ostream& operator<<(std::ostream& os, const SqlDataType dtype)
{
    using dt = SqlDataType;

    switch (dtype)
    {
        case dt::int32_t:
        case dt::int64_t:
        {
            os << "INT";
            break;
        }

        case dt::string_t:
        {
            os << "TEXT";
            break;
        }

        case dt::double_t:
        {
            os << "REAL";
            break;
        }

        case dt::blob_t:
        {
            os << "BLOB";
            break;
        }
    }

    return os;
}

/*!
 * \class Column
 *
 * \brief This class is used for creating SimDB tables.
 */
class Column
{
public:
    /// Construct with the column name and data type.
    Column(const std::string& column_name, const SqlDataType dt)
        : name_(column_name)
        , dt_(dt)
    {
    }

    /// Equivalence is defined as having the same name and data type.
    bool operator==(const Column& rhs) const
    {
        return name_ == rhs.name_ && dt_ == rhs.dt_;
    }

    /// Equivalence is defined as having the same name and data type.
    bool operator!=(const Column& rhs) const
    {
        return !(*this == rhs);
    }

    /// Get the name of this column.
    const std::string& getName() const
    {
        return name_;
    }

    /// Get the data type of this column.
    SqlDataType getDataType() const
    {
        return dt_;
    }

    /// Optionally specify a default value for this Column.
    /// Defaults for SqlBlob data types are not allowed and will
    /// throw if you attempt to set a SqlBlob default value.
    template <typename T> void setDefaultValue(const T val)
    {
        if (dt_ == SqlDataType::blob_t)
        {
            throw DBException("Cannot set default value for a database "
                              "column with blob data type");
        }

        switch (dt_)
        {
            case SqlDataType::int32_t:
            case SqlDataType::int64_t:
            {
                auto flag = std::integral_constant<bool, std::is_integral<T>::value>();
                auto err = "Default value type mismatch (expected integer type)";
                verifyDefaultValueIsCorrectType_(flag, err);
                break;
            }

            case SqlDataType::double_t:
            {
                auto flag = std::integral_constant<bool, std::is_floating_point<T>::value>();
                auto err = "Default value type mismatch (expected floating point type)";
                verifyDefaultValueIsCorrectType_(flag, err);
                break;
            }

            default: break;
        }

        std::ostringstream ss;
        writeDefaultValue_(ss, val);
        default_val_string_ = ss.str();

        if (default_val_string_.empty())
        {
            throw DBException("Unable to convert default value ") << val << " into a std::string";
        }
    }

    /// Called in order to set default values for TEXT columns.
    void setDefaultValue(const std::string& val)
    {
        if (dt_ != SqlDataType::string_t)
        {
            throw DBException("Unable to set default value string (data type mismatch)");
        }

        default_val_string_ = val;
    }

    /// Check if this column has a default value set or not.
    bool hasDefaultValue() const
    {
        return !default_val_string_.empty();
    }

    /// Get this Column's default value. These are returned as
    /// strings since the schema creation command is one string,
    /// e.g. "CREATE TABLE ..."
    const std::string& getDefaultValueAsString() const
    {
        return default_val_string_;
    }

private:
    /// Default values are stringified. For doubles, we need maximum precision.
    void writeDefaultValue_(std::ostringstream& oss, const double val) const
    {
        oss << std::numeric_limits<long double>::digits10 + 1 << val;
    }

    /// Default values are stringified. For non-doubles, e.g. INT and TEXT types,
    /// we use default precision.
    template <typename T> void writeDefaultValue_(std::ostringstream& oss, const T& val) const
    {
        static_assert(std::is_integral<T>::value || std::is_same<T, std::string>::value, "Data type mismatch!");
        oss << val;
    }

    /// This method is called when the correct data type <T> was used in setDefaultValue()
    void verifyDefaultValueIsCorrectType_(std::true_type, const std::string&)
    {
    }

    /// This method is called when the wrong data type <T> was used in setDefaultValue()
    void verifyDefaultValueIsCorrectType_(std::false_type, const std::string& err)
    {
        throw DBException(err);
    }

    /// Column name
    std::string name_;

    /// Column data type
    SqlDataType dt_;

    /// Optional default value (stringified)
    std::string default_val_string_;
};

/*!
 * \class Table
 *
 * \brief Table class used for creating SimDB schemas
 */
class Table
{
public:
    /// Construct with a name.
    Table(const std::string& table_name)
        : name_(table_name)
    {
    }

    /// Get the name of this table.
    const std::string& getName() const
    {
        return name_;
    }

    /// Add a column to this table's schema with a name and data type.
    Table& addColumn(const std::string& name, const SqlDataType dt)
    {
        columns_.emplace_back(new Column(name, dt));
        columns_by_name_[name] = columns_.back();
        return *this;
    }

    /// Get a column by its name. Throws if not found.
    const Column& getColumn(const std::string& col_name) const
    {
        auto iter = columns_by_name_.find(col_name);
        if (iter == columns_by_name_.end())
        {
            throw DBException("No column named ") << col_name << " in table " << name_;
        }

        return *(iter->second);
    }

    /// Check if this table has a column with the given name.
    bool hasColumn(const std::string& col_name) const
    {
        return columns_by_name_.find(col_name) != columns_by_name_.end();
    }

    /// Assign a default value for the given column.
    template <typename T> Table& setColumnDefaultValue(const std::string& col_name, const T default_val)
    {
        auto iter = columns_by_name_.find(col_name);
        if (iter == columns_by_name_.end())
        {
            throw DBException("No column named ") << col_name << " in table " << name_;
        }

        iter->second->setDefaultValue(default_val);
        return *this;
    }

    /// Assign a default value for the given column.
    Table& setColumnDefaultValue(const std::string& col_name, const std::string& default_val)
    {
        auto iter = columns_by_name_.find(col_name);
        if (iter == columns_by_name_.end())
        {
            throw DBException("No column named ") << col_name << " in table " << name_;
        }

        iter->second->setDefaultValue(default_val);
        return *this;
    }

    /// Index this table's records on the given column.
    /// CREATE INDEX IndexName ON TableName(ColumnName)
    Table& createIndexOn(const std::string& col_name)
    {
        return createCompoundIndexOn({col_name});
    }

    /// Index this table's records on the given columns.
    /// CREATE INDEX IndexName ON TableName(ColA,ColB,ColC)
    Table& createCompoundIndexOn(const std::initializer_list<std::string>& col_names)
    {
        for (const auto& col_name : col_names)
        {
            if (columns_by_name_.find(col_name) == columns_by_name_.end())
            {
                throw DBException("Column ") << col_name << " does not exist in table " << name_;
            }
        }

        std::ostringstream oss;
        oss << "CREATE INDEX " << name_ << "_Index" << index_creation_strs_.size() + 1 << " ON " << name_ << "(";

        size_t idx = 0;
        auto iter = col_names.begin();
        while (iter != col_names.end())
        {
            oss << *iter;
            if (idx != col_names.size() - 1)
            {
                oss << ",";
            }
            ++iter;
            ++idx;
        }
        oss << ")";

        index_creation_strs_.push_back(oss.str());
        return *this;
    }

    /// Disable the auto-incrementing primary key for this table.
    Table& disableAutoIncPrimaryKey()
    {
        use_auto_inc_primary_key_ = false;
        return *this;
    }

    /// Read-only access to this table's columns.
    const std::vector<std::shared_ptr<Column>>& getColumns() const
    {
        return columns_;
    }

private:
    /// Name of this table
    std::string name_;

    /// Columns in this table
    std::vector<std::shared_ptr<Column>> columns_;

    /// Map of columns by their name
    std::unordered_map<std::string, std::shared_ptr<Column>> columns_by_name_;

    /// List of index creation strings that are executed on the database
    /// when the schema is instantiated:
    ///
    ///     CREATE INDEX IndexName ON TableName(ColumnName)
    ///     CREATE INDEX IndexName ON TableName(ColA,ColB,ColC)
    ///      .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .
    std::vector<std::string> index_creation_strs_;

    /// Use an auto-incrementing primary key for this table by default.
    bool use_auto_inc_primary_key_ = true;

    friend class Connection;
};

/*!
 * \class Schema
 *
 * \brief This class is used to define SimDB schemas.
 */
class Schema
{
public:
    /// \brief  Create a new Table in this Schema with the given name
    ///
    /// \return Reference to the added table
    Table& addTable(const std::string& table_name)
    {
        for (auto& lhs : tables_)
        {
            if (lhs.getName() == table_name)
            {
                throw DBException("Cannot add table '" + table_name + "' to schema. A table with that name already exists.");
            }
        }

        tables_.emplace_back(table_name);
        return tables_.back();
    }

    /// Combine this schema with the tables from another schema.
    void appendSchema(const Schema& schema)
    {
        for (const auto& table : schema.getTables())
        {
            tables_.push_back(table);
        }
    }

    /// Read-only access to this schema's tables.
    const std::deque<Table>& getTables() const
    {
        return tables_;
    }

    /// Get a table by its name. Throws if not found.
    const Table& getTable(const std::string& table_name) const
    {
        for (const auto& table : tables_)
        {
            if (table.getName() == table_name)
            {
                return table;
            }
        }

        throw DBException("Table '" + table_name + "' not found in schema");
    }

    /// Before calling getTable(), you can use this method to
    /// check if a table with the given name exists in this schema.
    bool hasTable(const std::string& table_name) const
    {
        for (const auto& table : tables_)
        {
            if (table.getName() == table_name)
            {
                return true;
            }
        }

        return false;
    }

private:
    /// All the tables in this schema, whether added via
    /// addTable() or appendSchema().
    std::deque<Table> tables_;
};

} // namespace simdb
