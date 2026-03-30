// <Timestamps.hpp> -*- C++ -*-

#pragma once

#include "simdb/schema/SchemaDef.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/ValidValue.hpp"

namespace simdb::collection {

/// \class TimePointBase
/// \brief Type-agnostic base class which holds onto timestamp snapshots
class TimePointBase
{
public:
    /// Apply the stored type-specific time value to the INSERT at column 0
    virtual void apply(PreparedINSERT* inserter) const = 0;

    /// Check if the given time point is equal to ours (dynamic cast must succeed)
    virtual bool equals(const TimePointBase* time_point, bool must_be_equal_or_less = false) const = 0;

    /// Check if our time is less than the given time point (dynamic cast must succeed)
    virtual bool lessThan(const TimePointBase* time_point) const = 0;

    /// Create an entry in the Timestamps table and return the rowid
    virtual int createTimestampInDatabase(DatabaseManager* db_mgr) const = 0;
};

/// \class TimePoint
/// \brief Type-specific timestamp snapshot
template <typename TimeT> class TimePoint : public TimePointBase
{
public:
    explicit TimePoint(const TimeT time) : time_(time) {}

    /// Apply the stored type-specific time value to the INSERT at column 0
    void apply(PreparedINSERT* inserter) const override final
    {
        inserter->setColumnValue(0, time_);
    }

    /// Check if the given time point is equal to ours (dynamic cast must succeed)
    bool equals(const TimePointBase* time_point, bool must_be_equal_or_less = false) const override final
    {
        if (auto typed_time_point = dynamic_cast<const TimePoint<TimeT>*>(time_point))
        {
            if (time_ == typed_time_point->time_)
            {
                return true;
            }
            else if (must_be_equal_or_less && lessThan(typed_time_point))
            {
                return true;
            }
            else if (must_be_equal_or_less)
            {
                throw DBException("Time comparison failure: ")
                    << time_ << " <= " << typed_time_point->time_;
            }
            return false;
        }
        throw DBException("Dynamic cast failed");
    }

    /// Check if our time is less than the given time point (dynamic cast must succeed)
    bool lessThan(const TimePointBase* time_point) const override final
    {
        if (auto typed_time_point = dynamic_cast<const TimePoint<TimeT>*>(time_point))
        {
            return time_ < typed_time_point->time_;
        }
        throw DBException("Dynamic cast failed");
    }

    /// Create an entry in the Timestamps table and return the rowid
    int createTimestampInDatabase(DatabaseManager* db_mgr) const override final
    {
        return db_mgr->INSERT(SQL_TABLE("Timestamps"), SQL_VALUES(time_))->getId();
    }

private:
    const TimeT time_;
};

/// \class Timestamp
/// \brief Type-specific timestamp that can get current time values via
/// a backpointer, C-style function, or a std::function
template <typename TimeT> class Timestamp
{
public:
    /// \brief Construct with a backpointer to get the current time value
    Timestamp(const TimeT* backpointer) :
        backpointer_(backpointer)
    {
    }

    /// \brief Construct with a C-style function pointer to get the current time value
    Timestamp(TimeT (*fn)()) :
        cfuncpointer_(fn)
    {
    }

    /// \brief Construct with a std::function to get the current time value
    Timestamp(std::function<TimeT()> fn) :
        stdfunction_(fn)
    {
    }

    /// Add the type-specific time column in the given table
    void addTimeColumn(Table& tbl, const std::string& tbl_name = "Timestamp") const
    {
        using dt = SqlDataType;
        if constexpr (std::is_integral_v<TimeT>)
        {
            static_assert(std::is_unsigned_v<TimeT>, "Signed int timestamps not supported");
            if constexpr (std::is_same_v<TimeT, uint64_t>)
            {
                tbl.addColumn(tbl_name, dt::uint64_t);
            } else
            {
                tbl.addColumn(tbl_name, dt::uint32_t);
            }
        } else
        {
            tbl.addColumn(tbl_name, dt::double_t);
        }
    }

    /// Store the current type-specific time value
    std::shared_ptr<TimePointBase> snapshot()
    {
        TimeT time = 0;
        if (backpointer_)
        {
            time = *backpointer_;
        }
        else if (cfuncpointer_)
        {
            time = cfuncpointer_();
        }
        else
        {
            time = stdfunction_();
        }
        return std::make_shared<TimePoint<TimeT>>(time);
    }

private:
    const TimeT* backpointer_ = nullptr;
    TimeT (*cfuncpointer_)() = nullptr;
    std::function<TimeT()> stdfunction_ = nullptr;
    ValidValue<TimeT> time_;
};

} // namespace simdb::collection
