// <Timestamps.hpp> -*- C++ -*-

#pragma once

#include "simdb/schema/SchemaDef.hpp"
#include "simdb/sqlite/PreparedINSERT.hpp"
#include "simdb/utils/ValidValue.hpp"

namespace simdb::collection {

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
    void snapshot()
    {
        if (backpointer_)
        {
            time_ = *backpointer_;
        } else if (cfuncpointer_)
        {
            time_ = cfuncpointer_();
        } else
        {
            time_ = stdfunction_();
        }
    }

    /// Apply the stored type-specific time value to the INSERT at column 0
    void apply(PreparedINSERT* inserter) const { inserter->setColumnValue(0, time_.getValue()); }

private:
    const TimeT* backpointer_ = nullptr;
    TimeT (*cfuncpointer_)() = nullptr;
    std::function<TimeT()> stdfunction_ = nullptr;
    ValidValue<TimeT> time_;
};

} // namespace simdb::collection
