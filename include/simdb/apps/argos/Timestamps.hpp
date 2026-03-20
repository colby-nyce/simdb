// <Timestamps.hpp> -*- C++ -*-

#pragma once

#include "simdb/sqlite/PreparedINSERT.hpp"
#include "simdb/schema/SchemaDef.hpp"
#include "simdb/utils/ValidValue.hpp"

namespace simdb::collection {

//! \enum TimeAccessorType
//! \brief Defines supported ways to get a type-specific time value
enum class TimeAccessorType
{
    BACKPOINTER,
    CFUNCPOINTER,
    STDFUNCTION
};

template <typename TimeT, TimeAccessorType AccessorType>
struct time_accessor;

template <typename TimeT>
struct time_accessor<TimeT, TimeAccessorType::BACKPOINTER>
{
    using type = const TimeT*;
};

template <typename TimeT>
struct time_accessor<TimeT, TimeAccessorType::CFUNCPOINTER>
{
    using type = TimeT(*)();
};

template <typename TimeT>
struct time_accessor<TimeT, TimeAccessorType::STDFUNCTION>
{
    using type = std::function<TimeT()>;
};

template <typename TimeT, TimeAccessorType AccessorType>
using time_accessor_t = typename time_accessor<TimeT, AccessorType>::type;

//! \class TimeGetterBase
//! \brief Base class for all TimeGetter classes of the same <TimeT>
template <typename TimeT>
class TimeGetterBase
{
public:
    virtual ~TimeGetterBase() = default;
    virtual TimeT getTime() const = 0;
};

//! \class TimeGetter
//! \brief Type-specific time getter with support for backpointers,
//! C-style functions, and std::function to return the <TimeT> value
template <typename TimeT, TimeAccessorType AccessorType>
class TimeGetter : public TimeGetterBase<TimeT>
{
private:
    //! Hold all accessor in one tuple for convenience. To overhead
    //! to check which tuple element has our accessor due to the
    //! <AccessorType> in the template.
    std::tuple<const TimeT*,           //! backpointer
               TimeT(*)(),             //! C-style function
               std::function<TimeT()>> //! C++ std::function
    accessors_;

public:
    TimeGetter(time_accessor_t<TimeT, AccessorType> time_accessor)
    {
        std::get<AccessorType>(accessors_) = time_accessor;
    }

    TimeT getTime() const override
    {
        if constexpr (AccessorType == TimeAccessorType::BACKPOINTER)
        {
            return *std::get<AccessorType>(accessors_);
        }
        else if constexpr (AccessorType == TimeAccessorType::CFUNCPOINTER ||
                           AccessorType == TimeAccessorType::STDFUNCTION)
        {
            return std::get<AccessorType>(accessors_)();
        }
        else
        {
            static_assert(false);
        }
    }
};

//! \class TimestampBase
//! \brief Type-agnostic timestamp base class
class TimestampBase
{
public:
    virtual ~TimestampBase() = default;

    //! Set the type-specific time column in the given table
    virtual void setTimeColumn(Table& tbl) const = 0;

    //! Store the current type-specific time value
    virtual void snapshot() = 0;

    //! Apply the stored type-specific time value to the INSERT at column 0
    virtual void apply(PreparedINSERT* inserter) const = 0;
};

//! \class Timestamp
//! \brief Type-specific timestamp that can get current time values via
//! a backpointer, C-style function, or a std::function
template <typename TimeT, TimeAccessorType AccessorType>
class Timestamp : public TimestampBase
{
public:
    //! \brief Construct
    //! \param time_accessor Type depends on template params:
    //!         AccessorType                time_accessor
    //!         BACKPOINTER                 const TimeT*
    //!         CFUNCPOINTER                TimeT(*)()
    //!         STDFUNCTION                 std::function<TimeT()>
    Timestamp(time_accessor_t<TimeT, AccessorType> time_accessor)
        : getter_(time_accessor)
    {}

    //! Set the type-specific time column in the given table
    void setTimeColumn(Table& tbl) const override
    {
        using dt = SqlDataType;
        if constexpr (std::is_same_v<TimeT, uint64_t>)
        {
            tbl.addColumn("Timestamp", dt::uint64_t);
        }
        else if constexpr (std::is_floating_point_v<TimeT>)
        {
            tbl.addColumn("Timestamp", dt::double_t);
        }
        else if constexpr (std::is_integral_v<TimeT>)
        {
            static_assert(std::is_signed_v<TimeT>, "Unsigned int timestamps not supported");
            tbl.addColumn("Timestamp", dt::uint32_t);
        }
    }

    //! Store the current type-specific time value
    void snapshot() override
    {
        time_ = getter_.getTime();
    }

    //! Apply the stored type-specific time value to the INSERT at column 0
    void apply(PreparedINSERT* inserter) const override
    {
        inserter->setColumnValue(0, time_.getValue());
    }

private:
    TimeGetter<TimeT, AccessorType> getter_;
    ValidValue<TimeT> time_;
};

} // namespace simdb::collection
