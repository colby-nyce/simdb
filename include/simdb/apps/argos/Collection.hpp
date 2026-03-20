// <Collection.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/Timestamps.hpp"

namespace simdb::collection {

//! \class Collection
//! \brief Base class for all timestamped collections. Subclasses
//! will be templated on the specific <TimeT> type.
class Collection
{
public:
    virtual ~Collection() = default;

    //! TODO cnyce: add all the collectable creation apis here

protected:
    //! Not meant to be directly instantiated
    Collection() = default;
};

//! \class TimestampedCollection
//! \brief Collection with time values of a specific type e.g. double, uint64_t, ...
template <typename TimeT, TimeAccessorType AccessorType>
class TimestampedCollection : public Collection
{
public:
    //! \brief Construct
    //! \param time_accessor Type depends on template params:
    //!         AccessorType                time_accessor
    //!         BACKPOINTER                 const TimeT*
    //!         CFUNCPOINTER                TimeT(*)()
    //!         STDFUNCTION                 std::function<TimeT()>
    TimestampedCollection(time_accessor_t<TimeT, AccessorType> time_accessor) :
        timestamp_(time_accessor)
    {}

private:
    Timestamp<TimeT, AccessorType> timestamp_;
};

} // namespace simdb::collection
