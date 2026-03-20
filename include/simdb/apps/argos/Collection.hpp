// <Collection.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/Timestamps.hpp"

namespace simdb::collection {

//! \class CollectionBase
//! \brief Base class for all collections. Subclasses will be
//! templated on the specific <TimeT> type.
class CollectionBase
{
public:
    virtual ~CollectionBase() = default;
};

//! \class Collection
//! \brief Collection of collectables with type-specific timestamps
template <typename TimeT, TimeAccessorType AccessorType>
class Collection : public CollectionBase
{
public:
    //! \brief Construct
    //! \param time_accessor Type depends on template params:
    //!         AccessorType                time_accessor
    //!         BACKPOINTER                 const TimeT*
    //!         CFUNCPOINTER                TimeT(*)()
    //!         STDFUNCTION                 std::function<TimeT()>
    Collection(time_accessor_t<TimeT, AccessorType> time_accessor) :
        timestamp_(time_accessor)
    {}

private:
    Timestamp<TimeT, AccessorType> timestamp_;
};

} // namespace simdb::collection
