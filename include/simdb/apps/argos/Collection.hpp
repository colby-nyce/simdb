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
template <typename TimeT>
class TimestampedCollection : public Collection
{
public:
    //! \brief Use a backpointer to get the current time
    void timestampWith(const TimeT* backpointer)
    {
        timestamp_ = std::make_unique<Timestamp<TimeT>>(backpointer);
    }

    //! \brief Use a C-style function pointer to get the current time
    void timestampWith(TimeT(*fn)())
    {
        timestamp_ = std::make_unique<Timestamp<TimeT>>(fn);
    }

    //! \brief Use a C-style function pointer to get the current time
    void timestampWith(std::function<TimeT()> fn)
    {
        timestamp_ = std::make_unique<Timestamp<TimeT>>(fn);
    }

private:
    std::unique_ptr<Timestamp<TimeT>> timestamp_;
};

} // namespace simdb::collection
