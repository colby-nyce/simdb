// <Collectables.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/ArgosRecord.hpp"
#include "simdb/utils/TypeTraits.hpp"

namespace simdb::collection {

class Collection;

/// Base class for all collectables.
class CollectableBase
{
public:
    virtual ~CollectableBase() = default;

    /// Get the unique ID for this collection point.
    uint16_t getID() const { return argos_record_.getID(); }

    /// Enable collection
    void enable();

    /// Disable collection
    void disable();

    /// Move the ArgosRecord to DONT_READ, but keep auto-collection running
    void deactivate()
    {
        argos_record_.deactivate();
    }

    /// Run auto-collection and add the data bytes to the output buffer.
    virtual void autoCollect(std::vector<char> & buf)
    {
        (void)buf;
        throw DBException("This collectable does not support auto-collection");
    }

protected:
    CollectableBase(Collection* collection, size_t heartbeat)
        : collection_(collection)
        , heartbeat_(heartbeat)
    {}

    /// Unique ID generator.
    static uint16_t nextID_()
    {
        static uint16_t id = 1;
        return id++;
    }

    /// Get the heartbeat value for all collection points.
    size_t getHeartbeat_() const
    {
        return heartbeat_;
    }

    /// Raw data held in the SimDB collection "black box". Sent to the database
    /// for as long as the Status isn't set to DONT_READ.
    ArgosRecord argos_record_{nextID_()};

private:
    /// Collection object that owns 'this' collectable
    Collection *const collection_;

    /// Heartbeat value for this collection point. This is the
    /// maximum number of cycles SimDB will attempt to perform
    /// "minification" on the data before it is forced to write
    /// the whole un-minified value to the database again. Note
    /// that minification is simply an implementation detail
    /// for performance.
    const size_t heartbeat_;

    /// \brief Enabled flag
    bool enabled_ = true;

    /// \note Friendship needed to the enabled_ flag can be set
    friend class Collection;
};

/// Template class for all scalar types (POD, struct-like, string, enum, bool)
template <typename ScalarT>
class ScalarCollector : public CollectableBase
{
public:
    ScalarCollector(Collection* collection, size_t heartbeat) :
        CollectableBase(collection, heartbeat)
    {}

    using ValueType = type_traits::remove_any_pointer_t<ScalarT>;

    /// \brief On-demand collection, also called by auto-collecting subclass
    template <typename CollectedT>
    std::enable_if_t<std::is_same_v<CollectedT, ValueType>, void>
    collect(const ValueType & value)
    {
        (void)value;
    }

    /// \brief Pointer-version of collect()
    template <typename CollectedT>
    std::enable_if_t<type_traits::is_any_pointer_v<CollectedT>, void>
    collect(typename std::add_const<CollectedT>::type value)
    {
        if (value)
        {
            collect(*value);
        }
        else
        {
            deactivate();
        }
    }
};

/// Same as ScalarCollector, but supports auto-collection using a backpointer
template <typename ScalarT>
class AutoScalarCollector : public ScalarCollector<ScalarT>
{
public:
    /// \brief Construct with a backpointer to the auto-collected scalar
    AutoScalarCollector(Collection* collection, size_t heartbeat, const ScalarT* scalar)
        : ScalarCollector<ScalarT>(collection, heartbeat)
        , scalar_(scalar)
    {}

    /// Run auto-collection and add the data bytes to the output buffer.
    void autoCollect(std::vector<char> & buf) override
    {
        (void)buf;
    }

private:
    const ScalarT *const scalar_;
};

/// Template class for all container types (vector, deque, etc.)
template <typename ContainerT, bool Sparse>
class ContainerCollector : public CollectableBase
{
public:
    explicit ContainerCollector(Collection* collection, size_t heartbeat, size_t expected_capacity)
        : CollectableBase(collection, heartbeat)
        , expected_capacity_(expected_capacity)
    {}

    using ValueType = typename type_traits::remove_any_pointer_t<typename ContainerT::value_type>;

    /// \brief On-demand collection, also called by auto-collecting subclass
    template <typename CollectedT>
    std::enable_if_t<std::is_same_v<CollectedT, ContainerT>, void>
    collect(typename std::add_const<CollectedT>::type container)
    {
        (void)container;
    }

    /// \brief Pointer-version of collect()
    template <typename CollectedT>
    std::enable_if_t<type_traits::is_any_pointer_v<CollectedT>, void>
    collect(typename std::add_const<CollectedT>::type container)
    {
        if (container)
        {
            collect(*container);
        }
        else
        {
            deactivate();
        }
    }

protected:
    const size_t expected_capacity_;
};

/// \class AutoContainerCollector
/// \brief Container collectable that reads from a user-held const pointer for auto-collection (parallel to \ref AutoScalarCollector for scalars).
/// \tparam ContainerT Container type whose values are collected (vector, deque, etc.).
/// \tparam Sparse Reserved with \ref ContainerCollector for optional sparse-container semantics in future minification paths.
template <typename ContainerT, bool Sparse>
class AutoContainerCollector : public ContainerCollector<ContainerT, Sparse>
{
public:
    /// \brief Construct with a backpointer to the auto-collected container
    AutoContainerCollector(Collection* collection, size_t heartbeat, const ContainerT* container, size_t expected_capacity)
        : ContainerCollector<ContainerT, Sparse>(collection, heartbeat, expected_capacity)
        , container_(container)
    {}

    /// Run auto-collection and add the data bytes to the output buffer.
    void autoCollect(std::vector<char> & buf) override
    {
        (void)buf;
    }

private:
    const ContainerT *const container_;
};

} // namespace simdb::collection
