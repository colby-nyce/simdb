// <Collectables.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/DataTypeHierarchy.hpp"
#include "simdb/apps/argos/PipelineStager.hpp"
#include "simdb/utils/Demangle.hpp"
#include "simdb/utils/TypeTraits.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace simdb::collection {

class DomainCollection;

template <typename T>
static constexpr bool is_collectable_stl_v =
    type_traits::is_std_vector_v<T> ||
    type_traits::is_std_deque_v<T>;

template <typename ContainerT, bool Sparse>
inline uint16_t getNumElements(const ContainerT& container)
{
    // TODO cnyce: Do we support collecting things like vector<int>?
    // We use "if (*it)" to match legacy behavior, but that stops
    // vector<int> from collecting actual values of 0. It looks like
    // the legacy behavior is to assume that queues always store
    // pointers (which is a reasonable assumption for simulators,
    // but not so much for general-purpose collection).
    static_assert(type_traits::is_any_pointer_v<typename ContainerT::value_type>);

    size_t count = 0;
    for (auto it = container.begin(), end = container.end(); it != end; ++it)
    {
        bool valid = false;
        if constexpr (type_traits::is_std_vector_v<ContainerT>)
        {
            if (*it)
            {
                valid = true;
            }
        }
        else
        {
            if (it.isValid())
            {
                valid = true;
            }
        }

        if (valid)
        {
            ++count;
        }
        else if (!Sparse)
        {
            break;
        }
    }

    if (count > UINT16_MAX)
    {
        throw DBException("Queue too large to collect; uint16_t exceeded");
    }
    return static_cast<uint16_t>(count);
}

/// Base class for all collectables.
class CollectableBase
{
public:
    virtual ~CollectableBase() = default;

    /// Get the unique ID for this collection point.
    uint16_t getID() const { return cid_; }

    /// \brief Connect to the CollectorPipeline's main input queue
    void connectToPipeline(PipelineStagerBase* stager)
    {
        stager_ = stager;
    }

    /// Enable collection
    void enable()
    {
        enabled_ = true;
        stager_->onEnabledChanged(getID(), enabled_);
    }

    /// Disable collection
    void disable()
    {
        enabled_ = false;
        stager_->onEnabledChanged(getID(), enabled_);
    }

    /// Check enabled
    bool enabled() const
    {
        return enabled_;
    }

    /// Run auto-collection for this collectable
    virtual void autoCollect()
    {
        throw DBException("This collectable does not support auto-collection");
    }

    /// Demangled element type for scalars, or element demangle + \c _contig_capacityN / \c _sparse_capacityN for queues.
    virtual std::string collectableTypeNameForDb() const = 0;

    /// For testing purposes only. DO NOT CALL IN PRODUCTION.
    static void resetCIDs()
    {
        nextID_() = 0;
    }

protected:
    CollectableBase(DomainCollection* collection, size_t heartbeat)
        : collection_(collection)
        , heartbeat_(heartbeat)
    {}

    /// Get the heartbeat value for all collection points.
    size_t getHeartbeat_() const
    {
        return heartbeat_;
    }

    /// Stage collected bytes for pipeline processing.
    void stage_(CollectedData&& data)
    {
        stager_->stage(std::move(data));
    }

private:
    /// Unique ID generator.
    static uint16_t& nextID_()
    {
        static uint16_t counter = 0;
        ++counter;
        return counter;
    }

    /// Unique collectable ID
    const uint16_t cid_{nextID_()};

    /// Collection object that owns 'this' collectable
    DomainCollection *const collection_;

    /// Heartbeat value for this collection point. This is the
    /// maximum number of cycles SimDB will attempt to perform
    /// "minification" on the data before it is forced to write
    /// the whole un-minified value to the database again. Note
    /// that minification is simply an implementation detail
    /// for performance.
    const size_t heartbeat_;

    /// \brief Enabled flag
    bool enabled_ = true;

    /// \brief Main entry point into the pipeline
    PipelineStagerBase* stager_ = nullptr;

    /// \note Friendship needed to the enabled_ flag can be set
    friend class DomainCollection;
};

/// Template class for all scalar types (POD, struct-like, string, enum, bool)
template <typename ScalarT>
class ScalarCollector : public CollectableBase
{
public:
    using ValueType = type_traits::remove_any_pointer_t<ScalarT>;

    ScalarCollector(DomainCollection* collection,
                    size_t heartbeat,
                    std::shared_ptr<DataTypeHierarchy<ValueType>> dtype_hierarchy)
        : CollectableBase(collection, heartbeat)
        , dtype_hierarchy_(std::move(dtype_hierarchy))
    {}

    std::string collectableTypeNameForDb() const override
    {
        if constexpr (std::is_same_v<ValueType, std::string>)
        {
            return "string";
        }
        else
        {
            return simdb::demangle_type<ValueType>();
        }
    }

    /// \brief On-demand collection, also called by auto-collecting subclass
    template <typename T = ScalarT>
    std::enable_if_t<!type_traits::is_any_pointer_v<T>, void>
    collect(const T& value)
    {
        if (enabled())
        {
            CollectedData collected(getID());
            dtype_hierarchy_->writeBuffer(collected.getBuffer(), value);
            stage_(std::move(collected));
        }
    }

    /// \brief Pointer-version of collect()
    template <typename T = ScalarT>
    std::enable_if_t<type_traits::is_any_pointer_v<T>, void>
    collect(const T& value)
    {
        if (value)
        {
            collect(*value);
        }
        else
        {
            disable();
        }
    }

private:
    std::shared_ptr<DataTypeHierarchy<ValueType>> dtype_hierarchy_;
};

/// Same as ScalarCollector, but supports auto-collection using a backpointer
template <typename ScalarT>
class AutoScalarCollector : public ScalarCollector<ScalarT>
{
public:
    using ValueType = typename ScalarCollector<ScalarT>::ValueType;

    /// \brief Construct with a backpointer to the auto-collected scalar
    AutoScalarCollector(DomainCollection* collection,
                        size_t heartbeat,
                        std::shared_ptr<DataTypeHierarchy<ValueType>> dtype_hierarchy,
                        const ScalarT* scalar)
        : ScalarCollector<ScalarT>(collection, heartbeat, std::move(dtype_hierarchy))
        , scalar_(scalar)
    {}

    /// Run auto-collection for this collectable
    void autoCollect() override
    {
        assert(this->enabled());
        this->collect(*scalar_);
    }

private:
    const ScalarT *const scalar_;
};

/// ContainerCollector base class which adds non-template metadata APIs
class ContainerCollectorBase : public CollectableBase
{
public:
    using CollectableBase::CollectableBase;
    virtual size_t getMaxContainerSizeCollected() const = 0;
};

/// Template class for all container types (vector, deque, etc.)
template <typename ContainerT, bool Sparse>
class ContainerCollector : public ContainerCollectorBase
{
public:
    using ValueType = typename type_traits::remove_any_pointer_t<typename ContainerT::value_type>;

    explicit ContainerCollector(DomainCollection* collection,
                                size_t heartbeat,
                                size_t expected_capacity,
                                std::shared_ptr<DataTypeHierarchy<ValueType>> dtype_hierarchy)
        : ContainerCollectorBase(collection, heartbeat)
        , expected_capacity_(expected_capacity)
        , dtype_hierarchy_(std::move(dtype_hierarchy))
    {}

    std::string collectableTypeNameForDb() const override
    {
        std::string base = simdb::demangle_type<ValueType>();
        if constexpr (Sparse)
        {
            return base + "_sparse_capacity" + std::to_string(expected_capacity_);
        }
        return base + "_contig_capacity" + std::to_string(expected_capacity_);
    }

    /// \brief On-demand collection, also called by auto-collecting subclass
    template <typename T = ContainerT>
    std::enable_if_t<!type_traits::is_any_pointer_v<T>, void>
    collect(const T& container)
    {
        if (!enabled())
        {
            return;
        }

        CollectedData collected(getID());
        auto& buffer = collected.getBuffer();

        auto num_elements = getNumElements<T, Sparse>(container);
        buffer << num_elements;
        max_size_collected_ = std::max(max_size_collected_, num_elements);

        uint16_t bin_idx = 0;
        for (auto it = container.begin(), end = container.end(); it != end; ++it)
        {
            bool valid = false;
            if constexpr (is_collectable_stl_v<ContainerT>)
            {
                if (*it)
                {
                    valid = true;
                }
            }
            else
            {
                if (it.isValid())
                {
                    valid = true;
                }
            }

            if (valid)
            {
                if (Sparse)
                {
                    buffer << bin_idx;
                }
                dtype_hierarchy_->writeBuffer(buffer, *it);
            }
            else if (!Sparse)
            {
                break;
            }

            ++bin_idx;
        }

        stage_(std::move(collected));
    }

    /// \brief Pointer-version of collect()
    template <typename T = ContainerT>
    std::enable_if_t<type_traits::is_any_pointer_v<T>, void>
    collect(const T& container)
    {
        if (container)
        {
            collect(*container);
        }
        else
        {
            disable();
        }
    }

    size_t getMaxContainerSizeCollected() const override final
    {
        return max_size_collected_;
    }

protected:
    const size_t expected_capacity_;

private:
    std::shared_ptr<DataTypeHierarchy<ValueType>> dtype_hierarchy_;
    uint16_t max_size_collected_ = 0;
};

/// \class AutoContainerCollector
/// \brief Container collectable that reads from a user-held const pointer for auto-collection (parallel to \ref AutoScalarCollector for scalars).
/// \tparam ContainerT Container type whose values are collected (vector, deque, etc.).
/// \tparam Sparse Reserved with \ref ContainerCollector for optional sparse-container semantics in future minification paths.
template <typename ContainerT, bool Sparse>
class AutoContainerCollector : public ContainerCollector<ContainerT, Sparse>
{
public:
    using ValueType = typename ContainerCollector<ContainerT, Sparse>::ValueType;

    /// \brief Construct with a backpointer to the auto-collected container
    AutoContainerCollector(DomainCollection* collection,
                           size_t heartbeat,
                           const ContainerT* container,
                           size_t expected_capacity,
                           std::shared_ptr<DataTypeHierarchy<ValueType>> dtype_hierarchy)
        : ContainerCollector<ContainerT, Sparse>(collection, heartbeat, expected_capacity, std::move(dtype_hierarchy))
        , container_(container)
    {}

    /// Run auto-collection for this collectable
    void autoCollect() override
    {
        assert(this->enabled());
        this->collect(*container_);
    }

private:
    const ContainerT *const container_;
};

} // namespace simdb::collection
