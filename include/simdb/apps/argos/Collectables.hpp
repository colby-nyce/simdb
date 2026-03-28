// <Collectables.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/ArgosRecord.hpp"
#include "simdb/apps/argos/DataTypeHierarchy.hpp"
#include "simdb/apps/argos/PipelineStager.hpp"
#include "simdb/utils/Demangle.hpp"
#include "simdb/utils/TypeTraits.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace simdb::collection {

class Collection;

/// Base class for all collectables.
class CollectableBase
{
public:
    virtual ~CollectableBase() = default;

    /// Get the unique ID for this collection point.
    uint16_t getID() const { return argos_record_.getID(); }

    /// \brief Connect to the CollectorPipeline's main input queue
    void connectToPipeline(PipelineStagerBase* stager)
    {
        stager_ = stager;
    }

    /// Enable collection
    void enable();

    /// Disable collection
    void disable();

    /// Move the ArgosRecord to DONT_READ, but keep auto-collection running
    void deactivate()
    {
        argos_record_.deactivate();
    }

    /// Run auto-collection for this collectable
    virtual void autoCollect()
    {
        throw DBException("This collectable does not support auto-collection");
    }

    /// Demangled element type for scalars, or element demangle + \c _contig_capacityN / \c _sparse_capacityN for queues.
    virtual std::string collectableTypeNameForDb() const = 0;

    /// \c 1 if this collectable was registered for auto-collection, else \c 0 (matches \c CollectableTreeNodes.AutoCollected).
    virtual int32_t collectableAutoCollectedForDb() const = 0;

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

    /// Stage collected bytes for pipeline processing.
    void stage_(std::vector<char>&& bytes)
    {
        stager_->stage(std::move(bytes));
    }

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

    /// \brief Main entry point into the pipeline
    PipelineStagerBase* stager_ = nullptr;

    /// \note Friendship needed to the enabled_ flag can be set
    friend class Collection;
};

/// Template class for all scalar types (POD, struct-like, string, enum, bool)
template <typename ScalarT>
class ScalarCollector : public CollectableBase
{
public:
    using ValueType = type_traits::remove_any_pointer_t<ScalarT>;

    ScalarCollector(Collection* collection,
                    size_t heartbeat,
                    std::shared_ptr<DataTypeHierarchy<ValueType>> dtype_hierarchy)
        : CollectableBase(collection, heartbeat)
        , dtype_hierarchy_(std::move(dtype_hierarchy))
    {}

    std::string collectableTypeNameForDb() const override
    {
        return simdb::demangle_type<ValueType>();
    }

    int32_t collectableAutoCollectedForDb() const override { return 0; }

    /// \brief On-demand collection, also called by auto-collecting subclass
    template <typename T = ScalarT>
    std::enable_if_t<!type_traits::is_any_pointer_v<T>, void>
    collect(const T& value)
    {
        std::vector<char> bytes;
        StreamBuffer buffer(bytes);
        buffer << getID();
        dtype_hierarchy_->writeBuffer(bytes, value);
        stage_(std::move(bytes));
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
            deactivate();
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
    AutoScalarCollector(Collection* collection,
                        size_t heartbeat,
                        std::shared_ptr<DataTypeHierarchy<ValueType>> dtype_hierarchy,
                        const ScalarT* scalar)
        : ScalarCollector<ScalarT>(collection, heartbeat, std::move(dtype_hierarchy))
        , scalar_(scalar)
    {}

    /// Run auto-collection for this collectable
    void autoCollect() override
    {
        this->collect(*scalar_);
    }

    int32_t collectableAutoCollectedForDb() const override { return 1; }

private:
    const ScalarT *const scalar_;
};

/// Template class for all container types (vector, deque, etc.)
template <typename ContainerT, bool Sparse>
class ContainerCollector : public CollectableBase
{
public:
    using ValueType = typename type_traits::remove_any_pointer_t<typename ContainerT::value_type>;

    explicit ContainerCollector(Collection* collection,
                                size_t heartbeat,
                                size_t expected_capacity,
                                std::shared_ptr<DataTypeHierarchy<ValueType>> dtype_hierarchy)
        : CollectableBase(collection, heartbeat)
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

    int32_t collectableAutoCollectedForDb() const override { return 0; }

    /// \brief On-demand collection, also called by auto-collecting subclass
    template <typename T = ContainerT>
    std::enable_if_t<!type_traits::is_any_pointer_v<T>, void>
    collect(const T& container)
    {
        std::vector<char> bytes;
        StreamBuffer buffer(bytes);
        buffer << getID();

        appendSize_(buffer, container);

        auto it = container.begin();
        while (it++ != container.end())
        {

        }

        //dtype_hierarchy_->writeBuffer(buf, value);
        stage_(std::move(bytes));
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
            deactivate();
        }
    }

protected:
    const size_t expected_capacity_;

private:
    template <bool sparse = Sparse>
    std::enable_if_t<sparse, void>
    appendSize_(StreamBuffer& buffer, const ContainerT& container) const
    {
        uint64_t size = 0;
        for (auto it = container.begin(), end = container.end(); it != end; ++it)
        {
            if constexpr (type_traits::is_std_vector_v<ContainerT>)
            {
                if (*it)
                {
                    ++size;
                }
            }
            else
            {
                if (it.isValid())
                {
                    ++size;
                }
            }
        }

        if (size > UINT16_MAX)
        {
            throw DBException("Queue too large to collect; uint16_t exceeded");
        }
        buffer << (uint16_t)size;
    }

    template <bool sparse = Sparse>
    std::enable_if_t<!sparse, void>
    appendSize_(StreamBuffer& buffer, const ContainerT& container) const
    {
        auto size = container.size();
        if (size > UINT16_MAX)
        {
            throw DBException("Queue too large to collect; uint16_t exceeded");
        }
        buffer << (uint16_t)size;
    }

    std::shared_ptr<DataTypeHierarchy<ValueType>> dtype_hierarchy_;
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
    AutoContainerCollector(Collection* collection,
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
        this->collect(*container_);
    }

    int32_t collectableAutoCollectedForDb() const override { return 1; }

private:
    const ContainerT *const container_;
};

} // namespace simdb::collection
