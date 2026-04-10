// <Minifiers.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/DataTypeHierarchy.hpp"

namespace simdb::collection {

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
        if constexpr (type_traits::is_collectable_stl_v<ContainerT>)
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

template <typename ValueType>
class Minifier
{
    static_assert(!type_traits::is_collectable_stl_v<ValueType>);

public:
    Minifier(std::shared_ptr<DataTypeHierarchy<ValueType>> dtype_hierarchy,
             size_t heartbeat)
        : dtype_hierarchy_(dtype_hierarchy)
        , heartbeat_(heartbeat)
    {}

    void minifyAndAppend(const ValueType& value, StreamBuffer& buf)
    {
        StreamBuffer my_buffer(cur_extracted_bytes_);
        dtype_hierarchy_->writeBuffer(my_buffer, value);

        if (++counter_ % heartbeat_ == 0 || last_sent_bytes_ != cur_extracted_bytes_)
        {
            buf << MinifierAction::FULL;
            buf << cur_extracted_bytes_;
            last_sent_bytes_ = cur_extracted_bytes_;
            counter_ = 0;
        }
        else
        {
            buf << MinifierAction::CARRY;
        }
    }

private:
    enum class MinifierAction : uint16_t
    {
        FULL = 0,   // Value changed or we are at a heartbeat.
        CARRY       // Same value or not at a heartbeat.
    };

    std::shared_ptr<DataTypeHierarchy<ValueType>> dtype_hierarchy_;
    const size_t heartbeat_;
    size_t counter_ = 0;
    std::vector<char> last_sent_bytes_;
    std::vector<char> cur_extracted_bytes_;
};

template <typename ContainerType>
class ContigContainerMinifier
{
    static_assert(!type_traits::is_any_pointer_v<ContainerType>);
    using ValueType = type_traits::remove_any_pointer_t<typename ContainerType::value_type>;

public:
    ContigContainerMinifier(std::shared_ptr<DataTypeHierarchy<ValueType>> dtype_hierarchy,
                            size_t heartbeat,
                            size_t expected_capacity,
                            const std::string& elem_path)
        : dtype_hierarchy_(dtype_hierarchy)
        , heartbeat_(heartbeat)
        , prev_bins_(expected_capacity)
        , curr_bins_(expected_capacity)
        , elem_path_(elem_path)
    {}

    void minifyAndAppend(const ContainerType& container, StreamBuffer& buf)
    {
        // The CID is already written to the buffer. The next thing expected
        // in the buffer is the minifier action we took at this time point.
        // To get the action, serialize the incoming container's bins into
        // their raw bytes and compare against the previous bytes we collected.
        writeBins_(container, curr_bins_);
        auto action = getMinifierAction_(curr_bins_, prev_bins_);
    }

private:
    enum class MinifierAction : uint16_t
    {
        FULL = 0,   // Value changed or we are at a heartbeat.
        CARRY,      // Same value or not at a heartbeat.
        SWAP,       // One item changed.
        ARRIVE,     // One item arrived at the back of the container.
        DEPART,     // One item left the front of the container.
        BOOKENDS    // One arrived and one departed.
    };

    void writeBins_(const ContainerType& container, std::vector<std::vector<char>>& bins)
    {
        auto num_elements = getNumElements<ContainerType, false>(container);
        if (num_elements > bins.size())
        {
            if (warn_on_size_)
            {
                std::cout << "WARNING! The collected object '" << elem_path_ << "' has grown beyond the "
                          << "expected capacity (given at construction) for collection. Expected "
                          << bins.size() << " but grew to " << container.size() << ". This is your "
                          << "first and last warning.";
                warn_on_size_ = false;
            }
        }

        auto it = container.begin();
        size_t bin_idx = 0;
        while (bin_idx < num_elements)
        {
            writeBin_(*it++, bins[bin_idx++]);
        }
    }

    void writeBin_(const ValueType& bin_value, std::vector<char>& bin_buffer)
    {
        StreamBuffer buf(bin_buffer);
        dtype_hierarchy_->writeBuffer(buf, bin_value);
    }

    template <typename BinType>
    std::enable_if_t<type_traits::is_any_pointer_v<BinType>, void>
    writeBin_(const BinType& ptr, std::vector<char>& bin_buffer)
    {
        assert(ptr != nullptr);
        writeBin_(*ptr, bin_buffer);
    }

    static MinifierAction getMinifierAction_(
        const std::vector<std::vector<char>>& curr_bins,
        const std::vector<std::vector<char>>& prev_bins)
    {
        //TODO cnyce
        (void)curr_bins;
        (void)prev_bins;
        return MinifierAction::FULL;
    }

    std::shared_ptr<DataTypeHierarchy<ValueType>> dtype_hierarchy_;
    const size_t heartbeat_;
    size_t counter_ = 0;
    std::vector<std::vector<char>> prev_bins_;
    std::vector<std::vector<char>> curr_bins_;
    const std::string elem_path_;
    bool warn_on_size_ = true;
};

} // namespace simdb::collection
