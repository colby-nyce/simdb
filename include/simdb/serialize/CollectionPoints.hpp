// <CollectionPoints.hpp> -*- C++ -*-

#pragma once

#include "simdb/serialize/Serialize.hpp"

#include <stdint.h>
#include <cassert>
#include <memory>
#include <string>
#include <vector>

namespace simdb
{

/// Raw data held in the SimDB collection "black box". Sent to the database
/// for as long as the Status isn't set to DONT_READ.
struct ArgosRecord
{
    enum class Status
    {
        READ,
        READ_ONCE,
        DONT_READ
    };
    Status status = Status::DONT_READ;

    const uint16_t elem_id = 0;
    std::vector<char> data;

    ArgosRecord(uint16_t elem_id)
        : elem_id(elem_id)
    {
    }

    void reset()
    {
        status = Status::DONT_READ;
        data.clear();
    }
};

/// Base class for all collectables.
class CollectionPointBase
{
public:
    CollectionPointBase(uint16_t elem_id, uint16_t clk_id, size_t heartbeat, const std::string& dtype)
        : argos_record_(elem_id)
        , elem_id_(elem_id)
        , clk_id_(clk_id)
        , heartbeat_(heartbeat)
        , dtype_(dtype)
    {
    }

    /// Get the unique ID for this collection point.
    uint16_t getElemId() const
    {
        return elem_id_;
    }

    /// Get the clock database ID for this collection point.
    uint16_t getClockId() const
    {
        return clk_id_;
    }

    /// Get the heartbeat for this collection point. This is the
    /// maximum number of cycles SimDB will attempt to perform
    /// "minification" on the data before it is forced to write
    /// the whole un-minified value to the database again. Note
    /// that minification is simply an implementation detail
    /// for performance.
    size_t getHeartbeat() const
    {
        return heartbeat_;
    }

    /// Data type of this collectable, e.g. "uint64_t" or "MemPacket_contig_capacity32"
    const std::string& getDataTypeStr() const
    {
        return dtype_;
    }

    /// Append the collected data from the black box unless the status is DONT_READ.
    void sweep(std::vector<char>& swept_data)
    {
        if (argos_record_.status != ArgosRecord::Status::DONT_READ)
        {
            swept_data.insert(swept_data.end(), argos_record_.data.begin(), argos_record_.data.end());
        }

        if (argos_record_.status == ArgosRecord::Status::READ_ONCE)
        {
            argos_record_.reset();
        }
    }

    /// Called at the end of simulation / when the pipeline collector is destroyed.
    /// Given the DatabaseManager in case the collectable needs to write any final
    /// metadata etc.
    ///
    /// Note that postSim() is called inside a BEGIN/COMMIT TRANSACTION block.
    virtual void postSim(DatabaseManager*)
    {
    }

protected:
    ArgosRecord argos_record_;

private:
    const uint16_t elem_id_;
    const uint16_t clk_id_;
    const size_t heartbeat_;
    const std::string dtype_;
};

template <typename T> struct is_std_vector : std::false_type
{
};

template <typename T> struct is_std_vector<std::vector<T>> : std::true_type
{
};

template <typename T> static constexpr bool is_std_vector_v = is_std_vector<T>::value;

/// Collectable for non-iterable data (not a queue/vector/deque/etc.)
///   - POD
///   - struct
class CollectionPoint : public CollectionPointBase
{
public:
    /// Minification for these collectables can only write the whole value (WRITE)
    /// or say that the value hasn't changed (CARRY).
    enum class Action : uint8_t
    {
        WRITE,
        CARRY
    };

    template <typename... Args>
    CollectionPoint(Args&&... args)
        : CollectionPointBase(std::forward<Args>(args)...)
    {
    }

    /// Put this collectable in the black box for consumption until deactivate() is called.
    /// NOTE: There is no reason to call deactivate() on your own if "bool once = true".
    template <typename T>
    typename std::enable_if<meta_utils::is_any_pointer<T>::value, void>::type activate(const T& val, bool once = false)
    {
        ensureNumBytes_(val);
        if (val)
        {
            activate(*val, once);
        }
        else
        {
            deactivate();
        }
    }

    /// Put this collectable in the black box for consumption until deactivate() is called.
    /// NOTE: There is no reason to call deactivate() on your own if "bool once = true".
    template <typename T>
    typename std::enable_if<!meta_utils::is_any_pointer<T>::value, void>::type activate(const T& val, bool once = false)
    {
        ensureNumBytes_(val);
        minify_(val);
        argos_record_.status = once ? ArgosRecord::Status::READ_ONCE : ArgosRecord::Status::READ;
    }

    /// Remove this collectable from the black box (do not collect anymore until activate() is called again).
    void deactivate()
    {
        argos_record_.status = ArgosRecord::Status::DONT_READ;
    }

private:
    /// Write the collectable bytes in the smallest form possible.
    template <typename T> void minify_(const T& val)
    {
        CollectionBuffer buffer(argos_record_.data, getElemId());

        assert(num_bytes_ > 0);
        if constexpr (std::is_trivial<T>::value && std::is_standard_layout<T>::value)
        {
            if (num_bytes_ < 16)
            {
                // Collectables that are this small are written directly,
                // as any attempts at a DB size optimization would actually
                // make the database larger.
                buffer << val;
                return;
            }
        }

        StructSerializer<T>::getInstance()->extract(&val, curr_data_);
        if (num_carry_overs_ < getHeartbeat() && curr_data_ == prev_data_)
        {
            buffer << CollectionPoint::Action::CARRY;
            ++num_carry_overs_;
        }
        else
        {
            buffer << CollectionPoint::Action::WRITE;
            buffer << curr_data_;
            prev_data_ = curr_data_;
            num_carry_overs_ = 0;
        }
    }

    template <typename T> typename std::enable_if<meta_utils::is_any_pointer<T>::value, void>::type ensureNumBytes_(const T& val)
    {
        if (num_bytes_)
        {
            return;
        }

        if (val)
        {
            ensureNumBytes_(*val);
        }

        if (num_bytes_ == 0)
        {
            throw DBException("Could not determine number of bytes for data type");
        }
    }

    template <typename T> typename std::enable_if<!meta_utils::is_any_pointer<T>::value, void>::type ensureNumBytes_(const T& val)
    {
        if (num_bytes_)
        {
            return;
        }

        if constexpr (std::is_same_v<T, bool>)
        {
            num_bytes_ = sizeof(int);
        }
        else if constexpr (std::is_same_v<T, uint8_t>)
        {
            num_bytes_ = sizeof(uint8_t);
        }
        else if constexpr (std::is_same_v<T, int8_t>)
        {
            num_bytes_ = sizeof(int8_t);
        }
        else if constexpr (std::is_same_v<T, uint16_t>)
        {
            num_bytes_ = sizeof(uint16_t);
        }
        else if constexpr (std::is_same_v<T, int16_t>)
        {
            num_bytes_ = sizeof(int16_t);
        }
        else if constexpr (std::is_same_v<T, uint32_t>)
        {
            num_bytes_ = sizeof(uint32_t);
        }
        else if constexpr (std::is_same_v<T, int32_t>)
        {
            num_bytes_ = sizeof(int32_t);
        }
        else if constexpr (std::is_same_v<T, uint64_t>)
        {
            num_bytes_ = sizeof(uint64_t);
        }
        else if constexpr (std::is_same_v<T, int64_t>)
        {
            num_bytes_ = sizeof(int64_t);
        }
        else if constexpr (std::is_same_v<T, float>)
        {
            num_bytes_ = sizeof(float);
        }
        else if constexpr (std::is_same_v<T, double>)
        {
            num_bytes_ = sizeof(double);
        }
        else if constexpr (std::is_same_v<T, std::string>)
        {
            num_bytes_ = sizeof(uint32_t);
        }
        else if constexpr (std::is_enum<T>::value)
        {
            num_bytes_ = sizeof(typename std::underlying_type<T>::type);
        }
        else
        {
            num_bytes_ = StructSerializer<T>::getInstance()->getStructNumBytes();
        }

        if (num_bytes_ == 0)
        {
            throw DBException("Could not determine number of bytes for data type");
        }
    }

    std::vector<char> curr_data_;
    std::vector<char> prev_data_;
    size_t num_carry_overs_ = 0;
    size_t num_bytes_ = 0;
};

/// Collectable for contiguous (non-sparse) iterable data e.g. queue/vector/deque/etc.
class ContigIterableCollectionPoint : public CollectionPointBase
{
public:
    /// Minification for these collectables can do the following:
    ///    - ARRIVE:   A new element has been added to the end of the container
    ///    - DEPART:   An element has been removed from the front of the container
    ///    - BOOKENDS: An element was popped off the front and another pushed to the back of the container
    ///    - CHANGE:   Exactly one element in the container has changed
    ///    - CARRY:    Nothing has changed
    ///    - FULL:     Capture the entire container (during heartbeats or when the number
    ///                of changes do not fall neatly into one of the other categories)
    enum class Action : uint8_t
    {
        ARRIVE,
        DEPART,
        BOOKENDS,
        CHANGE,
        CARRY,
        FULL
    };

    ContigIterableCollectionPoint(uint16_t elem_id, uint16_t clk_id, size_t heartbeat, const std::string& dtype, size_t capacity)
        : CollectionPointBase(elem_id, clk_id, heartbeat, dtype)
        , curr_snapshot_(capacity)
        , prev_snapshot_(capacity)
    {
    }

    /// Put this collectable in the black box for consumption until deactivate() is called.
    /// NOTE: There is no reason to call deactivate() on your own if "bool once = true".
    template <typename T>
    typename std::enable_if<meta_utils::is_any_pointer<T>::value, void>::type activate(const T container, bool once = false)
    {
        activate(*container, once);
    }

    /// Put this collectable in the black box for consumption until deactivate() is called.
    /// NOTE: There is no reason to call deactivate() on your own if "bool once = true".
    template <typename T>
    typename std::enable_if<!meta_utils::is_any_pointer<T>::value, void>::type activate(const T& container, bool once = false)
    {
        minify_(container);
        argos_record_.status = once ? ArgosRecord::Status::READ_ONCE : ArgosRecord::Status::READ;
    }

    /// Remove this collectable from the black box (do not collect anymore until activate() is called again).
    void deactivate()
    {
        argos_record_.status = ArgosRecord::Status::DONT_READ;
    }

private:
    /// This class is used to compare the current container elements to the previous (last collected cycle).
    /// We do this to determine the most efficient way to write the container to the database (minification).
    class IterableSnapshot
    {
    public:
        IterableSnapshot(size_t expected_capacity)
            : bytes_by_bin_(expected_capacity)
        {
        }

        uint16_t size() const
        {
            uint16_t num_valid = 0;
            for (const auto& bin : bytes_by_bin_)
            {
                if (!bin.empty())
                {
                    ++num_valid;
                }
            }
            return num_valid;
        }

        uint16_t capacity() const
        {
            return bytes_by_bin_.size();
        }

        std::vector<char>& operator[](size_t idx)
        {
            return bytes_by_bin_[idx];
        }

        const std::vector<char>& operator[](size_t idx) const
        {
            return bytes_by_bin_[idx];
        }

        void clear()
        {
            for (auto& bin : bytes_by_bin_)
            {
                bin.clear();
            }
        }

        void compareAndMinify(IterableSnapshot& prev, CollectionBuffer& buffer)
        {
            uint16_t changed_idx = 0;
            switch (getMinificationAction_(prev, changed_idx))
            {
                case Action::CARRY:
                    buffer << ContigIterableCollectionPoint::Action::CARRY;
                    break;
                case Action::ARRIVE:
                    buffer << ContigIterableCollectionPoint::Action::ARRIVE;
                    buffer << bytes_by_bin_.back();
                    break;
                case Action::DEPART:
                    buffer << ContigIterableCollectionPoint::Action::DEPART;
                    break;
                case Action::CHANGE:
                    buffer << ContigIterableCollectionPoint::Action::CHANGE;
                    buffer << changed_idx;
                    buffer << bytes_by_bin_[changed_idx];
                    break;
                case Action::BOOKENDS:
                    buffer << ContigIterableCollectionPoint::Action::BOOKENDS;
                    buffer << bytes_by_bin_.back();
                    break;
                case Action::FULL:
                    auto num_elems = size();
                    buffer << ContigIterableCollectionPoint::Action::FULL;
                    buffer << num_elems;

                    uint64_t num_bytes_per_bin = 0;
                    for (uint16_t idx = 0; idx < num_elems; ++idx)
                    {
                        if (idx == 0)
                        {
                            num_bytes_per_bin = bytes_by_bin_[idx].size();
                        }
                        else if (bytes_by_bin_[idx].size() != num_bytes_per_bin)
                        {
                            throw DBException("All elements in the container must have the same number of bytes");
                        }
                        buffer << bytes_by_bin_[idx];
                    }
                    break;
            }
        }

    private:
        Action getMinificationAction_(IterableSnapshot& prev, uint16_t& changed_idx)
        {
            if (++action_count_ == capacity())
            {
                action_count_ = 0;
                return Action::FULL;
            }

            if (prev.bytes_by_bin_ == bytes_by_bin_)
            {
                if (bytes_by_bin_.empty())
                {
                    return Action::FULL;
                }
                return Action::CARRY;
            }

            auto prev_size = prev.size();
            auto curr_size = size();
            if (prev_size == curr_size)
            {
                // If the (ith+1) of the prev container == the ith of the current container, then return BOOKEND
                bool bookends = true;
                for (size_t idx = 1; idx < prev_size; ++idx)
                {
                    if (prev.bytes_by_bin_[idx] != bytes_by_bin_[idx - 1])
                    {
                        bookends = false;
                        break;
                    }
                }
                if (bookends)
                {
                    return Action::BOOKENDS;
                }

                // If exactly one element has changed, then return CHANGE
                changed_idx = 0;
                for (size_t idx = 0; idx < curr_size; ++idx)
                {
                    if (prev.bytes_by_bin_[idx] != bytes_by_bin_[idx])
                    {
                        if (changed_idx)
                        {
                            // If there is more than one change, then return FULL
                            return Action::FULL;
                        }
                        changed_idx = idx;
                        return Action::CHANGE;
                    }
                }

                // Not reachable
                assert(false);
            }
            else if (prev_size + 1 == curr_size)
            {
                bool arrive = true;
                for (size_t idx = 0; idx < prev_size; ++idx)
                {
                    if (prev.bytes_by_bin_[idx] != bytes_by_bin_[idx])
                    {
                        arrive = false;
                        break;
                    }
                }
                if (arrive)
                {
                    return Action::ARRIVE;
                }
            }
            else if (prev_size - 1 == curr_size)
            {
                bool depart = true;
                for (size_t idx = 0; idx < curr_size; ++idx)
                {
                    if (prev.bytes_by_bin_[idx + 1] != bytes_by_bin_[idx])
                    {
                        depart = false;
                        break;
                    }
                }
                if (depart)
                {
                    return Action::DEPART;
                }
            }

            return Action::FULL;
        }

        std::deque<std::vector<char>> bytes_by_bin_;
        size_t action_count_ = 0;
    };

    /// Write the collectable bytes in the smallest form possible.
    template <typename T> void minify_(const T& container)
    {
        auto size = container.size();
        if (size > prev_snapshot_.capacity())
        {
            size = prev_snapshot_.capacity();
        }

        queue_max_size_ = std::max(queue_max_size_, (uint16_t)size);
        curr_snapshot_.clear();

        auto itr = container.begin();
        auto eitr = container.end();
        uint16_t bin_idx = 0;

        while (itr != eitr && bin_idx < size)
        {
            if (!writeStruct_(*itr, curr_snapshot_, bin_idx))
            {
                break;
            }
            ++itr;
            ++bin_idx;
        }

        // Let the current snapshot take into account the previous
        // snapshot, and figure out the most compact way to represent
        // the current data.
        //
        // The only thing we must do for all collection points is to
        // write the element ID.
        CollectionBuffer buffer(argos_record_.data, getElemId());
        curr_snapshot_.compareAndMinify(prev_snapshot_, buffer);
        prev_snapshot_ = curr_snapshot_;
    }

    template <typename T>
    typename std::enable_if<meta_utils::is_any_pointer<T>::value, bool>::type
    writeStruct_(const T& el, IterableSnapshot& snapshot, uint16_t bin_idx)
    {
        if (el)
        {
            return writeStruct_(*el, snapshot, bin_idx);
        }
        return false;
    }

    template <typename T>
    typename std::enable_if<!meta_utils::is_any_pointer<T>::value, bool>::type
    writeStruct_(const T& el, IterableSnapshot& snapshot, uint16_t bin_idx)
    {
        StructSerializer<T>::getInstance()->extract(&el, snapshot[bin_idx]);
        return true;
    }

    /// Write the maximum size of this queue during simulation. This is used
    /// for Argos' SchedulingLines feature.
    ///
    /// Note that postSim() is called inside a BEGIN/COMMIT TRANSACTION block.
    void postSim(DatabaseManager* db_mgr) override;

    IterableSnapshot curr_snapshot_;
    IterableSnapshot prev_snapshot_;
    uint16_t queue_max_size_ = 0;
};

/// Collectable for sparse iterable data e.g. queue/vector/deque/etc.
class SparseIterableCollectionPoint : public CollectionPointBase
{
public:
    SparseIterableCollectionPoint(uint16_t elem_id, uint16_t clk_id, size_t heartbeat, const std::string& dtype, size_t capacity)
        : CollectionPointBase(elem_id, clk_id, heartbeat, dtype)
        , expected_capacity_(capacity)
    {
        prev_data_by_bin_.resize(capacity);
        num_carry_overs_by_bin_.resize(capacity, 0);
    }

    /// Put this collectable in the black box for consumption until deactivate() is called.
    /// NOTE: There is no reason to call deactivate() on your own if "bool once = true".
    template <typename T>
    typename std::enable_if<meta_utils::is_any_pointer<T>::value, void>::type activate(const T container, bool once = false)
    {
        activate(*container, once);
    }

    /// Put this collectable in the black box for consumption until deactivate() is called.
    /// NOTE: There is no reason to call deactivate() on your own if "bool once = true".
    template <typename T>
    typename std::enable_if<!meta_utils::is_any_pointer<T>::value, void>::type activate(const T& container, bool once = false)
    {
        minify_(container);
        argos_record_.status = once ? ArgosRecord::Status::READ_ONCE : ArgosRecord::Status::READ;
    }

    /// Remove this collectable from the black box (do not collect anymore until activate() is called again).
    void deactivate()
    {
        argos_record_.status = ArgosRecord::Status::DONT_READ;
    }

private:
    /// Write the collectable bytes in the smallest form possible.
    ///
    /// TODO cnyce - We are not currently performing any minification
    /// for sparse iterables types.
    template <typename T> void minify_(const T& container)
    {
        uint16_t num_valid = 0;

        {
            auto itr = container.begin();
            auto eitr = container.end();

            while (itr != eitr)
            {
                if constexpr (is_std_vector_v<T>)
                {
                    if (*itr)
                    {
                        ++num_valid;
                    }
                }
                else if (itr.isValid())
                {
                    ++num_valid;
                }
                ++itr;
            }
        }

        queue_max_size_ = std::max(queue_max_size_, num_valid);

        CollectionBuffer buffer(argos_record_.data, getElemId());
        buffer << num_valid;

        uint16_t bin_idx = 0;
        auto itr = container.begin();
        auto eitr = container.end();
        while (itr != eitr && bin_idx < expected_capacity_)
        {
            bool valid;
            if constexpr (is_std_vector_v<T>)
            {
                valid = *itr != nullptr;
            }
            else
            {
                valid = itr.isValid();
            }

            if (valid)
            {
                writeStruct_(*itr, buffer, bin_idx);
            }
            ++itr;
            ++bin_idx;
        }
    }

    template <typename T>
    typename std::enable_if<meta_utils::is_any_pointer<T>::value, bool>::type
    writeStruct_(const T& el, CollectionBuffer& buffer, uint16_t bin_idx)
    {
        if (el)
        {
            return writeStruct_(*el, buffer, bin_idx);
        }
        return false;
    }

    template <typename T>
    typename std::enable_if<!meta_utils::is_any_pointer<T>::value, bool>::type
    writeStruct_(const T& el, CollectionBuffer& buffer, uint16_t bin_idx)
    {
        buffer << bin_idx;
        StructSerializer<T>::getInstance()->writeStruct(&el, buffer);
        return true;
    }

    /// Write the maximum size of this queue during simulation. This is used
    /// for Argos' SchedulingLines feature.
    ///
    /// Note that postSim() is called inside a BEGIN/COMMIT TRANSACTION block.
    void postSim(DatabaseManager* db_mgr) override;

    const size_t expected_capacity_;
    std::vector<char> struct_bytes_;
    std::vector<std::vector<char>> prev_data_by_bin_;
    std::vector<size_t> num_carry_overs_by_bin_;
    uint16_t queue_max_size_ = 0;
};

} // namespace simdb
