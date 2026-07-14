// <CheckpointDeltas.hpp> -*- C++ -*-

#pragma once

#include "simdb/Exceptions.hpp"
#include "simdb/utils/ValidValue.hpp"

#include <cstdint>
#include <iostream>
#include <map>
#include <vector>

namespace simdb::argos {

//! Result of comparing two consecutive scalar payloads.
enum class ScalarDeltaKind { UNCHANGED, CHANGED };

//! Compare previous and current scalar payloads.
//! Returns CHANGED when \p prev is empty (first collection has no baseline).
//! Scalars only support CARRY when UNCHANGED; CHANGED implies a FULL snapshot.
inline ScalarDeltaKind classifyScalarChange(const std::vector<char>& prev, const std::vector<char>& curr)
{
    if (prev.empty())
    {
        return ScalarDeltaKind::CHANGED;
    }

    return prev == curr ? ScalarDeltaKind::UNCHANGED : ScalarDeltaKind::CHANGED;
}

inline std::ostream& operator<<(std::ostream& os, ScalarDeltaKind kind)
{
    switch (kind)
    {
    case ScalarDeltaKind::UNCHANGED:
        return os << "UNCHANGED";
    case ScalarDeltaKind::CHANGED:
        return os << "CHANGED";
    }
    throw DBException("Invalid ScalarDeltaKind");
}

//! Contiguous-container delta kinds.
enum class ContigDeltaKind { CARRY, SWAP, MULTI_SWAP, FULL };

//! Result of classifying a contig container transition.
struct ContigDeltaClassification
{
    ContigDeltaKind kind = ContigDeltaKind::FULL;
    simdb::ValidValue<uint16_t> swap_index;
    std::vector<char> payload;
    std::vector<uint16_t> swap_indices;
    std::vector<std::vector<char>> swap_payloads;
};

inline uint16_t countContigElements(const std::vector<std::vector<char>>& contig_bins)
{
    uint64_t count = 0;
    for (const auto& bytes : contig_bins)
    {
        if (!bytes.empty())
        {
            ++count;
        } else
        {
            break;
        }
    }
    assert(count <= UINT16_MAX);
    return count;
}

//! Classify how a contiguous container changed between consecutive collections.
//!
//! Returns FULL when \p prev is empty (no baseline). Heartbeat forcing is handled
//! by the checkpointer, not this helper.
inline ContigDeltaClassification classifyContigChange(const std::vector<std::vector<char>>& prev,
                                                      const std::vector<std::vector<char>>& curr)
{
    ContigDeltaClassification result;

    if (prev.empty())
    {
        result.kind = ContigDeltaKind::FULL;
        return result;
    }

    const auto curr_size = countContigElements(curr);
    const auto prev_size = countContigElements(prev);

    if (curr_size == prev_size)
    {
        ValidValue<uint16_t> changed_idx;
        uint16_t num_changes = 0;
        std::vector<uint16_t> changed_idxs;
        for (uint16_t i = 0; i < curr_size; ++i)
        {
            if (curr[i] != prev[i])
            {
                changed_idxs.push_back(i);
                changed_idx = i;
                ++num_changes;
                if (num_changes > 1)
                {
                    changed_idx.clearValid();
                }
            }
        }

        if (num_changes == 0)
        {
            result.kind = ContigDeltaKind::CARRY;
            return result;
        }

        if (num_changes == 1)
        {
            result.kind = ContigDeltaKind::SWAP;
            result.swap_index = changed_idx.getValue();
            result.payload = curr[changed_idx.getValue()];
            return result;
        }

        result.kind = ContigDeltaKind::MULTI_SWAP;
        result.swap_indices = std::move(changed_idxs);
        result.swap_payloads.reserve(result.swap_indices.size());
        for (const auto idx : result.swap_indices)
        {
            result.swap_payloads.push_back(curr[idx]);
        }
        return result;
    }

    result.kind = ContigDeltaKind::FULL;
    return result;
}

inline std::ostream& operator<<(std::ostream& os, ContigDeltaKind kind)
{
    switch (kind)
    {
    case ContigDeltaKind::CARRY:
        return os << "CARRY";
    case ContigDeltaKind::SWAP:
        return os << "SWAP";
    case ContigDeltaKind::MULTI_SWAP:
        return os << "MULTI_SWAP";
    case ContigDeltaKind::FULL:
        return os << "FULL";
    }
    throw DBException("Invalid ContigDeltaKind");
}

//! Sparse-container delta kinds.
enum class SparseDeltaKind { CARRY, SWAP, MULTI_SWAP, FULL };

//! Result of classifying a sparse container transition.
struct SparseDeltaClassification
{
    SparseDeltaKind kind = SparseDeltaKind::FULL;
    simdb::ValidValue<uint16_t> bin_index;
    std::vector<char> payload;
    std::vector<uint16_t> bin_indices;
    std::vector<std::vector<char>> payloads;
};

inline uint16_t countSparseElements(const std::map<uint16_t, std::vector<char>>& sparse_bins)
{
    uint64_t count = 0;
    for (const auto& [_, bytes] : sparse_bins)
    {
        if (!bytes.empty())
        {
            ++count;
        }
    }
    assert(count <= UINT16_MAX);
    return static_cast<uint16_t>(count);
}

//! Classify how a sparse container changed between consecutive collections.
//!
//! Returns FULL when \p prev is empty (no baseline). Heartbeat forcing is handled
//! by the checkpointer, not this helper.
inline SparseDeltaClassification classifySparseChange(const std::map<uint16_t, std::vector<char>>& prev,
                                                      const std::map<uint16_t, std::vector<char>>& curr)
{
    SparseDeltaClassification result;

    if (prev.empty())
    {
        result.kind = SparseDeltaKind::FULL;
        return result;
    }

    if (curr == prev)
    {
        result.kind = SparseDeltaKind::CARRY;
        return result;
    }

    const auto curr_size = countSparseElements(curr);
    const auto prev_size = countSparseElements(prev);
    if (curr_size != prev_size)
    {
        result.kind = SparseDeltaKind::FULL;
        return result;
    }

    std::vector<uint16_t> changed_idxs;
    for (const auto& [prev_idx, prev_bytes] : prev)
    {
        if (auto it = curr.find(prev_idx); it != curr.end())
        {
            if (prev_bytes != it->second)
            {
                changed_idxs.push_back(prev_idx);
            }
        } else
        {
            result.kind = SparseDeltaKind::FULL;
            return result;
        }
    }

    for (const auto& [curr_idx, _] : curr)
    {
        if (prev.find(curr_idx) == prev.end())
        {
            result.kind = SparseDeltaKind::FULL;
            return result;
        }
    }

    if (changed_idxs.size() == 1)
    {
        result.kind = SparseDeltaKind::SWAP;
        result.bin_index = changed_idxs[0];
        result.payload = curr.at(changed_idxs[0]);
        return result;
    }

    if (changed_idxs.size() >= 2)
    {
        result.kind = SparseDeltaKind::MULTI_SWAP;
        result.bin_indices = std::move(changed_idxs);
        result.payloads.reserve(result.bin_indices.size());
        for (const auto idx : result.bin_indices)
        {
            result.payloads.push_back(curr.at(idx));
        }
        return result;
    }

    result.kind = SparseDeltaKind::FULL;
    return result;
}

inline std::ostream& operator<<(std::ostream& os, SparseDeltaKind kind)
{
    switch (kind)
    {
    case SparseDeltaKind::CARRY:
        return os << "CARRY";
    case SparseDeltaKind::SWAP:
        return os << "SWAP";
    case SparseDeltaKind::MULTI_SWAP:
        return os << "MULTI_SWAP";
    case SparseDeltaKind::FULL:
        return os << "FULL";
    }
    throw DBException("Invalid SparseDeltaKind");
}

} // namespace simdb::argos
