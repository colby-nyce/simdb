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
enum class ContigDeltaKind {
    CARRY,
    SWAP,
    MULTI_SWAP,
    BOOKENDS,
    ARRIVE,
    DEPART,
    MIMO,
    FULL,
};

//! Result of classifying a contig container transition.
struct ContigDeltaClassification
{
    ContigDeltaKind kind = ContigDeltaKind::FULL;
    simdb::ValidValue<uint16_t> swap_index;
    std::vector<char> payload;
    uint8_t depart_count = 0;
    uint8_t arrive_count = 0;
    std::vector<std::vector<char>> arrive_payloads;
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
        for (uint16_t i = 0; i < curr_size; ++i)
        {
            if (curr[i] != prev[i])
            {
                result.kind = ContigDeltaKind::FULL;
                return result;
            }
        }

        result.kind = ContigDeltaKind::CARRY;
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
    case ContigDeltaKind::BOOKENDS:
        return os << "BOOKENDS";
    case ContigDeltaKind::ARRIVE:
        return os << "ARRIVE";
    case ContigDeltaKind::DEPART:
        return os << "DEPART";
    case ContigDeltaKind::MIMO:
        return os << "MIMO";
    case ContigDeltaKind::FULL:
        return os << "FULL";
    }
    throw DBException("Invalid ContigDeltaKind");
}

//! Sparse-container delta kinds.
enum class SparseDeltaKind {
    CARRY,
    SWAP,
    MULTI_SWAP,
    REMOVE,
    ADD,
    MULTI_REMOVE,
    FULL,
};

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
    case SparseDeltaKind::REMOVE:
        return os << "REMOVE";
    case SparseDeltaKind::ADD:
        return os << "ADD";
    case SparseDeltaKind::MULTI_REMOVE:
        return os << "MULTI_REMOVE";
    case SparseDeltaKind::FULL:
        return os << "FULL";
    }
    throw DBException("Invalid SparseDeltaKind");
}

} // namespace simdb::argos
