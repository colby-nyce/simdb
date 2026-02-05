// <Constraints.hpp> -*- C++ -*-

#pragma once

#include <iostream>

namespace simdb {

/// This enum holds all the comparators for WHERE clauses in queries (scalar
/// target values).
enum class Constraints {
    // WHERE val =  5
    EQUAL,
    // WHERE val != 5
    NOT_EQUAL,
    // WHERE val <  5
    LESS,
    // WHERE val <= 5
    LESS_EQUAL,
    // WHERE val >  5
    GREATER,
    // WHERE val >= 5
    GREATER_EQUAL,
    // For internal use only
    __NUM_CONSTRAINTS__
};

/// This enum holds all the comparators for WHERE clauses in queries (multiple
/// target values).
enum class SetConstraints {
    // WHERE val IN (4,5,6)
    IN_SET = (int)simdb::Constraints::__NUM_CONSTRAINTS__,
    // WHERE val NOT IN (4,5,6)
    NOT_IN_SET,
    // For internal use only
    __NUM_CONSTRAINTS__
};

/// This enum holds all the comparators for WHERE clauses in queries (string
/// target values).
enum class QueryOperator { AND, OR, __NUM_CONSTRAINTS__ };

/// Stringifier for the Constraints enum
inline std::string stringify(const simdb::Constraints constraint)
{
    switch (constraint)
    {
    case simdb::Constraints::EQUAL:
        return " =  ";
    case simdb::Constraints::NOT_EQUAL:
        return " != ";
    case simdb::Constraints::LESS:
        return " <  ";
    case simdb::Constraints::LESS_EQUAL:
        return " <= ";
    case simdb::Constraints::GREATER:
        return " >  ";
    case simdb::Constraints::GREATER_EQUAL:
        return " >= ";
    default:
        return "INVALID";
    }
}

/// Stringifier for the SetConstraints enum
inline std::string stringify(const simdb::SetConstraints constraint)
{
    switch (constraint)
    {
    case simdb::SetConstraints::IN_SET:
        return " IN ";
    case simdb::SetConstraints::NOT_IN_SET:
        return " NOT IN ";
    default:
        return "INVALID";
    }
}

} // namespace simdb
