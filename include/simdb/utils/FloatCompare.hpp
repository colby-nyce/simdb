// <FloatCompare.hpp> -*- C++ -*-

#pragma once

#include <cinttypes>
#include <cmath>
#include <limits>
#include <random>
#include <time.h>

namespace simdb {

/// Comparison of two floating-point values with
/// a supplied tolerance. The tolerance value defaults
/// to machine epsilon.
template <typename T>
typename std::enable_if<std::is_floating_point<T>::value, bool>::type
approximatelyEqual(const T a, const T b, const T epsilon = std::numeric_limits<T>::epsilon()) {
    const T fabs_a = std::fabs(a);
    const T fabs_b = std::fabs(b);
    const T fabs_diff = std::fabs(a - b);

    return fabs_diff <= ((fabs_a < fabs_b ? fabs_b : fabs_a) * epsilon);
}

} // namespace simdb
