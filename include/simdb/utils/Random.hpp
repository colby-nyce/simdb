#pragma once

#include <random>
#include <type_traits>
#include <vector>

namespace simdb::utils {

/// Generate a random vector of numbers. Used primarily by unit tests.
template <typename T>
inline std::vector<T> generateRandomData(size_t size, T min = T(0), T max = T(100)) {
    static_assert(std::is_arithmetic<T>::value, "T must be an arithmetic type");

    std::vector<T> data(size);
    std::mt19937 gen(std::random_device{}());

    if constexpr (std::is_integral<T>::value) {
        std::uniform_int_distribution<T> dis(min, max);
        for (auto &value : data) {
            value = dis(gen);
        }
    } else if constexpr (std::is_floating_point<T>::value) {
        std::uniform_real_distribution<T> dis(min, max);
        for (auto &value : data) {
            value = dis(gen);
        }
    }

    return data;
}

} // namespace simdb::utils
