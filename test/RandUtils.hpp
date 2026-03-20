#pragma once

#include <array>
#include <cassert>
#include <cstdint>
#include <limits>
#include <memory>
#include <ostream>
#include <random>
#include <string>
#include <vector>

namespace simdb {

/// Enum used to verify TinyStrings
enum class Colors { RED = 1, GREEN = 2, BLUE = 3, WHITE = 0, TRANSPARENT = -1 };

inline std::ostream& operator<<(std::ostream& os, const Colors& c)
{
    switch (c)
    {
    case Colors::RED:
        os << "RED";
        break;
    case Colors::GREEN:
        os << "GREEN";
        break;
    case Colors::BLUE:
        os << "BLUE";
        break;
    case Colors::WHITE:
        os << "WHITE";
        break;
    case Colors::TRANSPARENT:
        os << "TRANSPARENT";
        break;
    default:
        os << "UNKNOWN";
        break;
    }
    return os;
}

/// Example struct that contains a wide variety of supported data fields
struct DummyPacket
{
    Colors e_color;
    char ch;
    int8_t int8;
    int16_t int16;
    int32_t int32;
    int64_t int64;
    uint8_t uint8;
    uint16_t uint16;
    uint32_t uint32;
    uint64_t uint64;
    float flt;
    double dbl;
    bool b;
    std::string str;
};

using DummyPacketPtr = std::shared_ptr<DummyPacket>;
using DummyPacketPtrVec = std::vector<DummyPacketPtr>;

/// Thread-local Mersenne Twister, seeded from std::random_device (non-deterministic where supported).
inline std::mt19937& getRandomEngine()
{
    thread_local std::mt19937 gen = []() {
        std::random_device rd;
        std::array<std::uint32_t, 8> seed_data{};
        for (auto& word : seed_data)
        {
            word = rd();
        }
        std::seed_seq seq(seed_data.begin(), seed_data.end());
        return std::mt19937(seq);
    }();
    return gen;
}

/// Random number/string/struct generators
template <typename T> T generateRandomInt()
{
    constexpr auto minval = std::numeric_limits<T>::min();
    constexpr auto maxval = std::numeric_limits<T>::max();
    static std::uniform_int_distribution<T> distrib(minval, maxval);
    return distrib(getRandomEngine());
}

template <typename T> T generateRandomFloat()
{
    constexpr auto minval = std::numeric_limits<T>::min();
    constexpr auto maxval = std::numeric_limits<T>::max();
    static std::uniform_real_distribution<T> distrib(minval, maxval);
    return distrib(getRandomEngine());
}

inline char generateRandomChar()
{
    static std::uniform_int_distribution<int> dist(0, 25);
    return static_cast<char>('A' + dist(getRandomEngine()));
}

inline bool generateRandomBool()
{
    static std::bernoulli_distribution dist(0.5);
    return dist(getRandomEngine());
}

inline std::string generateRandomString(size_t minchars = 2, size_t maxchars = 8)
{
    assert(minchars <= maxchars);

    std::string str;
    while (str.size() < minchars)
    {
        str += generateRandomChar();
    }

    while (str.size() < maxchars)
    {
        str += generateRandomChar();
    }

    return str;
}

inline Colors generateRandomColor()
{
    // Same range as the old rand() % 6 - 1  ->  -1 .. 4
    static std::uniform_int_distribution<int> dist(-1, 4);
    return static_cast<Colors>(dist(getRandomEngine()));
}

inline DummyPacketPtr generateRandomDummyPacket()
{
    auto s = std::make_shared<DummyPacket>();

    s->e_color = generateRandomColor();
    s->ch = generateRandomChar();
    s->int8 = generateRandomInt<int8_t>();
    s->int16 = generateRandomInt<int16_t>();
    s->int32 = generateRandomInt<int32_t>();
    s->int64 = generateRandomInt<int64_t>();
    s->uint8 = generateRandomInt<uint8_t>();
    s->uint16 = generateRandomInt<uint16_t>();
    s->uint32 = generateRandomInt<uint32_t>();
    s->uint64 = generateRandomInt<uint64_t>();
    s->flt = generateRandomFloat<float>();
    s->dbl = generateRandomFloat<double>();
    s->b = generateRandomBool();
    s->str = generateRandomString();

    return s;
}

} // namespace simdb
