#pragma once

#include "simdb/Exceptions.hpp"

#include <cstdint>
#include <string>

namespace simdb::utils {

inline void uint64_to_utf16(uint64_t value, char16_t* out)
{
    for (int i = 19; i >= 0; --i)
    {
        out[i] = static_cast<char16_t>(u'0' + (value % 10));
        value /= 10;
    }
}

inline std::u16string uint64_to_utf16(uint64_t value)
{
    std::u16string result(20, u'0');
    uint64_to_utf16(value, &result[0]);
    return result;
}

uint64_t utf16_to_uint64(const char16_t* str, int length)
{
    if (length != 20)
    {
        throw DBException("Invalid UTF-16 uint64 length");
    }

    uint64_t value = 0;
    for (int i = 0; i < 20; ++i)
    {
        char16_t ch = str[i];
        if (ch < u'0' || ch > u'9')
        {
            throw DBException("Invalid digit");
        }
        value = value * 10 + (ch - u'0');
    }

    return value;
}

} // namespace simdb::utils
