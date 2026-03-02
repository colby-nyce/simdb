#pragma once

#include "simdb/Exceptions.hpp"

namespace simdb {

template <typename T>
class ValidValue
{
private:
    T value_ = 0;
    bool valid_ = false;

public:
    ValidValue& operator=(T val)
    {
        value_ = val;
        valid_ = true;
        return *this;
    }

    const T& getValue() const
    {
        if (!valid_)
        {
            throw DBException("Invalid value - not set");
        }
        return value_;
    }

    T& getValue()
    {
        if (!valid_)
        {
            throw DBException("Invalid value - not set");
        }
        return value_;
    }

    operator const T&() const
    {
        return getValue();
    }

    operator T&()
    {
        return getValue();
    }

    bool isValid() const
    {
        return valid_;
    }

    void clearValid()
    {
        valid_ = false;
    }
};

} // namespace simdb
