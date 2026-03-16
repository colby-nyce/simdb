#pragma once

#include <iostream>

namespace simdb
{

class ios_format_saver
{
public:
    explicit ios_format_saver(std::ios& s)
        : stream_(s)
    {
        state_.copyfmt(s);
    }

    ~ios_format_saver()
    {
        stream_.copyfmt(state_);
    }

private:
    std::ios& stream_;
    std::ios state_{nullptr};
};

} // namespace simdb
