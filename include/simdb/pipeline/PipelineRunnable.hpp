#pragma once

namespace simdb::pipeline {

class Runnable
{
public:
    virtual ~Runnable() = default;
    virtual bool run() = 0;
};

} // namespace simdb::pipeline
