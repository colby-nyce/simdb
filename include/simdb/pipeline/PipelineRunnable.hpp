#pragma once

#include <string>

namespace simdb::pipeline {

class Runnable
{
public:
    virtual ~Runnable() = default;
    virtual bool run() = 0;
    const std::string& getName() const { return name_; }

protected:
    Runnable(const std::string& name) : name_(name) {}

private:
    std::string name_;
};

} // namespace simdb::pipeline
