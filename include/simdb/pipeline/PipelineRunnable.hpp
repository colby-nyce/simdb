#pragma once

#include <iostream>
#include <string>

namespace simdb::pipeline {

class Runnable
{
public:
    virtual ~Runnable() = default;

    std::string getName() const
    {
        return !name_.empty() ? name_ : getName_();
    }

    void setName(const std::string& name)
    {
        name_ = name;
    }

    virtual void print(std::ostream& os, int indent = 0) const
    {
        os << std::string(indent, ' ') << getName() << "\n";
    }

    virtual bool run() = 0;

private:
    virtual std::string getName_() const = 0;
    std::string name_;
};

} // namespace simdb::pipeline
