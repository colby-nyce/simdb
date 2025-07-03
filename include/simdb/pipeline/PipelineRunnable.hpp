#pragma once

#include <string>
#include <regex>

namespace simdb::pipeline {

class Runnable
{
public:
    virtual ~Runnable() = default;

    std::string getName(bool pretty = false) const
    {
        auto name = !name_.empty() ? name_ : getName_();
        return pretty ? prettyName_(name) : name;
    }

    void setName(const std::string& name)
    {
        name_ = name;
    }

    virtual bool run() = 0;

private:
    virtual std::string getName_() const = 0;

    std::string prettyName_(const std::string& name) const
    {
        std::string pretty_name = name;

        // Regular expression to match patterns like:
        // std::vector<type, std::allocator<type>>
        std::regex allocator_pattern(
            R"(std::vector<\s*([^,<>]+(?:<[^<>]+>)?)\s*,\s*std::allocator<\s*\1\s*>\s*>)");

        // Keep simplifying until no more matches
        while (std::regex_search(pretty_name, allocator_pattern))
        {
            pretty_name = std::regex_replace(pretty_name, allocator_pattern, "std::vector<$1>");
        }

        return pretty_name;
    }

    std::string name_;
};

} // namespace simdb::pipeline
