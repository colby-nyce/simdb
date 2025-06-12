/// SimDB pipelines operate on a "chain" of functions that process data
/// entries. Each function can modify the entry (e.g. compress, transform, etc)
/// before continuing to the next function in the chain.

#pragma once

#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

namespace simdb
{

class PipelineEntry;
using PipelineFunc = void(*)(PipelineEntry&);

/// A chain of pipeline functions that can be composed together.
/// Similar to std::list<PipelineFunc> but with a more structured
/// interface for adding functions and managing the chain.
class PipelineChain
{
public:
    PipelineChain(PipelineFunc head = nullptr)
    {
        if (head)
        {
            setHead(head);
        }
    }

    PipelineChain(const PipelineChain& other)
        : funcs_(other.funcs_)
    {
    }

    PipelineChain& operator=(const PipelineChain& other)
    {
        if (this != &other)
        {
            funcs_ = other.funcs_;
        }
        return *this;
    }

    PipelineChain& append(PipelineFunc func)
    {
        if (func)
        {
            funcs_.push_back(func);
        }
        return *this;
    }

    PipelineChain operator+(const PipelineChain& other) const
    {
        PipelineChain new_chain(*this);
        for (const auto& func : other.funcs_)
        {
            new_chain.append(func);
        }
        return new_chain;
    }

    PipelineChain operator+(const PipelineFunc& func) const
    {
        PipelineChain new_chain(*this);
        if (func)
        {
            new_chain.append(func);
        }
        return new_chain;
    }

    void setHead(PipelineFunc head)
    {
        funcs_.clear();
        if (head)
        {
            funcs_.push_back(head);
        }
    }

    operator bool() const
    {
        return !funcs_.empty();
    }

    void reverse()
    {
        std::reverse(funcs_.begin(), funcs_.end());
    }

    void reset()
    {
        funcs_.clear();
    }

    // Process the entry through the pipeline chain.
    void operator()(PipelineEntry& entry) const
    {
        for (const auto& func : funcs_)
        {
            func(entry);
        }
    }

private:
    std::vector<PipelineFunc> funcs_;
};

} // namespace simdb
