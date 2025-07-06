#pragma once

#include "simdb/utils/Demangle.hpp"
#include <functional>

namespace simdb::pipeline {

/// Function task element.
template <typename FunctionIn, typename FunctionOut>
class Function
{
public:
    using InputType = FunctionIn;
    using OutputType = FunctionOut;

    using StdFunction = std::function<FunctionOut(FunctionIn&&)>;
    Function(StdFunction func) : func_(func) {}

    std::string getName() const
    {
        return "Function<" + demangle_type<FunctionIn>() + ", " + demangle_type<FunctionOut>() + ">";
    }

    bool operator()(FunctionIn&& in, ConcurrentQueue<FunctionOut>& out)
    {
        FunctionOut data = func_(std::move(in));
        out.emplace(std::move(data));
        return false;
    }

private:
    StdFunction func_;
};

} // namespace simdb::pipeline
