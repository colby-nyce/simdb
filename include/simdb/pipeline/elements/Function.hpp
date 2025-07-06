#pragma once

#include <functional>

namespace simdb::pipeline {

template <typename T>
struct is_pod
{
    static inline constexpr bool value = std::is_trivial_v<T> && std::is_standard_layout_v<T>;
};

/// Function task element.
template <typename FunctionIn, typename FunctionOut>
class Function
{
public:
    using InputType = FunctionIn;
    using OutputType = FunctionOut;

    using StdFunction = std::function<FunctionOut(FunctionIn&&)>;
    Function(StdFunction func) : func_(func) {}

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
