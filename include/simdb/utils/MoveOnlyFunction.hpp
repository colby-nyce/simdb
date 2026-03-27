#pragma once

#include <cassert>
#include <cstddef>
#include <functional>
#include <new>
#include <type_traits>
#include <utility>

namespace simdb::utils {

/// Move-only erased callable with small-buffer optimization. Prefer over
/// std::function for hot paths: no copy support, inlined storage for typical
/// captures, single indirect call.
template <typename Sig>
class MoveOnlyFunction;

template <typename R, typename... Args>
class MoveOnlyFunction<R(Args...)>
{
    struct VTable
    {
        R (*call)(void*, Args...);
        void (*destroy)(void*) noexcept;
        void (*relocate)(void* dest, void* src) noexcept;
    };

    static constexpr std::size_t buffer_size = 56;

    const VTable* vtable_ = nullptr;
    alignas(std::max_align_t) mutable unsigned char buffer_[buffer_size];

    void destroy() noexcept
    {
        if (vtable_)
        {
            vtable_->destroy(buffer_);
            vtable_ = nullptr;
        }
    }

    template <typename F>
    static R call_small(void* self, Args... args)
    {
        return std::invoke(*reinterpret_cast<F*>(self), std::forward<Args>(args)...);
    }

    template <typename F>
    static R call_large(void* self, Args... args)
    {
        auto* fp = *static_cast<F**>(self);
        return std::invoke(*fp, std::forward<Args>(args)...);
    }

    template <typename F>
    static void destroy_small(void* self) noexcept
    {
        reinterpret_cast<F*>(self)->~F();
    }

    template <typename F>
    static void destroy_large(void* self) noexcept
    {
        delete *static_cast<F**>(self);
    }

    template <typename F>
    static void relocate_small(void* dest, void* src) noexcept
    {
        auto* s = reinterpret_cast<F*>(src);
        new (dest) F(std::move(*s));
        s->~F();
    }

    template <typename F>
    static void relocate_large(void* dest, void* src) noexcept
    {
        auto& sp = *static_cast<F**>(src);
        auto& dp = *static_cast<F**>(dest);
        dp = sp;
        sp = nullptr;
    }

    template <typename F>
    static const VTable& small_vtable()
    {
        static const VTable vt{call_small<F>, destroy_small<F>, relocate_small<F>};
        return vt;
    }

    template <typename F>
    static const VTable& large_vtable()
    {
        static const VTable vt{call_large<F>, destroy_large<F>, relocate_large<F>};
        return vt;
    }

public:
    MoveOnlyFunction() noexcept = default;

    MoveOnlyFunction(std::nullptr_t) noexcept
        : MoveOnlyFunction()
    {
    }

    MoveOnlyFunction(const MoveOnlyFunction&) = delete;
    MoveOnlyFunction& operator=(const MoveOnlyFunction&) = delete;

    ~MoveOnlyFunction()
    {
        destroy();
    }

    MoveOnlyFunction(MoveOnlyFunction&& other) noexcept
    {
        if (other.vtable_)
        {
            vtable_ = other.vtable_;
            vtable_->relocate(buffer_, other.buffer_);
            other.vtable_ = nullptr;
        }
    }

    MoveOnlyFunction& operator=(MoveOnlyFunction&& other) noexcept
    {
        if (this != &other)
        {
            destroy();
            if (other.vtable_)
            {
                vtable_ = other.vtable_;
                vtable_->relocate(buffer_, other.buffer_);
                other.vtable_ = nullptr;
            }
        }
        return *this;
    }

    MoveOnlyFunction& operator=(std::nullptr_t) noexcept
    {
        destroy();
        return *this;
    }

    template <typename F,
              typename FStrip = std::decay_t<F>,
              typename = std::enable_if_t<std::is_invocable_r_v<R, FStrip&, Args...> &&
                                          !std::is_same_v<FStrip, MoveOnlyFunction>>>
    MoveOnlyFunction(F&& f)
    {
        static constexpr bool use_sbo = sizeof(FStrip) <= buffer_size &&
                                          alignof(FStrip) <= alignof(std::max_align_t) &&
                                          std::is_nothrow_move_constructible_v<FStrip>;

        if constexpr (use_sbo)
        {
            new (buffer_) FStrip(std::forward<F>(f));
            vtable_ = &small_vtable<FStrip>();
        }
        else
        {
            new (buffer_) FStrip*(new FStrip(std::forward<F>(f)));
            vtable_ = &large_vtable<FStrip>();
        }
    }

    explicit operator bool() const noexcept
    {
        return vtable_ != nullptr;
    }

    R operator()(Args... args) const
    {
        assert(vtable_);
        return vtable_->call(buffer_, std::forward<Args>(args)...);
    }
};

} // namespace simdb::utils
