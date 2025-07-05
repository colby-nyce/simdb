// <MetaStructs.hpp> -*- C++ -*-

#pragma once

#include <array>
#include <deque>
#include <forward_list>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <queue>
#include <set>
#include <stack>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace simdb::meta_utils {

template <typename> struct is_any_pointer : public std::false_type
{
};

template <typename T> struct is_any_pointer<T*> : public std::true_type
{
};

template <typename T> struct is_any_pointer<T* const> : public std::true_type
{
};

template <typename T> struct is_any_pointer<const T*> : public std::true_type
{
};

template <typename T> struct is_any_pointer<const T* const> : public std::true_type
{
};

template <typename T> struct is_any_pointer<std::shared_ptr<T>> : public std::true_type
{
};

template <typename T> struct is_any_pointer<std::shared_ptr<T> const> : public std::true_type
{
};

template <typename T> struct is_any_pointer<std::shared_ptr<T>&> : public std::true_type
{
};

template <typename T> struct is_any_pointer<std::shared_ptr<T> const&> : public std::true_type
{
};

template <typename T> struct is_any_pointer<std::unique_ptr<T>> : public std::true_type
{
};

template <typename T> struct is_any_pointer<std::unique_ptr<T> const> : public std::true_type
{
};

template <typename T> struct is_any_pointer<std::unique_ptr<T>&> : public std::true_type
{
};

template <typename T> struct is_any_pointer<std::unique_ptr<T> const&> : public std::true_type
{
};

template <typename T> struct is_any_pointer<std::weak_ptr<T>> : public std::true_type
{
};

template <typename T> struct is_any_pointer<std::weak_ptr<T> const> : public std::true_type
{
};

template <typename T> struct is_any_pointer<std::weak_ptr<T>&> : public std::true_type
{
};

template <typename T> struct is_any_pointer<std::weak_ptr<T> const&> : public std::true_type
{
};

template <typename T> struct remove_any_pointer
{
    using type = T;
};

template <typename T> struct remove_any_pointer<T*>
{
    using type = T;
};

template <typename T> struct remove_any_pointer<T* const>
{
    using type = T;
};

template <typename T> struct remove_any_pointer<const T*>
{
    using type = T;
};

template <typename T> struct remove_any_pointer<const T* const>
{
    using type = T;
};

template <typename T> struct remove_any_pointer<std::shared_ptr<T>>
{
    using type = T;
};

template <typename T> struct remove_any_pointer<std::shared_ptr<T> const>
{
    using type = T;
};

template <typename T> struct remove_any_pointer<std::shared_ptr<T>&>
{
    using type = T;
};

template <typename T> struct remove_any_pointer<std::shared_ptr<T> const&>
{
    using type = T;
};

template <typename T> struct remove_any_pointer<std::unique_ptr<T>>
{
    using type = T;
};

template <typename T> struct remove_any_pointer<std::unique_ptr<T> const>
{
    using type = T;
};

template <typename T> struct remove_any_pointer<std::unique_ptr<T>&>
{
    using type = T;
};

template <typename T> struct remove_any_pointer<std::unique_ptr<T> const&>
{
    using type = T;
};

template <typename T> struct remove_any_pointer<std::weak_ptr<T>>
{
    using type = T;
};

template <typename T> struct remove_any_pointer<std::weak_ptr<T> const>
{
    using type = T;
};

template <typename T> struct remove_any_pointer<std::weak_ptr<T>&>
{
    using type = T;
};

template <typename T> struct remove_any_pointer<std::weak_ptr<T> const&>
{
    using type = T;
};

template <typename T> using remove_any_pointer_t = typename remove_any_pointer<T>::type;

template <typename T> struct is_contiguous : std::false_type
{
};

template <typename T> struct is_contiguous<std::vector<T>> : std::true_type
{
};

template <typename T, size_t N> struct is_contiguous<std::array<T,N>> : std::true_type
{
};

} // namespace simdb::meta_utils
