// <PointerUtils> -*- C++ -*-

/**
 * \file PointerUtils.hpp
 * \brief Contains a collection implementation of various
 * compile-time metaprogramming and Type-Detection APIs useful
 * for Template Metaprogramming.
 */

#pragma once

#include <memory>

namespace simdb3
{
    /**
    * \brief This templated struct takes a type and gives
    *  back a nested typedef of a pointer to that type.
    */
    template<typename T>
    struct add_pointer { using type = T *; };

    template<typename T>
    struct add_pointer<T *> { using type = T; };

    template<typename T>
    struct add_pointer<const T *> { using type = T; };

    template<typename T>
    struct add_pointer<T * const> { using type = T; };

    template<typename T>
    struct add_pointer<const T * const> { using type = T; };

    /** \brief Alias Template for add_pointer.
    */
    template<typename T>
    using add_pointer_t = typename add_pointer<T>::type;

    /**
    * \brief This templated struct lets us know about
    *  whether the datatype is actually an ordinary object or
    *  pointer to that object. This is specialized for
    *  a couple different signatures.
    */
    template<typename>
    struct is_any_pointer : public std::false_type {};

    template<typename T>
    struct is_any_pointer<T *> : public std::true_type {};

    template<typename T>
    struct is_any_pointer<T * const> : public std::true_type {};

    template<typename T>
    struct is_any_pointer<const T *> : public std::true_type {};

    template<typename T>
    struct is_any_pointer<const T * const> : public std::true_type {};

    template<typename T>
    struct is_any_pointer<std::shared_ptr<T>> : public std::true_type {};

    template<typename T>
    struct is_any_pointer<std::shared_ptr<T> const> : public std::true_type {};

    template<typename T>
    struct is_any_pointer<std::shared_ptr<T> &> : public std::true_type {};

    template<typename T>
    struct is_any_pointer<std::shared_ptr<T> const &> : public std::true_type {};

    template<typename T>
    struct is_any_pointer<std::unique_ptr<T>> : public std::true_type {};

    template<typename T>
    struct is_any_pointer<std::unique_ptr<T> const> : public std::true_type {};

    template<typename T>
    struct is_any_pointer<std::unique_ptr<T> &> : public std::true_type {};

    template<typename T>
    struct is_any_pointer<std::unique_ptr<T> const &> : public std::true_type {};

    template<typename T>
    struct is_any_pointer<std::weak_ptr<T>> : public std::true_type {};

    template<typename T>
    struct is_any_pointer<std::weak_ptr<T> const> : public std::true_type {};

    template<typename T>
    struct is_any_pointer<std::weak_ptr<T> &> : public std::true_type {};

    template<typename T>
    struct is_any_pointer<std::weak_ptr<T> const &> : public std::true_type {};

    /*!
    * \brief Template type helper that removes any pointer.
    * A modeler may call certain APIs with shared pointers to the
    * actual Collectable classes, or templatize Collectables with
    * pointers to collectable objects.
    * To make our API have a single interface and still work when passed
    * pointers, we will remove the pointer and then do all the decision
    * making work, by default.
    * It is harmless if the modeler passes a non pointer type as
    * removing a pointer from something which is not a pointer
    * results in itself.
    */
    template<typename T>
    struct remove_any_pointer { using type = T; };

    template<typename T>
    struct remove_any_pointer<T *> { using type = T; };

    template<typename T>
    struct remove_any_pointer<T * const> { using type = T; };

    template<typename T>
    struct remove_any_pointer<const T *> { using type = T; };

    template<typename T>
    struct remove_any_pointer<const T * const> { using type = T; };

    template<typename T>
    struct remove_any_pointer<std::shared_ptr<T>> { using type = T; };

    template<typename T>
    struct remove_any_pointer<std::shared_ptr<T> const> { using type = T; };

    template<typename T>
    struct remove_any_pointer<std::shared_ptr<T> &> { using type = T; };

    template<typename T>
    struct remove_any_pointer<std::shared_ptr<T> const &> { using type = T; };

    template<typename T>
    struct remove_any_pointer<std::unique_ptr<T>> { using type = T; };

    template<typename T>
    struct remove_any_pointer<std::unique_ptr<T> const> { using type = T; };

    template<typename T>
    struct remove_any_pointer<std::unique_ptr<T> &> { using type = T; };

    template<typename T>
    struct remove_any_pointer<std::unique_ptr<T> const &> { using type = T; };

    template<typename T>
    struct remove_any_pointer<std::weak_ptr<T>> { using type = T; };

    template<typename T>
    struct remove_any_pointer<std::weak_ptr<T> const> { using type = T; };

    template<typename T>
    struct remove_any_pointer<std::weak_ptr<T> &> { using type = T; };

    template<typename T>
    struct remove_any_pointer<std::weak_ptr<T> const &> { using type = T; };

    /** \brief Alias Template for remove_pointer.
    */
    template<typename T>
    using remove_any_pointer_t = typename remove_any_pointer<T>::type;

} // namespace simdb3
