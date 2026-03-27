#pragma once

#include "simdb/apps/argos/DataTypeHierarchy.hpp"
#include "simdb/utils/Demangle.hpp"
#include "simdb/utils/TinyStrings.hpp"

#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace simdb::collection {

class ArgosFieldBase
{
public:
    virtual ~ArgosFieldBase() = default;
    virtual std::string getName() const = 0;
    virtual std::string getTypeName() const = 0;
    virtual bool isEnumField() const = 0;
    virtual bool isStructField() const = 0;
    virtual PodTypeKind getPodTypeKind() const = 0;
    virtual EnumBackingKind getEnumBackingKind() const = 0;
    virtual std::vector<EnumMember> getEnumMembers() const = 0;
    virtual std::string getStructTypeName() const = 0;
    virtual std::vector<const ArgosFieldBase*> getStructFields() const = 0;
    virtual void writeBufferErased(std::vector<char>&, const void*) const = 0;
    virtual const void* getStructPtrErased(const void*) const = 0;
    virtual void setTinyStrings(TinyStrings<>*) {}
};

template <typename CollectedT>
class ArgosCollectorBase
{
public:
    using collected_type = CollectedT;

    const std::vector<const ArgosFieldBase*>& getFields() const
    {
        return fields_;
    }

    void addField_(const ArgosFieldBase* f)
    {
        fields_.push_back(f);
    }

private:
    std::vector<const ArgosFieldBase*> fields_;
};

namespace detail {

template <auto Getter>
struct getter_traits;

template <typename OwnerT, typename RetT, RetT (OwnerT::*Getter)() const>
struct getter_traits<Getter>
{
    using owner_t = OwnerT;
    using return_t = RetT;
};

template <typename OwnerT, typename RetT, RetT (OwnerT::*Getter)()>
struct getter_traits<Getter>
{
    using owner_t = OwnerT;
    using return_t = RetT;
};

template <typename T>
using remove_cvref_t = std::remove_cv_t<std::remove_reference_t<T>>;

template <typename T, typename = void>
struct has_nested_argos_collector : std::false_type {};

template <typename T>
struct has_nested_argos_collector<T, std::void_t<typename remove_cvref_t<T>::ArgosCollector>> : std::true_type
{};

template <typename T>
inline constexpr bool has_nested_argos_collector_v = has_nested_argos_collector<T>::value;

} // namespace detail

// POD-only field implementation: invokes a getter and memcpy's returned scalar bytes.
template <typename OwnerT, auto Getter>
class ArgosPodField final : public ArgosFieldBase
{
    using traits = detail::getter_traits<Getter>;
    using raw_return_t = typename traits::return_t;
    using value_t = detail::remove_cvref_t<raw_return_t>;

public:
    ArgosPodField(ArgosCollectorBase<OwnerT>* owner, const char* name)
        : name_(name)
        , type_name_(demangle_type<value_t>())
    {
        static_assert(!std::is_enum_v<value_t>, "ArgosPodField only supports POD (non-enum) fields");
        owner->addField_(this);
    }

    std::string getName() const override { return name_; }
    std::string getTypeName() const override { return type_name_; }
    bool isEnumField() const override { return false; }
    bool isStructField() const override { return false; }
    PodTypeKind getPodTypeKind() const override { return detail::getPodTypeKind<value_t>(); }

    EnumBackingKind getEnumBackingKind() const override { return EnumBackingKind::i32; }
    std::vector<EnumMember> getEnumMembers() const override { return {}; }
    std::string getStructTypeName() const override { return {}; }
    std::vector<const ArgosFieldBase*> getStructFields() const override { return {}; }

    void writeBufferErased(std::vector<char>& buffer, const void* owner_void) const override
    {
        const auto* owner = static_cast<const OwnerT*>(owner_void);
        if constexpr (std::is_same_v<value_t, std::string>)
        {
            if (tiny_strings_ == nullptr)
            {
                throw DBException("TinyStrings not set before string collection");
            }
            const uint32_t id = tiny_strings_->getStringID(std::invoke(Getter, owner));
            const auto* bytes = reinterpret_cast<const char*>(&id);
            buffer.insert(buffer.end(), bytes, bytes + sizeof(id));
        }
        else if constexpr (std::is_same_v<value_t, bool>)
        {
            const uint8_t v = std::invoke(Getter, owner) ? 1u : 0u;
            const auto* bytes = reinterpret_cast<const char*>(&v);
            buffer.insert(buffer.end(), bytes, bytes + sizeof(v));
        }
        else
        {
            value_t v = static_cast<value_t>(std::invoke(Getter, owner));
            const auto* bytes = reinterpret_cast<const char*>(&v);
            buffer.insert(buffer.end(), bytes, bytes + sizeof(value_t));
        }
    }

    const void* getStructPtrErased(const void*) const override { return nullptr; }
    void setTinyStrings(TinyStrings<>* tiny_strings) override { tiny_strings_ = tiny_strings; }

private:
    std::string name_;
    std::string type_name_;
    TinyStrings<>* tiny_strings_ = nullptr;
};

// Enum: getter returns enum type; bytes are the underlying integral representation.
// Symbol names / values for the schema come from EnumDescriptor<EnumT>::members().
template <typename OwnerT, auto Getter>
class ArgosEnumField final : public ArgosFieldBase
{
    using traits = detail::getter_traits<Getter>;
    using enum_t = detail::remove_cvref_t<typename traits::return_t>;
    using int_t = std::underlying_type_t<enum_t>;

public:
    ArgosEnumField(ArgosCollectorBase<OwnerT>* owner, const char* name)
        : name_(name)
        , type_name_(demangle_type<enum_t>())
    {
        static_assert(std::is_enum_v<enum_t>, "ArgosEnumField requires an enum getter return type");
        owner->addField_(this);
    }

    std::string getName() const override { return name_; }
    std::string getTypeName() const override { return type_name_; }
    bool isEnumField() const override { return true; }
    bool isStructField() const override { return false; }
    PodTypeKind getPodTypeKind() const override { return PodTypeKind::i32; }
    EnumBackingKind getEnumBackingKind() const override { return detail::getBackingKind<int_t>(); }
    std::vector<EnumMember> getEnumMembers() const override { return EnumDescriptor<enum_t>::members(); }
    std::string getStructTypeName() const override { return {}; }
    std::vector<const ArgosFieldBase*> getStructFields() const override { return {}; }

    void writeBufferErased(std::vector<char>& buffer, const void* owner_void) const override
    {
        const auto* owner = static_cast<const OwnerT*>(owner_void);
        const int_t raw = static_cast<int_t>(std::invoke(Getter, owner));
        const auto* bytes = reinterpret_cast<const char*>(&raw);
        buffer.insert(buffer.end(), bytes, bytes + sizeof(int_t));
    }

    const void* getStructPtrErased(const void*) const override { return nullptr; }

private:
    std::string name_;
    std::string type_name_;
};

// Nested aggregate: getter returns const Nested& or const Nested* (or non-const pointer).
// Nested must define nested ArgosCollector : ArgosCollectorBase<Nested>.
template <typename OwnerT, auto Getter>
class ArgosStructField final : public ArgosFieldBase
{
    using traits = detail::getter_traits<Getter>;
    using raw_ret = typename traits::return_t;
    using bare_ret = std::remove_reference_t<raw_ret>;
    using nested_t = std::conditional_t<
        std::is_pointer_v<std::remove_cv_t<bare_ret>>,
        std::remove_pointer_t<std::remove_cv_t<bare_ret>>,
        std::remove_cv_t<bare_ret>>;

public:
    ArgosStructField(ArgosCollectorBase<OwnerT>* owner, const char* name)
        : name_(name)
        , struct_type_name_(demangle_type<nested_t>())
    {
        static_assert(!std::is_enum_v<nested_t>, "Use ARGOS_COLLECT for enum fields");
        static_assert(detail::has_nested_argos_collector_v<nested_t>,
                      "Nested type must define nested ArgosCollector");
        owner->addField_(this);
    }

    std::string getName() const override { return name_; }
    std::string getTypeName() const override { return struct_type_name_; }
    bool isEnumField() const override { return false; }
    bool isStructField() const override { return true; }
    PodTypeKind getPodTypeKind() const override { return PodTypeKind::i32; }
    EnumBackingKind getEnumBackingKind() const override { return EnumBackingKind::i32; }
    std::vector<EnumMember> getEnumMembers() const override { return {}; }
    std::string getStructTypeName() const override { return struct_type_name_; }

    std::vector<const ArgosFieldBase*> getStructFields() const override
    {
        static typename nested_t::ArgosCollector nested_schema;
        return nested_schema.getFields();
    }

    void writeBufferErased(std::vector<char>&, const void*) const override {}

    const void* getStructPtrErased(const void* owner_void) const override
    {
        const auto* owner = static_cast<const OwnerT*>(owner_void);
        if constexpr (std::is_pointer_v<bare_ret>)
        {
            return std::invoke(Getter, owner);
        } else
        {
            // Invoking the getter here yields an lvalue into `*owner`; do not
            // assign it to `auto` (that would copy temporaries and dangle).
            return static_cast<const void*>(std::addressof(std::invoke(Getter, owner)));
        }
    }

private:
    std::string name_;
    std::string struct_type_name_;
};

namespace detail {

template <typename OwnerT, auto Getter>
using auto_field_t = std::conditional_t<
    std::is_enum_v<remove_cvref_t<typename getter_traits<Getter>::return_t>>,
    ArgosEnumField<OwnerT, Getter>,
    ArgosPodField<OwnerT, Getter>>;

} // namespace detail

} // namespace simdb::collection

// Macro glue
#define ARGOS_COLLECT_CAT_(a, b) a##b
#define ARGOS_COLLECT_CAT(a, b)  ARGOS_COLLECT_CAT_(a, b)

// Scalar field: registers one getter-based scalar field in the owning ArgosCollector.
// Enums are auto-routed to ArgosEnumField; all other scalar types use ArgosPodField.
#define ARGOS_COLLECT(field_name, getter_ptr)                                                 \
    simdb::collection::detail::auto_field_t<collected_type, getter_ptr>                       \
        ARGOS_COLLECT_CAT(argos_collect_field_, __COUNTER__){this, #field_name};

#define ARGOS_COLLECT_STRUCT(field_name, getter_ptr)                                          \
    simdb::collection::ArgosStructField<collected_type, getter_ptr>                           \
        ARGOS_COLLECT_CAT(argos_collect_struct_, __COUNTER__){this, #field_name};

