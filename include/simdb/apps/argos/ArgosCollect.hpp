#pragma once

#include "simdb/apps/argos/DataTypeHierarchy.hpp"
#include "simdb/utils/Demangle.hpp"

#include <functional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace simdb::collection::cursor {

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
        , type_name_(simdb::demangle_type<value_t>())
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
        value_t v = static_cast<value_t>(std::invoke(Getter, owner));
        const auto* bytes = reinterpret_cast<const char*>(&v);
        buffer.insert(buffer.end(), bytes, bytes + sizeof(value_t));
    }

    const void* getStructPtrErased(const void*) const override { return nullptr; }

private:
    std::string name_;
    std::string type_name_;
};

} // namespace simdb::collection::cursor

// Macro glue
#define ARGOS_COLLECT_CAT_(a, b) a##b
#define ARGOS_COLLECT_CAT(a, b)  ARGOS_COLLECT_CAT_(a, b)

// POD-only: registers one getter-based scalar field in the owning ArgosCollector.
#define ARGOS_COLLECT(field_name, getter_ptr)                                                 \
    simdb::collection::cursor::ArgosPodField<collected_type, getter_ptr>                      \
        ARGOS_COLLECT_CAT(argos_collect_field_, __COUNTER__){this, #field_name};

