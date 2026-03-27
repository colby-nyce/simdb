#pragma once

#include "simdb/utils/Demangle.hpp"
#include "simdb/utils/TinyStrings.hpp"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace simdb::collection {

enum class NodeKind
{
    Pod,
    Enum,
    Struct
};

enum class EnumBackingKind
{
    i8,
    ui8,
    i16,
    ui16,
    i32,
    ui32,
    i64,
    ui64
};

enum class PodTypeKind
{
    c,
    i8,
    ui8,
    i16,
    ui16,
    i32,
    ui32,
    i64,
    ui64,
    d,
    f,
    logical,
    str
};

struct EnumMember
{
    std::string name;
    int64_t value = 0;
};

struct EnumMeta
{
    EnumBackingKind backing_kind = EnumBackingKind::i32;
    std::vector<EnumMember> members;
};

struct DataTypeNode
{
    NodeKind kind = NodeKind::Pod;
    DataTypeNode* parent = nullptr;
    std::string field_name;
    std::string type_name;
    std::unique_ptr<PodTypeKind> pod_type;
    std::unique_ptr<EnumMeta> enum_meta;
    std::vector<std::unique_ptr<DataTypeNode>> children;
    std::function<void(std::vector<char>&, const void*)> write_erased;

    // Set by DataTypeInspector::connect(). Used by string writers.
    TinyStrings<>* tiny_strings = nullptr;

    // Optional backpointer to the originating field descriptor.
    // DataTypeInspector uses this to inject TinyStrings into field writers.
    void* source_field = nullptr;
};

inline const char* podTypeKindToTypeName(PodTypeKind kind)
{
    switch (kind)
    {
    case PodTypeKind::c: return "char";
    case PodTypeKind::i8: return "int8_t";
    case PodTypeKind::ui8: return "uint8_t";
    case PodTypeKind::i16: return "int16_t";
    case PodTypeKind::ui16: return "uint16_t";
    case PodTypeKind::i32: return "int32_t";
    case PodTypeKind::ui32: return "uint32_t";
    case PodTypeKind::i64: return "int64_t";
    case PodTypeKind::ui64: return "uint64_t";
    case PodTypeKind::d: return "double";
    case PodTypeKind::f: return "float";
    case PodTypeKind::logical: return "bool";
    case PodTypeKind::str: return "std::string";
    }
    return "unknown";
}

namespace detail {

template <typename T>
using remove_cvref_t = std::remove_cv_t<std::remove_reference_t<T>>;

template <typename T>
constexpr bool is_pod_leaf_v =
    std::is_trivial_v<remove_cvref_t<T>> &&
    std::is_standard_layout_v<remove_cvref_t<T>> &&
    !std::is_enum_v<remove_cvref_t<T>>;

template <typename IntT>
constexpr EnumBackingKind getBackingKind()
{
    static_assert(std::is_integral_v<IntT>, "IntT must be integral");
    static_assert(!std::is_same_v<IntT, bool>, "bool is not a valid enum backing type");

    if constexpr (std::is_same_v<IntT, int8_t>)  return EnumBackingKind::i8;
    if constexpr (std::is_same_v<IntT, uint8_t>) return EnumBackingKind::ui8;
    if constexpr (std::is_same_v<IntT, int16_t>) return EnumBackingKind::i16;
    if constexpr (std::is_same_v<IntT, uint16_t>) return EnumBackingKind::ui16;
    if constexpr (std::is_same_v<IntT, int32_t>) return EnumBackingKind::i32;
    if constexpr (std::is_same_v<IntT, uint32_t>) return EnumBackingKind::ui32;
    if constexpr (std::is_same_v<IntT, int64_t>) return EnumBackingKind::i64;
    if constexpr (std::is_same_v<IntT, uint64_t>) return EnumBackingKind::ui64;

    if constexpr (std::is_signed_v<IntT>) return EnumBackingKind::i64;
    return EnumBackingKind::ui64;
}

template <typename PodT>
constexpr PodTypeKind getPodTypeKind()
{
    using value_t = remove_cvref_t<PodT>;

    if constexpr (std::is_same_v<value_t, std::string>)
    {
        return PodTypeKind::str;
    }
    else if constexpr (std::is_same_v<value_t, char>)
    {
        return PodTypeKind::c;
    }
    else if constexpr (std::is_same_v<value_t, double>)
    {
        return PodTypeKind::d;
    }
    else if constexpr (std::is_same_v<value_t, float>)
    {
        return PodTypeKind::f;
    }
    else if constexpr (std::is_same_v<value_t, bool>)
    {
        return PodTypeKind::logical;
    }
    else if constexpr (std::is_integral_v<value_t> && std::is_signed_v<value_t>)
    {
        if constexpr (sizeof(value_t) == 1) return PodTypeKind::i8;
        if constexpr (sizeof(value_t) == 2) return PodTypeKind::i16;
        if constexpr (sizeof(value_t) == 4) return PodTypeKind::i32;
        if constexpr (sizeof(value_t) == 8) return PodTypeKind::i64;
    }
    else if constexpr (std::is_integral_v<value_t> && std::is_unsigned_v<value_t>)
    {
        if constexpr (sizeof(value_t) == 1) return PodTypeKind::ui8;
        if constexpr (sizeof(value_t) == 2) return PodTypeKind::ui16;
        if constexpr (sizeof(value_t) == 4) return PodTypeKind::ui32;
        if constexpr (sizeof(value_t) == 8) return PodTypeKind::ui64;
    }
    else
    {
        static_assert(!std::is_same_v<value_t, value_t>, "Unsupported POD leaf type for DataTypeHierarchy");
    }
}

template <typename T, typename = void>
struct has_argos_collector : std::false_type {};

template <typename T>
struct has_argos_collector<T, std::void_t<typename remove_cvref_t<T>::ArgosCollector>> : std::true_type {};

template <typename T>
inline constexpr bool has_argos_collector_v = has_argos_collector<T>::value;

} // namespace detail

template <typename EnumT>
struct EnumDescriptor
{
    static std::vector<EnumMember> members()
    {
        return {};
    }
};

class DataTypeHierarchyBase
{
public:
    virtual ~DataTypeHierarchyBase() = default;
    virtual const DataTypeNode& getRoot() const = 0;
};

template <typename RootT>
class DataTypeHierarchy : public DataTypeHierarchyBase
{
public:
    const DataTypeNode& getRoot() const override
    {
        return root_;
    }

    void writeBuffer(std::vector<char>& buffer, const RootT* value) const
    {
        if (value == nullptr)
        {
            return;
        }
        if (root_.write_erased)
        {
            root_.write_erased(buffer, value);
        }
    }

private:
    template <typename T>
    friend std::unique_ptr<DataTypeHierarchy<detail::remove_cvref_t<T>>> createDataTypeHier();

    DataTypeNode root_;
};

template <typename T>
std::unique_ptr<DataTypeHierarchy<detail::remove_cvref_t<T>>> createDataTypeHier()
{
    using value_t = detail::remove_cvref_t<T>;
    auto hier = std::make_unique<DataTypeHierarchy<value_t>>();
    auto& node = hier->root_;
    node.type_name = demangle_type<value_t>();

    if constexpr (std::is_enum_v<value_t>)
    {
        node.kind = NodeKind::Enum;
        node.enum_meta = std::make_unique<EnumMeta>();
        using enum_int_t = std::underlying_type_t<value_t>;
        node.enum_meta->backing_kind = detail::getBackingKind<enum_int_t>();
        node.enum_meta->members = EnumDescriptor<value_t>::members();
        node.write_erased = [](std::vector<char>& buffer, const void* value_void) {
            const auto* value = static_cast<const value_t*>(value_void);
            const auto raw = static_cast<enum_int_t>(*value);
            const auto* bytes = reinterpret_cast<const char*>(&raw);
            buffer.insert(buffer.end(), bytes, bytes + sizeof(raw));
        };
    }
    else if constexpr (detail::has_argos_collector_v<value_t>)
    {
        node.kind = NodeKind::Struct;

        std::vector<std::string> active_struct_stack{node.type_name};

        auto populate_children = [&](DataTypeNode& parent,
                                     const auto& fields,
                                     auto&& self) -> std::function<void(std::vector<char>&, const void*)>
        {
            std::vector<std::function<void(std::vector<char>&, const void*)>> child_writers;

            for (const auto* field : fields)
            {
                if (field == nullptr)
                {
                    continue;
                }

                auto child = std::make_unique<DataTypeNode>();
                child->parent = &parent;
                child->field_name = field->getName();
                child->type_name = field->getTypeName();
                child->source_field = const_cast<void*>(static_cast<const void*>(field));

                if (field->isStructField())
                {
                    child->kind = NodeKind::Struct;
                    child->type_name = field->getStructTypeName();

                    if (std::find(active_struct_stack.begin(),
                                  active_struct_stack.end(),
                                  child->type_name) != active_struct_stack.end())
                    {
                        throw DBException(
                            "Recursive struct cycle detected while building data type hierarchy");
                    }

                    active_struct_stack.emplace_back(child->type_name);
                    auto nested_writer = self(*child, field->getStructFields(), self);
                    active_struct_stack.pop_back();

                    child->write_erased = [field, nested_writer](std::vector<char>& buffer, const void* parent_void) {
                        const auto nested_ptr = field->getStructPtrErased(parent_void);
                        if (nested_ptr == nullptr)
                        {
                            return;
                        }
                        nested_writer(buffer, nested_ptr);
                    };
                    child_writers.emplace_back(child->write_erased);
                }
                else if (field->isEnumField())
                {
                    child->kind = NodeKind::Enum;
                    child->enum_meta = std::make_unique<EnumMeta>();
                    child->enum_meta->backing_kind = field->getEnumBackingKind();
                    child->enum_meta->members = field->getEnumMembers();

                    child->write_erased = [field](std::vector<char>& buffer, const void* parent_void) {
                        field->writeBufferErased(buffer, parent_void);
                    };
                    child_writers.emplace_back(child->write_erased);
                }
                else
                {
                    child->kind = NodeKind::Pod;
                    child->pod_type = std::make_unique<PodTypeKind>(field->getPodTypeKind());

                    child->write_erased = [field](std::vector<char>& buffer, const void* parent_void) {
                        field->writeBufferErased(buffer, parent_void);
                    };
                    child_writers.emplace_back(child->write_erased);
                }

                parent.children.emplace_back(std::move(child));
            }

            return [child_writers](std::vector<char>& buffer, const void* owner_void) {
                for (const auto& writer : child_writers)
                {
                    writer(buffer, owner_void);
                }
            };
        };

        // IMPORTANT: Writers stored in the hierarchy may capture field pointers.
        // To keep those pointers valid beyond this function, the collector must
        // outlive the returned DataTypeHierarchy. For now, keep one static
        // collector instance per collected type.
        static typename value_t::ArgosCollector collector;
        node.write_erased = populate_children(node,
                                              collector.getFields(),
                                              populate_children);
    }
    else if constexpr (detail::is_pod_leaf_v<value_t>)
    {
        node.kind = NodeKind::Pod;
        node.pod_type = std::make_unique<PodTypeKind>(detail::getPodTypeKind<value_t>());
        if constexpr (std::is_same_v<value_t, std::string>)
        {
            node.write_erased = [&node](std::vector<char>& buffer, const void* value_void) {
                if (node.tiny_strings == nullptr)
                {
                    throw DBException("TinyStrings not set before string collection");
                }
                const auto* s = static_cast<const value_t*>(value_void);
                const uint32_t id = node.tiny_strings->getStringID(*s);
                const auto* bytes = reinterpret_cast<const char*>(&id);
                buffer.insert(buffer.end(), bytes, bytes + sizeof(id));
            };
        }
        else
        {
            node.write_erased = [](std::vector<char>& buffer, const void* value_void) {
                const auto* value = static_cast<const value_t*>(value_void);
                const auto* bytes = reinterpret_cast<const char*>(value);
                buffer.insert(buffer.end(), bytes, bytes + sizeof(value_t));
            };
        }
    }
    else
    {
        static_assert(detail::has_argos_collector_v<value_t>,
                      "Struct-like types must provide nested ArgosCollector");
    }

    return hier;
}

} // namespace simdb::collection
