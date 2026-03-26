// <StructSerializer.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/ElementTreeNode.hpp"
#include "simdb/apps/argos/DataTypeDescriptors.hpp"
#include "simdb/utils/StreamBuffer.hpp"
#include "simdb/utils/Demangle.hpp"

namespace simdb::collection {

class DataTypeSerializer;
class SimpleTypesSerializer;
class EnumSerializer;
class StructSerializer;
class StructFieldBase;

/// \class DataTypeVisitor
/// \brief TODO cnyce
class DataTypeVisitor
{
public:
    virtual ~DataTypeVisitor() = default;
    virtual void visitSimpleType(SimpleTypes type) 
    {
        (void)type;
    }
    virtual void visitEnum(const std::string& demangled_name, EnumIntTypes int_type)
    {
        (void)demangled_name;
        (void)int_type;
    }
    virtual void visitEnumField(const std::string& key, const std::string& value)
    {
        (void)key;
        (void)value;
    }
    virtual void visitStruct(const std::string& demangled_name)
    {
        (void)demangled_name;
    }
};

// Helper to register nested struct types from inside StructField, without
// requiring StructSerializer to be a complete type at the point StructField
// is defined in this header.
template <typename StructT>
void registerNestedStructType(StructSerializer* serializer);

template <typename Collector, auto GetterFunc>
class StructField;

/// \class StructFields
/// \brief Ordered registry of struct fields for Argos collection metadata
class StructFields
{
public:
    /// \return Pointers to all fields registered with this struct (non-owning)
    const std::vector<StructFieldBase*>& getFields() const
    {
        return fields_;
    }

private:
    template <typename Collector, auto GetterFunc>
    friend class StructField;
    std::vector<StructFieldBase*> fields_;
};

/// \class StructFieldBase
/// \brief Type-erased interface for one field of a collected struct
class StructFieldBase
{
public:
    virtual ~StructFieldBase() = default;

    /// \return Human-readable field name in collected output
    virtual std::string getName() const = 0;

    /// \brief TODO cnyce
    virtual void applyVisitor(DataTypeVisitor* visitor) const = 0;
};

/// \class StructField
/// \brief Concrete field: name, C++ value type, and how to read it from the collector
/// \tparam Collector Type that owns the value being sampled (often a packet or struct adapter)
/// \tparam GetterFunc Non-type template parameter: invocable as \c GetterFunc(collector) yielding \c FieldT
template <typename Collector, auto GetterFunc>
class StructField : public StructFieldBase
{
public:
    /// \param fields Owner list; this field appends itself on construction
    /// \param name Display / schema name for the field
    StructField(StructFields* fields, const std::string& name)
        : name_(name)
    {
        fields->fields_.push_back(this);
    }

    std::string getName() const override final
    {
        return name_;
    }

    void applyVisitor(DataTypeVisitor* visitor) const override final
    {
        using field_t = type_traits::remove_any_pointer_t<type_traits::return_type_t<decltype(GetterFunc)>>;
        if constexpr (std::is_enum_v<field_t>)
        {
            using int_t = std::underlying_type_t<field_t>;
            visitor->visitEnum(demangle_type<field_t>(), enum_type<int_t>::type_enum);

            std::map<std::string, int_t> map;
            defineEnumMap<field_t>(map);
            for (const auto& [key, value] : map)
            {
                visitor->visitEnumField(key, std::to_string(value));
            }
        }
        else if constexpr (std::is_trivial_v<field_t> && std::is_standard_layout_v<field_t>)
        {

        }
        else
        {

        }
    }

    void writeTo(StreamBuffer& buffer, const Collector* collector) const
    {
        buffer << std::invoke(GetterFunc, collector);
    }

private:
    template <typename SimpleTypeT>
    void registerSimple_(SimpleTypesSerializer* serializer) const
    {
        serializer->template registerSimpleType<SimpleTypeT>();
    }

    template <typename EnumT>
    void registerEnum_(EnumSerializer* serializer) const
    {
        serializer->template registerEnum<EnumT>();
    }

    template <typename StructT>
    void recurseRegisterStruct_(StructSerializer* serializer) const
    {
        using struct_t = type_traits::remove_any_pointer_t<StructT>;
        registerNestedStructType<struct_t>(serializer);
    }

    const std::string name_;
};

/// \class StructCollectorBase
/// \brief Abstract collector describing a struct-like aggregate and its fields for Argos
class StructCollectorBase
{
public:
    virtual ~StructCollectorBase() = default;

    /// \return Schema name for the struct (typically a demangled type name)
    virtual std::string getName() const = 0;

    /// \brief TODO cnyce
    virtual void applyVisitor(DataTypeVisitor* visitor) const = 0;

    /// \brief Registers every field's type with the shared serializer
    /// \note Implementations may be stubs until DataTypeSerializer is wired in.
    virtual void registerTypes(SimpleTypesSerializer* simple_serializer,
                               EnumSerializer* enum_serializer,
                               StructSerializer* struct_serializer) const = 0;

protected:
    StructFields fields_;
};

/// \class StructCollector
/// \brief Default \ref StructCollectorBase implementation keyed by the collected C++ type
/// \tparam StructT Struct or class type whose fields are described by \ref StructField members
template <typename StructT>
class StructCollector : public StructCollectorBase
{
public:
    using collected_type = StructT;

    std::string getName() const override { return demangle_type<StructT>(); }

    /// \brief TODO cnyce
    void applyVisitor(DataTypeVisitor* visitor) const override final
    {
        for (auto field : fields_.getFields())
        {
            field->applyVisitor(visitor);
        }
    }

    /// \note Stub; should iterate \ref StructFields and register each field when complete
    void registerTypes(SimpleTypesSerializer* simple_serializer,
                       EnumSerializer* enum_serializer,
                       StructSerializer* struct_serializer) const override final
    {
        for (auto field : fields_.getFields())
        {
            field->registerType(simple_serializer, enum_serializer, struct_serializer);
        }
    }
};

/// \class StructSerializer
/// \brief Owns the \ref SerializedTree subtree where collected struct types are registered and serialized
class StructSerializer
{
public:
    /// \brief Construct with a new tree. All structs will be placed
    /// under our tree's "root.dtypes.structs" node.
    StructSerializer()
        : owned_tree_(SerializedTree::createDefault())
        , tree_(owned_tree_.get())
        , simple_serializer_(*tree_)
        , enum_serializer_(*tree_)
    {}

    /// \brief Construct using another tree. All structs will be placed
    /// under the given tree's "root.dtypes.structs" node.
    StructSerializer(SerializedTree& tree)
        : tree_(&tree)
        , simple_serializer_(*tree_)
        , enum_serializer_(*tree_)
    {}

    /// \brief Ensure \a StructT participates in type registration (tree node TBD)
    /// \tparam StructT C++ struct or class type to expose in the Argos schema
    /// \note Implementation pending; no child node is attached yet.
    template <typename StructT>
    void registerStruct()
    {
        using struct_t = type_traits::remove_any_pointer_t<StructT>;
        auto struct_name = demangle_type<struct_t>();
        auto parent = getStructsNode_();
        parent->addChild(struct_name);
        recurseRegisterStructTypes_<struct_t>();
    }

    /// \brief Look up the database row id for a registered struct type
    /// \tparam StructT Same type key used with \ref registerStruct
    /// \param must_exist If true, missing nodes trigger assertions in the tree API
    /// \return Primary key from the last \ref serialize_, or 0 if \a must_exist is false and the type is absent
    template <typename StructT>
    int getStructDbId(bool must_exist = true) const
    {
        auto struct_name = demangle_type<StructT>();
        auto parent = getStructsNode_();
        auto struct_node = parent->getChildAs<SerializedTreeNode>(struct_name, must_exist);
        if (!struct_node)
        {
            return 0;
        }
        return struct_node->getDbId(must_exist);
    }

    /// \brief Serialize data types to the database (depth-first traversal)
    void serialize(DatabaseManager* db_mgr)
    {
        tree_->serialize(db_mgr);
    }

    /// \brief Serialize data types to the database (breadth-first traversal)
    void serializeBFS(DatabaseManager* db_mgr)
    {
        tree_->serializeBFS(db_mgr);
    }

private:
    /// \return Lazy-created \c "structs" grouping node under this serializer's tree
    Tree::TreeNode* getStructsNode_() const
    {
        if (!structs_node_)
        {
            structs_node_ = tree_->createNodes<>("dtypes.structs");
        }
        return structs_node_;
    }

    /// \brief Recursively walk the given struct type and register all data types,
    /// including support for nested structs
    template <typename StructT>
    void recurseRegisterStructTypes_()
    {
        using struct_t = type_traits::remove_any_pointer_t<StructT>;
        using collector_t = typename struct_t::ArgosCollector;
        auto collector = std::make_unique<collector_t>();
        collector->registerTypes(&simple_serializer_, &enum_serializer_, this);
    }

    /// \brief Tree storage when this serializer constructs the root (constructor without tree)
    std::unique_ptr<SerializedTree> owned_tree_;

    /// \brief Tree receiving struct nodes; non-owning when the serializer shares an external tree
    SerializedTree *const tree_;

    /// \brief Cached handle to the \c structs child folder node
    mutable Tree::TreeNode* structs_node_ = nullptr;

    /// \brief Keep track of simple data types we encounter in our structs, including nested
    SimpleTypesSerializer simple_serializer_;

    /// \brief Keep track of enums we encounter in our structs, including nested
    EnumSerializer enum_serializer_;
};

template <typename StructT>
inline void registerNestedStructType(StructSerializer* serializer)
{
    serializer->template registerStruct<StructT>();
}

} // namespace simdb::collection

/// Pastes after expanding \c b (needed so \c __COUNTER__ is not glued literally by \c ##).
#define ARGOS_COLLECT_CAT_(a, b) a##b
#define ARGOS_COLLECT_CAT(a, b)  ARGOS_COLLECT_CAT_(a, b)

/// \brief Declares a \ref simdb::collection::StructField member wired to \c fields_
/// \details Safe to invoke multiple times per collector class; each expansion gets a unique
/// member name via \c __COUNTER__.
#define ARGOS_COLLECT(collectable_name, collectable_getter)                      \
    simdb::collection::StructField<typename ArgosCollector::collected_type,      \
                                   collectable_getter>                           \
        ARGOS_COLLECT_CAT(collectable_, __COUNTER__){&fields_, #collectable_name};
