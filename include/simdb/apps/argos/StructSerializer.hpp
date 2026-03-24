// <StructSerializer.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/ElementTreeNode.hpp"
#include "simdb/utils/StreamBuffer.hpp"
#include "simdb/utils/Demangle.hpp"

namespace simdb::collection {

class DataTypesSerializer;
class StructFieldBase;

template <typename Collector, typename FieldT, auto GetterFunc>
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
    template <typename Collector, typename FieldT, auto GetterFunc>
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

    /// \brief Records this field's data type with the shared type registry
    /// \param serializer Target type registry for the collection pipeline
    /// \note Implementations may be stubs until DataTypesSerializer is wired in.
    virtual void registerType(DataTypesSerializer* serializer) const = 0;
};

/// \class StructField
/// \brief Concrete field: name, C++ value type, and how to read it from the collector
/// \tparam Collector Type that owns the value being sampled (often a packet or struct adapter)
/// \tparam FieldT C++ type of the stored field value
/// \tparam GetterFunc Non-type template parameter: invocable as \c GetterFunc(collector) yielding \c FieldT
template <typename Collector, typename FieldT, auto GetterFunc>
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

    /// \note Stub; no database / schema emission yet
    void registerType(DataTypesSerializer* serializer) const override final
    {
        (void)serializer;
    }

    /// \brief Append the field value for \a collector to the binary stream
    void writeTo(StreamBuffer& buffer, const Collector* collector) const
    {
        buffer << GetterFunc(collector);
    }

private:
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

    /// \brief Registers every field's type with the shared serializer
    /// \note Implementations may be stubs until DataTypesSerializer is wired in.
    virtual void registerTypes(DataTypesSerializer* serializer) const = 0;

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
    std::string getName() const override { return demangle_type<StructT>(); }

    /// \note Stub; should iterate \ref StructFields and register each field when complete
    void registerTypes(DataTypesSerializer* serializer) const override final
    {
        (void)serializer;
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
        : owned_tree_(std::make_unique<SerializedTree>())
        , tree_(owned_tree_.get())
    {}

    /// \brief Construct using another tree. All structs will be placed
    /// under the given tree's "root.dtypes.structs" node.
    explicit StructSerializer(SerializedTree& tree)
        : tree_(&tree)
    {}

    /// \brief Ensure \a StructT participates in type registration (tree node TBD)
    /// \tparam StructT C++ struct or class type to expose in the Argos schema
    /// \note Implementation pending; no child node is attached yet.
    template <typename StructT>
    void registerStruct()
    {
        //TODO cnyce
        //[[maybe_unused]] using struct_t = type_traits::remove_any_pointer_t<StructT>;
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
    SerializedTreeNode* getStructsNode_() const
    {
        if (!structs_node_)
        {
            structs_node_ = tree_->createNodes<ElementTreeNode>("dtypes.structs");
        }
        return structs_node_;
    }

    /// \brief Tree storage when this serializer constructs the root (constructor without tree)
    std::unique_ptr<SerializedTree> owned_tree_;

    /// \brief Tree receiving struct nodes; non-owning when the serializer shares an external tree
    SerializedTree *const tree_;

    /// \brief Cached handle to the \c structs child folder node
    mutable SerializedTreeNode* structs_node_ = nullptr;
};

} // namespace simdb::collection

/// \brief Opens a collector class body derived from \ref simdb::collection::StructCollector
#define ARGOS_COLLECTOR(Class) class Class : public simdb::collection::StructCollector<Class> {

/// \brief Closes an \ref ARGOS_COLLECTOR block (placeholder compile-time check)
#define END_ARGOS_COLLECTOR(Class) static_assert(std::is_same_v<decltype(this), Class>) };

/// \brief Declares a \ref simdb::collection::StructField member wired to \c fields_
#define ARGOS_COLLECT(collectable_name, collectable_getter) \
    simdb::collection::StructField<collectable_getter> collectable_name_{&fields_};
