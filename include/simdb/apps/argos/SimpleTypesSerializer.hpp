// <SimpleTypesSerializer.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/ElementTreeNode.hpp"
#include "simdb/utils/Demangle.hpp"

#include <memory>
#include <string>
#include <type_traits>

namespace simdb::collection {

/// \class SimpleTypeTreeNode
/// \brief TreeNode which represents a collected simple (scalar) type
/// \tparam SimpleTypeT C++ type to record (trivial, standard-layout, non-enum)
/// \note Serialization guaranteed to only occur once; no dups in database
template <typename SimpleTypeT>
class SimpleTypeTreeNode : public SerializedTreeNode
{
    static_assert(std::is_trivial_v<SimpleTypeT> && std::is_standard_layout_v<SimpleTypeT> && !std::is_enum_v<SimpleTypeT>,
                  "SimpleTypeTreeNode only supports trivial, standard-layout, non-enum types");

public:
    using SerializedTreeNode::SerializedTreeNode;

private:
    int serialize_(DatabaseManager* db_mgr) override final
    {
        auto demangled_name = demangle_type<SimpleTypeT>();
        auto num_bytes = static_cast<int>(sizeof(SimpleTypeT));
        auto record = db_mgr->INSERT(SQL_TABLE("CollectedDataTypes"), SQL_VALUES(demangled_name, num_bytes));
        return record->getId();
    }
};

/// \class SimpleTypesSerializer
/// \brief Handles serialization of simple scalar types used in an Argos collection
class SimpleTypesSerializer
{
public:
    /// \brief Construct with a new tree. All simple types will be placed
    /// under our tree's "root.dtypes.simple" node.
    SimpleTypesSerializer()
        : owned_tree_(std::make_unique<SerializedTree>())
        , tree_(owned_tree_.get())
    {}

    /// \brief Construct using another tree. All simple types will be placed
    /// under the given tree's "root.dtypes.simple" node.
    explicit SimpleTypesSerializer(SerializedTree& tree)
        : tree_(&tree)
    {}

    /// \brief Register scalar type \a SimpleTypeT under the shared \c builtins tree folder
    /// \tparam SimpleTypeT Trivial, standard-layout, non-enum type (see \ref SimpleTypeTreeNode)
    template <typename SimpleTypeT>
    void registerSimpleType()
    {
        using simple_t = type_traits::remove_any_pointer_t<SimpleTypeT>;
        auto type_name = demangle_type<simple_t>();
        auto parent = getBuiltInsNode_();
        parent->addChild<SimpleTypeTreeNode<simple_t>>(type_name);
    }

    /// \brief Look up the database row id for a registered built-in type
    /// \tparam SimpleTypeT Same type key used with \ref registerBuiltIn
    /// \param must_exist If true, missing nodes trigger assertions in the tree API
    /// \return Primary key from the last \ref serialize_, or 0 if \a must_exist is false and the type is absent
    template <typename SimpleTypeT>
    int getBuiltInDbId(bool must_exist = true) const
    {
        auto type_name = demangle_type<SimpleTypeT>();
        auto parent = getBuiltInsNode_();
        auto builtin_node = parent->getChildAs<SerializedTreeNode>(type_name, must_exist);
        if (!builtin_node)
        {
            return 0;
        }
        return builtin_node->getDbId(must_exist);
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

    /// \brief Tree this serializer reads/writes (owned or shared).
    SerializedTree* getTree() { return tree_; }

    /// \brief Tree this serializer reads/writes (owned or shared).
    const SerializedTree* getTree() const { return tree_; }

private:
    /// \return Lazy-created \c "builtins" grouping node under this serializer's tree
    SerializedTreeNode* getBuiltInsNode_() const
    {
        if (!builtins_node_)
        {
            builtins_node_ = tree_->createNodes<ElementTreeNode>("dtypes.simples");
        }
        return builtins_node_;
    }

    /// \brief Tree storage when this serializer constructs the root (constructor without tree)
    std::unique_ptr<SerializedTree> owned_tree_;

    /// \brief Tree receiving built-in type nodes; non-owning when the serializer shares an external tree
    SerializedTree *const tree_;

    /// \brief Cached handle to the \c builtins child folder node
    mutable SerializedTreeNode* builtins_node_ = nullptr;
};

} // namespace simdb::collection
