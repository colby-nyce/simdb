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
    /// \brief Construct using another tree. All simple types will be placed
    /// under the given tree's "builtins" node.
    explicit SimpleTypesSerializer(SerializedTree& tree)
        : builtins_node_(tree.createNode<ElementTreeNode>("builtins"))
    {}

    /// \brief Construct with a new tree.
    SimpleTypesSerializer()
        : owned_tree_(std::make_unique<SerializedTree>())
        , builtins_node_(owned_tree_->createNode<ElementTreeNode>("builtins"))
    {}

    template <typename SimpleTypeT>
    void registerBuiltIn()
    {
        auto type_name = demangle_type<SimpleTypeT>();
        builtins_node_->addChild<SimpleTypeTreeNode<SimpleTypeT>>(type_name);
    }

    template <typename SimpleTypeT>
    int getBuiltInDbId(bool must_exist = true) const
    {
        auto type_name = demangle_type<SimpleTypeT>();
        auto* builtin_node = builtins_node_->getChildAs<SerializedTreeNode>(type_name, must_exist);
        if (!builtin_node)
        {
            return 0;
        }
        return builtin_node->getDbId(must_exist);
    }

private:
    std::unique_ptr<SerializedTree> owned_tree_;
    SerializedTreeNode* builtins_node_ = nullptr;
};

} // namespace simdb::collection
