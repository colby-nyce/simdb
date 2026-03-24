// <EnumSerializer.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/ElementTreeNode.hpp"
#include "simdb/utils/Demangle.hpp"

#include <type_traits>
#include <map>
#include <string>

namespace simdb::collection {

template <typename EnumT>
using enum_int_t = typename std::underlying_type<EnumT>::type;

template <typename EnumT>
inline void defineEnumMap(std::map<std::string, enum_int_t<EnumT>>& map)
{
    static_assert(false, "Must implement template specialization");
}

/// \class EnumTreeNode
/// \brief TreeNode which represents an enum
/// \tparam EnumT enum type to serialize
/// \note Serialization guaranteed to only occur once; no dups in database
template <typename EnumT>
class EnumTreeNode : public SerializedTreeNode
{
public:
    using SerializedTreeNode::SerializedTreeNode;

private:
    int serialize_(DatabaseManager* db_mgr) override final
    {
        auto enum_name = demangle_type<EnumT>();
        auto int_type = demangle_type<enum_int_t<EnumT>>();
        auto enum_record = db_mgr->INSERT(SQL_TABLE("Enums"), SQL_VALUES(enum_name, int_type));
        auto enum_id = enum_record->getId();

        std::map<std::string, enum_int_t<EnumT>> map;
        defineEnumMap(map);

        for (const auto& [enum_name, enum_value] : map)
        {
            db_mgr->INSERT(SQL_TABLE("EnumFields"), SQL_VALUES(enum_id, enum_name, enum_value));
        }

        return enum_id;
    }
};

/// \class EnumSerializer
/// \brief This class handles serialization of all enum data types found
/// in an Argos collection
class EnumSerializer
{
public:
    /// \brief Construct using another tree. All enums will be placed
    /// under the given tree's "root.enums" node.
    explicit EnumSerializer(SerializedTree& tree)
        : enums_node_(tree.createNode<ElementTreeNode>("enums"))
    {}

    /// \brief Construct with a new tree.
    EnumSerializer()
        : owned_tree_(std::make_unique<SerializedTree>())
        , enums_node_(owned_tree_->createNode<ElementTreeNode>("enums"))
    {}

    template <typename EnumT>
    void registerEnum()
    {
        auto enum_name = demangle_type<EnumT>();
        enums_node_->addChild<EnumTreeNode<EnumT>>(enum_name);
    }

    template <typename EnumT>
    int getEnumDbId(bool must_exist = true) const
    {
        auto enum_name = demangle_type<EnumT>();
        auto enum_node = enums_node_->getChildAs<SerializedTreeNode>(enum_name, must_exist);
        if (!enum_node)
        {
            return 0;
        }
        return enum_node->getDbId(must_exist);
    }

private:
    std::unique_ptr<SerializedTree> owned_tree_;
    SerializedTreeNode* enums_node_ = nullptr;
};

} // namespace simdb::collection
