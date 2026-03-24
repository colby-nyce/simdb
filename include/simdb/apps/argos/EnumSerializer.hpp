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
        defineEnumMap<EnumT>(map);

        auto inserter = db_mgr->prepareINSERT(SQL_TABLE("EnumFields"));
        inserter->setColumnValue(0, enum_id);
        for (const auto& [enum_name, enum_value] : map)
        {
            inserter->setColumnValue(1, enum_name);
            if constexpr (std::is_same_v<enum_int_t<EnumT>, uint64_t>)
            {
                inserter->setColumnValue(2, enum_value);
            }
            else
            {
                inserter->setColumnValue(3, static_cast<int64_t>(enum_value));
            }
            inserter->createRecord();
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
    /// \brief Construct with a new tree. All enums will be placed
    /// under our tree's "root.dtypes.enums" node.
    EnumSerializer()
        : owned_tree_(std::make_unique<SerializedTree>())
        , tree_(owned_tree_.get())
    {}

    /// \brief Construct using another tree. All enums will be placed
    /// under the given tree's "root.dtypes.enums" node.
    explicit EnumSerializer(SerializedTree& tree)
        : tree_(&tree)
    {}

    /// \brief Register enum \a EnumT under the shared \c enums tree folder
    /// \tparam EnumT Enumeration type; the program must provide \c defineEnumMap<EnumT>
    template <typename EnumT>
    void registerEnum()
    {
        using enum_t = type_traits::remove_any_pointer_t<EnumT>;
        auto enum_name = demangle_type<enum_t>();
        auto parent = getEnumsNode_();
        parent->addChild<EnumTreeNode<enum_t>>(enum_name);
    }

    /// \brief Look up the database row id for a registered enum type
    /// \tparam EnumT Same type key used with \ref registerEnum
    /// \param must_exist If true, missing nodes trigger assertions in the tree API
    /// \return Primary key from the last \ref serialize_, or 0 if \a must_exist is false and the type is absent
    template <typename EnumT>
    int getEnumDbId(bool must_exist = true) const
    {
        using enum_t = type_traits::remove_any_pointer_t<EnumT>;
        auto enum_name = demangle_type<enum_t>();
        auto parent = getEnumsNode_();
        auto enum_node = parent->getChildAs<SerializedTreeNode>(enum_name, must_exist);
        if (!enum_node)
        {
            return 0;
        }
        return enum_node->getDbId(must_exist);
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
    /// \return Lazy-created \c "enums" grouping node under this serializer's tree
    SerializedTreeNode* getEnumsNode_() const
    {
        if (!enums_node_)
        {
            enums_node_ = tree_->createNodes<ElementTreeNode>("dtypes.enums");
        }
        return enums_node_;
    }

    /// \brief Tree storage when this serializer constructs the root (constructor without tree)
    std::unique_ptr<SerializedTree> owned_tree_;

    /// \brief Tree receiving enum nodes; non-owning when the serializer shares an external tree
    SerializedTree *const tree_;

    /// \brief Cached handle to the \c enums child folder node
    mutable SerializedTreeNode* enums_node_ = nullptr;
};

} // namespace simdb::collection
