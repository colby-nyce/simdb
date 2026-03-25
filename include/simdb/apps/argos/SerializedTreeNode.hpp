// <SerializedTreeNode.hpp> -*- C++ -*-

#pragma once

#include "simdb/utils/Tree.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/Exceptions.hpp"

#include <ostream>

namespace simdb::collection {

/// \class SerializedTreeNode
/// \brief TreeNode subclass which supports one-time serialization to the DB
class SerializedTreeNode : public Tree::TreeNode
{
public:
    using Tree::TreeNode::TreeNode;

    /// \brief Get our database ID after calling serialize()
    /// \throw Throws if must_exist and serialize() was never called
    /// \return Returns the ID of the written DB record
    int getDbId(bool must_exist = true) const
    {
        if (db_id_ == 0 && must_exist)
        {
            throw DBException("Serializer has not run yet");
        }
        return db_id_;
    }

    /// \brief Run the serializer, or a no-op if already run
    /// \return Returns the ID of the written DB record
    int serialize(DatabaseManager* db_mgr)
    {
        if (auto id = getDbId(false))
        {
            return id;
        }
        db_id_ = serialize_(db_mgr);
        if (db_id_ < 0)
        {
            throw DBException("Invalid database ID");
        }
        return db_id_;
    }

private:
    virtual int serialize_(DatabaseManager* db_mgr) = 0;
    int db_id_ = 0;
};

/// \class DefaultSerializedRootNode
/// \brief Default implemenetation of the root tree node
/// for the SerializedTree class.
class DefaultSerializedRootNode : public SerializedTreeNode
{
public:
    explicit DefaultSerializedRootNode(const std::string& root_name)
        : SerializedTreeNode(root_name)
    {}

private:
    int serialize_(DatabaseManager* db_mgr) override final
    {
        auto record = db_mgr->INSERT(SQL_TABLE("ElementTreeNodes"), SQL_VALUES(getName(), 0));
        return record->getId();
    };
};

/// \class SerializedTree
/// \brief Tree subclass which adds serialization APIs
class SerializedTree : public Tree
{
public:
    template <typename RootT>
    explicit SerializedTree(std::unique_ptr<RootT> root) :
        Tree(std::move(root))
    {}

    static std::unique_ptr<SerializedTree> createDefault(const std::string& root_name = "root")
    {
        return createWithRoot<DefaultSerializedRootNode>(root_name);
    }

    template <typename RootT, typename... Args>
    static std::unique_ptr<SerializedTree> createWithRoot(const std::string& root_name, Args&&... args)
    {
        return createWithNamedRoot<RootT>(root_name, std::forward<Args>(args)...);
    }

    template <typename RootT, typename... Args>
    static std::unique_ptr<SerializedTree> createWithNamedRoot(const std::string& root_name, Args&&... args)
    {
        auto root = std::make_unique<RootT>(root_name, std::forward<Args>(args)...);
        return std::make_unique<SerializedTree>(std::move(root));
    }

    /// \brief Serialize everything about this node to the database
    /// using depth-first traversal
    void serialize(DatabaseManager* db_mgr)
    {
        db_mgr->safeTransaction([&]()
        {
            dfsTypedNodes<SerializedTreeNode>([&](SerializedTreeNode* node)
            {
                node->serialize(db_mgr);
                return true;
            });
        });
    }

    /// \brief Serialize everything about this node to the database
    /// using breadth-first traversal
    void serializeBFS(DatabaseManager* db_mgr)
    {
        db_mgr->safeTransaction([&]()
        {
            dfsTypedNodes<SerializedTreeNode>([&](SerializedTreeNode* node)
            {
                node->serialize(db_mgr);
                return true;
            });
        });
    }
};

} // namespace simdb::collection
