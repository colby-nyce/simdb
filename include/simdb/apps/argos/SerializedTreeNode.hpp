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

    void print(std::ostream& os, unsigned depth = 0) const override
    {
        for (unsigned i = 0; i < depth; ++i)
        {
            os << "  ";
        }
        os << getName();
        if (const int id = getDbId(false); id != 0)
        {
            os << " [db_id=" << id << ']';
        }
        os << '\n';
        for (const auto& child : getChildren())
        {
            child->print(os, depth + 1);
        }
    }

private:
    virtual int serialize_(DatabaseManager* db_mgr) = 0;
    int db_id_ = 0;
};

/// \class SerializedTree
/// \brief Tree subclass which adds serialization APIs
class SerializedTree : public Tree
{
public:
    using Tree::Tree;

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
