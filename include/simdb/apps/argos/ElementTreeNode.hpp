// <ElementsTree.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/SerializedTreeNode.hpp"

namespace simdb::collection {

class ElementTreeNode : public SerializedTreeNode
{
private:
    using SerializedTreeNode::SerializedTreeNode;

    int serialize_(DatabaseManager* db_mgr) override
    {
        int parent_id = 0;
        if (auto parent = getParentAs<SerializedTreeNode>(false))
        {
            parent_id = parent->getDbId();
        }

        auto record = db_mgr->INSERT(
            SQL_TABLE("ElementTreeNodes"),
            SQL_VALUES(parent_id, getName()));

        return record->getId();
    }
};

} // namespace simdb::collection
