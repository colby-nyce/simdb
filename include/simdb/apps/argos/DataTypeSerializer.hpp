// <DataTypeSerializer.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/DataTypeInspector.hpp"

namespace simdb::collection {

/// \class TODO cnyce
class DataTypeSerializer
{
public:
    static void serialize(DatabaseManager* db_mgr) const
    {

    }

private:
    class Visitor : public DataTypeNodeVisitor
    {
    private:
        void visitSimpleVariable(const std::string& var_name, PodTypeKind simple_type)
        {
            (void)var_name;
            (void)simple_type;
        }
        virtual void visitEnumVariable(const std::string& enum_name,
                                       EnumBackingKind backing_kind,
                                       const std::vector<std::pair<std::string, std::string>>& name_value_pairs)
        {
            (void)enum_name;
            (void)backing_kind;
            (void)name_value_pairs;
        }
        void visitStructVariable(const std::string& struct_name) override
        {
            (void)struct_name;
        }
        void endVisitStructVariable(const std::string& struct_name) override
        {
            (void)struct_name;
        }
    };
};

} // namespace simdb::collection
