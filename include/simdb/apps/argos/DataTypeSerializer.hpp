// <DataTypeSerializer.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/SimpleTypesSerializer.hpp"
#include "simdb/apps/argos/EnumSerializer.hpp"
#include "simdb/apps/argos/StructSerializer.hpp"

namespace simdb::collection {

/// \class DataTypeSerializer
/// \brief This class wraps various serialers and is used to write
/// all data type information about collected data types
class DataTypeSerializer
{
public:
    /// \brief Construct with a new tree.
    DataTypeSerializer()
        : owned_tree_(std::make_unique<SerializedTree>())
        , simple_serializer_(*owned_tree_)
        , enum_serializer_(*owned_tree_)
        , struct_serializer_(*owned_tree_)
    {}

    /// \brief Construct using another tree.
    explicit DataTypeSerializer(SerializedTree& tree)
        : simple_serializer_(tree)
        , enum_serializer_(tree)
        , struct_serializer_(tree)
    {}

    /// \brief Serializer for collected trivial scalar (built-in) types
    /// \return Reference to the nested simple-type subtree (\c builtins node)
    SimpleTypesSerializer& getSimpleTypes()
    {
        return simple_serializer_;
    }

    /// \brief Serializer for enumerated types and their enumerators
    /// \return Reference to the nested enum subtree (\c enums node)
    EnumSerializer& getEnumTypes()
    {
        return enum_serializer_;
    }

    /// \brief Serializer for aggregate / struct types and their fields
    /// \return Reference to the nested struct subtree (\c structs node)
    StructSerializer& getStructTypes()
    {
        return struct_serializer_;
    }

    /// \brief Depth-first serialization of every registered type in one transaction
    /// \param db_mgr Database connection used for inserts during tree traversal
    void serialize(DatabaseManager* db_mgr)
    {
        db_mgr.safeTransaction([&]()
        {
            simple_serializer_.serialize(db_mgr);
            enum_serializer_.serialize(db_mgr);
            struct_serializer_.serialize(db_mgr);
        });
    }

    /// \brief Breadth-first serialization of every registered type in one transaction
    /// \param db_mgr Database connection used for inserts during tree traversal
    void serializeBFS(DatabaseManager* db_mgr)
    {
        db_mgr.safeTransaction([&]()
        {
            simple_serializer_.serializeBFS(db_mgr);
            enum_serializer_.serializeBFS(db_mgr);
            struct_serializer_.serializeBFS(db_mgr);
        });
    }

private:
    std::unique_ptr<SerializedTree> owned_tree_;
    SimpleTypesSerializer simple_serializer_;
    EnumSerializer enum_serializer_;
    StructSerializer struct_serializer_;
};

} // namespace simdb::collection
