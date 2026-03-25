// <DataTypeSerializer.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/SimpleTypesSerializer.hpp"
#include "simdb/apps/argos/EnumSerializer.hpp"
#include "simdb/apps/argos/StructSerializer.hpp"

#include <ostream>

namespace simdb::collection {

/// \class DataTypeSerializer
/// \brief This class wraps various serialers and is used to write
/// all data type information about collected data types
class DataTypeSerializer
{
public:
    /// \brief Construct with a new tree.
    DataTypeSerializer()
        : owned_tree_(SerializedTree::createDefault())
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

    /// \brief Get serializer for collected trivial scalar (non-enum)
    SimpleTypesSerializer& getSimpleTypes()
    {
        return simple_serializer_;
    }

    /// \brief Get serializer for collected enums
    EnumSerializer& getEnumTypes()
    {
        return enum_serializer_;
    }

    /// \brief Get serializer for collected structs
    StructSerializer& getStructTypes()
    {
        return struct_serializer_;
    }

    /// \brief Get correct serializer for the given type
    template <typename ValueType>
    auto* getSerializer()
    {
        using value_type = type_traits::remove_any_pointer_t<ValueType>;
        if constexpr (std::is_enum_v<value_type>)
        {
            return &enum_serializer_;
        }
        else if constexpr (std::is_trivial_v<value_type> && std::is_standard_layout_v<value_type>)
        {
            return &simple_serializer_;
        }
        else
        {
            return &struct_serializer_;
        }
    }

    /// \brief Depth-first serialization of every registered type in one transaction
    void serialize(DatabaseManager* db_mgr)
    {
        db_mgr->safeTransaction([&]()
        {
            simple_serializer_.serialize(db_mgr);
            enum_serializer_.serialize(db_mgr);
            struct_serializer_.serialize(db_mgr);
        });
    }

    /// \brief Breadth-first serialization of every registered type in one transaction
    void serializeBFS(DatabaseManager* db_mgr)
    {
        db_mgr->safeTransaction([&]()
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
