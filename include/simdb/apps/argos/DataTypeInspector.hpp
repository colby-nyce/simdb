// <DataTypeInspector.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/SerializedTreeNode.hpp"
#include "simdb/apps/argos/SimpleTypesSerializer.hpp"
#include "simdb/apps/argos/EnumSerializer.hpp"
#include "simdb/apps/argos/StructSerializer.hpp"
#include "simdb/utils/Demangle.hpp"

#include <map>
#include <memory>
#include <string>
#include <type_traits>

namespace simdb::collection {

/// \class DTypeHandler
/// \brief TODO cnyce
class DTypeHandler
{
public:
    virtual ~DTypeHandler() = default;
    virtual void applyVisitor(DataTypeVisitor* visitor) const = 0;
};

/// \class SimpleDTypeHandler
/// \brief TODO cnyce
template <typename SimpleTypeT>
class SimpleDTypeHandler : public DTypeHandler
{
public:
    void applyVisitor(DataTypeVisitor* visitor) const override final
    {
        visitor->visitSimpleType(simple_type<SimpleTypeT>::type_enum);
    }
};

/// \class EnumHandler
/// \brief TODO cnyce
template <typename EnumT>
class EnumHandler : public DTypeHandler
{
public:
    void applyVisitor(DataTypeVisitor* visitor) const override final
    {
        using int_t = std::underlying_type_t<EnumT>;
        visitor->visitEnum(demangle_type<EnumT>(), enum_type<int_t>::type_enum);
    }
};

/// \class StructHandler
/// \brief TODO cnyce
template <typename StructT>
class StructHandler : public DTypeHandler
{
public:
    void applyVisitor(DataTypeVisitor* visitor) const override final
    {
        auto struct_name = demangle_type<StructT>();
        visitor->visitStruct(struct_name);

        using collector_t = typename StructT::ArgosCollector;
        collector_t collector;
        collector.applyVisitor(visitor);
    }
};

/// \class DataTypeInspector
/// \brief TODO cnyce
class DataTypeInspector
{
public:
    /// \brief TODO cnyce
    template <typename Type>
    void registerType()
    {
        using value_type = type_traits::remove_any_pointer_t<Type>;
        auto type_name = demangle_type<value_type>();
        auto& handler = root_dtype_handlers_[type_name];
        if (!handler)
        {
            if constexpr (std::is_enum_v<value_type>)
            {
                handler = std::make_unique<EnumHandler<value_type>>();
            }
            else if constexpr (std::is_trivial_v<value_type> && std::is_standard_layout_v<value_type>)
            {
                handler = std::make_unique<SimpleDTypeHandler<value_type>>();
            }
            else
            {
                handler = std::make_unique<StructHandler<value_type>>();
            }
        }
    }

    void applyVisitor(DataTypeVisitor* visitor) const
    {
        for (auto& [dtype_name, dtype_handler] : root_dtype_handlers_)
        {
            dtype_handler->applyVisitor(visitor);
        }
    }

    void serialize(DatabaseManager*)
    {
    }

private:
    std::map<std::string, std::unique_ptr<DTypeHandler>> root_dtype_handlers_;
};

} // namespace simdb::collection
