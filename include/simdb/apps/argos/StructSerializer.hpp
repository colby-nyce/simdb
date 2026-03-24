// <StructSerializer.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/ElementTreeNode.hpp"
#include "simdb/utils/StreamBuffer.hpp"
#include "simdb/utils/Demangle.hpp"

namespace simdb::collection {

class DataTypesSerializer;
class StructFieldBase;

template <typename Collector, typename FieldT, auto GetterFunc>
class StructField;

/// \class StructFields
/// \brief Ordered registry of struct fields for Argos collection metadata
class StructFields
{
public:
    /// \return Pointers to all fields registered with this struct (non-owning)
    const std::vector<StructFieldBase*>& getFields() const
    {
        return fields_;
    }

private:
    template <typename Collector, typename FieldT, auto GetterFunc>
    friend class StructField;
    std::vector<StructFieldBase*> fields_;
};

/// \class StructFieldBase
/// \brief Type-erased interface for one field of a collected struct
class StructFieldBase
{
public:
    virtual ~StructFieldBase() = default;

    /// \return Human-readable field name in collected output
    virtual std::string getName() const = 0;

    /// \brief Records this field's data type with the shared type registry
    /// \param serializer Target type registry for the collection pipeline
    /// \note Implementations may be stubs until DataTypesSerializer is wired in.
    virtual void registerType(DataTypesSerializer* serializer) const = 0;
};

/// \class StructField
/// \brief Concrete field: name, C++ value type, and how to read it from the collector
/// \tparam Collector Type that owns the value being sampled (often a packet or struct adapter)
/// \tparam FieldT C++ type of the stored field value
/// \tparam GetterFunc Non-type template parameter: invocable as \c GetterFunc(collector) yielding \c FieldT
template <typename Collector, typename FieldT, auto GetterFunc>
class StructField : public StructFieldBase
{
public:
    /// \param fields Owner list; this field appends itself on construction
    /// \param name Display / schema name for the field
    StructField(StructFields* fields, const std::string& name)
        : name_(name)
    {
        fields->fields_.push_back(this);
    }

    std::string getName() const override final
    {
        return name_;
    }

    /// \note Stub; no database / schema emission yet
    void registerType(DataTypesSerializer* serializer) const override final
    {
        (void)serializer;
    }

    /// \brief Append the field value for \a collector to the binary stream
    void writeTo(StreamBuffer& buffer, const Collector* collector) const
    {
        buffer << GetterFunc(collector);
    }

private:
    const std::string name_;
};

/// \class StructCollectorBase
/// \brief Abstract collector describing a struct-like aggregate and its fields for Argos
class StructCollectorBase
{
public:
    virtual ~StructCollectorBase() = default;

    /// \return Schema name for the struct (typically a demangled type name)
    virtual std::string getName() const = 0;

    /// \brief Registers every field's type with the shared serializer
    /// \note Implementations may be stubs until DataTypesSerializer is wired in.
    virtual void registerTypes(DataTypesSerializer* serializer) const = 0;

protected:
    StructFields fields_;
};

/// \class StructCollector
/// \brief Default \ref StructCollectorBase implementation keyed by the collected C++ type
/// \tparam StructT Struct or class type whose fields are described by \ref StructField members
template <typename StructT>
class StructCollector : public StructCollectorBase
{
public:
    std::string getName() const override { return demangle_type<StructT>(); }

    /// \note Stub; should iterate \ref StructFields and register each field when complete
    void registerTypes(DataTypesSerializer* serializer) const override final
    {
        (void)serializer;
    }
};

} // namespace simdb::collection

/// \brief Opens a collector class body derived from \ref simdb::collection::StructCollector
#define ARGOS_COLLECTOR(Class) class Class : public simdb::collection::StructCollector<Class> {

/// \brief Closes an \ref ARGOS_COLLECTOR block (placeholder compile-time check)
#define END_ARGOS_COLLECTOR(Class) static_assert(std::is_same_v<decltype(this), Class>) };

/// \brief Declares a \ref simdb::collection::StructField member wired to \c fields_
#define ARGOS_COLLECT(collectable_name, collectable_getter) \
    simdb::collection::StructField<collectable_getter> collectable_name_{&fields_};
