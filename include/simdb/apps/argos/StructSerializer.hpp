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

/// \class TODO cnyce
/// \brief TODO cnyce
class StructFields
{
public:
    /// \brief TODO cnyce
    const std::vector<StructFieldBase*>& getFields() const
    {
        return fields_;
    }

private:
    template <typename Collector, typename FieldT, auto GetterFunc>
    friend class StructField;
    std::vector<StructFieldBase*> fields_;
};

/// \class TODO cnyce
/// \brief TODO cnyce
class StructFieldBase
{
public:
    /// \brief TODO cnyce
    virtual ~StructFieldBase() = default;

    /// \brief TODO cnyce
    virtual std::string getName() const = 0;

    /// \brief TODO cnyce
    virtual void registerType(DataTypesSerializer* serializer) const = 0;
};

/// \class TODO cnyce
/// \brief TODO cnyce
template <typename Collector, typename FieldT, auto GetterFunc>
class StructField : public StructFieldBase
{
public:
    /// \brief TODO cnyce
    StructField(StructFields* fields, const std::string& name) // TODO cnyce (description, getter)
        : name_(name)
    {
        fields->fields_.push_back(this);
    }

    /// \brief TODO cnyce
    std::string getName() const override final
    {
        return name_;
    }

    /// \brief TODO cnyce
    void registerType(DataTypesSerializer* serializer) const override final
    {
        // TODO cnyce
        (void)serializer;
    }

    /// \brief TODO cnyce
    void writeTo(StreamBuffer& buffer, const Collector* collector) const
    {
        buffer << GetterFunc(collector);
    }

private:
    const std::string name_;
};

/// \class StructCollectorBase
/// \brief TODO cnyce
class StructCollectorBase
{
public:
    /// \brief TODO cnyce
    virtual ~StructCollectorBase() = default;

    /// \brief TODO cnyce
    virtual std::string getName() const = 0;

    /// \brief TODO cnyce
    virtual void registerTypes(DataTypesSerializer* serializer) const = 0;

protected:
    StructFields fields_;
};

/// \class StructCollector
/// \tparam StructT Collected struct data type
template <typename StructT>
class StructCollector : public StructCollectorBase
{
public:
    /// \brief TODO cnyce
    std::string getName() const override { return demangle_type<StructT>(); }

    /// \brief TODO cnyce
    void registerTypes(DataTypesSerializer* serializer) const override final
    {
        //TODO cnyce
        (void)serializer;
    }
};

} // namespace simdb::collection

/// \brief TODO cnyce
#define ARGOS_COLLECTOR(Class) class Class : public simdb::collection::StructCollector<Class> {

/// \brief TODO cnyce
#define END_ARGOS_COLLECTOR(Class) static_assert(std::is_same_v<decltype(this), Class>) };

/// \brief TODO cnyce
#define ARGOS_COLLECT(collectable_name, collectable_getter) \
    simdb::collection::StructField<collectable_getter> collectable_name_{&fields_};
