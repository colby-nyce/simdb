// <DataTypeDescriptors.hpp> -*- C++ -*-

#pragma once

namespace simdb::collection {

enum class BasicTypes { SIMPLE, ENUM, STRUCT };
enum class SimpleTypes { c, i8, ui8, i16, ui16, i32, ui32, i64, ui64, d, f, str, logical };
enum class EnumIntTypes { i8, ui8, i16, ui16, i32, ui32, i64, ui64 };

template <typename> struct simple_type;
#define DEFINE_SIMPLE_TYPE(type, field) \
    template <> struct simple_type<type> { static constexpr auto type_enum = SimpleTypes::field; }

DEFINE_SIMPLE_TYPE(char,        c);
DEFINE_SIMPLE_TYPE(int8_t,      i8);
DEFINE_SIMPLE_TYPE(int16_t,     i16);
DEFINE_SIMPLE_TYPE(int32_t,     i32);
DEFINE_SIMPLE_TYPE(int64_t,     i64);
DEFINE_SIMPLE_TYPE(uint8_t,     ui8);
DEFINE_SIMPLE_TYPE(uint16_t,    ui16);
DEFINE_SIMPLE_TYPE(uint32_t,    ui32);
DEFINE_SIMPLE_TYPE(uint64_t,    ui64);
DEFINE_SIMPLE_TYPE(double,      d);
DEFINE_SIMPLE_TYPE(float,       f);
DEFINE_SIMPLE_TYPE(std::string, str);
DEFINE_SIMPLE_TYPE(const char*, str);
DEFINE_SIMPLE_TYPE(bool,        logical);

template <typename> struct enum_type;
#define DEFINE_ENUM_TYPE(int_type, field) \
    template <> struct enum_type<int_type> { static constexpr auto type_enum = EnumIntTypes::field; };

DEFINE_ENUM_TYPE(int8_t,   i8);
DEFINE_ENUM_TYPE(int16_t,  i16);
DEFINE_ENUM_TYPE(int32_t,  i32);
DEFINE_ENUM_TYPE(int64_t,  i64);
DEFINE_ENUM_TYPE(uint8_t,  ui8);
DEFINE_ENUM_TYPE(uint16_t, ui16);
DEFINE_ENUM_TYPE(uint32_t, ui32);
DEFINE_ENUM_TYPE(uint64_t, ui64);

class DataTypeBase
{
public:
    virtual ~DataTypeBase() = default;

    virtual BasicTypes getBasicType() const = 0;

    const std::string& getName() const
    {
        return name_;
    }

protected:
    DataTypeBase(const std::string& name)
        : name_(name)
    {}

private:
    const std::string name_;
};

class SimpleTypeBase : public DataTypeBase
{
public:
    using DataTypeBase::DataTypeBase;
    BasicTypes getBasicType() const override final { return BasicTypes::SIMPLE; }
    virtual SimpleTypes getSimpleType() const = 0;
};

template <typename SimpleTypeT>
class SimpleType : public SimpleTypeBase
{
    static_assert(std::is_trivial_v<SimpleTypeT> &&
                  std::is_standard_layout_v<SimpleTypeT> &&
                  !std::is_enum_v<SimpleTypeT>);

    static_assert(!type_traits::is_any_pointer_v<SimpleTypeT>);

public:
    using SimpleTypeBase::SimpleTypeBase;
    SimpleTypes getSimpleType() const override final { return simple_type<SimpleTypeT>::type_enum; }
};

class EnumTypeBase : public DataTypeBase
{
public:
    using DataTypeBase::DataTypeBase;
    BasicTypes getBasicType() const override final { return BasicTypes::ENUM; }
    virtual EnumIntTypes getEnumIntType() const = 0;
    virtual std::vector<std::string> getFieldNames() const = 0;
    virtual std::string getFieldValStr(const std::string& field_name) const = 0;
};

template <typename EnumT>
class EnumType : public EnumTypeBase
{
    static_assert(std::is_enum_v<EnumT>);
    static_assert(!type_traits::is_any_pointer_v<EnumT>);

public:
    using EnumTypeBase::EnumTypeBase;
    EnumTypeTypes getEnumIntType() const override final { return enum_type<EnumT>::type_enum; }

    std::vector<std::string> getFieldNames() const override final
    {
        if (field_names_.empty())
        {
            for (const auto& [field_name, _] : getEnumMap_())
            {
                field_names_.push_back(field_name);
            }
        }
        return field_names_;
    }

    std::string getFieldValStr(const std::string& field_name) const override final
    {
        return std::to_string(getEnumMap_().at(field_name));
    }

private:
    auto& getEnumMap_() const
    {
        if (enum_map_.empty())
        {
            defineEnumMap<EnumT>(enum_map_);
        }
        return enum_map_;
    }

    mutable std::vector<std::string> field_names_;
    mutable std::map<std::string, enum_int_t<EnumT>> enum_map_;
};

class StructTypeBase : public DataTypeBase
{
public:
    using DataTypeBase::DataTypeBase;

    BasicTypes getBasicType() const override final { return BasicTypes::STRUCT; }

    virtual std::vector<std::string> getFieldNames() const = 0;
    virtual BasicTypes getFieldType(const std::string& field_name) const = 0;
    virtual const DataTypeBase* getField(const std::string& field_name) const = 0;

    template <typename SimpleTypeT>
    const SimpleType<SimpleTypeT>* getSimpleFieldAs(const std::string& field_name) const
    {
        auto field = getField(field_name);
        if (!field)
        {
            throw DBException("Field does not exist: ") << field_name;
        }
        auto simple_field = dynamic_cast<const SimpleTypeBase*>(field);
        if (!simple_field)
        {
            throw DBException("Field is not a simple data type: ") << field_name;
        }
        auto typed_simple_field = dynamic_cast<const SimpleType<SimpleTypeT>*>(field);
        if (!typed_simple_field)
        {
            throw DBException("Field is a simple type, but the wrong kind: ") << field_name;
        }
        return typed_simple_field;
    }

    template <typename EnumT>
    const EnumType<EnumT>* getEnumFieldAs(const std::string& field_name) const
    {
        auto field = getField(field_name);
        if (!field)
        {
            throw DBException("Field does not exist: ") << field_name;
        }
        auto enum_field = dynamic_cast<const EnumTypeBase*>(field);
        if (!enum_field)
        {
            throw DBException("Field is not an enum data type: ") << field_name;
        }
        auto typed_enum_field = dynamic_cast<const EnumType<EnumT>*>(field);
        if (!typed_enum_field)
        {
            throw DBException("Field is an enum type, but the wrong kind: ") << field_name;
        }
        return typed_enum_field;
    }

    template <typename StructT>
    const StructType<StructT>* getStructFieldAs(const std::string& field_name) const
    {
        auto field = getField(field_name);
        if (!field)
        {
            throw DBException("Field does not exist: ") << field_name;
        }
        auto struct_field = dynamic_cast<const StructTypeBase*>(field);
        if (!struct_field)
        {
            throw DBException("Field is not a struct data type: ") << field_name;
        }
        auto typed_struct_field = dynamic_cast<const StructType<StructT>*>(field);
        if (!typed_struct_field)
        {
            throw DBException("Field is a struct type, but the wrong kind: ") << field_name;
        }
        return typed_struct_field;
    }
};

template <typename StructT>
class StructType : public StructTypeBase
{
    static_assert(!std::is_trivial_v<StructT> || !std::is_standard_layout_v<StructT>);
    static_assert(!std::is_enum_v<StructT>);
    static_assert(!type_traits::is_any_pointer_v<StructT>);

    std::vector<std::string> immediate_field_names_;

public:
    StructType()
    {
        typename StructT::ArgosCollector collector;
        for (const auto field : collector.getFields())
        {
            immediate_field_names_.push_back(field->getName());
        }
    }

    std::vector<std::string> getFieldNames() const override final
    {
        return immediate_field_names_;
    }

    BasicTypes getFieldType(const std::string& field_name) const override final
    {
        // TODO cnyce
        (void)field_name;
        return BasicTypes::SIMPLE;
    }

    const DataTypeBase* getField(const std::string& field_name) const override final
    {
        // TODO cnyce
        (void)field_name;
        return nullptr;
    }
};

} // namespace simdb::collection
