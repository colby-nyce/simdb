// <Serialize.hpp> -*- C++ -*-

#pragma once

#include "simdb/Exceptions.hpp"
#include "simdb/pipeline/CollectionBuffer.hpp"
#include "simdb/utils/Demangle.hpp"
#include "simdb/utils/TinyStrings.hpp"
#include "simdb/utils/TypeTraits.hpp"

#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_set>
#include <vector>

namespace simdb {

class DatabaseManager;

enum class Format { none = 0, hex = 1, boolalpha = 2 };

/// Data types supported by the collection system. Note that
/// enum struct fields use the std::underlying_type of that
/// enum, e.g. int32_t
enum class StructFields {
    char_t,
    int8_t,
    uint8_t,
    int16_t,
    uint16_t,
    int32_t,
    uint32_t,
    int64_t,
    uint64_t,
    float_t,
    double_t,
    string_t
};

template <typename FieldT> inline StructFields getFieldDTypeEnum() {
    if constexpr (std::is_same_v<FieldT, char>) {
        return StructFields::char_t;
    } else if constexpr (std::is_same_v<FieldT, int8_t>) {
        return StructFields::int8_t;
    } else if constexpr (std::is_same_v<FieldT, uint8_t>) {
        return StructFields::uint8_t;
    } else if constexpr (std::is_same_v<FieldT, int16_t>) {
        return StructFields::int16_t;
    } else if constexpr (std::is_same_v<FieldT, uint16_t>) {
        return StructFields::uint16_t;
    } else if constexpr (std::is_same_v<FieldT, int32_t>) {
        return StructFields::int32_t;
    } else if constexpr (std::is_same_v<FieldT, uint32_t>) {
        return StructFields::uint32_t;
    } else if constexpr (std::is_same_v<FieldT, int64_t>) {
        return StructFields::int64_t;
    } else if constexpr (std::is_same_v<FieldT, uint64_t>) {
        return StructFields::uint64_t;
    } else if constexpr (std::is_same_v<FieldT, float>) {
        return StructFields::float_t;
    } else if constexpr (std::is_same_v<FieldT, double>) {
        return StructFields::double_t;
    } else if constexpr (std::is_same_v<FieldT, std::string>) {
        return StructFields::string_t;
    } else if constexpr (std::is_same_v<FieldT, bool>) {
        return StructFields::int32_t;
    } else if constexpr (std::is_enum<FieldT>::value) {
        using enum_int_t = typename std::underlying_type<FieldT>::type;
        return getFieldDTypeEnum<enum_int_t>();
    } else {
        throw DBException("Unsupported data type: ") << demangle(typeid(FieldT).name());
    }
}

/// These dtype strings are stored in the database to inform the downstream
/// python module how to interpret the structs' serialized raw bytes.
inline std::string getFieldDTypeStr(const StructFields dtype) {
    switch (dtype) {
    case StructFields::char_t:
        return "char_t";
    case StructFields::int8_t:
        return "int8_t";
    case StructFields::uint8_t:
        return "uint8_t";
    case StructFields::int16_t:
        return "int16_t";
    case StructFields::uint16_t:
        return "uint16_t";
    case StructFields::int32_t:
        return "int32_t";
    case StructFields::uint32_t:
        return "uint32_t";
    case StructFields::int64_t:
        return "int64_t";
    case StructFields::uint64_t:
        return "uint64_t";
    case StructFields::float_t:
        return "float_t";
    case StructFields::double_t:
        return "double_t";
    case StructFields::string_t:
        return "string_t";
    }

    throw DBException("Invalid data type");
}

inline size_t getDTypeNumBytes(const StructFields dtype) {
    switch (dtype) {
    case StructFields::char_t:
        return sizeof(char);
    case StructFields::int8_t:
        return sizeof(int8_t);
    case StructFields::uint8_t:
        return sizeof(uint8_t);
    case StructFields::int16_t:
        return sizeof(int16_t);
    case StructFields::uint16_t:
        return sizeof(uint16_t);
    case StructFields::int32_t:
        return sizeof(int32_t);
    case StructFields::uint32_t:
        return sizeof(uint32_t);
    case StructFields::int64_t:
        return sizeof(int64_t);
    case StructFields::uint64_t:
        return sizeof(uint64_t);
    case StructFields::float_t:
        return sizeof(float);
    case StructFields::double_t:
        return sizeof(double);
    default:
        break;
    }

    throw DBException("Invalid data type");
}

/// Avoid all SQLite issues (such as not natively supporting uint64_t, or
/// truncating double values to less precision that you wanted), these
/// convertIntToBlob() methods convert scalar PODs to vector<char> (blob).
template <typename IntT> std::vector<char> convertIntToBlob(const IntT val) = delete;

template <> inline std::vector<char> convertIntToBlob<char>(const char val) { return {val}; }

template <> inline std::vector<char> convertIntToBlob<int8_t>(const int8_t val) {
    std::vector<char> blob(sizeof(int8_t));
    memcpy(blob.data(), &val, sizeof(int8_t));
    return blob;
}

template <> inline std::vector<char> convertIntToBlob<uint8_t>(const uint8_t val) {
    std::vector<char> blob(sizeof(uint8_t));
    memcpy(blob.data(), &val, sizeof(uint8_t));
    return blob;
}

template <> inline std::vector<char> convertIntToBlob<int16_t>(const int16_t val) {
    std::vector<char> blob(sizeof(int16_t));
    memcpy(blob.data(), &val, sizeof(int16_t));
    return blob;
}

template <> inline std::vector<char> convertIntToBlob<uint16_t>(const uint16_t val) {
    std::vector<char> blob(sizeof(uint16_t));
    memcpy(blob.data(), &val, sizeof(uint16_t));
    return blob;
}

template <> inline std::vector<char> convertIntToBlob<int32_t>(const int32_t val) {
    std::vector<char> blob(sizeof(int32_t));
    memcpy(blob.data(), &val, sizeof(int32_t));
    return blob;
}

template <> inline std::vector<char> convertIntToBlob<uint32_t>(const uint32_t val) {
    std::vector<char> blob(sizeof(uint32_t));
    memcpy(blob.data(), &val, sizeof(uint32_t));
    return blob;
}

template <> inline std::vector<char> convertIntToBlob<int64_t>(const int64_t val) {
    std::vector<char> blob(sizeof(int64_t));
    memcpy(blob.data(), &val, sizeof(int64_t));
    return blob;
}

template <> inline std::vector<char> convertIntToBlob<uint64_t>(const uint64_t val) {
    std::vector<char> blob(sizeof(uint64_t));
    memcpy(blob.data(), &val, sizeof(uint64_t));
    return blob;
}

/// Users specialize this template so we can serialize the int->string mapping
/// for their enums. Ints are stored in the database, and the python modules
/// turn them back into enums later.
template <typename EnumT>
void defineEnumMap(std::string &enum_name,
                   std::map<std::string, typename std::underlying_type<EnumT>::type> &map) = delete;

/*!
 * \class EnumMap<EnumT>
 * \brief This class holds and serializes the int->string mapping for the EnumT.
 */
template <typename EnumT> class EnumMap {
  public:
    using enum_int_t = typename std::underlying_type<EnumT>::type;
    using enum_map_t = std::shared_ptr<std::map<std::string, enum_int_t>>;

    static EnumMap *instance() {
        static EnumMap map;
        return &map;
    }

    const enum_map_t getMap() const { return map_; }

    const std::string &getEnumName() const { return enum_name_; }

    void serializeDefn(DatabaseManager *db_mgr) const;

  private:
    EnumMap() {
        map_ = std::make_shared<std::map<std::string, enum_int_t>>();
        defineEnumMap<EnumT>(enum_name_, *map_);
    }

    enum_map_t map_;
    std::string enum_name_;
    mutable bool serialized_ = false;
};

/// \class FieldBase
/// \brief This class is used to serialize information about a non-enum, non-string field.
class FieldBase {
  public:
    FieldBase(const std::string &name, const StructFields type, const Format format = Format::none)
        : name_(name), dtype_(type), format_(format) {}

    virtual ~FieldBase() = default;

    const std::string &getName() const { return name_; }

    Format getFormat() const { return format_; }

    StructFields getType() const { return dtype_; }

    virtual size_t getNumBytes() const { return getDTypeNumBytes(dtype_); }

    virtual void serializeDefn(DatabaseManager *db_mgr, const std::string &struct_name) const;

    void setIsAutocolorizeKey(bool is_autocolorize_key) {
        if (is_autocolorize_key_ && !is_autocolorize_key) {
            throw DBException("Only one column can be used as the autocolorize key");
        }
        is_autocolorize_key_ = is_autocolorize_key;
    }

    bool isAutocolorizeKey() const { return is_autocolorize_key_; }

    void setIsDisplayedByDefault(bool is_displayed_by_default) {
        is_displayed_by_default_ = is_displayed_by_default;
    }

    bool isDisplayedByDefault() const { return is_displayed_by_default_; }

  private:
    std::string name_;
    StructFields dtype_;
    Format format_;
    bool is_autocolorize_key_ = false;
    bool is_displayed_by_default_ = true;
};

/// \class EnumField
/// \brief This class is used to serialize information about an enum field.
template <typename EnumT> class EnumField : public FieldBase {
  public:
    EnumField(const char *name)
        : FieldBase(name, getFieldDTypeEnum<typename EnumMap<EnumT>::enum_int_t>()),
          map_(EnumMap<EnumT>::instance()->getMap()),
          enum_name_(EnumMap<EnumT>::instance()->getEnumName()) {}

    virtual void serializeDefn(DatabaseManager *db_mgr,
                               const std::string &struct_name) const override;

  private:
    const typename EnumMap<EnumT>::enum_map_t map_;
    std::string enum_name_;
};

/// \class StringField
/// \brief This class is used to serialize information about a string field.
class StringField : public FieldBase {
  public:
    StringField(const char *name) : FieldBase(name, StructFields::string_t) {}

    size_t getNumBytes() const override { return getDTypeNumBytes(StructFields::uint32_t); }
};

template <typename StructT> class StructFieldSerializer;

/// Users specialize this template to write the struct fields one by one into the serializer.
template <typename StructT>
void writeStructFields(const StructT *s, StructFieldSerializer<StructT> *serializer) {
    (void)serializer;
}

/// This helper class is used by the writeStructFields<T>() specializations supplied by the user.
///
/// namespace simdb
/// {
///     template <> void defineStructSchema<DummyPacket>(StructSchema<DummyPacket>& schema)
///     {
///         schema.addField<int32_t>("int32");
///         schema.addField<double>("dbl");
///         schema.addBool("bool");
///         schema.addString("str");
///     }
///
///     template <> void writeStructFields(const DummyPacket* all,
///     StructFieldSerializer<DummyPacket>* serializer)
///     {
///         serializer->writeField(all->int32);            <-- here
///         serializer->writeField(all->dbl);              <-- here
///         serializer->writeField(all->b);                <-- here
///         serializer->writeField(all->str);              <-- here
///     }
/// } // namespace simdb
///
template <typename StructT> class StructFieldSerializer {
  public:
    StructFieldSerializer(const std::vector<std::unique_ptr<FieldBase>> &fields,
                          CollectionBuffer &buffer)
        : fields_(fields), buffer_(buffer) {}

    /// Write an entire struct.
    void writeFields(const StructT *s) { writeStructFields<StructT>(s, this); }

    /// Write a non-string, non-enum field.
    template <typename FieldT>
    typename std::enable_if<
        !std::is_enum<FieldT>::value && !std::is_same<FieldT, std::string>::value, void>::type
    writeField(const FieldT val) {
        auto dtype = getFieldDTypeEnum<FieldT>();
        auto num_bytes = getDTypeNumBytes(dtype);

        if (fields_[current_field_idx_]->getNumBytes() != num_bytes) {
            throw DBException("Data type mismatch in writing struct field");
        }

        buffer_ << val;
        ++current_field_idx_;
        num_bytes_written_ += num_bytes;
    }

    /// Write an enum field.
    template <typename FieldT>
    typename std::enable_if<std::is_enum<FieldT>::value, void>::type writeField(const FieldT val) {
        using dtype = typename std::underlying_type<FieldT>::type;
        writeField<dtype>(static_cast<dtype>(val));
    }

    /// Write a string field.
    void writeField(const std::string &val) {
        if (dynamic_cast<const StringField *>(fields_[current_field_idx_].get())) {
            uint32_t string_id = StringMap::instance()->getStringId(val);
            writeField<uint32_t>(string_id);
        } else {
            throw DBException("Data type mismatch in writing struct field");
        }
    }

    /// Write a string field.
    void writeField(const char *val) { writeField(std::string(val)); }

    size_t numBytesWritten() const { return num_bytes_written_; }

    /// Get the buffer that the serializer is writing to.
    CollectionBuffer &getBuffer() { return buffer_; }

  private:
    const std::vector<std::unique_ptr<FieldBase>> &fields_;
    size_t current_field_idx_ = 0;
    CollectionBuffer &buffer_;
    size_t num_bytes_written_ = 0;
};

template <typename StructT> class StructSerializer;

/// This class is used in order to define a struct-like collectable <T>, which
/// only needs to be defined for non-trivial types.
///
/// namespace simdb
/// {
///     template <> void defineStructSchema<DummyPacket>(StructSchema<DummyPacket>& schema)
///     {
///         schema.addField<int32_t>("int32");        <-- here
///         schema.addField<double>("dbl");           <-- here
///         schema.addBool("bool");                   <-- here
///         schema.addString("str");                  <-- here
///     }
/// } // namespace simdb
///
template <typename StructT> class StructSchema {
  public:
    const std::string &getStructName() const { return struct_name_; }

    size_t getStructNumBytes() const {
        size_t num_bytes = 0;
        for (const auto &field : fields_) {
            num_bytes += field->getNumBytes();
        }
        return num_bytes;
    }

    template <typename FieldT> void addField(const char *name) {
        static_assert(!std::is_enum<FieldT>::value && !std::is_same<FieldT, std::string>::value &&
                          !std::is_same<FieldT, bool>::value,
                      "Use addEnum(), addString(), or addBool() instead");

        fields_.emplace_back(new FieldBase(name, getFieldDTypeEnum<FieldT>()));
        if (fields_.size() == 1) {
            setAutoColorizeColumn(name);
        }
    }

    template <typename FieldT> void addHex(const char *name) {
        static_assert(std::is_same<FieldT, uint64_t>::value ||
                          std::is_same<FieldT, uint32_t>::value,
                      "Hex modifier only supported for uint32_t and uint64_t");
        fields_.emplace_back(new FieldBase(name, getFieldDTypeEnum<FieldT>(), Format::hex));
        if (fields_.size() == 1) {
            setAutoColorizeColumn(name);
        }
    }

    void addBool(const char *name) {
        fields_.emplace_back(new FieldBase(name, getFieldDTypeEnum<int32_t>(), Format::boolalpha));
        if (fields_.size() == 1) {
            setAutoColorizeColumn(name);
        }
    }

    void addString(const char *name) {
        fields_.emplace_back(new StringField(name));
        if (fields_.size() == 1) {
            setAutoColorizeColumn(name);
        }
    }

    template <typename FieldT> void addEnum(const char *name) {
        static_assert(std::is_enum<FieldT>::value, "Use addField() for non-enum types");
        fields_.emplace_back(new EnumField<FieldT>(name));
        if (fields_.size() == 1) {
            setAutoColorizeColumn(name);
        }
    }

    void setAutoColorizeColumn(const char *name) {
        bool found = false;
        for (auto &field : fields_) {
            if (field->getName() == name) {
                field->setIsAutocolorizeKey(true);
                found = true;
            } else {
                field->setIsAutocolorizeKey(false);
            }
        }

        if (!found) {
            throw DBException("Field not found: ") << name;
        }
    }

    void makeColumnHiddenByDefault(const char *name) {
        for (auto &field : fields_) {
            if (field->getName() == name) {
                field->setIsDisplayedByDefault(false);
                return;
            }
        }

        throw DBException("Field not found: ") << name;
    }

    void serializeDefn(DatabaseManager *db_mgr) const {
        static std::unordered_set<std::string> serialized_structs;
        if (serialized_structs.insert(struct_name_).second) {
            for (auto &field : fields_) {
                field->serializeDefn(db_mgr, struct_name_);
            }
        }
    }

  private:
    const std::string struct_name_ = demangle(typeid(StructT).name());
    std::vector<std::unique_ptr<FieldBase>> fields_;

    friend class StructSerializer<StructT>;
};

/// This method is used in order to define a struct-like collectable <T>, which
/// only needs to be defined for non-trivial types.
///
/// namespace simdb
/// {
///     template <> void defineStructSchema<DummyPacket>(StructSchema<DummyPacket>& schema)
///     {
///         schema.addField<int32_t>("int32");        <-- here
///         schema.addField<double>("dbl");           <-- here
///         schema.addBool("bool");                   <-- here
///         schema.addString("str");                  <-- here
///     }
/// } // namespace simdb
///
template <typename StructT> inline void defineStructSchema(StructSchema<StructT> &schema) {
    (void)schema;
}

/// This class is used to read a struct's fields and write them into a CollectionBuffer
/// or directly into a std::vector<char>. It is also responsible for serializing the
/// struct's fields into the database so they can be deserialized in Python (e.g. Argos).
template <typename StructT> class StructSerializer {
  public:
    static StructSerializer *getInstance() {
        static_assert(!type_traits::is_any_pointer<StructT>::value,
                      "StructSerializer does not support pointer types");

        static StructSerializer serializer;
        return &serializer;
    }

    const std::string &getStructName() const { return schema_.getStructName(); }

    size_t getStructNumBytes() const { return schema_.getStructNumBytes(); }

    void serializeDefn(DatabaseManager *db_mgr) const { schema_.serializeDefn(db_mgr); }

    size_t writeStruct(const StructT *s, CollectionBuffer &buffer) const {
        StructFieldSerializer<StructT> field_serializer(schema_.fields_, buffer);
        field_serializer.writeFields(s);
        return field_serializer.numBytesWritten();
    }

    void extract(const StructT *s, std::vector<char> &bytes) const {
        CollectionBuffer buffer(bytes);
        writeStruct(s, buffer);
    }

  private:
    StructSerializer() { defineStructSchema<StructT>(schema_); }

    StructSchema<StructT> schema_;
};

} // namespace simdb
