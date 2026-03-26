#include "simdb/apps/argos/DataTypeHierarchy.hpp"
#include "simdb/apps/argos/ArgosCollect.hpp"

#include <cstddef>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace cursor = simdb::collection::cursor;

namespace {

template <typename T>
T readScalarFromBuffer(const std::vector<char>& buffer, size_t& offset)
{
    T value{};
    std::memcpy(&value, buffer.data() + offset, sizeof(T));
    offset += sizeof(T);
    return value;
}

using FieldBase = simdb::collection::cursor::ArgosFieldBase;

enum class Status : uint8_t
{
    Failed = 0,
    Passed = 1
};

struct PodField final : FieldBase
{
    PodField(std::string n, std::string t, cursor::PodTypeKind k, size_t off, size_t bytes)
        : name(std::move(n))
        , type_name(std::move(t))
        , kind(k)
        , offset(off)
        , num_bytes(bytes)
    {}

    std::string getName() const override { return name; }
    std::string getTypeName() const override { return type_name; }
    bool isEnumField() const override { return false; }
    bool isStructField() const override { return false; }
    cursor::PodTypeKind getPodTypeKind() const override { return kind; }
    cursor::EnumBackingKind getEnumBackingKind() const override { return cursor::EnumBackingKind::ui8; }
    std::vector<cursor::EnumMember> getEnumMembers() const override { return {}; }
    std::string getStructTypeName() const override { return {}; }
    std::vector<const FieldBase*> getStructFields() const override { return {}; }
    void writeBufferErased(std::vector<char>& buffer, const void* owner) const override
    {
        const auto* bytes = static_cast<const char*>(owner) + offset;
        buffer.insert(buffer.end(), bytes, bytes + num_bytes);
    }
    const void* getStructPtrErased(const void*) const override { return nullptr; }

    std::string name;
    std::string type_name;
    cursor::PodTypeKind kind;
    size_t offset = 0;
    size_t num_bytes = 0;
};

struct EnumField final : FieldBase
{
    EnumField(std::string n, std::string t, cursor::EnumBackingKind b, std::vector<cursor::EnumMember> m, size_t off, size_t bytes)
        : name(std::move(n))
        , type_name(std::move(t))
        , backing(b)
        , members(std::move(m))
        , offset(off)
        , num_bytes(bytes)
    {}

    std::string getName() const override { return name; }
    std::string getTypeName() const override { return type_name; }
    bool isEnumField() const override { return true; }
    bool isStructField() const override { return false; }
    cursor::PodTypeKind getPodTypeKind() const override { return cursor::PodTypeKind::ui8; }
    cursor::EnumBackingKind getEnumBackingKind() const override { return backing; }
    std::vector<cursor::EnumMember> getEnumMembers() const override { return members; }
    std::string getStructTypeName() const override { return {}; }
    std::vector<const FieldBase*> getStructFields() const override { return {}; }
    void writeBufferErased(std::vector<char>& buffer, const void* owner) const override
    {
        const auto* bytes = static_cast<const char*>(owner) + offset;
        buffer.insert(buffer.end(), bytes, bytes + num_bytes);
    }
    const void* getStructPtrErased(const void*) const override { return nullptr; }

    std::string name;
    std::string type_name;
    cursor::EnumBackingKind backing;
    std::vector<cursor::EnumMember> members;
    size_t offset = 0;
    size_t num_bytes = 0;
};

struct StructField final : FieldBase
{
    StructField(std::string n, std::string t, std::vector<const FieldBase*> f, size_t off)
        : name(std::move(n))
        , type_name(std::move(t))
        , fields(std::move(f))
        , offset(off)
    {}

    std::string getName() const override { return name; }
    std::string getTypeName() const override { return type_name; }
    bool isEnumField() const override { return false; }
    bool isStructField() const override { return true; }
    cursor::PodTypeKind getPodTypeKind() const override { return cursor::PodTypeKind::ui8; }
    cursor::EnumBackingKind getEnumBackingKind() const override { return cursor::EnumBackingKind::ui8; }
    std::vector<cursor::EnumMember> getEnumMembers() const override { return {}; }
    std::string getStructTypeName() const override { return type_name; }
    std::vector<const FieldBase*> getStructFields() const override { return fields; }
    void writeBufferErased(std::vector<char>&, const void*) const override {}
    const void* getStructPtrErased(const void* owner) const override
    {
        return static_cast<const char*>(owner) + offset;
    }

    std::string name;
    std::string type_name;
    std::vector<const FieldBase*> fields;
    size_t offset = 0;
};

struct Header
{
    uint32_t seq = 0;
    Status status = Status::Failed;

    struct ArgosCollector
    {
        std::vector<const FieldBase*> getFields() const
        {
            static const PodField seq{
                "seq", "uint32_t", cursor::PodTypeKind::ui32, offsetof(Header, seq), sizeof(Header::seq)
            };
            static const EnumField status{
                "status",
                "Status",
                cursor::EnumBackingKind::ui8,
                {{"Failed", 0}, {"Passed", 1}},
                offsetof(Header, status),
                sizeof(Header::status)
            };
            return {&seq, &status};
        }
    };
};

enum class Priority : uint16_t
{
    Low = 1,
    High = 2
};

struct Metadata
{
    uint64_t trace_id = 0;
    bool valid = false;
    Priority priority = Priority::Low;

    struct ArgosCollector
    {
        std::vector<const FieldBase*> getFields() const
        {
            static const PodField trace_id{
                "trace_id", "uint64_t", cursor::PodTypeKind::ui64, offsetof(Metadata, trace_id), sizeof(Metadata::trace_id)
            };
            static const PodField valid{
                "valid", "bool", cursor::PodTypeKind::logical, offsetof(Metadata, valid), sizeof(Metadata::valid)
            };
            static const EnumField priority{
                "priority",
                "Priority",
                cursor::EnumBackingKind::ui16,
                {{"Low", 1}, {"High", 2}},
                offsetof(Metadata, priority),
                sizeof(Metadata::priority)
            };
            return {&trace_id, &valid, &priority};
        }
    };
};

class Packet
{
    double timestamp = 0;
    Header header;
    Metadata metadata;

public:
    double getTimestamp() const { return timestamp; }
    const Header& getHeader() const { return header; }
    const Metadata& getMetadata() const { return metadata; }

    void setTimestamp(double v) { timestamp = v; }
    void setHeader(const Header& v) { header = v; }
    void setMetadata(const Metadata& v) { metadata = v; }

    struct ArgosCollector
        : simdb::collection::cursor::ArgosCollectorBase<Packet>
    {
        std::vector<const FieldBase*> getFields() const
        {
            auto fields = simdb::collection::cursor::ArgosCollectorBase<Packet>::getFields();

            static const PodField header_seq{
                "seq", "uint32_t", cursor::PodTypeKind::ui32, offsetof(Header, seq), sizeof(Header::seq)
            };
            static const EnumField header_status{
                "status",
                "Status",
                cursor::EnumBackingKind::ui8,
                {{"Failed", 0}, {"Passed", 1}},
                offsetof(Header, status),
                sizeof(Header::status)
            };
            static const StructField header{
                "header",
                "Header",
                {&header_seq, &header_status},
                offsetof(Packet, header)
            };

            static const PodField metadata_trace_id{
                "trace_id", "uint64_t", cursor::PodTypeKind::ui64, offsetof(Metadata, trace_id), sizeof(Metadata::trace_id)
            };
            static const PodField metadata_valid{
                "valid", "bool", cursor::PodTypeKind::logical, offsetof(Metadata, valid), sizeof(Metadata::valid)
            };
            static const EnumField metadata_priority{
                "priority",
                "Priority",
                cursor::EnumBackingKind::ui16,
                {{"Low", 1}, {"High", 2}},
                offsetof(Metadata, priority),
                sizeof(Metadata::priority)
            };
            static const StructField metadata{
                "metadata",
                "Metadata",
                {&metadata_trace_id, &metadata_valid, &metadata_priority},
                offsetof(Packet, metadata)
            };

            fields.push_back(&header);
            fields.push_back(&metadata);
            return fields;
        }

        ARGOS_COLLECT(timestamp, &Packet::getTimestamp);
    };
};

} // namespace

namespace simdb::collection::cursor {

template <>
struct EnumDescriptor<Status>
{
    static std::vector<EnumMember> members()
    {
        return {{"Failed", 0}, {"Passed", 1}};
    }
};

template <>
struct EnumDescriptor<Priority>
{
    static std::vector<EnumMember> members()
    {
        return {{"Low", 1}, {"High", 2}};
    }
};

} // namespace simdb::collection::cursor

int main()
{
    auto pod = cursor::createDataTypeHier<int16_t>();
    if (pod->getRoot().kind != cursor::NodeKind::Pod || !pod->getRoot().pod_type ||
        *pod->getRoot().pod_type != cursor::PodTypeKind::i16)
    {
        std::cerr << "POD hierarchy test failed\n";
        return 1;
    }

    auto en = cursor::createDataTypeHier<Status>();
    if (en->getRoot().kind != cursor::NodeKind::Enum || !en->getRoot().enum_meta)
    {
        std::cerr << "Enum hierarchy test failed\n";
        return 1;
    }
    if (en->getRoot().enum_meta->backing_kind != cursor::EnumBackingKind::ui8 ||
        en->getRoot().enum_meta->members.size() != 2)
    {
        std::cerr << "Enum metadata test failed\n";
        return 1;
    }

    auto packet = cursor::createDataTypeHier<Packet>();
    if (packet->getRoot().kind != cursor::NodeKind::Struct || packet->getRoot().children.size() != 3)
    {
        std::cerr << "Struct hierarchy root test failed\n";
        return 1;
    }

    const auto& ts = packet->getRoot().children[0];
    if (ts->field_name != "timestamp" || ts->kind != cursor::NodeKind::Pod || !ts->pod_type ||
        *ts->pod_type != cursor::PodTypeKind::d)
    {
        std::cerr << "Timestamp field test failed\n";
        return 1;
    }

    const auto& header = packet->getRoot().children[1];
    if (header->field_name != "header" || header->kind != cursor::NodeKind::Struct || header->children.size() != 2)
    {
        std::cerr << "Nested struct test failed\n";
        return 1;
    }

    const auto& header_status = header->children[1];
    if (header_status->field_name != "status" || header_status->kind != cursor::NodeKind::Enum ||
        !header_status->enum_meta || header_status->enum_meta->members.size() != 2)
    {
        std::cerr << "Nested enum field test failed\n";
        return 1;
    }

    const auto& metadata = packet->getRoot().children[2];
    if (metadata->field_name != "metadata" || metadata->kind != cursor::NodeKind::Struct || metadata->children.size() != 3)
    {
        std::cerr << "Metadata struct test failed\n";
        return 1;
    }

    const auto& metadata_priority = metadata->children[2];
    if (metadata_priority->field_name != "priority" || metadata_priority->kind != cursor::NodeKind::Enum ||
        !metadata_priority->enum_meta || metadata_priority->enum_meta->members.size() != 2)
    {
        std::cerr << "Metadata enum test failed\n";
        return 1;
    }

    Packet p{};
    p.setTimestamp(3.5);
    Header h{};
    h.seq = 42;
    h.status = Status::Passed;
    p.setHeader(h);
    Metadata m{};
    m.trace_id = 0xABCD;
    m.valid = true;
    m.priority = Priority::High;
    p.setMetadata(m);

    std::vector<char> buffer;
    packet->writeBuffer(buffer, &p);
    const auto expected_size =
        sizeof(double) + sizeof(uint32_t) + sizeof(Status) + sizeof(uint64_t) + sizeof(bool) +
        sizeof(Priority);
    if (buffer.size() != expected_size)
    {
        std::cerr << "writeBuffer size mismatch\n";
        return 1;
    }

    size_t offset = 0;
    const auto ts_buf = readScalarFromBuffer<double>(buffer, offset);
    const auto seq_buf = readScalarFromBuffer<uint32_t>(buffer, offset);
    const auto status_raw = readScalarFromBuffer<std::underlying_type_t<Status>>(buffer, offset);
    const auto trace_id_buf = readScalarFromBuffer<uint64_t>(buffer, offset);
    const auto valid_buf = readScalarFromBuffer<bool>(buffer, offset);
    const auto priority_raw = readScalarFromBuffer<std::underlying_type_t<Priority>>(buffer, offset);

    if (offset != buffer.size())
    {
        std::cerr << "Did not consume full buffer\n";
        return 1;
    }

    if (ts_buf != p.getTimestamp())
    {
        std::cerr << "timestamp buffer mismatch\n";
        return 1;
    }
    if (seq_buf != p.getHeader().seq)
    {
        std::cerr << "header.seq buffer mismatch\n";
        return 1;
    }
    if (status_raw != static_cast<std::underlying_type_t<Status>>(p.getHeader().status))
    {
        std::cerr << "header.status buffer mismatch\n";
        return 1;
    }
    if (trace_id_buf != p.getMetadata().trace_id)
    {
        std::cerr << "metadata.trace_id buffer mismatch\n";
        return 1;
    }
    if (valid_buf != p.getMetadata().valid)
    {
        std::cerr << "metadata.valid buffer mismatch\n";
        return 1;
    }
    if (priority_raw != static_cast<std::underlying_type_t<Priority>>(p.getMetadata().priority))
    {
        std::cerr << "metadata.priority buffer mismatch\n";
        return 1;
    }

    std::cout << "DataTypeHierarchy tests passed\n";
    return 0;
}
