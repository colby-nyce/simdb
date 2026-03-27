#include "simdb/apps/argos/DataTypeHierarchy.hpp"
#include "simdb/apps/argos/ArgosCollect.hpp"

#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace collection = simdb::collection;

namespace {

template <typename T>
T readScalarFromBuffer(const std::vector<char>& buffer, size_t& offset)
{
    T value{};
    std::memcpy(&value, buffer.data() + offset, sizeof(T));
    offset += sizeof(T);
    return value;
}

enum class Status : uint8_t
{
    Failed = 0,
    Passed = 1
};

enum class Priority : uint16_t
{
    Low = 1,
    High = 2
};

struct Header
{
    uint32_t seq = 0;
    Status status = Status::Failed;

    uint32_t getSeq() const { return seq; }
    Status getStatus() const { return status; }

    struct ArgosCollector : simdb::collection::ArgosCollectorBase<Header>
    {
        ARGOS_COLLECT(seq, &Header::getSeq);
        ARGOS_COLLECT_ENUM(status, &Header::getStatus);
    };
};

struct Metadata
{
    uint64_t trace_id = 0;
    bool valid = false;
    Priority priority = Priority::Low;

    uint64_t getTraceId() const { return trace_id; }
    bool getValid() const { return valid; }
    Priority getPriority() const { return priority; }

    struct ArgosCollector : simdb::collection::ArgosCollectorBase<Metadata>
    {
        ARGOS_COLLECT(trace_id, &Metadata::getTraceId);
        ARGOS_COLLECT(valid, &Metadata::getValid);
        ARGOS_COLLECT_ENUM(priority, &Metadata::getPriority);
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

    struct ArgosCollector : simdb::collection::ArgosCollectorBase<Packet>
    {
        ARGOS_COLLECT(timestamp, &Packet::getTimestamp);
        ARGOS_COLLECT_STRUCT(header, &Packet::getHeader);
        ARGOS_COLLECT_STRUCT(metadata, &Packet::getMetadata);
    };
};

} // namespace

namespace simdb::collection {

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

} // namespace simdb::collection

int main()
{
    auto pod = collection::createDataTypeHier<int16_t>();
    if (pod->getRoot().kind != collection::NodeKind::Pod || !pod->getRoot().pod_type ||
        *pod->getRoot().pod_type != collection::PodTypeKind::i16)
    {
        std::cerr << "POD hierarchy test failed\n";
        return 1;
    }

    auto en = collection::createDataTypeHier<Status>();
    if (en->getRoot().kind != collection::NodeKind::Enum || !en->getRoot().enum_meta)
    {
        std::cerr << "Enum hierarchy test failed\n";
        return 1;
    }
    if (en->getRoot().enum_meta->backing_kind != collection::EnumBackingKind::ui8 ||
        en->getRoot().enum_meta->members.size() != 2)
    {
        std::cerr << "Enum metadata test failed\n";
        return 1;
    }

    auto packet = collection::createDataTypeHier<Packet>();
    if (packet->getRoot().kind != collection::NodeKind::Struct || packet->getRoot().children.size() != 3)
    {
        std::cerr << "Struct hierarchy root test failed\n";
        return 1;
    }

    const auto& ts = packet->getRoot().children[0];
    if (ts->field_name != "timestamp" || ts->kind != collection::NodeKind::Pod || !ts->pod_type ||
        *ts->pod_type != collection::PodTypeKind::d)
    {
        std::cerr << "Timestamp field test failed\n";
        return 1;
    }

    const auto& header = packet->getRoot().children[1];
    if (header->field_name != "header" || header->kind != collection::NodeKind::Struct || header->children.size() != 2)
    {
        std::cerr << "Nested struct test failed\n";
        return 1;
    }
    if (header->children[0]->field_name != "seq" || header->children[1]->field_name != "status")
    {
        std::cerr << "Header field order: " << header->children[0]->field_name << ", "
                  << header->children[1]->field_name << '\n';
        std::cerr << "Nested struct test (field order) failed\n";
        return 1;
    }

    const auto& header_status = header->children[1];
    if (header_status->field_name != "status" || header_status->kind != collection::NodeKind::Enum ||
        !header_status->enum_meta || header_status->enum_meta->members.size() != 2)
    {
        std::cerr << "Nested enum field test failed\n";
        return 1;
    }

    const auto& metadata = packet->getRoot().children[2];
    if (metadata->field_name != "metadata" || metadata->kind != collection::NodeKind::Struct || metadata->children.size() != 3)
    {
        std::cerr << "Metadata struct test failed\n";
        return 1;
    }

    const auto& metadata_priority = metadata->children[2];
    if (metadata_priority->field_name != "priority" || metadata_priority->kind != collection::NodeKind::Enum ||
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
