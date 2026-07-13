// <ArgosCollector.hpp> -*- C++ *-*

#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/apps/argos/EntryPoint.hpp"
#include "simdb/apps/argos/EnumInspector.hpp"
#include "simdb/apps/argos/PipelineStagerInterface.hpp"
#include "simdb/apps/argos/Timestamps.hpp"
#include "simdb/sqlite/Dump.hpp"
#include "simdb/utils/TinyStrings.hpp"

namespace simdb::argos {

inline constexpr size_t DEFAULT_HEARTBEAT = 10;

//! \class ArgosCollector
//! \brief Main entry point into the Argos collection system.
class ArgosCollector : public App, public PipelineStagerInterface
{
public:
    //! Required by all SimDB apps
    static constexpr auto NAME = "argos-collector";

    ArgosCollector(DatabaseManager* db_mgr) :
        db_mgr_(db_mgr)
    {
    }

    static void defineSchema(Schema& schema)
    {
        using dt = SqlDataType;

        auto& globals_tbl = schema.addTable("CollectionGlobals");
        globals_tbl.addColumn("Heartbeat", dt::int32_t);

        auto& clks_tbl = schema.addTable("Clocks");
        clks_tbl.addColumn("Name", dt::string_t);
        clks_tbl.addColumn("Period", dt::uint32_t);
        clks_tbl.addColumn("Numer", dt::uint32_t);
        clks_tbl.addColumn("Denom", dt::uint32_t);
        clks_tbl.setColumnDefaultValue("Numer", 0);
        clks_tbl.setColumnDefaultValue("Denom", 0);

        auto& collectable_tns_tbl = schema.addTable("CollectableTreeNodes");
        collectable_tns_tbl.addColumn("CID", dt::int32_t);
        collectable_tns_tbl.addColumn("FullPath", dt::string_t);
        collectable_tns_tbl.addColumn("ClockID", dt::int32_t);
        collectable_tns_tbl.addColumn("TypeName", dt::string_t);
        collectable_tns_tbl.addColumn("ShowInUI", dt::int32_t);
        collectable_tns_tbl.setColumnDefaultValue("ShowInUI", 0);
        collectable_tns_tbl.ensureUnique("CID");
        collectable_tns_tbl.createIndexOn("CID");
        collectable_tns_tbl.unsetPrimaryKey();

        auto& enum_itypes_tbl = schema.addTable("CollectedEnums");
        enum_itypes_tbl.addColumn("EnumName", dt::string_t);
        enum_itypes_tbl.addColumn("EnumIntTypeName", dt::string_t);

        auto& enum_members_tbl = schema.addTable("EnumMembers");
        enum_members_tbl.addColumn("EnumID", dt::int32_t);
        enum_members_tbl.addColumn("MemberName", dt::string_t);
        enum_members_tbl.addColumn("MemberValueStr", dt::string_t);
    }

    void setHeartbeat(size_t heartbeat)
    {
        if (heartbeat == 0)
        {
            throw DBException("Cannot use 0 for Argos collector heartbeat");
        }
        heartbeat_ = heartbeat;
    }

    void addClock(const std::string& clk_name, size_t period) { addClock(clk_name, period, 0, 0); }

    void addClock(const std::string& clk_name, size_t period, size_t numer, size_t denom)
    {
        for (const auto& [_clk_name, _period, _numer, _denom] : clocks_)
        {
            if (clk_name == _clk_name)
            {
                if (period != _period || numer != _numer || denom != _denom)
                {
                    throw DBException("Clock mismatch - already registered with different params: ") << clk_name;
                }
            }
        }

        auto clk_desc = std::make_tuple(clk_name, period, numer, denom);
        clocks_.emplace_back(std::move(clk_desc));
    }

    void timestampWith(const uint64_t* backpointer)
    {
        if (timestamp_ != nullptr)
        {
            throw DBException("Cannot change timestamp object once created!");
        }
        timestamp_ = std::make_unique<Timestamp>(backpointer);
    }

    void timestampWith(uint64_t (*fn)())
    {
        if (timestamp_ != nullptr)
        {
            throw DBException("Cannot change timestamp object once created!");
        }
        timestamp_ = std::make_unique<Timestamp>(fn);
    }

    void timestampWith(std::function<uint64_t()> fn)
    {
        if (timestamp_ != nullptr)
        {
            throw DBException("Cannot change timestamp object once created!");
        }
        timestamp_ = std::make_unique<Timestamp>(fn);
    }

    //! TODO cnyce: Once the collection code from Sparta is moved to SimDB, change this
    //! to a template method so we can figure out the encoded data type name ourselves.
    //! Scalar types are encoded as follows:
    //!
    //!   For scalar PODs:
    //!   typeid(T).name()
    //!       "bool"
    //!       "unsigned long"
    //!       ...
    //!
    //!   For scalar enums with operator<< (defn held in separate table for string-int map):
    //!   typeid(T).name()
    //!       "IssueType"
    //!       "MMUState"
    //!       ...
    //!   Enum values in collection blobs are serialized as std::underlying_type_t<T>.
    //!
    //!   For scalar enums without operator<< (treated just like int PODs):
    //!   typeid(std::underlying_type_t<T>).name()
    //!       "int"
    //!       "unsigned int"
    //!       ...
    //!
    //!   For scalar struct-like types:
    //!   typeid(T).name()
    //!       "Packet"
    //!       "Inst"
    //!       ...
    //!
    //!   For scalar string-like types (std::string, const char*):
    //!       "string"
    EntryPoint* createScalarCollector(const std::string& path, const std::string& clk_name,
                                      const std::string& encoded_scalar_type)
    {
        auto entry_point = std::make_unique<EntryPoint>(this, &tiny_strings_);
        meta_by_cid_[entry_point->getID()] = std::make_tuple(path, clk_name, encoded_scalar_type);
        entry_points_.emplace_back(std::move(entry_point));
        return entry_points_.back().get();
    }

    //! TODO cnyce: Once the collection code from Sparta is moved to SimDB, change this
    //! to a template method so we can figure out the encoded data type name ourselves.
    //! Container types are encoded as follows:
    //!
    //!   <encoded_scalar_type>_<sparse/contig>_capacity<N>
    //!       "Inst_sparse_capacity32"
    //!       "bool_contig_capacity4"
    //!       ...
    EntryPoint* createContainerCollector(const std::string& path, const std::string& clk_name,
                                         const std::string& encoded_container_type)
    {
        auto entry_point = std::make_unique<EntryPoint>(this, &tiny_strings_);
        meta_by_cid_[entry_point->getID()] = std::make_tuple(path, clk_name, encoded_container_type);
        entry_points_.emplace_back(std::move(entry_point));
        return entry_points_.back().get();
    }

    TinyStrings<>* getTinyStrings() { return &tiny_strings_; }

    EnumInspector* getEnumInspector() { return &enum_inspector_; }

    void createPipeline(pipeline::PipelineManager* pipeline_mgr) override
    {
        // TODO
        (void)pipeline_mgr;
    }

    void postInit(int, char**) override
    {
        db_mgr_->INSERT(SQL_TABLE("CollectionGlobals"), SQL_VALUES(heartbeat_));

        std::map<std::string, int> clk_ids;
        auto clk_inserter = db_mgr_->prepareINSERT(SQL_TABLE("Clocks"));
        for (const auto& [_clk_name, _period, _numer, _denom] : clocks_)
        {
            auto id = clk_inserter->createRecordWithColValues(_clk_name, _period, _numer, _denom);
            clk_ids[_clk_name] = id;
        }

        auto ctn_inserter = db_mgr_->prepareINSERT(SQL_TABLE("CollectableTreeNodes"));
        for (const auto& collector : entry_points_)
        {
            auto cid = (int)collector->getID();
            const auto& full_path = std::get<0>(meta_by_cid_.at(cid));
            const auto& clk_name = std::get<1>(meta_by_cid_.at(cid));
            const auto& dtype_name = std::get<2>(meta_by_cid_.at(cid));
            const auto clk_id = clk_ids.at(clk_name);
            ctn_inserter->createRecordWithColValues(cid, full_path, clk_id, dtype_name);
        }
    }

    void stage(uint16_t cid, std::vector<char>&& scalar_bytes) override
    {
        // TODO
        (void)cid;
        (void)scalar_bytes;
    }

    void stage(uint16_t cid, std::vector<std::vector<char>>&& contig_bin_bytes) override
    {
        // TODO
        (void)cid;
        (void)contig_bin_bytes;
    }

    void stage(uint16_t cid, std::map<uint16_t, std::vector<char>>&& sparse_bin_bytes) override
    {
        // TODO
        (void)cid;
        (void)sparse_bin_bytes;
    }

    void closeRecord(uint16_t cid) override
    {
        // TODO
        (void)cid;
    }

    void postNotif(const std::string& notif, NotifType type) override
    {
        // TODO
        (void)notif;
        (void)type;
    }

    void postTeardown() override
    {
        tiny_strings_.serialize(db_mgr_);
        enum_inspector_.serializeEnumMaps(db_mgr_);

        if (verbose())
        {
            std::cout << "[simdb] Collection tables at the end of simulation (except timestamps/blobs):\n\n";
            dumpTable(db_mgr_, "CollectionGlobals");
            dumpTable(db_mgr_, "Clocks");
            dumpTable(db_mgr_, "CollectedEnums");
            dumpTable(db_mgr_, "EnumMembers");
            dumpTable(db_mgr_, "CollectableTreeNodes");
        }
    }

private:
    DatabaseManager* const db_mgr_;
    size_t heartbeat_ = DEFAULT_HEARTBEAT;

    using ClockDescriptor = std::tuple<std::string, // clk name
                                       uint32_t,    // period
                                       uint32_t,    // numerator
                                       uint32_t     // denominator
                                       >;
    std::vector<ClockDescriptor> clocks_;

    using CollectableMeta = std::tuple<std::string, // dot-delimited path
                                       std::string, // clk name
                                       std::string  // encoded data type
                                       >;
    std::map<uint16_t, CollectableMeta> meta_by_cid_;

    std::unique_ptr<Timestamp> timestamp_;
    std::vector<std::unique_ptr<EntryPoint>> entry_points_;
    TinyStrings<> tiny_strings_;
    EnumInspector enum_inspector_;
};

} // namespace simdb::argos
