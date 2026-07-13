// <ArgosCollector.hpp> -*- C++ *-*

#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/apps/argos/EnumInspector.hpp"
#include "simdb/apps/argos/Timestamps.hpp"
#include "simdb/sqlite/Dump.hpp"
#include "simdb/utils/TinyStrings.hpp"

namespace simdb::argos {

inline constexpr size_t DEFAULT_HEARTBEAT = 10;

//! \class ArgosCollector
//! \brief Main entry point into the Argos collection system.
class ArgosCollector : public App
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

    TinyStrings<>* getTinyStrings() { return &tiny_strings_; }

    EnumInspector* getEnumInspector() { return &enum_inspector_; }

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

    std::unique_ptr<Timestamp> timestamp_;
    TinyStrings<> tiny_strings_;
    EnumInspector enum_inspector_;
};

} // namespace simdb::argos
