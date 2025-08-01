#include "ROBCacheData.hpp"

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/elements/Buffer.hpp"
#include "simdb/pipeline/elements/Function.hpp"
#include "simdb/pipeline/AsyncDatabaseAccessor.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/Compress.hpp

#include <vector>

#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/iostreams/device/back_inserter.hpp>

// This test is to simulate the actual 
// Profile data pipeline that is to exist in 
// Shinro's performance model

/// Defining Instruction Type encoding
static const std::unordered_map<int32_t, std::string> inst_type_to_name = {
    {1: "memory_operation"},
    {2: "branch"},
    {4: "float"},
    {8: "vector"},
    {17, "store"},
    {33, "load"},
    {65, "atomic"},
    {18, "indirect"},
    {34, "conditional"},
    {66, "call"},
    {130, "return"}
};

/// Defining Metadata values
// TODO ztaufique: need to figure out if it makes more sense for these to be strings or something else
static const std::unordered_map<int32_t, std::string> metadata_to_name = {
    {1: "misaligned_cache_access"},
    {2: "bank_conflict"}
};

using ROBCacheDetailedEvents = std::vector<ROBCacheProfileData>;

struct ROBCacheDetailedAsBytes
{
    std::vector<char> rob_cache_detailed_bytes;
    uint32_t start_tick = 0;
    uint32_t end_tick = 0;
};

class ProfilerWriter : public simdb::app
{
    public:
        using ROBCacheSummaryContainer = std::unordered_map<uint64, ROBCacheSummaryData>;

        static constexpr auto NAME = "profiler-app";

        ProfilerWriter(simdb::DatabaseManager* db_mgr)
            : db_mgr_(db_mgr)
        {}

        ~ProfilerWriter() noexcept = default;

        static void defineSchema(simdb::Schema& schema)
        {
            using dt = simdb::SqlDataType;

            auto & rob_cache_summary_table = table.addTable("ROBCacheSummaryData");
            rob_cache_summary_table.addColumn("inst_pc", dt::int64_t);
            rob_cache_summary_table.addColumn("thread_id", dt::int32_t);
            rob_cache_summary_table.addColumn("cycle_first_appended", dt::int64_t);
            rob_cache_summary_table.addColumn("latest_ipc", dt::double_t);
            rob_cache_summary_table.addColumn("l1dcache_hits", dt::int32_t);
            rob_cache_summary_table.addColumn("l1dcache_misses", dt::int32_t);
            rob_cache_summary_table.addColumn("mispred_penalty", dt::int32_t);
            rob_cache_summary_table.addColumn("num_mispred", dt::int32_t);
            rob_cache_summary_table.addColumn("num_taken", dt::int32_t);
            rob_cache_summary_table.addColumn("cycles_cost", dt::int32_t);
            rob_cache_summary_table.addColumn("num_flushes", dt::int32_t);
            rob_cache_summary_table.addColumn("num_seen", dt::int32_t);

            auto & rob_cache_table = table.addTable("ROBCacheDetailedInstData");
            rob_cache_table.addColumn("start_tick", dt::int64_t);
            rob_cache_table.addColumn("end_tick", dt::int64_t);
            rob_cache_table.addColumn("inst_data", dt::blob_t);

            /// Additional columns that remain the same across tables
            auto & inst_type_table = table.addTable("InstTypeEncoding");
            inst_type_table.addColumn("inst_type", dt::int32_t);
            inst_type_table.addColumn("name", dt::string_t);

            auto & mem_region_encoding = table.addTable("MemoryRegionEncoding");
            mem_region_encoding.addColumn("mem_region", dt::int32_t);
            mem_region_encoding.addColumn("name", dt::string_t);
        }

        void process(ROBCacheProfile && data)
        {
            rob_cache_profile_data_input_queue_.emplace(std::move(data));
        }

        std::unique_ptr<simdb::pipeline::Pipeline> ProfilerWriterApp::createPipeline(
            simdb::pipeline::AsyncDatabaseAccessor* db_accessor) override
        {   

            //------------------------- TASK 1 -------------------------//
            /// Move semantic of ProfileData
            auto async_queue = simdb::pipeline::createTask<simdb::pipeline::Function<void, ROBCacheProfileData>>(
                [this](simdb::ConcurrentQueue<ROBCacheProfileData> & out)
                {
                    ROBCacheProfileData in;
                    if (rob_cache_profile_data_input_queue_.try_pop(in))
                    {
                        out.emplace(std::move(in));
                    }
                }
            );

            //------------------------- TASK 2 -------------------------//
            auto rob_cache_summaries_writer = db_accessor->createAsnycWriter<ProfilerWriter, void>(
                [](simdb::pipeline::AppPreparedINSERTs* tables)
                {   
                    // Remove all records previously stored in the table
                    db_mgr_.removeAllRecordsFromTable("ROBCacheSummaryData");

                    auto inserter = tables->getPreparedINSERT("ROBCacheSummaryData");

                    for (const auto& summary_pair : rob_cache_summaries_)
                    {
                        // Insert summary data rows to SQLite 
                        inserter->setColumnValue(0, summary_pair.second.inst_pc);
                        inserter->setColumnValue(1, summary_pair.second.thread_id);
                        inserter->setColumnValue(2, summary_pair.second.cycle_first_appended);
                        inserter->setColumnValue(3, summary_pair.second.latest_ipc);
                        inserter->setColumnValue(4, summary_pair.second.l1dcache_hits);
                        inserter->setColumnValue(5, summary_pair.second.l1dcache_misses);
                        inserter->setColumnValue(6, summary_pair.second.mispred_penalty);
                        inserter->setColumnValue(7, summary_pair.second.num_mispred);
                        inserter->setColumnValue(8, summary_pair.second.num_taken);
                        inserter->setColumnValue(9, summary_pair.second.cycles_cost);
                        inserter->setColumnValue(10, summary_pair.second.num_flushes);
                        inserter->setColumnValue(11, summary_pair.second.num_seen);

                        inserter->createRecord();
                    }                    
                }
            );

            auto update_summaries =   
                simdb::pipeline::createTask<simdb::pipeline::Function<ROBCacheProfileData, ROBCacheProfileData>>(
                    [rob_cache_summaries_writer, num_inst_seen = 0]
                    (ROBCacheProfileData&& profile_data, simdb::ConcurrentQueue<ROBCacheProfileData> & out)
                    {   

                        if (auto & summary = rob_cache_summaries_.find(profile_data.inst_pc); summary != rob_cache_summaries_.end())
                        {
                            // Accumate summaries
                            auto & summary_profile_data = summary->second;
                            
                            summary_profile_data.cycles_cost += profile_data.cycles_cost;
                            summary_profile_data.latest_ipc = profile_data.latest_ipc; 
                            (profile_data.ld1cache_hit) ? ++summary_profile_data.l1dcache_hits : ++summary_profile_data.l1dcache_misses;

                            summary_profile_data.total_mispred_penalty += profile_data.mispred_penalty;                
                            summary_profile_data.num_mispred += static_cast<uint32_t>(profile_data.is_mispred);
                            summary_profile_data.num_taken += static_cast<uint32_t>(profile_data.is_taken);
                            
                            summary_profile_data.num_flushes += static_cast<uint32_t>(profile_data.num_flushes);
                            ++summary_profile_data.num_seen;
                        }
                        else
                        {
                            rob_cache_summaries_.emplace({profile_data.inst_pc, profile_data.thread_id, profile_data.cycle_first_appended,
                                              profile_data.inst_type, profile_data.latest_ipc, static_cast<uint32_t>(profile_data.l1dcache_hit),
                                              static_cast<uint32_t>(profile_data.l1dcache_miss), profile_data.mispred_penalty,
                                              static_cast<uint32_t>(profile_data.is_mispred), static_cast<uint32_t>(profile_data.is_taken),
                                              profile_data.cycles_cost, static_cast<uint32_t>(profile_data.is_flush)});
                        }

                        ++num_inst_seen;

                        // Snapshot write of Summary Profile Data
                        if (num_inst_seen.size() % (2 * DETAILED_DATA_BUFFER_LEN_))
                        {   
                            rob_cache_summaries_writer->emplace(std::move(rob_cache_summaries_.copy()));
                        }
                    }
                );

            //------------------------- TASK 3 -------------------------//
            /// Function to update disassembly and other associative metadata


            //------------------------- TASK 1 -------------------------//
            /// This task buffer detailed data for better compression
            auto buffer_task = simdb::pipeline::createTask<simdb::pipeline::Buffer<ROBCacheProfileData>>(DETAILED_DATA_BUFFER_LEN_);

            //------------------------- TASK 2 -------------------------//
            /// This tasks takes the detailed data buffered events and serializes them to std::vector<char> buffers
            /// Serialization converts the complex data structure in a format that can be easily understood 
            /// also making the data storable by converting to byte stream 
            auto serialize_task = simdb::pipeline::createTask<simdb::pipeline::Function<ROBCacheDetailedEvents, ROBCacheDetailedRangeAsBytes>>(
                [](ROBCacheDetailedEvents&& evts, simdb::ConcurrentQueue<ROBCacheDetailedAsBytes>& out)
                {   
                    // TODO ztaufique: probably want to add a static assertion here
                    ROBCacheDetailedRangeAsBytes detailed_as_bytes;
                    detailed_as_bytes.start_tick = evts[0].cycle_first_appended;
                    detailed_as_bytes.end_tick = evts[DETAILED_DATA_BUFFER_LEN_-1].cycle_first_appended;

                    std::vector<char>& buffer = detailed_as_bytes.rob_cache_detailed_bytes;

                    boost::iostreams::back_insert_device>std::vector<char>> inserter(buffer);
                    boost::iostream::stream<boost::iostreams::back_insert_device<std::vector<char>>> os(inserter)l
                    boost::archive::binary_oarchive oa(os);
                    os << evts;
                    os.flush();

                    out.emplace(std::move(detailed_as_bytes));
                }
            );

            //------------------------- TASK 3 -------------------------//
            /// Perform zlib compression on the event ranges
            /// zlib compression is a software library used for data compression that reduces the size of the data
            /// for faster transimission
            /// works best on structure data which is often the result of serialization
            auto zlib_task = simdb::pipeline::createTask<simdb::pipeline::Function<ROBCacheDetailedAsBytes, ROBCacheDetailedAsBytes>>(
                [](ROBCacheDetailedAsBytes&& uncompressed, simdb::ConcurrentQueue<ROBCacheDetailedAsBytes>& out)
                {
                    ROBCacheDetailedAsBytes compressed;
                    compressed.start_tick = uncompressed.start_tick;
                    compressed.end_tick = uncompressed.end_tick;

                    simdb::compressData(uncompressed.rob_cache_detailed_bytes, compressed.rob_cache_detailed_bytes);
                    out.emplace(std::move(compressed));
                }
            );

            //------------------------- TASK 4 -------------------------//
            /// This task receives the serialized and compressed detailed data from the zlib task and writes them to disk on the DB thread
            auto async_writer = db_accessor->createAsnycWriter<ProfilerWriter, void>(
                [](ROBCacheDetailedAsBytes&& evts,
                    simdb::pipeline::AppPreparedINSERTs* tables)
                    {
                        auto inserter = tables->getPreparedINSERT("ROBCacheDetailedInstData");

                        inserter->setColumnValue(0, evts.start_tick);
                        inserter->setColumnValue(1, evts.end_tick);
                        inserter->setColumnValue(2, evts.rob_cache_detailed_bytes);
                        inserter->createRecord();
                    }
            ) 
        }

        // Responsible for writing 
        void postTeardown() override
        {
            
        }

    private:
        simdb::ConcurrentQueue<ROBCacheProfileData> rob_cache_profile_data_input_queue_;

        // In-memory summary data storage
        ROBCacheSummaryContainer rob_cache_summaries_;

        simdb::DatabaseManager* dg_mgr_ = nullptr;
        simdb::pipeline::AsyncDatabaseAccessor* async_db_accessor_ = nullptr;
};

REGISTER_SIMDB_APPLICATION(ProfilerWriter);

TEST_INIT;

int main(int argc, char** argv)
{
    simdb::DatabaseManager db_mgr("profiler_test.db", true);
    simdb::ApppManager app_mgr(&db_mgr);
    app_mgr.enableApp(ProfilerWriter::NAME);

    // Setup...
    app_mgr.createEnabledApp();
    app_mgr.createSchemas();
    app_mgr.postInit(argc, argv);
    app_mgr.openPipelines();

    // Simulate
    auto app = app_mgr.getApp<ProfilerWriter>();


    // Simulate 100M instructions with hopefully about ~3000 unique instruction addresses
    constexpr uint64_t TICKS = 100000000;
    for (uint64_t tick = 1; tick <= TICKS; ++ticks)
    {   
        srand(time(0));

        uint64_t inst_pc = rand() % 3000 + 1;

        auto profile_data_event = ROBCacheProfileData(inst_pc);

        app->process(std::move(profile_data_event));
    }

    // Finish...
    app_mgr.postSimLoopTeardown();

    REPORT_ERROR;
    return ERROR_CODE;
}
    
