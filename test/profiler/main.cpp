#include "ROBCacheData.hpp"

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/elements/Buffer.hpp"
#include "simdb/pipeline/elements/Function.hpp"
#include "simdb/pipeline/AsyncDatabaseAccessor.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/Compress.hpp"
#include "SimDBTester.hpp"

#include <vector>
#include <string>
#include <map>
#include <random>

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>

// This test is to simulate the actual 
// Profile data pipeline that is to exist in 
// Shinro's performance model

// /// Defining Instruction Type encoding
// static const std::unordered_map<int32_t, std::string> inst_type_to_name = {
//     {1, "memory_operation"},
//     {2, "branch"},
//     {4, "float"},
//     {8, "vector"},
//     {17, "store"},
//     {33, "load"},
//     {65, "atomic"},
//     {18, "indirect"},
//     {34, "conditional"},
//     {66, "call"},
//     {130, "return"}
// };

// /// Defining Metadata values
// // TODO ztaufique: need to figure out if it makes more sense for these to be strings or something else
// static const std::unordered_map<int32_t, std::string> metadata_to_name = {
//     {1, "misaligned_cache_access"},
//     {2, "bank_conflict"}
// };

using ROBCacheDetailedEvents = std::vector<ROBCacheProfileData>;
using ROBCacheSummaryContainer = std::unordered_map<uint64_t, ROBCacheSummaryData>;
using InstDisassemblyContainer = std::unordered_map<uint64_t, std::string>;

struct ROBCacheDetailedAsBytes
{
    std::vector<char> rob_cache_detailed_bytes;
    uint32_t start_tick = 0;
    uint32_t end_tick = 0;
};

class ProfilerWriter : public simdb::App
{
    public:
        static constexpr auto NAME = "profiler-app";

        ProfilerWriter(simdb::DatabaseManager* db_mgr)
            : db_mgr_(db_mgr)
        {}

        ~ProfilerWriter() noexcept = default;

        static void defineSchema(simdb::Schema& schema)
        {
            using dt = simdb::SqlDataType;

            auto & rob_cache_summary_table = schema.addTable("ROBCacheSummaryData");
            rob_cache_summary_table.addColumn("inst_pc", dt::uint64_t);
            rob_cache_summary_table.addColumn("thread_id", dt::int32_t);
            rob_cache_summary_table.addColumn("opcode", dt::uint64_t);
            rob_cache_summary_table.addColumn("cycle_first_appended", dt::uint64_t);
            rob_cache_summary_table.addColumn("inst_type", dt::int32_t);
            rob_cache_summary_table.addColumn("latest_ipc", dt::double_t);
            rob_cache_summary_table.addColumn("l1dcache_hits", dt::int32_t);
            rob_cache_summary_table.addColumn("l1dcache_misses", dt::int32_t);
            rob_cache_summary_table.addColumn("total_mispred_penalty", dt::int32_t);
            rob_cache_summary_table.addColumn("num_mispred", dt::int32_t);
            rob_cache_summary_table.addColumn("num_taken", dt::int32_t);
            rob_cache_summary_table.addColumn("cycles_cost", dt::int32_t);
            rob_cache_summary_table.addColumn("num_flushes", dt::int32_t);
            rob_cache_summary_table.addColumn("num_seen", dt::int32_t);

            auto & rob_cache_table = schema.addTable("ROBCacheDetailedInstData");
            rob_cache_table.addColumn("start_tick", dt::uint64_t);
            rob_cache_table.addColumn("end_tick", dt::uint64_t);
            rob_cache_table.addColumn("inst_data", dt::blob_t);

            auto & inst_disasm_table = schema.addTable("InstDisassembly");
            inst_disasm_table.addColumn("opcode", dt::uint64_t);
            inst_disasm_table.addColumn("disassembly", dt::string_t);

            // TODO: Discuss with AE which representation aligns with their need
            // /// Additional columns that remain the same across tables
            // auto & inst_type_table = schema.addTable("InstTypeEncoding");
            // inst_type_table.addColumn("inst_type", dt::int32_t);
            // inst_type_table.addColumn("name", dt::string_t);

            // auto & mem_region_encoding = schema.addTable("MemoryRegionEncoding");
            // mem_region_encoding.addColumn("mem_region", dt::int32_t);
            // mem_region_encoding.addColumn("name", dt::string_t);
        }

        void process(ROBCacheProfileData && profile_data)
        {   
            summary_pipeline_head_->emplace(profile_data); // TODO ztaufique this might not be what we need to do
            detailed_pipeline_head_->emplace(std::move(profile_data));
        }

        std::unique_ptr<simdb::pipeline::Pipeline> createPipeline(
            simdb::pipeline::AsyncDatabaseAccessor* db_accessor) override
        {   
            auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

            //------------------------- SUMMARY DATA PIPELINE TASKS -------------------------//
            //------------------------- TASK 1 -------------------------//
            /// Update In-Memory Data Structure
            auto update_summaries =   
                simdb::pipeline::createTask<simdb::pipeline::Function<ROBCacheProfileData, int>>(
                    [num_inst_seen = 0, this]
                    (ROBCacheProfileData&& profile_data, simdb::ConcurrentQueue<int>& out) mutable
                    {   
                        const uint64_t inst_pc = profile_data.inst_pc;

                        if (auto summary = this->rob_cache_summaries_.find(inst_pc); summary != this->rob_cache_summaries_.end())
                        {
                            // Accumate summaries
                            auto & summary_profile_data = summary->second;
                            
                            summary_profile_data.cycles_cost += profile_data.cycles_cost;
                            summary_profile_data.latest_ipc = profile_data.latest_ipc; 
                            (profile_data.l1dcache_hit) ? ++summary_profile_data.l1dcache_hits : ++summary_profile_data.l1dcache_misses;

                            summary_profile_data.total_mispred_penalty += profile_data.mispred_penalty;                
                            summary_profile_data.num_mispred += static_cast<uint32_t>(profile_data.is_mispred);
                            summary_profile_data.num_taken += static_cast<uint32_t>(profile_data.is_taken);
                            
                            summary_profile_data.num_flushes += static_cast<uint32_t>(profile_data.is_flush);
                            ++summary_profile_data.num_seen;
                        }
                        else
                        {
                            this->rob_cache_summaries_.try_emplace(profile_data.inst_pc, ROBCacheSummaryData{profile_data.inst_pc, profile_data.thread_id, profile_data.opcode,
                                              profile_data.cycle_first_appended,
                                              profile_data.inst_type, profile_data.latest_ipc, static_cast<uint32_t>(profile_data.l1dcache_hit),
                                              static_cast<uint32_t>(profile_data.l1dcache_miss), profile_data.mispred_penalty,
                                              static_cast<uint32_t>(profile_data.is_mispred), static_cast<uint32_t>(profile_data.is_taken),
                                              profile_data.cycles_cost, static_cast<uint32_t>(profile_data.is_flush)});

                            // Add to in-memory disassembly
                            this->inst_disassembly_.try_emplace(profile_data.opcode, profile_data.disassembly);
                        }

                        ++num_inst_seen;

                        out.push(num_inst_seen);
                    }
                );
        
            //------------------------- TASK 2 -------------------------//
            /// Summary Data Snapshots
            auto snapshot_summaries = db_accessor->createAsyncWriter<ProfilerWriter, int, void>(
                [this](int&& num_inst_seen, simdb::pipeline::AppPreparedINSERTs* tables)
                {   
                    // Snapshot write of summary Profile Data
                    if (num_inst_seen % (2 * DETAILED_DATA_BUFFER_LEN_) == 0)
                    {  
                        std::cout << "We are writing a snapshot of summaries" << std::endl;
                        // Remove all records previously stored in the table
                        db_mgr_->removeAllRecordsFromTable("ROBCacheSummaryData");

                        auto inserter = tables->getPreparedINSERT("ROBCacheSummaryData");

                        for (const auto& summary_pair : this->rob_cache_summaries_)
                        {
                            // Insert summary data rows to SQLite 
                            inserter->setColumnValue(0, summary_pair.second.inst_pc);
                            inserter->setColumnValue(1, summary_pair.second.thread_id);
                            inserter->setColumnValue(2, summary_pair.second.opcode);
                            inserter->setColumnValue(3, summary_pair.second.cycle_first_appended);
                            inserter->setColumnValue(4, summary_pair.second.inst_type);
                            inserter->setColumnValue(5, summary_pair.second.latest_ipc);
                            inserter->setColumnValue(6, summary_pair.second.l1dcache_hits);
                            inserter->setColumnValue(7, summary_pair.second.l1dcache_misses);
                            inserter->setColumnValue(8, summary_pair.second.total_mispred_penalty);
                            inserter->setColumnValue(9, summary_pair.second.num_mispred);
                            inserter->setColumnValue(10, summary_pair.second.num_taken);
                            inserter->setColumnValue(11, summary_pair.second.cycles_cost);
                            inserter->setColumnValue(12, summary_pair.second.num_flushes);
                            inserter->setColumnValue(13, summary_pair.second.num_seen);

                            inserter->createRecord();
                        }
                    }  
                }
            );

            //------------------------- DETAILED DATA PIPELINE TASKS -------------------------//
            //------------------------- TASK 1 -------------------------//
            /// This task buffer detailed data for better compression
            auto buffer_task = simdb::pipeline::createTask<simdb::pipeline::Buffer<ROBCacheProfileData>>(DETAILED_DATA_BUFFER_LEN_);

            //------------------------- TASK 2 -------------------------//
            /// This tasks takes the detailed data buffered events and serializes them to std::vector<char> buffers
            /// Serialization converts the complex data structure in a format that can be easily understood 
            /// also making the data storable by converting to byte stream 
            auto serialize_task = simdb::pipeline::createTask<simdb::pipeline::Function<ROBCacheDetailedEvents, ROBCacheDetailedAsBytes>>(
                [](ROBCacheDetailedEvents&& evts, simdb::ConcurrentQueue<ROBCacheDetailedAsBytes>& out)
                {   
                    // TODO ztaufique: probably want to add a static assertion here
                    ROBCacheDetailedAsBytes detailed_as_bytes;
                    detailed_as_bytes.start_tick = evts[0].cycle_first_appended;
                    detailed_as_bytes.end_tick = evts[DETAILED_DATA_BUFFER_LEN_-1].cycle_first_appended;

                    std::vector<char>& buffer = detailed_as_bytes.rob_cache_detailed_bytes;

                    boost::iostreams::back_insert_device<std::vector<char>> inserter(buffer);
                    boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> os(inserter);
                    boost::archive::binary_oarchive oa(os);
                    oa << evts;
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
            auto async_writer = db_accessor->createAsyncWriter<ProfilerWriter, ROBCacheDetailedAsBytes, void>(
                [](ROBCacheDetailedAsBytes&& evts,
                    simdb::pipeline::AppPreparedINSERTs* tables)
                    {
                        auto inserter = tables->getPreparedINSERT("ROBCacheDetailedInstData");

                        inserter->setColumnValue(0, evts.start_tick);
                        inserter->setColumnValue(1, evts.end_tick);
                        inserter->setColumnValue(2, evts.rob_cache_detailed_bytes);
                        inserter->createRecord();
                    }
            );
        
            // Connect tasks ---------------------------------------------------------------------------
            // TODO ztaufqiue: There is an issue with DB writes in that if the buffer is not full, some data will never be written
            *buffer_task >> *serialize_task >> *zlib_task >> *async_writer;
            *update_summaries >> *snapshot_summaries;

            // Two pipeline heads: One for detailed and one for summaries
            detailed_pipeline_head_ = buffer_task->getTypedInputQueue<ROBCacheProfileData>();
            summary_pipeline_head_ = update_summaries->getTypedInputQueue<ROBCacheProfileData>();

            pipeline->createTaskGroup("DetailedBufferAndCommit")
                    ->addTask(std::move(buffer_task))
                    ->addTask(std::move(serialize_task))
                    ->addTask(std::move(zlib_task));

            pipeline->createTaskGroup("SummaryAccumulateAndSnapshot")
                    ->addTask(std::move(update_summaries));

            async_db_accessor_ = db_accessor;
            return pipeline;
        }

        // Responsible for writing final summary data and writing disassembly
        void postTeardown() override
        {
            std::cout << "Removing record from the summaries table" << std::endl;
            db_mgr_->removeAllRecordsFromTable("ROBCacheSummaryData");
            
            assert(false == rob_cache_summaries_.empty());

            auto summaries_inserter = db_mgr_->prepareINSERT(
                SQL_TABLE("ROBCacheSummaryData"),
                SQL_COLUMNS("inst_pc", "thread_id", "opcode", "cycle_first_appended", "inst_type", "latest_ipc", "l1dcache_hits",
                            "l1dcache_misses", "total_mispred_penalty", "num_mispred", "num_taken", "cycles_cost", "num_flushes", "num_seen"));

            for (const auto& summary_pair : this->rob_cache_summaries_)
            {   
                // TODO ztaufique: Can this be a function?
                // Insert summary data rows to SQLite 
                summaries_inserter->setColumnValue(0, summary_pair.second.inst_pc);
                summaries_inserter->setColumnValue(1, summary_pair.second.thread_id);
                summaries_inserter->setColumnValue(2, summary_pair.second.opcode);
                summaries_inserter->setColumnValue(3, summary_pair.second.cycle_first_appended);
                summaries_inserter->setColumnValue(4, summary_pair.second.inst_type);
                summaries_inserter->setColumnValue(5, summary_pair.second.latest_ipc);
                summaries_inserter->setColumnValue(6, summary_pair.second.l1dcache_hits);
                summaries_inserter->setColumnValue(7, summary_pair.second.l1dcache_misses);
                summaries_inserter->setColumnValue(8, summary_pair.second.total_mispred_penalty);
                summaries_inserter->setColumnValue(9, summary_pair.second.num_mispred);
                summaries_inserter->setColumnValue(10, summary_pair.second.num_taken);
                summaries_inserter->setColumnValue(11, summary_pair.second.cycles_cost);
                summaries_inserter->setColumnValue(12, summary_pair.second.num_flushes);
                summaries_inserter->setColumnValue(13, summary_pair.second.num_seen);

                summaries_inserter->createRecord();
            }

            assert(false == inst_disassembly_.empty());

            // Write disassembly to SQLite DB
            auto disasm_inserter = db_mgr_->prepareINSERT(
                SQL_TABLE("InstDisassembly"),
                SQL_COLUMNS("opcode", "disassembly"));

            for (const auto& disasm_pair : this->inst_disassembly_)
            {
                // Insert disassembly rows to SQLite
                disasm_inserter->setColumnValue(0, disasm_pair.first);
                disasm_inserter->setColumnValue(1, disasm_pair.second);

                disasm_inserter->createRecord();
            }
        }

    private:
        static constexpr uint64_t DETAILED_DATA_BUFFER_LEN_ = 0x100000;

        simdb::DatabaseManager* db_mgr_ = nullptr;
        simdb::pipeline::AsyncDatabaseAccessor* async_db_accessor_ = nullptr;
        simdb::ConcurrentQueue<ROBCacheProfileData>* detailed_pipeline_head_ = nullptr;
        simdb::ConcurrentQueue<ROBCacheProfileData>* summary_pipeline_head_ = nullptr;

        // TODO ztaufqiue: This is temporary - It should not be stored in the pipeline
        ROBCacheSummaryContainer rob_cache_summaries_;
        InstDisassemblyContainer inst_disassembly_;
};

REGISTER_SIMDB_APPLICATION(ProfilerWriter);

TEST_INIT;

int main(int argc, char** argv)
{
    simdb::DatabaseManager db_mgr("profiler_test.db", true);
    simdb::AppManager app_mgr(&db_mgr);
    app_mgr.enableApp(ProfilerWriter::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.postInit(argc, argv);
    app_mgr.openPipelines();

    // Simulate
    auto app = app_mgr.getApp<ProfilerWriter>();

    std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<uint64_t> inst_pc_dis(1, 8001);
    std::uniform_int_distribution<uint64_t> opcode_dis(1, 3001);

    // Simulate 100M instructions with hopefully about ~3000 unique instruction addresses
    constexpr uint64_t TICKS = 100000000;
    for (uint64_t tick = 1; tick <= TICKS; ++tick)
    {   
        auto inst_pc = inst_pc_dis(gen);
        auto opcode = opcode_dis(gen);

        auto profile_data_event = ROBCacheProfileData(inst_pc, opcode);

        app->process(std::move(profile_data_event));
    }

    // Finish...
    app_mgr.postSimLoopTeardown();

    REPORT_ERROR;
    return ERROR_CODE;
}
    
