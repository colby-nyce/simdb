// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/elements/Buffer.hpp"
#include "simdb/pipeline/elements/Function.hpp"
#include "simdb/pipeline/elements/DatabaseQueue.hpp"
#include "simdb/utils/CircularBuffer.hpp"
#include "simdb/utils/Compress.hpp"
#include "SimDBTester.hpp"

// This test creates a SimDB app with a pipeline that implements a multi-stage cache.
// Similar to memory hierarchy, each stage of this cache will be slower than the one
// "upstream" of it when recreating data originally sent down the pipeline.
//
// Simulation thread:
//   - Use a small circular buffer of raw data not in pipeline yet (fastest)
//
// Processing thread:
//   - Holds onto copies of the pipeline data until it is committed to disk (fast)
//   - Performs buffering and compression on input data
//   - Sends compressed data to DB thread
//
// Database thread:
//   - Commits compressed data to the database
//   - Sends notification back to the cache to evict committed data
//   - Fallback for data retrieval if not in the circular buffer or cache (slow)
//
//               _____________________________________
//               |                ________________   |
//               |                |              |   |
//   Simulation  |--|-- copy ---> |     Cache    |<<=|======||
//   thread      |  |             |______________|   |      ||
//               |  |             ________________   |      ||      Processing
//               |  |             |              |   |      ||      thread
//               |  |-----------> |    Buffer    |   |      ||
//               |                |______________|   |      ||E
//               |                      ||||         |      ||V
//               |                      \\//         |      ||I
//               |                _______\/_______   |      ||C
//               |                |              |   |      ||T
//               |                |     Zlib     |   |      ||
//               |                |______________|   |      ||
//               |________________________|__________|      ||
//                                        |                 ||
//                                        |                 ||
//    ---------------------------------------------------------------------------
//                                        |                 ||
//               _________________________|___________      ||
//               |                ________|_______   |      ||
//               |                |              |   |      ||      Database
//               |                |    SQLite    |---|------||      thread
//               |                |______________|   |
//               |___________________________________|
//
//
enum class RegType : uint64_t
{
    INT,
    FP,
    VEC,
    CSR,
    __COUNT__
};

struct RegWrite
{
    RegType reg_type;
    uint64_t reg_num;
    uint64_t prev_val;
    uint64_t curr_val;

    static RegWrite createRandom()
    {
        RegWrite reg_write;
        reg_write.reg_type = static_cast<RegType>(rand() % (int)RegType::__COUNT__);
        reg_write.reg_num = rand() % 32;
        reg_write.prev_val = rand();
        reg_write.curr_val = rand();
        return reg_write;
    }
};

using InstEventUID = uint64_t;
using InstructionEventUIDRange = std::pair<uint64_t, uint64_t>;

template <typename T>
void append(std::vector<char>& bytes, const T val)
{
    bytes.resize(bytes.size() + sizeof(T));
    auto ptr = bytes.data() + bytes.size() - sizeof(T);
    memcpy(ptr, &val, sizeof(T));
}

template <>
void append(std::vector<char>& bytes, RegType reg_type)
{
    append(bytes, uint64_t(reg_type));
}

template <>
void append(std::vector<char>& bytes, const std::vector<RegWrite>& reg_writes)
{
    append(bytes, uint64_t(reg_writes.size()));
    for (const auto& reg_write : reg_writes)
    {
        append(bytes, reg_write.reg_type);
        append(bytes, reg_write.reg_num);
        append(bytes, reg_write.prev_val);
        append(bytes, reg_write.curr_val);
    }
}

class InstEvent
{
public:
    InstEventUID uid;
    uint64_t hart;
    uint64_t opcode;
    uint64_t curr_pc;
    uint64_t next_pc;
    std::vector<RegWrite> reg_writes;

    std::vector<char> toBytes() const
    {
        std::vector<char> bytes;
        append(bytes, uid);
        append(bytes, hart);
        append(bytes, opcode);
        append(bytes, curr_pc);
        append(bytes, next_pc);
        append(bytes, reg_writes);
        return bytes;
    }

    static InstEvent createRandom(InstEventUID uid)
    {
        InstEvent inst_evt;
        inst_evt.uid = uid;
        inst_evt.hart = rand() % 4;
        inst_evt.opcode = rand();
        inst_evt.curr_pc = rand();
        inst_evt.next_pc = inst_evt.next_pc + 4;

        for (auto i = 0; i < rand() % 2; ++i)
        {
            inst_evt.reg_writes.emplace_back(RegWrite::createRandom());
        }

        return inst_evt;
    }

    // Needed for std::lower_bound to work with uid directly
    bool operator<(uint64_t other_uid) const
    {
        return uid < other_uid;
    }
};

class MultiStageCache : public simdb::App
{
public:
    static constexpr auto NAME = "multi-stage-cache";

    MultiStageCache(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    ~MultiStageCache() noexcept = default;

    bool defineSchema(simdb::Schema& schema) override
    {
        using dt = simdb::SqlDataType;

        auto& tbl = schema.addTable("CompressedEvents");
        tbl.addColumn("StartUID", dt::int64_t);
        tbl.addColumn("EndUID", dt::int64_t);
        tbl.addColumn("CompressedEvtBytes", dt::blob_t);
        tbl.createCompoundIndexOn({"StartUID", "EndUID"});

        return true;
    }

    class Cache
    {
    public:
        // This method takes a copy of the event (the original goes down the pipeline).
        void addToCache(InstEvent evt)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            cached_evts_.emplace_back(std::move(evt));
        }

        // This method's data queue is fed by the database pipeline task, and consumed
        // on the same thread as addToCache() to reduce contention over mutex_.
        void evictFromCache(const InstructionEventUIDRange& uid_range)
        {
            std::lock_guard<std::mutex> lock(mutex_);

            const auto start_uid = uid_range.first;
            const auto end_uid = uid_range.second;

            // Find first event with uid >= start_uid
            auto first = std::lower_bound(cached_evts_.begin(), cached_evts_.end(), start_uid);

            // Find first event with uid > end_uid (i.e. strictly after range)
            auto last = std::upper_bound(cached_evts_.begin(), cached_evts_.end(), end_uid,
                [](uint64_t val, const InstEvent& d)
                {
                    return val < d.uid;
                });

            // Erase range [first, last)
            cached_evts_.erase(first, last);
        }

    private:
        std::mutex mutex_;
        std::deque<InstEvent> cached_evts_;
    };

    std::unique_ptr<simdb::pipeline::Pipeline> createPipeline() override
    {
        auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

        // Recall the pipeline we want to build:
        //               _____________________________________
        //               |                ________________   |
        //               |                |              |   |
        //   Simulation  |--|-- copy ---> |     Cache    |<<=|======||
        //   thread      |  |             |______________|   |      ||
        //               |  |             ________________   |      ||      Processing
        //               |  |             |              |   |      ||      thread
        //               |  |-----------> |    Buffer    |   |      ||
        //               |                |______________|   |      ||E
        //               |                      ||||         |      ||V
        //               |                      \\//         |      ||I
        //               |                _______\/_______   |      ||C
        //               |                |              |   |      ||T
        //               |                |     Zlib     |   |      ||
        //               |                |______________|   |      ||
        //               |________________________|__________|      ||
        //                                        |                 ||
        //                                        |                 ||
        //    ---------------------------------------------------------------------------
        //                                        |                 ||
        //               _________________________|___________      ||
        //               |                ________|_______   |      ||
        //               |                |              |   |      ||      Database
        //               |                |    SQLite    |---|------||      thread
        //               |                |______________|   |
        //               |___________________________________|
        //
        cache_ = std::make_shared<Cache>();

        // This task gives a copy of an event to the cache and sends the original down the pipeline
        auto new_evt_task = simdb::pipeline::createTask<simdb::pipeline::Function<InstEvent, InstEvent>>(
            [cache = cache_](InstEvent&& evt, simdb::ConcurrentQueue<InstEvent>& out)
            {
                cache->addToCache(evt);
                out.emplace(std::move(evt));
            }
        );

        // This task buffers 100 events for better compression
        auto buffer_task = simdb::pipeline::createTask<simdb::pipeline::Buffer<InstEvent>>(100);

        // This task takes buffered events and preps them for efficient DB insertion
        using InstEvents = std::vector<InstEvent>;

        struct InstEventsRange
        {
            InstEvents events;
            InstEventUID start_uid;
            InstEventUID end_uid;
        };

        auto range_task = simdb::pipeline::createTask<simdb::pipeline::Function<InstEvents, InstEventsRange>>(
            [](InstEvents&& evts, simdb::ConcurrentQueue<InstEventsRange>& out)
            {
                InstEventUID uid = evts.front().uid;
                for (size_t i = 1; i < evts.size(); ++i)
                {
                    if (evts[i].uid != uid + 1)
                    {
                        throw simdb::DBException("Could not validate event UIDs");
                    }
                    ++uid;
                }

                InstEventsRange range;
                range.start_uid = evts.front().uid;
                range.end_uid = evts.back().uid;
                range.events = std::move(evts);
                out.emplace(std::move(range));
            }
        );

        // This task takes a range of events and performs zlib compression on them
        struct CompressedInstEventsRange
        {
            std::vector<char> all_event_bytes;
            InstEventUID start_uid;
            InstEventUID end_uid;
        };

        auto zlib_task = simdb::pipeline::createTask<simdb::pipeline::Function<InstEventsRange, CompressedInstEventsRange>>(
            [](InstEventsRange&& evts, simdb::ConcurrentQueue<CompressedInstEventsRange>& out)
            {
                std::vector<char> uncompressed;
                for (const auto& evt : evts.events)
                {
                    const auto bytes = evt.toBytes();
                    uncompressed.insert(uncompressed.end(), bytes.begin(), bytes.end());
                }

                CompressedInstEventsRange compressed;
                simdb::compressData(uncompressed, compressed.all_event_bytes);
                compressed.start_uid = evts.start_uid;
                compressed.end_uid = evts.end_uid;

                out.emplace(std::move(compressed));
            }
        );

        // This task writes compressed events to disk and sends out eviction notices
        auto sqlite_task = simdb::pipeline::createTask<simdb::pipeline::DatabaseQueue<CompressedInstEventsRange, InstructionEventUIDRange>>(
            SQL_TABLE("CompressedEvents"),
            SQL_COLUMNS("StartUID", "EndUID", "CompressedEvtBytes"),
            [cache = cache_](CompressedInstEventsRange&& evts, simdb::ConcurrentQueue<InstructionEventUIDRange>& out, simdb::PreparedINSERT* inserter)
            {
                inserter->setColumnValue(0, evts.start_uid);
                inserter->setColumnValue(1, evts.end_uid);
                inserter->setColumnValue(2, evts.all_event_bytes);
                inserter->createRecord();

                InstructionEventUIDRange uid_range = std::make_pair(evts.start_uid, evts.end_uid);
                out.emplace(std::move(uid_range));
            }
        );

        // This task receives eviction notices from the sqlite task and notifies the cache
        auto eviction_task = simdb::pipeline::createTask<simdb::pipeline::Function<InstructionEventUIDRange, void>>(
            [cache = cache_](InstructionEventUIDRange&& uid_range)
            {
                cache->evictFromCache(uid_range);
            }
        );

        // Connect tasks ---------------------------------------------------------------------------
        *new_evt_task >> *buffer_task >> *range_task >> *zlib_task >> *sqlite_task >> *eviction_task;

        // Get the pipeline input (head) -----------------------------------------------------------
        pipeline_head_ = new_evt_task->getTypedInputQueue<InstEvent>();

        // Assign threads (task groups) ------------------------------------------------------------
        // Thread 1:
        pipeline->createTaskGroup("AllOneThread")
            ->addTask(std::move(new_evt_task))
            ->addTask(std::move(buffer_task))
            ->addTask(std::move(range_task))
            ->addTask(std::move(zlib_task))
            ->addTask(std::move(eviction_task));

        // Thread 2:
        pipeline->createTaskGroup("Database")
            ->addTask(std::move(sqlite_task));

        pipeline_ = pipeline.get();
        return pipeline;
    }

    void process(InstEvent&& in)
    {
        if (circ_buf_.full())
        {
            pipeline_head_->emplace(std::move(circ_buf_.pop()));
        }
        circ_buf_.push(std::move(in));
    }

private:
    simdb::CircularBuffer<InstEvent, 10> circ_buf_;
    simdb::ConcurrentQueue<InstEvent>* pipeline_head_ = nullptr;
    simdb::pipeline::Pipeline* pipeline_ = nullptr;
    simdb::DatabaseManager* db_mgr_ = nullptr;
    std::shared_ptr<Cache> cache_;
};

REGISTER_SIMDB_APPLICATION(MultiStageCache);

TEST_INIT;

int main(int argc, char** argv)
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);
    app_mgr.enableApp(MultiStageCache::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.postInit(argc, argv);
    app_mgr.openPipelines();

    // Simulate...
    auto app = app_mgr.getApp<MultiStageCache>();
    InstEventUID next_evt_uid = 1;
    for (size_t i = 1; i <= 10000; ++i)
    {
        app->process(InstEvent::createRandom(next_evt_uid++));
    }

    // Finish...
    app_mgr.postSim();
    app_mgr.teardown();
    app_mgr.destroy();

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
