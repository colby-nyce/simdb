// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/elements/Buffer.hpp"
#include "simdb/pipeline/elements/Function.hpp"
#include "simdb/pipeline/AsyncDatabaseAccessor.hpp"
#include "simdb/utils/CircularBuffer.hpp"
#include "simdb/utils/Compress.hpp"
#include "simdb/utils/RunningMean.hpp"
#include "SimDBTester.hpp"
#include <optional>
#include <limits>

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>

// This test shows how to create a pipeline that implements multi-stage data retrieval.
// Similar to memory hierarchy, each stage of this cache will be slower than the one
// "upstream" of it when recreating data originally sent down the pipeline.
//
// Simulation thread:
//   - Cache incoming data in a mutex-protected std::deque (fastest access)
//   - Send a copy of the incoming data to the processing thread
//
// Processing thread:
//   - Performs buffering and compression on data
//   - Sends compressed data to DB thread
//   - Receives cache eviction notifications from the DB thread, notifies cache
//
// Database thread:
//   - Commits compressed data to the database and sends eviction notification
//   - Fallback for data retrieval if not in the cache (slowest access)
//
//                   ________________________________________________________
//                   |                ________________                      |
//                   |                |              |                      |
//   Simulation -----|--|-- copy ---> |     Cache    |<<========||E         |
//   thread          |  |             |______________|          ||V         |
//                   |  |                                       ||I         |
//  -  -  -  -  -  - |- |-  -  -  -  -  -  -  -  -  -  -  -  -  ||C-  -  -  |  -  -
//                   |  |                                       ||T         |
//   Processing      |  |             ________________   _______||________  |
//   thread          |  |             |              |   |               |  |
//                   |  |-----------> |    Buffer    |   |  EvictionMgr  |  |
//                   |                |______________|   |_______________|  |
//                   |                      ||||                ||          |
//                   |                      \\//                ||N         |
//                   |                _______\/_______          ||O         |
//                   |                |              |          ||T         |
//                   |                |     Zlib     |          ||I         |
//                   |                |______________|          ||F         |
//                   |________________________|_________________||Y_________|
//                                            |                 ||
//  -  -  -  -  -  -  -  -  -  -  -  -  -  -  |  -  -  -  -  -  ||E-  -  -  -  -  -
//                   _________________________|___________      ||V
//   Database        |                ________|_______   |      ||I
//   thread          |                |              |   |      ||C
//                   |                |    SQLite    |---|------||T
//                   |                |______________|   |
//                   |___________________________________|
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

    bool operator==(const RegWrite& other) const
    {
        return reg_type == other.reg_type &&
               reg_num == other.reg_num &&
               prev_val == other.prev_val &&
               curr_val == other.curr_val;
    }

    template <typename Archive>
    void serialize(Archive& ar, const unsigned int /*version*/)
    {
        ar & reg_type;
        ar & reg_num;
        ar & prev_val;
        ar & curr_val;
    }

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

constexpr auto InstEventInvalidUID = std::numeric_limits<uint64_t>::max();
constexpr auto InstEventInvalidHart = std::numeric_limits<uint64_t>::max();
constexpr auto InstEventInvalidOpcode = uint64_t(0);
constexpr auto InstEventInvalidPC = uint64_t(0);

class InstEvent
{
public:
    InstEventUID euid = InstEventInvalidUID;
    uint64_t hart = InstEventInvalidHart;
    uint64_t opcode = InstEventInvalidOpcode;
    uint64_t curr_pc = InstEventInvalidPC;
    uint64_t next_pc = InstEventInvalidPC;
    std::vector<RegWrite> reg_writes;

    bool operator==(const InstEvent& other) const
    {
        if (this == &other)
        {
            return true;
        }

        return euid == other.euid &&
               hart == other.hart &&
               opcode == other.opcode &&
               curr_pc == other.curr_pc &&
               next_pc == other.next_pc &&
               reg_writes == other.reg_writes;
    }

    template <typename Archive>
    void serialize(Archive& ar, const unsigned int /*version*/)
    {
        ar & euid;
        ar & hart;
        ar & opcode;
        ar & curr_pc;
        ar & next_pc;
        ar & reg_writes;
    }

    std::vector<char> toBytes() const
    {
        std::vector<char> bytes;
        boost::iostreams::back_insert_device<std::vector<char>> inserter(bytes);
        boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> os(inserter);
        boost::archive::binary_oarchive oa(os);
        oa << *this;
        os.flush();
        return bytes;
    }

    static InstEvent createRandom(InstEventUID euid)
    {
        InstEvent inst_evt;
        inst_evt.euid = euid;
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

    // Needed for std::lower_bound to work with euid directly
    bool operator<(InstEventUID other_euid) const
    {
        return euid < other_euid;
    }
};

// We are going to send random InstEvents down the pipeline
// and periodically try to access them, either from the cache
// or recreated from disk.
struct TestInstEvent
{
    InstEvent orig_evt;
    uint64_t access_tick;
    bool check_evicted_first = false;
};

// Custom comparator for min-heap (based on the tick when we should access the event).
// Needed for std::priority_queue<TestInstEvent>
struct CompareTestInstEvent
{
    bool operator()(const TestInstEvent& evt1, const TestInstEvent& evt2) const
    {
        return evt1.access_tick > evt2.access_tick;
    }
};

std::ostream& operator<<(std::ostream& os, const InstEvent& evt)
{
    os << "InstEvent(" << evt.euid << ")";
    return os;
}

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
        tbl.addColumn("StartEuid", dt::int64_t);
        tbl.addColumn("EndEuid", dt::int64_t);
        tbl.addColumn("CompressedEvtBytes", dt::blob_t);
        tbl.createCompoundIndexOn({"StartEuid", "EndEuid"});

        return true;
    }

    std::unique_ptr<simdb::pipeline::Pipeline> createPipeline(simdb::pipeline::AsyncDatabaseAccessor* db_accessor) override
    {
        auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

        // Recall the pipeline we want to build:
        //                   ________________________________________________________
        //                   |                ________________                      |
        //                   |                |              |                      |
        //   Simulation -----|--|-- copy ---> |     Cache    |<<========||E         |
        //   thread          |  |             |______________|          ||V         |
        //                   |  |                                       ||I         |
        //  -  -  -  -  -  - |- |-  -  -  -  -  -  -  -  -  -  -  -  -  ||C-  -  -  |  -  -
        //                   |  |                                       ||T         |
        //   Processing      |  |             ________________   _______||________  |
        //   thread          |  |             |              |   |               |  |
        //                   |  |-----------> |    Buffer    |   |  EvictionMgr  |  |
        //                   |                |______________|   |_______________|  |
        //                   |                      ||||                ||          |
        //                   |                      \\//                ||N         |
        //                   |                _______\/_______          ||O         |
        //                   |                |              |          ||T         |
        //                   |                |     Zlib     |          ||I         |
        //                   |                |______________|          ||F         |
        //                   |________________________|_________________||Y_________|
        //                                            |                 ||
        //  -  -  -  -  -  -  -  -  -  -  -  -  -  -  |  -  -  -  -  -  ||E-  -  -  -  -  -
        //                   _________________________|___________      ||V
        //   Database        |                ________|_______   |      ||I
        //   thread          |                |              |   |      ||C
        //                   |                |    SQLite    |---|------||T
        //                   |                |______________|   |
        //                   |___________________________________|
        //

        // This task reads InstEvents out of the cache and sends them down the pipeline
        auto source_task = simdb::pipeline::createTask<simdb::pipeline::Function<void, InstEvent>>(
            [this, send_evt = InstEvent()](simdb::ConcurrentQueue<InstEvent>& out) mutable -> bool
            {
                if (pipeline_input_queue_.try_pop(send_evt))
                {
                    out.emplace(std::move(send_evt));
                    return true;
                }
                return false;
            }
        );

        // This task buffers 100 events for better compression
        auto buffer_task = simdb::pipeline::createTask<simdb::pipeline::Buffer<InstEvent>>(100);

        // This task takes buffered events and preps them for efficient DB insertion
        auto range_task = simdb::pipeline::createTask<simdb::pipeline::Function<InstEvents, InstEventsRange>>(
            [](InstEvents&& evts, simdb::ConcurrentQueue<InstEventsRange>& out)
            {
                InstEventUID euid = evts.front().euid;
                for (size_t i = 1; i < evts.size(); ++i)
                {
                    if (evts[i].euid != euid + 1)
                    {
                        throw simdb::DBException("Could not validate event UIDs");
                    }
                    ++euid;
                }

                InstEventsRange range;
                range.euid_range = std::make_pair(evts.front().euid, evts.back().euid);
                range.events = std::move(evts);
                out.emplace(std::move(range));
            }
        );

        // This task takes a range of events and serializes them to std::vector<char> buffers (keeping the euid range)
        auto serialize_task = simdb::pipeline::createTask<simdb::pipeline::Function<InstEventsRange, EventsRangeAsBytes>>(
            [](InstEventsRange&& evts, simdb::ConcurrentQueue<EventsRangeAsBytes>& out)
            {
                EventsRangeAsBytes range_as_bytes;
                range_as_bytes.euid_range = evts.euid_range;
                std::vector<char>& buffer = range_as_bytes.all_event_bytes;

                boost::iostreams::back_insert_device<std::vector<char>> inserter(buffer);
                boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> os(inserter);
                boost::archive::binary_oarchive oa(os);
                oa << evts;
                os.flush();

                out.emplace(std::move(range_as_bytes));
            }
        );

        // Perform zlib compression on the event ranges
        auto zlib_task = simdb::pipeline::createTask<simdb::pipeline::Function<EventsRangeAsBytes, EventsRangeAsBytes>>(
            [](EventsRangeAsBytes&& uncompressed, simdb::ConcurrentQueue<EventsRangeAsBytes>& out)
            {
                EventsRangeAsBytes compressed;
                compressed.euid_range = uncompressed.euid_range;
                simdb::compressData(uncompressed.all_event_bytes, compressed.all_event_bytes);
                out.emplace(std::move(compressed));
            }
        );

        // This task receives EventsRangeAsBytes from the zlib task on one thread and writes them to disk on the DB thread
        auto async_writer = db_accessor->createAsyncWriter<EventsRangeAsBytes, InstructionEventUIDRange>(
            SQL_TABLE("CompressedEvents"),
            SQL_COLUMNS("StartEuid", "EndEuid", "CompressedEvtBytes"),
            [](EventsRangeAsBytes&& evts, simdb::ConcurrentQueue<InstructionEventUIDRange>& out, simdb::PreparedINSERT* inserter)
            {
                inserter->setColumnValue(0, evts.euid_range.first);
                inserter->setColumnValue(1, evts.euid_range.second);
                inserter->setColumnValue(2, evts.all_event_bytes);
                inserter->createRecord();

                // Send this euid range for eviction from the cache
                out.emplace(std::move(evts.euid_range));
            }
        );

        // This task receives eviction notices from the sqlite task and notifies the cache
        auto eviction_task = simdb::pipeline::createTask<simdb::pipeline::Function<InstructionEventUIDRange, void>>(
            [this](InstructionEventUIDRange&& euid_range)
            {
                std::lock_guard<std::mutex> lock(mutex_);

                const auto start_euid = euid_range.first;
                const auto end_euid = euid_range.second;

                // Find first event with euid >= start_euid
                auto first = std::lower_bound(evt_cache_.begin(), evt_cache_.end(), start_euid);

                // Find first event with euid > end_euid (i.e. strictly after range)
                auto last = std::upper_bound(evt_cache_.begin(), evt_cache_.end(), end_euid,
                    [](InstEventUID euid, const InstEvent& evt)
                    {
                        return euid < evt.euid;
                    });

                // Erase range [first, last)
                evt_cache_.erase(first, last);
            }
        );

        // Connect tasks ---------------------------------------------------------------------------
        *source_task >> *buffer_task >> *range_task >> *serialize_task >> *zlib_task >> *async_writer >> *eviction_task;

        // Assign threads (task groups) ------------------------------------------------------------
        pipeline->createTaskGroup("PreProcessing")
            ->addTask(std::move(source_task))
            ->addTask(std::move(buffer_task))
            ->addTask(std::move(range_task))
            ->addTask(std::move(serialize_task))
            ->addTask(std::move(zlib_task))
            ->addTask(std::move(eviction_task));

        async_db_accessor_ = db_accessor;
        return pipeline;
    }

    void process(InstEvent&& evt)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            evt_cache_.emplace_back(evt);
        }

        pipeline_input_queue_.emplace(std::move(evt));
    }

    bool isInCache(InstEventUID euid)
    {
        auto evt = getCachedEvent_(euid);
        return evt.has_value();
    }

    InstEvent getEvent(InstEventUID euid)
    {
        auto evt = getCachedEvent_(euid);
        if (evt.has_value())
        {
            ++num_evts_retrieved_from_cache_;
            return *evt;
        }

        evt = recreateEvent_(euid);
        if (evt.has_value())
        {
            ++num_evts_retrieved_from_disk_;
            return *evt;
        }

        throw simdb::DBException("Could not get event with uid ") << euid;
    }

    void postTeardown() override
    {
        std::cout << "Event accesses:\n";
        std::cout << "    From cache: " << num_evts_retrieved_from_cache_ << "\n";

        if (num_evts_retrieved_from_disk_)
        {
            auto avg_latency_us = avg_microseconds_recreating_evts_.mean();
            std::cout << "    From disk:  " << num_evts_retrieved_from_disk_;
            std::cout << " (avg latency " << size_t(avg_latency_us) << " microseconds)\n\n";
        }
        else
        {
            std::cout << "    From disk:  0\n\n";
        }
    }

private:
    // Get a copy of an InstEvent from the cache
    std::optional<InstEvent> getCachedEvent_(InstEventUID euid)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!evt_cache_.empty() && euid >= evt_cache_.front().euid)
        {
            auto index = euid - evt_cache_.front().euid;
            if (index < evt_cache_.size())
            {
                return evt_cache_[index];
            }
        }

        return std::optional<InstEvent>();
    }

    // If not in the cache, recreate an InstEvent from disk
    std::optional<InstEvent> recreateEvent_(InstEventUID euid)
    {
        EventsRangeAsBytes compressed_evts_range;

        auto query_func = [&](simdb::DatabaseManager* db_mgr)
        {
            auto query = db_mgr->createQuery("CompressedEvents");
            query->addConstraintForInt("StartEuid", simdb::Constraints::LESS_EQUAL, euid);
            query->addConstraintForInt("EndEuid", simdb::Constraints::GREATER_EQUAL, euid);

            int64_t start, end;
            query->select("StartEuid", start);
            query->select("EndEuid", end);
            query->select("CompressedEvtBytes", compressed_evts_range.all_event_bytes);

            auto result_set = query->getResultSet();
            EXPECT_TRUE(result_set.getNextRecord());

            compressed_evts_range.euid_range = std::make_pair(start, end);
        };

        // Keep track of how long we spend waiting for a response
        auto start = std::chrono::high_resolution_clock::now();

        async_db_accessor_->eval(query_func);
        EXPECT_TRUE(!compressed_evts_range.all_event_bytes.empty());

        if (compressed_evts_range.all_event_bytes.empty())
        {
            return std::optional<InstEvent>();
        }

        EventsRangeAsBytes evts_range_as_bytes;
        simdb::decompressData(compressed_evts_range.all_event_bytes, evts_range_as_bytes.all_event_bytes);

        namespace bio = boost::iostreams;
        auto& bytes = evts_range_as_bytes.all_event_bytes;
        bio::array_source src(bytes.data(), bytes.size());
        bio::stream<bio::array_source> is(src);

        boost::archive::binary_iarchive ia(is);
        InstEventsRange evts_range;
        ia >> evts_range;

        if (euid >= evts_range.euid_range.first)
        {
            auto index = euid - evts_range.euid_range.first;
            if (index < evts_range.events.size())
            {
                // Keep track of how long we spend waiting for a response
                auto end = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double> dur = end - start;
                auto us = std::chrono::duration_cast<std::chrono::microseconds>(dur).count();
                avg_microseconds_recreating_evts_.add(us);

                return evts_range.events[index];
            }
        }

        return std::optional<InstEvent>();
    }

    using InstEvents = std::vector<InstEvent>;

    struct InstEventsRange
    {
        InstEvents events;
        InstructionEventUIDRange euid_range;

        template <typename Archive>
        void serialize(Archive& ar, const unsigned int /*version*/)
        {
            ar & events;
            ar & euid_range;
        }
    };

    struct EventsRangeAsBytes
    {
        std::vector<char> all_event_bytes;
        InstructionEventUIDRange euid_range;
    };

    simdb::DatabaseManager* db_mgr_ = nullptr;
    simdb::pipeline::AsyncDatabaseAccessor* async_db_accessor_ = nullptr;
    std::deque<InstEvent> evt_cache_;
    simdb::ConcurrentQueue<InstEvent> pipeline_input_queue_;
    mutable std::mutex mutex_;
    size_t num_evts_retrieved_from_cache_ = 0;
    size_t num_evts_retrieved_from_disk_ = 0;
    simdb::RunningMean avg_microseconds_recreating_evts_;
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
    InstEventUID next_evt_euid = 1;

    std::priority_queue<TestInstEvent, std::vector<TestInstEvent>, CompareTestInstEvent> evt_verif_queue;

    auto verify_top = [&evt_verif_queue, app]()
    {
        const auto& test_evt = evt_verif_queue.top();
        const auto test_evt_euid = test_evt.orig_evt.euid;

        // Ensure we recreate events from disk despite this "sim loop"
        // running so fast that events are usually still in the cache.
        if (test_evt.check_evicted_first)
        {
            while (app->isInCache(test_evt_euid))
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }

        const auto accessed_evt = app->getEvent(test_evt_euid);
        EXPECT_EQUAL(accessed_evt, test_evt.orig_evt);
        evt_verif_queue.pop();
    };

    constexpr uint64_t TICKS = 10000;
    for (uint64_t tick = 1; tick <= TICKS; ++tick)
    {
        auto evt = InstEvent::createRandom(next_evt_euid++);

        // Let's add an event for future retrieval every 10 sent events
        if (tick % 10 == 0)
        {
            TestInstEvent test_evt;
            test_evt.orig_evt = evt;

            // Most of the time we will access an event very soon after sending it.
            // These will very likely still be in the cache.
            if (rand() % 10)
            {
                test_evt.access_tick = tick + (rand() % 3 + 1);
                test_evt.access_tick = std::min(test_evt.access_tick, TICKS);
            }

            // Sometimes we will ask for events that are so old they are no
            // longer cached.
            else
            {
                test_evt.access_tick = tick + 1000 + rand() % 5;
                test_evt.access_tick = std::min(test_evt.access_tick, TICKS);
                test_evt.check_evicted_first = true;
            }

            evt_verif_queue.emplace(std::move(test_evt));
        }

        // See if we should verify a previous event at this time
        if (!evt_verif_queue.empty() && evt_verif_queue.top().access_tick == tick)
        {
            verify_top();
        }

        // Send down the pipeline
        app->process(std::move(evt));
    }

    while (!evt_verif_queue.empty())
    {
        verify_top();
    }

    // Finish...
    app_mgr.postSimLoopTeardown();

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
