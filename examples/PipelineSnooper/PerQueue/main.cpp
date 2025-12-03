// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/elements/Function.hpp"
#include "simdb/pipeline/elements/DatabaseTask.hpp"
#include "simdb/utils/Compress.hpp"
#include "simdb/utils/TickTock.hpp"
#include "SimDBTester.hpp"
#include <unordered_set>

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>

// This test shows how to create a pipeline that has "snoopers" assigned
// to each task. Snoopers allow us to peek into every inter-task queue
// in the pipeline looking for specific item(s).
//
// This test shows how to snoop on a per-queue basis, meaning that we
// look at all items in the queue at once. The per-queue snoopers
// work on non-const-qualified items, so they could be used to remove
// entire ranges of items from the pipeline if desired.

struct DummyData
{
    std::string uuid;
    std::vector<double> dbl_values;
    std::vector<std::string> str_values;

    static std::string generateUUID()
    {
        std::string uuid;
        for (size_t i = 0; i < 32; ++i)
        {
            uuid += "0123456789abcdef"[rand() % 16];
            if (i % 4 == 3 && i != 31)
            {
                uuid += "-";
            }
        }

        static std::unordered_set<std::string> existing_uuids;
        EXPECT_TRUE(existing_uuids.insert(uuid).second); // Ensure uniqueness
        return uuid;
    }

    static DummyData createRandom()
    {
        DummyData data;
        data.uuid = generateUUID();
        for (auto i = 0; i < 10 + rand() % 1000; ++i)
        {
            data.dbl_values.push_back(static_cast<double>(rand()));
        }
        for (auto i = 0; i < 10 + rand() % 100; ++i)
        {
            data.str_values.push_back(generateUUID());
        }
        return data;
    }

    // Support boost::serialization
    template <typename Archive>
    void serialize(Archive& ar, const unsigned int /*version*/)
    {
        ar & uuid;
        ar & dbl_values;
        ar & str_values;
    }
};

struct DummyDataBytes
{
    std::string uuid;
    std::vector<char> bytes;
    bool compressed = false;

    void compress()
    {
        if (!compressed)
        {
            std::vector<char> compressed_bytes;
            simdb::compressData(bytes, compressed_bytes);
            std::swap(bytes, compressed_bytes);
            compressed = true;
        }
    }

    void decompress()
    {
        if (compressed)
        {
            std::vector<char> decompressed_bytes;
            simdb::decompressData(bytes, decompressed_bytes);
            std::swap(bytes, decompressed_bytes);
            compressed = false;
        }
    }
};

class PipelineSnooper : public simdb::App
{
public:
    static constexpr auto NAME = "pipeline-snooper";

    PipelineSnooper(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    static void defineSchema(simdb::Schema& schema)
    {
        using dt = simdb::SqlDataType;

        auto& tbl = schema.addTable("SimDataBlobs");
        tbl.addColumn("UUID", dt::string_t);
        tbl.addColumn("DataBlob", dt::blob_t);
        tbl.createIndexOn("UUID");
    }

    void createPipeline(simdb::pipeline::PipelineManager* pipeline_mgr) override
    {
        pipeline_mgr_ = pipeline_mgr;
        auto pipeline = pipeline_mgr->createPipeline(NAME);
        auto db_accessor = pipeline_mgr->getAsyncDatabaseAccessor();

        // Task 1: Run the DummyData structs through boost::serialization
        auto serialize_task = simdb::pipeline::createTask<simdb::pipeline::Function<DummyData, DummyDataBytes>>(
            [this](DummyData&& data,
                   simdb::ConcurrentQueue<DummyDataBytes>& out,
                   bool /*force*/)
            {
                DummyDataBytes data_bytes;
                data_bytes.uuid = data.uuid;

                boost::iostreams::back_insert_device<std::vector<char>> inserter(data_bytes.bytes);
                boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> os(inserter);
                boost::archive::binary_oarchive oa(os);
                oa << data;
                os.flush();

                out.emplace(std::move(data_bytes));
                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        // Task 2: Compress the serialized data
        auto zlib_task = simdb::pipeline::createTask<simdb::pipeline::Function<DummyDataBytes, DummyDataBytes>>(
            [this](DummyDataBytes&& data,
                   simdb::ConcurrentQueue<DummyDataBytes>& out,
                   bool /*force*/)
            {
                data.compress();
                out.emplace(std::move(data));
                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        // Task 3: Write the compressed data to the database
        using WriteTask = simdb::pipeline::DatabaseTask<DummyDataBytes, std::vector<std::string>>;
        auto db_writer = simdb::pipeline::createTask<WriteTask>(
            db_mgr_,
            [this](DummyDataBytes&& data,
                   simdb::ConcurrentQueue<std::vector<std::string>>&,
                   simdb::pipeline::DatabaseAccessor& accessor,
                   bool /*force*/)
            {
                auto inserter = accessor.getTableInserter<PipelineSnooper>("SimDataBlobs");
                inserter->setColumnValue(0, data.uuid);
                inserter->setColumnValue(1, data.bytes);
                inserter->createRecord();
                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        // Task 4: Delete any remaining UUIDs that could not be snooped and deleted from the pipeline
        using ReadTask = simdb::pipeline::DatabaseTask<std::vector<std::string>, void>;
        auto uuid_deleter = simdb::pipeline::createTask<ReadTask>(
            db_mgr_,
            [](std::vector<std::string>&& uuids,
               simdb::pipeline::DatabaseAccessor& accessor,
               bool /*force*/)
            {
                auto db_mgr = accessor.getDatabaseManager();
                auto query = db_mgr->createQuery("SimDataBlobs");
                query->addConstraintForString("UUID", simdb::SetConstraints::IN_SET, uuids);
                query->deleteResultSet();
                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        // Connect tasks ---------------------------------------------------------------------------
        *serialize_task >> *zlib_task >> *db_writer >> *uuid_deleter;

        // Create the flusher -----------------------------------------------------------------------
        pipeline_flusher_ = std::make_unique<simdb::pipeline::RunnableFlusher>(
            *db_mgr_, serialize_task, zlib_task, db_writer, uuid_deleter);

        // Assign snoopers to each task's input queue ----------------------------------------------
        pipeline_flusher_->assignQueueSnooper<DummyData>(
            *serialize_task,
            [this](std::deque<DummyData>& queue) -> simdb::SnooperCallbackOutcome
            {
                return removeFromQueue_(queue, snooped_in_task1_);
            }
        );

        pipeline_flusher_->assignQueueSnooper<DummyDataBytes>(
            *zlib_task,
            [this](std::deque<DummyDataBytes>& queue) -> simdb::SnooperCallbackOutcome
            {
                return removeFromQueue_(queue, snooped_in_task2_);
            }
        );

        pipeline_flusher_->assignQueueSnooper<DummyDataBytes>(
            *db_writer,
            [this](std::deque<DummyDataBytes>& queue) -> simdb::SnooperCallbackOutcome
            {
                return removeFromQueue_(queue, snooped_in_task3_);
            }
        );

        // Store the head of the pipeline for sending data -----------------------------------------
        pipeline_head_ = serialize_task->getTypedInputQueue<DummyData>();

        // Store the tail of the pipeline for sending UUIDs to delete ------------------------------
        pipeline_tail_ = uuid_deleter->getTypedInputQueue<std::vector<std::string>>();

        // Assign threads (task groups) ------------------------------------------------------------
        pipeline->createTaskGroup("Processing")
            ->addTask(std::move(serialize_task))
            ->addTask(std::move(zlib_task));

        db_accessor->addTasks(
            std::move(db_writer),
            std::move(uuid_deleter)
        );
    }

    void sendOne(DummyData&& data)
    {
        pipeline_head_->emplace(std::move(data));
    }

    void deleteFromPipeline(const std::vector<std::string>& uuids)
    {
        PROFILE_METHOD

        // Use RAII to disable the entire pipeline. There are edge cases where
        // the pipeline might look like this:
        //
        //    Task1 input queue:       3 items
        //    Task1 evaluating lambda: 1 item
        //    Task2 input queue:       3 items
        //
        // Without pausing the pipeline and waiting for the std::promise to be
        // fulfilled, we might miss the items we are looking for if they are currently
        // being processed by a task. The snooper can only look into the task input
        // queues.
        {
            snooping_for_uuids_ = std::unordered_set<std::string>(uuids.begin(), uuids.end());
            auto disabler = pipeline_mgr_->scopedDisableAll(false);
            auto outcome = pipeline_flusher_->snoopAll();
            if (outcome.found())
            {
                EXPECT_TRUE(snooping_for_uuids_.empty());
                return;
            }
        }

        EXPECT_FALSE(snooping_for_uuids_.empty());

        // Since we couldn't flush every UUID from the pipeline queues, delete the remainders
        // from the database directly. Don't do this in the main thread however; simply forward
        // these remaining UUIDs to the last task in the pipeline which will delete them.
        const std::vector<std::string> remaining_uuids(snooping_for_uuids_.begin(), snooping_for_uuids_.end());
        snooping_for_uuids_.clear();
        pipeline_tail_->emplace(std::move(remaining_uuids));
    }

    void postTeardown() override
    {
        EXPECT_TRUE(snooped_in_task1_ > 0 || snooped_in_task2_ > 0 || snooped_in_task3_ > 0);

        std::cout << "Snooper stats:\n";
        std::cout << "    Snooped in Task 1: " << snooped_in_task1_ << "\n";
        std::cout << "    Snooped in Task 2: " << snooped_in_task2_ << "\n";
        std::cout << "    Snooped in Task 3: " << snooped_in_task3_ << "\n\n";
    }

private:
    template <typename T>
    simdb::SnooperCallbackOutcome removeFromQueue_(std::deque<T>& queue, uint32_t& snooped_in_task)
    {
        auto it = std::remove_if(queue.begin(), queue.end(),
            [this, &snooped_in_task](const T& item)
            {
                if (snooping_for_uuids_.find(item.uuid) != snooping_for_uuids_.end())
                {
                    ++snooped_in_task;
                    snooping_for_uuids_.erase(item.uuid);
                    return true; // Remove this item from the queue
                }
                return false; // Keep this item in the queue
            });

        queue.erase(it, queue.end());
        if (snooping_for_uuids_.empty())
        {
            return simdb::SnooperCallbackOutcome::FOUND_STOP;
        }
        return simdb::SnooperCallbackOutcome::NOT_FOUND_CONTINUE;
    }

    simdb::DatabaseManager* db_mgr_ = nullptr;
    simdb::pipeline::PipelineManager* pipeline_mgr_ = nullptr;
    std::unique_ptr<simdb::pipeline::RunnableFlusher> pipeline_flusher_;
    simdb::ConcurrentQueue<DummyData>* pipeline_head_ = nullptr;
    simdb::ConcurrentQueue<std::vector<std::string>>* pipeline_tail_ = nullptr;
    std::unordered_set<std::string> snooping_for_uuids_;
    uint32_t snooped_in_task1_ = 0;
    uint32_t snooped_in_task2_ = 0;
    uint32_t snooped_in_task3_ = 0;
};

REGISTER_SIMDB_APPLICATION(PipelineSnooper);

TEST_INIT;

int main()
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);
    app_mgr.enableApp(PipelineSnooper::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.openPipelines();

    // Simulate...
    auto app = app_mgr.getApp<PipelineSnooper>();
    std::vector<std::string> uuids_to_delete;
    std::vector<std::string> expect_deleted_uuids;

    for (uint64_t tick = 1; tick < 10000; ++tick)
    {
        auto data = DummyData::createRandom();
        if (rand() % 50 == 0)
        {
            uuids_to_delete.push_back(data.uuid);
        }

        app->sendOne(std::move(data));

        if (uuids_to_delete.size() == 10)
        {
            app->deleteFromPipeline(uuids_to_delete);
            expect_deleted_uuids.insert(expect_deleted_uuids.end(), uuids_to_delete.begin(), uuids_to_delete.end());
            uuids_to_delete.clear();
        }
    }

    if (!uuids_to_delete.empty())
    {
        app->deleteFromPipeline(uuids_to_delete);
        expect_deleted_uuids.insert(expect_deleted_uuids.end(), uuids_to_delete.begin(), uuids_to_delete.end());
        uuids_to_delete.clear();
    }

    // Finish...
    app_mgr.postSimLoopTeardown();

    // Verify that all expected UUIDs were deleted
    auto query = db_mgr.createQuery("SimDataBlobs");
    query->addConstraintForString("UUID", simdb::SetConstraints::IN_SET, expect_deleted_uuids);
    EXPECT_EQUAL(query->count(), 0);

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
