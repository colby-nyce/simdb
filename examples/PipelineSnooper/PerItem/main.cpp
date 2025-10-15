// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/elements/Function.hpp"
#include "simdb/utils/Compress.hpp"
#include "simdb/utils/TickTock.hpp"
#include "SimDBTester.hpp"
#include <queue>

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
// This test shows how to snoop on a per-item basis, meaning that we
// look at each item in the queue one at a time. The per-item snoopers
// work on const-qualified items, so they typically would be used to
// extract copies of items already in the pipeline.

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

    std::unique_ptr<simdb::pipeline::Pipeline> createPipeline(
        simdb::pipeline::AsyncDatabaseAccessor* db_accessor) override
    {
        auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

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
        auto db_writer = db_accessor->createAsyncWriter<PipelineSnooper, DummyDataBytes, void>(
            [this](DummyDataBytes&& data,
                   simdb::pipeline::AppPreparedINSERTs* tables,
                   bool /*force*/)
            {
                auto inserter = tables->getPreparedINSERT("SimDataBlobs");
                inserter->setColumnValue(0, data.uuid);
                inserter->setColumnValue(1, data.bytes);
                inserter->createRecord();
                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        // Connect tasks ---------------------------------------------------------------------------
        *serialize_task >> *zlib_task >> *db_writer;

        // Create the flusher -----------------------------------------------------------------------
        pipeline_flusher_ = std::make_unique<simdb::pipeline::RunnableFlusher>(
            *db_mgr_, serialize_task, zlib_task, db_writer);

        pipeline_flusher_->assignQueueItemSnooper<DummyData>(
            *serialize_task,
            [this](const DummyData& item) -> simdb::SnooperCallbackOutcome
            {
                assert(!snooping_for_uuid_.empty());

                // If the uuid matches, take a copy of the DummyData.
                if (item.uuid == snooping_for_uuid_)
                {
                    snooped_data_ = item;
                    ++snooped_in_task1_;
                    return simdb::SnooperCallbackOutcome::FOUND_STOP;
                }
                return simdb::SnooperCallbackOutcome::NOT_FOUND_CONTINUE;
            }
        );

        pipeline_flusher_->assignQueueItemSnooper<DummyDataBytes>(
            *zlib_task,
            [this](const DummyDataBytes& item) -> simdb::SnooperCallbackOutcome
            {
                assert(!snooping_for_uuid_.empty());

                // If the uuid matches, deserialize the bytes back into a DummyData struct
                if (item.uuid == snooping_for_uuid_)
                {
                    boost::iostreams::array_source src(item.bytes.data(), item.bytes.size());
                    boost::iostreams::stream<boost::iostreams::array_source> is(src);
                    boost::archive::binary_iarchive ia(is);
                    ia >> snooped_data_;

                    ++snooped_in_task2_;
                    return simdb::SnooperCallbackOutcome::FOUND_STOP;
                }
                return simdb::SnooperCallbackOutcome::NOT_FOUND_CONTINUE;
            }
        );

        pipeline_flusher_->assignQueueItemSnooper<DummyDataBytes>(
            *db_writer,
            [this](const DummyDataBytes& item) -> simdb::SnooperCallbackOutcome
            {
                assert(!snooping_for_uuid_.empty());

                // If the uuid matches, decompress and deserialize the bytes back into a DummyData struct
                if (item.uuid == snooping_for_uuid_)
                {
                    DummyDataBytes decompressed;
                    decompressed.uuid = item.uuid;
                    simdb::decompressData(item.bytes, decompressed.bytes);

                    boost::iostreams::array_source src(decompressed.bytes.data(), decompressed.bytes.size());
                    boost::iostreams::stream<boost::iostreams::array_source> is(src);
                    boost::archive::binary_iarchive ia(is);
                    ia >> snooped_data_;

                    ++snooped_in_task3_;
                    return simdb::SnooperCallbackOutcome::FOUND_STOP;
                }
                return simdb::SnooperCallbackOutcome::NOT_FOUND_CONTINUE;
            }
        );

        // Store the head of the pipeline for sending data -----------------------------------------
        pipeline_head_ = serialize_task->getTypedInputQueue<DummyData>();

        // Assign threads (task groups) ------------------------------------------------------------
        pipeline->createTaskGroup("Processing")
            ->addTask(std::move(serialize_task))
            ->addTask(std::move(zlib_task));

        return pipeline;
    }

    void sendOne(DummyData&& data)
    {
        pipeline_head_->emplace(std::move(data));
    }

    DummyData snoopPipeline(const std::string& uuid)
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
        // fulfilled, we might miss the item we are looking for if it is currently
        // being processed by a task. The snooper can only look into the task input
        // queues.
        {
            // We don't have to take the extra overhead to asynchronously pause
            // the polling threads. The reason for doing that is if we 100% need
            // to be sure that we do not miss any items being processed by a task.
            // In those edge cases, we will fall back to flushing the whole pipeline
            // and querying the database. The overall performance is faster for this
            // use case if we only disable the runnables and not the polling threads.
            //
            // To contrast, the use case for pausing the polling threads is if you
            // want to flush specific items from the pipeline and you want to ensure
            // that they do not "magically appear" anyway in the database because
            // the item that was to be removed was missed by the snooper while it
            // was being processed by a task.
            constexpr bool disable_threads_too = false;
            auto disabler = pipeline_flusher_->scopedDisableAll(disable_threads_too);

            snooping_for_uuid_ = uuid;
            auto outcome = pipeline_flusher_->snoopAll();

            if (outcome.found())
            {
                return snooped_data_;
            }
        }

        // Since we couldn't get the DummyData from snooping, flush the whole pipeline
        // and just query the database. This is the fallback, and quite a bit slower
        // for deep pipelines.
        pipeline_flusher_->waterfallFlush();

        DummyData db_dummy_data;

        // SELECT DataBlob FROM SimDataBlobs WHERE UUID = <uuid>
        auto query = db_mgr_->createQuery("SimDataBlobs");
        query->addConstraintForString("UUID", simdb::Constraints::EQUAL, uuid);

        DummyDataBytes dummy_data_bytes;
        dummy_data_bytes.compressed = true; // we know it's compressed in the DB
        query->select("DataBlob", dummy_data_bytes.bytes);

        auto results = query->getResultSet();
        if (!results.getNextRecord())
        {
            throw simdb::DBException("Could not find data with uuid ") << uuid;
        }

        // Decompress
        dummy_data_bytes.decompress();

        // Deserialize the blob back into a DummyData struct
        boost::iostreams::array_source src(dummy_data_bytes.bytes.data(), dummy_data_bytes.bytes.size());
        boost::iostreams::stream<boost::iostreams::array_source> is(src);
        boost::archive::binary_iarchive ia(is);
        ia >> db_dummy_data;

        // Sanity check
        if (db_dummy_data.uuid != uuid)
        {
            throw simdb::DBException("Database returned data with incorrect uuid");
        }

        return db_dummy_data;
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
    simdb::DatabaseManager* db_mgr_ = nullptr;
    std::unique_ptr<simdb::pipeline::RunnableFlusher> pipeline_flusher_;
    simdb::ConcurrentQueue<DummyData>* pipeline_head_ = nullptr;
    std::string snooping_for_uuid_;
    DummyData snooped_data_;
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

    std::vector<DummyData> verif_data;

    auto flush_verif_queue = [&]()
    {
        std::shuffle(verif_data.begin(), verif_data.end(), std::mt19937{std::random_device{}()});
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        for (const auto& expected : verif_data)
        {
            auto actual = app->snoopPipeline(expected.uuid);
            EXPECT_EQUAL(actual.uuid, expected.uuid);
            EXPECT_EQUAL(actual.dbl_values, expected.dbl_values);
            EXPECT_EQUAL(actual.str_values, expected.str_values);
        }
        verif_data.clear();
    };

    for (uint64_t tick = 1; tick < 10000; ++tick)
    {
        auto data = DummyData::createRandom();
        verif_data.push_back(data);
        app->sendOne(std::move(data));

        if (rand() % 500 == 0)
        {
            flush_verif_queue();
        }
    }

    flush_verif_queue();

    // Finish...
    app_mgr.postSimLoopTeardown();

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
