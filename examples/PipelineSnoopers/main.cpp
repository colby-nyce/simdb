// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/PipelineSnooper.hpp"
#include "simdb/utils/Compress.hpp"
#include "SimDBTester.hpp"

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>

/// This example demonstrates how to "snoop" a pipeline stages'
/// input queues looking for a specific item. It can be a lot
/// faster to look through all stage queues instead of flushing
/// the whole pipeline and querying the database for what you
/// are looking for.

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

struct DummyDataWindowBytes
{
    std::vector<std::string> uuids;
    std::vector<char> bytes;
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

        // Since multiple DummyData packets will exist in a given blob record,
        // we use a separate table to hold the UUIDs that hold foreign keys
        // referenced in the blob table.
        auto& uuids_tbl = schema.addTable("DummyDataUUIDs");
        uuids_tbl.addColumn("UUID", dt::string_t);
        uuids_tbl.addColumn("DummyDataBlobID", dt::int32_t);
        uuids_tbl.createIndexOn("UUID");

        auto& blobs_tbl = schema.addTable("DummyDataBlobs");
        blobs_tbl.addColumn("DataBlob", dt::blob_t);
    }

    void createPipeline(simdb::pipeline::PipelineManager* pipeline_mgr) override
    {
        auto pipeline = pipeline_mgr->createPipeline(NAME, this);

        auto buffer = pipeline->addStage<BufferStage>("buffer", snooped_in_buffer_queue_, snooped_in_buffer_stage_);
        auto boost_serializer = pipeline->addStage<BoostStage>("boost_serializer", snooped_in_boost_queue_);
        auto zlib_compressor = pipeline->addStage<ZlibStage>("zlib_compressor", snooped_in_zlib_queue_);
        auto database_writer = pipeline->addStage<DatabaseStage>("database_writer", snooped_in_db_queue_, snooped_in_database_);
        pipeline->noMoreStages();

        pipeline->bind("buffer.output_buffer", "boost_serializer.input_buffer");
        pipeline->bind("boost_serializer.output_bytes", "zlib_compressor.input_bytes");
        pipeline->bind("zlib_compressor.output_bytes", "database_writer.input_bytes");
        pipeline->noMoreBindings();

        pipeline_head_ = pipeline->getInPortQueue<DummyData>("buffer.input_data");
        pipeline_mgr_ = pipeline_mgr;

        // Note that createFlusher() takes a vector of stage names. The stages get flushed
        // in the order their names appear in this vector. If we do not pass in the vector,
        // then the flusher will be given the stages in the same order as the calls to the
        // addStage() method above.
        pipeline_flusher_ = pipeline->createFlusher();

        // Create the pipeline snooper. All our stages have a method with the same signature
        // to support retrieving a DummyData packet from a std::string uuid:
        //
        //     // Returns true if DummyData is valid (found/snooped successfully).
        //     bool snoop(const std::string& uuid, DummyData& snooped);
        pipeline_snooper_ = std::make_unique<simdb::pipeline::PipelineSnooper<std::string, DummyData>>();
        pipeline_snooper_->addStage(buffer);
        pipeline_snooper_->addStage(boost_serializer);
        pipeline_snooper_->addStage(zlib_compressor);
        pipeline_snooper_->addStage(database_writer);
    }

    void sendOne(DummyData&& data)
    {
        pipeline_head_->emplace(std::move(data));
    }

    bool snoopPipeline(const std::string& uuid, DummyData& snooped_data)
    {
        // It is typically a very good idea to disable the pipeline while
        // we snoop it looking for the DummyData with this uuid. This will
        // increase the chances that we find the DummyData at an earlier
        // stage, where we have fewer things to "undo" to get back the
        // original DummyData (like undoing zlib and/or boost serialization).
        auto disabler = pipeline_mgr_->scopedDisableAll();

        return pipeline_snooper_->snoopAllStages(uuid, snooped_data);
    }

    void postTeardown() override
    {
        EXPECT_TRUE(snooped_in_buffer_queue_ > 0 ||
                    snooped_in_buffer_stage_ > 0 ||
                    snooped_in_boost_queue_ > 0  ||
                    snooped_in_zlib_queue_ > 0   ||
                    snooped_in_db_queue_ > 0     ||
                    snooped_in_database_ > 0);

        std::cout << "Snooper stats:\n";
        std::cout << "    Snooped in buffer input queue:           " << snooped_in_buffer_queue_ << "\n";
        std::cout << "    Snooped in buffer stage internal vector: " << snooped_in_buffer_stage_ << "\n";
        std::cout << "    Snooped in boost::serialization queue:   " << snooped_in_boost_queue_ << "\n";
        std::cout << "    Snooped in zlib compression queue:       " << snooped_in_zlib_queue_ << "\n";
        std::cout << "    Snooped in database writer queue:        " << snooped_in_db_queue_ << "\n";
        std::cout << "    Snooped in database:                     " << snooped_in_database_ << "\n\n";
    }

private:
    class BufferStage : public simdb::pipeline::Stage
    {
    public:
        BufferStage(const std::string& name, simdb::pipeline::QueueRepo& queue_repo, uint32_t& snooped_queue_counter, uint32_t& snooped_internal_counter)
            : simdb::pipeline::Stage(name, queue_repo)
            , snooped_queue_counter_(snooped_queue_counter)
            , snooped_internal_counter_(snooped_internal_counter)
        {
            addInPort_<DummyData>("input_data", input_queue_);
            addOutPort_<std::vector<DummyData>>("output_buffer", output_queue_);
        }

        bool snoop(const std::string& uuid, DummyData& snooped)
        {
            bool found = false;

            // First look through the DummyData input queue feeding this stage
            input_queue_->snoop([&](const DummyData& queue_data)
            {
                if (queue_data.uuid == uuid)
                {
                    // Found it! Make a copy.
                    snooped = queue_data;

                    // Let the BufferStage::snoop() caller know that 'snooped' is valid.
                    found = true;

                    // Increment the appropriate counter for reporting purposes.
                    ++snooped_queue_counter_;

                    // Let the ConcurrentQueue know that we can stop iterating the queue.
                    // This lambda is called for every queue item until we return true.
                    return true;
                }

                // Keep going.
                return false;
            });

            if (found)
            {
                return true;
            }

            // We did not find it in the input queue. Now check our internal buffer.
            for (const auto& buffered_data : dummy_data_buf_)
            {
                if (buffered_data.uuid == uuid)
                {
                    // Found it! Make a copy.
                    snooped = buffered_data;

                    // Increment the appropriate counter for reporting purposes.
                    ++snooped_internal_counter_;

                    // Let the BufferStage::snoop() caller know that 'snooped' is valid.
                    return true;
                }
            }

            // Let the BufferStage::snoop() caller know that 'snooped' is invalid.
            return false;
        }

    private:
        simdb::pipeline::PipelineAction run_(bool force) override
        {
            auto action = simdb::pipeline::PipelineAction::SLEEP;

            DummyData input_data;
            while (input_queue_->try_pop(input_data))
            {
                action = simdb::pipeline::PipelineAction::PROCEED;
                dummy_data_buf_.emplace_back(std::move(input_data));
                if (dummy_data_buf_.size() == 10)
                {
                    output_queue_->emplace(std::move(dummy_data_buf_));
                }
            }

            if (force && !dummy_data_buf_.empty())
            {
                action = simdb::pipeline::PipelineAction::PROCEED;
                output_queue_->emplace(std::move(dummy_data_buf_));
            }

            return action;
        }

        simdb::ConcurrentQueue<DummyData>* input_queue_ = nullptr;
        simdb::ConcurrentQueue<std::vector<DummyData>>* output_queue_ = nullptr;
        std::vector<DummyData> dummy_data_buf_;
        uint32_t& snooped_queue_counter_;
        uint32_t& snooped_internal_counter_;
    };

    class BoostStage : public simdb::pipeline::Stage
    {
    public:
        BoostStage(const std::string& name, simdb::pipeline::QueueRepo& queue_repo, uint32_t& snooped_counter)
            : simdb::pipeline::Stage(name, queue_repo)
            , snooped_counter_(snooped_counter)
        {
            addInPort_<std::vector<DummyData>>("input_buffer", input_queue_);
            addOutPort_<DummyDataWindowBytes>("output_bytes", output_queue_);
        }

        bool snoop(const std::string& uuid, DummyData& snooped)
        {
            bool found = false;

            // Look through the DummyData input queue feeding this stage
            input_queue_->snoop([&](const std::vector<DummyData>& queue_data)
            {
                for (const auto& dummy_data : queue_data)
                {
                    if (dummy_data.uuid == uuid)
                    {
                        // Found it! Make a copy.
                        snooped = dummy_data;

                        // Let the BoostStage::snoop() caller know that 'snooped' is valid.
                        found = true;

                        // Increment the appropriate counter for reporting purposes.
                        ++snooped_counter_;

                        // Let the ConcurrentQueue know that we can stop iterating the queue.
                        // This lambda is called for every queue item until we return true.
                        return true;
                    }
                }

                // Keep going.
                return false;
            });

            // Let the BoostStage::snoop() caller know that 'snooped' is valid or not.
            return found;
        }

    private:
        simdb::pipeline::PipelineAction run_(bool) override
        {
            auto action = simdb::pipeline::PipelineAction::SLEEP;

            std::vector<DummyData> dummy_data_buf;
            while (input_queue_->try_pop(dummy_data_buf))
            {
                DummyDataWindowBytes window;
                for (const auto& dummy_data : dummy_data_buf)
                {
                    window.uuids.push_back(dummy_data.uuid);
                }

                namespace bio = boost::iostreams;
                bio::back_insert_device<std::vector<char>> inserter(window.bytes);
                bio::stream<bio::back_insert_device<std::vector<char>>> os(inserter);
                boost::archive::binary_oarchive oa(os);
                oa << dummy_data_buf;
                os.flush();

                output_queue_->emplace(std::move(window));
                action = simdb::pipeline::PipelineAction::PROCEED;
            }

            return action;
        }

        simdb::ConcurrentQueue<std::vector<DummyData>>* input_queue_ = nullptr;
        simdb::ConcurrentQueue<DummyDataWindowBytes>* output_queue_ = nullptr;
        uint32_t& snooped_counter_;
    };

    class ZlibStage : public simdb::pipeline::Stage
    {
    public:
        ZlibStage(const std::string& name, simdb::pipeline::QueueRepo& queue_repo, uint32_t& snooped_counter)
            : simdb::pipeline::Stage(name, queue_repo)
            , snooped_counter_(snooped_counter)
        {
            addInPort_<DummyDataWindowBytes>("input_bytes", input_queue_);
            addOutPort_<DummyDataWindowBytes>("output_bytes", output_queue_);
        }

        bool snoop(const std::string& uuid, DummyData& snooped)
        {
            bool found = false;

            // Look through the input queue feeding this stage
            input_queue_->snoop([&](const DummyDataWindowBytes& window)
            {
                auto it = std::find(window.uuids.begin(), window.uuids.end(), uuid);
                if (it != window.uuids.end())
                {
                    // Found it! Undo boost::serialization so we can get the original DummyData.
                    namespace bio = boost::iostreams;
                    bio::array_source src(window.bytes.data(), window.bytes.size());
                    bio::stream<bio::array_source> is(src);
                    boost::archive::binary_iarchive ia(is);

                    std::vector<DummyData> dummy_data_buf;
                    ia >> dummy_data_buf;

                    for (const auto& dummy_data : dummy_data_buf)
                    {
                        if (dummy_data.uuid == uuid)
                        {
                            // Make a copy.
                            snooped = dummy_data;

                            // Let the ZlibStage::snoop() caller know that 'snooped' is valid.
                            found = true;

                            // Increment the appropriate counter for reporting purposes.
                            ++snooped_counter_;

                            // Let the ConcurrentQueue know that we can stop iterating the queue.
                            // This lambda is called for every queue item until we return true.
                            return true;
                        }
                    }

                    // We should NEVER get here!
                    throw simdb::DBException("Internal error occurred - mismatching UUIDs");
                }

                // Keep going.
                return false;
            });

            // Let the ZlibStage::snoop() caller know that 'snooped' is valid or not.
            return found;
        }

    private:
        simdb::pipeline::PipelineAction run_(bool) override
        {
            auto action = simdb::pipeline::PipelineAction::SLEEP;

            DummyDataWindowBytes window;
            while (input_queue_->try_pop(window))
            {
                std::vector<char> compressed;
                simdb::compressData(window.bytes, compressed);
                std::swap(window.bytes, compressed);

                output_queue_->emplace(std::move(window));
                action = simdb::pipeline::PipelineAction::PROCEED;
            }

            return action;
        }

        simdb::ConcurrentQueue<DummyDataWindowBytes>* input_queue_ = nullptr;
        simdb::ConcurrentQueue<DummyDataWindowBytes>* output_queue_ = nullptr;
        uint32_t& snooped_counter_;
    };

    class DatabaseStage : public simdb::pipeline::DatabaseStage<PipelineSnooper>
    {
    public:
        DatabaseStage(const std::string& name, simdb::pipeline::QueueRepo& queue_repo, uint32_t& snooped_queue_counter, uint32_t& snooped_db_counter)
            : simdb::pipeline::DatabaseStage<PipelineSnooper>(name, queue_repo)
            , snooped_queue_counter_(snooped_queue_counter)
            , snooped_database_counter_(snooped_db_counter)
        {
            addInPort_<DummyDataWindowBytes>("input_bytes", input_queue_);
        }

        bool snoop(const std::string& uuid, DummyData& snooped)
        {
            bool found = false;

            // Look through the input queue feeding this stage
            input_queue_->snoop([&](const DummyDataWindowBytes& window)
            {
                auto it = std::find(window.uuids.begin(), window.uuids.end(), uuid);
                if (it != window.uuids.end())
                {
                    // Found it! Undo zlib.
                    std::vector<char> uncompressed;
                    simdb::decompressData(window.bytes, uncompressed);

                    // Undo boost::serialization so we can get the original DummyData.
                    namespace bio = boost::iostreams;
                    bio::array_source src(uncompressed.data(), uncompressed.size());
                    bio::stream<bio::array_source> is(src);
                    boost::archive::binary_iarchive ia(is);

                    std::vector<DummyData> dummy_data_buf;
                    ia >> dummy_data_buf;

                    for (const auto& dummy_data : dummy_data_buf)
                    {
                        if (dummy_data.uuid == uuid)
                        {
                            // Make a copy.
                            snooped = dummy_data;

                            // Let the ZlibStage::snoop() caller know that 'snooped' is valid.
                            found = true;

                            // Increment the appropriate counter for reporting purposes.
                            ++snooped_queue_counter_;

                            // Let the ConcurrentQueue know that we can stop iterating the queue.
                            // This lambda is called for every queue item until we return true.
                            return true;
                        }
                    }

                    // We should NEVER get here!
                    throw simdb::DBException("Internal error occurred - mismatching UUIDs");
                }

                // Keep going.
                return false;
            });

            if (!found)
            {
                // If we snooped all the stages through the DatabaseStage and did not find
                // the DummyData with the given uuid, then check the database directly.
                auto db_mgr = getDatabaseManager_();
                auto query = db_mgr->createQuery("DummyDataUUIDs");
                query->addConstraintForString("UUID", simdb::Constraints::EQUAL, uuid);

                int blob_id;
                query->select("DummyDataBlobID", blob_id);

                auto results1 = query->getResultSet();
                if (results1.getNextRecord())
                {
                    // Found the blob ID! Now run a query against the DummyDataBlobs table.
                    query = db_mgr->createQuery("DummyDataBlobs");
                    query->addConstraintForInt("Id", simdb::Constraints::EQUAL, blob_id);

                    std::vector<char> compressed;
                    query->select("DataBlob", compressed);

                    auto results2 = query->getResultSet();
                    if (results2.getNextRecord())
                    {
                        // Found it! Undo zlib.
                        std::vector<char> uncompressed;
                        simdb::decompressData(compressed, uncompressed);

                        // Undo boost::serialization so we can get the original DummyData.
                        namespace bio = boost::iostreams;
                        bio::array_source src(uncompressed.data(), uncompressed.size());
                        bio::stream<bio::array_source> is(src);
                        boost::archive::binary_iarchive ia(is);

                        std::vector<DummyData> dummy_data_buf;
                        ia >> dummy_data_buf;

                        for (const auto& dummy_data : dummy_data_buf)
                        {
                            if (dummy_data.uuid == uuid)
                            {
                                // Make a copy.
                                snooped = dummy_data;

                                // Let the DatabaseStage::snoop() caller know that 'snooped' is valid.
                                found = true;

                                // Increment the appropriate counter for reporting purposes.
                                ++snooped_database_counter_;
                            }
                        }

                        if (!found)
                        {
                            // We should NEVER get here!
                            throw simdb::DBException("Internal error occurred - mismatching UUIDs");
                        }
                    }
                    else
                    {
                        // We should NEVER get here!
                        throw simdb::DBException("Internal error occurred - database record ID not found");
                    }
                }
            }

            // Let the DatabaseStage::snoop() caller know that 'snooped' is valid or not.
            return found;
        }

    private:
        simdb::pipeline::PipelineAction run_(bool) override
        {
            auto action = simdb::pipeline::PipelineAction::SLEEP;
            auto blob_inserter = getTableInserter_("DummyDataBlobs");
            auto uuids_inserter = getTableInserter_("DummyDataUUIDs");

            DummyDataWindowBytes window;
            while (input_queue_->try_pop(window))
            {
                blob_inserter->setColumnValue(0, window.bytes);
                auto blob_id = blob_inserter->createRecord();

                for (const auto& uuid : window.uuids)
                {
                    uuids_inserter->setColumnValue(0, uuid);
                    uuids_inserter->setColumnValue(1, blob_id);
                    uuids_inserter->createRecord();
                }

                action = simdb::pipeline::PipelineAction::PROCEED;
            }

            return action;
        }

        simdb::ConcurrentQueue<DummyDataWindowBytes>* input_queue_ = nullptr;
        uint32_t& snooped_queue_counter_;
        uint32_t& snooped_database_counter_;
    };

    simdb::DatabaseManager* db_mgr_ = nullptr;
    simdb::pipeline::PipelineManager* pipeline_mgr_ = nullptr;
    std::unique_ptr<simdb::pipeline::Flusher> pipeline_flusher_;

    using Snooper = simdb::pipeline::PipelineSnooper<std::string, DummyData>;
    std::unique_ptr<Snooper> pipeline_snooper_;

    simdb::ConcurrentQueue<DummyData>* pipeline_head_ = nullptr;
    std::string snooping_for_uuid_;
    DummyData snooped_data_;
    uint32_t snooped_in_buffer_queue_ = 0;
    uint32_t snooped_in_buffer_stage_ = 0;
    uint32_t snooped_in_boost_queue_ = 0;
    uint32_t snooped_in_zlib_queue_ = 0;
    uint32_t snooped_in_db_queue_ = 0;
    uint32_t snooped_in_database_ = 0;
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
    app_mgr.initializePipelines();
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
            DummyData actual;
            EXPECT_TRUE(app->snoopPipeline(expected.uuid, actual));
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
