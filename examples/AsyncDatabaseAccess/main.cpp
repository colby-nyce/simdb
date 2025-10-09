// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/AsyncDatabaseAccessor.hpp"
#include "simdb/utils/TickTock.hpp"
#include "SimDBTester.hpp"
#include <unordered_set>

// This shows how to use the AsyncDatabaseAccessor to put writes/reads/updates
// all on the database thread. Specifically, we will create two pipelines in
// separate apps to test their respective performance in how to handle async
// database writes and updates (as opposed to a more obvious solution of just
// using a RunnableFlusher to flush the whole pipeline in the main thread and
// then using DatabaseManager directly, which may not have great performance in
// some use cases).
//
//   First pipeline:
//     - Send UPDATE/DELETE messages down the pipeline to ensure FIFO ordering
//
//   Second pipeline:
//     - Use a member variable and mutex that keeps track of UPDATE/DELETE requests
//       and handles them on the DB thread

struct Packet
{
    uint64_t tick = 0;

    struct DataVals
    {
        int x = rand() % 100;
        int y = rand() % 100;
        int z = rand() % 100;
    } data_vals;

    enum Operation { INSERT, UPDATE, DELETE };
    Operation op = INSERT;

    Packet(uint64_t t, Operation o) : tick(t), op(o) {}
    Packet(const Packet&) = default;
    Packet(Packet&&) = default;
    Packet& operator=(const Packet&) = default;
    Packet& operator=(Packet&&) = default;

    static Packet createINSERT(uint64_t tick)
    {
        return Packet(tick, Packet::INSERT);
    }

    static Packet createUPDATE(uint64_t tick, int x, int y, int z)
    {
        auto pkt = Packet(tick, Packet::UPDATE);
        pkt.data_vals.x = x;
        pkt.data_vals.y = y;
        pkt.data_vals.z = z;
        return pkt;
    }

    static Packet createDELETE(uint64_t tick)
    {
        return Packet(tick, Packet::DELETE);
    }

    // To support simdb::ConcurrentQueue::try_pop()
    Packet() = default;
};

// First pipeline:
//   - Send UPDATE/DELETE messages down the pipeline to ensure FIFO ordering
class StrictFIFO : public simdb::App
{
public:
    static constexpr auto NAME = "strict-fifo";

    StrictFIFO(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    static void defineSchema(simdb::Schema& schema)
    {
        using dt = simdb::SqlDataType;

        auto& tbl = schema.addTable("Packets");
        tbl.addColumn("Tick", dt::uint64_t);
        tbl.addColumn("X", dt::int32_t);
        tbl.addColumn("Y", dt::int32_t);
        tbl.addColumn("Z", dt::int32_t);
        tbl.createIndexOn("Tick");
    }

    std::unique_ptr<simdb::pipeline::Pipeline> createPipeline(
        simdb::pipeline::AsyncDatabaseAccessor* db_accessor) override
    {
        auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

        auto db_write = db_accessor->createAsyncWriter<StrictFIFO, Packet, Packet>(
            [](Packet&& in,
               simdb::ConcurrentQueue<Packet>& out,
               simdb::pipeline::AppPreparedINSERTs* tables,
               bool /*force*/)
            {
                if (in.op != Packet::INSERT)
                {
                    out.emplace(std::move(in));
                }

                auto inserter = tables->getPreparedINSERT("Packets");
                inserter->setColumnValue(0, in.tick);
                inserter->setColumnValue(1, in.data_vals.x);
                inserter->setColumnValue(2, in.data_vals.y);
                inserter->setColumnValue(3, in.data_vals.z);
                inserter->createRecord();

                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        auto db_update = db_accessor->createAsyncReader<Packet, Packet>(
            [](Packet&& in,
               simdb::ConcurrentQueue<Packet>& out,
               simdb::DatabaseManager* db_mgr,
               bool /*force*/)
            {
                if (in.op == Packet::INSERT)
                {
                    throw simdb::DBException("Cannot run INSERTs through an async db reader");
                }

                if (in.op == Packet::DELETE)
                {
                    out.emplace(std::move(in));
                    return simdb::pipeline::RunnableOutcome::DID_WORK;
                }

                auto query = db_mgr->createQuery("Packets");
                query->addConstraintForUInt64("Tick", simdb::Constraints::EQUAL, in.tick);
                query->deleteResultSet();

                db_mgr->INSERT(
                    SQL_TABLE("Packets"),
                    SQL_COLUMNS("Tick", "X", "Y", "Z"),
                    SQL_VALUES(in.tick, in.data_vals.x, in.data_vals.y, in.data_vals.z));

                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        auto db_delete = db_accessor->createAsyncReader<Packet, void>(
            [](Packet&& in,
               simdb::DatabaseManager* db_mgr,
               bool /*force*/)
            {
                if (in.op != Packet::DELETE)
                {
                    throw simdb::DBException("Cannot execute task - not a DELETE");
                }

                auto query = db_mgr->createQuery("Packets");
                query->addConstraintForUInt64("Tick", simdb::Constraints::EQUAL, in.tick);
                query->deleteResultSet();

                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        // Connect tasks ---------------------------------------------------------------------------
        *db_write >> *db_update >> *db_delete;

        // Store pipeline head ---------------------------------------------------------------------
        pipeline_head_ = db_write->getTypedInputQueue<Packet>();

        // Create pipeline flusher -----------------------------------------------------------------
        pipeline_flusher_ = std::make_unique<simdb::pipeline::RunnableFlusher>(
            *db_mgr_, db_write, db_update, db_delete);

        return pipeline;
    }

    void sendPacket(uint64_t tick)
    {
        auto packet = Packet::createINSERT(tick);
        pipeline_head_->emplace(std::move(packet));
    }

    void updatePacket(uint64_t tick, int x, int y, int z)
    {
        auto packet = Packet::createUPDATE(tick, x, y, z);
        updated_packet_values_.push_back(packet);
        pipeline_head_->emplace(std::move(packet));
    }

    void deletePacket(uint64_t tick)
    {
        auto packet = Packet::createDELETE(tick);
        deleted_packet_ticks_.insert(tick);
        pipeline_head_->emplace(std::move(packet));
    }

    void validate()
    {
        validateStrictFIFO_();
    }

    void postTeardown() override
    {
        validateStrictFIFO_();
    }

private:
    void validateStrictFIFO_()
    {
        PROFILE_METHOD
        pipeline_flusher_->waterfallFlush();

        auto query = db_mgr_->createQuery("Packets");

        // Validate updated packets
        for (const auto& packet : updated_packet_values_)
        {
            if (deleted_packet_ticks_.count(packet.tick))
            {
                continue;
            }

            query->resetSelections();
            query->resetConstraints();
            query->addConstraintForUInt64("Tick", simdb::Constraints::EQUAL, packet.tick);

            int actual_x, actual_y, actual_z;
            query->select("X", actual_x);
            query->select("Y", actual_y);
            query->select("Z", actual_z);

            int exp_x = packet.data_vals.x;
            int exp_y = packet.data_vals.y;
            int exp_z = packet.data_vals.z;

            auto results = query->getResultSet();
            EXPECT_TRUE(results.getNextRecord());
            EXPECT_EQUAL(actual_x, exp_x);
            EXPECT_EQUAL(actual_y, exp_y);
            EXPECT_EQUAL(actual_z, exp_z);
        }

        // Validate deleted packets
        if (!deleted_packet_ticks_.empty())
        {
            std::vector<uint64_t> deleted_ticks(deleted_packet_ticks_.begin(), deleted_packet_ticks_.end());
            query->resetSelections();
            query->resetConstraints();
            query->addConstraintForUInt64("Tick", simdb::SetConstraints::IN_SET, deleted_ticks);
            EXPECT_EQUAL(query->count(), 0);
        }
    }

    simdb::DatabaseManager* db_mgr_ = nullptr;
    simdb::ConcurrentQueue<Packet>* pipeline_head_ = nullptr;
    std::unique_ptr<simdb::pipeline::RunnableFlusher> pipeline_flusher_;
    std::vector<Packet> updated_packet_values_;
    std::unordered_set<uint64_t> deleted_packet_ticks_;
};

REGISTER_SIMDB_APPLICATION(StrictFIFO);

// Second pipeline:
//   - Use a member variable and mutex that keeps track of UPDATE/DELETE requests
//     and handles them on the DB thread
class RelaxedFIFO : public simdb::App
{
public:
    static constexpr auto NAME = "relaxed-fifo";

    RelaxedFIFO(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    static void defineSchema(simdb::Schema& schema)
    {
        using dt = simdb::SqlDataType;

        auto& tbl = schema.addTable("Packets");
        tbl.addColumn("Tick", dt::uint64_t);
        tbl.addColumn("X", dt::int32_t);
        tbl.addColumn("Y", dt::int32_t);
        tbl.addColumn("Z", dt::int32_t);
        tbl.createIndexOn("Tick");
    }

    std::unique_ptr<simdb::pipeline::Pipeline> createPipeline(
        simdb::pipeline::AsyncDatabaseAccessor* db_accessor) override
    {
        auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

        auto db_tasks = db_accessor->createAsyncWriter<StrictFIFO, Packet, void>(
            [this](Packet&& in,
                   simdb::pipeline::AppPreparedINSERTs* tables,
                   bool /*force*/)
            {
                simdb::pipeline::RunnableOutcome outcome = simdb::pipeline::RunnableOutcome::NO_OP;
                if (in.op == Packet::INSERT)
                {
                    auto inserter = tables->getPreparedINSERT("Packets");
                    inserter->setColumnValue(0, in.tick);
                    inserter->setColumnValue(1, in.data_vals.x);
                    inserter->setColumnValue(2, in.data_vals.y);
                    inserter->setColumnValue(3, in.data_vals.z);
                    inserter->createRecord();
                    outcome = simdb::pipeline::RunnableOutcome::DID_WORK;
                }
                else
                {
                    throw simdb::DBException("Cannot run non-INSERTs through an async db writer (RelaxedFIFO)");
                }

                std::lock_guard<std::mutex> lock(mutex_);

                if (pending_updates_.empty() && pending_deletes_.empty())
                {
                    return outcome;
                }

                auto query = db_mgr_->createQuery("Packets");

                if (!pending_updates_.empty())
                {
                    std::vector<uint64_t> delete_ticks;
                    for (const auto& packet : pending_updates_)
                    {
                        delete_ticks.push_back(packet.tick);
                    }

                    query->addConstraintForUInt64("Tick", simdb::SetConstraints::IN_SET, delete_ticks);
                    query->deleteResultSet();

                    for (const auto& packet : pending_updates_)
                    {
                        db_mgr_->INSERT(
                            SQL_TABLE("Packets"),
                            SQL_COLUMNS("Tick", "X", "Y", "Z"),
                            SQL_VALUES(packet.tick, packet.data_vals.x, packet.data_vals.y, packet.data_vals.z));
                    }

                    pending_updates_.clear();
                }

                if (!pending_deletes_.empty())
                {
                    query->resetConstraints();
                    query->addConstraintForUInt64("Tick", simdb::SetConstraints::IN_SET, pending_deletes_);
                    query->deleteResultSet();
                    pending_deletes_.clear();
                }

                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        // Store pipeline head ---------------------------------------------------------------------
        pipeline_head_ = db_tasks->getTypedInputQueue<Packet>();

        // Create pipeline flusher -----------------------------------------------------------------
        pipeline_flusher_ = std::make_unique<simdb::pipeline::RunnableFlusher>(*db_mgr_, db_tasks);

        return pipeline;
    }

    void sendPacket(uint64_t tick)
    {
        auto packet = Packet::createINSERT(tick);
        pipeline_head_->emplace(std::move(packet));
    }

    void updatePacket(uint64_t tick, int x, int y, int z)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_updates_.emplace_back(Packet::createUPDATE(tick, x, y, z));
        expected_updates_.emplace_back(pending_updates_.back());
    }

    void deletePacket(uint64_t tick)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_deletes_.push_back(tick);
        expected_deletes_.insert(tick);
    }

    void validate()
    {
        validateRelaxedFifo_();
    }

    void postTeardown() override
    {
        validateRelaxedFifo_();
    }

private:
    void validateRelaxedFifo_()
    {
        PROFILE_METHOD
        pipeline_flusher_->waterfallFlush();

        std::lock_guard<std::mutex> lock(mutex_);

        if (pending_updates_.empty() && pending_deletes_.empty())
        {
            return;
        }

        auto query = db_mgr_->createQuery("Packets");

        // Validate updated packets
        for (const auto& packet : expected_updates_)
        {
            if (expected_deletes_.count(packet.tick))
            {
                continue;
            }

            query->resetSelections();
            query->resetConstraints();
            query->addConstraintForUInt64("Tick", simdb::Constraints::EQUAL, packet.tick);

            int actual_x, actual_y, actual_z;
            query->select("X", actual_x);
            query->select("Y", actual_y);
            query->select("Z", actual_z);

            int exp_x = packet.data_vals.x;
            int exp_y = packet.data_vals.y;
            int exp_z = packet.data_vals.z;

            auto results = query->getResultSet();
            EXPECT_TRUE(results.getNextRecord());
            EXPECT_EQUAL(actual_x, exp_x);
            EXPECT_EQUAL(actual_y, exp_y);
            EXPECT_EQUAL(actual_z, exp_z);
        }

        // Validate deleted packets
        if (!expected_deletes_.empty())
        {
            std::vector<uint64_t> deleted_ticks(expected_deletes_.begin(), expected_deletes_.end());
            query->resetConstraints();
            query->addConstraintForUInt64("Tick", simdb::SetConstraints::IN_SET, deleted_ticks);
            EXPECT_EQUAL(query->count(), 0);
        }
    }

    simdb::DatabaseManager* db_mgr_ = nullptr;
    simdb::ConcurrentQueue<Packet>* pipeline_head_ = nullptr;
    std::unique_ptr<simdb::pipeline::RunnableFlusher> pipeline_flusher_;

    mutable std::mutex mutex_;
    std::vector<Packet> expected_updates_;
    std::vector<Packet> pending_updates_;
    std::unordered_set<uint64_t> expected_deletes_;
    std::vector<uint64_t> pending_deletes_;
};

REGISTER_SIMDB_APPLICATION(RelaxedFIFO);

TEST_INIT;

void TestStrictFIFO(int argc, char** argv)
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);
    app_mgr.enableApp(StrictFIFO::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.postInit(argc, argv);
    app_mgr.openPipelines();

    // Simulate...
    auto app = app_mgr.getApp<StrictFIFO>();

    for (uint64_t tick = 1; tick <= 10000; ++tick)
    {
        app->sendPacket(tick);

        if (tick % 1000 == 0)
        {
            auto old_tick = rand() % (tick - 500) + 1;
            auto rand_x = rand() % 100;
            auto rand_y = rand() % 100;
            auto rand_z = rand() % 100;
            app->updatePacket(old_tick, rand_x, rand_y, rand_z);
        }
        else if (tick % 500 == 0)
        {
            auto old_tick = rand() % (tick - 50) + 1;
            app->deletePacket(old_tick);
        }
        else if (tick % 50 == 0)
        {
            app->validate();
        }
    }

    // Finish...
    app_mgr.postSimLoopTeardown();
}

void TestRelaxedFIFO(int argc, char** argv)
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);
    app_mgr.enableApp(RelaxedFIFO::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.postInit(argc, argv);
    app_mgr.openPipelines();

    // Simulate...
    auto app = app_mgr.getApp<RelaxedFIFO>();

    for (uint64_t tick = 1; tick <= 10000; ++tick)
    {
        app->sendPacket(tick);

        if (tick % 1000 == 0)
        {
            auto old_tick = rand() % (tick - 500) + 1;
            auto rand_x = rand() % 100;
            auto rand_y = rand() % 100;
            auto rand_z = rand() % 100;
            app->updatePacket(old_tick, rand_x, rand_y, rand_z);
        }
        else if (tick % 500 == 0)
        {
            auto old_tick = rand() % (tick - 50) + 1;
            app->deletePacket(old_tick);
        }
        else if (tick % 50 == 0)
        {
            app->validate();
        }
    }

    // Finish...
    app_mgr.postSimLoopTeardown();
}

int main(int argc, char** argv)
{
    TestStrictFIFO(argc, argv);
    TestRelaxedFIFO(argc, argv);

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
