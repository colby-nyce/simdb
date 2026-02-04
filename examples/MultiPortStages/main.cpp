// clang-format off

#include "simdb/apps/App.hpp"
#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "SimDBTester.hpp"

/// This test demonstrates a pipeline with a multi-input, multi-output stage (MultiPortStages).

class MultiPortStages : public simdb::App
{
public:
    static constexpr auto NAME = "mimo-pipeline-app";

    MultiPortStages(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    ~MultiPortStages() noexcept = default;

    static void defineSchema(simdb::Schema& schema)
    {
        using dt = simdb::SqlDataType;

        auto& calc_tbl = schema.addTable("CalcValues");
        calc_tbl.addColumn("SumValues", dt::double_t);
        calc_tbl.addColumn("ProdValues", dt::double_t);

        auto& meta_tbl = schema.addTable("Metadata");
        meta_tbl.addColumn("NumUnalignedXY", dt::int32_t);
    }

    void createPipeline(simdb::pipeline::PipelineManager* pipeline_mgr) override
    {
        auto pipeline = pipeline_mgr->createPipeline(NAME, this);

        pipeline->addStage<SyncXYStage>("sync_xy");
        pipeline->addStage<DatabaseStage>("db_writer");
        pipeline->noMoreStages();

        pipeline->bind("sync_xy.xy_output", "db_writer.xy_input");
        pipeline->bind("sync_xy.num_unaligned_xy", "db_writer.num_unaligned_xy");
        pipeline->noMoreBindings();

        // As soon as we call noMoreBindings(), all input/output queues are available
        x_input_queue_ = pipeline->getInPortQueue<double>("sync_xy.x_input");
        y_input_queue_ = pipeline->getInPortQueue<double>("sync_xy.y_input");
    }

    void sendX(double x)
    {
        x_input_queue_->push(x);
    }

    void sendY(double y)
    {
        y_input_queue_->push(y);
    }

private:
    template <typename T>
    class ValidValue
    {
    private:
        T value_ = 0;
        bool valid_ = false;

    public:
        ValidValue& operator=(T val)
        {
            value_ = val;
            valid_ = true;
            return *this;
        }

        T getValue() const
        {
            if (!valid_)
            {
                throw simdb::DBException("Invalid value - not set");
            }
            return value_;
        }

        bool isValid() const
        {
            return valid_;
        }

        void clearValid()
        {
            valid_ = false;
        }
    };

    /// This stage uses two inputs X and Y, and forwards the pair <X,Y> when
    /// both X and Y are available on the input queues.
    class SyncXYStage : public simdb::pipeline::Stage
    {
    public:
        SyncXYStage()
        {
            // Receive X and Y values independently
            addInPort_<double>("x_input", x_input_queue_);
            addInPort_<double>("y_input", y_input_queue_);

            // Forward <X,Y> pair when both are available
            addOutPort_<std::pair<double, double>>("xy_output", xy_output_queue_);

            // When simulation is over, forward the number of unaligned XY pairs
            addOutPort_<size_t>("num_unaligned_xy", num_unaligned_output_queue_);
        }

    private:
        simdb::pipeline::PipelineAction run_(bool force) override
        {
            auto outcome = simdb::pipeline::SLEEP;
            while (sendXYPair_())
            {
                outcome = simdb::pipeline::PROCEED;
            }

            if (force && flushUnalignedXYPairs_())
            {
                outcome = simdb::pipeline::PROCEED;
            }
            return outcome;
        }

        bool sendXYPair_()
        {
            if (!ready_x_.isValid())
            {
                double x = 0;
                if (x_input_queue_->try_pop(x))
                {
                    ready_x_ = x;
                }
            }

            if (!ready_y_.isValid())
            {
                double y = 0;
                if (y_input_queue_->try_pop(y))
                {
                    ready_y_ = y;
                }
            }

            if (ready_x_.isValid() && ready_y_.isValid())
            {
                auto x = ready_x_.getValue();
                auto y = ready_y_.getValue();
                auto xy = std::make_pair(x, y);
                ready_x_.clearValid();
                ready_y_.clearValid();
                xy_output_queue_->emplace(std::move(xy));
                return true;
            }

            return false;
        }

        bool flushUnalignedXYPairs_()
        {
            auto x_count = x_input_queue_->size();
            auto y_count = y_input_queue_->size();

            if (x_count > 0 && y_count > 0)
            {
                throw simdb::DBException("More aligned XY pairs available on sim flush/terminate");
            }

            auto num_unaligned = std::max(x_count, y_count);
            if (num_unaligned == 0)
            {
                return false;
            }

            num_unaligned_output_queue_->push(num_unaligned + 1);

            // Flush the input queues now that simulation is over.
            double dummy = 0;
            while (x_input_queue_->try_pop(dummy)) { ; }
            while (y_input_queue_->try_pop(dummy)) { ; }

            return true;
        }

        simdb::ConcurrentQueue<double>* x_input_queue_ = nullptr;
        simdb::ConcurrentQueue<double>* y_input_queue_ = nullptr;
        simdb::ConcurrentQueue<std::pair<double, double>>* xy_output_queue_ = nullptr;
        simdb::ConcurrentQueue<size_t>* num_unaligned_output_queue_ = nullptr;
        ValidValue<double> ready_x_;
        ValidValue<double> ready_y_;
        size_t pair_count_ = 0;
    };

    /// Receive XY pairs and write the sum and product to the database. Also receive
    /// the number of unaligned X/Y values at the end of simulation and write that
    /// value to the database too.
    class DatabaseStage : public simdb::pipeline::DatabaseStage<MultiPortStages>
    {
    public:
        DatabaseStage()
        {
            addInPort_<std::pair<double, double>>("xy_input", xy_input_queue_);
            addInPort_<size_t>("num_unaligned_xy", num_unaligned_input_queue_);
        }

    private:
        simdb::pipeline::PipelineAction run_(bool) override
        {
            auto outcome = simdb::pipeline::SLEEP;

            auto calc_inserter = getTableInserter_("CalcValues");
            auto meta_inserter = getTableInserter_("Metadata");

            std::pair<double, double> xy;
            while (xy_input_queue_->try_pop(xy))
            {
                auto x = xy.first;
                auto y = xy.second;

                auto sum = x + y;
                auto prod = x * y;

                calc_inserter->setColumnValue(0, sum);
                calc_inserter->setColumnValue(1, prod);
                calc_inserter->createRecord();

                outcome = simdb::pipeline::PROCEED;
            }

            size_t num_unaligned = 0;
            if (num_unaligned_input_queue_->try_pop(num_unaligned))
            {
                meta_inserter->setColumnValue(0, (int)num_unaligned);
                meta_inserter->createRecord();
                outcome = simdb::pipeline::PROCEED;
            }

            return outcome;
        }

        simdb::ConcurrentQueue<std::pair<double, double>>* xy_input_queue_ = nullptr;
        simdb::ConcurrentQueue<size_t>* num_unaligned_input_queue_ = nullptr;
    };

    simdb::DatabaseManager* db_mgr_ = nullptr;
    simdb::ConcurrentQueue<double>* x_input_queue_ = nullptr;
    simdb::ConcurrentQueue<double>* y_input_queue_ = nullptr;
};

REGISTER_SIMDB_APPLICATION(MultiPortStages);

TEST_INIT;

int main()
{
    std::srand(std::time(nullptr));

    simdb::AppManagers app_mgrs;
    auto& app_mgr = app_mgrs.getAppManager("test.db");
    auto& db_mgr = app_mgrs.getDatabaseManager();

    // Setup...
    app_mgr.enableApp(MultiPortStages::NAME);
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.initializePipelines();
    app_mgr.openPipelines();

    // Simulate...
    auto app = app_mgr.getApp<MultiPortStages>();
    std::vector<double> sent_x_vals;
    std::vector<double> sent_y_vals;
    for (int i = 0; i < 1000; ++i)
    {
        if (rand() % 5 == 0)
        {
            auto x = rand() % 100 * M_PI;
            sent_x_vals.push_back(x);
            app->sendX(x);
        }

        if (rand() % 5 == 0)
        {
            auto y = rand() % 100 * M_PI;
            sent_y_vals.push_back(y);
            app->sendY(y);
        }
    }

    // Finalize...
    app_mgr.postSimLoopTeardown();

    // Validate...
    auto calc_query = db_mgr.createQuery("CalcValues");

    double sum;
    calc_query->select("SumValues", sum);

    double prod;
    calc_query->select("ProdValues", prod);

    auto calc_results = calc_query->getResultSet();

    auto num_aligned = std::min(sent_x_vals.size(), sent_y_vals.size());
    for (size_t i = 0; i < num_aligned; ++i)
    {
        const auto x = sent_x_vals[i];
        const auto y = sent_y_vals[i];

        const auto expected_sum = x + y;
        const auto expected_prod = x * y;

        EXPECT_TRUE(calc_results.getNextRecord());
        EXPECT_EQUAL(sum, expected_sum);
        EXPECT_EQUAL(prod, expected_prod);
    }

    const auto x_count = sent_x_vals.size();
    const auto y_count = sent_y_vals.size();
    const auto expected_unaligned_xy = std::max(x_count, y_count) - num_aligned;

    auto meta_query = db_mgr.createQuery("Metadata");
    if (expected_unaligned_xy > 0)
    {
        EXPECT_EQUAL(meta_query->count(), 1);

        int actual_unaligned_xy;
        meta_query->select("NumUnalignedXY", actual_unaligned_xy);

        auto meta_results = meta_query->getResultSet();
        EXPECT_TRUE(meta_results.getNextRecord());
        EXPECT_EQUAL(actual_unaligned_xy, expected_unaligned_xy);
        EXPECT_FALSE(meta_results.getNextRecord());
    }
    else
    {
        EXPECT_EQUAL(meta_query->count(), 0);
    }

    REPORT_ERROR;
    return ERROR_CODE;
}
