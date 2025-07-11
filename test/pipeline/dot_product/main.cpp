// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/elements/Buffer.hpp"
#include "simdb/pipeline/elements/Function.hpp"
#include "simdb/pipeline/elements/DatabaseQueue.hpp"
#include "simdb/utils/Compress.hpp"
#include "simdb/utils/Random.hpp"
#include "SimDBTester.hpp"

// This test shows how to configure and build a pipeline for SimDB apps.
//
// To showcase a variety of pipeline elements, this app will stream data
// to a dot product calculation engine, and send compressed results to
// the database asynchronously.
//
// In one thread, buffer the input vectors, compute the dot products, and buffer the
// dot products before sending them along down the pipeline.
//
// The dot product value vectors are compressed on a second thread, and
// writes to SQLite happen on a third thread.

using DotProdInput = std::vector<double>;
using BufferedDotProdInputs = std::vector<DotProdInput>;
using DotProductValue = double;
using BufferedDotProductValues = std::vector<DotProductValue>;
using CompressedBytes = std::vector<char>;

static constexpr size_t DOT_PROD_ARRAY_LEN = 2;
static constexpr size_t DOT_PROD_BUFLEN = 1000;

double GetDotProduct(const BufferedDotProdInputs& in)
{
    if (in.empty())
    {
        return 0;
    }

    const size_t num_rows = in.size();
    const size_t num_cols = in[0].size();

    if (num_rows != DOT_PROD_ARRAY_LEN)
    {
        throw simdb::DBException("Cannot compute dot product - did not buffer enough inputs");
    }

    for (const auto& row : in)
    {
        if (row.size() != num_cols)
        {
            throw simdb::DBException("All rows must have the same number of columns.");
        }
    }

    double sum = 0;
    for (size_t col = 0; col < num_cols; ++col)
    {
        double product = 1;
        for (size_t row = 0; row < num_rows; ++row)
        {
            product *= in[row][col];
        }
        sum += product;
    }

    return sum;
}

void SendDotProduct(BufferedDotProdInputs&& in, simdb::ConcurrentQueue<double>& out)
{
    out.push(GetDotProduct(in));
}

void CompressBytes(BufferedDotProductValues&& in, simdb::ConcurrentQueue<CompressedBytes>& out)
{
    CompressedBytes compressed;
    simdb::compressData(in, compressed);
    out.emplace(std::move(compressed));
}

void WriteCompressedBytes(CompressedBytes&& in, simdb::DatabaseManager* db_mgr)
{
    // This is on the dedicated DB thread. Note that we are inside a
    // larger BEGIN/COMMIT TRANSACTION block with many other DB writes
    // going on.
    db_mgr->INSERT(SQL_TABLE("DotProducts"),
                   SQL_COLUMNS("Blob"),
                   SQL_VALUES(std::move(in)));
}

class DotProductApp : public simdb::App
{
public:
    static constexpr auto NAME = "dot-products";

    DotProductApp(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    ~DotProductApp() noexcept = default;

    bool defineSchema(simdb::Schema& schema) override
    {
        using dt = simdb::SqlDataType;

        auto& dp_tbl = schema.addTable("DotProducts");
        dp_tbl.addColumn("Blob", dt::blob_t);

        return true;
    }

    void postInit(int argc, char** argv) override
    {
        (void)argc;
        (void)argv;
    }

    std::unique_ptr<simdb::pipeline::Pipeline> createPipeline() override
    {
        auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

        // Thread 1 tasks
        auto dot_prod_task1 = simdb::pipeline::createTask<simdb::pipeline::Buffer<DotProdInput>>(DOT_PROD_ARRAY_LEN);
        auto dot_prod_task2 = simdb::pipeline::createTask<simdb::pipeline::Function<BufferedDotProdInputs, double>>(SendDotProduct);
        auto dot_prod_task3 = simdb::pipeline::createTask<simdb::pipeline::Buffer<double>>(DOT_PROD_BUFLEN);

        // Thread 2 task
        auto zlib_task = simdb::pipeline::createTask<simdb::pipeline::Function<BufferedDotProductValues, CompressedBytes>>(CompressBytes);

        // Thread 3 task
        auto sqlite_task = simdb::pipeline::createTask<simdb::pipeline::DatabaseQueue<CompressedBytes, void>>(WriteCompressedBytes);

        // Connect tasks -------------------------------------------------------------------
        *dot_prod_task1 >> *dot_prod_task2 >> *dot_prod_task3 >> *zlib_task >> *sqlite_task;

        // Get the pipeline input (head) ---------------------------------------------------
        pipeline_head_ = dot_prod_task1->getTypedInputQueue<DotProdInput>();

        // Assign threads (task groups) ----------------------------------------------------
        // Thread 1:
        pipeline->createTaskGroup("DotProduct")
            ->addTask(std::move(dot_prod_task1))
            ->addTask(std::move(dot_prod_task2))
            ->addTask(std::move(dot_prod_task3));

        // Thread 2:
        pipeline->createTaskGroup("Compression")
            ->addTask(std::move(zlib_task));

        // Thread 3:
        pipeline->createTaskGroup("Database")
            ->addTask(std::move(sqlite_task));

        return pipeline;
    }

    void process(std::vector<double>&& input)
    {
        pipeline_head_->emplace(std::move(input));
    }

    void postSim() override
    {
    }

    void teardown() override
    {
    }

private:
    simdb::ConcurrentQueue<std::vector<double>>* pipeline_head_ = nullptr;
    simdb::DatabaseManager* db_mgr_ = nullptr;
};

REGISTER_SIMDB_APPLICATION(DotProductApp);

TEST_INIT;

int main(int argc, char** argv)
{
    DB_INIT;


    simdb::DatabaseManager db_mgr("test.db");
    simdb::AppManager app_mgr(&db_mgr);
    app_mgr.enableApp(DotProductApp::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.postInit(argc, argv);
    app_mgr.openPipelines();

    // Simulate...
    auto app = app_mgr.getApp<DotProductApp>();
    constexpr size_t STEPS = 10000;
    std::vector<std::vector<double>> sent;
    for (size_t i = 1; i <= STEPS; ++i)
    {
        // Generate random [a,b,c] vector for dot product.
        auto input = simdb::utils::generateRandomData<double>(3);
        sent.push_back(input);
        app->process(std::move(input));
    }

    // Finish...
    app_mgr.postSim();
    app_mgr.teardown();
    app_mgr.destroy();

    // Validate...
    std::vector<std::vector<double>> buffered_dot_products;
    std::vector<double> dot_products;

    for (size_t i = 0; i < sent.size() - DOT_PROD_ARRAY_LEN + 1; i += DOT_PROD_ARRAY_LEN)
    {
        std::vector<std::vector<double>> mat;
        for (size_t j = 0; j < DOT_PROD_ARRAY_LEN; ++j)
        {
            mat.push_back(sent.at(i+j));
        }

        dot_products.push_back(GetDotProduct(std::move(mat)));
        if (dot_products.size() == DOT_PROD_BUFLEN)
        {
            buffered_dot_products.push_back(dot_products);
            dot_products.clear();
        }
    }

    std::vector<std::vector<char>> expected_blobs;
    for (const auto& uncompressed : buffered_dot_products)
    {
        expected_blobs.push_back({});
        simdb::compressData(uncompressed, expected_blobs.back());
    }

    auto query = db_mgr.createQuery("DotProducts");
    EXPECT_EQUAL(query->count(), expected_blobs.size());

    std::vector<char> blob;
    query->select("Blob", blob);

    auto result_set = query->getResultSet();
    size_t i = 0;
    while (result_set.getNextRecord())
    {
        EXPECT_EQUAL(expected_blobs[i++], blob);
    }

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
