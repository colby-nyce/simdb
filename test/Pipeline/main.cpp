#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/utils/Compress.hpp"
#include "simdb/test/SimDBTester.hpp"
#include "simdb/utils/Random.hpp"

// clang-format off

// This test shows how to configure and build a pipeline for SimDB apps.
// Pipelines are composed of stages, and stages are composed of transforms.
// Each pipeline stage runs on its own thread, and both stages and transforms
// can have any input/output data type.
//
// To illustrate the flexibility of SimDB pipelines, we will write an app
// which performs dot products on input data, buffers 1000 of the dot product
// values, compresses them into a char buffer, and writes the compressed data
// to the database.
//
// Stage (thread)             Transform
// -----------------------------------------------------------------
// 1                          In:   std::vector<double>
//                            Do:   buffer N arrays
//                            Out:  std::vector<std::vector<double>>
//
// 1                          In:   std::vector<std::vector<double>>
//                            Do:   calculate dot product
//                            Out:  double
//
// 1                          In:   double
//                            Do:   buffer M dot products
//                            Out:  std::vector<double>
//
// 2                          In:   std::vector<double>
//                            Do:   zlib compression
//                            Out:  std::vector<char>
//
// 2                          In:   std::vector<char>
//                            Do:   write to database
//                            Out:  (no output)
//
using DotProdArray = std::vector<double>;
using DotProdArrays = std::vector<DotProdArray>;
using DotProdValue = double;
using DotProdValueBuffer = std::vector<DotProdValue>;
using CompressedDotProdValues = std::vector<char>;

constexpr size_t NUM_DOT_PROD_ARRAYS = 2;
constexpr size_t DOT_PROD_BUFFER_LEN = 1000;

double GetColumnwiseDotProduct(const std::vector<std::vector<double>>& mat)
{
    if (mat.empty())
    {
        return 0.0;
    }

    const size_t num_rows = mat.size();
    const size_t num_cols = mat[0].size();

    for (const auto& row : mat)
    {
        if (row.size() != num_cols)
        {
            throw simdb::DBException("All rows must have the same number of columns.");
        }
    }

    double sum = 0.0;
    for (size_t col = 0; col < num_cols; ++col)
    {
        double product = 1.0;
        for (size_t row = 0; row < num_rows; ++row)
        {
            product *= mat[row][col];
        }
        sum += product;
    }

    return sum;
}

class DotProductSerializer : public simdb::PipelineApp
{
public:
    static constexpr auto NAME = "dot-product";

    DotProductSerializer(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    bool defineSchema(simdb::Schema& schema) override
    {
        using dt = simdb::SqlDataType;

        auto& meta_tbl = schema.addTable("Metadata");
        meta_tbl.addColumn("NumDotProdArrays", dt::int32_t);
        meta_tbl.addColumn("DotProdBufferLen", dt::int32_t);
        meta_tbl.addColumn("SimCommandLine", dt::string_t);

        auto& data_tbl = schema.addTable("CompressedDotProducts");
        data_tbl.addColumn("DataBlob", dt::blob_t);

        return true;
    }

    std::vector<std::unique_ptr<simdb::PipelineStageBase>> configPipeline() override
    {
        // These are the state variables we will use inside the transforms below.
        // We are enforced to give ownership to the transform so it is guaranteed
        // safe to access from the pipeline thread.
        DotProdArrays transform1_array_buf;
        DotProdValueBuffer transform3_dot_prod_val_buf;

        // Stage 1:
        //   - Input type:      DotProdArray
        //   - Output type:     DotProdValueBuffer
        //   - Num transforms:  3
        //   - Database access: No
        auto stage1 = std::make_unique<simdb::PipelineStage<DotProdArray, DotProdValueBuffer>>();

        // Transform 1:
        //   - Input type:      DotProdArray
        //   - Output type:     DotProdArrays
        //   - Function:        Buffer N arrays
        auto transform1 = std::make_unique<simdb::PipelineTransform<DotProdArray, DotProdArrays, DotProdArrays>>(
            std::move(transform1_array_buf),
            [](DotProdArray& in, simdb::ConcurrentQueue<DotProdArrays>& out, DotProdArrays& buf)
            {
                buf.emplace_back(std::move(in));
                if (buf.size() == NUM_DOT_PROD_ARRAYS)
                {
                    out.emplace(std::move(buf));
                }
            },
            // If you want to take bake ownership of the transform state object,
            // provide a post-sim callback. This is called just prior to your
            // app's postSim() method is called.
            [this](simdb::DatabaseManager*, DotProdArrays&& arrays)
            {
                takeBack_(std::move(arrays));
            }
        );

        // Transform 2:
        //   - Input type:      DotProdArrays
        //   - Output type:     DotProdValue
        //   - Function:        Calculate dot product
        auto transform2 = std::make_unique<simdb::PipelineTransform<DotProdArrays, DotProdValue, void>>(
            [](DotProdArrays& in, simdb::ConcurrentQueue<DotProdValue>& out)
            {
                auto dot_product = GetColumnwiseDotProduct(in);
                out.push(dot_product);
            }
        );

        // Transform 3:
        //   - Input type:      DotProdValue
        //   - Output type:     DotProdValueBuffer
        //   - Function:        Buffer M dot products
        auto transform3 = std::make_unique<simdb::PipelineTransform<DotProdValue, DotProdValueBuffer, DotProdValueBuffer>>(
            std::move(transform3_dot_prod_val_buf),
            [](DotProdValue& in, simdb::ConcurrentQueue<DotProdValueBuffer>& out, DotProdValueBuffer& buf)
            {
                buf.push_back(in);
                if (buf.size() == DOT_PROD_BUFFER_LEN)
                {
                    out.emplace(std::move(buf));
                }
            },
            // If you want to take bake ownership of the transform state object,
            // provide a post-sim callback. This is called just prior to your
            // app's postSim() method is called.
            [this](simdb::DatabaseManager*, DotProdValueBuffer&& buffer)
            {
                takeBack_(std::move(buffer));
            }
        );

        // Connect stage 1
        stage1->first(std::move(transform1));
        stage1->then(std::move(transform2));
        stage1->last(std::move(transform3));

        // Stage 2:
        //   - Input type:      DotProdValueBuffer
        //   - Output type:     none
        //   - Num transforms:  2
        //   - Database access: Yes
        auto stage2 = std::make_unique<simdb::PipelineStage<DotProdValueBuffer, void>>(db_mgr_);

        // Transform 4:
        //   - Input type:      DotProdValueBuffer
        //   - Output type:     CompressedDotProdValues
        //   - Function:        Perform zlib compression
        auto transform4 = std::make_unique<simdb::PipelineTransform<DotProdValueBuffer, CompressedDotProdValues, void>>(
            [](DotProdValueBuffer& in, simdb::ConcurrentQueue<CompressedDotProdValues>& out)
            {
                std::vector<char> compressed;
                auto data_ptr = in.data();
                auto num_bytes = in.size() * sizeof(double);
                simdb::compressData(data_ptr, num_bytes, compressed);
                out.emplace(std::move(compressed));
            }
        );

        // Transform 5:
        //   - Input type:      CompressedDotProdValues
        //   - Output type:     none
        //   - Function:        Write to database
        auto transform5 = std::make_unique<simdb::PipelineTransform<CompressedDotProdValues, void, void>>(
            [this](CompressedDotProdValues& in)
            {
                // Even though we are accessing "this" member variable db_mgr_,
                // the INSERT method performs a safeTransaction() under the
                // hood, which is mutex-protected.
                db_mgr_->INSERT(SQL_TABLE("CompressedDotProducts"),
                                SQL_COLUMNS("DataBlob"),
                                SQL_VALUES(in));
            }
        );

        // Connect stage 2
        stage2->first(std::move(transform4));
        stage2->last(std::move(transform5));

        // Finalize
        std::vector<std::unique_ptr<simdb::PipelineStageBase>> stages;
        stages.emplace_back(std::move(stage1));
        stages.emplace_back(std::move(stage2));
        return stages;
    }

    void setPipelineInputQueue(simdb::TransformQueueBase* queue) override
    {
        if (auto q = dynamic_cast<simdb::TransformQueue<DotProdArray>*>(queue))
        {
            pipeline_queue_ = q->getQueue();
        }
        else
        {
            throw simdb::DBException("Invalid data type! Expected a ConcurrentQueue<DotProdArray>");
        }
    }

    void postInit(int argc, char** argv) override
    {
        std::ostringstream oss;
        for (int i = 0; i < argc; ++i)
        {
            oss << argv[i] << " ";
        }
        const auto sim_cmdline = oss.str();

        db_mgr_->INSERT(SQL_TABLE("Metadata"),
                        SQL_COLUMNS("NumDotProdArrays", "DotProdBufferLen", "SimCommandLine"),
                        SQL_VALUES(NUM_DOT_PROD_ARRAYS, DOT_PROD_BUFFER_LEN, sim_cmdline));
    }

    void postSim() override
    {
        EXPECT_TRUE(arrays_returned_);
        EXPECT_TRUE(buffer_returned_);
    }

    void process(const DotProdArray& data)
    {
        pipeline_queue_->push(data);
    }

    void process(DotProdArray&& data)
    {
        pipeline_queue_->emplace(std::move(data));
    }

private:
    void takeBack_(DotProdArrays&&)
    {
        arrays_returned_ = true;
    }

    void takeBack_(DotProdValueBuffer&&)
    {
        buffer_returned_ = true;
    }

    simdb::DatabaseManager *const db_mgr_;
    simdb::ConcurrentQueue<DotProdArray>* pipeline_queue_ = nullptr;
    bool arrays_returned_ = false;
    bool buffer_returned_ = false;
};

REGISTER_SIMDB_APPLICATION(DotProductSerializer);

int main(int argc, char** argv)
{
    DB_INIT;

    simdb::AppManager app_mgr;
    app_mgr.enableApp(DotProductSerializer::NAME);

    simdb::DatabaseManager db_mgr("test.db");

    // Setup...
    app_mgr.createEnabledApps(&db_mgr);
    app_mgr.createSchemas(&db_mgr);
    app_mgr.postInit(&db_mgr, argc, argv);

    auto serializer = app_mgr.getApp<DotProductSerializer>(&db_mgr);
    std::vector<std::vector<double>> sent;
    for (uint64_t tick = 0; tick < DOT_PROD_BUFFER_LEN * 2; ++tick)
    {
        // Push a random set of values e.g. [a1,a2,a3]
        auto values = simdb::utils::generateRandomData<double>(3);
        sent.push_back(values);
        serializer->process(std::move(values));
    }

    // Finish...
    app_mgr.postSim(&db_mgr);
    app_mgr.teardown(&db_mgr);
    app_mgr.destroy();

    // Validate...
    std::vector<double> dot_products;
    for (size_t i = 0; i < sent.size(); i += 2)
    {
        DotProdArrays mat;
        mat.push_back(sent[i]);
        mat.push_back(sent[i+1]);
        auto dot_product = GetColumnwiseDotProduct(mat);
        dot_products.push_back(dot_product);
    }

    EXPECT_EQUAL(dot_products.size(), DOT_PROD_BUFFER_LEN);

    std::vector<char> compressed_dot_products;
    auto data_ptr = dot_products.data();
    auto num_bytes = dot_products.size() * sizeof(double);
    simdb::compressData(data_ptr, num_bytes, compressed_dot_products);

    auto query = db_mgr.createQuery("CompressedDotProducts");
    std::vector<char> written_blob;
    query->select("DataBlob", written_blob);

    auto result_set = query->getResultSet();
    EXPECT_TRUE(result_set.getNextRecord());
    EXPECT_EQUAL(written_blob, compressed_dot_products);
    EXPECT_FALSE(result_set.getNextRecord());

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
