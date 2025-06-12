#include "simdb/apps/AppRegistration.hpp"
#include "simdb/apps/UniformSerializer.hpp"
#include "simdb/test/SimDBTester.hpp"

// clang-format off

using simdb::PipelineChain;
using simdb::PipelineEntry;

std::vector<double> generateRandomData(size_t size)
{
    std::vector<double> data(size);
    std::mt19937 gen(std::random_device{}());
    std::uniform_real_distribution<double> dis(0.0, 100.0);
    
    for (auto& value : data)
    {
        value = dis(gen);
    }
    
    return data;
}

/// This class shows how to setup a UniformSerializer with a fixed byte layout.
/// We assume that the data is just a vector of doubles, and each double belongs
/// to a named statistic. We will serialize the names in our own metadata table
/// and use the UniformSerializer to handle the bytes in the pipeline.
class FlatVectorSerializer : public simdb::UniformSerializer
{
public:
    static constexpr auto NAME = "FlatVectorSerializer";

    FlatVectorSerializer(simdb::AppPipeline& pipeline, PipelineChain& serialization_chain)
        : simdb::UniformSerializer(pipeline, serialization_chain)
    {
    }

    void addStat(const std::string& stat_name)
    {
        stat_names_.push_back(stat_name);
    }

    using simdb::UniformSerializer::process;

    void process(uint64_t tick, const std::vector<double>& data)
    {
        if (data.size() != stat_names_.size())
        {
            throw simdb::DBException("Data size does not match number of statistic names.");
        }

        simdb::VectorSerializer<double> serializer = createVectorSerializer<double>(&data);
        process(tick, std::move(serializer));
    }

private:
    void defineSchema_(simdb::Schema& schema) override
    {
        auto& tbl = schema.addTable("StatisticNames");
        tbl.addColumn("Name", simdb::SqlDataType::string_t);
    }

    void postInit_(int argc, char** argv) override
    {
        for (const auto& stat_name : stat_names_)
        {
            getDatabaseManager()->INSERT(
                SQL_TABLE("StatisticNames"),
                SQL_COLUMNS("Name"),
                SQL_VALUES(stat_name));
        }
    }

    std::string getByteLayoutYAML_() const
    {
        return "NOT YET IMPLEMENTED";
    }

    std::vector<std::string> stat_names_;
};

REGISTER_SIMDB_APPLICATION(FlatVectorSerializer);

int main(int argc, char** argv)
{
    DB_INIT;

    simdb::AppManager app_mgr;
    app_mgr.enableApp(FlatVectorSerializer::NAME);

    simdb::DatabaseManager db_mgr("test.db");

    // Setup...
    app_mgr.createEnabledApps(&db_mgr);

    auto serializer = app_mgr.getApp<FlatVectorSerializer>(&db_mgr);
    serializer->addStat("Temperature");
    serializer->addStat("Pressure");
    serializer->addStat("Humidity");

    app_mgr.createSchemas(&db_mgr);
    app_mgr.postInit(&db_mgr, argc, argv);

    for (uint64_t tick = 0; tick < 100; ++tick)
    {
        // 3 stats: Temperature, Pressure, Humidity
        auto data = generateRandomData(3);
        serializer->process(tick, data);
    }

    // Finish...
    app_mgr.postSim(&db_mgr);
    app_mgr.teardown();
    app_mgr.destroy();

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
