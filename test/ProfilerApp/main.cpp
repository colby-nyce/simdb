#include "simdb/apps/AppRegistration.hpp"
#include "simdb/apps/UniformSerializer.hpp"
#include "simdb/test/SimDBTester.hpp"


using simdb::PipelineEntry;

struct ProfileData {
    uint64_t inst_pc = 0;
    uint64_t uarch_id = 0;
    uint64_t retire_cycles_cost = 0;
};

// Generate random data
// ProfileData inevitably is contained in a vector of data
ProfileData generateRandomData ()
{
    std::uniform_int_distribution<uint64_t> dis (1, 10000);

    // Creates a random number generator seeded with truly random
    // values 
    std::mt19937 gen(std::random_device{}());

    // Sending a singular ProfileData instance
    return {dis(gen), dis(gen), dis(gen)};
}

class ProfileDataSerializer : public simdb::UniformSerializer {
public:
    static constexpr auto NAME = "ProfileDataSerializer";

    ProfileDataSerializer() = default;

    void configPipeline(simdb::PipelineConfig& config) override
    {   
        // Stage 1: Compress data
        // this is either dynamic or dependent on user entry
        config.asyncStage(1) >> CompressEntry;
        // Stage 2: Commit Entry
        // Adds data according to the AppID, the tick of commit,
        // the type of data, and if it is compressed
        config.asyncStage(2) >> CommitEntry;
    };

    // Defining a process which could possibly be overloaded
    void process(uint64_t tick, const ProfileData& data)
    {
        std::vector<char> buf;
        buf.resize(sizeof(ProfileData));
        
        std::memcpy(buf.data(), &data, sizeof(ProfileData));

        // Create a pipeline entry for the data
        simdb::PipelineEntry entry = prepareEntry(tick, std::move(buf));
        processEntry(std::move(entry));
    };
private:
    void defineSchema_(simdb::Schema& schema) override
    {
        auto& tbl = schema.addTable("ProfileData");
        tbl.addColumn("Data", simdb::SqlDataType::string_t);
    };

    void postInit_(int argc, char **argv) override
    {
        // Nothing to do here?
    };

    std::string getByteLayoutYAML_() const
    {
        return "NOT YET IMPLEMENTED";
    }
};

REGISTER_SIMDB_APPLICATION(ProfileDataSerializer);

int main(int argc, char** argv)
{
    DB_INIT;

    simdb::AppManager app_mgr;
    app_mgr.enableApp(ProfileDataSerializer::NAME);

    simdb::DatabaseManager db_mgr("profile_data_test.db");

    // Setup
    app_mgr.createEnabledApps(&db_mgr);

    auto serializer = app_mgr.getApp<ProfileDataSerializer>(&db_mgr);

    app_mgr.createSchemas(&db_mgr);

    for (uint64_t tick=0; tick<1000; ++tick)
    {
        auto profile_data = generateRandomData();
        serializer->process(tick, profile_data);
    }

    // Finish
    app_mgr.postSim(&db_mgr);
    app_mgr.teardown();
    app_mgr.destroy();

    REPORT_ERROR;
    return ERROR_CODE;
}