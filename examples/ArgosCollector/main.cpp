#include "RandUtils.hpp"
#include "SimDBTester.hpp"
#include "simdb/apps/AppManager.hpp"
#include "simdb/apps/argos/Collection.hpp"
#include "simdb/apps/argos/DataTypeHierarchy.hpp"

#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

/// This test shows how to use the SimDB data collection system for Argos.
TEST_INIT;

constexpr size_t RUN_TICKS = 1000;

enum InstType
{
    NO_OP,
    MEM,
    CSR,
    ILLEGAL,
    __N
};

// Template specializations
namespace simdb::collection {

template <>
struct EnumDescriptor<InstType>
{
    static std::vector<EnumMember> members()
    {
        return {{"NO_OP", 0},
                {"MEM", 1},
                {"CSR", 2},
                {"ILLEGAL", 3}};
    }
};

} // namespace simdb::collection

class Instruction
{
private:
    InstType type_ = InstType::NO_OP;
    uint64_t opcode_ = 0;
    std::string mnemonic_;
    uint32_t csr_ = 0;
    bool last_inst_ = 0;

    template <typename T>
    void compareAndAdvance_(const char*& theirs, const T& mine, simdb::TinyStrings<>* tiny_strings) const
    {
        if constexpr (std::is_enum_v<T>)
        {
            using int_type = std::underlying_type_t<T>;
            if (*reinterpret_cast<const int_type*>(theirs) != mine)
            {
                throw simdb::DBException("Value mismatch");
            }
            theirs += sizeof(int_type);
        }
        else if constexpr (std::is_same_v<T, bool>)
        {
            using int_type = uint8_t;
            if (*reinterpret_cast<const int_type*>(theirs) != mine)
            {
                throw simdb::DBException("Value mismatch");
            }
            theirs += sizeof(int_type);
        }
        else if constexpr (std::is_same_v<T, std::string>)
        {
            using int_type = uint32_t;
            if (*reinterpret_cast<const int_type*>(theirs) != tiny_strings->getStringID(mine))
            {
                throw simdb::DBException("Value mismatch");
            }
            theirs += sizeof(int_type);
        }
        else if constexpr (std::is_trivial_v<T> && std::is_standard_layout_v<T>)
        {
            if (*reinterpret_cast<const T*>(theirs) != mine)
            {
                throw simdb::DBException("Value mismatch");
            }
            theirs += sizeof(T);
        }
        else
        {
            static_assert(false);
        }
    }

public:
    InstType getType() const { return type_; }
    uint64_t getOpcode() const { return opcode_; }
    const std::string& getMnemonic() const { return mnemonic_; }
    uint32_t getCsr() const { return csr_; }
    bool finishesSim() const { return last_inst_; }

    Instruction(InstType type, uint64_t opcode, const std::string& mnemonic, uint32_t csr = 0, bool last_inst = false)
        : type_(type)
        , opcode_(opcode)
        , mnemonic_(mnemonic)
        , csr_(csr)
        , last_inst_(last_inst)
    {}

    Instruction(const Instruction&) = default;

    static std::shared_ptr<Instruction> genRandom()
    {
        auto type = static_cast<InstType>(rand() % InstType::__N);
        auto opcode = rand();

        static const char* mnemonics[] = {
            "add", "addi", "li", "b", "jlr"
        };
        auto mnemonic = mnemonics[rand() % 5];
        auto csr = type == InstType::CSR ? rand() % 256 : 0;
        auto last_inst = rand() % 1000 == 500;
        return std::make_shared<Instruction>(type, opcode, mnemonic, csr, last_inst);
    }

    static size_t getFixedNumBytes()
    {
        size_t bytes = 0;

        //InstType type_ = InstType::NO_OP;
        bytes += sizeof(std::underlying_type_t<InstType>);

        //uint64_t opcode_ = 0;
        bytes += sizeof(uint64_t);

        //std::string mnemonic_;
        //  --> strings as uint32_t in the DB (TinyStrings)
        bytes += sizeof(uint32_t);

        //uint32_t csr_ = 0;
        bytes += sizeof(uint32_t);

        //bool last_inst_ = 0;
        //  --> bools as uint8_t in the DB
        bytes += sizeof(uint8_t);

        return bytes;
    }

    class ArgosCollector : public simdb::collection::ArgosCollectorBase<Instruction>
    {
    public:
        ARGOS_COLLECT(type, &Instruction::getType, "Instruction type");
        ARGOS_COLLECT(opcode, &Instruction::getOpcode, "Opcode");
        ARGOS_COLLECT(mnemonic, &Instruction::getMnemonic, "Mnemonic");
        ARGOS_COLLECT(csr, &Instruction::getCsr, "CSR number");
        ARGOS_COLLECT(last, &Instruction::finishesSim, "Last instruction");
    };

    void compare(const char*& bytes, simdb::TinyStrings<>* tiny_strings) const
    {
        compareAndAdvance_(bytes, type_, tiny_strings);
        compareAndAdvance_(bytes, opcode_, tiny_strings);
        compareAndAdvance_(bytes, mnemonic_, tiny_strings);
        compareAndAdvance_(bytes, csr_, tiny_strings);
        compareAndAdvance_(bytes, last_inst_, tiny_strings);
    }
};

/// Row of scalar/struct values written by \ref TestScalarCollection (JSON must match Python deserializers).
struct AllData
{
    uint32_t pod;
    std::string str;
    InstType itype;
    bool flag;
    std::shared_ptr<Instruction> inst;
};

// Final validation struction
using ExpectedInsts =
    std::map<uint64_t,          // Tick
             std::map<uint16_t, // CID
                      std::shared_ptr<Instruction>>>;

const char* instTypeToString(InstType t)
{
    switch (t)
    {
    case InstType::NO_OP:
        return "NO_OP";
    case InstType::MEM:
        return "MEM";
    case InstType::CSR:
        return "CSR";
    case InstType::ILLEGAL:
        return "ILLEGAL";
    default:
        return "UNKNOWN";
    }
}

void RunFrontEndValidation(simdb::DatabaseManager* db_mgr, const std::string& truth = "/tmp/collection.json")
{
    const auto db_path = db_mgr->getDatabaseFilePath();
    std::ostringstream cmd;
    cmd << "python3 ./validate.py --db-file " << std::quoted(db_path)
        << " --json-file " << std::quoted(truth);
    const auto rc = std::system(cmd.str().c_str());
    EXPECT_EQUAL(rc, 0);
}

void RunSmokeTest()
{
    uint64_t tick = 0;
    size_t heartbeat = 3;
    simdb::collection::Collection<uint64_t> collection(heartbeat);
    collection.timestampWith(&tick);
    collection.addCollection("root", 1);

    auto inst_collector_1 = collection.collectScalarManually<Instruction>(
        "inst1", "root");

    auto inst_collector_2 = collection.collectScalarManually<Instruction>(
        "inst2", "root");

    auto inst_collector_3 = collection.collectScalarManually<Instruction>(
        "inst3", "root");

    simdb::AppManagers app_mgrs;
    app_mgrs.registerApp<simdb::collection::CollectionPipeline>();

    auto& app_mgr = app_mgrs.createAppManager("test.db");
    app_mgr.enableApp<simdb::collection::CollectionPipeline>();

    app_mgr.parameterizeAppFactory<simdb::collection::CollectionPipeline>(&collection);
    app_mgrs.createEnabledApps();
    app_mgrs.createSchemas();
    app_mgrs.postInit(0, nullptr);
    app_mgrs.initializePipelines();
    app_mgrs.openPipelines();

    // Collect both insts at tick 1
    tick = 1;
    auto A = Instruction::genRandom();
    auto B = Instruction::genRandom();
    inst_collector_1->collect(A);
    inst_collector_2->collect(B);

    // Only collect inst1 at ticks 2-5
    auto C = Instruction::genRandom();
    auto D = Instruction::genRandom();
    auto E = Instruction::genRandom();
    auto F = Instruction::genRandom();

    tick = 2;
    inst_collector_1->collect(C);

    tick = 3;
    inst_collector_1->collect(D);

    tick = 4;
    inst_collector_1->collect(E);

    tick = 5;
    inst_collector_1->collect(F);

    // Collect both insts at tick 6 (notice we call collect()
    // in "reverse" order as we usually do when both are collected;
    // we do this to ensure that collection is not paying attention
    // to what is collected in what order)
    //
    // We also need to be resilient to multiple collect() calls
    // for the same collectable at the same time point. The DB
    // should only take the last of these values. To test this,
    // we will first inject throwaway instructions before collecting
    // the "real" instructions we wish to test.
    tick = 6;
    inst_collector_1->collect(Instruction::genRandom());
    inst_collector_1->collect(Instruction::genRandom());
    inst_collector_2->collect(Instruction::genRandom());

    auto G = Instruction::genRandom();
    auto H = Instruction::genRandom();
    inst_collector_2->collect(H);
    inst_collector_1->collect(G);

    // Collect the same value for inst1 at tick7, and collect
    // a different value for inst2
    tick = 7;
    auto I = Instruction::genRandom();
    inst_collector_1->collect(G);
    inst_collector_2->collect(I);

    // Only collect inst1 at ticks 8-12, but use the same
    // collected Instruction every time
    for (tick = 8; tick <= 12; ++tick)
    {
        inst_collector_1->collect(G);
    }

    // Write a different value for the 1st inst at tick 13
    auto J = Instruction::genRandom();
    inst_collector_1->collect(J);

    // Negative test: verify we cannot go back in time
    auto restore_tick = tick;
    tick = 3;
    EXPECT_THROW(inst_collector_1->collect(Instruction::genRandom()));
    tick = restore_tick;

    // Ensure there are no problems collecting something for
    // the first time once things are already running
    tick = 14;
    auto K = Instruction::genRandom();
    inst_collector_3->collect(K);

    app_mgrs.postSimLoopTeardown();

    std::map<uint64_t, std::vector<std::shared_ptr<Instruction>>> expected_db_insts = {
                            //   Arrived      Refreshed
        {1,  {A,B}},        //   A,B
        {2,  {C}},          //   C
        {3,  {D}},          //   D
        {4,  {E,B}},        //   E            B
        {5,  {F}},          //   F
        {6,  {G,H}},        //   G,H
        {7,  {I}},          //   I
        {8,  {}},           //
        {9,  {G}},          //                G
        {10, {I}},          //                I
        {11, {}},           //
        {12, {G}},          //                G
        {13, {J,I}},        //   J            I
        {14, {K}}           //   K
    };

    std::map<const Instruction*, uint16_t> collectable_ids_for_insts = {
        {A.get(), inst_collector_1->getID()},
        {B.get(), inst_collector_2->getID()},
        {C.get(), inst_collector_1->getID()},
        {D.get(), inst_collector_1->getID()},
        {E.get(), inst_collector_1->getID()},
        {F.get(), inst_collector_1->getID()},
        {G.get(), inst_collector_1->getID()},
        {H.get(), inst_collector_2->getID()},
        {I.get(), inst_collector_2->getID()},
        {J.get(), inst_collector_1->getID()},
        {K.get(), inst_collector_3->getID()}
    };

    auto db_mgr = app_mgr.getDatabaseManager();
    simdb::TinyStrings<> tiny_strings(db_mgr);

    auto validate_collection_at_time = [&](int timestamp_id, uint64_t tick)
    {
        auto query = db_mgr->createQuery("CollectionRecords");
        query->addConstraintForInt("TimestampID", simdb::Constraints::EQUAL, timestamp_id);

        const auto& expected_insts = expected_db_insts.at(tick);
        if (expected_insts.empty())
        {
            EXPECT_EQUAL(query->count(), 0);
            return;
        }
        EXPECT_EQUAL(query->count(), 1);

        std::vector<char> compressed_collection_blob;
        query->select("Records", compressed_collection_blob);

        auto results = query->getResultSet();
        EXPECT_TRUE(results.getNextRecord());

        // Validate the number of bytes
        std::vector<char> collection_blob;
        simdb::decompressData(compressed_collection_blob, collection_blob);
        auto num_bytes = Instruction::getFixedNumBytes() + sizeof(uint16_t);
        auto expected_bytes = num_bytes * expected_insts.size();
        auto actual_bytes = collection_blob.size();
        EXPECT_EQUAL(expected_bytes, actual_bytes);

        // Validate the collected Instruction bytes
        std::map<uint16_t, std::shared_ptr<Instruction>> expected_cid_insts;
        for (auto expected_inst : expected_insts)
        {
            uint16_t cid = collectable_ids_for_insts.at(expected_inst.get());
            expected_cid_insts[cid] = expected_inst;
        }

        const char* ptr = collection_blob.data();
        auto end = ptr + collection_blob.size();
        while (ptr != end)
        {
            // First read off the uint16_t cid
            uint16_t cid = *reinterpret_cast<const uint16_t*>(ptr);
            ptr += sizeof(uint16_t);

            // Get a ref to the expected inst
            const auto& expected_inst = expected_cid_insts.at(cid);

            // Create a vector of bytes which holds the Instruction bytes
            expected_inst->compare(ptr, &tiny_strings);
        }
    };

    auto query = db_mgr->createQuery("Timestamps");

    int timestamp_id;
    query->select("Id", timestamp_id);

    uint64_t timestamp_tick;
    query->select("Timestamp", timestamp_tick);

    auto timestamp_results = query->getResultSet();
    while (timestamp_results.getNextRecord())
    {
        validate_collection_at_time(timestamp_id, timestamp_tick);
    }
}

void TestScalarCollection()
{
    uint64_t tick = 0;
    size_t heartbeat = 3;
    simdb::collection::Collection<uint64_t> collection(heartbeat);
    collection.timestampWith(&tick);
    collection.addCollection("root", 1);

    // Collect a variety of scalars:
    //   - PODs
    //   - strings
    //   - enums
    //   - bools
    //   - structs

    auto pod_collector = collection.collectScalarManually<uint32_t>(
        "pod", "root");

    auto str_collector = collection.collectScalarManually<std::string>(
        "str", "root");

    auto enum_collector = collection.collectScalarManually<InstType>(
        "itype", "root");

    auto bool_collector = collection.collectScalarManually<bool>(
        "flag", "root");

    auto inst_collector = collection.collectScalarManually<Instruction>(
        "inst", "root");

    simdb::AppManagers app_mgrs;
    app_mgrs.registerApp<simdb::collection::CollectionPipeline>();

    auto& app_mgr = app_mgrs.createAppManager("test.db");
    app_mgr.enableApp<simdb::collection::CollectionPipeline>();

    app_mgr.parameterizeAppFactory<simdb::collection::CollectionPipeline>(&collection);
    app_mgrs.createEnabledApps();
    app_mgrs.createSchemas();
    app_mgrs.postInit(0, nullptr);
    app_mgrs.initializePipelines();
    app_mgrs.openPipelines();

    std::vector<AllData> all_data = {
        {4u, "foo", InstType::MEM,     true,  Instruction::genRandom()},
        {5u, "bar", InstType::NO_OP,   false, Instruction::genRandom()},
        {6u, "fiz", InstType::MEM,     true,  Instruction::genRandom()},
        {7u, "biz", InstType::ILLEGAL, false, Instruction::genRandom()},
        {8u, "fuz", InstType::ILLEGAL, true,  Instruction::genRandom()},
        {9u, "buz", InstType::CSR,     false, Instruction::genRandom()}
    };

    for (tick = 1; tick <= all_data.size(); ++tick)
    {
        auto idx = tick - 1;
        pod_collector->collect(all_data[idx].pod);
        str_collector->collect(all_data[idx].str);
        enum_collector->collect(all_data[idx].itype);
        bool_collector->collect(all_data[idx].flag);
        inst_collector->collect(all_data[idx].inst);
    }

    app_mgrs.postSimLoopTeardown();

    // TODO cnyce
    // RunFrontEndValidation(app_mgr.getDatabaseManager(), "/tmp/scalar_collection.json");
}

void TestEnabledLogic()
{
    uint64_t tick = 0;
    size_t heartbeat = 3;
    simdb::collection::Collection<uint64_t> collection(heartbeat);
    collection.timestampWith(&tick);
    collection.addCollection("root", 1);

    auto inst_collector = collection.collectScalarManually<Instruction>(
        "inst", "root");

    simdb::AppManagers app_mgrs;
    app_mgrs.registerApp<simdb::collection::CollectionPipeline>();

    auto& app_mgr = app_mgrs.createAppManager("test.db");
    app_mgr.enableApp<simdb::collection::CollectionPipeline>();

    app_mgr.parameterizeAppFactory<simdb::collection::CollectionPipeline>(&collection);
    app_mgrs.createEnabledApps();
    app_mgrs.createSchemas();
    app_mgrs.postInit(0, nullptr);
    app_mgrs.initializePipelines();
    app_mgrs.openPipelines();

    // Keep track of all expected Instructions and their ticks
    ExpectedInsts expected_insts;

    // Collect data from ticks 1-5 while collectable is enabled
    std::shared_ptr<Instruction> I;
    for (tick = 1; tick <= 5; ++tick)
    {
        I = Instruction::genRandom();
        inst_collector->collect(I);
        expected_insts[tick] = {{inst_collector->getID(), I}};
    }

    // Disable and collect a few times (no-op)
    tick = 6;
    inst_collector->disable();

    tick = 7;
    inst_collector->collect(Instruction::genRandom());

    tick = 8;
    inst_collector->collect(Instruction::genRandom());

    // Re-enable at tick 9, but do not collect anything.
    // There should not be anything in the database at
    // this tick; we last saw a value at tick 5 before
    // disabling collection, and the collector should
    // know NOT to put the previously seen bytes back
    // in the pipeline.
    tick = 9;
    inst_collector->enable();

    // Collect ticks 10 and 11
    for (tick = 10; tick <= 11; ++tick)
    {
        I = Instruction::genRandom();
        inst_collector->collect(I);
        expected_insts[tick] = {{inst_collector->getID(), I}};
    }

    app_mgrs.postSimLoopTeardown();

    // TODO cnyce
    // RunFrontEndValidation(app_mgr.getDatabaseManager());
}

int main()
{
    RunSmokeTest();
    TestScalarCollection();
    TestEnabledLogic();

    // TODO cnyce: Bash all kinds of scalars, and sparse/contig containers.
    // TODO cnyce: Dedicated test for all the edge cases for disable/enable.

    REPORT_ERROR;
    return ERROR_CODE;
}
