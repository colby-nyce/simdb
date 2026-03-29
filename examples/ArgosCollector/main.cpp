#include "RandUtils.hpp"
#include "SimDBTester.hpp"
#include "simdb/apps/AppManager.hpp"
#include "simdb/apps/argos/Collection.hpp"
#include "simdb/apps/argos/DataTypeHierarchy.hpp"

/// This test shows how to use the SimDB data collection system for Argos.
TEST_INIT;

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

template <>
struct EnumDescriptor<simdb::Colors>
{
    static std::vector<EnumMember> members()
    {
        return {{"WHITE", 0},
                {"RED", 1},
                {"GREEN", 2},
                {"BLUE", 3},
                {"TRANSPARENT", -1}};
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

    class ArgosCollector : public simdb::collection::ArgosCollectorBase<Instruction>
    {
    public:
        ARGOS_COLLECT(type, &Instruction::getType, "Instruction type");
        ARGOS_COLLECT(opcode, &Instruction::getOpcode, "Opcode");
        ARGOS_COLLECT(mnemonic, &Instruction::getMnemonic, "Mnemonic");
        ARGOS_COLLECT(csr, &Instruction::getCsr, "CSR number");
    };
};

class Scalars
{
public:
    Scalars()
    {
        randomize();
    }

    void createCollectables(simdb::collection::Collection<uint64_t>& collection)
    {
        ui16_collector_ = collection.collectScalarWithAutoCollection<uint16_t>(
            "auto.ui16", "root", &ui16_);

        ui32_collector_ = collection.collectScalarWithAutoCollection<uint32_t>(
            "auto.ui32", "root", &ui32_);

        dbl_collector_ = collection.collectScalarWithAutoCollection<double>(
            "auto.dbl",  "root", &dbl_);

        str_collector_ = collection.collectScalarWithAutoCollection<std::string>(
            "auto.str",  "root", &str_);

        flag_collector_ = collection.collectScalarWithAutoCollection<bool>(
            "auto.flag", "root", &flag_);

        color_collector_ = collection.collectScalarWithAutoCollection<simdb::Colors>(
            "auto.color", "root", &color_);
        /*
    class Packet
    {
    private:
        int intval_ = 8;
        std::string strval_ = "hello";
        simdb::Colors color_ = simdb::Colors::RED;

    public:
        int getInt() const { return intval_; }
        const std::string& getString() const { return strval_; }
        simdb::Colors getColor() const { return color_; }

        class ArgosCollector : public simdb::collection::ArgosCollectorBase<Packet>
        {
        public:
            ARGOS_COLLECT(intval, &Packet::getInt, "Signed integer payload sample");
            ARGOS_COLLECT(strval, &Packet::getString, "Short text label carried with the packet");
            ARGOS_COLLECT(color, &Packet::getColor, "Wire enum: simdb::Colors");
        };
    } packet;

    auto auto_packet_collector = collection.collectScalarWithAutoCollection<Packet>(
        "auto.packet", "root", &packet);

    auto manual_packet_collector = collection.collectScalarManually<Packet>(
        "manual.packet", "root");

    using PacketQueue = std::vector<std::shared_ptr<Packet>>;
    PacketQueue packet_queue;

    auto auto_packet_q_collector = collection.collectContainerWithAutoCollection<PacketQueue, false>(
        "auto.packet_q", "root", &packet_queue, 8);

    auto manual_packet_q_collector = collection.collectContainerManually<PacketQueue, false>(
        "manual.packet_q", "root", 8);*/
    }

    void randomize()
    {
        ui16_ = simdb::generateRandomInt<uint16_t>();
        ui32_ = simdb::generateRandomInt<uint32_t>();
        dbl_ = simdb::generateRandomFloat<double>();
        str_ = simdb::generateRandomString();
        flag_ = simdb::generateRandomBool();
        color_ = simdb::generateRandomColor();
    }

private:
    uint16_t ui16_;
    uint32_t ui32_;
    double dbl_;
    std::string str_;
    bool flag_;
    simdb::Colors color_;

    std::shared_ptr<simdb::collection::AutoScalarCollector<uint16_t>> ui16_collector_;
    std::shared_ptr<simdb::collection::AutoScalarCollector<uint32_t>> ui32_collector_;
    std::shared_ptr<simdb::collection::AutoScalarCollector<double>> dbl_collector_;
    std::shared_ptr<simdb::collection::AutoScalarCollector<std::string>> str_collector_;
    std::shared_ptr<simdb::collection::AutoScalarCollector<bool>> flag_collector_;
    std::shared_ptr<simdb::collection::AutoScalarCollector<simdb::Colors>> color_collector_;
};

void TestSimpleScalars()
{
    uint64_t tick = 0;
    simdb::collection::Collection<uint64_t> collection;
    collection.timestampWith(&tick);
    collection.addCollection("root", 1);

    Scalars scalars;
    scalars.createCollectables(collection);

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

    // TODO cnyce

    app_mgrs.postSimLoopTeardown();
}

void TestStructScalars()
{

}

void TestContainers()
{

}

int main()
{
    TestSimpleScalars();
    TestStructScalars();
    TestContainers();
}
