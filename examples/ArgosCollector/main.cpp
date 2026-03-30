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

    template <typename T>
    void compareAndAdvance_(const char*& theirs, const T& mine, simdb::TinyStrings<>* tiny_strings)
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

    class ArgosCollector : public simdb::collection::ArgosCollectorBase<Instruction>
    {
    public:
        ARGOS_COLLECT(type, &Instruction::getType, "Instruction type");
        ARGOS_COLLECT(opcode, &Instruction::getOpcode, "Opcode");
        ARGOS_COLLECT(mnemonic, &Instruction::getMnemonic, "Mnemonic");
        ARGOS_COLLECT(csr, &Instruction::getCsr, "CSR number");
        ARGOS_COLLECT(last, &Instruction::finishesSim, "Last instruction");
    };

    void compare(const char*& bytes, simdb::TinyStrings<>* tiny_strings)
    {
        compareAndAdvance_(bytes, type_, tiny_strings);
        compareAndAdvance_(bytes, opcode_, tiny_strings);
        compareAndAdvance_(bytes, mnemonic_, tiny_strings);
        compareAndAdvance_(bytes, csr_, tiny_strings);
        compareAndAdvance_(bytes, last_inst_, tiny_strings);
    }
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
            "ui16",  "root", &ui16_);

        ui32_collector_ = collection.collectScalarWithAutoCollection<uint32_t>(
            "ui32",  "root", &ui32_);

        dbl_collector_ = collection.collectScalarWithAutoCollection<double>(
            "dbl",   "root", &dbl_);

        str_collector_ = collection.collectScalarWithAutoCollection<std::string>(
            "str",   "root", &str_);

        flag_collector_ = collection.collectScalarWithAutoCollection<bool>(
            "flag",  "root", &flag_);

        color_collector_ = collection.collectScalarWithAutoCollection<simdb::Colors>(
            "color", "root", &color_);

        inst_collector_ = collection.collectScalarWithAutoCollection<std::shared_ptr<Instruction>>(
            "inst",  "root", &inst_);
    }

    void randomize()
    {
        ui16_ = simdb::generateRandomInt<uint16_t>();
        ui32_ = simdb::generateRandomInt<uint32_t>();
        dbl_ = simdb::generateRandomFloat<double>();
        str_ = simdb::generateRandomString();
        flag_ = simdb::generateRandomBool();
        color_ = simdb::generateRandomColor();
        inst_ = Instruction::genRandom();
    }

    class ValidatorBase
    {
    public:
        virtual ~ValidatorBase() = default;
        virtual size_t requiredBytes() const = 0;
        virtual void validate(simdb::TinyStrings<>* tiny_strings, const char*& bytes) = 0;
    };

    template <typename T>
    class Validator : public ValidatorBase
    {
    public:
        Validator(const T& value) : value_(value) {}

        size_t requiredBytes() const override
        {
            if constexpr (std::is_enum_v<T>)
            {
                using int_type = std::underlying_type_t<T>;
                return sizeof(int_type);
            }
            else if constexpr (std::is_same_v<T, bool>)
            {
                return sizeof(uint8_t);
            }
            else if constexpr (std::is_same_v<T, std::string>)
            {
                return sizeof(uint32_t);
            }
            else if constexpr (std::is_trivial_v<T> && std::is_standard_layout_v<T>)
            {
                return sizeof(T);
            }
            else
            {
                using value_type = simdb::type_traits::remove_any_pointer_t<T>;
                typename value_type::ArgosCollector collector;
                size_t num_bytes = 0;
                for (const auto field : collector.getFields())
                {
                    num_bytes += field->requiredBytes();
                }
                return num_bytes;
            }
        }

        void validate(simdb::TinyStrings<>* tiny_strings, const char*& bytes) override
        {
            if constexpr (std::is_enum_v<T>)
            {
                using int_type = std::underlying_type_t<T>;
                const auto mine = static_cast<int_type>(value_);
                const auto theirs = *reinterpret_cast<const int_type*>(bytes);
                if (mine != theirs)
                {
                    throw simdb::DBException("Value mismatch");
                }
                bytes += sizeof(int_type);
            }
            else if constexpr (std::is_same_v<T, bool>)
            {
                const bool& mine = value_;
                const bool theirs = *reinterpret_cast<const uint8_t*>(bytes);
                if (mine != theirs)
                {
                    throw simdb::DBException("Value mismatch");
                }
                bytes += sizeof(uint8_t);
            }
            else if constexpr (std::is_same_v<T, std::string>)
            {
                using encoding_t = uint32_t;
                const auto mine = tiny_strings->getStringID(value_);
                const auto theirs = *reinterpret_cast<const encoding_t*>(bytes);
                if (mine != theirs)
                {
                    throw simdb::DBException("Value mismatch");
                }
                bytes += sizeof(encoding_t);
            }
            else if constexpr (std::is_trivial_v<T> && std::is_standard_layout_v<T>)
            {
                const auto& mine = value_;
                const auto theirs = *reinterpret_cast<const T*>(bytes);
                if (mine != theirs)
                {
                    throw simdb::DBException("Value mismatch");
                }
                bytes += sizeof(T);
            }
            else
            {
                value_.compare(bytes, tiny_strings);
            }
        }

    private:
        T value_;
    };

    std::map<uint16_t, std::shared_ptr<ValidatorBase>> snapshot() const
    {
        return {
            {ui16_collector_->getID(),  std::make_shared<Validator<uint16_t>>(ui16_)},
            {ui32_collector_->getID(),  std::make_shared<Validator<uint32_t>>(ui32_)},
            {dbl_collector_->getID(),   std::make_shared<Validator<double>>(dbl_)},
            {str_collector_->getID(),   std::make_shared<Validator<std::string>>(str_)},
            {flag_collector_->getID(),  std::make_shared<Validator<bool>>(flag_)},
            {color_collector_->getID(), std::make_shared<Validator<simdb::Colors>>(color_)},
            {inst_collector_->getID(),  std::make_shared<Validator<Instruction>>(*inst_)}
        };
    }

private:
    uint16_t ui16_;
    uint32_t ui32_;
    double dbl_;
    std::string str_;
    bool flag_;
    simdb::Colors color_;
    std::shared_ptr<Instruction> inst_;

    std::shared_ptr<simdb::collection::AutoScalarCollector<uint16_t>> ui16_collector_;
    std::shared_ptr<simdb::collection::AutoScalarCollector<uint32_t>> ui32_collector_;
    std::shared_ptr<simdb::collection::AutoScalarCollector<double>> dbl_collector_;
    std::shared_ptr<simdb::collection::AutoScalarCollector<std::string>> str_collector_;
    std::shared_ptr<simdb::collection::AutoScalarCollector<bool>> flag_collector_;
    std::shared_ptr<simdb::collection::AutoScalarCollector<simdb::Colors>> color_collector_;
    std::shared_ptr<simdb::collection::AutoScalarCollector<std::shared_ptr<Instruction>>> inst_collector_;
};

void TestAutoCollectScalars()
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

    std::map<uint64_t,       // Tick
        std::map<uint16_t,   // CID
            std::shared_ptr<Scalars::ValidatorBase>>> state_validators;

    while (tick < 1000)
    {
        ++tick;
        scalars.randomize();
        state_validators[tick] = scalars.snapshot();
        collection.performAutoCollection("root");
    }
    app_mgrs.postSimLoopTeardown();

    auto db_mgr = app_mgr.getDatabaseManager();

    simdb::TinyStrings<> tiny_strings(db_mgr);

    auto time_query = db_mgr->createQuery("Timestamps");
    EXPECT_EQUAL(time_query->count(), 1000);

    int time_id;
    time_query->select("Id", time_id);

    uint64_t expected_tick = 1;
    uint64_t actual_tick;
    time_query->select("Timestamp", actual_tick);

    auto get_cid = [](const char*& begin, const char* end) -> uint16_t
    {
        if (begin + sizeof(uint16_t) >= end)
        {
            throw simdb::DBException("Could not extract collectable ID");
        }
        const auto cid = *reinterpret_cast<const uint16_t*>(begin);
        begin += sizeof(uint16_t);
        return cid;
    };

    auto get_validator = [&](uint64_t tick, uint16_t cid)
    {
        auto& tick_validators = state_validators[tick];
        auto& validator = tick_validators[cid];
        if (!validator)
        {
            throw simdb::DBException("Could not get validator");
        }
        return validator.get();
    };

    auto validate_collection = [&](int timestamp_id)
    {
        EXPECT_EQUAL(expected_tick++, actual_tick);

        auto query = db_mgr->createQuery("CollectionRecords");
        query->addConstraintForInt("TimestampID", simdb::Constraints::EQUAL, timestamp_id);

        std::vector<char> bytes;
        query->select("Records", bytes);

        auto results = query->getResultSet();
        EXPECT_TRUE(results.getNextRecord());
        EXPECT_FALSE(results.getNextRecord());

        if (bytes.empty())
        {
            throw simdb::DBException("Could not find CollectionRecords row");
        }

        std::vector<char> uncompressed;
        simdb::decompressData(bytes, uncompressed);

        const char* front = uncompressed.data();
        const char* end = front + uncompressed.size();
        while (front != end)
        {
            auto cid = get_cid(front, end);
            auto validator = get_validator(actual_tick, cid);
            if (front + validator->requiredBytes() > end)
            {
                throw simdb::DBException("Reached end of byte buffer early");
            }
            validator->validate(&tiny_strings, front);
        }
    };

    auto time_results = time_query->getResultSet();
    while (time_results.getNextRecord())
    {
        validate_collection(time_id);
    }
}

class Containers
{
public:
    Containers(size_t capacity = 32)
        : capacity_(capacity)
    {
        randomize();
    }

    using InstPtr = std::shared_ptr<Instruction>;
    using InstQueue = std::vector<InstPtr>;
    using BoolQueue = std::vector<bool>;
    using StringQueue = std::vector<std::string>;
    using EnumQueue = std::vector<simdb::Colors>;

    void createCollectables(simdb::collection::Collection<uint64_t>& collection)
    {
        inst_q_collector_ = collection.collectContainerWithAutoCollection<InstQueue, false>(
            "inst_q", "root", &inst_queue_, capacity_);

        sparse_inst_q_collector_ = collection.collectContainerWithAutoCollection<InstQueue, true>(
            "sparse_inst_q", "root", &sparse_inst_queue_, capacity_);
    }

    void randomize()
    {
        inst_queue_.clear();
        auto num_insts = rand() % capacity_ + 1;
        while (inst_queue_.size() < num_insts)
        {
            inst_queue_.push_back(Instruction::genRandom());
        }

        sparse_inst_queue_.clear();
        sparse_inst_queue_.resize(capacity_);
        for (size_t idx = 0; idx < capacity_; ++idx)
        {
            if (rand() % 10 == 5)
            {
                sparse_inst_queue_[idx] = Instruction::genRandom();
            }
        }

        flag_queue_.clear();
        auto num_flags = rand() % capacity_ + 1;
        while (flag_queue_.size() < num_flags)
        {
            flag_queue_.push_back(rand() % 2 == 0);
        }

        string_queue_.clear();
        auto num_strings = rand() % capacity_ + 1;
        while (string_queue_.size() < num_strings)
        {
            const auto min_chars = 4;
            const auto max_chars = min_chars;
            string_queue_.push_back(simdb::generateRandomString(min_chars, max_chars));
        }

        enum_queue_.clear();
        auto num_enums = rand() % capacity_ + 1;
        while (enum_queue_.size() < num_enums)
        {
            enum_queue_.push_back(simdb::generateRandomColor());
        }
    }

private:
    const size_t capacity_;

    InstQueue inst_queue_;
    InstQueue sparse_inst_queue_;
    BoolQueue flag_queue_;
    StringQueue string_queue_;
    EnumQueue enum_queue_;

    std::shared_ptr<simdb::collection::AutoContainerCollector<InstQueue, false>> inst_q_collector_;
    std::shared_ptr<simdb::collection::AutoContainerCollector<InstQueue, true>> sparse_inst_q_collector_;
    std::shared_ptr<simdb::collection::AutoContainerCollector<BoolQueue, false>> flag_q_collector_;
    std::shared_ptr<simdb::collection::AutoContainerCollector<StringQueue, false>> string_q_collector_;
    std::shared_ptr<simdb::collection::AutoContainerCollector<EnumQueue, false>> enum_q_collector_;
};

void TestAutoCollectContainers()
{
    uint64_t tick = 0;
    simdb::collection::Collection<uint64_t> collection;
    collection.timestampWith(&tick);
    collection.addCollection("root", 1);

    Containers containers;
    containers.createCollectables(collection);

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

    while (tick < 1000)
    {
        ++tick;
        containers.randomize();
        collection.performAutoCollection("root");
    }

    app_mgrs.postSimLoopTeardown();
}

int main()
{
    TestAutoCollectScalars();
    //TestAutoCollectContainers();
}
