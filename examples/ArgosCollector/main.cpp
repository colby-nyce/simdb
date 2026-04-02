#include "RandUtils.hpp"
#include "SimDBTester.hpp"
#include "simdb/apps/AppManager.hpp"
#include "simdb/apps/argos/Collection.hpp"
#include "simdb/apps/argos/DataTypeHierarchy.hpp"
#include <iostream>
#include <optional>

/// This test shows how to use the SimDB data collection system for Argos.
TEST_INIT;

class ValidatorBase
{
public:
    virtual ~ValidatorBase() = default;
    virtual size_t requiredBytes() const = 0;
    virtual void validate(simdb::TinyStrings<>* tiny_strings, const char*& bytes) const = 0;

protected:
    template <typename EnumT>
    void validateEnum_(const EnumT expected, const char*& bytes) const
    {
        static_assert(std::is_enum_v<EnumT>);
        using int_type = std::underlying_type_t<EnumT>;
        const auto ours = static_cast<int_type>(expected);
        const auto theirs = *reinterpret_cast<const int_type*>(bytes);
        if (theirs != ours)
        {
            throw simdb::DBException("Value mismatch");
        }
        bytes += sizeof(int_type);
    }

    void validateBool_(const bool expected, const char*& bytes) const
    {
        const auto ours = static_cast<uint8_t>(expected);
        const auto theirs = *reinterpret_cast<const uint8_t*>(bytes);
        if (theirs != ours)
        {
            throw simdb::DBException("Value mismatch");
        }
        bytes += sizeof(uint8_t);
    }

    void validateString_(const std::string& expected, const char*& bytes, simdb::TinyStrings<>* tiny_strings) const
    {
        using encoding_t = uint32_t;
        const auto ours = tiny_strings->getStringID(expected);
        const auto theirs = *reinterpret_cast<const encoding_t*>(bytes);
        if (theirs != ours)
        {
            throw simdb::DBException("Value mismatch");
        }
        bytes += sizeof(encoding_t);
    }

    template <typename SimpleType>
    void validateSimple_(const SimpleType expected, const char*& bytes) const
    {
        static_assert(std::is_trivial_v<SimpleType> && std::is_standard_layout_v<SimpleType>);
        const auto ours = expected;
        const auto theirs = *reinterpret_cast<const SimpleType*>(bytes);
        if (theirs != ours)
        {
            throw simdb::DBException("Value mismatch");
        }
        bytes += sizeof(SimpleType);
    }
};

using CollectionSnapshot =
    std::map<uint16_t,  // CID
        std::shared_ptr<ValidatorBase>>;

using CollectionSnapshots =
    std::map<uint64_t, // Tick
       CollectionSnapshot>;

constexpr size_t RUN_TICKS = 1000;

void ValidateCollectionInDatabase(
    simdb::DatabaseManager* db_mgr,
    const CollectionSnapshots& snapshots);

template <typename T>
struct scalar_num_bytes
{
    static size_t value()
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
};

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

void SmokeTest()
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

    // Collect both insts at tick 6
    tick = 6;
    auto G = Instruction::genRandom();
    auto H = Instruction::genRandom();
    inst_collector_1->collect(G);
    inst_collector_2->collect(H);

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

    // Write one last different value for the 1st inst
    // (the tick is 13)
    auto J = Instruction::genRandom();
    inst_collector_1->collect(J);

    // TODO cnyce: sendCollectedDataToPipeline() needs to get called
    // automatically from preTeardown()
    collection.sendCollectedDataToPipeline("root");
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
        {13, {J,I}}         //   J            I
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
        {J.get(), inst_collector_1->getID()}
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

    // TODO cnyce: incorporate enable/disable (especially auto collected, but
    // see now it manifests in manually collected too)
}

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

    template <typename T>
    class Validator : public ValidatorBase
    {
    public:
        Validator(const T& value) : value_(value) {}

        size_t requiredBytes() const override
        {
            return scalar_num_bytes<T>::value();
        }

        void validate(simdb::TinyStrings<>* tiny_strings, const char*& bytes) const override
        {
            if constexpr (std::is_enum_v<T>)
            {
                validateEnum_<T>(value_, bytes);
            }
            else if constexpr (std::is_same_v<T, bool>)
            {
                validateBool_(value_, bytes);
            }
            else if constexpr (std::is_same_v<T, std::string>)
            {
                validateString_(value_, bytes, tiny_strings);
            }
            else if constexpr (std::is_trivial_v<T> && std::is_standard_layout_v<T>)
            {
                validateSimple_<T>(value_, bytes);
            }
            else
            {
                value_.compare(bytes, tiny_strings);
            }
        }

    private:
        T value_;
    };

    CollectionSnapshot snapshot() const
    {
        if (!auto_enabled_)
        {
            return {};
        }

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

    void disableCollection()
    {
        auto_enabled_ = false;
        ui16_collector_->disable();
        ui32_collector_->disable();
        dbl_collector_->disable();
        str_collector_->disable();
        flag_collector_->disable();
        color_collector_->disable();
        inst_collector_->disable();
    }

    void enableCollection()
    {
        auto_enabled_ = true;
        ui16_collector_->enable();
        ui32_collector_->enable();
        dbl_collector_->enable();
        str_collector_->enable();
        flag_collector_->enable();
        color_collector_->enable();
        inst_collector_->enable();
    }

private:
    uint16_t ui16_;
    uint32_t ui32_;
    double dbl_;
    std::string str_;
    bool flag_;
    simdb::Colors color_;
    std::shared_ptr<Instruction> inst_;
    bool auto_enabled_ = true;

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

    CollectionSnapshots snapshots;
    while (tick < RUN_TICKS)
    {
        ++tick;
        scalars.randomize();
        auto snapshot = scalars.snapshot();
        if (!snapshot.empty())
        {
            snapshots[tick] = snapshot;
        }
        collection.performAutoCollection("root");

        // Disable collection from tick [200-250)
        if (tick == 200)
        {
            scalars.disableCollection();
        }
        else if (tick == 250)
        {
            scalars.enableCollection();
        }
    }

    app_mgrs.postSimLoopTeardown();
    auto db_mgr = app_mgr.getDatabaseManager();
    ValidateCollectionInDatabase(db_mgr, snapshots);
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

    using BoolPtr = std::shared_ptr<bool>;
    using BoolQueue = std::vector<BoolPtr>;

    using StringPtr = std::shared_ptr<std::string>;
    using StringQueue = std::vector<StringPtr>;

    using ColorPtr = std::shared_ptr<simdb::Colors>;
    using EnumQueue = std::vector<ColorPtr>;

    void createCollectables(simdb::collection::Collection<uint64_t>& collection)
    {
        inst_q_collector_ = collection.collectContainerWithAutoCollection<InstQueue, false>(
            "inst_q", "root", &inst_queue_, capacity_);

        sparse_inst_q_collector_ = collection.collectContainerWithAutoCollection<InstQueue, true>(
            "sparse_inst_q", "root", &sparse_inst_queue_, capacity_);

        flag_q_collector_ = collection.collectContainerWithAutoCollection<BoolQueue, false>(
            "flag_q", "root", &flag_queue_, capacity_);

        string_q_collector_ = collection.collectContainerWithAutoCollection<StringQueue, false>(
            "string_q", "root", &string_queue_, capacity_);

        enum_q_collector_ = collection.collectContainerWithAutoCollection<EnumQueue, false>(
            "enum_q", "root", &enum_queue_, capacity_);
    }

    void disableCollection()
    {
        inst_q_collector_->disable();
        sparse_inst_q_collector_->disable();
        flag_q_collector_->disable();
        string_q_collector_->disable();
        enum_q_collector_->disable();
        auto_enabled_ = false;
    }

    void enableCollection()
    {
        inst_q_collector_->enable();
        sparse_inst_q_collector_->enable();
        flag_q_collector_->enable();
        string_q_collector_->enable();
        enum_q_collector_->enable();
        auto_enabled_ = true;
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
            flag_queue_.push_back(std::make_shared<bool>(rand() % 2 == 0));
        }

        string_queue_.clear();
        auto num_strings = rand() % capacity_ + 1;
        while (string_queue_.size() < num_strings)
        {
            const auto min_chars = 4;
            const auto max_chars = min_chars;
            const auto str = simdb::generateRandomString(min_chars, max_chars);
            auto strptr = std::make_shared<std::string>(str);
            string_queue_.push_back(strptr);
        }

        enum_queue_.clear();
        auto num_enums = rand() % capacity_ + 1;
        while (enum_queue_.size() < num_enums)
        {
            const auto enumval = simdb::generateRandomColor();
            auto enumptr = std::make_shared<simdb::Colors>(enumval);
            enum_queue_.push_back(enumptr);
        }
    }

    template <typename ContainerT, bool Sparse>
    class Validator : public ValidatorBase
    {
    public:
        Validator(const ContainerT& container, size_t capacity)
            : container_(container)
            , capacity_(capacity)
            , size_(simdb::collection::getNumElements<ContainerT, Sparse>(container))
        {}

        size_t requiredBytes() const override
        {
            using value_type =
                simdb::type_traits::remove_any_pointer_t<
                    typename ContainerT::value_type>;

            // Start with the number of bytes for one element
            size_t num_bytes = scalar_num_bytes<value_type>::value();

            // Multiply by the number of elements we have to write
            num_bytes *= size_;

            // Contig and sparse both write the uint16_t queue num elements
            num_bytes += sizeof(uint16_t);

            // Only sparse containers write the uint16_t bin index
            // prior to every element's raw bytes
            if constexpr (Sparse)
            {
                num_bytes += sizeof(uint16_t) * size_;
            }

            return num_bytes;
        }

        void validate(simdb::TinyStrings<>* tiny_strings, const char*& bytes) const override
        {
            // uint16_t size always comes first
            const auto mine = size_;
            const auto theirs = *reinterpret_cast<const uint16_t*>(bytes);
            if (mine != theirs)
            {
                throw simdb::DBException("Value mismatch");
            }
            bytes += sizeof(uint16_t);

            uint16_t count = 0;
            while (count < size_)
            {
                uint16_t bin_idx = count;

                // Only sparse containers explicitly collect the bin idx
                if constexpr (Sparse)
                {
                    bin_idx = *reinterpret_cast<const uint16_t*>(bytes);
                    bytes += sizeof(uint16_t);
                }

                validateBin_(container_.at(bin_idx), tiny_strings, bytes);
                ++count;
            }
        }

    private:
        using value_type = simdb::type_traits::remove_any_pointer_t<typename ContainerT::value_type>;

        template <typename ValueType>
        std::enable_if_t<simdb::type_traits::is_any_pointer_v<ValueType>, void>
        validateBin_(const ValueType& bin, simdb::TinyStrings<>* tiny_strings, const char*& bytes) const
        {
            assert(bin != nullptr);
            validateBin_(*bin, tiny_strings, bytes);
        }

        void validateBin_(
            const value_type& bin,
            simdb::TinyStrings<>* tiny_strings,
            const char*& bytes) const
        {
            if constexpr (std::is_enum_v<value_type>)
            {
                validateEnum_<value_type>(bin, bytes);
            }
            else if constexpr (std::is_same_v<value_type, bool>)
            {
                validateBool_(bin, bytes);
            }
            else if constexpr (std::is_same_v<value_type, std::string>)
            {
                validateString_(bin, bytes, tiny_strings);
            }
            else if constexpr (std::is_trivial_v<value_type> && std::is_standard_layout_v<value_type>)
            {
                validateSimple_<value_type>(bin, bytes);
            }
            else
            {
                bin.compare(bytes, tiny_strings);
            }
        }

        ContainerT container_;
        const size_t capacity_;
        const uint16_t size_;
    };

    CollectionSnapshot snapshot() const
    {
        if (!auto_enabled_)
        {
            return {};
        }

        return {
            {inst_q_collector_->getID(),        std::make_shared<Validator<InstQueue, false>>(inst_queue_, capacity_)},
            {sparse_inst_q_collector_->getID(), std::make_shared<Validator<InstQueue, true>>(sparse_inst_queue_, capacity_)},
            {flag_q_collector_->getID(),        std::make_shared<Validator<BoolQueue, false>>(flag_queue_, capacity_)},
            {string_q_collector_->getID(),      std::make_shared<Validator<StringQueue, false>>(string_queue_, capacity_)},
            {enum_q_collector_->getID(),        std::make_shared<Validator<EnumQueue, false>>(enum_queue_, capacity_)}
        };
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
    bool auto_enabled_ = true;
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

    CollectionSnapshots snapshots;
    while (tick < RUN_TICKS)
    {
        ++tick;
        containers.randomize();
        auto snapshot = containers.snapshot();
        if (!snapshot.empty())
        {
            snapshots[tick] = snapshot;
        }
        collection.performAutoCollection("root");

        // Disable auto collection from tick [200-250)
        if (tick == 200)
        {
            containers.disableCollection();
        }
        else if (tick == 250)
        {
            containers.enableCollection();
        }
    }

    app_mgrs.postSimLoopTeardown();
    auto db_mgr = app_mgr.getDatabaseManager();
    ValidateCollectionInDatabase(db_mgr, snapshots);
}

class FullScale
{
public:
    FullScale(size_t heartbeat, size_t capacity = 32)
        : heartbeat_(heartbeat)
        , capacity_(capacity)
    {
        randomize();
    }

    void createCollectables(simdb::collection::Collection<uint64_t>& collection)
    {
        ipc_collector_ = collection.collectScalarWithAutoCollection<double>(
            "ipc", "root", &ipc_);

        inst_q_collector_ = collection.collectContainerWithAutoCollection<InstQueue, false>(
            "inst_q", "root", &inst_queue_, capacity_);

        injected_inst_collector_ = collection.collectScalarManually<Instruction>(
            "inj_inst", "root");

        injected_insts_collector_ = collection.collectContainerManually<InstQueue, false>(
            "inj_insts", "root", capacity_);
    }

    void disableAutoCollection()
    {
        ipc_collector_->disable();
        inst_q_collector_->disable();
        auto_enabled_ = false;
    }

    void enableAutoCollection()
    {
        ipc_collector_->enable();
        inst_q_collector_->enable();
        auto_enabled_ = true;
    }

    void disableManualCollection()
    {
        injected_inst_collector_->disable();
        injected_insts_collector_->disable();
        manual_enabled_ = false;
    }

    void enableManualCollection()
    {
        injected_inst_collector_->enable();
        injected_insts_collector_->enable();
        manual_enabled_ = true;
    }

    void randomize()
    {
        ipc_ = 1.0 * rand() / RAND_MAX;
        inst_queue_.clear();
        auto num_insts = rand() % capacity_;
        while (inst_queue_.size() < num_insts)
        {
            inst_queue_.push_back(Instruction::genRandom());
        }
    }

    CollectionSnapshot snapshot()
    {
        CollectionSnapshot snapshot;

        if (auto_enabled_)
        {
            snapshot.insert({
                {ipc_collector_->getID(),    std::make_shared<typename Scalars::Validator<double>>(ipc_)},
                {inst_q_collector_->getID(), std::make_shared<typename Containers::Validator<InstQueue, false>>(inst_queue_, capacity_)}
            });
        }

        if (manual_enabled_ &&
            last_injected_inst_ &&
            inj_inst_snapshot_counter_++ % heartbeat_ == 0)
        {
            snapshot[injected_inst_collector_->getID()] = std::make_shared<typename Scalars::Validator<Instruction>>(*last_injected_inst_);
            inj_inst_snapshot_counter_ = 0;
        }

        if (manual_enabled_ &&
            last_injected_insts_ &&
            inj_insts_snapshot_counter_++ % heartbeat_ == 0)
        {
            snapshot[injected_insts_collector_->getID()] = std::make_shared<typename Containers::Validator<InstQueue, false>>(*last_injected_insts_, capacity_);
            inj_insts_snapshot_counter_ = 0;
        }
        return snapshot;
    }

    using InstQueue = typename Containers::InstQueue;

    void inject(std::shared_ptr<Instruction> inst)
    {
        last_injected_inst_ = inst;
        injected_inst_collector_->collect(inst);
        inj_inst_snapshot_counter_ = 0;
    }

    void inject(std::shared_ptr<InstQueue> insts)
    {
        last_injected_insts_ = insts;
        injected_insts_collector_->collect(insts);
        inj_insts_snapshot_counter_ = 0;
    }

private:
    const size_t heartbeat_;
    const size_t capacity_;
    size_t inj_inst_snapshot_counter_ = 0;
    size_t inj_insts_snapshot_counter_ = 0;

    std::shared_ptr<Instruction> last_injected_inst_;
    std::shared_ptr<InstQueue> last_injected_insts_;

    // Auto-collected variables
    double ipc_ = 0;
    InstQueue inst_queue_;

    // Collectables (auto)
    std::shared_ptr<simdb::collection::AutoScalarCollector<double>> ipc_collector_;
    std::shared_ptr<simdb::collection::AutoContainerCollector<InstQueue, false>> inst_q_collector_;

    bool auto_enabled_ = true;
    bool manual_enabled_ = true;

    // Collectables (manual)
    std::shared_ptr<simdb::collection::ScalarCollector<Instruction>> injected_inst_collector_;
    std::shared_ptr<simdb::collection::ContainerCollector<InstQueue, false>> injected_insts_collector_;
};

void TestFullScale()
{
    uint64_t tick = 0;
    simdb::collection::Collection<uint64_t> collection;
    collection.timestampWith(&tick);
    collection.addCollection("root", 1);

    FullScale full_scale(collection.getHeartbeat());
    full_scale.createCollectables(collection);

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

    CollectionSnapshots snapshots;
    while (tick < RUN_TICKS)
    {
        ++tick;

        // Periodically inject instructions
        if (rand() % 10 == 0)
        {
            auto inj_inst = Instruction::genRandom();
            full_scale.inject(inj_inst);

            if (rand() % 10 == 0)
            {
                auto insts = std::make_shared<typename Containers::InstQueue>();
                size_t num_insts = rand() % 32;
                num_insts = std::max(num_insts, 1ul);
                while (insts->size() < num_insts)
                {
                    insts->push_back(Instruction::genRandom());
                }
                full_scale.inject(insts);
            }
        }

        full_scale.randomize();
        auto snapshot = full_scale.snapshot();
        if (!snapshot.empty())
        {
            snapshots[tick] = snapshot;
        }
        collection.performAutoCollection("root");

        // Disable auto collection from tick [200-250)
        if (tick == 200)
        {
            full_scale.disableAutoCollection();
        }
        else if (tick == 250)
        {
            full_scale.enableAutoCollection();
        }

        // Disable manual collection from tick [600-650)
        if (tick == 600)
        {
            full_scale.disableManualCollection();
        }
        else if (tick == 650)
        {
            full_scale.enableManualCollection();
        }
    }

    app_mgrs.postSimLoopTeardown();
    auto db_mgr = app_mgr.getDatabaseManager();
    ValidateCollectionInDatabase(db_mgr, snapshots);
}

void ValidateCollectionInDatabase(
    simdb::DatabaseManager* db_mgr,
    const CollectionSnapshots& snapshots)
{
    simdb::TinyStrings<> tiny_strings(db_mgr);

    auto time_query = db_mgr->createQuery("Timestamps");
    EXPECT_EQUAL(time_query->count(), snapshots.size());

    int time_id;
    time_query->select("Id", time_id);

    std::queue<uint64_t> expected_ticks;
    for (const auto& [tick, _] : snapshots)
    {
        expected_ticks.push(tick);
    }

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
        auto it1 = snapshots.find(tick);
        if (it1 == snapshots.end())
        {
            throw simdb::DBException("Tick not found in snapshot");
        }
        auto it2 = it1->second.find(cid);
        if (it2 == it1->second.end())
        {
            throw simdb::DBException("Collectable ID not found in snapshot at tick ") << tick;
        }
        const auto& validator = it2->second;
        if (!validator)
        {
            throw simdb::DBException("Found null validator");
        }
        return validator.get();
    };

    auto validate_collection = [&](int timestamp_id)
    {
        EXPECT_FALSE(expected_ticks.empty());
        EXPECT_EQUAL(expected_ticks.front(), actual_tick);
        expected_ticks.pop();

        std::set<uint16_t> to_validate;
        auto it = snapshots.find(actual_tick);
        if (it != snapshots.end())
        {
            for (const auto& [cid, _] : it->second)
            {
                to_validate.insert(cid);
            }
        }

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
            to_validate.erase(cid);
        }

        EXPECT_TRUE(to_validate.empty());
    };

    auto time_results = time_query->getResultSet();
    while (time_results.getNextRecord())
    {
        validate_collection(time_id);
    }
}

int main()
{
    SmokeTest();
    //TestAutoCollectScalars();
    //TestAutoCollectContainers();
    //TestFullScale();

    REPORT_ERROR;
    return ERROR_CODE;
}
