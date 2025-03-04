# Introduction to SimDB
This is a C++11 SQLite database module which intends to address common use cases encountered in simulation systems:

* [Sparta](https://github.com/sparcians/map)
* [SystemC](https://github.com/accellera-official/systemc)
* [gem5](https://github.com/gem5/gem5)
* etc.

SimDB features include:

* Ease of use over sqlite3 C API
* Data collection API with high-performance, multi-threaded processing queue
* Argos: Front-end viewer to analyze collected data

# Code examples

## Database creation

    using dt = simdb::SqlDataType;

    simdb::Schema schema;

    schema.addTable("SimHierarchy")
        .addColumn("ParentNodeID", dt::int32_t)
        .addColumn("NodeName", dt::string_t)
        .createIndexOn("ParentNodeID");

    schema.addTable("SnapshotData")
        .addColumn("Cycle", dt::uint64_t)
        .addColumn("SimHierNodeID", dt::int32_t)
        .addColumn("RawData", dt::blob_t)
        .createCompoundIndexOn(SQL_COLUMNS("Cycle", "SimHierNodeID"));

    simdb::DatabaseManager db_mgr("sim.db");
    db_mgr.createDatabaseFromSchema(schema);

## INSERT

    // struct SimHierNode {
    //     std::string name;
    //     std::vector<std::unique_ptr<SimHierNode>> children;
    //     int db_id;
    // };

    void writeHierarchy(simdb::DatabaseManager& db_mgr, const SimHierNode& node, int parent_db_id = 0)
    {
        auto record = db_mgr.INSERT(SQL_TABLE("SimHierarchy"),
                                    SQL_COLUMNS("ParentNodeID", "NodeName"),
                                    SQL_VALUES(parent_db_id, node.name));

        node.db_id = record->getID();

        for (auto& child : node.children) {
            writeHierarchy(db_mgr, *child, record->getId());
        }
    }

## SELECT

    // SELECT Cycle,RawData FROM SnapshotData WHERE Cycle>50 AND Cycle<=100 AND SimHierNodeID=309

    auto query = db_mgr.createQuery("SnapshotData");

    uint64_t cycle;
    query->select("Cycle", cycle);

    std::vector<double> raw_data;
    query->select("RawData", raw_data);

    query->addConstraintForInt("Cycle", simdb::Constraints::GREATER, 50);
    query->addConstraintForInt("Cycle", simdb::Constraints::LESS_EQUAL, 100);
    query->addConstraintForInt("SimHierNodeID", simdb::Constraints::EQUAL, 309);

    auto result_set = query->getResultSet();
    while (result_set.getNextRecord()) {
        std::cout << "Found " << raw_data.size() << " stats at cycle " << cycle << "\n";
    }

## UPDATE

    // UPDATE SimHierarchy NodeName="root" WHERE Id=1
    auto record = db_mgr.getRecord("SimHierarchy", 1);
    record->setPropertyString("NodeName", "root");

## Collection System / Argos

One of SimDB's most powerful features is its data collection system and the front-end viewer "Argos". Its design is generic enough to support:

* Cycle-driven simulation
* Event-driven simulation
* Multiple clock domains on different frequencies
* Any data type (scalar or in a container)

Argos reads the collection database and provides various widgets to analyze data and find issues, bottlenecks, etc.

The primary use case driving Argos design is pipeline collection for CPU/GPU
performance models e.g. [Olympia](https://github.com/riscv-software-src/riscv-perf-model)

The collection system requires you to supply various template specializations in order to collect your data types:

    enum TargetUnit {
        DECODE,
        RENAME,
        RETIRE
    };

    struct MemPacket {
        uint64_t opcode;
        TargetUnit target;
        const char* mnemonic;
    };

    using MemPacketPtr = std::shared_ptr<MemPacket>;

    namespace simdb {

        template <> void defineEnumMap<TargetEnum>(std::string& enum_name, std::map<std::string, int>& map)
        {
            map["Decode"] = TargetUnit::DECODE;
            map["Rename"] = TargetUnit::RENAME;
            map["Retire"] = TargetUnit::RETIRE;
        }

        template <> void defineStructSchema<MemPacket>(StructSchema<MemPacket>& schema)
        {
            schema.addHex<uint64_t>("opcode");
            schema.addEnum<TargetUnit>("target");
            schema.addString("mnemonic");
        }

        template <> void writeStructFields<MemPacket>(const MemPacket* pkt, StructFieldSerializer<MemPacket>* serializer)
        {
            serializer->writeField(pkt->opcode);
            serializer->writeField(pkt->target);
            serializer->writeField(pkt->mnemonic);
        }

    } namespace simdb

Then you can setup SimDB collection points like this:

    simdb::DatabaseManager db_mgr("sim.db");
    db_mgr.enableCollection();

    auto collection_mgr = db_mgr.getCollectionMgr();
    collection_mgr->addClock("rootclk", 10);

    const uint32_t CAPACITY = 32;
    static constexpr bool SPARSE_FLAG = false;

    // Collect every MemPacket that comes through the receive() method,
    // but only if the downstream unit can accept it.
    std::shared_ptr<simdb::CollectionPoint> ready_pkt_collectable =
        collection_mgr->createCollectable<MemPacket>("SinglePkt", "rootclk");

    // Collect all the MemPackets that are currently queued up waiting on
    // the downstream unit to become available for more packets.
    std::shared_ptr<simdb::ContigIterableCollectionPoint> waiting_pkts_collectable =
        collection_mgr->createIterableCollector<MemPacket, SPARSE_FLAG>(
            "PktQueue", "rootclk", CAPACITY);

    .......................... simulation loop ..........................

    void Unit::receive(MemPacket* pkt)
    {
        // Assume we can either send this packet downstream somewhere,
        // or it has to be queued for later processing.
        if (downstream->ready()) {
            ready_packet = pkt;
        } else {
            ready_packet = nullptr;
            waiting_pkts.push_back(pkt);
        }
    }

    void Unit::collect()
    {
        if (ready_packet) {
            ready_pkt_collectable->activate(ready_packet);
        } else {
            waiting_pkts_collectable->activate(waiting_pkts);
        }
    }

    void Simulation::collect()
    {
        // Get all the collectables' bytes ready in the collection system
        for (auto unit : units) {
            unit->collect();
        }

        // "Sweep" the activated collectables that operate on the
        // root clock, and tell SimDB to timestamp the collection
        // blob for Argos queries.
        collection_mgr->sweep("rootclk", current_tick);
    }

See a complete example with a toy simulator here:

simdb/test/Collection/main.cpp

# Argos viewer

See simdb/python/viewer/README

# Python API

TBD

# Dependencies

The following dependencies must be installed to use SimDB (sudo apt install, conda, etc.)

* sqlite3
* zlib
* rapidjson
* wxPython (for Argos viewer only)
