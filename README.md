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

## SimDB Pipelines

TBD

## SimDB Apps

TBD

## Pipeline Collection (Argos)

TBD

# Argos viewer

TBD

# Python API

TBD

# Dependencies

The following dependencies must be installed to use SimDB (sudo apt install, conda, etc.)

* sqlite3
* zlib
* wxPython (for Argos viewer only)
