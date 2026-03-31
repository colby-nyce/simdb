// <CollectionBase.hpp> -*- C++ -*-

#pragma once

#include "simdb/schema/SchemaDef.hpp"
#include "simdb/apps/argos/PipelineStager.hpp"
#include "simdb/utils/TinyStrings.hpp"

namespace simdb::collection {

class ManualCollectorHandler;

/// \class CollectionBase
/// \brief Base class for all collections (type-specific time values)
class CollectionBase
{
public:
    virtual ~CollectionBase() = default;
    virtual size_t getHeartbeat() const = 0;
    virtual SqlDataType getSqlTimeType() const = 0;
    virtual void writeMetaOnPostInit(DatabaseManager* db_mgr) = 0;
    virtual void connectToPipeline(ConcurrentQueue<Payload>* pipeline_head) = 0;
    virtual void setManualCollectorHandlers(std::unordered_map<uint16_t, std::unique_ptr<ManualCollectorHandler>>* handlers) = 0;
    virtual TinyStrings<>* getTinyStrings() const = 0;
    virtual void collectableEnabledAt(std::shared_ptr<TimePointBase> time_point, uint16_t cid, bool enabled) = 0;
    virtual void writeMetaOnPostTeardown(DatabaseManager* db_mgr) = 0;
};

} // namespace simdb::collection
