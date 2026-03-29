// <CollectionBase.hpp> -*- C++ -*-

#pragma once

#include "simdb/schema/SchemaDef.hpp"
#include "simdb/apps/argos/PipelineStager.hpp"

namespace simdb::collection {

/// \class CollectionBase
/// \brief Base class for all collections (type-specific time values)
class CollectionBase
{
public:
    virtual ~CollectionBase() = default;
    virtual size_t getHeartbeat() const = 0;
    virtual SqlDataType getSqlTimeType() const = 0;
    virtual void writeMetaOnPostInit(DatabaseManager* db_mgr) = 0;
    virtual void openStage(ConcurrentQueue<Payload>* pipeline_head) = 0;
};

} // namespace simdb::collection
