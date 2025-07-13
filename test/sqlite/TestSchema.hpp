#pragma once

#include "simdb/schema/SchemaDef.hpp"

namespace test::utils {

inline void defineTestSchema(simdb::Schema& schema)
{
    using dt = simdb::SqlDataType;

    schema.addTable("IntegerTypes")
        .addColumn("SomeInt32", dt::int32_t)
        .addColumn("SomeInt64", dt::int64_t);

    schema.addTable("FloatingPointTypes")
        .addColumn("SomeDouble", dt::double_t);

    schema.addTable("StringTypes")
        .addColumn("SomeString", dt::string_t);

    schema.addTable("BlobTypes")
        .addColumn("SomeBlob", dt::blob_t);

    schema.addTable("MixAndMatch")
        .addColumn("SomeInt32", dt::int32_t)
        .addColumn("SomeString", dt::string_t)
        .addColumn("SomeBlob", dt::blob_t);

    schema.addTable("DefaultValues")
        .addColumn("DefaultInt32", dt::int32_t)
        .addColumn("DefaultInt64", dt::int64_t)
        .addColumn("DefaultDouble", dt::double_t)
        .addColumn("DefaultString", dt::string_t)
        .setColumnDefaultValue("DefaultInt32", TEST_INT32)
        .setColumnDefaultValue("DefaultInt64", TEST_INT64)
        .setColumnDefaultValue("DefaultDouble", TEST_DOUBLE)
        .setColumnDefaultValue("DefaultString", TEST_STRING);

    schema.addTable("DefaultDoubles")
        .addColumn("DefaultEPS", dt::double_t)
        .addColumn("DefaultMIN", dt::double_t)
        .addColumn("DefaultMAX", dt::double_t)
        .addColumn("DefaultPI", dt::double_t)
        .addColumn("DefaultEXACT", dt::double_t)
        .addColumn("DefaultINEXACT", dt::double_t)
        .setColumnDefaultValue("DefaultEPS", TEST_EPSILON)
        .setColumnDefaultValue("DefaultMIN", TEST_DOUBLE_MIN)
        .setColumnDefaultValue("DefaultMAX", TEST_DOUBLE_MAX)
        .setColumnDefaultValue("DefaultPI", TEST_DOUBLE_PI)
        .setColumnDefaultValue("DefaultEXACT", TEST_DOUBLE_EXACT)
        .setColumnDefaultValue("DefaultINEXACT", TEST_DOUBLE_INEXACT);

    schema.addTable("IndexedColumns")
        .addColumn("SomeInt32", dt::int32_t)
        .addColumn("SomeDouble", dt::double_t)
        .addColumn("SomeString", dt::string_t)
        .createCompoundIndexOn({"SomeInt32", "SomeDouble", "SomeString"});

    schema.addTable("NonIndexedColumns")
        .addColumn("SomeInt32", dt::int32_t)
        .addColumn("SomeDouble", dt::double_t)
        .addColumn("SomeString", dt::string_t);
}

} // namespace test::utils
