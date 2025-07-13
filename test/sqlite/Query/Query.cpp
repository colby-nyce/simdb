// clang-format off

#include "SimDBTester.hpp"
#include "TestData.hpp"
#include "TestSchema.hpp"

TEST_INIT;

/// This test covers basic SELECT functionality for SimDB.

int main()
{
    simdb::Schema schema;
    test::utils::defineTestSchema(schema);

    // TODO cnyce

    REPORT_ERROR;
    return ERROR_CODE;
}
