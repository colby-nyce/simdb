/*
 * This test shows how to build a basic SimDB application. For simplicity
 * we will only collect metadata. More sophisticated apps that use async
 * processing pipelines for data collection / pipelined apps are shown
 * in other tests.
 */

// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/test/SimDBTester.hpp"

TEST_INIT;

int main(int argc, char** argv)
{
    DB_INIT;


    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
