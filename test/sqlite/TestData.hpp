#pragma once

#include "simdb/schema/Blob.hpp"

#include <limits>
#include <math.h>
#include <string>
#include <vector>

static constexpr auto TEST_INT32 = std::numeric_limits<int32_t>::max();
static constexpr auto TEST_INT64 = std::numeric_limits<int64_t>::max();
static constexpr auto TEST_DOUBLE = std::numeric_limits<double>::max();
static constexpr auto TEST_EPSILON = std::numeric_limits<double>::epsilon();
static constexpr auto TEST_DOUBLE_MIN = std::numeric_limits<double>::min();
static constexpr auto TEST_DOUBLE_MAX = std::numeric_limits<double>::max();
static constexpr auto TEST_DOUBLE_PI = M_PI;
static constexpr auto TEST_DOUBLE_EXACT = 1.0;
static constexpr auto TEST_DOUBLE_INEXACT = (0.1 + 0.1 + 0.1);
static const std::string TEST_STRING = "TheExampleString";
static const std::vector<int> TEST_VECTOR = {1, 2, 3, 4, 5};
static const std::vector<int> TEST_VECTOR2 = {6, 7, 8, 9, 10};
static const simdb::SqlBlob TEST_BLOB = TEST_VECTOR;
static const simdb::SqlBlob TEST_BLOB2 = TEST_VECTOR2;
