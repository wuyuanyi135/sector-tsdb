//
// Created by wuyua on 2023/1/17.
//
#include "catch_amalgamated.hpp"
#include "tsdb/common.h"

using namespace tsdb;
TEST_CASE("common") {
  SECTION("min secto for size") {
    REQUIRE(min_sector_for_size(1) == 1);
    REQUIRE(min_sector_for_size(511) == 1);
    REQUIRE(min_sector_for_size(512) == 1);
    REQUIRE(min_sector_for_size(513) == 2);
    REQUIRE(min_sector_for_size(1024) == 2);
    REQUIRE(min_sector_for_size(8192) == 16);
    REQUIRE(min_sector_for_size(8191) == 16);
    REQUIRE(min_sector_for_size(8193) == 17);
  }
}