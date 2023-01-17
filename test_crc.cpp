//
// Created by wuyua on 2023/1/17.
//
#include "catch_amalgamated.hpp"
#include "tsdb/crc.h"

using namespace tsdb;
TEST_CASE("crc") {
  CRCDefault crc;
  SECTION("full") {
    const char* data = "123456789";
    crc.update(data, strlen(data));
    REQUIRE(crc.get() == 0xFC891918);
  }

  SECTION("partial") {
    crc.update("1234", 4);
    crc.update("56789", 5);
    REQUIRE(crc.get() == 0xFC891918);
  }
}
