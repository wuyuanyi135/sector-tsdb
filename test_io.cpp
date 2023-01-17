#include <catch_amalgamated.hpp>

#include "tsdb/io.h"
using namespace tsdb;
TEST_CASE("io") {
  SectorMemoryIO io{32};

  SECTION("sector number") {
    REQUIRE(io.n_sectors() == 32);
  }

  SECTION("write partial sectors") {
    SECTION("write full sector") {
      std::array<uint8_t, sector_size> data{};
      memset(data.data(), 0xa1, data.size());
      io.write_bytes_to_sectors(data.data(), data.size(), 0);
      io.write_bytes_to_sectors(data.data(), data.size(), 2);

      REQUIRE(data == io.mem[0]);
      REQUIRE(data == io.mem[2]);
    }

    SECTION("write multiple full sectors") {
      std::array<uint8_t, sector_size*5> data{};
      memset(data.data(), 0xa1, data.size());
      io.write_bytes_to_sectors(data.data(), data.size(), 0);

      for (int i = 0; i < 5; ++i) {
        REQUIRE(memcmp(data.data() + i * sector_size, io.mem[i].data(), sector_size) == 0);
      }
    }

    SECTION("write partial sector") {
      std::array<uint8_t, sector_size> data{};
      memset(data.data(), 0xa1, 300);
      io.write_bytes_to_sectors(data.data(), 300, 0);

      REQUIRE(data == io.mem[0]);
    }

    SECTION("write full and partial sectors") {
      std::array<uint8_t, sector_size * 3 + 330> data{};
      memset(data.data(), 0xa1, data.size());

      io.write_bytes_to_sectors(data.data(), data.size(), 0);

      for (int i = 0; i < 3; ++i) {
        REQUIRE(memcmp(data.data() + i * sector_size, io.mem[i].data(), sector_size) == 0);
      }
      REQUIRE(memcmp(data.data() + 3 * sector_size, io.mem[3].data(), 330) == 0);
    }
  }

  SECTION("read partial sectors") {
    SECTION("read full sectors") {
      for (int i = 0; i < 3; ++i) {
        memset(io.mem[i].data(), i, sector_size);
      }

      std::array<uint8_t, 3 * sector_size> data{};
      io.read_bytes_from_sectors(data.data(), data.size(), 0);

      for (int i = 0; i < data.size(); ++i) {
        REQUIRE(data[i] == i / sector_size);
      }
    }

    SECTION("read full and partial sectors") {
      for (int i = 0; i < 3; ++i) {
        memset(io.mem[i].data(), i, sector_size);
      }

      // Deliberately use larger sector size but not read all
      std::array<uint8_t, 3 * sector_size> data{};
      io.read_bytes_from_sectors(data.data(), 2 * sector_size + 300, 0);

      for (int i = 0; i < 2 * sector_size + 300; ++i) {
        REQUIRE(data[i] == i / sector_size);
      }
      for (int i = 2 * sector_size + 300; i < data.size(); ++i) {
        REQUIRE(data[i] == 0);
      }
    }
  }

  SECTION("error cases") {
    std::array<uint8_t, sector_size*33> buf{};
    REQUIRE_THROWS_AS(io.write_sectors(buf.data(), 0, 33), IOError);
    REQUIRE_THROWS_AS(io.read_sectors(buf.data(), 0, 33), IOError);
  }
}