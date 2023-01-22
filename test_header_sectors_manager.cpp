//
// Created by wuyua on 2023/1/16.
//

#include <catch_amalgamated.hpp>

#include "tsdb/header_sectors_manager.h"
#include "tsdb/io.h"
using namespace tsdb;

auto is_overlapping = [](const LogEntry& e1, const LogEntry& e2) {
  return std::max(e1.begin_sector_offset, e2.begin_sector_offset) <= std::min(e1.end_sector_addr(), e2.end_sector_addr());
};

TEST_CASE("header sectors manager") {
  SECTION("simple append") {
    SectorMemoryIO io{32};
    HeaderSectorsManager hsm{io, 1, 1, 31};

    auto expected_entries = std::vector<LogEntry>({
        {1673879016, 0xff, 0, 256, 0},
        {1673879017, 0xfe, 0, 1000, 0},
        {1673879019, 0xaa, 0, 1, 0},
    });

    int expected_sector_begin_addr[] = {2, 3, 5, 6};

    int i = 0;
    for (auto& e : expected_entries) {
      auto begin_sector_addr = hsm.add_log(e.size, e.checksum, e.timestamp);
      REQUIRE(hsm.sector_addr_r2a(begin_sector_addr) == expected_sector_begin_addr[i]);
      e.begin_sector_offset = begin_sector_addr;

      auto& sector = hsm.header_sector_cache();
      auto& entry = sector.entries[i];

      REQUIRE(entry == e);

      auto entries = hsm.get_entries(false);
      REQUIRE(entries.size() == i + 1);
      REQUIRE(entries[i] == e);

      i++;
    }

    {
      // Reload from io
      HeaderSectorsManager hsm1{io, 1, 1, 31};

      auto entries = hsm.get_entries(false);
      REQUIRE(entries == expected_entries);
    }
  }

  SECTION("append to one or many sector") {
    SectorMemoryIO io{256};
    uint32_t n_header_sectors = GENERATE(1, 2, 5);
    HeaderSectorsManager hsm{io, 0, n_header_sectors, 256};

    int repetitions = GENERATE(5, 1000, 5000);
    for (int i = 0; i < repetitions; ++i) {
      auto m = i % 3;
      switch (m) {
        case 0:
          hsm.add_log(10, 0x01, i + 1);
          break;
        case 1:
          hsm.add_log(1023, 0x01, i + 1);
          break;
        case 2:
          hsm.add_log(9000, 0x01, i + 1);
          break;
      }
    }

    auto entries = hsm.get_entries(false, 950, 990);
    for (int i = 0; i < entries.size(); ++i) {
      auto& current = entries[i];
      REQUIRE(current.timestamp >= 950);
      REQUIRE(current.timestamp < 990);
      REQUIRE(current.checksum == 0x01);
    }
    for (int i = 1; i < entries.size(); ++i) {
      auto& current = entries[i - 1];
      auto& next = entries[i];

      REQUIRE(current.timestamp < next.timestamp);
      REQUIRE(!is_overlapping(current, next));
    }
  }

  SECTION("load without available slot") {
    SectorMemoryIO io{32};
    HeaderSectorsManager hsm{io, 0, 1, 32};
    for (int i = 0; i < HeaderSector::n_entries; ++i) {
      hsm.add_log(1, 1, 1 + i);
    }

    hsm.sync_current_sector();

    HeaderSectorsManager hsm1{io, 0, 1, 32};
    auto entries = hsm1.get_entries(false);
    for (int i = 0; i < entries.size() - 1; ++i) {
      auto& current = entries[i];
      auto& next = entries[i + 1];
      REQUIRE(current.timestamp < next.timestamp);
      REQUIRE(!is_overlapping(current, next));
    }
  }

  SECTION("load with non-monotonic case") {
    SectorMemoryIO io{32};
    HeaderSectorsManager hsm{io, 0, 1, 32};
    for (int i = 0; i < HeaderSector::n_entries + 1; ++i) {
      hsm.add_log(1, 1, 1 + i);
    }
    hsm.sync_current_sector();
    HeaderSectorsManager hsm1{io, 0, 1, 32};
    auto entries = hsm1.get_entries(false);
    for (int i = 0; i < entries.size() - 1; ++i) {
      auto& current = entries[i];
      auto& next = entries[i + 1];
      REQUIRE(current.timestamp < next.timestamp);
      REQUIRE(!is_overlapping(current, next));
    }
  }
}

TEST_CASE("If timestamps are the same, should load in growing order") {
  SectorMemoryIO io{100};
  HeaderSectorsManager hsm{io, 0, 3, 100};
  for (int i = 0; i < 50; ++i) {
    hsm.add_log(1, i, 1);
  }
  auto entries = hsm.get_entries(false);

  for (int i = 0; i < entries.size(); ++i) {
    REQUIRE(entries[i].checksum == i);
  }
}

TEST_CASE("clear header sector managers") {
  SectorMemoryIO io{1000};
  {
    HeaderSectorsManager hsm{io, 0, 3, 1000};
    for (int i = 1; i < 50; ++i) {
      hsm.add_log(i * 20, i, i);
    }

    hsm.sync_current_sector();
  }
  {
    HeaderSectorsManager hsm{io, 0, 3, 1000};
    auto entries = hsm.get_entries(false);
    REQUIRE(entries.size() == 49);
    for (int i = 1; i < 50; ++i) {
      REQUIRE(entries[i - 1].checksum == i);
      REQUIRE(entries[i - 1].size == i * 20);
      REQUIRE(entries[i - 1].timestamp == i);
    }

    hsm.clear();
    entries = hsm.get_entries();
    REQUIRE(entries.size() == 0);
  }
  {
    HeaderSectorsManager hsm{io, 0, 3, 1000};
    REQUIRE(hsm.get_entries().size() == 0);
  }
}