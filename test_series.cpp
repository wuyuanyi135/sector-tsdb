#include "catch_amalgamated.hpp"
#include "tsdb/series.h"
using namespace tsdb;
using namespace tsdb::literals;

TEST_CASE("series insufficent slots") {
  SectorMemoryIO io{32};

  auto partition = Partition::create(0, 32);
  // 10 will be round up to 31
  Series series{io, partition, SeriesConfig{10, 8_kb}};

  std::string data1 = "hello, world";
  for (int i = 0; i < 1000; ++i) {
    series.insert(data1.c_str(), data1.size());
  }

  std::vector<std::string> results;
  series.iterate(
      [&](auto& data_log_entry) {
        std::string read_buffer;
        read_buffer.resize(data_log_entry.log_entry.size);
        data_log_entry.read(read_buffer.data(), read_buffer.size());
        auto crc = data_log_entry.get_accumulated_crc();
        REQUIRE(crc == data_log_entry.log_entry.checksum);
        results.push_back(read_buffer);
        return true;
      },
      false);

  REQUIRE(results.size() == HeaderSector::n_entries);
  for (auto& r : results) {
    REQUIRE(r == data1);
  }
}

TEST_CASE("series insufficent data sectors") {
  SectorMemoryIO io{32};

  auto partition = Partition::create(0, 32);
  // 10 will be round up to 31
  Series series{io, partition, SeriesConfig{500, 8_kb}};

  std::string data1;
  data1.resize(1024);
  std::fill(data1.begin(), data1.end(), 0xf1);

  for (int i = 0; i < 1000; ++i) {
    series.insert(data1.c_str(), data1.size());
  }

  std::vector<std::string> results;
  series.iterate(
      [&](auto& data_log_entry) {
        std::string read_buffer;
        read_buffer.resize(data_log_entry.log_entry.size);
        data_log_entry.read(read_buffer.data(), read_buffer.size());
        auto crc = data_log_entry.get_accumulated_crc();
        REQUIRE(crc == data_log_entry.log_entry.checksum);
        results.push_back(read_buffer);
        return true;
      },
      false);

  for (auto& r : results) {
    REQUIRE(r == data1);
  }
}

TEST_CASE("series simple") {
  SectorMemoryIO io{32};

  auto partition = Partition::create(0, 32);
  Series series{io, partition, SeriesConfig{128, 8_kb}};

  std::string data1 = "hello, world";
  series.insert(data1.c_str(), data1.size());

  std::string big;
  big.resize(8_kb);
  std::fill(big.begin(), big.end(), 0xf3);
  series.insert(big.data(), big.size());

  std::vector<std::string> results{};
  series.iterate(
      [&](auto& data_log_entry) {
        std::string read_buffer;
        read_buffer.resize(data_log_entry.log_entry.size);
        data_log_entry.read(read_buffer.data(), read_buffer.size());
        auto crc = data_log_entry.get_accumulated_crc();
        REQUIRE(crc == data_log_entry.log_entry.checksum);
        results.push_back(read_buffer);
        return true;
      },
      false);
  REQUIRE(results[0] == data1);
  REQUIRE(results[1] == big);
}

TEST_CASE("partial write and partial read") {
  SectorMemoryIO io{32};

  auto partition = Partition::create(5, 20);
  Series series{io, partition, SeriesConfig{10, 8_kb}};

  std::string big_data;
  big_data.resize(8_kb);
  std::fill(big_data.begin(), big_data.end(), 0x99);
  auto transaction = series.begin_insert_transaction(big_data.size());

  SECTION("complete write") {
    for (int i = 0; i < 8_kb; i += 1_kb) {
      transaction.write(big_data.data() + i, 1_kb);
    }

    SECTION("finalize") {
      transaction.finalize();
      REQUIRE(transaction.is_finalized);
    }
    SECTION("do not finalize") {
      REQUIRE(transaction.is_finalized);
    }

    series.iterate(
        [](auto& data_log_entry) {
          std::string read_buffer;
          read_buffer.resize(data_log_entry.log_entry.size);
          for (int i = 0; i < data_log_entry.log_entry.size; i += sector_size) {
            data_log_entry.read(read_buffer.data() + i, sector_size);
          }

          REQUIRE(data_log_entry.get_accumulated_crc() == data_log_entry.log_entry.checksum);
          return true;
        },
        false);
  }
}