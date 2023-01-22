#include <thread>

#include "catch_amalgamated.hpp"
#include "fakeit.hpp"
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

TEST_CASE("ESP32 errorous write sector order") {
  SectorMemoryIO io{512};

  auto partition = Partition::create_with_sector_address(10, 120);
  Series series{io, partition, SeriesConfig{100, 4096}};

  for (int i = 0; i < 100; ++i) {
    series.insert(&i, 4);
  }
}

TEST_CASE("get cfg and partition") {
  SectorMemoryIO io{512};

  auto partition = Partition::create_with_sector_address(10, 120);
  Series series{io, partition, SeriesConfig{100, 4096}};

  auto& p = series.get_partition();
  auto& cfg = series.get_series_config();

  REQUIRE(p == partition);
  REQUIRE(cfg.max_entries == 100);
  REQUIRE(cfg.max_file_size == 4096);
}

TEST_CASE("series clear") {
  SectorMemoryIO io{512};

  auto partition = Partition::create_with_sector_address(10, 120);
  Series series{io, partition, SeriesConfig{100, 4096}};

  std::string buffer = "hello, world!";
  for (int i = 0; i < 5; ++i) {
    series.insert(buffer.data(), buffer.size());
  }

  {
    int count = 0;
    series.iterate([&](auto& data_log_entry) -> bool {
      count++;
      return true;
    });
    REQUIRE(count == 5);
  }

  series.clear();

  {
    int count = 0;
    series.iterate([&](auto& data_log_entry) -> bool {
      count++;
      return true;
    });
    REQUIRE(count == 0);
  }
}

TEST_CASE("Multithread", "[.]") {
  using namespace std::chrono_literals;
  SectorMemoryIO io{512};

  auto partition1 = Partition::create_with_sector_address(10, 120);
  auto partition2 = Partition::create_with_sector_address(121, 300);
  Series series1{io, partition1, SeriesConfig{100, 4096}};
  Series series2{io, partition2, SeriesConfig{100, 4096}};

  std::thread thread1([&] {
    for (int i = 0; i < 10; ++i) {
      std::string data = "hello, world!";
      series1.insert(data.data(), data.size());
      std::this_thread::sleep_for(0.5s);
    }
  });

  std::thread thread2([&] {
    for (int i = 0; i < 20; ++i) {
      std::string data;
      data.resize(1024);
      std::fill(data.begin(), data.end(), 0x03);
      series2.insert(data.data(), data.size());
      std::this_thread::sleep_for(0.2s);
    }
  });

  std::thread thread3([&] {
    for (int i = 0; i < 10; ++i) {
      series1.iterate([](auto& data_log_entry) {
        std::string data;
        data.resize(1024);
        data_log_entry.read(data.data(), data.size());
        REQUIRE(strcmp(data.c_str(), "hello, world!") == 0);
        return true;
      });
      std::this_thread::sleep_for(0.5s);
    }
  });

  std::thread thread4([&] {
    for (int i = 0; i < 30; ++i) {
      series2.iterate([](auto& data_log_entry) {
        std::string data;
        data.resize(1024);
        auto sz = data_log_entry.read(data.data(), data.size());
        REQUIRE(sz == 1024);
        for (int j = 0; j < sz; ++j) {
          REQUIRE(data[j] == 0x03);
        }
        return true;
      });
      std::this_thread::sleep_for(0.1s);
    }
  });

  thread1.join();
  thread2.join();
  thread3.join();
  thread4.join();
}

TEST_CASE("set and get attribute") {
  SectorMemoryIO io{1024};

  auto partition = Partition::create_with_sector_address(0, 1024);
  {
    Series series{io, partition, SeriesConfig{100, 4096}};

    std::vector<uint8_t> data;
    data.resize(1024);
    for (int i = 0; i < 71; ++i) {
      series.insert(data.data(), data.size(), i, i + 1);
    }

    series.sync();
  }

  {
    Series series{io, partition, SeriesConfig{100, 4096}};
    size_t count = 0;
    series.iterate(
        [&](auto& data_log_entry) {
          REQUIRE(data_log_entry.log_entry.attr == count);
          count++;
          return true;
        },
        false);
    REQUIRE(count == 71);
  }

  {
    Series series{io, partition, SeriesConfig{100, 4096}};
    series.clear();

    std::vector<uint8_t> data;

    data.resize(1024);
    // overflowing
    for (int i = 0; i < 200; ++i) {
      series.insert(data.data(), data.size(), i, i + 1);
    }

    series.sync();
  }

  {
    Series series{io, partition, SeriesConfig{100, 4096}};
    size_t count = 0;
    series.iterate([&](auto& data_log_entry) {
      REQUIRE(data_log_entry.log_entry.attr == count + 95);
      count++;
      return true;
    },
                   false);
    REQUIRE(count == 105);
  }
}

TEST_CASE("Overwrite and all sectors are monotonic, starting from beginning") {
  const size_t n_header_sectors = 3;
  SectorMemoryIO io{10240};
  auto partition = Partition::create(0, 10240);
  Series series{io, partition, SeriesConfig{3 * HeaderSector::n_entries - 1, 4096}};
  uint32_t dummy = 0;
  int timestamp_counter = 1;
  for (int i = 0; i < n_header_sectors; ++i) {
    for (int j = 0; j < HeaderSector::n_entries; ++j) {
      series.insert(&dummy, sizeof(dummy), 0, timestamp_counter++);
    }
  }
  series.sync();
  // Make sure the timestamp is monotonic for all sectors
  uint64_t previous_timestamp = 0;
  for (int i = 0; i < n_header_sectors; ++i) {
    auto& h = (HeaderSector&)io.mem[i];
    for (int j = 0; j < HeaderSector::n_entries; ++j) {
      REQUIRE(h.entries[j].timestamp > previous_timestamp);
      previous_timestamp = h.entries[j].timestamp;
    }
  }

  // Reload the series
  Series series1{io, partition, SeriesConfig{3 * HeaderSector::n_entries - 1, 4096}};

  series1.insert(&dummy, sizeof(dummy), 0, timestamp_counter++);
  series1.sync();

  {
    auto& h = (HeaderSector&)io.mem[0];
    REQUIRE(h.entries[0].timestamp == timestamp_counter - 1);
    REQUIRE(h.entries[1].timestamp == 2);
  }

  // Reload again, iterate and check how many items are there
  Series series2{io, partition, SeriesConfig{3 * HeaderSector::n_entries - 1, 4096}};
  size_t count = 0;
  series.iterate([&](auto& data_log_entry) {
    count++;
    return true;
  });
  REQUIRE(count == 3 * HeaderSector::n_entries);
}

TEST_CASE("Overwrite and all sectors are monotonic, starting from the second sector") {
  const size_t n_header_sectors = 3;
  SectorMemoryIO io{10240};
  auto partition = Partition::create(0, 10240);
  Series series{io, partition, SeriesConfig{3 * HeaderSector::n_entries - 1, 4096}};
  uint32_t dummy = 0;
  int timestamp_counter = 1;
  for (int i = 0; i < n_header_sectors + 1; ++i) {
    for (int j = 0; j < HeaderSector::n_entries; ++j) {
      series.insert(&dummy, sizeof(dummy), 0, timestamp_counter++);
    }
  }
  series.sync();

  Series series1{io, partition, SeriesConfig{3 * HeaderSector::n_entries - 1, 4096}};

  uint64_t timestamp = HeaderSector::n_entries+1;
  size_t count = 0;
  series1.iterate([&](auto& data_log_entry){
    count ++;
    REQUIRE(data_log_entry.log_entry.timestamp == timestamp ++);
    return true;
  }, false);
  REQUIRE(count == 3 * HeaderSector::n_entries);

  series1.insert(&dummy, sizeof(dummy), timestamp_counter++);
  series1.sync();

  Series series2{io, partition, SeriesConfig{3 * HeaderSector::n_entries - 1, 4096}};
  count = 0;
  timestamp += 1;
  series1.iterate([&](auto& data_log_entry){
    count ++;
    INFO(data_log_entry.log_entry.timestamp);
//    REQUIRE(data_log_entry.log_entry.timestamp == timestamp ++);
    return true;
  }, false);
  REQUIRE(count == 3 * HeaderSector::n_entries);
}