//
// Created by wuyua on 2023/1/22.
//
//#define TSDB_DEBUG

#include "fmt/format.h"
#include "thread"
#include "tsdb/series.h"

int main() {
  using namespace fmt;
  using namespace std::chrono_literals;
  using namespace tsdb;
  using namespace tsdb::literals;
  SectorMemoryIO io{20000};

  auto partition = Partition::create(0, 20000);

  auto series = std::make_unique<Series<decltype(io)>>(io, partition, SeriesConfig{42, 4_kb});
  std::vector<uint8_t> data;
  data.resize(2048);
  size_t timestamp = 1;
  while (true) {
    size_t count = 0;
    series->iterate([&](auto& data_log_entry){
      count ++;
      return true;
    });
    print("Count = {}\n", count);

    series->insert(data.data(), data.size(), 0, timestamp ++);

    std::this_thread::sleep_for(100ms);
  }
}