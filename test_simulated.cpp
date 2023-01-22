//
// Created by wuyua on 2023/1/22.
//
#include "catch_amalgamated.hpp"
#include "tsdb/series.h"

using namespace tsdb;
using namespace tsdb::literals;

SectorMemoryIO io{10240};
auto make_series = []() -> std::array<std::unique_ptr<Series<SectorMemoryIO>>, 2> {
  auto partition1 = Partition::create(0, 5000);
  auto partition2 = Partition::create(5000, 5240);

  auto series1 = std::make_unique<Series<decltype(io)>>(io, partition1, SeriesConfig{100, 2_mb});
  auto series2 = std::make_unique<Series<decltype(io)>>(io, partition2, SeriesConfig{100, 2_mb});

  return {std::move(series1), std::move(series2)};
};

TEST_CASE("Simulated case") {
  {
    // Initial should be zero
    size_t count = 0;
    auto series = make_series();
    for (auto& s : series) {
      s->iterate([&](auto& data_log_entry) {count ++; return true; });
      REQUIRE(count == 0);
    }
  }
  {
    // Push in some small data, but not triggering auto saving
    auto series = make_series();
    for (auto& s : series) {
      for (int i = 0; i < 10; ++i) {
        std::string data;
        data.resize(10_kb, (char)i);
        s->insert(data.data(), data.size());
      }
      size_t count = 0;
      s->iterate(
          [&](auto& data_log_entry) {
            std::string recv;
            recv.resize(data_log_entry.log_entry.size);
            auto len = data_log_entry.read(recv.data(), recv.size());
            REQUIRE(len == 10_kb);
            for (int j = 0; j < len; ++j) {
              REQUIRE(recv[j] == count);
            }
            count++;
            return true;
          },
          false);
    }

    {
      // Reload, should still be empty
      size_t count = 0;
      auto series = make_series();
      for (auto& s : series) {
        s->iterate([&](auto& data_log_entry) {count ++; return true; });
        REQUIRE(count == 0);
      }
    }

    {
      // Push in some small data, then sync
      auto series = make_series();
      for (auto& s : series) {
        for (int i = 0; i < 10; ++i) {
          std::string data;
          data.resize(10_kb, (char)i);
          s->insert(data.data(), data.size());
        }
        s->sync();
      }
    }
  }

  {
    //  After sync, there should be data
    auto series = make_series();
    for (auto& s : series) {
      size_t count = 0;
      s->iterate(
          [&](auto& data_log_entry) {
            std::string recv;
            recv.resize(data_log_entry.log_entry.size);
            auto len = data_log_entry.read(recv.data(), recv.size());
            REQUIRE(len == 10_kb);
            for (int j = 0; j < len; ++j) {
              REQUIRE(recv[j] == count);
            }
            count++;
            return true;
          },
          false);
      REQUIRE(count == 10);
    }
  }

  {
    // After inserting more than sector page, it should automatically save.
    auto series = make_series();
    for (auto& s : series) {
      for (int i = 0; i < HeaderSector::n_entries; ++i) {
        std::string data;
        data.resize(10_kb, (char)(i + 10));
        s->insert(data.data(), data.size());
      }
    }
  }

  // It should auto save
  auto series = make_series();
  for (auto& s : series) {
    size_t count = 0;
    s->iterate(
        [&](auto& data_log_entry) {
          std::string recv;
          recv.resize(data_log_entry.log_entry.size);
          auto len = data_log_entry.read(recv.data(), recv.size());
          REQUIRE(len == 10_kb);
          for (int j = 0; j < len; ++j) {
            REQUIRE(recv[j] == count);
          }
          count++;
          return true;
        },
        false);
    REQUIRE(count == HeaderSector::n_entries);
  }

  {
    // After clear, it should get back to empty
    auto series = make_series();
    for (auto& s : series) {
      s->clear();
    }
  }

  {
    auto series = make_series();
    for (auto& s : series) {
      size_t count = 0;
      s->iterate(
          [&](auto& data_log_entry) {
            count++;
            return true;
          },
          false);
      REQUIRE(count == 0);
    }
  }

  {
    // Insert many data, overflow and sync
    auto series = make_series();
    for (auto& s : series) {
      for (int i = 0; i < 200; ++i) {
        std::vector<uint8_t> data;
        data.resize(512, (uint8_t)i);
        // Warning: timestamp should not duplicate if it is crossing the sector boundary!!!!
        s->insert(data.data(), data.size());
      }
      s->sync();
    }
  }

  {
    auto series = make_series();
    for (auto& s : series) {
      size_t count = 0;
      std::vector<size_t> h;
      s->iterate(
          [&](auto& data_log_entry) {
            std::vector<uint8_t> recv;
            recv.resize(data_log_entry.log_entry.size);
            auto len = data_log_entry.read(recv.data(), recv.size());
            REQUIRE(len == 512);
            data_log_entry.read(recv.data(), recv.size());
            h.push_back(recv[0]);
            count++;
            return true;
          },
          false);
      REQUIRE(count == 105);
      size_t i = 0;
      for (auto& hh : h) {
        REQUIRE(hh == 200 - 105 + i);
        i++;
      }
    }
  }
}