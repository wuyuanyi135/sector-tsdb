//
// Created by wuyua on 2023/1/15.
//

#pragma once
#include <array>
#include <vector>
#include <cassert>
#include <memory>
#include <cstring>
#include "common.h"
#include "exception.h"

namespace tsdb {

template <typename T>
struct IO {
  void write_sectors(const void* in, uint32_t begin_sector, uint32_t n_sector) {
    static_cast<T*>(this)->write_sectors(in, begin_sector, n_sector);
  }

  void read_sectors(void* out, uint32_t begin_sector, uint32_t n_sector) {
    static_cast<T*>(this)->read_sectors(out, begin_sector, n_sector);
  }

  uint32_t n_sectors() { return static_cast<T*>(this)->n_sectors(); }

  void write_bytes_to_sectors(void* buffer, uint32_t len, uint32_t sector_addr) {
    auto n_sectors = min_sector_for_size(len);
    auto partial_size = len % sector_size;
    auto error = IOError("Failed to write sectors");
    if (partial_size == 0) {
      // no partial
      write_sectors(buffer, sector_addr, n_sectors);
    } else {
      // If there is full sector, write full sector
      if (n_sectors > 1) {
        write_sectors(buffer, sector_addr, n_sectors - 1);
      }

      // Write partial sectors
      auto tmp = std::make_unique<uint8_t[]>(sector_size);
      memcpy(tmp.get(), (uint8_t*)buffer + sector_size * (n_sectors - 1), partial_size);
      memset(tmp.get() + partial_size, 0, sector_size - partial_size);
      write_sectors(tmp.get(), sector_addr + n_sectors - 1, 1);
    }
  }

  void read_bytes_from_sectors(void* buffer, uint32_t len, uint32_t sector_addr) {
    auto n_sectors = min_sector_for_size(len);
    auto partial_size = len % sector_size;
    if (partial_size == 0) {
      // no partial
      read_sectors(buffer, sector_addr, n_sectors);
    } else {
      // See read_bytes_from_sectors
      if (n_sectors > 1) {
        read_sectors(buffer, sector_addr, n_sectors - 1);
      }

      auto tmp = std::make_unique<uint8_t[]>(sector_size);
      read_sectors(tmp.get(), sector_addr + n_sectors - 1, 1);
      memset(tmp.get() + partial_size, 0, sector_size - partial_size);
      memcpy((uint8_t*)buffer + (n_sectors - 1) * sector_size, tmp.get(), partial_size);
    }
  }
};

struct SectorMemoryIO : IO<SectorMemoryIO> {
  using SectorType = std::array<uint8_t, sector_size>;

  explicit SectorMemoryIO(uint32_t n_sectors) { mem.resize(n_sectors); }

  std::vector<SectorType> mem;

  void write_sectors(const void* in, uint32_t begin_sector, uint32_t n_sector) {
    assert(in);
    assert(n_sector);

    if (begin_sector + n_sector > mem.size()) {
      throw IOError("Failed to write sectors");
    }
    for (int i = 0; i < n_sector; ++i) {
      memcpy(mem[begin_sector + i].data(), (uint8_t*)in + i * sector_size, sector_size);
    }
  }

  void read_sectors(void* out, uint32_t begin_sector, uint32_t n_sector) {
    assert(n_sector);

    if (begin_sector + n_sector > mem.size()) {
      throw IOError("Failed to read sectors");
    }
    for (int i = 0; i < n_sector; ++i) {
      memcpy((uint8_t*)out + i * sector_size, mem[begin_sector + i].data(), sector_size);
    }
  }

  uint32_t n_sectors() { return mem.size(); }
};
}  // namespace tsdb