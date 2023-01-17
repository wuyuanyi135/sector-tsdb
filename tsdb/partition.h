//
// Created by wuyua on 2023/1/15.
//

#pragma once

#include <cassert>
namespace tsdb {

namespace literals {
constexpr size_t operator"" _b(unsigned long long sz) {
  return sz;
}

constexpr size_t operator"" _kb(unsigned long long sz) {
  return sz << 10;
}
constexpr size_t operator"" _mb(unsigned long long sz) {
  return sz << 20;
}
}  // namespace literals

struct Partition {
  uint32_t begin_sector_addr;
  uint32_t n_sectors;

 private:
  Partition(uint32_t begin_sector_addr, uint32_t n_sectors) : begin_sector_addr(begin_sector_addr), n_sectors(n_sectors) {}

 public:
  static Partition create(uint32_t begin_sector_addr, uint32_t n_sectors) {
    assert(n_sectors > 0);
    return {begin_sector_addr, n_sectors};
  }

  static Partition create_with_sector_address(uint32_t begin_sector_addr, uint32_t end_sector_addr) {
    assert(end_sector_addr > begin_sector_addr);
    return {begin_sector_addr, end_sector_addr - begin_sector_addr};
  }

  static Partition create_with_size(uint32_t begin_sector_addr, uint32_t size) {
    assert(size % sector_size == 0);
    return {begin_sector_addr, size / sector_size};
  }
};

}  // namespace tsdb