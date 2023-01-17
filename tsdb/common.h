//
// Created by wuyua on 2023/1/15.
//

#pragma once
namespace tsdb {
const static uint32_t sector_size = 512;
inline size_t min_sector_for_size(size_t bytes) {
  return ((bytes - 1) & (~(sector_size - 1))) / 512 + 1;
}

typedef uint32_t RelativeSectorAddress;
typedef uint32_t AbsoluteSectorAddress;
}  // namespace tsdb