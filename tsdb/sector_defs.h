//
// Created by wuyua on 2023/1/15.
//

#pragma once
#include "common.h"
namespace tsdb {

struct LogEntry {
  uint32_t timestamp;
  uint32_t checksum;
  uint32_t begin_sector_offset;
  uint32_t size;

  inline size_t end_sector_addr() const {
    return begin_sector_offset + min_sector_for_size(size) - 1;
  }

  bool operator==(LogEntry const&) const = default;

} __attribute__((packed));

struct HeaderSector {
  uint32_t crc;
  uint32_t last_update_timestamp;
  uint32_t init_count;
  uint32_t write_count;
  constexpr static uint32_t n_entries = 31;
  LogEntry entries[n_entries];

  /// This function scan through each entries and gives the index of usable slot.
  /// If there is an entry with timestamp == 0 (this entry is not used), return this entry's index.
  /// If the timestamps are all monotonic,this sector is fully occupied, returns -1.
  /// If the timestamps of i-th entry is greater than (i+1)th, return i+1;

  /// \return
  int find_empty_slot() {
    uint32_t greatest_timestamp = 0;
    for (int i = 0; i < n_entries; ++i) {
      auto ts = entries[i].timestamp;
      if (ts == 0) {
        return i;
      }

      if (ts > greatest_timestamp) {
        // monotonic, this is a used entry
        greatest_timestamp = ts;
      } else {
        // this timestamp is less than the previous one, this entry will be overwritten next.
        return i;
      }
    }
    return -1;
  }

  template <typename CRC>
  uint32_t compute_crc() {
    CRC crc_computer;
    auto offset = offsetof(HeaderSector, entries);
    crc_computer.update((uint8_t*)this + offset, sector_size - offset);
    return crc_computer.get();
  }

  template <typename CRC>
  uint32_t update_crc() {
    crc = compute_crc<CRC>();
    return crc;
  }

  template <typename CRC>
  bool check_crc() {
    return compute_crc<CRC>() == crc;
  }

  void clear(bool clear_stats = true) {
    if (clear_stats) {
      write_count = 0;
      init_count = 0;
    }
    auto offset = offsetof(HeaderSector, entries);
    memset((uint8_t*)this + offset, 0, sector_size - offset);
  }

} __attribute__((packed));
static_assert(sizeof(HeaderSector) == sector_size);
}  // namespace tsdb