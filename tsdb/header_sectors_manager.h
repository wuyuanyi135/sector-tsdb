//
// Created by wuyua on 2023/1/16.
//
#pragma once
#include <cassert>
#include <chrono>
#include <memory>
#include <vector>

#include "crc.h"
#include "exception.h"
#include "sector_defs.h"

namespace tsdb {
template <typename IO, typename CRC = CRCDefault, typename ClockType = std::chrono::system_clock>
struct HeaderSectorsManager {
  explicit HeaderSectorsManager(
      IO& io,
      uint32_t begin_sector_addr,
      uint32_t n_header_sectors,
      uint32_t n_total_sectors)
      : io(io),
        begin_sector_addr(begin_sector_addr),
        n_header_sectors(n_header_sectors),
        n_total_sectors(n_total_sectors) {
    assert(n_header_sectors < n_total_sectors);
    init();
  }

 private:
  IO& io;

 private:
  const uint32_t begin_sector_addr;
  const uint32_t n_header_sectors;
  const uint32_t n_total_sectors;
  const uint32_t n_data_sectors{n_total_sectors - n_header_sectors};

  std::unique_ptr<HeaderSector> current_header_sector{std::make_unique<HeaderSector>()};
  uint32_t current_header_sector_idx{0};
  uint32_t current_slot_idx{0};

  // Offset from the first data sector
  uint32_t current_data_sector_offset{0};

  uint32_t previous_timestamp{0};

 protected:
  void init() {
    // Check crc of each header sector. If bad, clear the sector
    for (int i = 0; i < n_header_sectors; ++i) {
      load_header_sector(i);
      if (!current_header_sector->check_crc<CRC>()) {
        current_header_sector->clear();
        sync_current_sector();
      }
    }

    // Search through header sectors, find the available slot (last written one + 1 or the first slot)
    for (int i = 0; i < n_header_sectors; ++i) {
      load_header_sector(i);
      auto slot = current_header_sector->find_empty_slot();
      if (slot == -1) {
        // No more slot in this sector, check next.
        current_data_sector_offset = current_header_sector->entries[HeaderSector::n_entries - 1].end_sector_addr() + 1;
        continue;
      } else {
        // Found available slot.
        if (slot == 0) {
          // In this case we will use the data sector offset of the previous sector, hence not updating it here.
        } else {
          current_data_sector_offset = current_header_sector->entries[slot - 1].end_sector_addr() + 1;
        }
        current_slot_idx = slot;
        return;
      }
    }

    // If reach here, cannot find any available slot, start from the first sector and first slot.
    load_header_sector(0);
    current_slot_idx = 0;
  }

  void load_header_sector(size_t sector_idx) {
    current_header_sector_idx = sector_idx;
    io.read_sectors(current_header_sector.get(), begin_sector_addr + sector_idx, 1);
  }

  /// This function only go backward along the header sectors. No check performed on the validity of the entries.
  /// \param sector_mem mutable memory. will be loaded to the previous sector mem. For first iteration, load with the current cache.
  /// \param sector_idx mutable idx, will be set to the previous idx if load occurs.
  /// \param slot_idx mutable idx. will be set to the idx after step backward.
  /// \return ref to the previous entry.
  LogEntry& previous_log_entry(std::unique_ptr<HeaderSector>& sector_mem, uint32_t& sector_idx, uint32_t& slot_idx) const {
    if (slot_idx == 0) {
      if (n_header_sectors == 1) {
        // single sector and its pointing to 0 = go back to the last entry
        slot_idx = HeaderSector::n_entries - 1;
        return sector_mem->entries[slot_idx];
      }
      // The last one is in the previous page (may round back to the last)
      sector_idx = sector_idx == 0 ? n_header_sectors - 1 : sector_idx - 1;
      io.read_sectors(sector_mem.get(), begin_sector_addr + sector_idx, 1);
      slot_idx = HeaderSector::n_entries - 1;
      return sector_mem->entries[slot_idx];
    } else {
      slot_idx = slot_idx - 1;
      return sector_mem->entries[slot_idx];
    }
  }

  void advance_header_sector() {
    // Save the current sector.
    sync_current_sector();

    // load the next sector
    load_header_sector((current_header_sector_idx + 1) % n_header_sectors);
  }

 public:
  /// This method is used when the checksum of the file is not yet known.
  /// The reference to the entry is returned for updating the checksum
  /// After done, call advance_slot().
  /// \return
  LogEntry& add_log_partial(uint32_t data_size, uint32_t timestamp, uint32_t attr = 0) {
    if (timestamp < previous_timestamp) {
      // This is a bit dangerous?
      timestamp = previous_timestamp + 1;
    }
    previous_timestamp = timestamp;

    if (data_size == 0) {
      throw Error("zero data size");
    }

    size_t required_sectors = min_sector_for_size(data_size);
    if (required_sectors > n_data_sectors) {
      throw Error("data size too big");
    }

    if (required_sectors > n_data_sectors - current_data_sector_offset) {
      // No space on the tail of the data sectors, start from head
      current_data_sector_offset = 0;
    }

    auto& entry = current_header_sector->entries[current_slot_idx];
    entry.timestamp = timestamp;
    entry.size = data_size;
    entry.checksum = 0;
    entry.begin_sector_offset = current_data_sector_offset;
    entry.attr = attr;
    current_data_sector_offset += required_sectors;
    return entry;
  }

  /// returns the *relative* begin sector address for this entry
  RelativeSectorAddress add_log(uint32_t data_size, uint32_t checksum, uint32_t timestamp, uint32_t attr = 0) {
    auto& entry = add_log_partial(data_size, timestamp, attr);
    entry.checksum = checksum;
    // Copy it since advance_slot will change entry!
    auto ret = entry.begin_sector_offset;
    advance_slot();
    return ret;
  }

  void advance_slot() {
    if (++current_slot_idx >= HeaderSector::n_entries) {
      advance_header_sector();
      current_slot_idx = 0;
    }
  }

  void sync_current_sector() {
    current_header_sector->write_count++;
    current_header_sector->update_crc<CRC>();

    io.write_sectors(current_header_sector.get(), begin_sector_addr + current_header_sector_idx, 1);
  }

  [[nodiscard]] const HeaderSector& header_sector_cache() const {
    return *current_header_sector;
  }

  AbsoluteSectorAddress sector_addr_r2a(RelativeSectorAddress addr) {
    return addr + n_header_sectors + begin_sector_addr;
  }

  /// \param after inclusive
  /// \param before exclusive
  /// \return
  std::vector<LogEntry> get_entries(bool descending = true, uint32_t after = 0, uint32_t before = 0) {
    std::vector<LogEntry> entries;

    auto tmp_header_sector = std::make_unique<HeaderSector>();
    memcpy(tmp_header_sector.get(), current_header_sector.get(), sizeof(HeaderSector));

    // Recording which slot of the tmp_header_sector is currently pointing to;
    uint32_t tmp_slot_idx = current_slot_idx;
    uint32_t tmp_sector_idx = current_header_sector_idx;

    // Rules when iterating backward:
    // 1. if timestamp is zero (not set)
    // 1. If the timestamp is no longer monotonically decreasing, the inflecting point is the terminating point (tail reaching head)
    // 2. respecting the before and after. if they are zero, ignore.
    // 3. If the adjacent entries has overlapping data sectors

    uint64_t decreasing_timestamp = UINT64_MAX;

    auto last = previous_log_entry(tmp_header_sector, tmp_sector_idx, tmp_slot_idx);

    // check condition 3 for 'last'
    if ((last.timestamp != 0) && (before == 0 || last.timestamp < before) && (after == 0 || last.timestamp >= after)) {
      entries.push_back(last);
    }

    while (true) {
      // Load the previous one.
      auto& prev = previous_log_entry(tmp_header_sector, tmp_sector_idx, tmp_slot_idx);

      // Condition 1
      if (prev.timestamp == 0) {
        break;
      }

      // Condition 2
      if (prev.timestamp > decreasing_timestamp) {
        break;
      } else {
        decreasing_timestamp = prev.timestamp;
      }

      // Condition 3
      if ((before > 0 && prev.timestamp >= before) || (after > 0 && prev.timestamp < after)) {
        continue;
      }

      // Condition 4
      if (prev.begin_sector_offset <= last.end_sector_addr() && prev.end_sector_addr() >= last.end_sector_addr()) {
        break;
      }

      entries.push_back(prev);
    }

    if (!descending) {
      std::reverse(entries.begin(), entries.end());
    }
    return entries;
  }

  /// Remove all entries
  void clear() {
    for (int i = 0; i < n_header_sectors; ++i) {
      load_header_sector(i);
      current_header_sector->clear();
      sync_current_sector();
    }

    // Load initial state
    load_header_sector(0);
    current_slot_idx = 0;
    current_data_sector_offset = 0;
    previous_timestamp = 0;
  }
};
}  // namespace tsdb