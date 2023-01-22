//
// Created by wuyua on 2023/1/15.
//

#pragma once
#include <chrono>

#include "common.h"
#include "exception.h"
#include "header_sectors_manager.h"
#include "io.h"
#include "partition.h"
#include "sector_defs.h"

namespace tsdb {

struct SeriesConfig {
  // Used to determine how many header sector is required. Will be round up to the sector's capacity
  uint32_t max_entries;
  uint32_t max_file_size;
};

template <typename IO, typename CRC = CRCDefault, typename ClockType = std::chrono::system_clock>
struct Series {
  using HeaderSectorsManagerType = HeaderSectorsManager<IO, CRC, ClockType>;
  explicit Series(IO& io, const Partition& partition, const SeriesConfig& cfg)
      : io(io), cfg(cfg), partition(partition) {
    assert(n_header_sectors > 0);
    assert(n_total_sectors > 0);
    assert(cfg.max_entries > 0);
    assert(cfg.max_file_size > 0);

    assert(n_header_sectors < n_total_sectors);
  }

 private:
  IO& io;
  SeriesConfig cfg;
  Partition partition;

  uint32_t n_header_sectors{cfg.max_entries / HeaderSector::n_entries + 1};
  uint32_t n_total_sectors{partition.n_sectors};

  HeaderSectorsManagerType header_sectors_manager{io, partition.begin_sector_addr, n_header_sectors, n_total_sectors};

  std::mutex lock{};

 public:
  const Partition& get_partition() {
    return partition;
  }

  const SeriesConfig& get_series_config() {
    return cfg;
  }

  /// Insert whole buffer at once
  void insert(const void* buffer, uint32_t len, uint32_t timestamp = 0) {
    assert(buffer);
    assert(len);
    assert(len <= cfg.max_file_size);

    std::lock_guard g(lock);

    if (timestamp == 0) {
      timestamp = duration_cast<std::chrono::seconds>(ClockType::now().time_since_epoch()).count();
    }

    CRC crc_computer;
    crc_computer.update(buffer, len);
    auto checksum = crc_computer.get();

    RelativeSectorAddress relative_sector_address = header_sectors_manager.add_log(len, checksum, timestamp);
    AbsoluteSectorAddress absolute_sector_address = header_sectors_manager.sector_addr_r2a(relative_sector_address);

    write_data_sectors(buffer, len, absolute_sector_address);
  }

  struct InsertTransaction {
    InsertTransaction(IO& io, HeaderSectorsManagerType& header_sectors_manager, LogEntry& entry, std::mutex& lock) : entry(entry), io(io), header_sectors_manager(header_sectors_manager), lock(lock) {}

    void write(void* buf, uint32_t len) {
      if (is_finalized) {
        throw Error("Overflow");
      }
      assert(len % sector_size == 0);
      assert(written_length + len <= entry.size);
      crc_computer.update(buf, len);
      size_t required_sectors = min_sector_for_size(len);
      io.write_sectors(buf, header_sectors_manager.sector_addr_r2a(entry.begin_sector_offset) + write_sector_idx, required_sectors);
      write_sector_idx += required_sectors;

      written_length += len;
      if (written_length == entry.size) {
        finalize();
      }
    }

    virtual ~InsertTransaction() {
      finalize();
    }

   private:
    IO& io;
    HeaderSectorsManagerType& header_sectors_manager;
    CRC crc_computer;
    std::mutex& lock;

    LogEntry& entry;
    uint32_t write_sector_idx{0};
    uint32_t written_length{0};

   public:
    bool is_finalized{false};
    void finalize() {
      if (is_finalized) {
        return;
      }
      entry.checksum = crc_computer.get();
      header_sectors_manager.advance_slot();
      lock.unlock();
      is_finalized = true;
    }
  };

  InsertTransaction begin_insert_transaction(uint32_t len, uint32_t timestamp = 0) {
    assert(len);
    assert(len <= cfg.max_file_size);

    lock.lock();

    if (timestamp == 0) {
      timestamp = duration_cast<std::chrono::seconds>(ClockType::now().time_since_epoch()).count();
    }

    auto& entry = header_sectors_manager.add_log_partial(len, timestamp);
    return {io, header_sectors_manager, entry, lock};
  }

  struct DataLogEntry {
   public:
    DataLogEntry(const LogEntry& log_entry, uint32_t data_sector_begin_addr, IO& io) : log_entry(log_entry), data_sector_begin_addr(data_sector_begin_addr), io(io) {}

   public:
    const LogEntry log_entry;

    CRC crc_computer;

    ///
    /// \param out
    /// \param len
    /// \return read bytes
    uint32_t read(void* out, uint32_t len) {
      assert(len % sector_size == 0 || len + sector_size * idx == log_entry.size);

      if (idx > min_sector_for_size(log_entry.size)) {
        return 0;
      }
      len = std::min(len, log_entry.size - sector_size * idx);
      if (len == 0) {
        return 0;
      }
      io.read_bytes_from_sectors(out, len, data_sector_begin_addr + log_entry.begin_sector_offset + idx);
      crc_computer.update(out, len);
      idx += min_sector_for_size(len);
      return len;
    }

    uint32_t get_accumulated_crc() {
      return crc_computer.get();
    }

   protected:
    uint32_t data_sector_begin_addr{};
    uint32_t idx{0};
    IO& io;
  };

  template <typename TCb>
    requires std::is_invocable_r_v<bool, TCb, DataLogEntry&>
  void iterate(const TCb& fcn, bool descending = true, uint32_t after = 0, uint32_t before = 0) {
    std::lock_guard g(lock);
    auto entries = header_sectors_manager.get_entries(descending, after, before);

    for (auto& e : entries) {
      DataLogEntry data_log_entry(e, header_sectors_manager.sector_addr_r2a(0), io);
      if (!fcn(data_log_entry)) {
        break;
      }
    }
  }

  void clear() {
    std::lock_guard g(lock);
    header_sectors_manager.clear();
  }

 protected:
  void write_data_sectors(const void* buffer, uint32_t len, AbsoluteSectorAddress begin_sector_addr) {
    io.write_sectors(buffer, begin_sector_addr, min_sector_for_size(len));
  }
};
}  // namespace tsdb