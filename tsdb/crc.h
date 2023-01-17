//
// Created by wuyua on 2023/1/17.
//
#pragma once
namespace tsdb {
template <typename T>
struct CRC {
 protected:
  uint32_t poly;
  uint32_t crc;

 public:
  explicit CRC(uint32_t poly = 0x04C11DB7, uint32_t init = UINT32_MAX) : poly(poly), crc(init) {}

  void update(const void* data, size_t len) {
    return static_cast<T*>(this)->update(data, len, poly);
  }

  uint32_t get() {
    return ~crc;
  }
};

struct CRCDefault : CRC<CRCDefault> {
  explicit CRCDefault(uint32_t poly = 0x04C11DB7, uint32_t init = UINT32_MAX) : CRC(poly, init) {}

  void update(const void* data, size_t len) {
    const auto* buffer = (const unsigned char*)data;

    while (len--) {
      crc = crc ^ (*buffer++ << 24);
      for (int bit = 0; bit < 8; bit++) {
        if (crc & (1L << 31))
          crc = (crc << 1) ^ poly;
        else
          crc = (crc << 1);
      }
    }
  }
};

}  // namespace tsdb