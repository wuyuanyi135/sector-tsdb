//
// Created by wuyua on 2023/1/15.
//

#pragma once
#include <string>
namespace tsdb {

struct Error : std::runtime_error {
  explicit Error(const std::string& msg) : runtime_error(msg) {}
};

struct CorruptedDataError : Error {
  explicit CorruptedDataError(const std::string& msg) : Error(msg) {}
};

struct IOError : Error {
  explicit IOError(const std::string& msg) : Error(msg) {}
};
}  // namespace tsdb