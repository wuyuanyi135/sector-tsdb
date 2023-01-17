//
// Created by wuyua on 2023/1/15.
//
#pragma once
#include "fmt/format.h"

namespace tsdb {

template <typename T>
struct Logger {
  enum struct Level {
    Error,
    Warning,
    Info,
    Debug,
    Verbose,
  };

  void log(Level level, const std::string& msg) {
    static_cast<T*>(this)->log(level, msg);
  }

  void error(const std::string& msg) { this->log(Level::Error, msg); }

  void warning(const std::string& msg) { this->log(Level::Warning, msg); }

  void info(const std::string& msg) { this->log(Level::Info, msg); }

  void debug(const std::string& msg) { this->log(Level::Debug, msg); }

  void verbose(const std::string& msg) { this->log(Level::Verbose, msg); }
};

struct NullLogger : Logger<NullLogger> {
  void log(Level level, const std::string& msg) {}
};

struct FMTLogger : Logger<FMTLogger> {
  void log(Level level, const std::string& msg) {
    const char* level_str;
    switch (level) {
      case Level::Error:
        level_str = "Error";
        break;
      case Level::Warning:
        level_str = "Warning";
        break;
      case Level::Info:
        level_str = "Info";
        break;
      case Level::Debug:
        level_str = "Debug";
        break;
      case Level::Verbose:
        level_str = "Verbose";
        break;
    }
    fmt::print("[{}] {}\n", level_str, msg);
  }
};
}  // namespace tsdb