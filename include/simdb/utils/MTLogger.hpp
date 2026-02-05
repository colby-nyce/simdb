#pragma once

#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>

namespace simdb::utils {

/// This class handles multi-threaded logging to a file.
class MTLogger {
  public:
    class LogLine {
      public:
        LogLine(std::mutex &m, uint64_t id, std::ostream &out) : mutex_(m), id_(id), out_(out) {
            // Capture thread id as string
            std::ostringstream oss;
            oss << std::this_thread::get_id();
            thread_id_ = oss.str();
        }

        ~LogLine() {
            // Emit atomically
            std::lock_guard<std::mutex> lock(mutex_);

            out_ << "msg[id:" << id_ << ", thread:" << thread_id_ << "] " << stream_.str()
                 << std::endl;
        }

        // Handle generic types
        template <typename T> LogLine &operator<<(T &&v) {
            stream_ << std::forward<T>(v);
            return *this;
        }

        // Handle manipulators like std::endl
        LogLine &operator<<(std::ostream &(*manip)(std::ostream &)) {
            manip(stream_);
            return *this;
        }

      private:
        std::mutex &mutex_;
        uint64_t id_;
        std::ostream &out_;
        std::string thread_id_;
        std::ostringstream stream_;
    };

    /// Constructor (std::cout)
    MTLogger() : out_(std::cout) {}

    /// Constructor (file - std::cout if empty)
    MTLogger(const std::string &filename)
        : fout_(!filename.empty() ? std::make_unique<std::ofstream>(filename) : nullptr),
          out_(fout_ ? *fout_ : std::cout) {}

    LogLine operator()() { return LogLine(mutex_, id_counter_++, out_); }

  private:
    std::mutex mutex_;
    std::atomic<uint64_t> id_counter_{0};
    std::unique_ptr<std::ofstream> fout_;
    std::ostream &out_;
};

} // namespace simdb::utils
