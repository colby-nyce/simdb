#pragma once

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace simdb::utils {

class SelfProfiler
{
public:
    static SelfProfiler* getInstance() {
        static SelfProfiler profiler;
        return &profiler;
    }

    ~SelfProfiler() {
        if (results_.empty()) {
            return;
        }

        std::cout << "Self-profiler by method, sorted by average time:\n\n";

        std::vector<double> sorted_avg_times;
        for (const auto& [method_name, method_results] : results_) {
            sorted_avg_times.push_back(method_results.second / method_results.first);
        }

        std::sort(sorted_avg_times.begin(), sorted_avg_times.end(), std::greater<double>());
        for (auto avg_time : sorted_avg_times) {
            for (const auto& [method_name, method_results] : results_) {
                if (method_results.second / method_results.first != avg_time) {
                    continue;
                }

                std::cout << std::left << std::setw(80) << std::setfill('.') << method_name << " -- ";
                std::cout << "called " << method_results.first << " time" << (method_results.first > 1 ? "s" : "") << " ";
                std::cout << ", " << std::setprecision(6) << method_results.second / method_results.first << " avg S+C seconds\n";
            }
        }
    }

    class MethodTimer
    {
    public:
        MethodTimer(const char* method_name)
            : method_name_(method_name)
            , start_time_(now_())
        {}

        ~MethodTimer() {
            auto end_time = now_();
            auto elap = end_time - start_time_;
            auto dur = std::chrono::duration<double>(elap);
            SelfProfiler::getInstance()->update_(method_name_, dur.count());
        }

    private:
        using TimeT = std::chrono::high_resolution_clock::time_point;

        static TimeT now_() {
            return std::chrono::high_resolution_clock::now();
        }

        std::string method_name_;
        TimeT start_time_;
    };

    MethodTimer profile(const char* method_name) const {
        return MethodTimer(method_name);
    }

private:
    SelfProfiler() = default;
    using MethodResults = std::pair<uint64_t, double>;                 // num calls, S+C time
    using AllResults = std::unordered_map<std::string, MethodResults>; // method name, method results
    AllResults results_;

    void update_(const std::string& method_name, double elap_seconds) {
        if (results_.find(method_name) == results_.end()) {
            results_[method_name] = std::make_pair(1, elap_seconds);
        } else {
            auto& res = results_[method_name];
            res.first++;
            res.second += elap_seconds;
        }
    }

    friend class MethodTimer;
};

} // namespace simdb::utils

#define CONCAT(a, b) CONCAT_INNER(a, b)
#define CONCAT_INNER(a, b) a##b

#define PROFILE_BLOCK(block_name) \
    auto CONCAT(__block_timer_, __COUNTER__) = simdb::utils::SelfProfiler::getInstance()->profile(block_name);

#define PROFILE_METHOD PROFILE_BLOCK(__FUNCTION__)
