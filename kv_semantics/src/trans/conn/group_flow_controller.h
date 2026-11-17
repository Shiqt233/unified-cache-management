/**
 * MIT License
 *
 * Copyright (c) 2026 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * */
#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>

namespace kv {

class GroupFlowController {
public:
    struct Snapshot {
        bool configured{false};
        bool enabled{false};
        bool congested{false};
        std::uint64_t normalLimit{0};
        std::uint64_t busyFloor{0};
        std::uint64_t currentLimit{0};
        std::optional<std::chrono::steady_clock::time_point> stableSuccessStartedAt;
        std::optional<std::chrono::steady_clock::time_point> lastSuccessAt;
    };

    void Configure(std::uint64_t normalLimit, std::uint64_t busyFloor, bool enabled,
                   std::uint64_t stableSuccessMs);
    bool CanAcquire(std::uint32_t inflight) const;
    void OnBusy();
    void OnBusinessSuccess(std::chrono::steady_clock::time_point now);
    void OnNonSuccess();
    Snapshot GetSnapshot() const;
    // Bounded wait used by FLOW_WAIT. Notifications never change ownership of inflight.
    void WaitForRelease(std::chrono::milliseconds maxWait);
    void NotifyRelease();
    void NotifyAll();

private:
    mutable std::mutex mu_;
    std::condition_variable flowCv_;
    bool configured_{false};
    bool enabled_{false};
    bool congested_{false};
    std::uint64_t normalLimit_{0};
    std::uint64_t busyFloor_{0};
    std::uint64_t currentLimit_{0};
    std::uint64_t stableSuccessMs_{60000};
    std::optional<std::chrono::steady_clock::time_point> stableSuccessStartedAt_;
    std::optional<std::chrono::steady_clock::time_point> lastSuccessAt_;
};

}  // namespace kv
