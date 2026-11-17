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
#include "group_flow_controller.h"
#include <algorithm>

namespace kv {

void GroupFlowController::Configure(std::uint64_t normalLimit, std::uint64_t busyFloor, bool enabled,
                                    std::uint64_t stableSuccessMs)
{
    std::lock_guard<std::mutex> lock(mu_);
    configured_ = true;
    enabled_ = enabled;
    normalLimit_ = normalLimit;
    busyFloor_ = std::min(busyFloor, normalLimit);
    currentLimit_ = normalLimit;
    congested_ = false;
    stableSuccessMs_ = stableSuccessMs;
    stableSuccessStartedAt_.reset();
    lastSuccessAt_.reset();
    flowCv_.notify_all();
}

bool GroupFlowController::CanAcquire(std::uint32_t inflight) const
{
    std::lock_guard<std::mutex> lock(mu_);
    if (!configured_ || !enabled_) { return true; }
    return static_cast<std::uint64_t>(inflight) < currentLimit_;
}

void GroupFlowController::OnBusy()
{
    std::lock_guard<std::mutex> lock(mu_);
    if (!configured_ || !enabled_) { return; }
    currentLimit_ = std::max(busyFloor_, currentLimit_ > busyFloor_ + 2 ? currentLimit_ - 2
                                                                       : busyFloor_);
    congested_ = true;
    stableSuccessStartedAt_.reset();
}

void GroupFlowController::OnBusinessSuccess(std::chrono::steady_clock::time_point now)
{
    std::lock_guard<std::mutex> lock(mu_);
    if (!configured_ || !enabled_) { return; }

    const auto stableDuration = std::chrono::milliseconds(stableSuccessMs_);
    if (lastSuccessAt_.has_value() && now - *lastSuccessAt_ > stableDuration) {
        stableSuccessStartedAt_.reset();
    }
    if (!stableSuccessStartedAt_.has_value()) { stableSuccessStartedAt_ = now; }
    lastSuccessAt_ = now;
    currentLimit_ = std::min(normalLimit_, currentLimit_ + 1);

    if (currentLimit_ == normalLimit_ ||
        (stableSuccessStartedAt_.has_value() && now - *stableSuccessStartedAt_ >= stableDuration)) {
        congested_ = false;
        currentLimit_ = normalLimit_;
    }
    flowCv_.notify_one();
}

void GroupFlowController::OnNonSuccess()
{
    std::lock_guard<std::mutex> lock(mu_);
    if (!configured_ || !enabled_) { return; }
    stableSuccessStartedAt_.reset();
}

GroupFlowController::Snapshot GroupFlowController::GetSnapshot() const
{
    std::lock_guard<std::mutex> lock(mu_);
    return Snapshot{configured_, enabled_, congested_, normalLimit_, busyFloor_, currentLimit_,
                    stableSuccessStartedAt_, lastSuccessAt_};
}

void GroupFlowController::WaitForRelease(std::chrono::milliseconds maxWait)
{
    std::unique_lock<std::mutex> lock(mu_);
    flowCv_.wait_for(lock, maxWait);
}

void GroupFlowController::NotifyRelease() { flowCv_.notify_one(); }

void GroupFlowController::NotifyAll() { flowCv_.notify_all(); }

}  // namespace kv
