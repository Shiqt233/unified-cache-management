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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include "task_context.h"
#include "conn/connection_manager.h"
#include "kv_types.h"

namespace kv {

class AsuTransportImpl;
class ProbeTaskLease;

// Client-wide or standalone-owner budget. No thread is associated with this object.
class ProbeBudget {
public:
    explicit ProbeBudget(std::uint32_t maxInflight, std::uint32_t maxSubmissionsPerRound = 16)
        : limit(maxInflight), roundLimit(maxSubmissionsPerRound) {}

    bool TryAcquire();
    void Release();
    bool TryAcquireSubmission();
    void BeginRound() { roundSubmissions_.store(0, std::memory_order_release); }
    std::uint32_t GetActive() const { return active.load(std::memory_order_acquire); }
    std::uint32_t GetRoundSubmissions() const { return roundSubmissions_.load(std::memory_order_acquire); }
    std::uint64_t GetTotalSubmissions() const { return totalSubmissions_.load(std::memory_order_acquire); }

    const std::uint32_t limit;
    const std::uint32_t roundLimit;

private:
    std::atomic<std::uint32_t> active{0};
    std::atomic<std::uint32_t> roundSubmissions_{0};
    std::atomic<std::uint64_t> totalSubmissions_{0};
};

// Owns one channel probe reservation and one budget reservation. Release is idempotent
// and is also called by the destructor, so every terminal path has a cleanup fallback.
class ProbeTaskLease {
public:
    ProbeTaskLease(std::shared_ptr<ProbeBudget> budget,
                   std::shared_ptr<ConnectionChannel> channel);
    ~ProbeTaskLease();

    ProbeTaskLease(const ProbeTaskLease&) = delete;
    ProbeTaskLease& operator=(const ProbeTaskLease&) = delete;

    void Release();
    bool TryAcquireSubmission();
    bool IsReleased() const { return released_.load(std::memory_order_acquire); }

private:
    std::shared_ptr<ProbeBudget> budget_;
    std::shared_ptr<ConnectionChannel> channel_;
    std::atomic<bool> released_{false};
};

// A ProbeTarget is a passive, per-Transport adapter. It never owns a std::thread.
// ClientProbeScheduler supplies the global thread and global ProbeBudget. RunOnce is
// also available for a caller-owned scheduler and deterministic tests.
class ProbeTarget {
public:
    ProbeTarget(AsuTransportImpl& transport, ConnectionManager& connectionManager,
                const TransportConfig& config);

    bool IsIdle(const std::shared_ptr<ConnectionChannel>& channel,
                std::chrono::steady_clock::time_point now,
                std::chrono::steady_clock::duration period) const;
    bool TryStartProbe(const std::shared_ptr<ConnectionChannel>& channel);
    std::size_t VisitChannels(std::size_t budget, const ConnectionManager::ChannelVisitor& visitor);
    Status SubmitProbe(const std::shared_ptr<ConnectionChannel>& channel,
                       const std::shared_ptr<ProbeTaskLease>& lease,
                       const TransportConfig& config);
    void RunOnce();

    const AsuTransportImpl* GetTransportAddress() const { return &transport_; }

private:
    AsuTransportImpl& transport_;
    ConnectionManager& connManager_;
    const TransportConfig& config_;
    std::shared_ptr<ProbeBudget> localBudget_;
    std::size_t channelCursor_{0};
};

// Backward-compatible name for the passive target. It no longer creates a thread.
using ProbeScheduler = ProbeTarget;

Status ValidateProbeConfig(const TransportConfig& config);

}  // namespace kv
