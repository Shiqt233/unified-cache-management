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
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>
#include "connection_manager.h"
#include "group_flow_controller.h"
#include "kv_transport.h"
#include "kv_types.h"
#include "trans_provider.h"

namespace kv {

enum class ChannelState : std::uint8_t {
    ACTIVE,
    DRAINING,
    FAILED,
    RECONNECTING,
};

class ConnectionChannel {
public:
    ConnectionChannel(std::uint32_t id, ConnectionGroup* grp,
                      TransProvider::ConnectionHandle handle, TransProvider* provider,
                      std::uint64_t generation = 1, std::uint64_t logicalSlotId = 0);
    ~ConnectionChannel();

    std::uint32_t GetChannelId() const { return channelId; }
    std::uint64_t GetLogicalSlotId() const { return logicalSlotId; }
    std::uint64_t GetGeneration() const { return generation; }
    ChannelState GetState() const { return state.load(std::memory_order_acquire); }
    std::uint32_t GetInflightCount() const { return inflightCount.load(std::memory_order_acquire); }
    ConnectionGroup* GetGroup() const { return group; }
    std::shared_ptr<ConnectionGroup> GetGroupLease() const { return groupLease.lock(); }
    void SetGroupLease(const std::weak_ptr<ConnectionGroup>& lease) { groupLease = lease; }
    TransProvider::ConnectionHandle GetConnection() const { return handle_; }

    std::uint32_t FetchAddErrorCount(std::uint32_t val);
    std::uint32_t GetErrorCount() const;
    void ResetErrorCount();

    // Test helper to set state directly
    void SetState(ChannelState s) { state.store(s, std::memory_order_release); }
    void SetInflightCount(std::uint32_t c) { inflightCount.store(c, std::memory_order_release); }

    void IncrementInflight();
    void ReleaseInflight();
    bool MarkForDrain();
    void MarkBusinessSubmit();
    bool IsIdle(std::chrono::steady_clock::time_point now,
                std::chrono::steady_clock::duration period) const;
    bool TryStartProbe(std::uint64_t expectedGeneration);
    void FinishProbe();
    bool IsProbeInFlight() const { return probeInFlight.load(std::memory_order_acquire); }

private:
    // inflightCount: Maintained by upper-level users, it records the number of requests currently
    // being processed on the channel
    alignas(64) std::atomic<std::uint32_t> inflightCount{0};
    alignas(64) std::atomic<ChannelState> state{ChannelState::ACTIVE};

    std::uint32_t channelId{0};
    std::uint64_t logicalSlotId{0};
    std::uint64_t generation{1};
    ConnectionGroup* group{nullptr};
    std::weak_ptr<ConnectionGroup> groupLease;
    TransProvider::ConnectionHandle handle_{nullptr};
    TransProvider* provider_{nullptr};

    // Cold path
    std::atomic<std::uint32_t> errorCount{0};
    std::atomic<std::int64_t> lastBusinessSubmitNs{0};
    std::atomic<bool> probeInFlight{false};
};

class ConnectionGroup : public std::enable_shared_from_this<ConnectionGroup> {
public:
    ConnectionGroup(std::uint32_t id, const NodeEndpoint& ep,
                    const ServerKvCapabilities& capabilities = {});

    std::uint32_t GetGroupId() const { return groupId; }
    const NodeEndpoint& GetEndpoint() const { return endpoint; }
    const ServerKvCapabilities& GetServerCapabilities() const { return serverCapabilities; }
    const std::vector<std::shared_ptr<ConnectionChannel>>& GetChannels() const { return channels; }

    std::shared_ptr<ConnectionChannel> AddChannel(ConnectionHandle handle, TransProvider* provider,
                                                  std::uint64_t logicalSlotId = UINT64_MAX);
    void RemoveChannel(ConnectionChannel* channel);
    void NotifyWaiters();
    bool HasActiveChannel() const;
    void ConfigureFlow(std::uint64_t normalLimit, std::uint64_t busyFloor, bool enabled,
                       std::uint64_t stableSuccessMs, std::uint32_t maxInflightPerChannel);
    bool CanAcquire() const;
    void IncrementInflight();
    void ReleaseInflight();
    std::uint32_t GetInflightCount() const { return inflightCount.load(std::memory_order_acquire); }
    std::uint32_t GetMaxInflightPerChannel() const { return maxInflightPerChannel_; }
    void OnBusy();
    void OnBusinessSuccess(std::chrono::steady_clock::time_point now);
    void OnNonSuccess();
    GroupFlowController::Snapshot GetFlowSnapshot() const;
    void WaitForFlowRelease(std::chrono::milliseconds maxWait);

private:
    std::uint32_t groupId{0};
    NodeEndpoint endpoint;
    ServerKvCapabilities serverCapabilities;
    std::vector<std::shared_ptr<ConnectionChannel>> channels;
    std::atomic<std::uint32_t> nextChannelId_{0};
    std::atomic<std::uint64_t> nextGeneration_{1};
    GroupFlowController flowController_;
    std::atomic<std::uint32_t> inflightCount{0};
    std::uint32_t maxInflightPerChannel_{256};
};

inline ConnectionAttemptKey MakeAttemptKey(const ConnectionChannel& channel)
{
    const auto* group = channel.GetGroup();
    return ConnectionAttemptKey{group == nullptr ? 0 : group->GetGroupId(),
                                channel.GetLogicalSlotId(), channel.GetGeneration()};
}

}  // namespace kv
