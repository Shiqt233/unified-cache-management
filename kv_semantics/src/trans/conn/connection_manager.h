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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include "kv_transport.h"
#include "kv_types.h"
#include "proto/kv_protocol.h"
#include "trans_provider.h"

namespace kv {

using ConnectionHandle = void*;

class ConnectionChannel;
class ConnectionGroup;

// Stable identity of a logical QP incarnation. logicalSlotId is reused across
// reconnects, while generation changes on every successful replacement.
struct ConnectionAttemptKey {
    std::uint32_t groupId{0};
    std::uint64_t logicalSlotId{0};
    std::uint64_t generation{0};

    bool operator<(const ConnectionAttemptKey& other) const noexcept
    {
        return groupId != other.groupId ? groupId < other.groupId
                                       : logicalSlotId != other.logicalSlotId
                                             ? logicalSlotId < other.logicalSlotId
                                             : generation < other.generation;
    }
    bool operator==(const ConnectionAttemptKey& other) const noexcept
    {
        return groupId == other.groupId && logicalSlotId == other.logicalSlotId &&
               generation == other.generation;
    }
};

enum class AcquireCode : std::uint8_t {
    OK,
    NO_ACTIVE,
    CHANNEL_FULL,
    GROUP_LIMIT,
    PROBE_BUSY,
};

struct ConnectionRequest {
    std::set<ConnectionAttemptKey> excludedGenerations;
    std::shared_ptr<ConnectionChannel> preferredChannel;
    bool probe{false};
    std::uint64_t targetGeneration{0};
};

struct AcquireResult {
    AcquireCode code{AcquireCode::NO_ACTIVE};
    Status status{Status::Error(StatusCode::CONNECTION_ERROR, "no active connection")};
    std::shared_ptr<ConnectionChannel> channel;
    std::shared_ptr<ConnectionGroup> group;
    std::uint64_t generation{0};

    bool ok() const { return code == AcquireCode::OK && channel != nullptr; }
};

enum class RoutingPolicy : std::uint8_t {
    ROUND_ROBIN,
    LEAST_LOADED,
};

class ConnectionChannel;
class ConnectionGroup;
struct ScatterGatherEntry;

class ConnectionManager {
public:
    ConnectionManager(TransProvider& provider, const std::string& localIp, std::uint32_t timeout,
                      std::uint32_t maxErrorCount = 2);
    ~ConnectionManager();

    Status AddGroup(const NodeEndpoint& endpoint, std::uint32_t qp_num);
    Status Shutdown();

    std::shared_ptr<ConnectionChannel> SelectConnection();
    std::shared_ptr<ConnectionChannel> SelectConnection(
        const std::set<ConnectionAttemptKey>& excludedGenerations);
    // Backward-compatible pointer exclusion used by older tests and callers.
    std::shared_ptr<ConnectionChannel> SelectConnection(
        const std::set<const ConnectionChannel*>& excludedChannels);
    // Detailed admission result used by FLOW_WAIT and probe paths.
    AcquireResult TryAcquire(const ConnectionRequest& request);
    std::shared_ptr<ConnectionChannel> GetActiveConnection();
    bool AcquireChannel(const std::shared_ptr<ConnectionChannel>& channel);
    bool AcquireProbe(const std::shared_ptr<ConnectionChannel>& channel, std::uint64_t generation);
    bool IsCurrentGeneration(const std::shared_ptr<ConnectionChannel>& channel,
                             std::uint64_t generation) const;
    Status GetHealthSnapshot(NodeHealthSnapshot& snapshot) const;
    std::vector<std::shared_ptr<ConnectionChannel>> GetChannelsSnapshot() const;
    using ChannelVisitor = std::function<bool(const std::shared_ptr<ConnectionChannel>&)>;
    // Visit at most budget channels and advance cursor by the number actually visited.
    // The visitor runs after the structure lock is released and receives a bounded window.
    std::size_t VisitChannels(std::size_t& cursor, std::size_t budget,
                              const ChannelVisitor& visitor);
    void SetRoutingPolicy(RoutingPolicy policy);
    void ConfigureFlow(bool enabled, std::uint32_t busyFloor, std::uint64_t stableSuccessMs);
    void ConfigureRecovery(std::uint32_t retryPerRound, std::uint64_t initialBackoffMs,
                           std::uint64_t maxBackoffMs);
    void SetMaxInflightPerChannel(std::uint32_t maxInflight) { maxInflightPerChannel_ = maxInflight; }
    void ReportFailure(const std::shared_ptr<ConnectionChannel>& channel);
    void ReportSuccess(const std::shared_ptr<ConnectionChannel>& channel);

    void StartRecoverLoop();
    void StopRecoverLoop();

    std::int64_t TotalInflightCount();
    std::vector<ServerKvCapabilities> GetServerCapabilities();

private:
    std::vector<std::shared_ptr<ConnectionGroup>> groups_;
    mutable std::shared_mutex structureMu_;  // shared_mutex allows concurrent reads

    std::vector<std::shared_ptr<ConnectionChannel>> channelCache_;
    std::mutex channelCacheMu_;
    std::atomic<bool> cacheDirty_{false};

    std::atomic<bool> shuttingDown_{false};

    std::atomic<std::uint32_t> rrIndex_{0};
    RoutingPolicy routingPolicy_{RoutingPolicy::ROUND_ROBIN};
    static constexpr std::uint64_t kRecoverIntervalMs = 100;

    std::thread recoverWorker_;
    std::atomic<bool> stopRecover_{false};

    std::shared_mutex drainMu_;
    std::vector<std::shared_ptr<ConnectionChannel>> drainList_;

    struct RecoverState {
        std::uint64_t backoffMs{100};
        std::chrono::steady_clock::time_point nextRetryAt{std::chrono::steady_clock::time_point::min()};
    };
    std::uint32_t connectRetryPerRound_{0};
    std::uint64_t recoverInitialBackoffMs_{100};
    std::uint64_t recoverMaxBackoffMs_{5000};
    mutable std::mutex recoverStateMu_;
    std::unordered_map<const ConnectionChannel*, RecoverState> recoverStates_;

    TransProvider& provider_;
    std::string localIp_;
    std::uint32_t timeout_;
    std::uint32_t maxErrorCount_;
    std::uint32_t maxInflightPerChannel_{256};
    bool flowControlEnabled_{false};
    std::uint32_t busyFloor_{32};
    std::uint64_t stableSuccessMs_{60000};

    void RecoverLoop();
    void RebuildChannelCache();
    Status CreateConnectionWithRetry(const NodeEndpoint& endpoint, std::uint32_t qpNum,
                                     std::uint64_t initialBackoffMs,
                                     std::vector<ConnectionHandle>& handles);
    RecoverState GetRecoverState(const std::shared_ptr<ConnectionChannel>& channel);
    void ScheduleRecoverFailure(const std::shared_ptr<ConnectionChannel>& channel,
                                RecoverState state);
    void ResetRecoverState(const std::shared_ptr<ConnectionChannel>& channel);
    AcquireResult BuildNoCandidateResult(const std::set<ConnectionAttemptKey>& excluded);
    std::shared_ptr<ConnectionChannel> SelectByRoundRobin(
        const std::set<ConnectionAttemptKey>& excludedGenerations = {});
    std::shared_ptr<ConnectionChannel> SelectByLeastLoaded(
        const std::set<ConnectionAttemptKey>& excludedGenerations = {});
};

}  // namespace kv
