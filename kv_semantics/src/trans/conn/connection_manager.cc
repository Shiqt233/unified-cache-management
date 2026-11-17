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
#include <algorithm>
#include <chrono>
#include <cstdint>
#include "connection_internal.h"
#include "logger.h"

namespace kv {

ConnectionManager::ConnectionManager(TransProvider& provider, const std::string& localIp,
                                     std::uint32_t timeout, std::uint32_t maxErrorCount)
    : provider_(provider), localIp_(localIp), timeout_(timeout), maxErrorCount_(maxErrorCount)
{
}

ConnectionManager::~ConnectionManager() { Shutdown(); }

void ConnectionManager::ConfigureRecovery(std::uint32_t retryPerRound, std::uint64_t initialBackoffMs,
                                          std::uint64_t maxBackoffMs)
{
    connectRetryPerRound_ = retryPerRound;
    recoverInitialBackoffMs_ = initialBackoffMs;
    recoverMaxBackoffMs_ = maxBackoffMs;
    std::lock_guard<std::mutex> lock(recoverStateMu_);
    recoverStates_.clear();
}

Status ConnectionManager::CreateConnectionWithRetry(const NodeEndpoint& endpoint, std::uint32_t qpNum,
                                                    std::uint64_t initialBackoffMs,
                                                    std::vector<ConnectionHandle>& handles)
{
    Status lastStatus = Status::OK();
    auto backoffMs = initialBackoffMs;
    for (std::uint32_t attempt = 0; attempt <= connectRetryPerRound_; ++attempt) {
        handles.clear();
        lastStatus = provider_.CreateConnection(localIp_, endpoint.ip, endpoint.port, qpNum, timeout_,
                                                handles);
        if (lastStatus.ok() && !handles.empty()) { return lastStatus; }
        if (lastStatus.ok()) {
            lastStatus = Status::Error(StatusCode::INTERNAL_ERROR,
                                       "provider returned no connection handles");
        }
        if (attempt < connectRetryPerRound_) {
            if (backoffMs != 0) { std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs)); }
            backoffMs = backoffMs > recoverMaxBackoffMs_ / 2 ? recoverMaxBackoffMs_ : backoffMs * 2;
        }
    }
    return lastStatus;
}

ConnectionManager::RecoverState ConnectionManager::GetRecoverState(
    const std::shared_ptr<ConnectionChannel>& channel)
{
    std::lock_guard<std::mutex> lock(recoverStateMu_);
    const auto iter = recoverStates_.find(channel.get());
    return iter == recoverStates_.end() ? RecoverState{recoverInitialBackoffMs_,
                                                       std::chrono::steady_clock::time_point::min()}
                                        : iter->second;
}

void ConnectionManager::ScheduleRecoverFailure(const std::shared_ptr<ConnectionChannel>& channel,
                                               RecoverState state)
{
    state.nextRetryAt = std::chrono::steady_clock::now() + std::chrono::milliseconds(state.backoffMs);
    state.backoffMs = state.backoffMs > recoverMaxBackoffMs_ / 2 ? recoverMaxBackoffMs_
                                                                 : state.backoffMs * 2;
    std::lock_guard<std::mutex> lock(recoverStateMu_);
    recoverStates_[channel.get()] = state;
}

void ConnectionManager::ResetRecoverState(const std::shared_ptr<ConnectionChannel>& channel)
{
    std::lock_guard<std::mutex> lock(recoverStateMu_);
    recoverStates_.erase(channel.get());
}

Status ConnectionManager::AddGroup(const NodeEndpoint& endpoint, std::uint32_t qp_num)
{
    if (shuttingDown_.load(std::memory_order_acquire)) {
        return Status::Error(StatusCode::NOT_INITIALIZED, "connection manager shutting down");
    }
    KV_DEBUG("ConnectionManager::AddGroup endpoint={} qp_num={}", endpoint.ip, qp_num);

    std::vector<TransProvider::ConnectionHandle> handles;
    auto createStatus = CreateConnectionWithRetry(endpoint, qp_num, recoverInitialBackoffMs_, handles);
    if (!createStatus.ok()) { return createStatus; }
    if (handles.empty()) {
        return Status::Error(StatusCode::INTERNAL_ERROR, "provider returned no connection handles");
    }

    ServerKvCapabilities capabilities;
    auto capabilityStatus = provider_.GetServerCapabilities(handles.front(), capabilities);
    if (!capabilityStatus.ok() && capabilityStatus.code != StatusCode::UNSUPPORTED) {
        const auto deleteStatuses = provider_.DeleteConnections(handles);
        for (const auto& deleteStatus : deleteStatuses) {
            if (!deleteStatus.ok()) {
                KV_WARN(
                    "ConnectionManager::AddGroup cleanup failed after capability query failure: "
                    "code={} message={}",
                    static_cast<int>(deleteStatus.code), deleteStatus.message);
            }
        }
        return capabilityStatus;
    }
    if (capabilityStatus.code == StatusCode::UNSUPPORTED) {
        capabilities = {};
        KV_DEBUG(
            "ConnectionManager::AddGroup server capability query is not supported. Use default "
            "configurations instead");
    }
    if (capabilities.ioQueueDepth != 0) {
        maxInflightPerChannel_ = std::min(maxInflightPerChannel_, capabilities.ioQueueDepth);
    }

    {
        std::lock_guard<std::mutex> cacheLock(channelCacheMu_);
        std::unique_lock<std::shared_mutex> lock(structureMu_);
        auto gid = static_cast<std::uint32_t>(groups_.size());
        auto group = std::make_shared<ConnectionGroup>(gid, endpoint, capabilities);
        for (auto handle : handles) { group->AddChannel(handle, &provider_); }
        const auto effectiveMax = capabilities.ioQueueDepth != 0
                                      ? std::min(maxInflightPerChannel_, capabilities.ioQueueDepth)
                                      : maxInflightPerChannel_;
        group->ConfigureFlow(static_cast<std::uint64_t>(qp_num) * effectiveMax,
                             static_cast<std::uint64_t>(qp_num) * busyFloor_, flowControlEnabled_,
                             stableSuccessMs_, effectiveMax);
        groups_.push_back(std::move(group));

        for (const auto& channel : groups_.back()->GetChannels()) {
            channelCache_.push_back(channel);
        }
    }
    cacheDirty_.store(false, std::memory_order_release);
    KV_DEBUG("ConnectionManager::AddGroup OK");
    return Status::OK();
}

Status ConnectionManager::Shutdown()
{
    KV_DEBUG("ConnectionManager::Shutdown start");
    shuttingDown_.store(true, std::memory_order_release);
    StopRecoverLoop();

    channelCache_.clear();
    {
        std::unique_lock<std::shared_mutex> lock(drainMu_);
        drainList_.clear();
    }

    // Now safe to destroy ConnectionGroup objects
    {
        std::unique_lock<std::shared_mutex> lock(structureMu_);
        groups_.clear();
    }
    {
        std::lock_guard<std::mutex> lock(recoverStateMu_);
        recoverStates_.clear();
    }

    KV_DEBUG("ConnectionManager::Shutdown done");
    return Status::OK();
}

std::shared_ptr<ConnectionChannel> ConnectionManager::SelectConnection()
{
    return SelectConnection(std::set<ConnectionAttemptKey>{});
}

std::shared_ptr<ConnectionChannel> ConnectionManager::SelectConnection(
    const std::set<ConnectionAttemptKey>& excludedGenerations)
{
    ConnectionRequest request;
    request.excludedGenerations = excludedGenerations;
    auto result = TryAcquire(request);
    return result.ok() ? result.channel : nullptr;
}

std::shared_ptr<ConnectionChannel> ConnectionManager::SelectConnection(
    const std::set<const ConnectionChannel*>& excludedChannels)
{
    std::set<ConnectionAttemptKey> excluded;
    for (const auto* channel : excludedChannels) {
        if (channel != nullptr) { excluded.insert(MakeAttemptKey(*channel)); }
    }
    return SelectConnection(excluded);
}

AcquireResult ConnectionManager::TryAcquire(const ConnectionRequest& request)
{
    AcquireResult result;
    if (shuttingDown_.load(std::memory_order_acquire)) { return result; }

    const auto& preferred = request.preferredChannel;
    if (preferred == nullptr && request.probe) {
        result.status = Status::Error(StatusCode::CONNECTION_ERROR, "probe target is not specified");
        return result;
    }
    if (preferred != nullptr &&
        request.targetGeneration != 0 && preferred->GetGeneration() != request.targetGeneration) {
        result.channel = preferred;
        result.group = preferred->GetGroupLease();
        result.generation = preferred->GetGeneration();
        result.status = Status::Error(StatusCode::CONNECTION_ERROR, "probe target generation is stale");
        return result;
    }

    if (preferred != nullptr) {
        result.channel = preferred;
        result.group = preferred->GetGroupLease();
        result.generation = preferred->GetGeneration();
        if (request.probe) {
            if (AcquireProbe(preferred, preferred->GetGeneration())) {
                result.code = AcquireCode::OK;
                result.status = Status::OK();
                return result;
            }
            if (preferred->GetState() != ChannelState::ACTIVE) {
                result.code = AcquireCode::NO_ACTIVE;
                result.status = Status::Error(StatusCode::CONNECTION_ERROR, "probe target is not active");
            } else {
                result.code = AcquireCode::PROBE_BUSY;
                result.status = Status::Error(StatusCode::RESOURCE_BUSY, "probe already in flight");
            }
            return result;
        }
        if (AcquireChannel(preferred)) {
            result.code = AcquireCode::OK;
            result.status = Status::OK();
            return result;
        }
        if (preferred->GetState() != ChannelState::ACTIVE) {
            result.code = AcquireCode::NO_ACTIVE;
            result.status = Status::Error(StatusCode::CONNECTION_ERROR, "preferred channel is not active");
        } else if (!preferred->GetGroup()->CanAcquire()) {
            result.code = AcquireCode::GROUP_LIMIT;
            result.status = Status::Error(StatusCode::IN_PROGRESS, "group inflight limit reached");
        } else {
            result.code = AcquireCode::CHANNEL_FULL;
            result.status = Status::Error(StatusCode::IN_PROGRESS, "channel inflight limit reached");
        }
        return result;
    }

    auto channel = routingPolicy_ == RoutingPolicy::LEAST_LOADED
                       ? SelectByLeastLoaded(request.excludedGenerations)
                       : SelectByRoundRobin(request.excludedGenerations);
    if (channel != nullptr) {
        result.code = AcquireCode::OK;
        result.status = Status::OK();
        result.channel = channel;
        result.group = channel->GetGroupLease();
        result.generation = channel->GetGeneration();
        return result;
    }
    return BuildNoCandidateResult(request.excludedGenerations);
}

AcquireResult ConnectionManager::BuildNoCandidateResult(
    const std::set<ConnectionAttemptKey>& excludedGenerations)
{
    AcquireResult result;
    std::lock_guard<std::mutex> cacheLock(channelCacheMu_);
    if (cacheDirty_.load(std::memory_order_acquire)) { RebuildChannelCache(); }
    for (const auto& channel : channelCache_) {
        if (channel->GetState() != ChannelState::ACTIVE ||
            excludedGenerations.find(MakeAttemptKey(*channel)) != excludedGenerations.end()) {
            continue;
        }
        result.channel = channel;
        result.group = channel->GetGroupLease();
        result.generation = channel->GetGeneration();
        if (!channel->GetGroup()->CanAcquire()) {
            result.code = AcquireCode::GROUP_LIMIT;
            result.status = Status::Error(StatusCode::IN_PROGRESS, "group inflight limit reached");
        } else {
            result.code = AcquireCode::CHANNEL_FULL;
            result.status = Status::Error(StatusCode::IN_PROGRESS, "channel inflight limit reached");
        }
        return result;
    }
    result.status = Status::Error(StatusCode::CONNECTION_ERROR, "no active connection available");
    return result;
}

std::shared_ptr<ConnectionChannel> ConnectionManager::GetActiveConnection()
{
    if (shuttingDown_.load(std::memory_order_acquire)) { return nullptr; }

    std::lock_guard<std::mutex> cacheLock(channelCacheMu_);
    if (cacheDirty_.load(std::memory_order_acquire)) { RebuildChannelCache(); }

    for (const auto& channel : channelCache_) {
        if (channel->GetState() == ChannelState::ACTIVE) { return channel; }
    }
    return nullptr;
}

void ConnectionManager::SetRoutingPolicy(RoutingPolicy policy) { routingPolicy_ = policy; }

void ConnectionManager::ConfigureFlow(bool enabled, std::uint32_t busyFloor,
                                      std::uint64_t stableSuccessMs)
{
    flowControlEnabled_ = enabled;
    busyFloor_ = busyFloor;
    stableSuccessMs_ = stableSuccessMs;
}

bool ConnectionManager::AcquireChannel(const std::shared_ptr<ConnectionChannel>& channel)
{
    if (channel == nullptr || shuttingDown_.load(std::memory_order_acquire)) { return false; }
    std::lock_guard<std::mutex> cacheLock(channelCacheMu_);
    if (channel->GetState() != ChannelState::ACTIVE ||
        channel->GetInflightCount() >= channel->GetGroup()->GetMaxInflightPerChannel() ||
        !channel->GetGroup()->CanAcquire()) {
        return false;
    }
    channel->IncrementInflight();
    channel->GetGroup()->IncrementInflight();
    return true;
}

bool ConnectionManager::IsCurrentGeneration(const std::shared_ptr<ConnectionChannel>& channel,
                                            std::uint64_t generation) const
{
    if (channel == nullptr || generation == 0) { return generation == 0; }
    auto leasedGroup = channel->GetGroupLease();
    const ConnectionGroup* group = leasedGroup != nullptr ? leasedGroup.get() : channel->GetGroup();
    if (group == nullptr) { return false; }
    std::shared_lock<std::shared_mutex> lock(structureMu_);
    for (const auto& current : group->GetChannels()) {
        if (current->GetLogicalSlotId() == channel->GetLogicalSlotId() &&
            current->GetGeneration() == generation) {
            return true;
        }
    }
    return false;
}

bool ConnectionManager::AcquireProbe(const std::shared_ptr<ConnectionChannel>& channel,
                                     std::uint64_t generation)
{
    return channel != nullptr && channel->TryStartProbe(generation);
}

std::vector<std::shared_ptr<ConnectionChannel>> ConnectionManager::GetChannelsSnapshot() const
{
    std::vector<std::shared_ptr<ConnectionChannel>> channels;
    std::shared_lock<std::shared_mutex> lock(structureMu_);
    for (const auto& group : groups_) {
        const auto& groupChannels = group->GetChannels();
        channels.insert(channels.end(), groupChannels.begin(), groupChannels.end());
    }
    return channels;
}

std::size_t ConnectionManager::VisitChannels(std::size_t& cursor, std::size_t budget,
                                             const ChannelVisitor& visitor)
{
    if (budget == 0 || !visitor) { return 0; }

    std::vector<std::shared_ptr<ConnectionChannel>> window;
    std::size_t total = 0;
    std::size_t start = 0;
    {
        std::shared_lock<std::shared_mutex> lock(structureMu_);
        for (const auto& group : groups_) { total += group->GetChannels().size(); }
        if (total == 0) {
            cursor = 0;
            return 0;
        }
        start = cursor % total;
        const auto count = std::min(budget, total);
        window.reserve(count);
        for (std::size_t index = 0; index < count; ++index) {
            auto position = (start + index) % total;
            for (const auto& group : groups_) {
                const auto& channels = group->GetChannels();
                if (position >= channels.size()) {
                    position -= channels.size();
                    continue;
                }
                window.emplace_back(channels[position]);
                break;
            }
        }
    }

    std::size_t visited = 0;
    for (const auto& channel : window) {
        ++visited;
        if (!visitor(channel)) { break; }
    }
    cursor = (start + visited) % total;
    return visited;
}

void ConnectionManager::ReportFailure(const std::shared_ptr<ConnectionChannel>& channel)
{
    if (channel == nullptr) { return; }
    if (shuttingDown_.load(std::memory_order_acquire)) { return; }
    auto oldCount = channel->FetchAddErrorCount(1);
    KV_DEBUG("ConnectionManager::ReportFailure ch_id={} group_id={} error_count={} threshold={}",
             channel->GetChannelId(), channel->GetGroup()->GetGroupId(), oldCount + 1,
             maxErrorCount_);
    if (oldCount + 1 < maxErrorCount_) {
        KV_DEBUG("ConnectionManager::ReportFailure below threshold, skip drain");
        return;
    }

    if (!channel->MarkForDrain()) {
        KV_DEBUG(
            "ConnectionManager::ReportFailure MarkForDrain CAS failed "
            "(already draining/failed)");
        return;
    }

    KV_DEBUG("ConnectionManager::ReportFailure MarkForDrain OK, ch_id={} state=DRAINING",
             channel->GetChannelId());
    cacheDirty_.store(true, std::memory_order_release);

    {
        std::unique_lock<std::shared_mutex> lock(drainMu_);
        for (const auto& existing : drainList_) {
            if (existing.get() == channel.get()) return;
        }
        drainList_.push_back(channel);
    }
    channel->GetGroup()->NotifyWaiters();
}

void ConnectionManager::ReportSuccess(const std::shared_ptr<ConnectionChannel>& channel)
{
    if (channel == nullptr) { return; }
    channel->ResetErrorCount();
}

void ConnectionManager::StartRecoverLoop()
{
    KV_DEBUG("ConnectionManager::StartRecoverLoop start");
    if (recoverWorker_.joinable()) { return; }
    stopRecover_.store(false, std::memory_order_release);
    recoverWorker_ = std::thread(&ConnectionManager::RecoverLoop, this);
}

void ConnectionManager::StopRecoverLoop()
{
    KV_DEBUG("ConnectionManager::StopRecoverLoop start");
    stopRecover_.store(true, std::memory_order_release);
    if (recoverWorker_.joinable()) { recoverWorker_.join(); }
}

void ConnectionManager::RecoverLoop()
{
    KV_DEBUG("ConnectionManager::RecoverLoop started");
    const auto requeue = [this](const std::shared_ptr<ConnectionChannel>& channel) {
        std::unique_lock<std::shared_mutex> lock(drainMu_);
        for (const auto& existing : drainList_) {
            if (existing.get() == channel.get()) { return; }
        }
        drainList_.push_back(channel);
    };

    while (!stopRecover_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kRecoverIntervalMs));
        if (stopRecover_.load(std::memory_order_acquire)) { break; }

        std::vector<std::shared_ptr<ConnectionChannel>> to_recover;
        {
            std::unique_lock<std::shared_mutex> lock(drainMu_);
            to_recover.swap(drainList_);
        }

        for (auto& channel : to_recover) {
            auto state = GetRecoverState(channel);
            if (state.nextRetryAt > std::chrono::steady_clock::now()) {
                requeue(channel);
                continue;
            }

            ConnectionGroup* grp = channel->GetGroup();
            const auto endpoint = grp->GetEndpoint();
            channel->SetState(ChannelState::RECONNECTING);
            std::vector<TransProvider::ConnectionHandle> newHandles;
            const auto createStatus = CreateConnectionWithRetry(endpoint, 1, state.backoffMs, newHandles);
            if (!createStatus.ok()) {
                channel->SetState(ChannelState::FAILED);
                KV_DEBUG("ConnectionManager::RecoverLoop rebuild failed, schedule backoff group_id={}",
                         grp->GetGroupId());
                ScheduleRecoverFailure(channel, state);
                requeue(channel);
                continue;
            }

            {
                std::unique_lock<std::shared_mutex> lock(structureMu_);
                grp->RemoveChannel(channel.get());
                auto newChannel = grp->AddChannel(newHandles[0], &provider_, channel->GetLogicalSlotId());
                grp->NotifyWaiters();
                KV_DEBUG("ConnectionManager::RecoverLoop channel recovered group_id={} old_ch_id={} "
                         "new_ch_id={} logicalSlot={} generation={}",
                         grp->GetGroupId(), channel->GetChannelId(), newChannel->GetChannelId(),
                         newChannel->GetLogicalSlotId(), newChannel->GetGeneration());
            }
            ResetRecoverState(channel);
            cacheDirty_.store(true, std::memory_order_release);
        }
    }
    KV_DEBUG("ConnectionManager::RecoverLoop stopped");
}

void ConnectionManager::RebuildChannelCache()
{
    if (!cacheDirty_.load(std::memory_order_acquire)) { return; }
    std::shared_lock<std::shared_mutex> struct_lock(structureMu_);
    std::vector<std::shared_ptr<ConnectionChannel>> new_cache;
    for (const auto& g : groups_) {
        for (const auto& channel : g->GetChannels()) {
            if (channel->GetState() == ChannelState::ACTIVE) { new_cache.push_back(channel); }
        }
    }
    channelCache_ = std::move(new_cache);
    cacheDirty_.store(false, std::memory_order_release);
}

std::shared_ptr<ConnectionChannel> ConnectionManager::SelectByRoundRobin(
    const std::set<ConnectionAttemptKey>& excludedGenerations)
{
    std::lock_guard<std::mutex> cacheLock(channelCacheMu_);
    if (cacheDirty_.load(std::memory_order_acquire)) { RebuildChannelCache(); }

    if (channelCache_.empty()) { return nullptr; }

    auto idx = rrIndex_.fetch_add(1, std::memory_order_relaxed);
    std::size_t total = channelCache_.size();
    std::size_t start = idx % total;
    const auto maxInflightPerChannel = maxInflightPerChannel_;

    for (std::size_t i = 0; i < total; ++i) {
        std::size_t pos = (start + i) % total;
        const auto& channel = channelCache_[pos];
        if (excludedGenerations.find(MakeAttemptKey(*channel)) != excludedGenerations.end()) { continue; }
        if (channel->GetState() != ChannelState::ACTIVE) { continue; }
        const auto maxInflightPerChannel = channel->GetGroup()->GetMaxInflightPerChannel();
        if (channel->GetInflightCount() < maxInflightPerChannel && channel->GetGroup()->CanAcquire()) {
            channel->IncrementInflight();
            channel->GetGroup()->IncrementInflight();
            return channel;
        }
    }
    KV_DEBUG(
        "ConnectionManager::SelectByRoundRobin no available channel: all ACTIVE channels "
        "reached maximum inflight limit, max_inflight_per_channel={}",
        maxInflightPerChannel);
    return nullptr;
}

std::shared_ptr<ConnectionChannel> ConnectionManager::SelectByLeastLoaded(
    const std::set<ConnectionAttemptKey>& excludedGenerations)
{
    std::lock_guard<std::mutex> cacheLock(channelCacheMu_);
    if (cacheDirty_.load(std::memory_order_acquire)) { RebuildChannelCache(); }

    if (channelCache_.empty()) { return nullptr; }

    std::shared_ptr<ConnectionChannel> selected;
    std::uint32_t min_inflight = std::numeric_limits<std::uint32_t>::max();
    const auto maxInflightPerChannel = maxInflightPerChannel_;

    for (const auto& channel : channelCache_) {
        if (excludedGenerations.find(MakeAttemptKey(*channel)) != excludedGenerations.end()) { continue; }
        auto inflight = channel->GetInflightCount();
        const auto maxInflightPerChannel = channel->GetGroup()->GetMaxInflightPerChannel();
        if (channel->GetState() == ChannelState::ACTIVE && inflight < maxInflightPerChannel &&
            inflight < min_inflight && channel->GetGroup()->CanAcquire()) {
            min_inflight = inflight;
            selected = channel;
            if (min_inflight == 0) break;
        }
    }

    if (selected) {
        selected->IncrementInflight();
        selected->GetGroup()->IncrementInflight();
        return selected;
    }
    KV_DEBUG(
        "ConnectionManager::SelectByLeastLoaded no available channel: all ACTIVE channels "
        "reached maximum inflight limit, max_inflight_per_channel={}",
        maxInflightPerChannel);
    return nullptr;
}

Status ConnectionManager::GetHealthSnapshot(NodeHealthSnapshot& snapshot) const
{
    if (shuttingDown_.load(std::memory_order_acquire)) {
        return Status::Error(StatusCode::NOT_INITIALIZED, "connection manager shutting down");
    }
    snapshot = NodeHealthSnapshot{};
    std::shared_lock<std::shared_mutex> structureLock(structureMu_);
    std::lock_guard<std::mutex> recoverLock(recoverStateMu_);
    std::uint32_t total = 0;
    for (const auto& group : groups_) {
        for (const auto& channel : group->GetChannels()) {
            ++total;
            snapshot.generation = std::max(snapshot.generation, channel->GetGeneration());
            switch (channel->GetState()) {
            case ChannelState::ACTIVE:
                ++snapshot.activeCount;
                break;
            case ChannelState::DRAINING:
                ++snapshot.drainingCount;
                break;
            case ChannelState::RECONNECTING:
                ++snapshot.reconnectingCount;
                break;
            case ChannelState::FAILED:
                ++snapshot.failedCount;
                break;
            }
            const auto recoverIt = recoverStates_.find(channel.get());
            if (recoverIt != recoverStates_.end()) {
                snapshot.nextRetryAt = std::min(snapshot.nextRetryAt, recoverIt->second.nextRetryAt);
            }
        }
    }
    snapshot.health = snapshot.activeCount == 0
                          ? NodeHealth::RED
                          : (snapshot.activeCount < total ? NodeHealth::YELLOW : NodeHealth::GREEN);
    return Status::OK();
}

std::int64_t ConnectionManager::TotalInflightCount()
{
    if (shuttingDown_.load(std::memory_order_acquire)) { return 0; }
    std::int64_t sum = 0;
    std::shared_lock<std::shared_mutex> lock(structureMu_);
    for (const auto& group : groups_) {
        for (const auto& channel : group->GetChannels()) { sum += channel->GetInflightCount(); }
    }
    return sum;
}

std::vector<ServerKvCapabilities> ConnectionManager::GetServerCapabilities()
{
    std::vector<ServerKvCapabilities> capabilities;
    std::shared_lock<std::shared_mutex> lock(structureMu_);
    capabilities.reserve(groups_.size());
    for (const auto& group : groups_) { capabilities.push_back(group->GetServerCapabilities()); }
    return capabilities;
}

}  // namespace kv
