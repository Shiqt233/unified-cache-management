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
#include "probe_scheduler.h"
#include <chrono>
#include <utility>
#include "conn/connection_internal.h"
#include "kv_transport_impl.h"

namespace kv {

bool ProbeBudget::TryAcquire()
{
    auto current = active.load(std::memory_order_acquire);
    while (current < limit) {
        if (active.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel)) {
            return true;
        }
    }
    return false;
}

void ProbeBudget::Release()
{
    auto current = active.load(std::memory_order_acquire);
    while (current > 0) {
        if (active.compare_exchange_weak(current, current - 1, std::memory_order_acq_rel)) {
            return;
        }
    }
}

bool ProbeBudget::TryAcquireSubmission()
{
    auto current = roundSubmissions_.load(std::memory_order_acquire);
    while (current < roundLimit) {
        if (roundSubmissions_.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel)) {
            totalSubmissions_.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
    }
    return false;
}

ProbeTaskLease::ProbeTaskLease(std::shared_ptr<ProbeBudget> budget,
                               std::shared_ptr<ConnectionChannel> channel)
    : budget_(std::move(budget)), channel_(std::move(channel))
{
}

ProbeTaskLease::~ProbeTaskLease() { Release(); }

void ProbeTaskLease::Release()
{
    if (released_.exchange(true, std::memory_order_acq_rel)) { return; }
    if (channel_ != nullptr) { channel_->FinishProbe(); }
    if (budget_ != nullptr) { budget_->Release(); }
}

bool ProbeTaskLease::TryAcquireSubmission()
{
    if (released_.load(std::memory_order_acquire)) { return false; }
    return budget_ == nullptr || budget_->TryAcquireSubmission();
}

ProbeTarget::ProbeTarget(AsuTransportImpl& transport, ConnectionManager& connectionManager,
                         const TransportConfig& config)
    : transport_(transport), connManager_(connectionManager), config_(config),
      localBudget_(std::make_shared<ProbeBudget>(config.probeMaxInflight,
                                                 config.probeMaxSubmissionsPerRound))
{
}

bool ProbeTarget::IsIdle(const std::shared_ptr<ConnectionChannel>& channel,
                         std::chrono::steady_clock::time_point now,
                         std::chrono::steady_clock::duration period) const
{
    return channel != nullptr && channel->IsIdle(now, period);
}

bool ProbeTarget::TryStartProbe(const std::shared_ptr<ConnectionChannel>& channel)
{
    return channel != nullptr && connManager_.AcquireProbe(channel, channel->GetGeneration());
}

std::size_t ProbeTarget::VisitChannels(std::size_t budget,
                                       const ConnectionManager::ChannelVisitor& visitor)
{
    return connManager_.VisitChannels(channelCursor_, budget, visitor);
}

Status ProbeTarget::SubmitProbe(const std::shared_ptr<ConnectionChannel>& channel,
                                const std::shared_ptr<ProbeTaskLease>& lease,
                                const TransportConfig& config)
{
    if (channel == nullptr || lease == nullptr) {
        return Status::Error(StatusCode::INVALID_ARGUMENT, "probe channel or lease is null");
    }

    auto task = std::make_shared<TransportTask>();
    task->opType = AsuOpType::KEEP_ALIVE;
    task->origin = TaskOrigin::PROBE;
    task->targetChannel = channel;
    task->targetGeneration = channel->GetGeneration();
    task->entryStatus.assign(1, Status::OK());
    task->deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(config.probeTimeoutMs);
    task->probeLease = lease;
    if (!lease->TryAcquireSubmission()) {
        return Status::Error(StatusCode::RESOURCE_BUSY, "probe round submission budget exhausted");
    }
    return transport_.Submit(task);
}

void ProbeTarget::RunOnce()
{
    if (!config_.probeEnabled) { return; }
    localBudget_->BeginRound();

    const auto now = std::chrono::steady_clock::now();
    const auto idlePeriod = std::chrono::milliseconds(config_.probePeriodMs);
    std::size_t submissions = 0;
    (void)VisitChannels(config_.probeChannelScanBudgetPerTransport,
                        [this, now, idlePeriod, &submissions](
                            const std::shared_ptr<ConnectionChannel>& channel) {
                            if (submissions >= config_.probeMaxSubmissionsPerRound) {
                                return false;
                            }
                            if (!IsIdle(channel, now, idlePeriod)) { return true; }
                            if (!localBudget_->TryAcquire()) { return false; }
                            if (!TryStartProbe(channel)) {
                                localBudget_->Release();
                                return true;
                            }

                            std::shared_ptr<ProbeTaskLease> lease;
                            try {
                                lease = std::make_shared<ProbeTaskLease>(localBudget_, channel);
                            } catch (...) {
                                channel->FinishProbe();
                                localBudget_->Release();
                                throw;
                            }
                            const auto status = SubmitProbe(channel, lease, config_);
                            ++submissions;
                            if (!status.ok()) { lease->Release(); }
                            return submissions < config_.probeMaxSubmissionsPerRound;
                        });
}

Status ValidateProbeConfig(const TransportConfig& config)
{
    if (!config.probeEnabled) { return Status::OK(); }
    if (config.probePeriodMs == 0) {
        return Status::Error(StatusCode::INVALID_ARGUMENT,
                             "probePeriodMs must be greater than 0 when probing is enabled");
    }
    if (config.probeTimeoutMs == 0) {
        return Status::Error(StatusCode::INVALID_ARGUMENT,
                             "probeTimeoutMs must be greater than 0 when probing is enabled");
    }
    if (config.probeMaxInflight == 0) {
        return Status::Error(StatusCode::INVALID_ARGUMENT,
                             "probeMaxInflight must be greater than 0 when probing is enabled");
    }
    if (config.probeMaxSubmissionsPerRound == 0) {
        return Status::Error(StatusCode::INVALID_ARGUMENT,
                             "probeMaxSubmissionsPerRound must be greater than 0 when probing is enabled");
    }
    if (config.probeTransportScanBudget == 0) {
        return Status::Error(StatusCode::INVALID_ARGUMENT,
                             "probeTransportScanBudget must be greater than 0 when probing is enabled");
    }
    if (config.probeChannelScanBudgetPerTransport == 0) {
        return Status::Error(StatusCode::INVALID_ARGUMENT,
                             "probeChannelScanBudgetPerTransport must be greater than 0 when probing is enabled");
    }
    return Status::OK();
}

}  // namespace kv
