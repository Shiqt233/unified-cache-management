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
#include <algorithm>
#include <chrono>
#include <utility>
#include "conn/connection_internal.h"
#include "kv_transport_impl.h"

namespace kv {

ClientProbeScheduler::ClientProbeScheduler(TransportConfig config)
    : config_(std::move(config)),
      budget_(std::make_shared<ProbeBudget>(config_.probeMaxInflight,
                                            config_.probeMaxSubmissionsPerRound))
{
}

ClientProbeScheduler::~ClientProbeScheduler()
{
    Stop();
    std::lock_guard<std::mutex> lock(registryMu_);
    transports_.clear();
}

void ClientProbeScheduler::Start()
{
    if (!config_.probeEnabled || worker_.joinable() || !ValidateProbeConfig(config_).ok()) {
        return;
    }
    stop_.store(false, std::memory_order_release);
    worker_ = std::thread(&ClientProbeScheduler::Loop, this);
}

void ClientProbeScheduler::Stop()
{
    stop_.store(true, std::memory_order_release);
    waitCv_.notify_all();
    if (worker_.joinable()) { worker_.join(); }
}

void ClientProbeScheduler::Loop()
{
    const auto basePeriod = std::max<std::chrono::milliseconds>(
        std::chrono::milliseconds(1), std::chrono::milliseconds(config_.probePeriodMs));
    const auto jitterMax = std::min<std::uint64_t>(50, static_cast<std::uint64_t>(basePeriod.count() / 10));
    std::uint64_t round = 0;
    while (!stop_.load(std::memory_order_acquire)) {
        RunOnce();
        const auto jitter = jitterMax == 0 ? 0 : round++ % (jitterMax + 1);
        std::unique_lock<std::mutex> lock(waitMu_);
        waitCv_.wait_for(lock, basePeriod + std::chrono::milliseconds(jitter),
                         [this] { return stop_.load(std::memory_order_acquire); });
    }
}

void ClientProbeScheduler::RegisterTransport(const std::shared_ptr<AsuTransport>& transport)
{
    auto* impl = dynamic_cast<AsuTransportImpl*>(transport.get());
    if (impl == nullptr || impl->connManager_ == nullptr) { return; }

    auto entry = std::make_shared<TransportEntry>();
    entry->transport = transport;
    entry->key = impl;
    entry->target = std::make_shared<ProbeTarget>(*impl, *impl->connManager_, impl->config_);

    std::lock_guard<std::mutex> lock(registryMu_);
    for (auto& existing : transports_) {
        if (existing->key != impl) { continue; }
        existing->transport = transport;
        if (existing->target == nullptr) { existing->target = entry->target; }
        return;
    }
    transports_.push_back(std::move(entry));
}

void ClientProbeScheduler::UnregisterTransport(const AsuTransport* transport)
{
    const auto* impl = dynamic_cast<const AsuTransportImpl*>(transport);
    if (impl == nullptr) { return; }
    std::lock_guard<std::mutex> lock(registryMu_);
    transports_.erase(std::remove_if(transports_.begin(), transports_.end(),
                                     [impl](const auto& entry) {
                                         return entry->key == impl;
                                     }),
                      transports_.end());
    transportCursor_ = transports_.empty() ? 0 : transportCursor_ % transports_.size();
}

std::size_t ClientProbeScheduler::GetRegisteredTransportCount() const
{
    std::lock_guard<std::mutex> lock(registryMu_);
    return transports_.size();
}

std::size_t ClientProbeScheduler::GetWorkerThreadCount() const
{
    return worker_.joinable() ? 1 : 0;
}

ClientProbeScheduler::Stats ClientProbeScheduler::GetStats() const
{
    return Stats{budget_->GetActive(), lastRoundTransports_.load(std::memory_order_acquire),
                 lastRoundChannels_.load(std::memory_order_acquire),
                 budget_->GetRoundSubmissions(), budget_->GetTotalSubmissions(),
                 worker_.joinable() && !stop_.load(std::memory_order_acquire)};
}

std::vector<std::shared_ptr<ClientProbeScheduler::TransportEntry>>
ClientProbeScheduler::VisitTransports()
{
    std::vector<std::shared_ptr<TransportEntry>> entries;
    std::lock_guard<std::mutex> lock(registryMu_);
    if (transports_.empty()) {
        transportCursor_ = 0;
        return entries;
    }

    const auto total = transports_.size();
    const auto start = transportCursor_ % total;
    const auto count = std::min<std::size_t>(config_.probeTransportScanBudget, total);
    entries.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
        entries.emplace_back(transports_[(start + index) % total]);
    }
    transportCursor_ = (start + entries.size()) % total;
    return entries;
}

std::shared_ptr<ProbeTaskLease> ClientProbeScheduler::AcquireLease(
    ProbeTarget& target, const std::shared_ptr<ConnectionChannel>& channel)
{
    if (!budget_->TryAcquire()) { return nullptr; }
    if (!target.TryStartProbe(channel)) {
        budget_->Release();
        return nullptr;
    }
    try {
        return std::make_shared<ProbeTaskLease>(budget_, channel);
    } catch (...) {
        channel->FinishProbe();
        budget_->Release();
        throw;
    }
}

void ClientProbeScheduler::RunOnce()
{
    std::lock_guard<std::mutex> runLock(runMu_);
    if (!config_.probeEnabled || stop_.load(std::memory_order_acquire)) { return; }
    budget_->BeginRound();
    lastRoundSubmissions_.store(0, std::memory_order_release);
    if (budget_->GetActive() >= budget_->limit) {
        lastRoundTransports_.store(0, std::memory_order_release);
        lastRoundChannels_.store(0, std::memory_order_release);
        lastRoundSubmissions_.store(0, std::memory_order_release);
        return;
    }

    const auto now = std::chrono::steady_clock::now();
    const auto idlePeriod = std::chrono::milliseconds(config_.probePeriodMs);
    const auto entries = VisitTransports();
    std::size_t submissions = 0;
    std::size_t channelsVisited = 0;

    for (const auto& entry : entries) {
        if (stop_.load(std::memory_order_acquire) ||
            submissions >= config_.probeMaxSubmissionsPerRound) {
            break;
        }
        if (!entry || !entry->target) { continue; }
        auto transport = entry->transport.lock();
        if (!transport) { continue; }
        auto* impl = dynamic_cast<AsuTransportImpl*>(transport.get());
        if (impl == nullptr || impl->connManager_ == nullptr) { continue; }

        auto& target = *entry->target;
        channelsVisited += target.VisitChannels(
            config_.probeChannelScanBudgetPerTransport,
            [this, &target, now, idlePeriod, &submissions](
                const std::shared_ptr<ConnectionChannel>& channel) {
                if (stop_.load(std::memory_order_acquire) ||
                    submissions >= config_.probeMaxSubmissionsPerRound) {
                    return false;
                }
                if (!target.IsIdle(channel, now, idlePeriod)) { return true; }

                auto lease = AcquireLease(target, channel);
                if (!lease) {
                    return budget_->GetActive() >= budget_->limit ? false : true;
                }
                const auto status = target.SubmitProbe(channel, lease, config_);
                ++submissions;
                totalSubmissions_.fetch_add(1, std::memory_order_relaxed);
                if (!status.ok()) { lease->Release(); }
                return submissions < config_.probeMaxSubmissionsPerRound &&
                       !stop_.load(std::memory_order_acquire);
            });
    }

    lastRoundTransports_.store(static_cast<std::uint32_t>(entries.size()), std::memory_order_release);
    lastRoundChannels_.store(static_cast<std::uint32_t>(channelsVisited), std::memory_order_release);
    lastRoundSubmissions_.store(budget_->GetRoundSubmissions(), std::memory_order_release);
}

}  // namespace kv
