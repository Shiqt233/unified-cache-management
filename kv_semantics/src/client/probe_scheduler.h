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
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include "kv_types.h"
#include "task/probe_scheduler.h"

namespace kv {

class AsuTransport;
class AsuTransportImpl;

// One shared scheduler per KvClientImpl. ProbeTarget instances never own threads.
class ClientProbeScheduler {
public:
    struct Stats {
        std::uint32_t activeProbeCount{0};
        std::uint32_t lastRoundTransports{0};
        std::uint32_t lastRoundChannels{0};
        std::uint32_t lastRoundSubmissions{0};
        std::uint64_t totalSubmissions{0};
        bool workerRunning{false};
    };

    explicit ClientProbeScheduler(TransportConfig config);
    ~ClientProbeScheduler();

    ClientProbeScheduler(const ClientProbeScheduler&) = delete;
    ClientProbeScheduler& operator=(const ClientProbeScheduler&) = delete;

    void Start();
    void Stop();
    // Execute one bounded round. Also usable by an external/standalone scheduler.
    void RunOnce();

    void RegisterTransport(const std::shared_ptr<AsuTransport>& transport);
    void UnregisterTransport(const AsuTransport* transport);

    std::size_t GetRegisteredTransportCount() const;
    std::size_t GetWorkerThreadCount() const;
    Stats GetStats() const;

private:
    struct TransportEntry {
        std::weak_ptr<AsuTransport> transport;
        std::shared_ptr<ProbeTarget> target;
        const AsuTransportImpl* key{nullptr};
    };

    std::vector<std::shared_ptr<TransportEntry>> VisitTransports();
    std::shared_ptr<ProbeTaskLease> AcquireLease(
        ProbeTarget& target, const std::shared_ptr<ConnectionChannel>& channel);
    void Loop();

    TransportConfig config_;
    std::shared_ptr<ProbeBudget> budget_;

    mutable std::mutex registryMu_;
    std::vector<std::shared_ptr<TransportEntry>> transports_;
    std::size_t transportCursor_{0};

    std::mutex runMu_;
    std::atomic<bool> stop_{false};
    std::thread worker_;
    std::mutex waitMu_;
    std::condition_variable waitCv_;

    std::atomic<std::uint32_t> lastRoundTransports_{0};
    std::atomic<std::uint32_t> lastRoundChannels_{0};
    std::atomic<std::uint32_t> lastRoundSubmissions_{0};
    std::atomic<std::uint64_t> totalSubmissions_{0};
};

}  // namespace kv
