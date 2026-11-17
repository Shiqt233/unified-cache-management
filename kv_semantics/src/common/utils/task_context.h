/**
 * MIT License
 *
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
#include <functional>
#include <memory>
#include <mutex>
#include <vector>
#include "kv_metrics/metrics.h"
#include "kv_types.h"
#include "trans_types.h"

namespace kv {

class AsuTransport;
class ConnectionChannel;
struct ClientTask;
class ProbeTaskLease;
struct TransportSubBatchContext;
struct ViewSnapshot;

enum class ClientTaskState {
    PENDING = 0,
    INFLIGHT = 1,
    FLOW_WAIT = 2,
    COMPLETED = 3,
};

enum class AsuOpType {
    QUERY = 0,
    LOAD = 1,
    // STORE is create-and-fill: absent keys are created, non-full keys may be
    // overwritten, and full keys reject later writes. It is non-idempotent on
    // outcome-unknown paths even though some partial writes may be repeatable.
    STORE = 2,
    BATCH_LOAD = 3,
    BATCH_STORE = 4,
    DELETE = 5,
    KEEP_ALIVE = 6,
};

enum class TaskOrigin : std::uint8_t {
    BUSINESS = 0,
    // KeepAlive probe tasks use the same transport machinery but do not represent
    // business writes and are completed with their original probe status.
    PROBE = 1,
};

enum class TransportTaskState {
    PENDING = 0,
    INFLIGHT = 1,
    FLOW_WAIT = 2,
    COMPLETED = 3,
};

enum class TransportSubBatchState {
    // No final result yet; this includes an attempt waiting for response or retry backoff.
    PENDING = 0,
    // Final result is fixed and aggregation may proceed; an earlier attempt may have
    // completed without making the whole subBatch COMPLETED.
    COMPLETED = 1,
};

inline bool IsEntryBatchOp(AsuOpType opType)
{
    return opType == AsuOpType::LOAD || opType == AsuOpType::STORE ||
           opType == AsuOpType::BATCH_LOAD || opType == AsuOpType::BATCH_STORE;
}

inline bool IsKeyBatchOp(AsuOpType opType)
{
    return opType == AsuOpType::DELETE || opType == AsuOpType::QUERY;
}

inline bool IsKeepAliveOp(AsuOpType opType) { return opType == AsuOpType::KEEP_ALIVE; }

using TransportSubBatchList = std::vector<TransportSubBatchContext>;

struct TransportTask {
    TransportTask();

    NodeId nodeId{0};
    TaskId taskId{kInvalidTaskId};
    AsuOpType opType{AsuOpType::QUERY};
    TaskOrigin origin{TaskOrigin::BUSINESS};
    std::shared_ptr<ConnectionChannel> targetChannel;
    std::uint64_t targetGeneration{0};
    std::shared_ptr<ProbeTaskLease> probeLease;
    std::weak_ptr<AsuTransport> transport;
    std::weak_ptr<ClientTask> clientTask;
    std::vector<CacheKey> keys;
    std::vector<KVBuffer> entries;
    std::vector<std::size_t> originalIndices;
    std::vector<Status> entryStatus;
    bool clientCompleted{false};

    std::shared_ptr<TransportSubBatchList> subBatchContexts;
    std::uint32_t remainingSubBatchCount{0};
    std::chrono::steady_clock::time_point deadline{std::chrono::steady_clock::time_point::max()};
    TaskCompletionCallback onComplete;
    std::atomic<bool> completionNotified{false};
    std::atomic<bool> cancelRequested{false};
    std::chrono::steady_clock::time_point submittedAt{};
    std::chrono::steady_clock::time_point sendCompletedAt{};
    std::function<void()> onSendComplete;
    std::atomic<bool> sendReturned{false};
    metrics::CachedMetric* asuCompletionMetric{nullptr};

    std::atomic<TransportTaskState> state{TransportTaskState::PENDING};
    Status finalStatus{Status::OK()};

    std::mutex mutex;

    bool Done() const;
    bool NotifyCompletion(TaskResult result);
    Status BuildFinalStatus() const;
    void InitializeRemainingSubBatchCount();
    void TryFinalizeFromSubBatches();
};

using TransportTaskPtr = std::shared_ptr<TransportTask>;

struct ClientTask {
    TaskId taskId{kInvalidTaskId};
    AsuOpType opType{AsuOpType::LOAD};
    std::function<Status()> prerequisite;
    std::shared_ptr<ViewSnapshot> viewSnapshot;
    std::vector<KVBuffer> entries;
    std::vector<CacheKey> keys;
    std::vector<TransportTaskPtr> transportTasks;
    std::vector<Status> entryStatus;
    QueryResult queryResult;

    std::atomic<std::size_t> remainingTransportTasks{0};
    std::atomic<std::size_t> remainingTransportSendTasks{0};
    std::chrono::steady_clock::time_point submittedAt{};
    std::chrono::steady_clock::time_point enqueuedAt{};
    std::chrono::steady_clock::time_point processingStartedAt{};
    std::atomic<bool> completionMetricRecorded{false};
    std::atomic<ClientTaskState> state{ClientTaskState::PENDING};
    std::atomic<bool> cancelRequested{false};
    Status finalStatus{Status::OK()};

    std::mutex waitMu;
    std::condition_variable cv;

    bool Done() const;
    bool AllTransportTasksCompleted() const;
};

using ClientTaskPtr = std::shared_ptr<ClientTask>;

}  // namespace kv
