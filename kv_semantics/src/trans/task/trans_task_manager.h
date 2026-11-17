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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <vector>
#include "buffer/buffer_manager.h"
#include "conn/connection_manager.h"
#include "kv_transport.h"
#include "task_context.h"
#include "task_manager_base.h"
#include "task/error_classifier.h"

namespace kv {

class SqeRequest;

// State of one send attempt. RETRY_BACKOFF means another attempt may still be
// scheduled; it is not the final completion state of the owning subBatch.
enum class AttemptState : std::uint8_t {
    READY,
    SENDING,
    WAITING_RESPONSE,
    FLOW_WAIT,
    RETRY_BACKOFF,
    COMPLETED,
    CANCELLED,
};

struct MRLifecycleLease {
    MRHandle canonicalHandle{kInvalidMRHandle};
    std::set<MRHandle> handles;
    std::uint32_t refCount{0};
    bool quarantined{false};
    bool unregisterRequested{false};
};

struct AttemptContext {
    std::uint64_t attemptId{0};
    std::uint16_t requestCid{0};
    std::uint64_t generation{0};
    AttemptState state{AttemptState::READY};
    std::chrono::steady_clock::time_point deadline{std::chrono::steady_clock::time_point::max()};
    bool sendAttempted{false};
    bool responseConfirmed{false};
    std::shared_ptr<ConnectionChannel> channel;
    std::shared_ptr<ConnectionGroup> group;
    std::shared_ptr<MRLifecycleLease> mrLease;
    std::shared_ptr<SqeRequest> request;
};

struct SubBatchRetryState {
    std::uint32_t busyAttempts{0};
    std::uint32_t failoverAttempts{0};
    std::set<ConnectionAttemptKey> attemptedGenerations;
    std::shared_ptr<ConnectionChannel> preferredChannel;
    Status lastError{Status::OK()};
    RetryAction retryAction{RetryAction::RETURN_RESULT};
    bool retryPending{false};
    std::chrono::steady_clock::time_point retryAt{std::chrono::steady_clock::time_point::min()};
    AcquireCode flowWaitReason{AcquireCode::OK};
    std::shared_ptr<ConnectionGroup> flowWaitGroup;
    std::set<std::uint32_t> quarantinedSendSlots;
    std::set<std::uint32_t> quarantinedFlagSlots;
};

struct TransportSubBatchContext {
    std::uint16_t cid{0};
    AsuOpType opType{AsuOpType::QUERY};
    TransportSubBatchState state{TransportSubBatchState::PENDING};
    Status status{Status::OK()};
    std::shared_ptr<ConnectionChannel> channel;
    bool useSeekControl{false};
    ScatterGatherEntry sendSge;
    ScatterGatherEntry flagBuffer;
    std::vector<Status> entryStatus;
    // Original request template. Each retry clones it into currentAttempt.request.
    std::shared_ptr<SqeRequest> request;
    AttemptContext currentAttempt;
    SubBatchRetryState retryState;
};

void FillEntryStatusFromCqeResult(const KvResponse& response,
                                  TransportSubBatchContext& subBatchContext);

class TransportTaskManager : public TaskManagerBase<TransportTask, TransportTaskState> {
public:
    TransportTaskManager() : TaskManagerBase(TransportTaskState::PENDING, "transport") {}

    void NotifyCompletion(const TransportTaskPtr& task);
    static void BuildResult(const TransportTask& task, TaskResult& result);
};

}  // namespace kv
