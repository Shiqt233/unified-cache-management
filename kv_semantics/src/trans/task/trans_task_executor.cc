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
#include "trans_task_executor.h"
#include "probe_scheduler.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <string>
#include <utility>
#include "conn/connection_internal.h"
#include "kv_metrics/metrics.h"
#include "logger.h"
#include "utils/trans_task_utils.h"

namespace kv {

namespace {

std::shared_ptr<MRLifecycleLease> MakeTaskMrLease(const TransportTask& task)
{
    auto lease = std::make_shared<MRLifecycleLease>();
    for (const auto& entry : task.entries) {
        if (entry.buffer.handle == kInvalidMRHandle) { continue; }
        lease->handles.insert(entry.buffer.handle);
        if (lease->canonicalHandle == kInvalidMRHandle) {
            lease->canonicalHandle = entry.buffer.handle;
        }
    }
    lease->refCount = lease->handles.empty() ? 0 : 1;
    return lease;
}

}  // namespace

TransportTaskExecutor::TransportTaskExecutor(
    const TransportConfig& config, const std::shared_ptr<TransProvider>& transProvider,
    const std::unique_ptr<ConnectionManager>& connectionManager)
    : config_(config),
      ioScheduler_(config),
      transProvider_(transProvider),
      connManager_(connectionManager),
      retryController_(config),
      asuSendMetric_("kv_transport_node_task_send_duration_seconds",
                     {
                         {"node_id", std::to_string(config.nodeId)}
}),
      asuCompletionMetric_("kv_transport_node_task_completion_duration_seconds",
                           {{"node_id", std::to_string(config.nodeId)}})
{
}

TransportTaskExecutor::~TransportTaskExecutor()
{
    const auto status = Shutdown();
    if (!status.ok()) {
        KV_WARN("TransportTaskExecutor destructor cleanup failed: code={} message={}",
                static_cast<int>(status.code), status.message);
    }
}

Status TransportTaskExecutor::Init()
{
    (void)metrics::RegisterMetricLabels(asuSendMetric_.Name(), asuSendMetric_.Labels());
    (void)metrics::RegisterMetricLabels(asuCompletionMetric_.Name(), asuCompletionMetric_.Labels());
    auto status = sendBufferManager_.Init("asu send buffer", MemoryType::HOST_PINNED,
                                          config_.sendBufferSlotSize, config_.sendBufferSlotNum);
    if (!status.ok()) {
        (void)Shutdown();
        return status;
    }

    status = flagBufferManager_.Init("asu flag buffer", MemoryType::HOST_PINNED,
                                     config_.flagBufferSlotSize, config_.flagBufferSlotNum);
    if (!status.ok()) {
        (void)Shutdown();
        return status;
    }

    status = RegisterBufferMemory(sendBufferManager_, sendBufferMrHandle_);
    if (!status.ok()) {
        const auto registerStatus =
            Status::Error(status.code, "failed to register send buffer: " + status.message);
        const auto shutdownStatus = Shutdown();
        if (!shutdownStatus.ok()) {
            KV_WARN(
                "TransportTaskExecutor::Init cleanup failed after send buffer registration "
                "failure: code={} message={}",
                static_cast<int>(shutdownStatus.code), shutdownStatus.message);
        }
        return registerStatus;
    }

    status = RegisterBufferMemory(flagBufferManager_, flagBufferMrHandle_);
    if (!status.ok()) {
        const auto registerStatus =
            Status::Error(status.code, "failed to register flag buffer: " + status.message);
        const auto shutdownStatus = Shutdown();
        if (!shutdownStatus.ok()) {
            KV_WARN(
                "TransportTaskExecutor::Init cleanup failed after flag buffer registration "
                "failure: code={} message={}",
                static_cast<int>(shutdownStatus.code), shutdownStatus.message);
        }
        return registerStatus;
    }
    return Status::OK();
}

Status TransportTaskExecutor::Shutdown()
{
    ReleaseQuarantinedSlots();
    Status finalStatus = Status::OK();
    auto status = UnregisterBufferMemory(flagBufferMrHandle_);
    if (!status.ok()) {
        KV_WARN("Failed to unregister flag buffer: code={} message={}",
                static_cast<int>(status.code), status.message);
        finalStatus =
            Status::Error(status.code, "failed to unregister flag buffer: " + status.message);
    }

    status = UnregisterBufferMemory(sendBufferMrHandle_);
    if (!status.ok()) {
        KV_WARN("Failed to unregister send buffer: code={} message={}",
                static_cast<int>(status.code), status.message);
        if (finalStatus.ok()) {
            finalStatus =
                Status::Error(status.code, "failed to unregister send buffer: " + status.message);
        }
    }

    if (flagBufferMrHandle_ == kInvalidMRHandle) { flagBufferManager_.Shutdown(); }
    if (sendBufferMrHandle_ == kInvalidMRHandle) { sendBufferManager_.Shutdown(); }
    return finalStatus;
}

Status TransportTaskExecutor::RegisterBufferMemory(BufferManager& bufferManager, MRHandle& mrHandle)
{
    if (!transProvider_) {
        return Status::Error(StatusCode::NOT_INITIALIZED, "TransProvider is null");
    }

    TransProvider::RegisterMemoryDesc desc;
    auto status = bufferManager.GetRegisterMemoryDesc(desc);
    if (!status.ok()) { return status; }

    std::vector<MRHandle> mrHandles;
    status = transProvider_->RegisterMemory({desc}, mrHandles);
    if (!status.ok()) {
        return Status::Error(StatusCode::INTERNAL_ERROR,
                             "provider memory registration failed: " + status.message);
    }
    if (mrHandles.empty() || mrHandles[0] == kInvalidMRHandle) {
        return Status::Error(StatusCode::INTERNAL_ERROR, "provider returned no valid MR handle");
    }

    mrHandle = mrHandles[0];
    std::uint32_t tokenId = 0;
    status = transProvider_->GetMemTokenId(mrHandle, tokenId);
    if (!status.ok()) {
        const auto unregisterStatus = UnregisterBufferMemory(mrHandle);
        if (!unregisterStatus.ok()) {
            KV_WARN("Failed to unregister memory after token lookup failure: {}",
                    unregisterStatus.message);
        }
        return Status::Error(StatusCode::INTERNAL_ERROR,
                             "memory token lookup failed: " + status.message);
    }

    bufferManager.SetTokenId(tokenId);
    return Status::OK();
}

Status TransportTaskExecutor::UnregisterBufferMemory(MRHandle& mrHandle)
{
    if (mrHandle == kInvalidMRHandle) { return Status::OK(); }
    if (!transProvider_) {
        return Status::Error(StatusCode::NOT_INITIALIZED, "TransProvider is null");
    }

    const auto statuses =
        transProvider_->UnregisterMemory({TransProvider::UnregisterMemoryDesc{mrHandle}});
    for (const auto& status : statuses) {
        if (!status.ok()) {
            return Status::Error(status.code,
                                 "provider memory unregistration failed: " + status.message);
        }
    }
    mrHandle = kInvalidMRHandle;
    return Status::OK();
}

// Release only resources whose provider outcome is known. A sent attempt without
// a confirmed response keeps its slots and CID quarantined until final shutdown;
// this is deliberately conservative because the provider has no drain-proof API.
void TransportTaskExecutor::ReleaseSubBatchResources(TransportSubBatchContext& subBatchContext)
{
    const bool shouldQuarantine = subBatchContext.currentAttempt.sendAttempted &&
                                  !subBatchContext.currentAttempt.responseConfirmed;
    if (subBatchContext.sendSge.slot_index != UINT32_MAX) {
        const auto slotIndex = subBatchContext.sendSge.slot_index;
        if (shouldQuarantine) {
            const auto status = sendBufferManager_.Quarantine(slotIndex);
            if (!status.ok()) {
                KV_WARN("Failed to quarantine send buffer slot({}): {}", slotIndex, status.message);
            } else {
                subBatchContext.retryState.quarantinedSendSlots.insert(slotIndex);
                std::lock_guard<std::mutex> lock(cidMu_);
                quarantinedSendSlots_.insert(slotIndex);
            }
        } else {
            const auto status = sendBufferManager_.Free(slotIndex);
            if (!status.ok()) {
                KV_ERROR("Failed to release sub-batch send buffer slot({}): {}", slotIndex,
                         status.message);
            }
        }
        subBatchContext.sendSge = {};
    }

    if (subBatchContext.flagBuffer.slot_index != UINT32_MAX) {
        const auto slotIndex = subBatchContext.flagBuffer.slot_index;
        if (shouldQuarantine) {
            const auto status = flagBufferManager_.Quarantine(slotIndex);
            if (!status.ok()) {
                KV_WARN("Failed to quarantine flag buffer slot({}): {}", slotIndex, status.message);
            } else {
                subBatchContext.retryState.quarantinedFlagSlots.insert(slotIndex);
                std::lock_guard<std::mutex> lock(cidMu_);
                quarantinedFlagSlots_.insert(slotIndex);
            }
        } else {
            const auto status = flagBufferManager_.Free(slotIndex);
            if (!status.ok()) {
                KV_ERROR("Failed to release sub-batch flag buffer slot({}): {}", slotIndex,
                         status.message);
            }
        }
        subBatchContext.flagBuffer = {};
    }

    if (shouldQuarantine) {
        if (subBatchContext.currentAttempt.mrLease != nullptr) {
            subBatchContext.currentAttempt.mrLease->quarantined = true;
            std::lock_guard<std::mutex> lock(cidMu_);
            quarantinedMrHandles_.insert(subBatchContext.currentAttempt.mrLease->handles.begin(),
                                         subBatchContext.currentAttempt.mrLease->handles.end());
        }
    } else {
        subBatchContext.currentAttempt.mrLease.reset();
        const auto cid = subBatchContext.currentAttempt.requestCid != 0
                             ? subBatchContext.currentAttempt.requestCid
                             : subBatchContext.cid;
        // A request that was never sent has no future CQE to protect against.
        if (!subBatchContext.currentAttempt.sendAttempted) { ReleaseRequestCid(cid); }
    }

    if (subBatchContext.opType == AsuOpType::KEEP_ALIVE) {
        if (subBatchContext.channel != nullptr) {
            subBatchContext.channel->FinishProbe();
        } else if (subBatchContext.currentAttempt.channel != nullptr) {
            subBatchContext.currentAttempt.channel->FinishProbe();
        }
    } else if (subBatchContext.channel != nullptr) {
        subBatchContext.channel->ReleaseInflight();
        if (subBatchContext.currentAttempt.group != nullptr) {
            subBatchContext.currentAttempt.group->ReleaseInflight();
        } else if (subBatchContext.channel->GetGroup() != nullptr) {
            subBatchContext.channel->GetGroup()->ReleaseInflight();
        }
    }
    subBatchContext.channel = nullptr;
    subBatchContext.currentAttempt.channel.reset();
    subBatchContext.currentAttempt.group.reset();
}

void TransportTaskExecutor::ReleaseAllSubBatchResources(
    std::vector<TransportSubBatchContext>& subBatchContexts)
{
    for (auto& subBatchContext : subBatchContexts) { ReleaseSubBatchResources(subBatchContext); }
}

// Abort before Provider::Send. Unsent entries are marked IN_PROGRESS rather than
// OK or another application result: no server-side execution can be inferred. The
// first failed subBatch keeps its original error; later unsent subBatches are canceled.
void TransportTaskExecutor::AbortSubBatchesBeforeSend(
    TransportTask& task, std::vector<TransportSubBatchContext>& subBatchContexts)
{
    const auto notSentStatus = Status::Error(StatusCode::IN_PROGRESS, "sub-batch not sent");
    const auto canceledStatus =
        Status::Error(StatusCode::CANCELED, "sub-batch canceled after pre-send failure");
    std::fill(task.entryStatus.begin(), task.entryStatus.end(), notSentStatus);

    bool hasFailedSubBatch = false;
    for (auto& subBatchContext : subBatchContexts) {
        ReleaseSubBatchResources(subBatchContext);
        if (!subBatchContext.status.ok()) {
            hasFailedSubBatch = true;
        } else if (hasFailedSubBatch) {
            std::fill(subBatchContext.entryStatus.begin(), subBatchContext.entryStatus.end(),
                      canceledStatus);
            subBatchContext.status = canceledStatus;
        } else {
            std::fill(subBatchContext.entryStatus.begin(), subBatchContext.entryStatus.end(),
                      notSentStatus);
            subBatchContext.status = notSentStatus;
        }
        subBatchContext.state = TransportSubBatchState::COMPLETED;
        subBatchContext.currentAttempt.state = AttemptState::CANCELLED;
    }
}

// Complete the final subBatch result, not merely one attempt. Do not call this
// while another attempt is allowed. Retry paths may release an attempt's resources
// earlier, but only this final transition fixes the final status and decrements
// the count used by TransportTask aggregation.
void TransportTaskExecutor::CompleteSubBatch(TransportTask& task,
                                             TransportSubBatchContext& subBatchContext,
                                             const Status& status)
{
    ReleaseSubBatchResources(subBatchContext);
    subBatchContext.state = TransportSubBatchState::COMPLETED;
    subBatchContext.status = status;
    subBatchContext.currentAttempt.state = AttemptState::COMPLETED;
    subBatchContext.retryState.retryPending = false;
    subBatchContext.retryState.retryAt = std::chrono::steady_clock::time_point::min();
    subBatchContext.retryState.lastError = status;
    subBatchContext.retryState.flowWaitReason = AcquireCode::OK;
    subBatchContext.retryState.flowWaitGroup.reset();
    --task.remainingSubBatchCount;
}

bool TransportTaskExecutor::AnySending(const TransportTask& task) const
{
    if (!task.subBatchContexts) { return false; }
    for (const auto& subBatchContext : *task.subBatchContexts) {
        if (subBatchContext.currentAttempt.state == AttemptState::SENDING) { return true; }
    }
    return false;
}

bool TransportTaskExecutor::Cancel(const TransportTaskPtr& task)
{
    if (!task) { return false; }

    task->cancelRequested.store(true, std::memory_order_release);
    std::lock_guard<std::mutex> lock(task->mutex);
    if (task->Done()) { return false; }
    if (AnySending(*task)) { return false; }
    const auto canceledStatus = Status::Error(StatusCode::CANCELED, "transport task canceled");
    std::fill(task->entryStatus.begin(), task->entryStatus.end(), canceledStatus);
    for (auto& subBatchContext : *task->subBatchContexts) {
        if (subBatchContext.state == TransportSubBatchState::COMPLETED) { continue; }
        std::fill(subBatchContext.entryStatus.begin(), subBatchContext.entryStatus.end(),
                  canceledStatus);
        subBatchContext.status = canceledStatus;
        subBatchContext.state = TransportSubBatchState::COMPLETED;
        subBatchContext.currentAttempt.state = AttemptState::CANCELLED;
    }
    task->finalStatus = canceledStatus;
    ReleaseAllSubBatchResources(*task->subBatchContexts);
    if (task->origin == TaskOrigin::PROBE && task->targetChannel != nullptr) {
        task->targetChannel->FinishProbe();
    }
    task->state.store(TransportTaskState::COMPLETED, std::memory_order_release);
    task->probeLease.reset();
    return true;
}

std::uint16_t TransportTaskExecutor::AllocateRequestCid(std::uint64_t attemptId)
{
    std::lock_guard<std::mutex> lock(cidMu_);
    for (std::size_t count = 0; count < 65536; ++count) {
        const auto requestCid = nextRequestCid_.fetch_add(1, std::memory_order_relaxed);
        if (requestCid == 0) { continue; }
        if (!activeCids_.insert(requestCid).second) { continue; }
        if (attemptId != 0) { cidToAttemptId_[requestCid] = attemptId; }
        return requestCid;
    }
    return 0;
}

void TransportTaskExecutor::BindAttemptCid(std::uint16_t cid, std::uint64_t attemptId)
{
    if (cid == 0 || attemptId == 0) { return; }
    std::lock_guard<std::mutex> lock(cidMu_);
    if (activeCids_.find(cid) != activeCids_.end()) { cidToAttemptId_[cid] = attemptId; }
}

void TransportTaskExecutor::ReleaseRequestCid(std::uint16_t cid)
{
    if (cid == 0) { return; }
    std::lock_guard<std::mutex> lock(cidMu_);
    activeCids_.erase(cid);
    cidToAttemptId_.erase(cid);
}

bool TransportTaskExecutor::HasQuarantinedMemory(MRHandle handle) const
{
    if (handle == kInvalidMRHandle) { return false; }
    std::lock_guard<std::mutex> lock(cidMu_);
    return quarantinedMrHandles_.find(handle) != quarantinedMrHandles_.end();
}

void TransportTaskExecutor::ReleaseQuarantinedSlots()
{
    std::set<std::uint32_t> sendSlots;
    std::set<std::uint32_t> flagSlots;
    {
        std::lock_guard<std::mutex> lock(cidMu_);
        sendSlots.swap(quarantinedSendSlots_);
        flagSlots.swap(quarantinedFlagSlots_);
    }
    for (const auto slot : sendSlots) { (void)sendBufferManager_.ReleaseQuarantined(slot); }
    for (const auto slot : flagSlots) { (void)flagBufferManager_.ReleaseQuarantined(slot); }
    std::lock_guard<std::mutex> lock(cidMu_);
    activeCids_.clear();
    cidToAttemptId_.clear();
    quarantinedMrHandles_.clear();
}

bool TransportTaskExecutor::HasRetryWork(const TransportTaskPtr& task) const
{
    if (!task || task->Done() || !task->subBatchContexts ||
        task->cancelRequested.load(std::memory_order_acquire) ||
        std::chrono::steady_clock::now() >= task->deadline) {
        return false;
    }
    std::lock_guard<std::mutex> lock(task->mutex);
    const auto now = std::chrono::steady_clock::now();
    for (const auto& subBatchContext : *task->subBatchContexts) {
        if (subBatchContext.retryState.retryPending &&
            subBatchContext.currentAttempt.state != AttemptState::SENDING &&
            subBatchContext.retryState.retryAt <= now) {
            return true;
        }
    }
    return false;
}

Status TransportTaskExecutor::AssignSubBatchConnections(
    TransportTask& task, std::vector<TransportSubBatchContext>& subBatchContexts)
{
    if (task.origin == TaskOrigin::PROBE) {
        const auto& target = task.targetChannel;
        if (!target || !connManager_->IsCurrentGeneration(target, task.targetGeneration)) {
            for (auto& subBatchContext : subBatchContexts) {
                const auto status = Status::Error(StatusCode::CONNECTION_ERROR,
                                                  "probe channel generation is no longer active");
                std::fill(subBatchContext.entryStatus.begin(), subBatchContext.entryStatus.end(),
                          status);
                subBatchContext.state = TransportSubBatchState::COMPLETED;
                subBatchContext.status = status;
                subBatchContext.currentAttempt.state = AttemptState::COMPLETED;
            }
            return Status::Error(StatusCode::CONNECTION_ERROR,
                                 "probe channel generation is no longer active");
        }
        for (auto& subBatchContext : subBatchContexts) {
            subBatchContext.channel = target;
            subBatchContext.retryState.preferredChannel = target;
            subBatchContext.currentAttempt.channel = target;
            subBatchContext.currentAttempt.group = target->GetGroupLease();
            subBatchContext.currentAttempt.generation =
                task.targetGeneration != 0 ? task.targetGeneration : target->GetGeneration();
            subBatchContext.currentAttempt.attemptId = nextAttemptId_.fetch_add(1);
            subBatchContext.currentAttempt.requestCid = subBatchContext.cid;
            BindAttemptCid(subBatchContext.currentAttempt.requestCid,
                           subBatchContext.currentAttempt.attemptId);
            subBatchContext.currentAttempt.sendAttempted = false;
            subBatchContext.currentAttempt.responseConfirmed = false;
            subBatchContext.currentAttempt.mrLease = MakeTaskMrLease(task);
            subBatchContext.currentAttempt.state = AttemptState::SENDING;
        }
        return Status::OK();
    }

    for (auto& subBatchContext : subBatchContexts) {
        ConnectionRequest request;
        auto acquired = connManager_->TryAcquire(request);
        if (!acquired.ok()) {
            const bool canWait = (acquired.code == AcquireCode::CHANNEL_FULL ||
                                  acquired.code == AcquireCode::GROUP_LIMIT) &&
                                 config_.flowControlEnabled && config_.channelFullPolicy == "flow_wait";
            if (canWait) {
                // No inflight was acquired. Keep the original SQE/CID until ExecuteRetry
                // releases it safely, and remember only the Group used for wakeups.
                subBatchContext.retryState.retryPending = true;
                subBatchContext.retryState.retryAction = RetryAction::RETRY_SAME_CHANNEL;
                subBatchContext.retryState.preferredChannel = acquired.channel;
                subBatchContext.retryState.flowWaitReason = acquired.code;
                subBatchContext.retryState.flowWaitGroup = acquired.group;
                subBatchContext.currentAttempt.state = AttemptState::FLOW_WAIT;
                subBatchContext.currentAttempt.attemptId = nextAttemptId_.fetch_add(1);
                subBatchContext.currentAttempt.requestCid = subBatchContext.cid;
                BindAttemptCid(subBatchContext.currentAttempt.requestCid,
                               subBatchContext.currentAttempt.attemptId);
                subBatchContext.currentAttempt.sendAttempted = false;
                subBatchContext.currentAttempt.responseConfirmed = false;
                subBatchContext.currentAttempt.mrLease = MakeTaskMrLease(task);
                subBatchContext.status = Status::Error(StatusCode::IN_PROGRESS, "waiting for flow capacity");
                continue;
            }
            metrics::UpdateStats(KV_METRIC("kv_transport_task_connection_errors_total"), 1.0);
            const auto subBatchStatus =
                acquired.status.code == StatusCode::IN_PROGRESS
                    ? Status::Error(StatusCode::CONNECTION_ERROR, acquired.status.message)
                    : acquired.status;
            std::fill(subBatchContext.entryStatus.begin(), subBatchContext.entryStatus.end(),
                      subBatchStatus);
            subBatchContext.state = TransportSubBatchState::COMPLETED;
            subBatchContext.status = subBatchStatus;
            return subBatchStatus;
        }

        auto channel = acquired.channel;
        channel->MarkBusinessSubmit();
        subBatchContext.channel = channel;
        subBatchContext.retryState.preferredChannel = channel;
        subBatchContext.currentAttempt.channel = channel;
        subBatchContext.currentAttempt.group = acquired.group != nullptr ? acquired.group
                                                                         : channel->GetGroupLease();
        subBatchContext.currentAttempt.generation = acquired.generation;
        subBatchContext.currentAttempt.attemptId = nextAttemptId_.fetch_add(1);
        subBatchContext.currentAttempt.requestCid = subBatchContext.cid;
        BindAttemptCid(subBatchContext.currentAttempt.requestCid,
                       subBatchContext.currentAttempt.attemptId);
        subBatchContext.currentAttempt.sendAttempted = false;
        subBatchContext.currentAttempt.responseConfirmed = false;
        subBatchContext.currentAttempt.mrLease = MakeTaskMrLease(task);
        subBatchContext.currentAttempt.state = AttemptState::SENDING;
    }
    return Status::OK();
}

// Translate one failed attempt into a final result or a scheduled retry. The
// classifier decides from operation semantics and error evidence; this function
// owns the side effects on connection health, retry state and subBatch completion.
bool TransportTaskExecutor::HandleAttemptFailure(TransportTask& task,
                                                 TransportSubBatchContext& subBatchContext,
                                                 const Status& status, ErrorOrigin origin)
{
    auto plan = retryController_.BuildSubBatchRetryPlan(subBatchContext, status, origin);
    subBatchContext.retryState.lastError = status;
    subBatchContext.retryState.retryAction = plan.decision.retryAction;
    const bool currentAttemptChannel =
        subBatchContext.channel == nullptr || subBatchContext.currentAttempt.generation == 0 ||
        connManager_->IsCurrentGeneration(subBatchContext.channel,
                                          subBatchContext.currentAttempt.generation);

    if (task.cancelRequested.load(std::memory_order_acquire)) {
        CompleteSubBatch(task, subBatchContext,
                         Status::Error(StatusCode::CANCELED, "task canceled before retry"));
        return false;
    }

    if (task.origin == TaskOrigin::PROBE && currentAttemptChannel &&
        (status.code == StatusCode::CONNECTION_ERROR ||
         status.code == StatusCode::CQE_INTERNAL_ERROR ||
         status.code == StatusCode::CQE_WRITE_FAULT ||
         status.code == StatusCode::CQE_UNRECOVERED_READ_ERROR ||
         status.code == StatusCode::CQE_IO_TIMEOUT) &&
        subBatchContext.retryState.failoverAttempts < config_.probeRetryCount) {
        // PROBE retries use the original target QP and a new CID/attemptId. The
        // shared ProbeBudget charges each actual attempt but not another in-flight lease.
        ++subBatchContext.retryState.failoverAttempts;
        subBatchContext.retryState.retryPending = true;
        subBatchContext.retryState.retryAction = RetryAction::RETRY_SAME_CHANNEL;
        subBatchContext.retryState.preferredChannel = subBatchContext.channel;
        subBatchContext.currentAttempt.state = AttemptState::RETRY_BACKOFF;
        subBatchContext.retryState.retryAt =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.probeRetryBackoffMs);
        subBatchContext.status = Status::Error(StatusCode::IN_PROGRESS, "probe retry scheduled");
        ReleaseSubBatchResources(subBatchContext);
        return true;
    }

    if (task.origin == TaskOrigin::PROBE) {
        if (currentAttemptChannel &&
            (status.code == StatusCode::CONNECTION_ERROR ||
             status.code == StatusCode::CQE_INTERNAL_ERROR ||
             status.code == StatusCode::CQE_WRITE_FAULT ||
             status.code == StatusCode::CQE_UNRECOVERED_READ_ERROR ||
             status.code == StatusCode::CQE_IO_TIMEOUT)) {
            connManager_->ReportFailure(subBatchContext.channel);
        }
        CompleteSubBatch(task, subBatchContext, status);
        return false;
    }

    if (subBatchContext.channel != nullptr) {
        subBatchContext.channel->GetGroup()->OnNonSuccess();
        if (plan.decision.flowAction == FlowAction::ON_BUSY) {
            subBatchContext.channel->GetGroup()->OnBusy();
        }
    }

    if (plan.needsRetry) {
        if (subBatchContext.channel != nullptr && currentAttemptChannel &&
            plan.decision.healthAction == ChannelHealthAction::RECORD_FAILURE) {
            // Isolate the failed incarnation before selecting a different channel.
            connManager_->ReportFailure(subBatchContext.channel);
        }

        if (plan.decision.retryAction == RetryAction::RETRY_SAME_CHANNEL &&
            subBatchContext.retryState.busyAttempts < config_.busyRetryCount) {
            // Busy retry stays on the original channel and gets a new CID on the
            // next attempt. It is valid only under the remote-busy/no-state-change
            // contract; it must not be inferred from local RESOURCE_BUSY.
            ++subBatchContext.retryState.busyAttempts;
            subBatchContext.retryState.retryPending = true;
            subBatchContext.currentAttempt.state = AttemptState::RETRY_BACKOFF;
            subBatchContext.retryState.retryAt =
                std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.busyRetryBackoffMs);
            subBatchContext.status = Status::Error(StatusCode::IN_PROGRESS, "busy retry scheduled");
            if (subBatchContext.retryState.preferredChannel == nullptr) {
                subBatchContext.retryState.preferredChannel = subBatchContext.channel;
            }
            ReleaseSubBatchResources(subBatchContext);
            return true;
        }

        if (plan.decision.retryAction == RetryAction::FAILOVER &&
            subBatchContext.retryState.failoverAttempts < config_.failoverRetryCount) {
            // Failover resends the complete original subBatch to a different active
            // channel, not just failed Entries. Successful and application-error
            // Entries may therefore be executed again, including non-idempotent STORE.
            ++subBatchContext.retryState.failoverAttempts;
            if (subBatchContext.channel != nullptr) {
                subBatchContext.retryState.attemptedGenerations.insert(MakeAttemptKey(*subBatchContext.channel));
            }
            subBatchContext.retryState.retryPending = true;
            subBatchContext.currentAttempt.state = AttemptState::RETRY_BACKOFF;
            subBatchContext.status = Status::Error(StatusCode::IN_PROGRESS, "failover scheduled");
            if (subBatchContext.retryState.preferredChannel == nullptr) {
                subBatchContext.retryState.preferredChannel = subBatchContext.channel;
            }
            ReleaseSubBatchResources(subBatchContext);
            return true;
        }
    }

    if (subBatchContext.channel != nullptr && currentAttemptChannel &&
        plan.decision.healthAction == ChannelHealthAction::RECORD_FAILURE) {
        connManager_->ReportFailure(subBatchContext.channel);
    }
    CompleteSubBatch(task, subBatchContext, status);
    return false;
}

// Execute one scheduled attempt on an existing worker/completion path. Each retry
// receives a new CID, attemptId and current generation, while the request template
// still describes the complete original subBatch and its original Entry ordering.
bool TransportTaskExecutor::ExecuteRetry(const TransportTaskPtr& task)
{
    if (!task) { return false; }
    if (task->cancelRequested.load(std::memory_order_acquire)) { return Cancel(task); }
    if (std::chrono::steady_clock::now() >= task->deadline) { return Poll(task); }

    TransportSubBatchContext* retryContext = nullptr;
    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(task->mutex);
        if (task->Done()) { return true; }
        for (auto& subBatchContext : *task->subBatchContexts) {
            if (subBatchContext.retryState.retryPending &&
                subBatchContext.retryState.retryAt <= now &&
                subBatchContext.currentAttempt.state != AttemptState::SENDING) {
                retryContext = &subBatchContext;
                if (retryContext->currentAttempt.state == AttemptState::FLOW_WAIT) {
                    // The initial FLOW_WAIT owns buffers but no Channel inflight.
                    const auto preferred = retryContext->retryState.preferredChannel;
                    const auto flowGroup = retryContext->retryState.flowWaitGroup;
                    ReleaseSubBatchResources(*retryContext);
                    retryContext->retryState.preferredChannel = preferred;
                    retryContext->retryState.flowWaitGroup = flowGroup;
                }
                retryContext->currentAttempt.state = AttemptState::SENDING;
                break;
            }
        }
    }
    if (retryContext == nullptr) { return task->Done(); }

    AcquireResult acquired;
    if (task->origin == TaskOrigin::PROBE) {
        const auto target = retryContext->retryState.preferredChannel;
        if (target != nullptr &&
            connManager_->IsCurrentGeneration(target, retryContext->currentAttempt.generation) &&
            connManager_->AcquireProbe(target, target->GetGeneration())) {
            acquired.code = AcquireCode::OK;
            acquired.status = Status::OK();
            acquired.channel = target;
            acquired.group = target->GetGroupLease();
            acquired.generation = target->GetGeneration();
        } else {
            acquired.code = AcquireCode::NO_ACTIVE;
            acquired.status = Status::Error(StatusCode::CONNECTION_ERROR,
                                            "probe target generation is no longer active");
        }
    } else {
        ConnectionRequest request;
        request.excludedGenerations = retryContext->retryState.attemptedGenerations;
        if (retryContext->retryState.retryAction == RetryAction::RETRY_SAME_CHANNEL) {
            request.preferredChannel = retryContext->retryState.preferredChannel;
        }
        acquired = connManager_->TryAcquire(request);
    }
    if (!acquired.ok()) {
        const bool canWait = (acquired.code == AcquireCode::CHANNEL_FULL ||
                              acquired.code == AcquireCode::GROUP_LIMIT) &&
                             config_.flowControlEnabled && config_.channelFullPolicy == "flow_wait";
        std::shared_ptr<ConnectionGroup> waitGroup;
        {
            std::lock_guard<std::mutex> lock(task->mutex);
            if (canWait) {
                retryContext->retryState.retryPending = true;
                retryContext->retryState.flowWaitReason = acquired.code;
                retryContext->retryState.flowWaitGroup = acquired.group;
                if (acquired.channel != nullptr) {
                    retryContext->retryState.preferredChannel = acquired.channel;
                }
                retryContext->currentAttempt.state = AttemptState::FLOW_WAIT;
                retryContext->currentAttempt.channel.reset();
                retryContext->currentAttempt.group.reset();
                retryContext->currentAttempt.generation = 0;
                retryContext->currentAttempt.sendAttempted = false;
                retryContext->currentAttempt.responseConfirmed = false;
                retryContext->status = Status::Error(StatusCode::IN_PROGRESS, "waiting for flow capacity");
                task->state.store(TransportTaskState::FLOW_WAIT, std::memory_order_release);
                if (const auto clientTask = task->clientTask.lock()) {
                    clientTask->state.store(ClientTaskState::FLOW_WAIT, std::memory_order_release);
                }
                waitGroup = retryContext->retryState.flowWaitGroup;
            } else {
                retryContext->retryState.retryPending = false;
                auto finalStatus = acquired.status;
                if (finalStatus.code == StatusCode::IN_PROGRESS) {
                    finalStatus = Status::Error(StatusCode::CONNECTION_ERROR, acquired.status.message);
                }
                if (!retryContext->retryState.lastError.ok()) {
                    finalStatus = retryContext->retryState.lastError;
                }
                CompleteSubBatch(*task, *retryContext, finalStatus);
                task->TryFinalizeFromSubBatches();
                return task->Done();
            }
        }
        if (waitGroup != nullptr) {
            waitGroup->WaitForFlowRelease(std::chrono::milliseconds(1));
        } else {
            std::this_thread::yield();
        }
        return false;
    }

    const auto& channel = acquired.channel;
    retryContext->channel = channel;
    retryContext->currentAttempt.channel = channel;
    retryContext->currentAttempt.group = acquired.group != nullptr ? acquired.group
                                                                   : channel->GetGroupLease();
    retryContext->currentAttempt.generation = acquired.generation;
    retryContext->currentAttempt.attemptId = nextAttemptId_.fetch_add(1);
    retryContext->currentAttempt.requestCid = AllocateRequestCid(retryContext->currentAttempt.attemptId);
    if (retryContext->currentAttempt.requestCid == 0) {
        std::lock_guard<std::mutex> lock(task->mutex);
        retryContext->retryState.retryPending = false;
        CompleteSubBatch(*task, *retryContext,
                         Status::Error(StatusCode::RESOURCE_BUSY, "request CID space exhausted"));
        task->TryFinalizeFromSubBatches();
        return task->Done();
    }
    retryContext->cid = retryContext->currentAttempt.requestCid;
    retryContext->currentAttempt.sendAttempted = false;
    retryContext->currentAttempt.responseConfirmed = false;
    if (retryContext->currentAttempt.mrLease == nullptr) {
        retryContext->currentAttempt.mrLease = MakeTaskMrLease(*task);
    }
    retryContext->retryState.preferredChannel = channel;
    retryContext->retryState.flowWaitReason = AcquireCode::OK;
    retryContext->retryState.flowWaitGroup.reset();
    retryContext->retryState.retryAt = std::chrono::steady_clock::time_point::min();
    {
        std::lock_guard<std::mutex> lock(task->mutex);
        task->state.store(TransportTaskState::INFLIGHT, std::memory_order_release);
        if (const auto clientTask = task->clientTask.lock()) {
            clientTask->state.store(ClientTaskState::INFLIGHT, std::memory_order_release);
        }
    }

    auto status = PrepareRetryRequest(*retryContext);
    if (status.ok() && task->origin == TaskOrigin::PROBE && task->probeLease != nullptr &&
        !task->probeLease->TryAcquireSubmission()) {
        status = Status::Error(StatusCode::RESOURCE_BUSY, "probe round submission budget exhausted");
    }
    if (status.ok()) { status = SendSingleAttempt(*retryContext); }

    std::lock_guard<std::mutex> lock(task->mutex);
    if (task->cancelRequested.load(std::memory_order_acquire)) {
        CompleteSubBatch(*task, *retryContext,
                         Status::Error(StatusCode::CANCELED, "task canceled during retry"));
        task->TryFinalizeFromSubBatches();
        return task->Done();
    }
    if (status.ok()) {
        retryContext->retryState.retryPending = false;
        retryContext->currentAttempt.state = AttemptState::WAITING_RESPONSE;
        retryContext->status = Status::Error(StatusCode::IN_PROGRESS, "retry in progress");
        task->TryFinalizeFromSubBatches();
        return task->Done();
    }

    (void)HandleAttemptFailure(*task, *retryContext, status, ErrorOrigin::PROVIDER_SEND);
    task->TryFinalizeFromSubBatches();
    return task->Done();
}

bool TransportTaskExecutor::Execute(const TransportTaskPtr& task)
{
    TransportTaskState expected = TransportTaskState::PENDING;
    if (!task->state.compare_exchange_strong(expected, TransportTaskState::INFLIGHT,
                                             std::memory_order_acq_rel)) {
        return false;
    }
    const auto processingStartedAt = std::chrono::steady_clock::now();
    task->asuCompletionMetric = &asuCompletionMetric_;

    std::vector<TransportSubBatchContext> subBatchContexts;
    auto status = PrepareTaskSubBatches(*task, subBatchContexts);
    if (status.ok()) { status = AssignSubBatchConnections(*task, subBatchContexts); }

    std::vector<TransProvider::SendIoBatch> ioBatches;
    if (status.ok()) { BuildSubBatchSendBuffers(subBatchContexts, ioBatches); }
    if (status.ok() && ioBatches.empty()) {
        for (const auto& subBatchContext : subBatchContexts) {
            if (subBatchContext.currentAttempt.state == AttemptState::FLOW_WAIT) {
                task->state.store(TransportTaskState::FLOW_WAIT, std::memory_order_release);
                if (const auto clientTask = task->clientTask.lock()) {
                    clientTask->state.store(ClientTaskState::FLOW_WAIT, std::memory_order_release);
                }
                break;
            }
        }
    }

    {
        // Publish ownership before Send so Cancel can see in-flight attempts.
        std::lock_guard<std::mutex> lock(task->mutex);
        if (task->Done()) {
            ReleaseAllSubBatchResources(subBatchContexts);
            return false;
        }
        *task->subBatchContexts = std::move(subBatchContexts);
        task->InitializeRemainingSubBatchCount();
    }

    if (!status.ok()) {
        KV_ERROR("Abort transport task before send task_id={} code={} message={}", task->taskId,
                 static_cast<int>(status.code), status.message);
        std::lock_guard<std::mutex> lock(task->mutex);
        AbortSubBatchesBeforeSend(*task, *task->subBatchContexts);
        task->InitializeRemainingSubBatchCount();
        task->TryFinalizeFromSubBatches();
        return task->Done();
    }

    if (!ioBatches.empty()) {
        const auto preSendAt = std::chrono::steady_clock::now();
        const metrics::MetricUpdate preSendUpdates[] = {
            {KV_METRIC("kv_transport_task_pre_send_duration_seconds"),
             std::chrono::duration<double>(preSendAt - task->submittedAt).count()},
            {KV_METRIC("kv_transport_task_queue_duration_seconds"),
             std::chrono::duration<double>(processingStartedAt - task->submittedAt).count()},
            {KV_METRIC("kv_transport_task_process_duration_seconds"),
             std::chrono::duration<double>(preSendAt - processingStartedAt).count()},
        };
        metrics::UpdateStats(preSendUpdates, std::size(preSendUpdates));
        SendSubBatchBuffers(*task, ioBatches);
        task->sendCompletedAt = std::chrono::steady_clock::now();
        task->sendReturned.store(true, std::memory_order_release);
        const metrics::MetricUpdate sendUpdates[] = {
            {KV_METRIC("kv_transport_task_send_duration_seconds"),
             std::chrono::duration<double>(task->sendCompletedAt - task->submittedAt).count()},
            {asuSendMetric_,
             std::chrono::duration<double>(task->sendCompletedAt - task->submittedAt).count()},
        };
        metrics::UpdateStats(sendUpdates, std::size(sendUpdates));
        if (task->onSendComplete) { task->onSendComplete(); }
    }

    if (task->cancelRequested.load(std::memory_order_acquire)) { return Cancel(task); }

    std::lock_guard<std::mutex> lock(task->mutex);
    task->InitializeRemainingSubBatchCount();
    task->TryFinalizeFromSubBatches();
    KV_DEBUG(
        "TransportTaskExecutor::Execute submitted task_id={} op_type={} entries={} keys={} "
        "sub_batches={} done={} code={} message={}",
        task->taskId, static_cast<int>(task->opType), task->entries.size(), task->keys.size(),
        task->subBatchContexts->size(), task->Done(), static_cast<int>(task->finalStatus.code),
        task->finalStatus.message);
    return task->Done();
}

bool TransportTaskExecutor::Poll(const TransportTaskPtr& task)
{
    if (!task) { return false; }

    std::lock_guard<std::mutex> lock(task->mutex);
    const auto taskState = task->state.load(std::memory_order_acquire);
    if (taskState != TransportTaskState::INFLIGHT && taskState != TransportTaskState::FLOW_WAIT) {
        return false;
    }
    if (task->subBatchContexts->empty()) { return false; }

    if (task->cancelRequested.load(std::memory_order_acquire) && !AnySending(*task)) {
        const auto canceledStatus = Status::Error(StatusCode::CANCELED, "transport task canceled");
        for (auto& subBatchContext : *task->subBatchContexts) {
            if (subBatchContext.state == TransportSubBatchState::COMPLETED) { continue; }
            std::fill(subBatchContext.entryStatus.begin(), subBatchContext.entryStatus.end(),
                      canceledStatus);
            CompleteSubBatch(*task, subBatchContext, canceledStatus);
        }
        task->finalStatus = canceledStatus;
        task->state.store(TransportTaskState::COMPLETED, std::memory_order_release);
        task->probeLease.reset();
        return true;
    }

    // Never free a Slot while Provider::Send may still write its response into it.
    if (AnySending(*task)) { return false; }

    const auto isCurrentAttemptChannel = [this](const TransportSubBatchContext& subBatchContext) {
        if (subBatchContext.channel == nullptr) {
            return subBatchContext.currentAttempt.generation == 0;
        }
        return subBatchContext.currentAttempt.generation == 0 ||
               connManager_->IsCurrentGeneration(subBatchContext.channel,
                                                 subBatchContext.currentAttempt.generation);
    };

    if (std::chrono::steady_clock::now() >= task->deadline) {
        // The generic task deadline keeps its original TIMEOUT result. CQE timeout
        // and stale-generation responses use the normal link-failure failover path.
        metrics::UpdateStats(KV_METRIC("kv_transport_task_completion_timeouts_total"), 1.0);
        const auto timeoutStatus =
            Status::Error(StatusCode::TIMEOUT, "transport task execution timeout");
        std::fill(task->entryStatus.begin(), task->entryStatus.end(), timeoutStatus);
        for (auto& subBatchContext : *task->subBatchContexts) {
            if (subBatchContext.state == TransportSubBatchState::COMPLETED) { continue; }

            std::fill(subBatchContext.entryStatus.begin(), subBatchContext.entryStatus.end(),
                      timeoutStatus);
            if (isCurrentAttemptChannel(subBatchContext)) {
                connManager_->ReportFailure(subBatchContext.channel);
            }
            CompleteSubBatch(*task, subBatchContext, timeoutStatus);
        }
        task->finalStatus = timeoutStatus;
        task->state.store(TransportTaskState::COMPLETED, std::memory_order_release);
        task->probeLease.reset();
        return true;
    }

    for (auto& subBatchContext : *task->subBatchContexts) {
        if (subBatchContext.state == TransportSubBatchState::COMPLETED) { continue; }
        if (subBatchContext.currentAttempt.state == AttemptState::SENDING) { continue; }

        if (subBatchContext.flagBuffer.local_addr == 0 ||
            subBatchContext.flagBuffer.slot_index == UINT32_MAX) {
            continue;
        }
        std::uint16_t completedCid = 0;
        const void* responseData = reinterpret_cast<void*>(subBatchContext.flagBuffer.local_addr);
        if (const auto status = protocolManager_.PollResponseCid(responseData, completedCid);
            !status.ok()) {
            continue;
        }
        if (completedCid == 0 || completedCid != subBatchContext.cid) { continue; }
        if (subBatchContext.currentAttempt.attemptId != 0) {
            std::lock_guard<std::mutex> cidLock(cidMu_);
            const auto attemptIt = cidToAttemptId_.find(completedCid);
            if (attemptIt == cidToAttemptId_.end() ||
                attemptIt->second != subBatchContext.currentAttempt.attemptId) {
                continue;
            }
        }

        // A matching CID is not sufficient after reconnect: a response from a retired
        // channel incarnation must not complete the current attempt. Convert it to a
        // CQE_IO_TIMEOUT so the normal link-failure policy can fail over. This applies
        // to every operation; a repeated non-idempotent STORE may return KEY_EXIST.
        // The retired channel is not reported again as a new-generation failure.
        if (!isCurrentAttemptChannel(subBatchContext)) {
            if (!subBatchContext.retryState.retryPending) {
                const auto staleStatus =
                    Status::Error(StatusCode::CQE_IO_TIMEOUT, "stale channel generation response");
                std::fill(subBatchContext.entryStatus.begin(), subBatchContext.entryStatus.end(),
                          staleStatus);
                (void)HandleAttemptFailure(*task, subBatchContext, staleStatus, ErrorOrigin::CQE);
            }
            continue;
        }

        KvResponse response;
        const auto batchNumber = static_cast<std::uint16_t>(subBatchContext.entryStatus.size());
        if (const auto status = protocolManager_.UnpackResponse(
                responseData, ToKvOpcode(subBatchContext.opType), batchNumber, response);
            !status.ok()) {
            CompleteSubBatch(*task, subBatchContext, status);
            continue;
        }

        subBatchContext.currentAttempt.responseConfirmed = true;
        ReleaseConfirmedCid(subBatchContext.currentAttempt.requestCid);
        subBatchContext.status = KvResponseStatusToSubBatchStatus(response.status);
        FillEntryStatusFromCqeResult(response, subBatchContext);

        if (task->origin == TaskOrigin::PROBE) {
            (void)HandleAttemptFailure(*task, subBatchContext, subBatchContext.status,
                                       ErrorOrigin::CQE);
            continue;
        }

        // Query hit/miss values are normal business results, not link failures.
        // They still complete the subBatch and can drive the qualified success path.
        const bool queryResultBufferStatus =
            subBatchContext.opType == AsuOpType::QUERY &&
            (subBatchContext.status.code == StatusCode::CQE_CHECK_RESULT_BUFFER ||
             subBatchContext.status.code == StatusCode::CQE_KEY_NOT_EXIST);
        if (subBatchContext.status.ok() || queryResultBufferStatus) {
            connManager_->ReportSuccess(subBatchContext.channel);
            if (subBatchContext.channel != nullptr) {
                subBatchContext.channel->GetGroup()->OnBusinessSuccess(
                    std::chrono::steady_clock::now());
            }
            CompleteSubBatch(*task, subBatchContext, Status::OK());
            continue;
        }

        (void)HandleAttemptFailure(*task, subBatchContext, subBatchContext.status,
                                   ErrorOrigin::CQE);
    }
    task->TryFinalizeFromSubBatches();
    return task->Done();
}

}  // namespace kv
