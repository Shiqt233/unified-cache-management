#include "trans_task_manager.h"
#include "probe_scheduler.h"
#include <chrono>
#include <utility>
#include "kv_metrics/metrics.h"
#include "utils/trans_task_utils.h"

namespace kv {

void FillEntryStatusFromCqeResult(const KvResponse& response,
                                  TransportSubBatchContext& subBatchContext)
{
    FillEntryStatusFromCqeResult(response, subBatchContext.opType, subBatchContext.useSeekControl,
                                 subBatchContext.status, subBatchContext.entryStatus);
}

TransportTask::TransportTask() : subBatchContexts(std::make_shared<TransportSubBatchList>()) {}

bool TransportTask::Done() const
{
    return state.load(std::memory_order_acquire) == TransportTaskState::COMPLETED;
}

bool TransportTask::NotifyCompletion(TaskResult result)
{
    if (!onComplete || completionNotified.exchange(true, std::memory_order_acq_rel)) {
        return false;
    }
    onComplete(std::move(result));
    return true;
}

// Aggregate final subBatch results without starting another retry. Normal Query
// hit/miss values are excluded. Any remaining application, protocol or transport
// error produces PARTIAL_FAILED; no separate outcome status is synthesized here.
Status TransportTask::BuildFinalStatus() const
{
    bool anyError = false;

    const auto accumulate = [&](const Status& status) {
        if (!status.ok()) { anyError = true; }
    };

    for (const auto& subBatchContext : *subBatchContexts) {
        const auto isNormalQueryResult = [&subBatchContext](const Status& status) {
            return subBatchContext.opType == AsuOpType::QUERY &&
                   (status.code == StatusCode::ENTRY_KEY_EXIST ||
                    status.code == StatusCode::ENTRY_KEY_NOT_EXIST ||
                    status.code == StatusCode::CQE_KEY_NOT_EXIST);
        };
        bool hasEntryError = false;
        for (const auto& entryStatus : subBatchContext.entryStatus) {
            if (isNormalQueryResult(entryStatus)) { continue; }
            if (!entryStatus.ok()) { hasEntryError = true; }
            accumulate(entryStatus);
        }
        // When Entry decoding did not produce a failure, retain the subBatch error
        // (for example, response-unpack failure) instead of reporting a false success.
        if (!hasEntryError && !isNormalQueryResult(subBatchContext.status)) {
            accumulate(subBatchContext.status);
        }
    }

    if (anyError) { return Status::Error(StatusCode::PARTIAL_FAILED, "transport task partially failed"); }
    return Status::OK();
}

void TransportTask::InitializeRemainingSubBatchCount()
{
    remainingSubBatchCount = 0;
    for (const auto& subBatchContext : *subBatchContexts) {
        if (subBatchContext.state == TransportSubBatchState::PENDING) { ++remainingSubBatchCount; }
    }
}

// Finalize only after every subBatch has reached its final state. An attempt in
// retry backoff is not a completed subBatch and must keep the task alive. Probe
// leases are released on every terminal path, including an empty subBatch list.
void TransportTask::TryFinalizeFromSubBatches()
{
    if (subBatchContexts->empty()) {
        finalStatus = Status::Error(StatusCode::PARTIAL_FAILED, "transport task partially failed");
        state.store(TransportTaskState::COMPLETED, std::memory_order_release);
        probeLease.reset();
        return;
    }

    if (remainingSubBatchCount != 0) { return; }

    finalStatus = BuildFinalStatus();
    state.store(TransportTaskState::COMPLETED, std::memory_order_release);
    probeLease.reset();
}

// Deliver the final TaskResult once, then remove the transport task and release
// any remaining probe lease. Completion notification and result aggregation are
// deliberately separate from retry execution.
void TransportTaskManager::NotifyCompletion(const TransportTaskPtr& task)
{
    const bool sendReturned = task->sendReturned.load(std::memory_order_acquire);
    const auto completedAt = std::chrono::steady_clock::now();
    const metrics::MetricUpdate updates[] = {
        {KV_METRIC("kv_transport_task_e2e_duration_seconds"),
         std::chrono::duration<double>(completedAt - task->submittedAt).count()},
        {KV_METRIC("kv_transport_task_completion_duration_seconds"),
         sendReturned ? std::chrono::duration<double>(completedAt - task->sendCompletedAt).count()
                      : 0.0                                                    },
        {task->asuCompletionMetric,
         sendReturned ? std::chrono::duration<double>(completedAt - task->sendCompletedAt).count()
                      : 0.0                                                    },
    };
    metrics::UpdateStats(updates, sendReturned ? std::size_t{3} : std::size_t{1});
    TaskResult result;
    BuildResult(*task, result);
    (void)task->NotifyCompletion(std::move(result));
    (void)Remove(task->taskId);
    task->probeLease.reset();
}

void TransportTaskManager::BuildResult(const TransportTask& task, TaskResult& result)
{
    result.status = task.finalStatus;
    result.entryStatus = task.entryStatus;
    if (!task.subBatchContexts->empty()) {
        std::size_t resultIndex = 0;
        for (const auto& subBatchContext : *task.subBatchContexts) {
            for (const auto& status : subBatchContext.entryStatus) {
                if (resultIndex >= result.entryStatus.size()) { break; }
                result.entryStatus[resultIndex++] = status;
            }
        }
    }

    result.queryResult.reset();
    if (task.opType == AsuOpType::QUERY) {
        result.queryResult = BuildQueryResultFromEntryStatus(result.entryStatus);
    }
}

}  // namespace kv
