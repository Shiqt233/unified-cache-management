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
#include "error_classifier.h"

namespace kv {
namespace {

ErrorDecision MakeDecision(ErrorCategory category,
                           RetryAction retryAction = RetryAction::RETURN_RESULT,
                           ChannelHealthAction healthAction = ChannelHealthAction::NO_CHANGE,
                           FlowAction flowAction = FlowAction::NO_CHANGE)
{
    return ErrorDecision{category, retryAction, healthAction, flowAction};
}

ErrorDecision MakeNormalDecision(AsuOpType opType, bool queryMiss = false)
{
    const auto flowAction = opType == AsuOpType::KEEP_ALIVE ? FlowAction::NO_CHANGE
                                                            : FlowAction::ON_BUSINESS_SUCCESS;
    return MakeDecision(queryMiss ? ErrorCategory::QUERY_MISS : ErrorCategory::SUCCESS,
                        RetryAction::RETURN_RESULT, ChannelHealthAction::RECORD_SUCCESS,
                        flowAction);
}

// Timeout follows the original SR retry behavior: record the link failure and
// fail over the complete subBatch. There is no separate public outcome state or
// non-idempotent replay veto; a repeated STORE may therefore return KEY_EXIST.
ErrorDecision MakeTimeoutFailoverDecision()
{
    return MakeDecision(ErrorCategory::LINK_FAILURE, RetryAction::FAILOVER,
                        ChannelHealthAction::RECORD_FAILURE);
}

bool NeedsRetry(RetryAction retryAction)
{
    return retryAction == RetryAction::RETRY_SAME_CHANNEL || retryAction == RetryAction::FAILOVER;
}

bool IsQueryMiss(const Status& status, AsuOpType opType)
{
    return opType == AsuOpType::QUERY &&
           (status.code == StatusCode::ENTRY_KEY_NOT_EXIST ||
            status.code == StatusCode::CQE_KEY_NOT_EXIST);
}

bool IsSubBatchWrapper(StatusCode code)
{
    return code == StatusCode::CQE_CHECK_RESULT_BUFFER || code == StatusCode::PARTIAL_FAILED;
}

// Certainty is derived from the error source, not inferred from the numeric status
// alone. In particular, PROVIDER_SEND can be MAYBE_SENT even when it returns an
// error, while CLIENT_WAIT only reports that the caller stopped waiting.
SendCertainty CertaintyForOrigin(ErrorOrigin origin)
{
    switch (origin) {
        case ErrorOrigin::ATTEMPT_TIMEOUT: return SendCertainty::MAYBE_SENT;
        case ErrorOrigin::LOCAL_VALIDATE:
        case ErrorOrigin::LOCAL_QUEUE:
        case ErrorOrigin::PROBE_LOCAL:
        case ErrorOrigin::SHUTDOWN: return SendCertainty::NOT_SENT;
        case ErrorOrigin::PROVIDER_SEND: return SendCertainty::MAYBE_SENT;
        case ErrorOrigin::PROVIDER_CONNECT:
        case ErrorOrigin::CQE:
        case ErrorOrigin::RESULT_ENTRY:
        case ErrorOrigin::CLIENT_WAIT: return SendCertainty::RESPONSE_CONFIRMED;
    }
    return SendCertainty::NOT_SENT;
}

}  // namespace

ErrorDecision ErrorClassifier::Classify(const ErrorContext& context)
{
    if (context.status.ok()) { return MakeNormalDecision(context.opType); }

    // A local probe failure says nothing about the remote channel, even if its local
    // status happens to share a value with a remote protocol error.
    if (context.origin == ErrorOrigin::PROBE_LOCAL) {
        return MakeDecision(ErrorCategory::INCONCLUSIVE);
    }

    if (context.origin == ErrorOrigin::ATTEMPT_TIMEOUT) {
        // Check send certainty before CQE_IO_TIMEOUT. Even a timeout-shaped status
        // must remain an ordinary TIMEOUT when the request was definitely not sent.
        switch (context.certainty) {
            case SendCertainty::MAYBE_SENT: return MakeTimeoutFailoverDecision();
            case SendCertainty::NOT_SENT:
            case SendCertainty::DEFINITELY_FAILED:
                return MakeDecision(ErrorCategory::TIMEOUT);
            case SendCertainty::RESPONSE_CONFIRMED: break;
        }
    }

    if (context.status.code == StatusCode::CQE_IO_TIMEOUT) {
        return MakeTimeoutFailoverDecision();
    }

    if (context.origin == ErrorOrigin::CLIENT_WAIT && context.status.code == StatusCode::TIMEOUT) {
        return MakeDecision(ErrorCategory::TIMEOUT);
    }

    if (context.origin == ErrorOrigin::LOCAL_QUEUE &&
        context.status.code == StatusCode::RESOURCE_BUSY) {
        return MakeDecision(ErrorCategory::LOCAL_BUSY);
    }

    if (context.origin == ErrorOrigin::LOCAL_VALIDATE) {
        switch (context.status.code) {
            case StatusCode::INVALID_ARGUMENT:
            case StatusCode::BUFFER_NOT_REGISTERED:
            case StatusCode::BUFFER_NOT_SUPPORTED:
            case StatusCode::UNSUPPORTED: return MakeDecision(ErrorCategory::APPLICATION);
            default: break;
        }
    }

    if ((context.origin == ErrorOrigin::PROVIDER_SEND || context.origin == ErrorOrigin::CQE) &&
        context.status.code == StatusCode::CQE_RESOURCE_BUSY) {
        return MakeDecision(ErrorCategory::BUSY, RetryAction::RETRY_SAME_CHANNEL,
                            ChannelHealthAction::NO_CHANGE, FlowAction::ON_BUSY);
    }

    if ((context.origin == ErrorOrigin::PROVIDER_SEND ||
         context.origin == ErrorOrigin::PROVIDER_CONNECT) &&
        (context.status.code == StatusCode::CONNECTION_ERROR ||
         (context.origin == ErrorOrigin::PROVIDER_CONNECT &&
          context.status.code == StatusCode::TIMEOUT))) {
        return MakeDecision(ErrorCategory::LINK_FAILURE, RetryAction::FAILOVER,
                            ChannelHealthAction::RECORD_FAILURE);
    }

    switch (context.status.code) {
        case StatusCode::CQE_INTERNAL_ERROR:
        case StatusCode::CQE_WRITE_FAULT:
        case StatusCode::CQE_UNRECOVERED_READ_ERROR:
            return MakeDecision(ErrorCategory::LINK_FAILURE, RetryAction::FAILOVER,
                                ChannelHealthAction::RECORD_FAILURE);
        default: break;
    }

    if (context.status.code == StatusCode::ENTRY_KEY_EXIST) {
        return context.opType == AsuOpType::QUERY ? MakeNormalDecision(context.opType)
                                                  : MakeDecision(ErrorCategory::APPLICATION);
    }

    if (context.status.code == StatusCode::ENTRY_KEY_NOT_EXIST ||
        context.status.code == StatusCode::CQE_KEY_NOT_EXIST) {
        return context.opType == AsuOpType::QUERY ? MakeNormalDecision(context.opType, true)
                                                  : MakeDecision(ErrorCategory::APPLICATION);
    }

    switch (context.status.code) {
        case StatusCode::CQE_INVALID_COMMAND_OPCODE:
        case StatusCode::CQE_INVALID_FIELD_IN_COMMAND:
        case StatusCode::CQE_OUT_OF_CREATE_SIZE:
        case StatusCode::CQE_KEY_ALREADY_EXISTED:
        case StatusCode::ENTRY_NO_RETRY_ADVISED:
        case StatusCode::ENTRY_KEY_NOT_FOUND:
        case StatusCode::ENTRY_DATA_NOT_EXIST:
        case StatusCode::ENTRY_DELETE_FAILED: return MakeDecision(ErrorCategory::APPLICATION);
        case StatusCode::ENTRY_RETRY_ADVISED:
        case StatusCode::CQE_CHECK_RESULT_BUFFER:
        case StatusCode::PARTIAL_FAILED: return MakeDecision(ErrorCategory::UNKNOWN);
        default: break;
    }

    // This also covers PROVIDER_SEND IO_ERROR/INTERNAL_ERROR, unknown raw values and
    // CQE unpack/poll errors: none is sufficient evidence for an automatic retry.
    return MakeDecision(ErrorCategory::UNKNOWN);
}

// Build a retry decision for the complete subBatch, never for an Entry subset.
// Failure codes are deduplicated after decoding the response buffer. OK and normal
// Query results do not count as failures. A single failure-code type uses its own
// strategy; mixed types fail over whenever at least one type requires retry.
SubBatchRetryDecision ErrorClassifier::ClassifySubBatch(const Status& subBatchStatus,
                                                        const std::vector<Status>& entryStatus,
                                                        AsuOpType opType, ErrorOrigin origin)
{
    struct Failure {
        StatusCode code;
        ErrorDecision decision;
    };

    std::vector<Failure> failures;
    bool hasQueryMiss = false;

    const auto addStatus = [&](const Status& status, bool allowWrapper) {
        if (IsNormalResult(status, opType)) {
            hasQueryMiss = hasQueryMiss || IsQueryMiss(status, opType);
            return;
        }
        if (allowWrapper && IsSubBatchWrapper(status.code)) { return; }

        ErrorContext context;
        context.status = status;
        context.rawStatus = static_cast<std::uint32_t>(status.code);
        context.origin = origin;
        context.opType = opType;
        context.certainty = CertaintyForOrigin(origin);
        const auto decision = Classify(context);

        for (const auto& failure : failures) {
            if (failure.code == status.code) { return; }
        }
        failures.push_back(Failure{status.code, decision});
    };

    // A wrapper is not a final error once per-entry results are available. Without
    // entry results it remains an UNKNOWN error rather than a false success.
    addStatus(subBatchStatus, !entryStatus.empty());
    for (const auto& status : entryStatus) { addStatus(status, false); }

    SubBatchRetryDecision result;
    result.hasMultipleErrorCodes = failures.size() > 1;

    if (failures.empty()) {
        result.decision = MakeNormalDecision(opType, hasQueryMiss);
        return result;
    }

    if (failures.size() == 1) {
        result.decision = failures.front().decision;
        result.needsRetry = NeedsRetry(result.decision.retryAction);
    } else {
        result.decision = MakeDecision(ErrorCategory::MIXED);
        bool anyRetryable = false;
        for (const auto& failure : failures) {
            const auto& decision = failure.decision;
            anyRetryable = anyRetryable || NeedsRetry(decision.retryAction);
            if (decision.healthAction == ChannelHealthAction::RECORD_FAILURE) {
                result.decision.healthAction = ChannelHealthAction::RECORD_FAILURE;
            }

            if (decision.flowAction == FlowAction::ON_BUSY) {
                result.decision.flowAction = FlowAction::ON_BUSY;
            }
        }

        if (anyRetryable) {
            result.decision.retryAction = RetryAction::FAILOVER;
            result.needsRetry = true;
        }
    }

    return result;
}

// Query hit/miss values are normal results only for QUERY. For other operations,
// the same values remain application errors and must not turn into a false success.
bool ErrorClassifier::IsNormalResult(const Status& status, AsuOpType opType)
{
    if (status.ok()) { return true; }
    if (opType != AsuOpType::QUERY) { return false; }
    return status.code == StatusCode::ENTRY_KEY_EXIST ||
           status.code == StatusCode::ENTRY_KEY_NOT_EXIST ||
           status.code == StatusCode::CQE_KEY_NOT_EXIST;
}

}  // namespace kv
