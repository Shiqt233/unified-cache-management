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

#include <cstdint>
#include <vector>
#include "kv_types.h"
#include "task_context.h"

namespace kv {

enum class ErrorOrigin {
    LOCAL_VALIDATE,
    LOCAL_QUEUE,
    PROVIDER_CONNECT,
    PROVIDER_SEND,
    CQE,
    RESULT_ENTRY,
    // One attempt exceeded its response deadline. SendCertainty determines whether
    // this is an ordinary timeout or an outcome-unknown result.
    ATTEMPT_TIMEOUT,
    PROBE_LOCAL,
    // The caller stopped waiting; this alone says nothing about server execution.
    CLIENT_WAIT,
    SHUTDOWN,
};

enum class ErrorCategory {
    SUCCESS,
    APPLICATION,
    QUERY_MISS,
    LOCAL_BUSY,
    BUSY,
    LINK_FAILURE,
    // Ordinary wait/deadline timeout with no special outcome category.
    TIMEOUT,
    INCONCLUSIVE,
    UNKNOWN,
    MIXED,
};

enum class RetryAction {
    RETURN_RESULT,
    RETRY_SAME_CHANNEL,
    FAILOVER,
};

enum class ChannelHealthAction {
    RECORD_SUCCESS,
    RECORD_FAILURE,
    NO_CHANGE,
};

enum class FlowAction {
    // Only a remote busy response may drive the congestion controller.
    ON_BUSY,
    ON_BUSINESS_SUCCESS,
    NO_CHANGE,
};

enum class SendCertainty {
    NOT_SENT,
    DEFINITELY_FAILED,
    // The provider may have accepted the request. No response is therefore not
    // evidence that a non-idempotent write was not executed.
    MAYBE_SENT,
    RESPONSE_CONFIRMED,
};

struct ErrorContext {
    Status status{Status::OK()};
    std::uint32_t rawStatus{0};
    ErrorOrigin origin{ErrorOrigin::CQE};
    AsuOpType opType{AsuOpType::QUERY};
    SendCertainty certainty{SendCertainty::NOT_SENT};
};

struct ErrorDecision {
    ErrorCategory category{ErrorCategory::UNKNOWN};
    RetryAction retryAction{RetryAction::RETURN_RESULT};
    ChannelHealthAction healthAction{ChannelHealthAction::NO_CHANGE};
    FlowAction flowAction{FlowAction::NO_CHANGE};
};

struct SubBatchRetryDecision {
    ErrorDecision decision;
    bool needsRetry{false};
    bool hasMultipleErrorCodes{false};
};

class ErrorClassifier {
public:
    static ErrorDecision Classify(const ErrorContext& context);

    // entryStatus must already be decoded when subBatchStatus is CQE_CHECK_RESULT_BUFFER.
    // OK and normal Query hit/miss results are excluded from failure-code counting.
    // One failure-code type uses its own strategy; mixed types fail over only when at
    // least one type needs retry.
    static SubBatchRetryDecision ClassifySubBatch(
        const Status& subBatchStatus, const std::vector<Status>& entryStatus, AsuOpType opType,
        ErrorOrigin origin = ErrorOrigin::CQE);

    // Query hit/miss is normal only for QUERY; the same raw value is an application
    // error for STORE/LOAD/DELETE and must not be normalized to success there.
    static bool IsNormalResult(const Status& status, AsuOpType opType);
};

}  // namespace kv
