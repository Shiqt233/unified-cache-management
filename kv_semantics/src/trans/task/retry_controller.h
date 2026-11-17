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

#include <memory>
#include "task/error_classifier.h"
#include "task/trans_task_manager.h"

namespace kv {

enum class SubBatchRetryAction : std::uint8_t {
    // No further attempt is needed; preserve the final response for aggregation.
    COMPLETE,
    // One failure-code type drives its own strategy (busy/link/timeout/application).
    SINGLE_ERROR_STRATEGY,
    // Any retryable error in a mixed response causes the complete original subBatch
    // to fail over. Successful and application-error entries may be sent again.
    FAILOVER_WHOLE_SUBBATCH,
};

struct SubBatchRequestDescriptor {
    AsuOpType opType{AsuOpType::QUERY};
    // Immutable template for the complete original subBatch, not only failed entries.
    // Attempt resources are cloned per attempt; payload bytes are not copied here.
    std::shared_ptr<const SqeRequest> request;
};

struct SubBatchRetryPlan {
    SubBatchRetryAction action{SubBatchRetryAction::COMPLETE};
    ErrorDecision decision;
    bool needsRetry{false};
    bool hasMultipleErrorCodes{false};
    SubBatchRequestDescriptor retryRequest;
};

class RetryController {
public:
    explicit RetryController(const TransportConfig& config) : config_(config) {}

    SubBatchRetryPlan BuildSubBatchRetryPlan(const TransportSubBatchContext& batch) const;
    SubBatchRetryPlan BuildSubBatchRetryPlan(const TransportSubBatchContext& batch,
                                             const Status& status, ErrorOrigin origin) const;

private:
    const TransportConfig& config_;
    ErrorClassifier classifier_;
};

}  // namespace kv
