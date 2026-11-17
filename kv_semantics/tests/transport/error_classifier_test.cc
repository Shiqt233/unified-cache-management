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
#include "task/error_classifier.h"
#include <gtest/gtest.h>
#include <vector>

namespace kv::test {
namespace {

Status MakeStatus(StatusCode code)
{
    return code == StatusCode::OK ? Status::OK() : Status::Error(code, "test status");
}

ErrorContext MakeContext(StatusCode code, ErrorOrigin origin,
                         AsuOpType opType = AsuOpType::STORE,
                         SendCertainty certainty = SendCertainty::RESPONSE_CONFIRMED,
                         std::uint32_t rawStatus = 0)
{
    ErrorContext context;
    context.status = MakeStatus(code);
    context.rawStatus = rawStatus;
    context.origin = origin;
    context.opType = opType;
    context.certainty = certainty;
    return context;
}

void ExpectDecision(const ErrorDecision& decision, ErrorCategory category, RetryAction retryAction,
                    ChannelHealthAction healthAction = ChannelHealthAction::NO_CHANGE,
                    FlowAction flowAction = FlowAction::NO_CHANGE)
{
    EXPECT_EQ(decision.category, category);
    EXPECT_EQ(decision.retryAction, retryAction);
    EXPECT_EQ(decision.healthAction, healthAction);
    EXPECT_EQ(decision.flowAction, flowAction);
}

TEST(ErrorClassifierTest, ClassifiesSuccessAndNormalQueryResults)
{
    ExpectDecision(ErrorClassifier::Classify(MakeContext(StatusCode::OK, ErrorOrigin::CQE)),
                   ErrorCategory::SUCCESS, RetryAction::RETURN_RESULT,
                   ChannelHealthAction::RECORD_SUCCESS, FlowAction::ON_BUSINESS_SUCCESS);

    EXPECT_TRUE(ErrorClassifier::IsNormalResult(MakeStatus(StatusCode::OK), AsuOpType::STORE));
    EXPECT_TRUE(ErrorClassifier::IsNormalResult(MakeStatus(StatusCode::ENTRY_KEY_EXIST),
                                                AsuOpType::QUERY));
    EXPECT_TRUE(ErrorClassifier::IsNormalResult(MakeStatus(StatusCode::ENTRY_KEY_NOT_EXIST),
                                                AsuOpType::QUERY));
    EXPECT_TRUE(ErrorClassifier::IsNormalResult(MakeStatus(StatusCode::CQE_KEY_NOT_EXIST),
                                                AsuOpType::QUERY));
    EXPECT_FALSE(ErrorClassifier::IsNormalResult(MakeStatus(StatusCode::ENTRY_KEY_EXIST),
                                                 AsuOpType::STORE));
    EXPECT_FALSE(ErrorClassifier::IsNormalResult(MakeStatus(StatusCode::ENTRY_KEY_NOT_EXIST),
                                                 AsuOpType::LOAD));
    EXPECT_FALSE(ErrorClassifier::IsNormalResult(MakeStatus(StatusCode::CQE_KEY_NOT_EXIST),
                                                 AsuOpType::LOAD));

    ExpectDecision(ErrorClassifier::Classify(MakeContext(StatusCode::ENTRY_KEY_EXIST,
                                                         ErrorOrigin::RESULT_ENTRY,
                                                         AsuOpType::QUERY)),
                   ErrorCategory::SUCCESS, RetryAction::RETURN_RESULT,
                   ChannelHealthAction::RECORD_SUCCESS, FlowAction::ON_BUSINESS_SUCCESS);

    for (const auto code : {StatusCode::ENTRY_KEY_NOT_EXIST, StatusCode::CQE_KEY_NOT_EXIST}) {
        SCOPED_TRACE(static_cast<int>(code));
        const auto origin = code == StatusCode::CQE_KEY_NOT_EXIST ? ErrorOrigin::CQE
                                                                  : ErrorOrigin::RESULT_ENTRY;
        ExpectDecision(ErrorClassifier::Classify(
                           MakeContext(code, origin, AsuOpType::QUERY)),
                       ErrorCategory::QUERY_MISS, RetryAction::RETURN_RESULT,
                       ChannelHealthAction::RECORD_SUCCESS, FlowAction::ON_BUSINESS_SUCCESS);
    }
}

TEST(ErrorClassifierTest, TreatsNormalQueryEntryCodesAsApplicationErrorsForOtherOperations)
{
    for (const auto code : {StatusCode::ENTRY_KEY_EXIST, StatusCode::ENTRY_KEY_NOT_EXIST,
                            StatusCode::CQE_KEY_NOT_EXIST}) {
        SCOPED_TRACE(static_cast<int>(code));
        const auto origin = code == StatusCode::CQE_KEY_NOT_EXIST ? ErrorOrigin::CQE
                                                                  : ErrorOrigin::RESULT_ENTRY;
        ExpectDecision(ErrorClassifier::Classify(MakeContext(code, origin, AsuOpType::LOAD)),
                       ErrorCategory::APPLICATION, RetryAction::RETURN_RESULT);
    }
}

TEST(ErrorClassifierTest, DistinguishesLocalBusyFromRemoteBusy)
{
    ExpectDecision(ErrorClassifier::Classify(
                       MakeContext(StatusCode::RESOURCE_BUSY, ErrorOrigin::LOCAL_QUEUE)),
                   ErrorCategory::LOCAL_BUSY, RetryAction::RETURN_RESULT);

    for (const auto origin : {ErrorOrigin::PROVIDER_SEND, ErrorOrigin::CQE}) {
        SCOPED_TRACE(static_cast<int>(origin));
        ExpectDecision(ErrorClassifier::Classify(
                           MakeContext(StatusCode::CQE_RESOURCE_BUSY, origin)),
                       ErrorCategory::BUSY, RetryAction::RETRY_SAME_CHANNEL,
                       ChannelHealthAction::NO_CHANGE, FlowAction::ON_BUSY);
    }
}

TEST(ErrorClassifierTest, ClassifiesExplicitLinkFailures)
{
    ExpectDecision(ErrorClassifier::Classify(
                       MakeContext(StatusCode::CONNECTION_ERROR, ErrorOrigin::PROVIDER_SEND)),
                   ErrorCategory::LINK_FAILURE, RetryAction::FAILOVER,
                   ChannelHealthAction::RECORD_FAILURE);

    for (const auto code : {StatusCode::CQE_INTERNAL_ERROR, StatusCode::CQE_WRITE_FAULT,
                            StatusCode::CQE_UNRECOVERED_READ_ERROR}) {
        SCOPED_TRACE(static_cast<int>(code));
        ExpectDecision(ErrorClassifier::Classify(MakeContext(code, ErrorOrigin::CQE)),
                       ErrorCategory::LINK_FAILURE, RetryAction::FAILOVER,
                       ChannelHealthAction::RECORD_FAILURE);
    }
}

TEST(ErrorClassifierTest, TimeoutUsesLinkFailureFailoverForAllOperations)
{
    for (const auto opType : {AsuOpType::LOAD, AsuOpType::STORE}) {
        SCOPED_TRACE(static_cast<int>(opType));
        ExpectDecision(ErrorClassifier::Classify(
                           MakeContext(StatusCode::CQE_IO_TIMEOUT, ErrorOrigin::CQE, opType)),
                       ErrorCategory::LINK_FAILURE, RetryAction::FAILOVER,
                       ChannelHealthAction::RECORD_FAILURE);
    }

    for (const auto opType : {AsuOpType::QUERY, AsuOpType::BATCH_STORE}) {
        SCOPED_TRACE(static_cast<int>(opType));
        ExpectDecision(ErrorClassifier::Classify(
                           MakeContext(StatusCode::TIMEOUT, ErrorOrigin::ATTEMPT_TIMEOUT, opType,
                                       SendCertainty::MAYBE_SENT)),
                       ErrorCategory::LINK_FAILURE, RetryAction::FAILOVER,
                       ChannelHealthAction::RECORD_FAILURE);
    }
}

TEST(ErrorClassifierTest, AttemptTimeoutBeforeSendHasKnownOutcomeAndDoesNotRetry)
{
    for (const auto certainty : {SendCertainty::NOT_SENT, SendCertainty::DEFINITELY_FAILED}) {
        SCOPED_TRACE(static_cast<int>(certainty));
        ExpectDecision(ErrorClassifier::Classify(MakeContext(StatusCode::TIMEOUT,
                                                             ErrorOrigin::ATTEMPT_TIMEOUT,
                                                             AsuOpType::STORE, certainty)),
                       ErrorCategory::TIMEOUT, RetryAction::RETURN_RESULT);
    }
}

TEST(ErrorClassifierTest, AttemptCertaintyPrecedesTimeoutStatusCode)
{
    const auto context = MakeContext(StatusCode::CQE_IO_TIMEOUT, ErrorOrigin::ATTEMPT_TIMEOUT,
                                     AsuOpType::STORE, SendCertainty::NOT_SENT);

    const auto decision = ErrorClassifier::Classify(context);

    EXPECT_EQ(decision.category, ErrorCategory::TIMEOUT);
    EXPECT_EQ(decision.retryAction, RetryAction::RETURN_RESULT);
}

TEST(ErrorClassifierTest, ClassifiesApplicationErrorsWithoutDamagingChannel)
{
    for (const auto code : {StatusCode::CQE_INVALID_COMMAND_OPCODE,
                            StatusCode::CQE_INVALID_FIELD_IN_COMMAND,
                            StatusCode::CQE_OUT_OF_CREATE_SIZE,
                            StatusCode::CQE_KEY_ALREADY_EXISTED}) {
        SCOPED_TRACE(static_cast<int>(code));
        ExpectDecision(ErrorClassifier::Classify(MakeContext(code, ErrorOrigin::CQE)),
                       ErrorCategory::APPLICATION, RetryAction::RETURN_RESULT);
    }

    for (const auto code : {StatusCode::ENTRY_NO_RETRY_ADVISED, StatusCode::ENTRY_KEY_NOT_FOUND,
                            StatusCode::ENTRY_DATA_NOT_EXIST, StatusCode::ENTRY_DELETE_FAILED}) {
        SCOPED_TRACE(static_cast<int>(code));
        ExpectDecision(ErrorClassifier::Classify(MakeContext(code, ErrorOrigin::RESULT_ENTRY)),
                       ErrorCategory::APPLICATION, RetryAction::RETURN_RESULT);
    }
}

TEST(ErrorClassifierTest, UnknownSendEntryAndCqeErrorsDoNotRetryOrChangeHealth)
{
    for (const auto code : {StatusCode::IO_ERROR, StatusCode::INTERNAL_ERROR,
                            StatusCode::RESOURCE_BUSY}) {
        SCOPED_TRACE(static_cast<int>(code));
        ExpectDecision(ErrorClassifier::Classify(MakeContext(code, ErrorOrigin::PROVIDER_SEND)),
                       ErrorCategory::UNKNOWN, RetryAction::RETURN_RESULT);
    }

    ExpectDecision(ErrorClassifier::Classify(MakeContext(StatusCode::ENTRY_RETRY_ADVISED,
                                                         ErrorOrigin::RESULT_ENTRY)),
                   ErrorCategory::UNKNOWN, RetryAction::RETURN_RESULT);
    ExpectDecision(ErrorClassifier::Classify(MakeContext(StatusCode::IO_ERROR, ErrorOrigin::CQE,
                                                         AsuOpType::STORE,
                                                         SendCertainty::RESPONSE_CONFIRMED,
                                                         0xFFFF)),
                   ErrorCategory::UNKNOWN, RetryAction::RETURN_RESULT);
    ExpectDecision(ErrorClassifier::Classify(MakeContext(StatusCode::CQE_CHECK_RESULT_BUFFER,
                                                         ErrorOrigin::CQE)),
                   ErrorCategory::UNKNOWN, RetryAction::RETURN_RESULT);
}

TEST(ErrorClassifierTest, LocalValidationAndProbeFailuresDoNotDamageChannel)
{
    for (const auto code : {StatusCode::INVALID_ARGUMENT, StatusCode::BUFFER_NOT_REGISTERED,
                            StatusCode::BUFFER_NOT_SUPPORTED, StatusCode::UNSUPPORTED}) {
        SCOPED_TRACE(static_cast<int>(code));
        ExpectDecision(ErrorClassifier::Classify(MakeContext(code, ErrorOrigin::LOCAL_VALIDATE)),
                       ErrorCategory::APPLICATION, RetryAction::RETURN_RESULT);
    }

    ExpectDecision(ErrorClassifier::Classify(
                       MakeContext(StatusCode::INTERNAL_ERROR, ErrorOrigin::PROBE_LOCAL,
                                   AsuOpType::KEEP_ALIVE, SendCertainty::NOT_SENT)),
                   ErrorCategory::INCONCLUSIVE, RetryAction::RETURN_RESULT);
}

TEST(ErrorClassifierTest, ClientWaitTimeoutIsOrdinaryTimeout)
{
    ExpectDecision(ErrorClassifier::Classify(MakeContext(StatusCode::TIMEOUT,
                                                         ErrorOrigin::CLIENT_WAIT,
                                                         AsuOpType::STORE,
                                                         SendCertainty::MAYBE_SENT)),
                   ErrorCategory::TIMEOUT, RetryAction::RETURN_RESULT);
}

TEST(ErrorClassifierTest, SubBatchWithNoFailuresCompletesSuccessfully)
{
    const auto result = ErrorClassifier::ClassifySubBatch(
        Status::OK(), {Status::OK(), Status::OK()}, AsuOpType::BATCH_STORE);

    ExpectDecision(result.decision, ErrorCategory::SUCCESS, RetryAction::RETURN_RESULT,
                   ChannelHealthAction::RECORD_SUCCESS, FlowAction::ON_BUSINESS_SUCCESS);
    EXPECT_FALSE(result.needsRetry);
    EXPECT_FALSE(result.hasMultipleErrorCodes);
}

TEST(ErrorClassifierTest, SubBatchExcludesNormalQueryResultsFromFailureCodes)
{
    const auto result = ErrorClassifier::ClassifySubBatch(
        MakeStatus(StatusCode::CQE_CHECK_RESULT_BUFFER),
        {MakeStatus(StatusCode::ENTRY_KEY_EXIST), MakeStatus(StatusCode::ENTRY_KEY_NOT_EXIST),
         MakeStatus(StatusCode::CQE_KEY_NOT_EXIST)},
        AsuOpType::QUERY);

    ExpectDecision(result.decision, ErrorCategory::QUERY_MISS, RetryAction::RETURN_RESULT,
                   ChannelHealthAction::RECORD_SUCCESS, FlowAction::ON_BUSINESS_SUCCESS);
    EXPECT_FALSE(result.needsRetry);
    EXPECT_FALSE(result.hasMultipleErrorCodes);

    const auto allExist = ErrorClassifier::ClassifySubBatch(
        MakeStatus(StatusCode::CQE_CHECK_RESULT_BUFFER),
        {MakeStatus(StatusCode::ENTRY_KEY_EXIST), MakeStatus(StatusCode::ENTRY_KEY_EXIST)},
        AsuOpType::QUERY);
    EXPECT_EQ(allExist.decision.category, ErrorCategory::SUCCESS);
    EXPECT_FALSE(allExist.needsRetry);
}

TEST(ErrorClassifierTest, SubBatchUsesSingleErrorDecision)
{
    const auto result = ErrorClassifier::ClassifySubBatch(
        MakeStatus(StatusCode::CQE_RESOURCE_BUSY),
        {MakeStatus(StatusCode::CQE_RESOURCE_BUSY), MakeStatus(StatusCode::CQE_RESOURCE_BUSY)},
        AsuOpType::BATCH_STORE);

    ExpectDecision(result.decision, ErrorCategory::BUSY, RetryAction::RETRY_SAME_CHANNEL,
                   ChannelHealthAction::NO_CHANGE, FlowAction::ON_BUSY);
    EXPECT_TRUE(result.needsRetry);
    EXPECT_FALSE(result.hasMultipleErrorCodes);
}

TEST(ErrorClassifierTest, SubBatchCanClassifyTopLevelErrorWithoutEntryStatuses)
{
    const auto result = ErrorClassifier::ClassifySubBatch(
        MakeStatus(StatusCode::CQE_RESOURCE_BUSY), {}, AsuOpType::STORE);

    EXPECT_EQ(result.decision.category, ErrorCategory::BUSY);
    EXPECT_EQ(result.decision.retryAction, RetryAction::RETRY_SAME_CHANNEL);
    EXPECT_TRUE(result.needsRetry);
    EXPECT_FALSE(result.hasMultipleErrorCodes);
}

TEST(ErrorClassifierTest, MixedErrorCodesFailOverWholeSubBatchWhenAnyCodeIsRetryable)
{
    const auto result = ErrorClassifier::ClassifySubBatch(
        MakeStatus(StatusCode::CQE_CHECK_RESULT_BUFFER),
        {Status::OK(), MakeStatus(StatusCode::ENTRY_NO_RETRY_ADVISED),
         MakeStatus(StatusCode::CQE_RESOURCE_BUSY)},
        AsuOpType::BATCH_STORE);

    EXPECT_EQ(result.decision.category, ErrorCategory::MIXED);
    EXPECT_EQ(result.decision.retryAction, RetryAction::FAILOVER);
    EXPECT_EQ(result.decision.flowAction, FlowAction::ON_BUSY);
    EXPECT_TRUE(result.needsRetry);
    EXPECT_TRUE(result.hasMultipleErrorCodes);
}

TEST(ErrorClassifierTest, MixedErrorCodesDoNotRetryWhenEveryCodeIsNonRetryable)
{
    const auto result = ErrorClassifier::ClassifySubBatch(
        MakeStatus(StatusCode::CQE_CHECK_RESULT_BUFFER),
        {MakeStatus(StatusCode::ENTRY_NO_RETRY_ADVISED),
         MakeStatus(StatusCode::ENTRY_RETRY_ADVISED)},
        AsuOpType::BATCH_STORE);

    ExpectDecision(result.decision, ErrorCategory::MIXED, RetryAction::RETURN_RESULT);
    EXPECT_FALSE(result.needsRetry);
    EXPECT_TRUE(result.hasMultipleErrorCodes);
}

TEST(ErrorClassifierTest, MixedTimeoutErrorsFailOverWholeSubBatch)
{
    for (const auto opType : {AsuOpType::BATCH_STORE, AsuOpType::BATCH_LOAD}) {
        SCOPED_TRACE(static_cast<int>(opType));
        const auto result = ErrorClassifier::ClassifySubBatch(
            MakeStatus(StatusCode::CQE_CHECK_RESULT_BUFFER),
            {MakeStatus(StatusCode::CQE_RESOURCE_BUSY), MakeStatus(StatusCode::CQE_IO_TIMEOUT),
             MakeStatus(StatusCode::ENTRY_NO_RETRY_ADVISED)},
            opType);

        EXPECT_EQ(result.decision.category, ErrorCategory::MIXED);
        EXPECT_EQ(result.decision.retryAction, RetryAction::FAILOVER);
        EXPECT_EQ(result.decision.healthAction, ChannelHealthAction::RECORD_FAILURE);
        EXPECT_TRUE(result.needsRetry);
        EXPECT_TRUE(result.hasMultipleErrorCodes);
    }
}

TEST(ErrorClassifierTest, SingleTimeoutUsesLinkFailureFailover)
{
    const auto result = ErrorClassifier::ClassifySubBatch(
        MakeStatus(StatusCode::CQE_IO_TIMEOUT), {MakeStatus(StatusCode::CQE_IO_TIMEOUT)},
        AsuOpType::STORE);

    ExpectDecision(result.decision, ErrorCategory::LINK_FAILURE, RetryAction::FAILOVER,
                   ChannelHealthAction::RECORD_FAILURE);
    EXPECT_TRUE(result.needsRetry);
    EXPECT_FALSE(result.hasMultipleErrorCodes);
}

}  // namespace
}  // namespace kv::test
