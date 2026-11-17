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
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#define private public
#include "kv_transport_impl.h"
#undef private
#include "conn/connection_internal.h"
#include "trans_provider.h"

namespace kv::test {
namespace {

struct AttemptScript {
    Status sendStatus{Status::OK()};
    std::uint16_t cqeStatus{0};
    std::uint32_t resultWord{0};
};

class ScriptedTransProvider : public TransProvider {
public:
    std::vector<AttemptScript> scripts;
    std::atomic<std::size_t> sendCalls{0};
    std::vector<ConnectionHandle> sentHandles;
    std::vector<std::uint16_t> sentCids;

    Status CreateConnection(const std::string&, const std::string&, std::uint32_t,
                            std::uint32_t qpNum, std::uint32_t,
                            std::vector<ConnectionHandle>& handles) override
    {
        handles.clear();
        for (std::uint32_t i = 0; i < qpNum; ++i) {
            handles.push_back(reinterpret_cast<ConnectionHandle>(nextHandle_.fetch_add(1)));
        }
        return Status::OK();
    }

    std::vector<Status> DeleteConnections(const std::vector<ConnectionHandle>& handles) override
    {
        return std::vector<Status>(handles.size(), Status::OK());
    }

    std::vector<Status> Send(const std::vector<SendIoBatch>& ioBatches, std::uint32_t,
                             std::uint32_t) override
    {
        const auto call = sendCalls.fetch_add(1);
        const auto& script = scripts[std::min(call, scripts.size() - 1)];
        std::vector<Status> result;
        result.reserve(ioBatches.size());
        for (const auto& batch : ioBatches) {
            sentHandles.push_back(batch.connectionHandle);
            const auto cid = (static_cast<const std::uint32_t*>(batch.sendBuffer)[0] >> 16) & 0xFFFFU;
            sentCids.push_back(static_cast<std::uint16_t>(cid));
            auto* cqe = static_cast<std::uint32_t*>(batch.flagBuffer);
            cqe[3] = cid | (static_cast<std::uint32_t>(script.cqeStatus) << 17);
            cqe[4] = script.resultWord;
            result.push_back(script.sendStatus);
        }
        return result;
    }

    Status RegisterMemory(const std::vector<RegisterMemoryDesc>& descs,
                          std::vector<MRHandle>& handles) override
    {
        handles.clear();
        for (std::size_t i = 0; i < descs.size(); ++i) {
            handles.push_back(reinterpret_cast<MRHandle>(static_cast<std::uintptr_t>(i + 1)));
        }
        return Status::OK();
    }

    std::vector<Status> UnregisterMemory(const std::vector<UnregisterMemoryDesc>& descs) override
    {
        return std::vector<Status>(descs.size(), Status::OK());
    }

    Status AllocThread(std::uint32_t, const std::vector<std::uint32_t>&,
                       std::vector<ThreadHandle>&) override
    {
        return Status::OK();
    }

    std::vector<Status> FreeThread(const std::vector<ThreadHandle>&) override { return {}; }

    Status GetMemTokenId(MRHandle, std::uint32_t& tokenId) override
    {
        tokenId = 1;
        return Status::OK();
    }

private:
    std::atomic<std::uintptr_t> nextHandle_{100};
};

TransportConfig MakeConfig()
{
    TransportConfig config;
    config.endpoints = {NodeEndpoint{}};
    config.qpNum = 2;
    config.timeoutMs = 1000;
    config.maxInflightTasks = 16;
    config.maxErrorCount = 1;
    config.sendBufferSlotSize = 4096;
    config.sendBufferSlotNum = 16;
    config.flagBufferSlotSize = 128;
    config.flagBufferSlotNum = 16;
    config.completionPollSpinLimit = 1;
    return config;
}

TransportTaskPtr MakeBatchTask(std::size_t count)
{
    auto task = std::make_shared<TransportTask>();
    task->opType = AsuOpType::BATCH_STORE;
    task->entries.resize(count);
    task->entryStatus.assign(count, Status::OK());
    task->originalIndices.resize(count);
    for (std::size_t i = 0; i < count; ++i) {
        auto& entry = task->entries[i];
        entry.key[0] = static_cast<std::byte>(i + 1);
        entry.buffer.region.addr = 0x1000 + i * 512;
        entry.buffer.region.size = 512;
        entry.buffer.handle = 1;
        entry.mrKey = 1;
        entry.offset = 0;
        task->originalIndices[i] = i;
    }
    return task;
}

TransportTaskPtr MakeQueryTask(std::size_t count)
{
    auto task = std::make_shared<TransportTask>();
    task->opType = AsuOpType::QUERY;
    task->keys.resize(count);
    task->entryStatus.assign(count, Status::OK());
    for (std::size_t i = 0; i < count; ++i) { task->keys[i][0] = static_cast<std::byte>(i + 1); }
    return task;
}

void RetireChannel(ConnectionManager& manager, const std::shared_ptr<ConnectionChannel>& channel,
                   TransProvider& provider)
{
    auto* group = channel->GetGroup();
    group->RemoveChannel(channel.get());
    group->AddChannel(reinterpret_cast<ConnectionHandle>(static_cast<std::uintptr_t>(0x200)),
                      &provider);
    manager.cacheDirty_.store(true, std::memory_order_release);
}

TaskResult WaitForCompletion(AsuTransportImpl& transport, const TransportTaskPtr& task)
{
    std::mutex mutex;
    std::condition_variable cv;
    bool completed = false;
    TaskResult result;
    task->onComplete = [&](TaskResult value) {
        std::lock_guard<std::mutex> lock(mutex);
        result = std::move(value);
        completed = true;
        cv.notify_one();
    };

    const auto submitStatus = transport.Submit(task);
    if (!submitStatus.ok()) {
        result.status = submitStatus;
        return result;
    }

    std::unique_lock<std::mutex> lock(mutex);
    if (!cv.wait_for(lock, std::chrono::seconds(3), [&] { return completed; })) {
        result.status = Status::Error(StatusCode::TIMEOUT, "test completion wait timed out");
    }
    return result;
}

}  // namespace

TEST(RetryTransportTest, BusyRetriesOriginalChannelWithNewCid)
{
    auto provider = std::make_shared<ScriptedTransProvider>();
    provider->scripts = {
        AttemptScript{Status::OK(), 0x731, 0},
        AttemptScript{Status::OK(), 0x731, 0},
        AttemptScript{Status::OK(), 0, 0},
    };
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(MakeConfig(), provider).ok());
    auto result = WaitForCompletion(transport, MakeBatchTask(1));

    EXPECT_TRUE(result.status.ok()) << result.status.message;
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{3});
    ASSERT_EQ(provider->sentCids.size(), std::size_t{3});
    EXPECT_NE(provider->sentCids[0], provider->sentCids[1]);
    EXPECT_NE(provider->sentCids[1], provider->sentCids[2]);
    EXPECT_EQ(provider->sentHandles[0], provider->sentHandles[1]);
    EXPECT_EQ(provider->sentHandles[1], provider->sentHandles[2]);
    EXPECT_EQ(result.entryStatus.size(), std::size_t{1});
    EXPECT_TRUE(result.entryStatus[0].ok());
    EXPECT_TRUE(transport.Shutdown().ok());
}

TEST(RetryTransportTest, ExplicitLinkFailureFailsOverToAnotherActiveChannel)
{
    auto provider = std::make_shared<ScriptedTransProvider>();
    provider->scripts = {
        AttemptScript{Status::OK(), 0x006, 0},
        AttemptScript{Status::OK(), 0, 0},
    };
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(MakeConfig(), provider).ok());
    auto result = WaitForCompletion(transport, MakeBatchTask(1));

    EXPECT_TRUE(result.status.ok()) << result.status.message;
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{2});
    ASSERT_EQ(provider->sentHandles.size(), std::size_t{2});
    EXPECT_NE(provider->sentHandles[0], provider->sentHandles[1]);
    EXPECT_TRUE(result.entryStatus[0].ok());
    EXPECT_TRUE(transport.Shutdown().ok());
}

TEST(RetryTransportTest, StoreTimeoutFailsOverToAnotherActiveChannel)
{
    auto provider = std::make_shared<ScriptedTransProvider>();
    provider->scripts = {
        AttemptScript{Status::OK(), 0x716, 0},
        AttemptScript{Status::OK(), 0, 0},
    };
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(MakeConfig(), provider).ok());
    auto result = WaitForCompletion(transport, MakeBatchTask(1));

    EXPECT_TRUE(result.status.ok()) << result.status.message;
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{2});
    ASSERT_EQ(provider->sentHandles.size(), std::size_t{2});
    EXPECT_NE(provider->sentHandles[0], provider->sentHandles[1]);
    ASSERT_EQ(result.entryStatus.size(), std::size_t{1});
    EXPECT_TRUE(result.entryStatus[0].ok());
    EXPECT_TRUE(transport.Shutdown().ok());
}

TEST(RetryTransportTest, QueryTimeoutFailsOverToAnotherActiveChannel)
{
    auto provider = std::make_shared<ScriptedTransProvider>();
    provider->scripts = {
        AttemptScript{Status::OK(), 0x716, 0},
        AttemptScript{Status::OK(), 0, 0},
    };
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(MakeConfig(), provider).ok());
    auto result = WaitForCompletion(transport, MakeQueryTask(1));

    EXPECT_TRUE(result.status.ok()) << result.status.message;
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{2});
    ASSERT_EQ(provider->sentHandles.size(), std::size_t{2});
    EXPECT_NE(provider->sentHandles[0], provider->sentHandles[1]);
    ASSERT_TRUE(result.queryResult.has_value());
    ASSERT_EQ(result.queryResult->exists.size(), std::size_t{1});
    EXPECT_EQ(result.queryResult->exists[0], std::uint8_t{1});
    EXPECT_TRUE(transport.Shutdown().ok());
}

TEST(RetryTransportTest, MixedErrorsFailOverWholeSubBatch)
{
    auto provider = std::make_shared<ScriptedTransProvider>();
    provider->scripts = {
        AttemptScript{Status::OK(), 0, 0},
        AttemptScript{Status::OK(), 0, 0},
    };
    auto connectionManager = std::make_unique<ConnectionManager>(*provider, "", 5000);
    ASSERT_TRUE(connectionManager->AddGroup(NodeEndpoint{}, 2).ok());
    auto config = MakeConfig();
    std::shared_ptr<TransProvider> providerBase = provider;
    TransportTaskExecutor executor(config, providerBase, connectionManager);
    ASSERT_TRUE(executor.Init().ok());

    auto task = MakeBatchTask(2);
    task->deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    ASSERT_FALSE(executor.Execute(task));

    // Feed the executor a decoded mixed result: one application error and one busy Entry.
    // The presence of busy makes the complete original subBatch fail over.
    auto& subBatch = (*task->subBatchContexts)[0];
    subBatch.status = Status::Error(StatusCode::CQE_CHECK_RESULT_BUFFER, "mixed entry results");
    subBatch.entryStatus[0] = Status::Error(StatusCode::ENTRY_KEY_NOT_FOUND, "missing key");
    subBatch.entryStatus[1] = Status::Error(StatusCode::CQE_RESOURCE_BUSY, "remote busy");
    EXPECT_TRUE(executor.HandleAttemptFailure(*task, subBatch, subBatch.status, ErrorOrigin::CQE));
    EXPECT_EQ(subBatch.retryState.retryAction, RetryAction::FAILOVER);

    EXPECT_FALSE(executor.ExecuteRetry(task));
    EXPECT_TRUE(executor.Poll(task));
    TaskResult result;
    TransportTaskManager::BuildResult(*task, result);
    EXPECT_TRUE(result.status.ok()) << result.status.message;
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{2});
    ASSERT_EQ(provider->sentHandles.size(), std::size_t{2});
    EXPECT_NE(provider->sentHandles[0], provider->sentHandles[1]);
    ASSERT_EQ(result.entryStatus.size(), std::size_t{2});
    EXPECT_TRUE(result.entryStatus[0].ok());
    EXPECT_TRUE(result.entryStatus[1].ok());
    EXPECT_TRUE(executor.Shutdown().ok());
    EXPECT_TRUE(connectionManager->Shutdown().ok());
}

TEST(RetryTransportTest, StaleGenerationQueryResponseFailsOverWithoutRequeue)
{
    auto provider = std::make_shared<ScriptedTransProvider>();
    provider->scripts = {
        AttemptScript{Status::OK(), 0, 0},
        AttemptScript{Status::OK(), 0, 0},
    };
    auto connectionManager = std::make_unique<ConnectionManager>(*provider, "", 5000, 1);
    ASSERT_TRUE(connectionManager->AddGroup(NodeEndpoint{}, 2).ok());
    auto config = MakeConfig();
    std::shared_ptr<TransProvider> providerBase = provider;
    TransportTaskExecutor executor(config, providerBase, connectionManager);
    ASSERT_TRUE(executor.Init().ok());

    auto task = MakeQueryTask(1);
    task->deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    ASSERT_FALSE(executor.Execute(task));
    auto& subBatch = (*task->subBatchContexts)[0];
    auto oldChannel = subBatch.channel;
    const auto oldGeneration = subBatch.currentAttempt.generation;
    RetireChannel(*connectionManager, oldChannel, *provider);

    EXPECT_FALSE(executor.Poll(task));
    EXPECT_TRUE(subBatch.retryState.retryPending);
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{1});
    EXPECT_TRUE(std::none_of(connectionManager->drainList_.begin(), connectionManager->drainList_.end(),
                             [&](const auto& channel) { return channel.get() == oldChannel.get(); }));

    EXPECT_FALSE(executor.ExecuteRetry(task));
    EXPECT_TRUE(executor.Poll(task));
    TaskResult result;
    TransportTaskManager::BuildResult(*task, result);
    EXPECT_TRUE(result.status.ok()) << result.status.message;
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{2});
    ASSERT_EQ(provider->sentHandles.size(), std::size_t{2});
    EXPECT_NE(provider->sentHandles[0], provider->sentHandles[1]);
    EXPECT_GT(subBatch.currentAttempt.generation, oldGeneration);
    EXPECT_TRUE(std::none_of(connectionManager->drainList_.begin(), connectionManager->drainList_.end(),
                             [&](const auto& channel) { return channel.get() == oldChannel.get(); }));
    EXPECT_TRUE(executor.Shutdown().ok());
    EXPECT_TRUE(connectionManager->Shutdown().ok());
}

TEST(RetryTransportTest, StaleGenerationStoreResponseFailsOverWithoutRequeue)
{
    auto provider = std::make_shared<ScriptedTransProvider>();
    provider->scripts = {
        AttemptScript{Status::OK(), 0, 0},
        AttemptScript{Status::OK(), 0, 0},
    };
    auto connectionManager = std::make_unique<ConnectionManager>(*provider, "", 5000, 1);
    ASSERT_TRUE(connectionManager->AddGroup(NodeEndpoint{}, 2).ok());
    auto config = MakeConfig();
    std::shared_ptr<TransProvider> providerBase = provider;
    TransportTaskExecutor executor(config, providerBase, connectionManager);
    ASSERT_TRUE(executor.Init().ok());

    auto task = MakeBatchTask(1);
    task->deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    ASSERT_FALSE(executor.Execute(task));
    auto& subBatch = (*task->subBatchContexts)[0];
    auto oldChannel = subBatch.channel;
    const auto oldGeneration = subBatch.currentAttempt.generation;
    RetireChannel(*connectionManager, oldChannel, *provider);

    EXPECT_FALSE(executor.Poll(task));
    EXPECT_TRUE(subBatch.retryState.retryPending);
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{1});
    EXPECT_TRUE(std::none_of(connectionManager->drainList_.begin(), connectionManager->drainList_.end(),
                             [&](const auto& channel) { return channel.get() == oldChannel.get(); }));

    EXPECT_FALSE(executor.ExecuteRetry(task));
    EXPECT_TRUE(executor.Poll(task));
    TaskResult result;
    TransportTaskManager::BuildResult(*task, result);
    EXPECT_TRUE(result.status.ok()) << result.status.message;
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{2});
    ASSERT_EQ(provider->sentHandles.size(), std::size_t{2});
    EXPECT_NE(provider->sentHandles[0], provider->sentHandles[1]);
    EXPECT_GT(subBatch.currentAttempt.generation, oldGeneration);
    EXPECT_TRUE(std::none_of(connectionManager->drainList_.begin(), connectionManager->drainList_.end(),
                             [&](const auto& channel) { return channel.get() == oldChannel.get(); }));
    EXPECT_TRUE(executor.Shutdown().ok());
    EXPECT_TRUE(connectionManager->Shutdown().ok());
}

TEST(RetryTransportTest, AllNonRetryableEntriesCompleteWithoutAnotherSend)
{
    auto provider = std::make_shared<ScriptedTransProvider>();
    provider->scripts = {AttemptScript{Status::OK(), 0x732, 0x03 | (0x01U << 4)}};
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(MakeConfig(), provider).ok());
    auto result = WaitForCompletion(transport, MakeBatchTask(2));

    EXPECT_EQ(result.status.code, StatusCode::PARTIAL_FAILED);
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{1});
    ASSERT_EQ(result.entryStatus.size(), std::size_t{2});
    EXPECT_EQ(result.entryStatus[0].code, StatusCode::ENTRY_KEY_NOT_FOUND);
    EXPECT_EQ(result.entryStatus[1].code, StatusCode::ENTRY_RETRY_ADVISED);
    EXPECT_TRUE(transport.Shutdown().ok());
}

TEST(RetryTransportTest, QueryKeyNotExistIsNormalResult)
{
    auto provider = std::make_shared<ScriptedTransProvider>();
    provider->scripts = {AttemptScript{Status::OK(), 0x701, 0}};
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(MakeConfig(), provider).ok());
    auto result = WaitForCompletion(transport, MakeQueryTask(1));

    EXPECT_TRUE(result.status.ok()) << result.status.message;
    ASSERT_TRUE(result.queryResult.has_value());
    ASSERT_EQ(result.queryResult->exists.size(), std::size_t{1});
    EXPECT_EQ(result.queryResult->exists[0], std::uint8_t{0});
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{1});
    EXPECT_TRUE(transport.Shutdown().ok());
}

TEST(RetryTransportTest, QueryMixedNormalResultsDoNotPartialFail)
{
    auto provider = std::make_shared<ScriptedTransProvider>();
    provider->scripts = {AttemptScript{Status::OK(), 0x732, 0x01}};
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(MakeConfig(), provider).ok());
    auto result = WaitForCompletion(transport, MakeQueryTask(2));

    EXPECT_TRUE(result.status.ok()) << result.status.message;
    ASSERT_TRUE(result.queryResult.has_value());
    ASSERT_EQ(result.queryResult->exists.size(), std::size_t{2});
    EXPECT_EQ(result.queryResult->exists[0], std::uint8_t{1});
    EXPECT_EQ(result.queryResult->exists[1], std::uint8_t{0});
    EXPECT_TRUE(transport.Shutdown().ok());
}

TEST(RetryTransportTest, FlowWaitWakesAfterChannelRelease)
{
    auto provider = std::make_shared<ScriptedTransProvider>();
    provider->scripts = {AttemptScript{Status::OK(), 0, 0}};
    auto config = MakeConfig();
    config.qpNum = 1;
    config.maxInflightPerChannel = 1;
    config.flowControlEnabled = true;
    config.channelFullPolicy = "flow_wait";
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(config, provider).ok());
    auto held = transport.connManager_->SelectConnection();
    ASSERT_NE(held, nullptr);

    std::thread release([held] {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        held->ReleaseInflight();
        held->GetGroup()->ReleaseInflight();
    });
    auto result = WaitForCompletion(transport, MakeBatchTask(1));
    release.join();

    EXPECT_TRUE(result.status.ok()) << result.status.message;
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{1});
    EXPECT_TRUE(transport.Shutdown().ok());
}

TEST(RetryTransportTest, FlowWaitDeadlineStopsWithoutSending)
{
    auto provider = std::make_shared<ScriptedTransProvider>();
    provider->scripts = {AttemptScript{Status::OK(), 0, 0}};
    auto config = MakeConfig();
    config.qpNum = 1;
    config.maxInflightPerChannel = 1;
    config.flowControlEnabled = true;
    config.channelFullPolicy = "flow_wait";
    config.timeoutMs = 30;
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(config, provider).ok());
    auto held = transport.connManager_->SelectConnection();
    ASSERT_NE(held, nullptr);

    auto result = WaitForCompletion(transport, MakeBatchTask(1));
    EXPECT_EQ(result.status.code, StatusCode::TIMEOUT);
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{0});
    held->ReleaseInflight();
    held->GetGroup()->ReleaseInflight();
    EXPECT_TRUE(transport.Shutdown().ok());
}

TEST(RetryTransportTest, BusyRetryBackoffIsHonored)
{
    auto provider = std::make_shared<ScriptedTransProvider>();
    provider->scripts = {
        AttemptScript{Status::OK(), 0x731, 0},
        AttemptScript{Status::OK(), 0, 0},
    };
    auto config = MakeConfig();
    config.busyRetryCount = 1;
    config.busyRetryBackoffMs = 5;
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(config, provider).ok());

    const auto startedAt = std::chrono::steady_clock::now();
    auto result = WaitForCompletion(transport, MakeBatchTask(1));
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - startedAt);

    EXPECT_TRUE(result.status.ok()) << result.status.message;
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{2});
    EXPECT_GE(elapsed.count(), 4);
    EXPECT_TRUE(transport.Shutdown().ok());
}

}  // namespace kv::test
