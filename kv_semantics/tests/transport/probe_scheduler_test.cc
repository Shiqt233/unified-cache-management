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
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#define private public
#include "kv_transport_impl.h"
#undef private
#include "conn/connection_internal.h"
#include "proto/kv_protocol.h"
#include "task/probe_scheduler.h"
#include "../common/probe_stub_provider.h"

namespace kv::test {
namespace {

bool WaitForTasksToDrain(AsuTransportImpl& transport)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (transport.taskManager_.GetAll().empty()) { return true; }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return transport.taskManager_.GetAll().empty();
}

void EnableManualProbe(TransportConfig& config) { config.probeEnabled = true; }

}  // namespace

TEST(ProbeSchedulerTest, ProbesOnlyIdleActiveChannelsAndReleasesProbeLease)
{
    auto provider = std::make_shared<ProbeStubProvider>();
    auto config = MakeProbeConfig();
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(config, provider).ok());
    ProbeScheduler scheduler(transport, *transport.connManager_, config);
    EnableManualProbe(config);

    scheduler.RunOnce();
    ASSERT_TRUE(WaitForTasksToDrain(transport));
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{2});
    ASSERT_EQ(provider->sentHandles.size(), std::size_t{2});
    for (const auto& channel : transport.connManager_->GetChannelsSnapshot()) {
        EXPECT_FALSE(channel->IsProbeInFlight());
    }
    EXPECT_TRUE(transport.Shutdown().ok());
}

TEST(ProbeSchedulerTest, BusyChannelIsSkipped)
{
    auto provider = std::make_shared<ProbeStubProvider>();
    auto config = MakeProbeConfig();
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(config, provider).ok());
    auto busyChannel = transport.connManager_->SelectConnection();
    ASSERT_NE(busyChannel, nullptr);
    ProbeScheduler scheduler(transport, *transport.connManager_, config);
    EnableManualProbe(config);

    scheduler.RunOnce();
    ASSERT_TRUE(WaitForTasksToDrain(transport));
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{1});
    ASSERT_EQ(provider->sentHandles.size(), std::size_t{1});
    EXPECT_NE(provider->sentHandles[0], busyChannel->GetConnection());
    busyChannel->ReleaseInflight();
    EXPECT_TRUE(transport.Shutdown().ok());
}

TEST(ProbeSchedulerTest, ProbeDoesNotOverlapAcrossPeriods)
{
    auto provider = std::make_shared<ProbeStubProvider>();
    auto config = MakeProbeConfig();
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(config, provider).ok());
    ProbeScheduler scheduler(transport, *transport.connManager_, config);
    EnableManualProbe(config);

    scheduler.RunOnce();
    scheduler.RunOnce();
    ASSERT_TRUE(WaitForTasksToDrain(transport));
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{2});

    scheduler.RunOnce();
    ASSERT_TRUE(WaitForTasksToDrain(transport));
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{4});
    EXPECT_TRUE(transport.Shutdown().ok());
}

TEST(ProbeSchedulerTest, CancelBeforeExecuteReleasesProbeLease)
{
    auto provider = std::make_shared<ProbeStubProvider>();
    auto config = MakeProbeConfig();
    auto connectionManager = std::make_unique<ConnectionManager>(*provider, "", 5000, 1);
    ASSERT_TRUE(connectionManager->AddGroup(NodeEndpoint{}, 1).ok());
    const auto channels = connectionManager->GetChannelsSnapshot();
    ASSERT_EQ(channels.size(), std::size_t{1});
    const auto& channel = channels.front();
    ASSERT_TRUE(connectionManager->AcquireProbe(channel, channel->GetGeneration()));
    EXPECT_TRUE(channel->IsProbeInFlight());

    std::shared_ptr<TransProvider> providerBase = provider;
    TransportTaskExecutor executor(config, providerBase, connectionManager);
    ASSERT_TRUE(executor.Init().ok());
    auto task = std::make_shared<TransportTask>();
    task->opType = AsuOpType::KEEP_ALIVE;
    task->origin = TaskOrigin::PROBE;
    task->targetChannel = channel;
    task->targetGeneration = channel->GetGeneration();
    task->entryStatus.assign(1, Status::OK());

    EXPECT_TRUE(executor.Cancel(task));
    EXPECT_FALSE(channel->IsProbeInFlight());
    EXPECT_EQ(task->finalStatus.code, StatusCode::CANCELED);
    EXPECT_TRUE(executor.Shutdown().ok());
    EXPECT_TRUE(connectionManager->Shutdown().ok());
}

TEST(ProbeSchedulerTest, LinkFailureMarksChannelDrainingWithoutBusinessRetry)
{
    auto provider = std::make_shared<ProbeStubProvider>();
    provider->cqeStatus = 0x716;
    auto config = MakeProbeConfig();
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(config, provider).ok());
    transport.connManager_->StopRecoverLoop();
    ProbeScheduler scheduler(transport, *transport.connManager_, config);
    EnableManualProbe(config);

    scheduler.RunOnce();
    ASSERT_TRUE(WaitForTasksToDrain(transport));
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{2});
    for (const auto& channel : transport.connManager_->GetChannelsSnapshot()) {
        EXPECT_EQ(channel->GetState(), ChannelState::DRAINING);
        EXPECT_FALSE(channel->IsProbeInFlight());
    }

    scheduler.RunOnce();
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{2});
    EXPECT_TRUE(transport.Shutdown().ok());
}

TEST(ProbeSchedulerTest, ProbeRetriesOnceAfterTimeout)
{
    auto provider = std::make_shared<ProbeStubProvider>();
    provider->cqeStatuses = {0x716, 0};
    auto config = MakeProbeConfig();
    config.qpNum = 1;
    config.probeRetryCount = 1;
    config.probeMaxSubmissionsPerRound = 2;
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(config, provider).ok());
    ProbeScheduler scheduler(transport, *transport.connManager_, config);
    EnableManualProbe(config);

    scheduler.RunOnce();
    ASSERT_TRUE(WaitForTasksToDrain(transport));

    EXPECT_EQ(provider->sendCalls.load(), std::size_t{2});
    ASSERT_EQ(provider->sentCids.size(), std::size_t{2});
    EXPECT_EQ(std::set<std::uint16_t>(provider->sentCids.begin(), provider->sentCids.end()).size(),
              std::size_t{2});
    for (const auto& channel : transport.connManager_->GetChannelsSnapshot()) {
        EXPECT_EQ(channel->GetState(), ChannelState::ACTIVE);
        EXPECT_FALSE(channel->IsProbeInFlight());
    }
    EXPECT_TRUE(transport.Shutdown().ok());
}

TEST(ProbeSchedulerTest, ProbeRetryBackoffIsHonored)
{
    auto provider = std::make_shared<ProbeStubProvider>();
    provider->cqeStatuses = {0x716, 0};
    auto config = MakeProbeConfig();
    config.qpNum = 1;
    config.probeRetryCount = 1;
    config.probeRetryBackoffMs = 5;
    config.probeMaxSubmissionsPerRound = 2;
    AsuTransportImpl transport;
    ASSERT_TRUE(transport.Init(config, provider).ok());
    ProbeScheduler scheduler(transport, *transport.connManager_, config);
    EnableManualProbe(config);

    const auto startedAt = std::chrono::steady_clock::now();
    scheduler.RunOnce();
    ASSERT_TRUE(WaitForTasksToDrain(transport));
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - startedAt);

    EXPECT_GE(elapsed.count(), 4);
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{2});
    EXPECT_EQ(provider->sentCids.size(), std::size_t{2});
    for (const auto& channel : transport.connManager_->GetChannelsSnapshot()) {
        EXPECT_EQ(channel->GetState(), ChannelState::ACTIVE);
        EXPECT_FALSE(channel->IsProbeInFlight());
    }
    EXPECT_TRUE(transport.Shutdown().ok());
}

TEST(ProbeSchedulerTest, KeepAliveUnpacksCqe)
{
    KvKeepAliveProtocol protocol;
    std::uint32_t cqe[4] = {};
    cqe[3] = 77U | (0x006U << 17);
    KvResponse response;
    ASSERT_TRUE(protocol.UnpackCqe(cqe, 1, response).ok());
    EXPECT_EQ(response.cid, 77);
    EXPECT_EQ(response.status, 0x006);
}

}  // namespace kv::test
