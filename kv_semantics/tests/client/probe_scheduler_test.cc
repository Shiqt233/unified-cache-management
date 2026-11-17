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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#define private public
#include "probe_scheduler.h"
#include "kv_transport_impl.h"
#include "kv_client_impl.h"
#undef private
#include "../common/probe_stub_provider.h"
#include "conn/connection_internal.h"

namespace kv::test {
namespace {

class ProbeNoopTransport final : public AsuTransport {
public:
    Status Init(const TransportConfig&, std::shared_ptr<TransProvider>) override
    {
        return Status::OK();
    }
    Status Init(const std::string&, std::shared_ptr<TransProvider>) override
    {
        return Status::Error(StatusCode::UNSUPPORTED, "noop transport config path");
    }
    Status Shutdown() override { return Status::OK(); }
    Status CheckHealth() override { return Status::OK(); }
    Status Submit(const TransportTaskPtr&) override
    {
        return Status::Error(StatusCode::UNSUPPORTED, "noop transport does not execute tasks");
    }
    Status Cancel(TaskId) override { return Status::OK(); }
};

TransportConfig MakeSharedProbeConfig()
{
    auto config = MakeProbeConfig();
    config.probeEnabled = true;
    config.probePeriodMs = 1;
    config.probeTimeoutMs = 50;
    config.probeMaxInflight = 2;
    config.probeMaxSubmissionsPerRound = 2;
    config.probeTransportScanBudget = 1;
    config.probeChannelScanBudgetPerTransport = 2;
    return config;
}

TransportConfig MakeClientTransportConfig(NodeId nodeId)
{
    auto config = MakeProbeConfig();
    config.nodeId = nodeId;
    config.providerType = TransProviderType::FAKE;
    config.probeEnabled = true;
    config.probePeriodMs = 1000;
    return config;
}

std::shared_ptr<AsuTransportImpl> MakeTransport(const TransportConfig& config,
                                                const std::shared_ptr<ProbeStubProvider>& provider)
{
    auto transport = std::make_shared<AsuTransportImpl>();
    auto initConfig = config;
    // The transport never starts a probe thread itself; this also keeps the test
    // compatible with manual scheduler configuration using a zero idle period.
    initConfig.probeEnabled = false;
    EXPECT_TRUE(transport->Init(initConfig, provider).ok());
    return transport;
}

bool WaitForTasksToDrain(AsuTransportImpl& transport)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (transport.taskManager_.GetAll().empty()) { return true; }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return transport.taskManager_.GetAll().empty();
}

bool WaitForSendCount(const std::shared_ptr<ProbeStubProvider>& provider, std::size_t expected)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (provider->sendCalls.load(std::memory_order_acquire) >= expected) { return true; }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return provider->sendCalls.load(std::memory_order_acquire) >= expected;
}

void StopSchedulerAndTransport(ClientProbeScheduler& scheduler,
                               const std::shared_ptr<AsuTransportImpl>& transport)
{
    scheduler.UnregisterTransport(transport.get());
    scheduler.Stop();
    EXPECT_TRUE(transport->Shutdown().ok());
}

}  // namespace

TEST(ClientProbeSchedulerTest, SharedSchedulerCreatesOneThreadForManyTransports)
{
    auto config = MakeSharedProbeConfig();
    config.probePeriodMs = 1000;
    ClientProbeScheduler scheduler(config);
    // Exercise registry scale without creating 1000 real transports/threads.
    scheduler.transports_.resize(1000);

    scheduler.Start();
    EXPECT_EQ(scheduler.GetWorkerThreadCount(), std::size_t{1});
    EXPECT_EQ(scheduler.GetRegisteredTransportCount(), std::size_t{1000});
    EXPECT_TRUE(scheduler.GetStats().workerRunning);

    scheduler.Stop();
    EXPECT_EQ(scheduler.GetWorkerThreadCount(), std::size_t{0});
    EXPECT_FALSE(scheduler.GetStats().workerRunning);
}

TEST(ClientProbeSchedulerTest, WindowedBudgetAndInflightAreReleasedOnTimeout)
{
    auto provider = std::make_shared<ProbeStubProvider>();
    provider->writeResponse.store(false, std::memory_order_release);
    auto config = MakeSharedProbeConfig();
    auto transport = MakeTransport(config, provider);
    ClientProbeScheduler scheduler(config);
    std::shared_ptr<AsuTransport> transportBase = transport;
    scheduler.RegisterTransport(transportBase);

    scheduler.RunOnce();
    ASSERT_TRUE(WaitForSendCount(provider, 2));
    auto stats = scheduler.GetStats();
    EXPECT_EQ(stats.lastRoundTransports, std::uint32_t{1});
    EXPECT_EQ(stats.lastRoundChannels, std::uint32_t{2});
    EXPECT_EQ(stats.lastRoundSubmissions, std::uint32_t{2});
    EXPECT_EQ(stats.activeProbeCount, std::uint32_t{2});
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{2});

    // The global in-flight cap prevents a third task and another full scan.
    scheduler.RunOnce();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    stats = scheduler.GetStats();
    EXPECT_EQ(stats.lastRoundSubmissions, std::uint32_t{0});
    EXPECT_EQ(stats.lastRoundChannels, std::uint32_t{0});
    EXPECT_EQ(stats.activeProbeCount, std::uint32_t{2});
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{2});

    ASSERT_TRUE(WaitForTasksToDrain(*transport));
    stats = scheduler.GetStats();
    EXPECT_EQ(stats.activeProbeCount, std::uint32_t{0});
    for (const auto& channel : transport->connManager_->GetChannelsSnapshot()) {
        EXPECT_FALSE(channel->IsProbeInFlight());
    }
    StopSchedulerAndTransport(scheduler, transport);
}

TEST(ClientProbeSchedulerTest, CursorUsesBoundedWindowsAndAdvancesFairly)
{
    auto providerA = std::make_shared<ProbeStubProvider>();
    auto providerB = std::make_shared<ProbeStubProvider>();
    auto config = MakeSharedProbeConfig();
    config.probePeriodMs = 0;
    config.probeMaxInflight = 64;
    config.probeMaxSubmissionsPerRound = 2;
    config.probeTransportScanBudget = 1;
    config.probeChannelScanBudgetPerTransport = 2;
    config.qpNum = 4;

    auto transportA = MakeTransport(config, providerA);
    auto transportB = MakeTransport(config, providerB);
    ClientProbeScheduler scheduler(config);
    std::shared_ptr<AsuTransport> baseA = transportA;
    std::shared_ptr<AsuTransport> baseB = transportB;
    scheduler.RegisterTransport(baseA);
    scheduler.RegisterTransport(baseB);

    for (std::size_t round = 0; round < 4; ++round) {
        scheduler.RunOnce();
        const auto stats = scheduler.GetStats();
        EXPECT_EQ(stats.lastRoundTransports, std::uint32_t{1});
        EXPECT_EQ(stats.lastRoundChannels, std::uint32_t{2});
        EXPECT_EQ(stats.lastRoundSubmissions, std::uint32_t{2});
        ASSERT_TRUE(WaitForTasksToDrain(*transportA));
        ASSERT_TRUE(WaitForTasksToDrain(*transportB));
    }

    EXPECT_EQ(providerA->sendCalls.load(), std::size_t{4});
    EXPECT_EQ(providerB->sendCalls.load(), std::size_t{4});
    EXPECT_EQ(providerA->sentHandles.size(), std::size_t{4});
    EXPECT_EQ(providerB->sentHandles.size(), std::size_t{4});
    EXPECT_EQ(std::set<ConnectionHandle>(providerA->sentHandles.begin(), providerA->sentHandles.end()).size(),
              std::size_t{4});
    EXPECT_EQ(std::set<ConnectionHandle>(providerB->sentHandles.begin(), providerB->sentHandles.end()).size(),
              std::size_t{4});
    EXPECT_EQ(scheduler.GetStats().activeProbeCount, std::uint32_t{0});

    StopSchedulerAndTransport(scheduler, transportA);
    StopSchedulerAndTransport(scheduler, transportB);
}

TEST(ClientProbeSchedulerTest, StandaloneTransportDoesNotAutoProbe)
{
    auto provider = std::make_shared<ProbeStubProvider>();
    auto config = MakeSharedProbeConfig();
    auto transport = std::make_shared<AsuTransportImpl>();
    ASSERT_TRUE(transport->Init(config, provider).ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(provider->sendCalls.load(), std::size_t{0});
    EXPECT_TRUE(transport->Shutdown().ok());
}

TEST(ClientProbeSchedulerTest, InvalidProbeBudgetIsRejected)
{
    auto provider = std::make_shared<ProbeStubProvider>();
    auto config = MakeSharedProbeConfig();
    config.probeMaxInflight = 0;
    EXPECT_EQ(ValidateProbeConfig(config).code, StatusCode::INVALID_ARGUMENT);

    ClientProbeScheduler scheduler(config);
    scheduler.Start();
    EXPECT_EQ(scheduler.GetWorkerThreadCount(), std::size_t{0});

    auto transport = std::make_shared<AsuTransportImpl>();
    EXPECT_EQ(transport->Init(config, provider).code, StatusCode::INVALID_ARGUMENT);
    EXPECT_TRUE(transport->Shutdown().ok());
}

TEST(ClientProbeSchedulerTest, ProbeLeaseReleaseIsIdempotent)
{
    auto provider = std::make_shared<ProbeStubProvider>();
    ConnectionManager connectionManager(*provider, "", 5000, 1);
    ASSERT_TRUE(connectionManager.AddGroup(NodeEndpoint{}, 1).ok());
    auto channels = connectionManager.GetChannelsSnapshot();
    ASSERT_EQ(channels.size(), std::size_t{1});
    auto channel = channels.front();
    auto budget = std::make_shared<ProbeBudget>(1);
    ASSERT_TRUE(budget->TryAcquire());
    ASSERT_TRUE(connectionManager.AcquireProbe(channel, channel->GetGeneration()));
    auto lease = std::make_shared<ProbeTaskLease>(budget, channel);

    lease->Release();
    lease->Release();
    EXPECT_TRUE(lease->IsReleased());
    EXPECT_EQ(budget->GetActive(), std::uint32_t{0});
    EXPECT_FALSE(channel->IsProbeInFlight());
    EXPECT_TRUE(connectionManager.Shutdown().ok());
}

TEST(ClientProbeSchedulerTest, ClientLifecycleRegistersRealTransportsAndProbes)
{
    auto providerA = std::make_shared<ProbeStubProvider>();
    auto providerB = std::make_shared<ProbeStubProvider>();
    KvClientConfig config;
    config.transportConfigs = {MakeClientTransportConfig(10), MakeClientTransportConfig(20)};

    auto client = std::make_unique<KvClientImpl>(
        [] { return std::unique_ptr<AsuTransport>(new AsuTransportImpl()); },
        nullptr,
        [providerA, providerB](const TransportConfig& transportConfig,
                               std::shared_ptr<TransProvider>& provider) {
            provider = transportConfig.nodeId == 10 ? providerA : providerB;
            return Status::OK();
        });
    ASSERT_TRUE(client->Init(config).ok());
    ASSERT_NE(client->probeScheduler_, nullptr);
    EXPECT_EQ(client->probeScheduler_->GetWorkerThreadCount(), std::size_t{1});
    EXPECT_EQ(client->probeScheduler_->GetRegisteredTransportCount(), std::size_t{2});

    ASSERT_TRUE(WaitForSendCount(providerA, 2));
    ASSERT_TRUE(WaitForSendCount(providerB, 2));
    EXPECT_TRUE(client->Shutdown().ok());
    EXPECT_EQ(client->probeScheduler_, nullptr);
}

TEST(ClientProbeSchedulerTest, ClientLifecycleCreatesOnlyOneSharedProbeThread)
{
    KvClientConfig config;
    for (NodeId nodeId : {NodeId{10}, NodeId{20}}) {
        TransportConfig transportConfig;
        transportConfig.nodeId = nodeId;
        transportConfig.providerType = TransProviderType::FAKE;
        transportConfig.probeEnabled = true;
        transportConfig.probePeriodMs = 1000;
        config.transportConfigs.emplace_back(std::move(transportConfig));
    }

    auto client = std::make_unique<KvClientImpl>(
        [] { return std::unique_ptr<AsuTransport>(new ProbeNoopTransport()); });
    ASSERT_TRUE(client->Init(config).ok());
    ASSERT_NE(client->probeScheduler_, nullptr);
    EXPECT_EQ(client->probeScheduler_->GetWorkerThreadCount(), std::size_t{1});
    // NoopTransport is not AsuTransportImpl and must not be registered as a probe target.
    EXPECT_EQ(client->probeScheduler_->GetRegisteredTransportCount(), std::size_t{0});
    EXPECT_TRUE(client->Shutdown().ok());
    EXPECT_EQ(client->probeScheduler_, nullptr);
}

TEST(ClientProbeSchedulerTest, ProbeRetriesConsumeSharedSubmissionBudget)
{
    auto provider = std::make_shared<ProbeStubProvider>();
    provider->cqeStatuses = {0x716, 0x716, 0, 0};
    auto config = MakeSharedProbeConfig();
    config.probeRetryCount = 1;
    config.probeMaxSubmissionsPerRound = 4;
    config.probeMaxInflight = 4;
    auto transport = MakeTransport(config, provider);
    ClientProbeScheduler scheduler(config);
    std::shared_ptr<AsuTransport> transportBase = transport;
    scheduler.RegisterTransport(transportBase);

    scheduler.RunOnce();
    ASSERT_TRUE(WaitForTasksToDrain(*transport));

    EXPECT_EQ(provider->sendCalls.load(), std::size_t{4});
    const auto stats = scheduler.GetStats();
    EXPECT_EQ(stats.lastRoundSubmissions, std::uint32_t{4});
    EXPECT_EQ(stats.activeProbeCount, std::uint32_t{0});
    StopSchedulerAndTransport(scheduler, transport);
}

TEST(ClientProbeSchedulerTest, ProbeRetriesStopAtSharedSubmissionBudget)
{
    auto provider = std::make_shared<ProbeStubProvider>();
    provider->cqeStatuses = {0x716, 0x716, 0, 0};
    auto config = MakeSharedProbeConfig();
    config.probeRetryCount = 1;
    config.probeMaxSubmissionsPerRound = 3;
    config.probeMaxInflight = 4;
    auto transport = MakeTransport(config, provider);
    ClientProbeScheduler scheduler(config);
    std::shared_ptr<AsuTransport> transportBase = transport;
    scheduler.RegisterTransport(transportBase);

    scheduler.RunOnce();
    ASSERT_TRUE(WaitForTasksToDrain(*transport));

    EXPECT_EQ(provider->sendCalls.load(), std::size_t{3});
    const auto stats = scheduler.GetStats();
    EXPECT_EQ(stats.lastRoundSubmissions, std::uint32_t{3});
    EXPECT_EQ(stats.activeProbeCount, std::uint32_t{0});
    StopSchedulerAndTransport(scheduler, transport);
}

}  // namespace kv::test
