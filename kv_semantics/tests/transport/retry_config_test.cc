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
#include <filesystem>
#include <fstream>
#include <string>
#include <gtest/gtest.h>
#include "parser_common.h"
#include "task/probe_scheduler.h"
#include "utils/trans_config_utils.h"

namespace kv::test {
namespace {

std::string MakeConfigText(bool clientStyle)
{
    const std::string prefix = clientStyle ? "transport." : "";
    std::string text = clientStyle ? "transport.asuIds=10\n" : "nodeId=10\nendpoint=127.0.0.1:0\n";
    text += "kv_ns_id=1\n";
    text += "dtype=0\n";
    text += "dspec=0\n";
    text += "lr=false\n";
    text += "kernel_count=1\n";
    text += "quiet_count=1\n";
    text += prefix + "busyRetryCount=3\n";
    text += prefix + "failover_retry_count=4\n";
    text += prefix + "busy_retry_backoff_ms=5\n";
    text += prefix + "probe_enabled=true\n";
    text += prefix + "probe_period_ms=7\n";
    text += prefix + "probe_retry_count=2\n";
    text += prefix + "probe_retry_backoff_ms=8\n";
    text += prefix + "probe_max_inflight=64\n";
    text += prefix + "probe_max_submissions_per_round=16\n";
    text += prefix + "probe_transport_scan_budget=8\n";
    text += prefix + "probe_channel_scan_budget_per_transport=32\n";
    text += prefix + "probe_timeout_ms=1000\n";
    text += prefix + "flow_control_enabled=true\n";
    text += prefix + "channel_full_policy=flow_wait\n";
    text += prefix + "busy_floor=9\n";
    text += prefix + "stable_success_ms=10\n";
    text += prefix + "max_inflight_per_channel=12\n";
    text += prefix + "connect_retry_per_round=1\n";
    text += prefix + "recover_initial_backoff_ms=11\n";
    text += prefix + "recover_max_backoff_ms=22\n";
    text += prefix + "selection_policy=least_loaded\n";
    return text;
}

void WriteConfig(const std::filesystem::path& path, const std::string& text)
{
    std::ofstream output(path);
    ASSERT_TRUE(output.is_open());
    output << text;
    output.close();
}

void ExpectRetryFields(const TransportConfig& config)
{
    EXPECT_EQ(config.busyRetryCount, std::uint32_t{3});
    EXPECT_EQ(config.failoverRetryCount, std::uint32_t{4});
    EXPECT_EQ(config.busyRetryBackoffMs, std::uint64_t{5});
    EXPECT_TRUE(config.probeEnabled);
    EXPECT_EQ(config.probePeriodMs, std::uint64_t{7});
    EXPECT_EQ(config.probeRetryCount, std::uint32_t{2});
    EXPECT_EQ(config.probeRetryBackoffMs, std::uint64_t{8});
    EXPECT_EQ(config.probeMaxInflight, std::uint32_t{64});
    EXPECT_EQ(config.probeMaxSubmissionsPerRound, std::uint32_t{16});
    EXPECT_EQ(config.probeTransportScanBudget, std::uint32_t{8});
    EXPECT_EQ(config.probeChannelScanBudgetPerTransport, std::uint32_t{32});
    EXPECT_EQ(config.probeTimeoutMs, std::uint64_t{1000});
    EXPECT_TRUE(config.flowControlEnabled);
    EXPECT_EQ(config.channelFullPolicy, "flow_wait");
    EXPECT_EQ(config.busyFloor, std::uint32_t{9});
    EXPECT_EQ(config.stableSuccessMs, std::uint64_t{10});
    EXPECT_EQ(config.maxInflightPerChannel, std::uint32_t{12});
    EXPECT_EQ(config.connectRetryPerRound, std::uint32_t{1});
    EXPECT_EQ(config.recoverInitialBackoffMs, std::uint64_t{11});
    EXPECT_EQ(config.recoverMaxBackoffMs, std::uint64_t{22});
    EXPECT_EQ(config.selectionPolicy, "least_loaded");
}

}  // namespace

TEST(RetryConfigTest, AppliesRetryFieldsAndAllowsZeroRetries)
{
    TransportConfig config;
    ASSERT_TRUE(ApplyTransportRetryConfigField(config, "busyRetryCount", "3").ok());
    ASSERT_TRUE(ApplyTransportRetryConfigField(config, "failover_retry_count", "4").ok());
    ASSERT_TRUE(ApplyTransportRetryConfigField(config, "probe_enabled", "true").ok());
    ASSERT_TRUE(ApplyTransportRetryConfigField(config, "flow_control_enabled", "1").ok());
    ASSERT_TRUE(ApplyTransportRetryConfigField(config, "selection_policy", "least_loaded").ok());
    ASSERT_TRUE(ApplyTransportRetryConfigField(config, "connect_retry_per_round", "2").ok());
    ASSERT_TRUE(ApplyTransportRetryConfigField(config, "recover_initial_backoff_ms", "7").ok());
    ASSERT_TRUE(ApplyTransportRetryConfigField(config, "recover_max_backoff_ms", "19").ok());
    ASSERT_TRUE(ApplyTransportRetryConfigField(config, "max_inflight_per_channel", "12").ok());

    ASSERT_TRUE(ApplyTransportRetryConfigField(config, "busyRetryCount", "0").ok());
    ASSERT_TRUE(ApplyTransportRetryConfigField(config, "failoverRetryCount", "0").ok());
    ASSERT_TRUE(ApplyTransportRetryConfigField(config, "connectRetryPerRound", "0").ok());
    EXPECT_EQ(config.busyRetryCount, std::uint32_t{0});
    EXPECT_EQ(config.failoverRetryCount, std::uint32_t{0});
    EXPECT_EQ(config.connectRetryPerRound, std::uint32_t{0});
}

TEST(RetryConfigTest, RejectsInvalidRetryValues)
{
    TransportConfig config;
    EXPECT_EQ(ApplyTransportRetryConfigField(config, "busyRetryCount", "-1").code,
              StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(ApplyTransportRetryConfigField(config, "probeEnabled", "maybe").code,
              StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(ApplyTransportRetryConfigField(config, "selectionPolicy", "random").code,
              StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(ApplyTransportRetryConfigField(config, "busyRetryCount", "2x").code,
              StatusCode::INVALID_ARGUMENT);

    TransportConfig invalidProbe = config;
    invalidProbe.probeEnabled = true;
    invalidProbe.probeTimeoutMs = 0;
    EXPECT_EQ(ValidateProbeConfig(invalidProbe).code, StatusCode::INVALID_ARGUMENT);
}

TEST(RetryConfigTest, StandaloneAndClientPathsShareRetryConfig)
{
    const auto standalonePath = std::filesystem::temp_directory_path() / "kv_retry_standalone.conf";
    const auto clientPath = std::filesystem::temp_directory_path() / "kv_retry_client.conf";
    WriteConfig(standalonePath, MakeConfigText(false));
    WriteConfig(clientPath, MakeConfigText(true));

    TransportConfig standalone;
    ASSERT_TRUE(LoadTransportConfig(standalonePath.string(), standalone).ok());
    KvClientConfig client;
    ASSERT_TRUE(KvClientConfig::LoadFromFile(clientPath.string(), client).ok());
    ASSERT_EQ(client.transportConfigs.size(), std::size_t{1});

    ExpectRetryFields(standalone);
    ExpectRetryFields(client.transportConfigs.front());
    std::filesystem::remove(standalonePath);
    std::filesystem::remove(clientPath);
}

TEST(RetryConfigTest, ValidatesChannelFullPolicy)
{
    TransportConfig config;
    ASSERT_TRUE(ApplyTransportRetryConfigField(config, "channel_full_policy", "flow_wait").ok());
    EXPECT_EQ(config.channelFullPolicy, "flow_wait");
    ASSERT_TRUE(NormalizeChannelFullPolicy(config.channelFullPolicy).ok());
    config.channelFullPolicy = "invalid-policy";
    EXPECT_EQ(NormalizeChannelFullPolicy(config.channelFullPolicy).code, StatusCode::INVALID_ARGUMENT);
}

}  // namespace kv::test
