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
#include <thread>
#include <gtest/gtest.h>
#include "conn/group_flow_controller.h"

namespace kv::test {

TEST(GroupFlowControllerTest, BusyDecreasesByTwoAndSuccessIncreasesByOne)
{
    GroupFlowController flow;
    flow.Configure(8, 2, true, 60000);
    flow.OnBusy();
    EXPECT_EQ(flow.GetSnapshot().currentLimit, std::uint64_t{6});
    flow.OnBusy();
    EXPECT_EQ(flow.GetSnapshot().currentLimit, std::uint64_t{4});
    flow.OnBusy();
    EXPECT_EQ(flow.GetSnapshot().currentLimit, std::uint64_t{2});
    flow.OnBusy();
    EXPECT_EQ(flow.GetSnapshot().currentLimit, std::uint64_t{2});
    EXPECT_FALSE(flow.CanAcquire(2));
    EXPECT_TRUE(flow.CanAcquire(1));

    flow.OnBusinessSuccess(std::chrono::steady_clock::now());
    EXPECT_EQ(flow.GetSnapshot().currentLimit, std::uint64_t{3});
    flow.OnBusinessSuccess(std::chrono::steady_clock::now());
    EXPECT_EQ(flow.GetSnapshot().currentLimit, std::uint64_t{4});
    EXPECT_TRUE(flow.GetSnapshot().congested);
}

TEST(GroupFlowControllerTest, IdleDoesNotExitCongestion)
{
    GroupFlowController flow;
    flow.Configure(4, 1, true, 5);
    flow.OnBusy();
    EXPECT_EQ(flow.GetSnapshot().currentLimit, std::uint64_t{2});
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_TRUE(flow.GetSnapshot().congested);

    flow.OnBusinessSuccess(std::chrono::steady_clock::now());
    EXPECT_EQ(flow.GetSnapshot().currentLimit, std::uint64_t{3});
    EXPECT_TRUE(flow.GetSnapshot().congested);
    flow.OnBusinessSuccess(std::chrono::steady_clock::now());
    EXPECT_EQ(flow.GetSnapshot().currentLimit, std::uint64_t{4});
    EXPECT_FALSE(flow.GetSnapshot().congested);
}

TEST(GroupFlowControllerTest, NonSuccessResetsStableSuccessTimer)
{
    GroupFlowController flow;
    flow.Configure(4, 1, true, 5);
    flow.OnBusy();
    const auto firstSuccess = std::chrono::steady_clock::now();
    flow.OnBusinessSuccess(firstSuccess);
    flow.OnNonSuccess();
    flow.OnBusinessSuccess(firstSuccess + std::chrono::milliseconds(20));
    EXPECT_EQ(flow.GetSnapshot().currentLimit, std::uint64_t{4});
    EXPECT_FALSE(flow.GetSnapshot().congested);
}

TEST(GroupFlowControllerTest, ReleaseNotificationWakesBoundedWait)
{
    GroupFlowController flow;
    flow.Configure(1, 1, true, 1000);
    std::atomic_bool started{false};
    std::thread notifier([&] {
        started.store(true, std::memory_order_release);
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        flow.NotifyRelease();
    });
    while (!started.load(std::memory_order_acquire)) { std::this_thread::yield(); }
    const auto begin = std::chrono::steady_clock::now();
    flow.WaitForRelease(std::chrono::milliseconds(200));
    notifier.join();
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    EXPECT_LT(elapsed, std::chrono::milliseconds(100));
}

}  // namespace kv::test
