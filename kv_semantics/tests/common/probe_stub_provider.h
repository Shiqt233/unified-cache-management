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

#include <atomic>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>
#include "kv_types.h"
#include "trans_provider.h"

namespace kv::test {

class ProbeStubProvider : public TransProvider {
public:
    std::atomic<std::size_t> sendCalls{0};
    std::vector<ConnectionHandle> sentHandles;
    std::vector<std::uint16_t> sentCids;
    std::vector<std::uint16_t> cqeStatuses;
    std::uint16_t cqeStatus{0};
    std::atomic<bool> writeResponse{true};

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
        std::vector<Status> result;
        result.reserve(ioBatches.size());
        const auto responseStatus = cqeStatuses.empty()
                                        ? cqeStatus
                                        : cqeStatuses[std::min(call, cqeStatuses.size() - 1)];
        for (const auto& batch : ioBatches) {
            sentHandles.push_back(batch.connectionHandle);
            const auto cid = (static_cast<const std::uint32_t*>(batch.sendBuffer)[0] >> 16) & 0xFFFFU;
            sentCids.push_back(static_cast<std::uint16_t>(cid));
            if (writeResponse.load(std::memory_order_acquire)) {
                auto* cqe = static_cast<std::uint32_t*>(batch.flagBuffer);
                cqe[3] = cid | (static_cast<std::uint32_t>(responseStatus) << 17);
                cqe[4] = 0;
            }
            result.push_back(Status::OK());
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

inline TransportConfig MakeProbeConfig()
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
    config.probeEnabled = false;
    config.probePeriodMs = 0;
    return config;
}

}  // namespace kv::test
