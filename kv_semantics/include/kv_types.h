/**
 * MIT License
 *
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 */
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace kv {

using TaskId = std::uint64_t;
using MRHandle = std::uint64_t;
using NodeId = std::uint64_t;
constexpr std::size_t kCacheKeySizeBytes = 8;
using CacheKey = std::array<std::byte, kCacheKeySizeBytes>;

// KV protocol requires 512B alignment for device buffers and offsets.
constexpr std::uint32_t kAlignmentBytes = 512;

constexpr TaskId kInvalidTaskId = 0;
constexpr MRHandle kInvalidMRHandle = 0;

enum class StatusCode {
    // For a server IO operation, OK confirms that the current write was accepted.
    // For STORE it does not, by itself, prove that the key's complete range is full.
    OK = 0,
    INVALID_ARGUMENT,
    NOT_INITIALIZED,
    // Waiting or task deadline expired. This is not equivalent to an unknown server outcome.
    TIMEOUT,
    NOT_FOUND,
    // Aggregate result, not a protocol error and not proof of partial byte writes.
    PARTIAL_FAILED,
    CONNECTION_ERROR,
    IO_ERROR,
    BUFFER_NOT_REGISTERED,
    BUFFER_NOT_SUPPORTED,
    TASK_NOT_FOUND,
    RESOURCE_BUSY,
    UNSUPPORTED,
    IN_PROGRESS,
    INTERNAL_ERROR,
    CANCELED,

    // Entry status codes keep raw entry result values in the low byte.
    // These ENTRY_* values belong to protocol result buffers, not to the generic
    // transport error categories. ENTRY_KEY_EXIST is a normal Query hit, not a
    // STORE "range full" response.
    ENTRY_RETRY_ADVISED = 0x0100 | 0x01,
    ENTRY_NO_RETRY_ADVISED = 0x0100 | 0x02,
    ENTRY_KEY_NOT_FOUND = 0x0100 | 0x03,
    ENTRY_DATA_NOT_EXIST = 0x0100 | 0x04,
    ENTRY_DELETE_FAILED = 0x0200 | 0x01,
    ENTRY_KEY_NOT_EXIST = 0x0300 | 0x00,
    ENTRY_KEY_EXIST = 0x0300 | 0x01,

    CQE_INVALID_COMMAND_OPCODE = 0x10000 | 0x001,
    CQE_INVALID_FIELD_IN_COMMAND = 0x10000 | 0x002,
    CQE_INTERNAL_ERROR = 0x10000 | 0x006,
    CQE_WRITE_FAULT = 0x10000 | 0x280,
    CQE_UNRECOVERED_READ_ERROR = 0x10000 | 0x281,
    CQE_KEY_NOT_EXIST = 0x10000 | 0x701,
    CQE_OUT_OF_CREATE_SIZE = 0x10000 | 0x712,
    // Raw protocol timeout. It follows the normal link-failure failover policy;
    // retry exhaustion returns the original protocol/aggregate error.
    CQE_IO_TIMEOUT = 0x10000 | 0x716,
    // STORE application response: the key's range is full (or a create-only key already
    // exists), so the current write is rejected. This is not a link failure or an
    // outcome-unknown result and must not be silently converted to OK.
    CQE_KEY_ALREADY_EXISTED = 0x10000 | 0x723,
    // Remote busy is distinct from local RESOURCE_BUSY and has its own retry policy.
    CQE_RESOURCE_BUSY = 0x10000 | 0x731,
    // Batch wrapper: decode per-entry results before selecting a retry strategy.
    CQE_CHECK_RESULT_BUFFER = 0x10000 | 0x732,
};

struct Status {
    StatusCode code{StatusCode::OK};
    std::string message;

    bool ok() const noexcept { return code == StatusCode::OK; }

    static Status OK() { return {}; }
    static Status Error(StatusCode c, std::string msg) { return Status{c, std::move(msg)}; }
};

struct QueryResult {
    std::vector<std::uint8_t> exists;
    std::uint32_t prefixHitKeys{0};
};

enum class MemoryType {
    HOST = 0,
    HOST_PINNED = 1,
    DEVICE = 2,
};

struct MemoryRegion {
    MemoryType memoryType{MemoryType::HOST};
    std::uint64_t addr{0};
    std::uint64_t size{0};
    std::int32_t deviceId{-1};
    std::int32_t numaNode{-1};
};

struct Buffer {
    MemoryRegion region;
    MRHandle handle{kInvalidMRHandle};
};

struct KVBuffer {
    CacheKey key;
    Buffer buffer;
    std::uint32_t offset{0};
    // Resolved by KvClient from buffer.handle before the entry reaches the transport.
    std::optional<std::uint32_t> mrKey;
};

// Describes a registered memory region returned by registration or reused for binding.
struct RegisteredMemory {
    MemoryRegion region;
    MRHandle handle{kInvalidMRHandle};
    std::uint32_t tokenId{0};
};

// status is the aggregate task result; entryStatus preserves per-entry application
// and protocol results. PARTIAL_FAILED is only an aggregation label: it does not
// prove that a value was partially written, nor does it replace individual statuses.
struct TaskResult {
    Status status;
    std::vector<Status> entryStatus;
    std::optional<QueryResult> queryResult;
};

enum class Protocol {
    UB = 0,
    ROCE = 1,
    TCP = 2,
};

enum class TransProviderType { AICPU, FAKE, AIV, UNSUPPORTED };

struct NodeEndpoint {
    std::string ip;
    std::uint16_t port{0};
    Protocol protocol{Protocol::ROCE};
    std::int32_t numaNode{-1};
    std::string hcaName;
    std::uint8_t hcaPort{1};
    std::unordered_map<std::string, std::string> attrs;
};

struct NodeInfo {
    std::vector<NodeEndpoint> endpoints;
};

struct TransportConfig {
    // TODO: 拆分Config，按逻辑模块细化
    std::string nodeName;
    NodeId nodeId{0};
    // Local logical device ID used by the transport provider.
    std::int32_t deviceId{-1};
    std::vector<NodeEndpoint> endpoints;

    TransProviderType providerType{TransProviderType::AICPU};

    std::uint32_t qpNum{1};

    std::uint32_t maxInflightTasks{1024};
    std::uint64_t maxInflightBytes{1ULL << 30};

    std::uint64_t timeoutMs{100};
    std::uint32_t maxErrorCount{2};
    // Extra attempts after the first subBatch attempt. Zero disables that retry type.
    std::uint32_t busyRetryCount{2};
    std::uint32_t failoverRetryCount{2};
    std::uint64_t busyRetryBackoffMs{1};
    // KeepAlive probing is opt-in so existing business behavior is unchanged.
    bool probeEnabled{false};
    std::uint64_t probePeriodMs{1000};
    std::uint32_t probeRetryCount{0};
    std::uint64_t probeRetryBackoffMs{5};
    std::uint32_t probeMaxInflight{64};
    std::uint32_t probeMaxSubmissionsPerRound{16};
    std::uint32_t probeTransportScanBudget{8};
    std::uint32_t probeChannelScanBudgetPerTransport{32};
    std::uint64_t probeTimeoutMs{1000};
    bool flowControlEnabled{false};
    // fail_fast preserves existing behavior; flow_wait waits on Group release notifications.
    std::string channelFullPolicy{"fail_fast"};
    std::uint32_t busyFloor{32};
    std::uint64_t stableSuccessMs{60000};
    std::uint32_t maxInflightPerChannel{256};
    std::uint32_t connectRetryPerRound{0};
    std::uint64_t recoverInitialBackoffMs{100};
    std::uint64_t recoverMaxBackoffMs{5000};
    std::string selectionPolicy{"round_robin"};
    std::size_t completionPollSpinLimit{16};

    bool enableDeviceDirect{true};
    bool enableHostFallback{false};
    bool preconnect{true};
    bool bindCqPoller{true};

    // Slot sizes are caller-visible capacities; BufferManager computes the
    // aligned physical stride used for allocation and memory registration.
    std::size_t sendBufferSlotSize{4160};
    std::size_t sendBufferSlotNum{128};
    // Maximum memory required by a batch store/retrieve response flag buffer.
    std::size_t flagBufferSlotSize{71};
    std::size_t flagBufferSlotNum{4096};
    std::size_t batchLoadIoNum{110};
    std::size_t batchStoreIoNum{110};
    std::size_t deleteIoNum{254};
    std::size_t queryIoNum{256};

    // Transport attrs loaded from config, including SQE request attrs
    // (kv_ns_id, dtype, dspec, lr, sc) and send attrs (kernel_count, quiet_count).
    std::unordered_map<std::string, std::string> attrs;
};

enum class SharedProviderMode : std::uint8_t { INDEPENDENT = 0, SHARED = 1 };

struct KvClientConfig {
    // Loads client configuration from a key-value file without initializing a client.
    static Status LoadFromFile(const std::string& configPath, KvClientConfig& config);

    std::string clientId;
    std::vector<std::string> viewServiceAddrs;

    std::vector<TransportConfig> transportConfigs;

    std::uint32_t maxInflightTasks{1024};
    std::uint64_t defaultWaitTimeoutMs{100};
    std::uint64_t timeoutMs{100};
    // Keep single-key requests on the currently supported batch IO path by default.
    bool useBatchIoForSingleKey{true};
    SharedProviderMode sharedProviderMode{SharedProviderMode::INDEPENDENT};
    std::unordered_map<std::string, std::string> attrs;
};

}  // namespace kv
