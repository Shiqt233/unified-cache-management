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
#include "parser_common.h"
#include <algorithm>
#include <cctype>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace kv {
namespace {

std::uint64_t ParseUnsignedConfigValue(const std::string& value, std::size_t* consumed)
{
    return std::stoull(value, consumed, 0);
}

void ApplyTransportEndpointField(NodeEndpoint& endpoint, const std::string& key,
                                 const std::string& value)
{
    if (key == "ip" || key == "local.comm_id" || key == "localCommId") {
        endpoint.ip = value;
    } else if (key == "port") {
        endpoint.port = static_cast<std::uint16_t>(ParseConfigUint64(value));
    } else if (key == "protocol") {
        endpoint.protocol = ParseConfigProtocol(value);
    } else if (key == "numa_node" || key == "numaNode") {
        endpoint.numaNode = static_cast<std::int32_t>(ParseConfigUint64(value));
    } else if (key == "hca_name" || key == "hcaName") {
        endpoint.hcaName = value;
    } else if (key == "hca_port" || key == "hcaPort") {
        endpoint.hcaPort = static_cast<std::uint8_t>(ParseConfigUint64(value));
    } else {
        endpoint.attrs[key] = value;
    }
}

}  // namespace

std::string TrimConfigValue(const std::string& value)
{
    const auto begin = value.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) { return ""; }
    const auto end = value.find_last_not_of(" \t\r\n");
    return value.substr(begin, end - begin + 1);
}

std::vector<std::string> SplitConfigValue(const std::string& value, char delimiter)
{
    std::vector<std::string> parts;
    std::stringstream stream{value};
    std::string part;
    while (std::getline(stream, part, delimiter)) {
        part = TrimConfigValue(part);
        if (!part.empty()) { parts.emplace_back(std::move(part)); }
    }
    return parts;
}

std::uint64_t ParseConfigUint64(const std::string& value)
{
    return ParseUnsignedConfigValue(value, nullptr);
}

std::uint64_t ParseConfigUint64(const std::string& value, std::uint64_t fallback)
{
    try {
        std::size_t consumed = 0;
        const auto parsed = ParseUnsignedConfigValue(value, &consumed);
        // Match the previous strtoull end-pointer check, including embedded NULs.
        return value.c_str()[consumed] == '\0' ? parsed : fallback;
    } catch (const std::invalid_argument&) {
        return fallback;
    } catch (const std::out_of_range&) {
        return fallback;
    }
}

std::uint32_t ParseConfigUint32(const std::string& value, std::uint32_t fallback)
{
    const auto parsed = ParseConfigUint64(value, fallback);
    return parsed > std::numeric_limits<std::uint32_t>::max() ? fallback
                                                              : static_cast<std::uint32_t>(parsed);
}

std::uint16_t ParseConfigUint16(const std::string& value, std::uint16_t fallback)
{
    const auto parsed = ParseConfigUint32(value, fallback);
    return parsed > std::numeric_limits<std::uint16_t>::max() ? fallback
                                                              : static_cast<std::uint16_t>(parsed);
}

bool GetAttr(const std::unordered_map<std::string, std::string>& attrs, const std::string& key,
             std::string& value)
{
    const auto it = attrs.find(key);
    if (it == attrs.end()) { return false; }
    value = it->second;
    return true;
}

std::string GetAttr(const std::unordered_map<std::string, std::string>& attrs,
                    std::initializer_list<const char*> names)
{
    std::string value;
    for (const auto* name : names) {
        if (GetAttr(attrs, name, value)) { return value; }
    }
    return {};
}

std::string GetConfigAttr(const TransportConfig& config, std::initializer_list<const char*> names)
{
    return GetAttr(config.attrs, names);
}

std::string GetEndpointAttr(const NodeEndpoint* endpoint, std::initializer_list<const char*> names)
{
    return endpoint == nullptr ? std::string{} : GetAttr(endpoint->attrs, names);
}

Protocol ParseConfigProtocol(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::toupper(ch)); });
    if (value == "UB" || value == "UBOE" || value == "UBG") { return Protocol::UB; }
    if (value == "ROCE") { return Protocol::ROCE; }
    if (value == "TCP") { return Protocol::TCP; }
    return Protocol::TCP;
}

std::string ToLower(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

TransProviderType ParseConfigTransProviderType(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::toupper(ch)); });
    if (value == "FAKE") { return TransProviderType::FAKE; }
    if (value == "AIV") { return TransProviderType::AIV; }
    if (value == "AICPU") { return TransProviderType::AICPU; }
    return TransProviderType::UNSUPPORTED;
}

bool ApplyTransportBufferConfigField(TransportConfig& config, const std::string& key,
                                     const std::string& value)
{
    if (key == "sendBufferSlotSize" || key == "send_buffer_slot_size" ||
        key == "ioBuffer.sendBufferSlotSize" || key == "io_buffer.send_buffer_slot_size") {
        config.sendBufferSlotSize = static_cast<std::size_t>(ParseConfigUint64(value));
    } else if (key == "sendBufferSlotNum" || key == "send_buffer_slot_num" ||
               key == "ioBuffer.sendBufferSlotNum" || key == "io_buffer.send_buffer_slot_num") {
        config.sendBufferSlotNum = static_cast<std::size_t>(ParseConfigUint64(value));
    } else if (key == "flagBufferSlotSize" || key == "flag_buffer_slot_size" ||
               key == "ioBuffer.flagBufferSlotSize" || key == "io_buffer.flag_buffer_slot_size") {
        config.flagBufferSlotSize = static_cast<std::size_t>(ParseConfigUint64(value));
    } else if (key == "flagBufferSlotNum" || key == "flag_buffer_slot_num" ||
               key == "ioBuffer.flagBufferSlotNum" || key == "io_buffer.flag_buffer_slot_num") {
        config.flagBufferSlotNum = static_cast<std::size_t>(ParseConfigUint64(value));
    } else {
        return false;
    }
    return true;
}

bool ApplyTransportIoNumConfigField(TransportConfig& config, const std::string& key,
                                    const std::string& value)
{
    if (key == "batchLoadIoNum" || key == "batch_load_io_num") {
        config.batchLoadIoNum = static_cast<std::size_t>(ParseConfigUint64(value));
    } else if (key == "batchStoreIoNum" || key == "batch_store_io_num") {
        config.batchStoreIoNum = static_cast<std::size_t>(ParseConfigUint64(value));
    } else if (key == "deleteIoNum" || key == "delete_io_num") {
        config.deleteIoNum = static_cast<std::size_t>(ParseConfigUint64(value));
    } else if (key == "queryIoNum" || key == "query_io_num") {
        config.queryIoNum = static_cast<std::size_t>(ParseConfigUint64(value));
    } else {
        return false;
    }
    return true;
}

bool ApplyTransportCompletionConfigField(TransportConfig& config, const std::string& key,
                                         const std::string& value)
{
    if (key != "completionPollSpinLimit" && key != "completion_poll_spin_limit") { return false; }
    config.completionPollSpinLimit = static_cast<std::size_t>(ParseConfigUint64(value));
    return true;
}

bool ApplyTransportProviderConfigField(TransportConfig& config, const std::string& key,
                                       const std::string& value)
{
    if (key == "providerBackend" || key == "provider_backend" || key == "transProviderBackend" ||
        key == "trans_provider_backend" || key == "providerType" || key == "provider_type" ||
        key == "transProviderType" || key == "trans_provider_type") {
        config.providerType = ParseConfigTransProviderType(value);
    } else {
        return false;
    }
    return true;
}

bool ApplyTransportDeviceConfigField(TransportConfig& config, const std::string& key,
                                     const std::string& value)
{
    if (key == "deviceId" || key == "device_id") {
        config.deviceId = static_cast<std::int32_t>(ParseConfigUint64(value));
        return true;
    }
    return false;
}

namespace {

Status ParseRetryUnsigned(const std::string& key, const std::string& value, std::uint64_t maxValue,
                          std::uint64_t& output)
{
    if (value.empty() || value.front() == '-') {
        return Status::Error(StatusCode::INVALID_ARGUMENT, key + " must be an unsigned integer");
    }
    try {
        std::size_t consumed = 0;
        const auto parsed = ParseUnsignedConfigValue(value, &consumed);
        if (consumed != value.size() || parsed > maxValue) {
            return Status::Error(StatusCode::INVALID_ARGUMENT,
                                 key + " has an out-of-range unsigned integer");
        }
        output = parsed;
        return Status::OK();
    } catch (...) {
        return Status::Error(StatusCode::INVALID_ARGUMENT, key + " has an invalid unsigned integer");
    }
}

Status ParseRetryBool(const std::string& key, const std::string& value, bool& output)
{
    const auto normalized = ToLower(value);
    if (normalized == "true" || normalized == "1") {
        output = true;
        return Status::OK();
    }
    if (normalized == "false" || normalized == "0") {
        output = false;
        return Status::OK();
    }
    return Status::Error(StatusCode::INVALID_ARGUMENT, key + " must be true/false or 1/0");
}

Status ParseSelectionPolicy(TransportConfig& config, const std::string& value)
{
    const auto normalized = ToLower(value);
    if (normalized == "round_robin" || normalized == "rr") {
        config.selectionPolicy = "round_robin";
        return Status::OK();
    }
    if (normalized == "least_loaded" || normalized == "least-loaded" || normalized == "leastloaded") {
        config.selectionPolicy = "least_loaded";
        return Status::OK();
    }
    return Status::Error(StatusCode::INVALID_ARGUMENT,
                         "selectionPolicy must be round_robin or least_loaded");
}

}  // namespace

Status NormalizeChannelFullPolicy(std::string& policy)
{
    const auto normalized = ToLower(TrimConfigValue(policy));
    if (normalized == "fail_fast" || normalized == "fail-fast" || normalized == "failfast") {
        policy = "fail_fast";
        return Status::OK();
    }
    if (normalized == "flow_wait" || normalized == "flow-wait" || normalized == "flowwait") {
        policy = "flow_wait";
        return Status::OK();
    }
    return Status::Error(StatusCode::INVALID_ARGUMENT,
                         "channelFullPolicy must be fail_fast or flow_wait");
}

Status ApplyTransportRetryConfigField(TransportConfig& config, const std::string& key,
                                      const std::string& value)
{
#define KV_ASSIGN_U32(camel, snake, field)                                                       \
    if (key == camel || key == snake) {                                                          \
        std::uint64_t parsed = 0;                                                                \
        const auto status = ParseRetryUnsigned(key, value, std::numeric_limits<std::uint32_t>::max(), parsed); \
        if (!status.ok()) { return status; }                                                     \
        config.field = static_cast<std::uint32_t>(parsed);                                       \
        return Status::OK();                                                                     \
    }
#define KV_ASSIGN_U64(camel, snake, field)                                                       \
    if (key == camel || key == snake) {                                                          \
        std::uint64_t parsed = 0;                                                                \
        const auto status = ParseRetryUnsigned(key, value, std::numeric_limits<std::uint64_t>::max(), parsed); \
        if (!status.ok()) { return status; }                                                     \
        config.field = parsed;                                                                   \
        return Status::OK();                                                                     \
    }
#define KV_ASSIGN_BOOL(camel, snake, field)                                                      \
    if (key == camel || key == snake) {                                                          \
        bool parsed = false;                                                                     \
        const auto status = ParseRetryBool(key, value, parsed);                                  \
        if (!status.ok()) { return status; }                                                     \
        config.field = parsed;                                                                   \
        return Status::OK();                                                                     \
    }
#define KV_ASSIGN_STRING(camel, snake, field)                                                    \
    if (key == camel || key == snake) {                                                          \
        config.field = value;                                                                    \
        return Status::OK();                                                                     \
    }

    KV_ASSIGN_U32("busyRetryCount", "busy_retry_count", busyRetryCount)
    KV_ASSIGN_U32("failoverRetryCount", "failover_retry_count", failoverRetryCount)
    KV_ASSIGN_U64("busyRetryBackoffMs", "busy_retry_backoff_ms", busyRetryBackoffMs)
    KV_ASSIGN_BOOL("probeEnabled", "probe_enabled", probeEnabled)
    KV_ASSIGN_U64("probePeriodMs", "probe_period_ms", probePeriodMs)
    KV_ASSIGN_U32("probeRetryCount", "probe_retry_count", probeRetryCount)
    KV_ASSIGN_U64("probeRetryBackoffMs", "probe_retry_backoff_ms", probeRetryBackoffMs)
    KV_ASSIGN_U32("probeMaxInflight", "probe_max_inflight", probeMaxInflight)
    KV_ASSIGN_U32("probeMaxSubmissionsPerRound", "probe_max_submissions_per_round", probeMaxSubmissionsPerRound)
    KV_ASSIGN_U32("probeTransportScanBudget", "probe_transport_scan_budget", probeTransportScanBudget)
    KV_ASSIGN_U32("probeChannelScanBudgetPerTransport", "probe_channel_scan_budget_per_transport", probeChannelScanBudgetPerTransport)
    KV_ASSIGN_U64("probeTimeoutMs", "probe_timeout_ms", probeTimeoutMs)
    KV_ASSIGN_BOOL("flowControlEnabled", "flow_control_enabled", flowControlEnabled)
    KV_ASSIGN_STRING("channelFullPolicy", "channel_full_policy", channelFullPolicy)
    KV_ASSIGN_U32("busyFloor", "busy_floor", busyFloor)
    KV_ASSIGN_U64("stableSuccessMs", "stable_success_ms", stableSuccessMs)
    KV_ASSIGN_U32("maxInflightPerChannel", "max_inflight_per_channel", maxInflightPerChannel)
    KV_ASSIGN_U32("connectRetryPerRound", "connect_retry_per_round", connectRetryPerRound)
    KV_ASSIGN_U64("recoverInitialBackoffMs", "recover_initial_backoff_ms", recoverInitialBackoffMs)
    KV_ASSIGN_U64("recoverMaxBackoffMs", "recover_max_backoff_ms", recoverMaxBackoffMs)

    if (key == "selectionPolicy" || key == "selection_policy") { return ParseSelectionPolicy(config, value); }
    return Status::Error(StatusCode::UNSUPPORTED, "unsupported retry config field: " + key);

#undef KV_ASSIGN_U32
#undef KV_ASSIGN_U64
#undef KV_ASSIGN_BOOL
#undef KV_ASSIGN_STRING
}

NodeEndpoint ParseTransportEndpoint(const std::string& value)
{
    NodeEndpoint endpoint;
    if (value.find('=') == std::string::npos) {
        auto parts = SplitConfigValue(value, ':');
        if (!parts.empty()) { endpoint.ip = parts[0]; }
        if (parts.size() > 1) {
            endpoint.port = static_cast<std::uint16_t>(ParseConfigUint64(parts[1]));
        }
        if (parts.size() > 2) { endpoint.protocol = ParseConfigProtocol(parts[2]); }
        return endpoint;
    }

    for (const auto& item : SplitConfigValue(value, ',')) {
        const auto pos = item.find('=');
        if (pos == std::string::npos) { continue; }
        ApplyTransportEndpointField(endpoint, TrimConfigValue(item.substr(0, pos)),
                                    TrimConfigValue(item.substr(pos + 1)));
    }
    return endpoint;
}

}  // namespace kv
