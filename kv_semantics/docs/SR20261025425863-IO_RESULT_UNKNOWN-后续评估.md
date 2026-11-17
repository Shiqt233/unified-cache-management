# IO_RESULT_UNKNOWN 后续评估记录

日期：2026-11-16  
状态：**待评估，未确认；不属于当前实现或当前 Story/design 的生效设计**

## 1. 当前处置

按本次评审决定，暂时从当前实现和当前 Story/design 中移除 `IO_RESULT_UNKNOWN` 及其配套逻辑，本文仅保存后续评估所需背景，不得据此认定该状态码已经实现、允许使用或已经验收。

本次移除的内容包括：

- 公共 `StatusCode::IO_RESULT_UNKNOWN = 16` 及 CLI 状态命名；
- `ErrorCategory::TIMEOUT_UNKNOWN`、`RetryAction::RETURN_OUTCOME_UNKNOWN`；
- `ChannelHealthAction::ISOLATE_GENERATION`；
- `ErrorDecision::outcomeUnknown`；
- `SubBatchRetryDecision::nonIdempotentOutcomeUnknown`；
- `SubBatchRetryAction::OUTCOME_UNKNOWN`；
- 按操作幂等性区分超时处理的 `IsIdempotent` 分支及 `deleteMissingIsSuccess` 配置；
- Entry/TransportTask/ClientTask 的 UNKNOWN 专用映射与聚合；
- 对应的软件测试、Story、design 和实现证据。

## 2. 移除后的当前行为

当前实现按原 SR 的链路故障方向处理超时：

```text
CQE_IO_TIMEOUT / 已发送 attempt 的结果未知
  → ErrorCategory::LINK_FAILURE
  → RetryAction::FAILOVER
  → ChannelHealthAction::RECORD_FAILURE
  → 按既有 N 次额外尝试重发完整原始 subBatch
```

因此：

- 不再生成公共 `IO_RESULT_UNKNOWN`；
- 不再根据 STORE 是否非幂等而禁止超时重试；
- 非幂等 STORE 也可能随完整原始 subBatch 重发；
- 重试成功返回最终有效响应，重试耗尽或没有候选 Channel 时保留原始 CQE/TIMEOUT 等错误；
- 聚合层沿用 `OK/PARTIAL_FAILED` 等既有结果，不做 UNKNOWN 特殊聚合。

这意味着当前明确接受以下风险：

1. 第一次 STORE 已创建或写满 Key、但响应未知时，重试可能返回 `CQE_KEY_ALREADY_EXISTED`。
2. `KEY_EXIST` 不能证明第一次请求由当前客户端成功执行，也不能自动转换为 OK。
3. BATCH_STORE 整体重试会重复执行已成功或已返回应用错误的 Entry，最后一次响应可能覆盖第一次 OK。
4. 当前行为不提供 exactly-once，也不提供执行结果补偿或对账结论。

## 3. 原提案要解决的问题

### 3.1 STORE 的真实状态机

按已明确的 KV 语义：

```text
Key 不存在
  → 创建 Key 并写入

Key 已存在但范围未写满
  → 可以覆盖写

Key 的范围已写满
  → 返回 CQE_KEY_ALREADY_EXISTED（key_exist）
```

`OK` 只表示本次写入被接受，不必然表示 Key 已写满；`KEY_EXIST` 表示 Key 已写满、当前写入被拒绝。二者都属于服务端应用结果。

### 3.2 原 UNKNOWN 提案

原提案希望增加一个由客户端生成的状态，用于表达：

> 请求可能已经发送，但客户端没有可信的最终应用响应，无法确认是否创建、写入或写满。

它与 `OK`、`KEY_EXIST` 的区别是：

- `OK/KEY_EXIST` 来自明确的服务端响应；
- `IO_RESULT_UNKNOWN` 来自客户端对响应缺失、超时或旧代际响应的不确定性判断。

原提案的核心用途不是改变重试粒度，而是在进入完整 subBatch 重试前 veto 自动重放，并让上层知道需要补偿，而不是盲目再次写入。

### 3.3 典型风险场景

```text
Entry0：创建 KeyA 并写满，第一次返回 OK
Entry1：写入 KeyB，结果未知

若直接重发完整 subBatch：
Entry0 可能从 OK 变为 KEY_EXIST
Entry1 可能变为 OK
```

原 UNKNOWN veto 会阻止整个 subBatch 自动重发，保留 Entry0=OK、Entry1=UNKNOWN，并将补偿责任交给上层。

## 4. 后续可选方案（均待评审）

| 方案 | 内容 | 主要代价/前提 |
| --- | --- | --- |
| 公共 UNKNOWN 状态码 | 恢复独立的 Entry/顶层状态及聚合 | 公共 API 兼容、两层聚合口径、调用方补偿契约 |
| TIMEOUT + 内部标记 | 公共仍返回 TIMEOUT，内部记录 outcomeUnknown | 上层无法仅凭 TaskResult 区分普通超时与执行不确定 |
| 非幂等写禁止自动重试 | 不新增公共状态，只在内部禁止重放 | 可用性下降，仍需定义最终返回与补偿方式 |
| 服务端请求去重 | requestId/idempotency key，重复请求返回首次结果 | 需要协议和服务端支持，CID/attemptId/generation 不能替代 |
| 条件写/版本控制 | 版本、CAS、per-key sequence/fencing | 防止旧 attempt 覆盖新写，需扩展协议 |
| 上层对账 | Query/Load/版本/覆盖范围查询后补偿 | 需要可证明写者归属与范围状态，普通 exists 不足以证明 |

任何方案都必须同时定义：

- Entry、subBatch、TransportTask、ClientTask 的结果传播；
- 单 STORE 与 BATCH_STORE 的差异；
- `KEY_EXIST` 是否仅为应用结果，禁止自动转 OK；
- 超时、busy、发送错误、写故障和旧 generation 的执行确定性；
- MR、Payload、Send/Flag Slot 在迟到响应期间的生命周期；
- 调用方补偿与测试证据。

## 5. 重新评估前必须确认的协议事实

1. `value_size`、本次写入 length、offset 和已写覆盖范围的独立定义。
2. `CQE_KEY_ALREADY_EXISTED` 在单 STORE 与 BATCH_STORE 逐 Entry 结果中的准确编码。
3. busy 是否保证未创建、未写入、未推进已写范围。
4. `CONNECTION_ERROR`、`CQE_WRITE_FAULT` 等错误是否可能部分执行。
5. Batch 是否提供原子性；没有原子性时如何表达部分成功与重复执行。
6. `is_exists` 是否只对写满 Key 命中，部分写如何查询和对账。
7. Provider 对旧 CQE 停止写回、MR 注销和 Slot 安全复用的证明。

## 6. 后续验证场景

- 创建但未写满后响应丢失；
- 创建并写满后响应丢失；
- 已写满 Key 首次 STORE 返回 KEY_EXIST；
- 第一次 OK、第二次 KEY_EXIST，验证不能把第二次响应当作第一次成功证明；
- BATCH_STORE 中 OK+busy、OK+timeout、KEY_EXIST+timeout 的组合；
- 无候选 Channel、N 次耗尽、旧 generation 响应和迟到 CQE；
- 同 Key 新旧写乱序、Payload 在重试期间变化、MR 注销与 Slot 复用；
- 两层 TaskResult 对 OK+UNKNOWN、全 UNKNOWN、UNKNOWN+应用错误的统一聚合（若未来重新引入）。

当前 Fake 的覆盖写和文件存在性检查不代表真实 ASU 的写满/可见性语义，不能作为恢复 UNKNOWN 或认定重试安全的证据。

## 7. 相关历史分析

以下文档保留相关分析作为历史/后续评审材料；其中 UNKNOWN 内容均不视为当前生效设计：

- `SR20261025425863-Wait-MR-Buffer生命周期必要性分析.md`
- `SR20261025425863-subBatch扩展合理性分析.md`

在重新形成明确结论前，不得把本文或上述历史材料中的 UNKNOWN 方案标记为 implemented、已确认或已验收。
