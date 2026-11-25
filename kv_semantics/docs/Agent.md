# KV Client连接可靠性-Agent编程精简版

## 1. 范围
**做**：错误分类与重试路由、连接健康（Channel 状态机 X=1 排空 + FAILED 复活）、Endpoint 整组恢复、KeepAlive共享探针（探测 `{ACTIVE 空闲，FAILED}`，成功即复活）、`UpdateEndpoints` 增量动态改IP、Group 动态流控（-2/+1 迟滞）、节点三态快照（GREEN/YELLOW/RED）、配置结构化（5个新增 Config 结构）。
**不做（禁止顺手实现）**：FLOW_WAIT（仅可选扩展，非默认）、Quarantine/MR 隔离（MR/内存生命周期由调用方与底层保证）、`logicalSlotId`、完整`ConnectionAttemptKey`、单 QP 独立建链恢复、Client 层重试状态机（Client只路由/聚合Wait消费）、GlobalView 修改与自动改投；**不把探针成功视为下游业务能力恢复**（探针成功仅代表 KeepAlive 通路往返，业务大 IO 仍可能失败）；**不为复活/动态修改新增公共错误码**（沿用既有 Status 集合）

## 2. 外部接口契约
| 接口 | 签名 | 关键语义 |
| --- | --- | --- |
| KvClient::Init/Shutdown | `Init(const KvClientConfig&)`；`Shutdown()` | Init 校验并**固化配置副本**；Shutdown 幂等、可重入。Init 失败语义：首轮建连 1+K 失败**逆序释放已建资源**返回错误；K 次重试**不复用已失败连接对象、不泄露已注册 MR**；**内部 Buffer 注册失败（Send/Flag MR 或 Token获取失败）-> Shutdown 已创建资源、禁止启动Worker**；配置校验失败**不得覆盖已正常运行的旧快照**。防混淆：`maxInflightTasks`（Client 提交队列容量）!= 单 QP inflight；`defaultWaitTimeoutMs` != Transport 总Deadline |
| QueryAsync/LoadAsync/StoreAsync/DeleteAsync | 现有签名 | 同步 OK 仅表示入队；最终状态必须经 Wait；`entryStatus` 才含逐 Entry错误，顶层 `PARTIAL_FAILED` 不能替代错误分类；`CQE_IO_TIMEOUT` 按链路故障换链 |
| Check/Wait| 现有签名 | Wait 超时只消费 ClientTask，**不干预底层任务、不联动 Cancel 底层、不据此 ReportFailuer**；**Check 未知 ID 或已完成均返回true**（返回 true 不能证明任务曾成功） |
| AsuTransport 新增 | `GetConnectionHealth(NodeHealthSnapshot&)` `UpdateEndpoints(toAdd, toRemove, UpdateResult&)`| 两个新增 virtual 默认返回 `UNSUPPORTED`，由 Impl 覆盖，Stub 无需实现 |
| TransProvider SPI | CreateConnection/Send/RegisterMemory/BindMemory/DeleteConnections | 建链超时入参 `connectTimeoutMs`；保留 Provider 返回原始码（rawStatsu）与产生点日志上下文，不设独立错误来源枚举；**DeleteConnections 排空契约：删除该 Endpoint 全部连接后，旧在途请求不再产生写回（排空）-- 整组重建的正确性依赖该契约，测试桩 `ScriptedTransProvider` 亦必须遵守**|

**UpdateEndpoints 契约**：增量传参，统一**先删后加**；**不新增线程**（建链/断链复用既有 RecoverLoop/RecoverWorker 语义）。toRemove 按 IP:port 匹配，无匹配skip（幂等）-> 匹配则**先摘表后断链**（先出 groups_ 遍历范围，再异步排空整组 DeleteConnections，不误触发整组恢复）。toAdd 在删后 groups_ 上按 ip:port 匹配：全等skip（保留状态/退避/流控）、属性不同自动替换（旧组走删除路径+新组走创建路径，禁止 endpoint 双 Group 并存）、无现组则建链（复用既有 AddGroup 路径：建链->能力查询->上表，失败自动回滚本组、不回滚已成功组；每轮 1+K 次、指数退避，与 Init/恢复同状态机）。best-effort 无回滚，部分失败如 UpdateResult.failedEndpoints；重推同一 patch 幂等收敛；入口 fail-fast 原子拒绝：toAdd 与 toRemove 均为空，或任一含非法 Endpoint（端口 0 等）-> INVALIDE_ARGUMENT，不修改任何现有 Gorup。快照 updating 状态区分配置窗口 RED 与故障 RED

**GetConnectionHealth 三态契约**：GREEN=该 ASU 全部Group的全部Channel ACTIVE；YELLOW=存在非ACTIVE且该ASU仍有ACTIVE链路；RED=该ASU 全部Group均无ACTIVE（与节点级无候选fail-fast一致，与`SelectConnection`无候选必然CONNECTION_ERROR对应）。单Group恢复窗口且其他 Group 由 ACTIVE时节点为 YELLOW，不独立触发节点 RED。NodeHealthSnapshow 字段仅 health（跨Group聚合） + updating(bool)：不设per-group明细与计数字段 （Group/Channel级观测走既有字段-Channel级lastProbeResult 不收影响-与结构化日志），快照只承载最终状态与配置窗口标志。快照只读派生：由Channel状态按Group聚合后再跨Group聚合；不修改GlobalView、不驱动自动改投。 CheckHealth 仅Worker存活语义，与QP健康分离；StubTransport 默认 UNSUPPORTED。

## 3. 新增配置结构 （Init校验，非法值返回 INVALID_ARGUMENT，禁止静默回退）
| 数据结构 | 字段=默认 | 说明 |
| --- | --- | --- |
| RetryConfig | busyRetryCount=2 (M，0..16); busyRetryBackoffBaseMs=1; busyRetryBackoffMaxMs=100; failoverRetryCount=2 (N，0..16); taskTimeoutMs=100 (>0 任务总Deadline); attemptTimeoutMs=100 (>0，且min(attemptTimeoutMs, 剩余总时间)); | 原链退避指数封顶（仅 BuSY）；第 k 次退避 = min(busyRetryBackOffMaxMs, busyRetryBackoffBaseMsx2^(k-1)); ENTRY_RETRY_ADVISED 与 FAILOVER 均无退避立刻换链；不加jitter|
| RecoveryConfig | connectTimeoutMs=5000; connectRetryPerRound=2(K，0..16); recoverInitialBackoffMs=100; recoverMaxBackoffMs=5000; recoverCheckIntervalMs=100 | K 次建链 Init/运行期共用状态机；成功后退避重置为初始值 |
| ProbeConfig | probeEnabled=true; probePeriodMs=1000; probeRetryCount=目标 1；当前默认 0 (m, 0..16); probeRetryBackoffMs=5; probeTimeoutMs=1000; maxConcurrentTransports=4 (1..1024); maxChannelsPerTransport=16 (1..4096)| 全局在途上限派生 = maxConcurrentTransportsxmax maxChannelsPerTransport (默认4x16=64，不可独立配置，上限 1024x1024=4M)；activeProbeCount<64亦有该派生取小|
| FlowControlConfig | flowControlEnabled=可配置启用(当前默认 false); | maxInflightPerChannel=256(被服务端 ioQueueDepth 收紧); busyFloor=32(0<T<maxInflightPerChannel); stableSuccessMs=60000 | busyFloor 需小于实际 ioQueueDepth，否则Init拒绝；满载信息在写死 fail-fast|
| ConnectionConfig | selectionPolicy=ROUND_ROBIN (兼容 LEAST_LOADED); maxErrorCount=1 (X=1，连续链路失拜>=X即DRAINING；有效响应清零失败计数-X为连续失败阈值，非累积计数) | 兼容性：统一X=1，迁移测试必须覆盖 X=1路径|

## 4. 运行时数据结构（关键字段）

TransportSubBatchContext (单层内联，禁止独立AttemptContext/SubBatchRetryState对象)
保留基线字段（`cid`、`opType`、`state`、`status`、`channel`、`useSeekControl`、`sendSge`、`flagBuffer`、`entryStatus`） + 新增: `attemptDeadline`、`busyRetries`、`pendingRetryCategory`、`attemptedChannels`、`retryAt`、`group(shared_ptr lease)`、`request`(不设独立retryPending布尔-NONE态兼做挂起标志位)
- attempt 状态不持久化枚举，由派生：READY=无在途且 pendingRetryCategory=NONE；SENDING=Send调用内瞬时态；WAITING_RESPONSE=Send 已返回 OK 未裁决；RETRY_BACKOFF=pendingRetryCategory!=NONE；COMPLETED/CANCELLED=任务终态。
- WAITING_RESPONSE 时Channel已排空/已替换后到达的响应->合成CQE_IO_TIMEOUT，按LINK_FAILUER换链处理（不以旧对象裁决新对象）
- 持久字段pendingRetryCategory（取值集合{NONE，BUSY，LINK_FAILURE}, NONE=无挂起重试）；ExecuteRetry发送前直接读取选M/backoff与是否ON_BUSY，同时复位为NONE(新attempt响应到达后按结果重新设置)，不得依赖lastError反查（退避期间可能被迟到响应覆盖）；ENTRY_RETRY_ADVISED换链记LINK_FAILUER，其不降载不记失败在分类时由ErrorDecision输出，不依赖该字段
- 派生位：failoverAttempts=attemptedChannels.size；retryAction=由pendingRetryCategory派生（BUSY->原链，LINK_FAILOVER，RA换链共用FAILOVER力学、无独立计数）；preferredChannel=channel字段。
- 候选排除用ConnectionChannel*（对象身份去重）

TransportTask新增：origin（BUSINESS/PROBE）、cancelRequested、probeLease；ConnectGroup新增：flowController_、inflightCount；ConnectionChannel字段：channelId/state/inflightCount/errorCount/lastBusinessSubmitNs/probeInFlight/weak Group lease(不设 consecutiveProbeSuccess观测计数)

探针家族（全新）：ClientProbeScheduler（config_/budget_/transportProvider_/transportCursor_/work_/runMu_/waitMu_/waitCv_，每Client<=1线程）、ProbeTarget（无线程）、ProbeBudget（limit=派生上限、active CAS）、ProbeTaskLease（预算幂等释放；probeInFlight为Channel级计数经channel_递减）

bufferManager等现有模块零修改；仅使用既有Init/Shutdown/Allocate/Free

## 5. 错误分类与重试路由（SFEMA核心）
探针、聚合值（PARTIAL_FAILED）与清单排除项不进映射表。
不进ErrorClassifier的清单排除项：
1）前置校验/生命周期错误（INVALID_ARGUMENT/NOT_INITIALIZED/TASK_NOT_FOUND/UNSUPPORTED/BUFFER_*）API层同步拒绝或直接返回
2）建链错误（CreateConnection失败，含建链资源不可用RESOURCE_BUSY=11-HCOMM channel本端/远端资源不足）由RecoverLoop状态机自控K次+退避，不记业务Channel错误计数
3）未发超时（总Deadline到期且无在途attempt）Executor直接TaskResult.status=TIMEOUT终结，不换链不记失败不重试；已实际Send后的草率已TIMEOUT进入分类器（调Classify前由Executor在途attempt判定保证非未发超时）
4）本地执行错误（Executor/产生方内部INTERNAL_ERROR类失败，如内存分配失败）由产生方直接终结RETURN_RESULT，不换链不记失败不重试
5）本地任务队列或Slot满的RESOURCE_BUSY(受理前同步返回，可退避后重新提交)；Provider Send路径不返回RESOURCE_BUSY，远端忙必须经合肥CQE0x10731表达
6）Wait超时TIMEOUT（只消费ClientTask，任务由自身Deadline终结）

核心映射
| 来源 | Category | RetryAction | 健康/流控 |
| --- | --- | --- | --- |
| CONNECTION_ERROR （Send）/CQE_INTERNAL_ERROR / CQE_WRITE_FAULT / CQE_UNRECOVERED_READ_ERROR / CQE_IO_TIMEOUT / TIMEOUT（已实际Send后超时，attempt超时裁决） | LINK_FAILURE  |  FAILOVER，额外最多N次，排除已试Channel  | 当前Channel记失败（达X排空->FAILED）；旧CQE按CID隔离；流控NO_CHANGE |
| CQE_RESOURCE_BUSY(远端=0x10731，合法CQE) | BUSY  |  原链重试<=M1，增长型退避  | 不记失败；ON_BUSY降载；最终Busy不换链 |
| ENTRY_RETRY_ADVISED | RETRY_ADVISORY  |  FAILOVER 换链 <=N，无退避立即换链、排除已试Channel（RA单码或与条码级应用码混合均换链；RA仅存在于逐条目表达场景，不于BUSY/链路码工程）  | 不记失败；不降载；耗尽保留原始错误 |
| CQE_CHECK_RESULT_BUFFER | 非最终分类  |  先解逐Entry再ClassifySubBatch；解码本身失败按兜底换链（最多N次）；Batch错误码归并：所有子操作为相同原因错误时subBatch级错误码按对应CQE_\*归并（处置同单操作路径），否则逐子操作按ENTRY_\*表达  | 禁止整批盲目重试 |
| 应用类（INVALID_*/KEY_ALREADY_EXISTED/OUT_OF_CREATE_SIZE/ENTRY_NO_RETRY/KEY_NOT_FOUNT/DATA_NOT_EXIST/DELETE_FAILED/CQE_INVALID_COMMAND_OPCODE/FIELD; CQE_KEY_NOT_EXIST按OpType区分：LOAD/STORE->RETURN_RESULT、QUERY->正常查询结果（query miss，optype+status驱动，与category无关）；ENTRY_KEY_NOT_EXIST->QUERY下正常查询结果（query miss）；ENTRY_KEY_EXIST->正常业务结果） | APPLICATION |  RETURN_RESULT不重试 | 不记链路失败 |
| 未知/协议错误/数量不匹配/IO_ERROR无明确语义 | LINK_FAILURE（兜底，fallback=true）  |  默认FAILOVER 换链最多N次；路由仅有StatusCode+产生路径决定；五类排除项不适用兜底：应用类/协议校验类错误、本地RESOURCE_BUSY、未发超时、建链失败、本地执行错误（Executor内部INTERAL_ERROR类由产生方直接终结RETURN_RESULT，不进ErrorClassifier、不换链、不记链路失败）  | 当前Channel记失败并记FALLBACK标签（区分于EXPLICIT，由ErrorDecision.fallback置位驱动）；保留rawStatus首次解析记录 |

subBatch判定规则：单一失败错误码按其策略执行（Batch错误码归并：所有子操作作为相同原因错误时subBatch级错误码按对应CQE_\* 归并返回，处置与单操作路径同构；否则逐子操作按ENTRY_\*表达，CQE_CHECK_RESULT_BUFFER经作为解析umbrella）；多种失败错误码混合只发生再逐条目表达场景，失败码只含ENTRY_\*码（busy/链路故障/超时/未知均为subBatch级单码-每次attempt顶层只产生一个CQE，恒不进条目级混合）：纯应用码混合不重试直接返回；存在ENTRY_RETRY_ADVISED即完整原始subBatch换链<=N（无退避立即换链；RA单码同；应用错误与成功Entry随整体重试，不拆子集；BUSY原链M1属BUSY单码场景，不存在BUSY与其他失败码混合分钟）；换链时pendingRetryCategory记LINK_FAILURE、busyRetries不递增，category取最高优先级（LINK_FAILURE>BUSY>RETRY_ADVISORY>APPLICATION，ErrorCategory枚举仅此4值：无SUCCESS-分类器只处理失败结果，正常结果由IsNormalREsult前置排除；无QUERY_MISS-query miss 呈现由opType+status在聚合层驱动；无OTHER-兜底由fallback 布尔位标记，category与明确链路故障同为LINK_FAILURE）仅作记账口径（降载/失败记账/fallback标记，不派生动作），hasMultipleErrorCodes为独立bool（MIXED不是枚举）；IsNormalResult不得只调status.ok() (Query的exists结果要排除)。单Store或全部Entry均为超时错误时：entryStatus保留CQE_IO_TIMEOUT等原始错误，顶层仍既有规则PARTIAL_FAILED，不新增超时专用公共错误码；Query的exists/prefixHitKeys按原始Key顺序计算。重试为at-least-once：按最终有效响应+originalIndices只回写异常；禁止复制待Slot/lease的TransportSubBatchContext；外层Client/TransportTask不得二次重试。

分类顺序：先解CQE_CHECK_RESULT_BUFFER逐Entry->按协议含义分类->非协议明确值按无明确语义走兜底->映射三Action，最后未被排除项命中即落兜底。rawStatus第一次解析必须保留（未知raw不得折叠成IO_ERROR；Provider来源差异由rawStatus与产生点日志承载，不设独立错误来源枚举）。兜底规则：未知/无明确语义错误默认标记链接异常、按LINK_FAILURE换链最多N次：五类排除项（应用类/协议校验类错误、本地RESOURCE_BUSY、未发超时、建链失败、本地执行错误）必须在ErrorClassifier入口静态白名单判定，先于兜底分支（本地执行错误与未发超时、建链失败根本不进分类器，由产生方/Executor/RecoverLoop直接终结）；兜底换链日志/指标记FALLBACK标签（区别于EXPLICIT），验收观测兜底占比


## 6. 核心算法规则
选路（SelectConnection/AcquireChannel）：RR/LEAST_LOADED；成功同时增Channel+Group 的 inflight。原链重试使用 AcquireChannel(channel)（不重置 channel 字段）；failover 使用 SelectConnection(excludedChannels)两级优先：跨Group优先->同Group兜底; attemptedChannels对象身份去重；满载无候选fail-fast

attempt流转：BUSY->busyRetries++、pendingRetryCategory=BUSY、retryAt=now+min(busyRetryBackoffMaxMs, busyRetryBackoffBaseMsx2^(busyRetries-1)); ENTRY_RETRY_ADVISED->FAILOVER换链<=N(无退避立即换链，不降载不记失败)；LINK_FAILURE->FAILOVER(耗尽保留原始错误)；RETRY_BACKOFF由CompletionLoop HasRetryWork/ExecuteRetry非阻塞驱动（禁止在共享线程内sleep（backoff））；总Deadline到期：未完成 Entry=TIMEOUT，不再启动新 attempt。重试请求构造（PrepareRetryRequest）必须同步更新SQE中的CID与响应Buffer/MR，不得只修改subBatchContext.cid（SQE注册信息与context字段不一致会导致响应错配）。跨subBatch部分中止(AbortSubBatchesBeforeSend)：Batch后续subBatch尚未发送而前置subBatch已失败时-首个失败subBatch的未发送Entry保留IN_PROGRESS，其后subBatch标为CANCELED（部分中止，不整批全弃）

连接状态机（ChannelState）：INIT->ACTIVE/FAILED；ACTIVE->DRAINING(达到X=1)；DRAINING->FAILED(inflight=0)；FAILED->ACTIVE 仅探针复活；不存在ACTIVE->FAILED直接边、不存在单QP独立建链。FAILED=UNAVALILABLE别名，退役待复活。每次状态更新后DRAINING/FAILED不接新IO，响应用CID匹配校验。

整组恢复（RecoverLoop，Endpoint级）：触发条件=该Group全部 Channel 均非 ACTIVE（可探针二次确认）；RecoverState仅存backoffMs/nextRetryAt，存在即恢复窗口。窗口内：新请求立即CONNECTION_ERROR、FAILED Channel 禁止探针复活、建链失败不进分类器。有界等待组 inflight 归零，上限max（taskTimeoutMs，recoverInitialBackoffMs）；超时后强制断链，在途任务按CQE_IO_TIMEOUT终结，不得死等）-> 整组DeleteConnections -> CreateConnection 每轮 1+K 次(每次connectTimeoutMs，指数退避封顶 recoverMaxBackoffMs) -> 成功一次性替换全部新Channel对象（计数器从零，禁复用旧对象计数）；失败backoffx2下轮重试

KeepAlive探针：调度器线程启动即 RunOnce，之后probePeriodMs + 有界 jitter；游标分批<=maxChannelsPerTransport(16) Channel。探测白名单={ACTIVE且businessInflight==0，FAILED}；DRAINING跳过；已有probeInFlight跳过。探针三值与处置：FLUSH（Send同步返回flush=QP硬件ERROR）- FAILED Channel维持FAILED记lastProbeResult=FLUSH，ACTIVE Channel 按LINK_FAILURE ReportFailure（达X排空该QP）；SUCCESS（合法KeepAlive CQE完整往返且opcode/CID/result校验哦通过，收到任意CQE不足判SUCCESS）- FAILED Channel且该组无RecoverState ->复活（本地状态转移、计数器从零、无次数限制无冷却、不触发整组动作），ACTIVE Channel只CompleteSubBatch不升载不清errorCount；TIMEOUT（Send已发出、probeTimeoutMs内无响应）-FAILED Channel维持FAILED 记lastProbeResult=TIMEOUT，ACTVE Channel按m轮内重试、耗尽后ReportFailure排空；INCONCLUSIVE/其他非链路结果-保持状态、lastProbeResult不更新。探针与恢复互斥三规则：1）该组存在RecoverState（恢复窗口）本轮跳过该组全部Channel（含FAILED），不提交新PROBE不发起复活；2）恢复触发单向优先不可撤销-探针结论不撤销已触发的整组恢复；3）在途探针兜底-恢复触发时已提交PROBE由有界排空（含PROBE在途）+Channel对象身份+CID迟到忽略兜底，不依赖探针主动取消。lease：提交失败立即Release；完成/超时/取消/Shutdown幂等Relase。排空口径二分：transport层等待含PROBE在途；业务inflight仅含BUSINESS。已知窄病理限制：KeepAlive通但业务大IO返回CQE_WRITE_FAULT类的QP按probePeriodMs节奏周期性复活（正确性无损），运营处置=UpdateEndpoints替换或重启进程。

动态流控（GroupFlowController）：初始化groupNormalLimit = Σ min(config.maxInflightPerChannel, ioQueueDepth)、groupBusyFloor = Σ config.busyFloor、currentLimit = groupNormalLimit（无congested字段：降载态 currentLimit < groupNormalLimit，观测走日志）。legal CQE busy -> currentLimit - 2(下限groupBusyFloor)、复位稳定计时。业务成功：可执行口径=非PROBE的BUSINESS subBatch完整成功，Query miss按正常查询完成计入；探针/应用错误/部分失败/链路故障/超时均不升载） -> currentLimit + 1(上限normalLimit)、更新lastSuccessAt；达到normalLimit或稳定成功持续>= stableSuccessMs -> 解除降载、恢复全量（currentLimit=normalLimit）。非成功响应，或距上次成功>stableSuccessMs(stableSuccessMs 双语义：持续成功解除实际 最大成功响应间隔)->复位稳定计数。准入：actualInflight < currentLimit 才允许，达到 即 fail-fast。CanAcquire/OnBusy/OnBusinessSuccess/OnNonSuccess，无等待队列无ResumeWaiters

UpdateEndpoints与恢复正交三规则：1）摘表后清理该组drainList_ 条目（structureMu_ 与 drainMu_ 不嵌套持有）；2）恢复侧建链完成后发布前在structureMu_ 下按gid复核成员身份，非成员（已被摘表） -> 丢弃重建结果+DeleteConnetions（新Handle），不做Remove/AddChannel不断链；3）整组断链由摘表者独占（claim）。跨锁持Group一律shared_ptr lease(禁裸指针channel->GetGroup())；nextGroupId_ 单调计数器替代 gid=groups_.size()

## 7. 并发与生命周期
线程模型（每Client）：1 Client Worker + 3T(Submit/Completion/Recover, T=存活真实 AsuTransportImpl含retired) + P(probe=0/1) + R(临时refresh)；探针线程数与Transport数无关；任何模块不得为attempt/subBatch/per-QP创建线程。retired Transport不再产生新PORBE；其已提交PROBE仍由该Transport的CompletionLoop安全完成或取消（计入3T的retired线程仍存活至上层注销）。ClientProbeScheduler::RunOnce只做分批选择与预算控制；ProbeTarget::RunOnce为被动接口（独立使用AsuTransport时由调用方提供调度与预算）。Completion Worker空转策略：空任务时每50us轮询1次；有任务时执行 HasRetryWork/ExecuteRetry/Poll，连续无完成达到completionPollSpinLimit后yield

锁协议：runMu_串行化 RunOnce；structureMu_ 保护 groups_; channelCacheMu_保护选路缓存；GroupFlowController::mu_保护动态预算；endpointUpdateMu_串行化UpdateEndpoints；producerMu_ 保护提交。全局锁规约：任何CreateConnection/Send/RegisterMemory/DeleteConnections调用不得持有结构锁（structureMu_/channelCacheMu_/drainMu_等）或task-> mutex -- Provider调用一律锁外执行（防死锁，适用于所有Shard）。VisitChannels仅在捕获有限窗口时持structureMu_，Visitor/Submit锁外执行

迟到CQE三防线（必然发生，按确定性窗口处理）：1）CID/attempt校验，旧CID响应忽略； 2）迟到响应只容许ChannelHealthAction=NO_CHANGE/释放资源，不许记账； 3）有界排空压窗口。内存安全已知限制：任务终态即释放Slot/解除MR忙态，不实现Quarantine（显式声明）

## 8. 模块与方法清单（实现落点）

| 模块 | 方法/接口 | 职责要点 |
| --- | --- | --- |
| `ErrorClassifier` | ClassifySubBatch(subBatch, rawStatus)/IsNormalResult` | 输入直接取自TransportSubBatchContext(status/entryStatus/opType, channel同由其持有) + 解析点（Poll Unpack后）随调传入rawStatus-不设独立分类上下文结构；输出ErrorDecision(category/hasMultipleErrorCodes/fallback/retryAction/healthAction/flowAction; fallback=true表示未知/无明确语义兜底，驱动日志/指标 FALLBACK标签；三Action为category的派生位、分类器内单点映射；retryAction=APPLICATION->RETURN_RESULT/BUSY->RETYR_SAME_CHANNEL/RETRY_ADVISORY、LINK_FAILURE->FAILOVER, healthAction仅LINK_FAILURE->RECORD_FAILURE, flowAction仅BUSY->ON_BUSY; 不修改连接状态，不创建线程) |
| `ConnectionManager` | SelectConnection/AcquireChannel/AcquireProbe/VisitChannels/ReportFailure/ReportSuccess/RecoverLoop/GetHealthSnapshot/UpdateGroups/RemoveGroup | Group/Channel管理; AcquireProbe校验探测资格（ACTIVE空闲或FAILED，DRAINING排除）；drainList仅承载排空工作项不作为恢复触发源；整组恢复；三态聚合；动态Endpoint |
| `ConnectionChannel` | TryStartProbe/FinishProbe（PROBE互斥） | 状态/inflight/errorCount/probeInFlight；由subBatch以shared_ptr持有 |
| `GroupFlowController` | CanAcquire/OnBusy/OnBusinessSuccess/OnNonSuccess | 动态预算+稳定成功计时；不设GetSnapshot/Snapshot观测面（流控状态观测走结构化日志） |
| `ClientProbeScheduler` | `Start/Stop/RunOnce/GetStats`（构造注入transportProvider_） | 每 Client<=1线程；分批选择+全局在途预算 |
| `ProbeTarget` | IsIdle/TryStartProbe/VisitChannels/SubmitProbe/RunOnce | 无线程目标适配；结果由 `Executor::Poll` 处理，不创建结果线程 |
| `TransportTaskExecutor` | Execute/Poll/Cancel/HasRetryWork/ExecuteRetry/PrepareRetryRequest/AbortSubBatchesBeforeSend/MarkBusinessSubmit/ReleaseSubBatchResources | SQE/CQE/重试限额内联计算/资源释放；业务数据面入口  |
| `ProtocolManager` | 业务与 KeepAlive SQE/CQE 编解码和校验 | 不感知连接状态与重试；`KvKeepAliveProtocol::UnpackCqe` 已实现 |
| `BufferManager` | 既有`Init/Shutdown/Allocate/Free/IsValidPointer` | **零修改**；Slot复用走既有机制 |
| `KvClientImpl` | 提交任务/Wait/视图刷新/Shutdown | 只路由/聚合/Wait消费；不感知单次 attempt；不承担 MR生命周期 |

## 9. 日志与可定位性（必须打的日志）

结构化日志： taskId、subBatchId、CID、node/group/channel（gid用nextGroupId_ 关联）、错误类别、重试次数、退避、结果；ClientTaskId与TranportTaskId分属不同ID域，日志必须同时记录，禁止混淆

attempt/task 两级指标：attempt=单次 Send/响应/错误类别/重试结果; task=排队/退避/全部重试后的端到端结果；连接指标=inflight、errorCount、状态、当前预算

探针结论转换日志：FAILED Channel三值结论（FLUSH/SUCCESS/TIMEOUT）变化、复活、复活后再失败退役均记录关键日志；连续相同TIMEOUT/FLUSH限流；同一Channel高频复活动作限流（1HZ毛刺场景聚合计数防刷屏）

UpdateEndpoints日志：入参摘要（toAdd/toRemove及逐项结论 创建/删除/skip替换）、updating窗口起止、每个失败Endpoint及原因

限流：busy 和探针高频结果限流，状态转换和最终失败保留关键日志

禁止：业务 Payload、凭证、完整Token、完整内部地址、敏感拓扑；Key按既有规范脱敏；对外Status message同样不得包含。View/配置文件错误不得导致旧快照被替换，刷新失败继续使用旧快照。

## 10. 实现拆分（Shard）与测试

| 序号 | Shard名称 | 职责 | 依赖 | 变更文件 | 测试文件 | ctest -R | 完成门禁 |
| --- | --- | --- | --- | --- | --- | --- | --- | 
| 1 | 错误模型与配置 | ErrorOrigin/Category/Decision、CQE_IO_TIMEOUT、配置校验 | 无 | `src/trans/task/error_classifier.*`、`include/kv_types.h`、`src/common/utils/parser_common.*` |`tests/transport/error_classifier_test.cc`、`tests/transport/trans_config_utils_test.cc` | "ErrorClassifier|TransConfigUtils" | 错误映射和配置DT全绿 |
| 2 | 连接状态、Endpoint 级恢复与节点三态与动态Endpoint | 准入、整组“断链→建链”与退避、三态快照、FAILED复活、UpdateGroups/RemoveGroups增量收敛 | Shard 1  | `src/trans/conn/*` |  `tests/transport/connection_manager_test.cc`、`tests/transport/connection_recovery_test.cc` | "ConnectionManager|ConnectionRecovery" | 状态/恢复/三态/并发 DT 全绿 |
| 3 | 重试与 Batch | attempt、原链/换链、完整原始 subBatch 重试、静态准入、Deadline | Shard 1、2 |  `src/trans/task/trans_task_executor.*`、`kv_submit_flow.cc`、`task_context.h` | `tests/transport/trans_task_executor_test.cc`等 | "ErrorClassifier|TransportTaskExecutor" | 重试/超时/Batch/Deadline DT 全绿 |
| 4 | 主动探针与资源预算 | 共享调度、空闲检测、指定 QP、CQE 解包、三态结果、批限/在途派生上限 | Shard 1～3 | `src/client/probe_scheduler.*`、`src/trans/task/probe_scheduler.*`、`src/trans/proto/kv_protocol.*` | `tests/client/probe_scheduler_test.cc`、`tests/transport/probe_scheduler_test.cc`、`tests/transport/kv_protocol_test.cc` | "ProbeScheduler|KvProtocol"  | 空闲/异常/迟到探针 DT 全绿；大量 Transport 下每 Client 仅 1 个共享探针线程，扫描/提交/在途均不越界 |
| 5 | Group 动态流控 | 两级预算、快降慢升、动态解除； | Shard 1～3 |  `src/trans/conn/group_flow_controller.*` | `tests/transport/group_flow_controller_test.cc` | "GroupFlowController" | 阈值/预算/稳定成功 DT 全绿；|


相关测试基于每Shard按照 测试因子分解，等价类划分，边界值分析来生成相关DT用例进行验证