# Kafka READ 撤权后的预取残留与 ACL 保证边界（基于 kafka-python）

本目录用仓库自带的**假 broker**（`test/mock_broker.py`，走真实协议编解码、内存传输，无需起 TCP 服务）
回答一个安全事件场景中的常见疑问：

> broker 撤销某用户对 topic 的 READ 权限后，应用下一次 `poll()` 为什么还可能拿到消息？
> 这是 bug 吗？服务端 ACL 到底能保证什么？

结论先行：**这能由正常预取（prefetch）解释，不是 bug。**
ACL 是 broker 对**后续 FetchRequest 的逐请求准入判断**，对已经进入客户端进程的数据没有任何追溯能力。

## 运行方式

```bash
.venv/bin/python docs/acl-revocation-demo/01_prefetch_timeline.py
.venv/bin/python docs/acl-revocation-demo/02_containment_actions.py
.venv/bin/python docs/acl-revocation-demo/03_incident_runbook.py
.venv/bin/python docs/acl-revocation-demo/04_inflight_revocation.py
```

## 1. 时间点：Fetch 缓冲、poll、反序列化分别发生在哪

kafka-python 3.x 的消费者**没有后台拉取线程**，所有动作都在调用 `poll()` 的线程内被驱动
（`KafkaConsumer._poll_once` → `Fetcher.fetch_records`）。一次 poll 的内部顺序：

1. `Fetcher.fetch_records()` 先调用 `fetched_records()` 排空本地缓冲；缓冲空了才 `send_fetches()`
   发新的 `FetchRequest`（`kafka/consumer/fetcher.py:198`、`:223`）。
2. 响应回来后，回调 `_handle_fetch_response()` 把每个分区包成 `CompletedFetch` 放进
   `_completed_fetches` 队列——**此刻只是原始线格式字节进了进程内存，还没反序列化**
   （`fetcher.py:1354`、`:1388`）。
3. 下一次 `fetched_records()` 取出 `CompletedFetch`，`_parse_fetched_data()` 用
   `MemoryRecords` 包装字节并构造 `PartitionRecords`（`fetcher.py:1423`、`:1464`）。
4. **反序列化是惰性的**：只有 `PartitionRecords.take()` 真正取记录时才调用 value/key
   deserializer（`fetcher.py:1716` 的 `_unpack_records` → `_deserialize`），即发生在
   `poll()` 内部、记录返回给应用之前。
5. 若响应分区带错误码 29（`TOPIC_AUTHORIZATION_FAILED`），`_parse_fetched_data()` 直接抛
   `TopicAuthorizationFailedError`（`fetcher.py:1552`）；异常在 `fetched_records()` 里被
   延迟到没有其它已排空数据时抛出（`fetcher.py:628` 的 `_next_in_line_exception_metadata`）。

`01_prefetch_timeline.py` 用带时间戳的输出把上面 5 个点完整演了一遍，并在反序列化器里打点，
证明“字节到达”与“反序列化/交给应用”是两个不同时刻。

### 为什么撤权后下一次 poll 仍可能拿到消息

- 撤权前的某次 `FetchRequest` 已被 broker 受理，响应字节已经进入 `_completed_fetches` 或
  内联的 `_next_partition_records`。一次响应可包含多达 `max_poll_records` 之外的整批数据
  （受 `fetch_max_bytes` / `max_partition_fetch_bytes` 限制），应用每次 `poll()` 只取走一部分，
  剩余记录继续驻留客户端。
- 这些记录的读取不再产生任何网络请求，ACL 变更无法“远程删除”它们。
- 另外还有一个更窄的在途窗口：撤权发生在某个 `FetchRequest` 在途期间时，broker 在处理该请求时
  才做鉴权，已受理并生成好的响应仍可能带回数据——见 `04_inflight_revocation.py`。

## 2. 服务端 ACL 到底能保证什么、不能保证什么

**能保证（控制面边界）**

- 对 ACL 生效后 broker 处理的每一个 `FetchRequest`，无 READ 权限一律返回错误码 29，客户端在
  后续 `poll()` 中抛 `TopicAuthorizationFailedError`。
- 同样的准入判断也作用在元数据与提交路径：元数据刷新遇到未授权 topic 会抛错
  （`kafka/coordinator/consumer.py:183`），`OffsetCommit` 对未授权 topic 收集后抛错
  （`coordinator/consumer.py:994`）。
- 鉴权是**逐请求、在 broker 端**进行的；客户端连接不会因撤权被主动断开，也没有“推送式”回收。

**不能保证（必须明确的残留风险）**

- 不能阻止撤权前已受理请求的数据返回（在途窗口，见示例 4）。
- 不能触及已经进入客户端进程的任何东西：`_completed_fetches` 里的原始字节、
  `_next_partition_records` 里已解析的记录、应用已经从 `poll()` 拿到的对象。
- 不能清理应用在撤权前写出的日志、指标、缓存、下游系统副本、堆内存/转储。
- READ 撤权不等于踢组：组成员管理与心跳受 group 权限约束，topic READ 被撤的消费者在某些
  配置下仍可能保持组成员身份，只是 fetch 被拒。
- 多 broker 集群上 ACL 更新有传播延迟（依赖 authorizer 实现，如 ZooKeeper/KRaft 元数据传播），
  “撤权生效时刻”不是全局瞬时的；客户端缓存的元数据也有 `metadata_max_age_ms` 刷新周期。

## 3. pause / 重新分配分区 / close 各自能拦住什么

`02_containment_actions.py` 用同一初始条件（一批 50 条、首次 poll 取走 10 条、40 条仍驻留客户端）
分别施加三种动作，实测结果：

| 动作 | 新的 FetchRequest | 已缓冲但未返回的数据 | 已从 poll() 返回的数据 |
|---|---|---|---|
| `pause(tp)` | 停止（被排除出 `_fetchable_partitions`，`fetcher.py:1228`） | **保留**：原始完成批进 `_paused_completed_fetches`，已解析批进 `_paused_partition_records`；`resume()` 后原样返回（KAFKA-7548，`fetcher.py:591`） | 不受影响，应用继续持有 |
| 重新分配 / 撤分区（`assign([])` 或 rebalance 撤销） | 停止（分区不再 fetchable） | **阻断交付**：`_append()` 对未分配/不可取分区直接跳过并 `drain()`（`fetcher.py:640`、`:684`） | 不受影响，应用继续持有 |
| `close()` | 全部停止，连接关闭 | 释放：`Fetcher.close()` 排空内联槽与停车数据（`fetcher.py:1587`），之后 `poll()` 抛 `IllegalStateError` | 不受影响，应用继续持有 |

要点：

- **pause 是流控，不是安全遏制**。它的语义就是“暂时不取、数据留着”，`resume()` 不需要重新请求
  broker 就能拿到同一批预取记录，因此不能用于撤权场景的数据隔离。
- **重新分配分区（或让 rebalance 收回分区）是客户端侧能阻止“缓冲但未交付”数据的最小动作**；
  手动分配模式用 `assign(剩余分区)`，订阅模式通过取消订阅相关 topic 触发 rebalance。
- **close 是最强的单进程遏制**，适用于整个凭证/主体都不再可信的情形；但它同样无法追回
  已经交给应用代码的记录。
- 三种动作有一个共同边界：**已经从 `poll()` 返回的数据在任何情况下都不受 Kafka 客户端控制**，
  只能由应用层处理（隔离、停止下游写入、审计）。

## 4. 适合安全事件处理服务的撤权流程

`03_incident_runbook.py` 给出可运行的参考形态。分层职责如下：

**服务端（止血，阻止后续外泄）**

1. 移除主体对涉事 topic 的 READ ACL（必要时一并移除 DESCRIBE、组权限），记录变更时间戳。
2. 若凭证本身失陷，轮换/作废凭证（SASL 密码、mTLS 证书），使旧连接重连时无法再通过认证。
3. 等待 authorizer 传播并核对生效（用该主体发一次请求验证返回 errno 29）。

**客户端（遏制残留数据，ACL 管不到的部分）**

4. 让相关消费者实例进入受控路径：先 `pause()` 防止遏制过程中发出新 fetch，再重新分配/撤销分区，
   阻止缓冲区内未交付记录继续被 `poll()` 返回。
5. 对**已经从 `poll()` 拿到、尚未处理完**的记录做应用级隔离（quarantine），暂停一切下游写入，
   而不是假装它们不存在。
6. 记录遏制时刻各分区的 `position()`，作为事件时间线与后续 offset 核对依据。
7. 主体级失陷时直接 `close()` 消费者，并视需要终止/隔离整个工作进程。

**事件后（处理真正的残留面）**

8. 排查并清理撤权前已落地的副本：应用日志、监控指标、本地缓存、对象存储、下游 topic/数据库、
   core dump / heap dump。
9. 审计 offset 提交与处理日志，划定“可能已被读取的数据区间”（从最后一个可信提交位置到遏制位置），
   该区间就是本次事件的数据暴露边界。

### 设计期建议（缩小未来残留窗口）

- 调小 `max_poll_records`、`fetch_max_bytes`、`max_partition_fetch_bytes`，减少单次预取驻留量；
  代价是吞吐下降，需要按事件敏感度权衡。
- 处理逻辑保持幂态、最小化缓冲，poll 到的记录尽快处理完并提交，不要在应用内再堆长队列。
- 把“收到 `TopicAuthorizationFailedError`”接入监控告警与自动遏制钩子，而不是当成普通重试异常；
  注意 kafka-python 不会自动清理消费者，需要应用决定 pause/reassign/close。
- 敏感 topic 考虑 mTLS 短生命周期证书，凭证作废能同时阻断重连，弥补 ACL 传播延迟。

## 5. 一句话边界

> 服务端 ACL 保证“**之后的请求拿不到新数据**”；客户端动作（reassign/close）保证“**已缓冲但未交付
> 的数据不再交付**”；而**已经到达进程内存或已落地的数据，没有任何远端权限修改能追回**，这部分
> 只能靠应用隔离与事件响应流程覆盖。

## 附：演示文件与对应事实

| 文件 | 演示的事实 |
|---|---|
| `fakebroker_util.py` | 共享脚手架：脚本化假 broker、记录批编码、errno 29 响应构造 |
| `01_prefetch_timeline.py` | 预取字节到达、poll 排空、惰性反序列化、撤权后下一次 fetch 被拒的完整时序 |
| `02_containment_actions.py` | pause 保留数据 / reassign 阻断缓冲交付 / close 释放缓冲的实测对比 |
| `03_incident_runbook.py` | 安全事件服务的撤权+遏制流程与残留风险清单（可运行） |
| `04_inflight_revocation.py` | 撤权发生在 FetchRequest 在途期间的两种结局（仍返回数据 / 返回 errno 29） |
