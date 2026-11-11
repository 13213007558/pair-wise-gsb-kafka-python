# kafka-python `check_crcs` 拒绝顺序审计报告

日期：2026-11-11　对象：工作区 kafka-python 3.1.0.dev　配置：`check_crcs=True`（默认值，见 `kafka/consumer/fetcher.py:92`）

## 1. 结论

**是。`check_crcs=True` 时，kafka-python 对 CRC 损坏记录的拒绝严格发生在业务反序列化器之前。** 34 个独立构造的线格式用例全部符合预期：没有任何一个 CRC 损坏的记录触发过反序列化器调用（副作用计数为 0）。但有三类边界必须注意：

1. **保证是"按记录"而非"按 fetch"的**：legacy 格式（含 legacy gzip 内层）中，排在损坏记录**之前**的完好记录会先被正常反序列化（副作用会触发），随后才抛出 `CorruptRecordError`。v2 格式则是整批原子拒绝，批内任何损坏都不会让任何记录到达反序列化器。
2. **CRC 覆盖区外的字段（offset、length、leader epoch 等）CRC 本来就检测不到**（见第 4 节）。
3. **CRC 是无密钥校验和，不是认证码**：篡改者重算 CRC 后消息被照常接受（用例 A1/A2 实证，见第 5 节）。

## 2. 验证方法与独立性

- 线格式样例全部由 `audit_crc/wire_fixtures.py` 按 Kafka 协议规范用 `struct`/`gzip` 手工打包，**未使用被测的 `MemoryRecordsBuilder` / `LegacyRecordBatchBuilder` / `DefaultRecordBatchBuilder`**。
- 预期 CRC 由模块内独立实现的查表法 CRC-32/ISO-HDLC（poly `0xEDB88320`）与 CRC-32C/Castagnoli（poly `0x82F63B78`）计算，仅用公开校验向量（`"123456789"` → `0xCBF43926` / `0xE3069283`）和 50 组伪随机输入与 `zlib.crc32`、kafka 的 `calc_crc32c` 交叉验证过实现正确性；所有反例的答案均不来自被测 builder。
- 执行路径是消费者真实路径：`Fetcher.PartitionRecords` + `take()`（与 `Fetcher._append` 的调用方式一致，`kafka/consumer/fetcher.py:658`），反序列化器为带副作用（审计写）的探针，四个 `validate_crc` 方法被插桩记录调用序列。
- 原始结果：`audit_crc/audit_results.json`；复现：`python3 audit_crc/run_audit.py`。

## 3. 调用顺序证据

代码顺序（`kafka/consumer/fetcher.py` 的 `PartitionRecords._unpack_records`）：

- 批级 CRC：`fetcher.py:1671` `if self.check_crcs and not batch.validate_crc(): raise CorruptRecordError` —— 在进入任何记录迭代之前。
- 记录级 CRC：`fetcher.py:1709` `if self.check_crcs and not record.validate_crc(): raise CorruptRecordError` —— 在 `fetcher.py:1715-1716` 的 `_deserialize(...)` 之前。
- v2 的 `DefaultRecord.validate_crc()` 恒为 True（`kafka/record/default_records.py:431`），因为批级 CRC32C 已覆盖全部记录；legacy 的记录级校验对压缩批的内层消息逐条进行（`kafka/record/legacy_records.py:364`）。

运行时事件日志（插桩实测，完整数据见 JSON）：

```
I1 legacy-v0 value 字节翻转（CRC 覆盖区内）:
  0. validate_crc LegacyRecordBatch -> False
  -> CorruptRecordError；deserialize 调用 0 次

I7 v2 记录 value 字节翻转（CRC 覆盖区内）:
  0. validate_crc DefaultRecordBatch -> False
  -> CorruptRecordError；deserialize 调用 0 次（批级拒绝，连解压/迭代都未发生）

I11 v2 双记录批、第 2 条损坏:
  0. validate_crc DefaultRecordBatch -> False
  -> CorruptRecordError；deserialize 调用 0 次（整批原子拒绝，第 1 条好记录也未反序列化）

I6 legacy-gzip 内层记录 2 CRC 损坏、外层 CRC 正确:
  0. validate_crc LegacyRecordBatch -> True   (外层 CRC 覆盖压缩字节，通过)
  1. validate_crc LegacyRecord -> True        (内层记录 1)
  2. deserialize key 'key-1'                  (副作用发生：好记录)
  3. deserialize value 'value-payload-1'
  4. validate_crc LegacyRecord -> False       (内层记录 2，在反序列化之前拦截)
  -> CorruptRecordError；损坏记录本身的 deserialize 调用 0 次

M1 legacy messageset [好, 坏]:
  0. validate_crc LegacyRecordBatch -> True
  1. validate_crc LegacyRecord -> True
  2. deserialize key/value (好记录，副作用发生)
  4. validate_crc LegacyRecordBatch -> False
  -> CorruptRecordError

C1 对照组：同样的损坏 + check_crcs=False:
  0. deserialize key 'key-1'
  1. deserialize value 'value-payload-�'   (损坏数据直接进入反序列化器)
  -> 无错误 —— 证明拦截能力完全来自 check_crcs
```

## 4. 覆盖矩阵（34/34 通过）

图例：deser = 反序列化器调用次数；结果列为实测行为。

### 正对照（独立构造的合法样本必须被正常解析）

| 用例 | 样本 | 结果 | deser |
|---|---|---|---|
| P1 | legacy v0 合法 | 接受，1 条 | 2 |
| P2 | legacy v1 合法 | 接受，1 条 | 2 |
| P3 | legacy v1 gzip（2 条内层） | 接受，2 条 | 4 |
| P4 | v2 合法（2 条，含 headers） | 接受，2 条 | 4 |
| P5 | v2 gzip 合法 | 接受，2 条 | 4 |

### CRC 覆盖区内的修改 —— 全部在反序列化前拒绝（deser=0）

| 用例 | 修改点 | 结果 |
|---|---|---|
| I1 | legacy v0 value 字节 | CorruptRecordError，deser=0 |
| I2 | legacy v1 key 字节 | CorruptRecordError，deser=0 |
| I3 | legacy v1 attributes 字节 | CorruptRecordError，deser=0 |
| I4 | legacy v1 timestamp 字节 | CorruptRecordError，deser=0 |
| I5 | legacy gzip 压缩负载字节 | 外层 CRC 拒绝，**解压前**拦截 |
| I6 | legacy gzip 内层 CRC 坏、外层 CRC 合法 | 内层逐条校验拦截；前序好记录已反序列化（见第 6 节） |
| I7 | v2 记录 value 字节 | 批级 CRC32C 拒绝，deser=0 |
| I8 | v2 attributes 字节 | 批级 CRC32C 拒绝，deser=0 |
| I9 | v2 last_offset_delta 字节 | 批级 CRC32C 拒绝，deser=0 |
| I10 | v2 gzip 压缩负载字节 | 批级 CRC32C 拒绝，**解压前**拦截 |
| I11 | v2 双记录批第 2 条损坏 | 整批原子拒绝，deser=0 |

### CRC 覆盖区外的修改 —— CRC 本来就检测不到，实测行为如下

| 用例 | 修改点 | 实测行为 | 说明 |
|---|---|---|---|
| O1 | legacy offset 字段 0→5 | **静默接受**，记录以 offset=5 消费 | offset 不在 CRC 内 |
| O2 | legacy message_size 减 1 | CorruptRecordError | 间接捕获：切片变短改变了 CRC 输入 |
| O3 | legacy message_size 加 4 | **静默丢弃**，无错误、0 记录 | 声明长度超出缓冲区，framing 直接丢弃 |
| O4 | v2 base_offset 0→5 | **静默接受**，offset 平移为 5/6 | base_offset 不在 CRC32C 内 |
| O5 | v2 leader_epoch 改写 | **静默接受** | leader epoch 不在 CRC32C 内 |
| O6 | v2 batch_length 减 4 | CorruptRecordError | 间接捕获（同 O2 机制） |
| O7 | v2 batch_length 加 4 | **静默丢弃**，无错误 | 同 O3 机制 |
| O8 | v2 magic 字节 2→1 | CorruptRecordError | magic 不在 CRC 内；批次被**重新解释**为 legacy 后偶然校验失败，非 CRC 设计保证 |

### 截断

| 用例 | 场景 | 实测行为 |
|---|---|---|
| T1 | legacy 消息中间截断 | **静默丢弃**，无错误，deser=0 |
| T2 | legacy 截到 <12 字节（LOG_OVERHEAD） | **静默丢弃**，无错误 |
| T3 | legacy 声明 size 使切片 < MIN_SLICE | CorruptRecordError（framing 级，CRC 之前） |
| T4 | v2 批次中间截断 | **静默丢弃**，无错误 |
| T5 | v2 截断记录但同步改小 length | CorruptRecordError（framing 一致后 CRC 失败） |
| T6 | v2 gzip 截断负载并改小 length | CorruptRecordError（解压前 CRC 失败） |

### 篡改 + 重算 CRC（证明 CRC 非认证）

| 用例 | 场景 | 实测行为 |
|---|---|---|
| A1 | v2 value 换为 `FORGED-payload!!` 并重算 CRC32C | **接受**，反序列化器收到伪造数据 |
| A2 | legacy v1 value 换为伪造数据并重算 CRC32 | **接受** |

### 混合与对照

| 用例 | 场景 | 实测行为 |
|---|---|---|
| M1 | legacy messageset [好, 坏] | 好记录反序列化（副作用触发）后抛 CorruptRecordError |
| C1 | 损坏 + `check_crcs=False` | 损坏数据直接进入反序列化器，无错误 |

## 5. 哪些修改 CRC 本来就检测不到

1. **覆盖区外的帧头字段**。legacy：offset（8B）与 message_size（4B）；v2：base_offset（8B）、batch_length（4B）、partition_leader_epoch（4B）、magic（1B）。CRC 分别从 magic 之后（legacy，`legacy_records.py:183-186`）和 attributes 起（v2，`default_records.py:356-363`）计算。其中 offset/base_offset/leader_epoch 的篡改会被**静默接受**（O1/O4/O5）；length 类字段的篡改则表现为间接 CRC 失败或静默丢弃（O2/O3/O6/O7），取决于声明长度是否超出缓冲区。
2. **尾部截断**。声明长度超出实际缓冲区时，`MemoryRecords._cache_next` 直接停止切批（`memory_records.py:81-86`），剩余字节被静默丢弃——无错误、无反序列化调用。这是可用性问题而非完整性问题，但意味着"没报错"不等于"数据完整"。
3. **重算 CRC 的任何篡改**。CRC 是无密钥函数，任何人都能对修改后的数据重算（A1/A2）。
4. **v2 记录级细粒度篡改**：v2 没有逐条记录 CRC（`DefaultRecord.validate_crc` 恒 True），批级 CRC 一旦（被重算）通过，批内任何记录级修改都不再有任何校验兜底。

## 6. 为什么 CRC 通过不能作为消息来源认证

- **无密钥**：CRC-32/CRC-32C 是公开的多项式校验和，设计目标是检测随机比特错误（突发错误、噪声），不是抵抗主动篡改。任何能改数据的人都能用同一算法重算 CRC——用例 A1/A2 实测伪造消息被消费者正常接受并进入反序列化器。
- **线性性**：CRC 是线性函数，攻击者可以构造保持 CRC 不变的定向差分（无需穷举），即使无法重算整个 CRC 的场景也不安全。
- **覆盖不完整**：如第 5 节，帧头字段根本不在 CRC 输入内，offset 平移等攻击完全不触及校验。
- 结论：CRC 只能回答"数据在传输/存储中是否被随机损坏"，不能回答"这条消息是否来自可信生产者"。来源认证需要 Kafka 层的 SASL 认证 + TLS 信道保护，或应用层的 MAC/数字签名（如 HMAC、Ed25519）；对审计场景，应在反序列化器之前或之内验证应用层签名，而不是依赖 `check_crcs`。

## 7. 对带副作用审计反序列化器的注意事项

1. **损坏记录本身不会触发副作用**（check_crcs=True 时，本报告 34 例实证），但**同一 fetch 中排在其前的完好记录会**（I6、M1）。审计副作用必须幂等/可去重（建议以 topic+partition+offset 为键），因为：
   - `CorruptRecordError` 抛出后分区位置不前进，重新拉取会**再次**反序列化前面的好记录；
   - 消费者重平衡、重试也会造成重复投递（Kafka 本就是 at-least-once）。
2. 该保护完全依赖 `check_crcs=True`（默认值）。对照组 C1 证明一旦关闭，损坏字节会原样进入反序列化器。
3. "CRC 通过"不等于"来源可信"（第 6 节）；审计链路的完整性应依赖认证加密/签名，CRC 仅作损坏检测。
4. 静默丢弃（O3/O7/T1/T2/T4）不触发任何错误或回调；如需发现截断/丢批，应监控 offset 连续性（`next_fetch_offset` 与消费进度的缺口），而不是等待异常。
