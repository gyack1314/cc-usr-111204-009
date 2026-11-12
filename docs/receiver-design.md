# QoS2入站持久收集器设计

`receiver`包只消费适配器已解码的 MQTT5 事件（CONNACK/PUBLISH/PUBREL/
DISCONNECT），不实现报文编解码、不联网、不操控设备。确认通过虚拟通道
`AckChannel` 仅记录发送意图。所有持久状态落在一个 SQLite 库中。

## 状态与世代

- `generations`：每次 `session_present=false` 的 CONNACK 开启一个新世代。
  - `session_present=true` 必须本地仍有 `active` 世代才允许续接，否则按
    协议冲突处理。
  - 为假时，旧世代置 `lost`，其未完成交换置 `lost` 并保留可审阅；新会话
    不会补发任何旧确认，旧 packet_id 的孤立 PUBREL 回 `0x92`。
- `exchanges`：按 (世代, packet_id) 跟踪在途交换，状态仅
  `received -> completed`（或 `lost`）。
  - 部分唯一索引保证同一世代同一 packet_id 至多一个 `received` 交换；
    `completed/lost` 不占键，因此完成后的 packet_id 可被新 PUBLISH 合法
    复用，packet_id 不是永久业务去重键。
- `samples`：正式样本表，主键即 `exchange_id`，一个交换至多一条正式记录。
- `ack_ledger`：PUBREC/PUBCOMP 发送意图台账，`resend_of` 链接被重发的
  上一条确认，使确认顺序可解释。
- `raw_journal`：原始事件与确认意图的只增日志，是状态重演的唯一事实源。

## 一次交换的持久化与确认顺序

PUBLISH（命中过滤器且 QoS=2）：

1. 解析连接内 topic alias 得到完整 topic；alias 映射只在内存，重连清空。
2. 在**一个本地事务**内做容量判定（在途上限、磁盘配额）并插入 exchange、
   写原始日志；容量不足直接 `CapacityReached`，停止接收，绝不先确认再丢。
3. 事务提交后，先把 PUBREC 意图写入台账（与日志同事务），再经通道发出。
   - 同一在途 ID 的重复 PUBLISH：topic/payload/retained/业务属性一致才
     接受，仅重发 PUBREC（记 `resend_of`），不产生第二条样本；载荷不同是
     `ProtocolViolation`，不覆盖。比较时剔除 `topic_alias`，连接内编号
     变化不算冲突。

PUBREL：

1. 在**一个本地事务**内把 exchange 置 `completed` 并向 `samples` 插入正式
   样本——“正式可见”与“交换完成”原子发生，故正式样本恰好出现一次。
2. 事务提交后先持久化 PUBCOMP 意图，再经通道发出。
   - 完成记录仍保留时的重复 PUBREL：重发成功 PUBCOMP，不再发布样本。
   - 从未见过或已放弃世代的 ID：回 `0x92`（Packet Identifier not found），
     绝不凭空生成样本。

retained 标志与原始 properties 原样保存；retained 消息只是该 topic 的一条
消息，不构成全部 topic 当前值的完整快照。不同交换即使 payload 相同也各自
独立记录，不做跨交换去重。

## 崩溃边界与恢复

`CRASH_POINTS` 列出八个可注入 `ProcessExit` 的边界：

| 边界 | 崩溃后果 | 恢复行为 |
| --- | --- | --- |
| before_publish_commit | 事务回滚，无落盘痕迹 | 重放 PUBLISH 重新持久化 |
| after_publish_commit | 样本已存为 received，确认未发 | 重发 PUBREC，样本不重复 |
| before/after_pubrec_send | 意图已记账，发送可能未达 | 按台账重发 PUBREC |
| before_rel_commit | 事务回滚，样本未正式可见 | 重放 PUBREL 再原子提交 |
| after_rel_commit | 正式样本已可见，PUBCOMP 未发 | 重发 PUBCOMP，样本不重复 |
| before/after_pubcomp_send | 意图已记账，发送可能未达 | 按台账重发 PUBCOMP |

`replay.run_feed` 在 `ProcessExit` 后以全新进程重开数据库（内存 alias 随之
清空）并重放当前事件；`CrashPlan` 支持在多个边界各注入一次退出。

## 从原始消息重演

`replay.rebuild_from_journal` 把 `raw_journal` 重放到一个全新内存库：
事件类条目走引擎重新推导状态（`record_acks=False`，不再写确认台账），
确认类条目按日志原样复原。重演结果与主库在 samples/exchanges/acks/
generations/journal 五张表上逐表一致，即状态可完全由原始消息解释。
