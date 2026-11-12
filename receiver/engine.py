"""MQTT5 QoS2入站交付状态机：持久化先于确认，正式样本恰好可见一次。

处理范围仅限配置topic过滤器命中的QoS2 PUBLISH；本模块不做报文编解码、
不操控设备，只消费适配器已解码的事件字典并经虚拟通道记录确认意图。
"""
import json

from .alias import AliasMap
from .channel import AckChannel
from .errors import CapacityReached, ProtocolViolation
from .filters import accepted
from .store import Store, canonical_json

# MQTT原因码
RC_SUCCESS = 0
RC_PACKET_ID_NOT_FOUND = 0x92

# 可注入进程退出的持久化/发送边界
CRASH_POINTS = (
    'before_publish_commit', 'after_publish_commit',
    'before_pubrec_send', 'after_pubrec_send',
    'before_rel_commit', 'after_rel_commit',
    'before_pubcomp_send', 'after_pubcomp_send',
)


class ProcessExit(Exception):
    """模拟进程在边界退出；触发时对应事务已按语义提交或回滚。"""


class InboundReceiver:
    def __init__(self, store, *, topic_filters, inflight_limit,
                 disk_quota_bytes, topic_alias_maximum,
                 channel=None, crash_hooks=None, client_id=None,
                 record_acks=True):
        self.store = store
        self.filters = list(topic_filters)
        self.inflight_limit = self._positive(inflight_limit)
        self.disk_quota_bytes = self._non_negative(disk_quota_bytes)
        self.alias = AliasMap(topic_alias_maximum)
        self.channel = channel or AckChannel()
        self.crash_hooks = dict(crash_hooks or {})
        # 日志重建模式下确认完全由raw_journal复原，事件重放不再写确认台账
        self.record_acks = record_acks
        unknown = set(self.crash_hooks) - set(CRASH_POINTS)
        if unknown:
            raise ValueError(f'未知崩溃边界: {sorted(unknown)}')
        self.client_id = client_id
        gen = store.active_generation()
        self.generation = gen['generation'] if gen else None

    @staticmethod
    def _positive(x):
        if type(x) is not int or x < 1:
            raise ValueError('上限必须为正整数')
        return x

    @staticmethod
    def _non_negative(x):
        if type(x) is not int or x < 0:
            raise ValueError('配额必须为非负整数')
        return x

    def _exit(self, point):
        if point in self.crash_hooks:
            raise ProcessExit(point)

    def _require_generation(self):
        if self.generation is None:
            raise ProtocolViolation('首个事件前未收到CONNACK，无活动世代')
        return self.generation

    # ---- 事件入口 ----
    def handle(self, event, event_seq):
        kind = event.get('type')
        if kind == 'CONNACK':
            return self._on_connack(event, event_seq)
        if kind == 'DISCONNECT':
            return self._on_disconnect(event, event_seq)
        if kind == 'PUBLISH':
            return self._on_publish(event, event_seq)
        if kind == 'PUBREL':
            return self._on_pubrel(event, event_seq)
        raise ValueError(f'未知源事件: {kind!r}')

    # ---- 连接生命周期 ----
    def _on_connack(self, event, seq):
        if 'session_present' not in event or not isinstance(
                event['session_present'], bool):
            raise ValueError('CONNACK必须携带布尔session_present')
        self.alias.reset()  # 重连必须清空连接内alias映射
        sp = event['session_present']
        s = self.store
        if sp:
            current = s.active_generation()
            if current is None:
                raise ProtocolViolation(
                    'Session Present为真但本地无活动服务端会话可续接')
            self.generation = current['generation']
            outcome = 'resumed'
        else:
            # 为假：开启新世代；旧在途交换保留为“已失去服务端会话”的未完成记录，
            # 不向新会话补发任何旧确认。
            with s.transaction() as conn:
                if self.generation is not None:
                    s.mark_generation_lost(conn, self.generation)
                new_gen = s.next_generation()
                s.open_generation(conn, new_gen, self.client_id or '',
                                  seq, False)
                s.append_journal(conn, seq, new_gen, 'CONNACK',
                                 canonical_json(event), 'new_generation')
            self.generation = new_gen
            return {'event_seq': seq, 'type': 'CONNACK',
                    'generation': new_gen, 'outcome': 'new_generation'}
        with s.transaction() as conn:
            s.append_journal(conn, seq, self.generation, 'CONNACK',
                             canonical_json(event), outcome)
        return {'event_seq': seq, 'type': 'CONNACK',
                'generation': self.generation, 'outcome': outcome}

    def _on_disconnect(self, event, seq):
        gen = self._require_generation()
        self.alias.reset()  # alias只属于一条连接
        with self.store.transaction() as conn:
            self.store.append_journal(conn, seq, gen, 'DISCONNECT',
                                      canonical_json(event), 'disconnected')
        return {'event_seq': seq, 'type': 'DISCONNECT',
                'generation': gen, 'outcome': 'disconnected'}

    # ---- PUBLISH ----
    def _parse_publish(self, event):
        pid = event.get('packet_id')
        if type(pid) is not int or not (1 <= pid <= 65535):
            raise ProtocolViolation(f'packet_id越界: {pid!r}')
        if event.get('qos') != 2:
            raise ValueError('采集范围只接受QoS 2消息')
        if not isinstance(event.get('dup'), bool):
            raise ValueError('dup必须为布尔')
        if not isinstance(event.get('retained'), bool):
            raise ValueError('retained必须为布尔')
        properties = event.get('properties')
        if not isinstance(properties, dict):
            raise ValueError('properties必须为对象')
        payload = bytes.fromhex(event['payload_hex'])
        alias = properties.get('topic_alias')
        if alias is not None and (type(alias) is not int or alias < 0):
            raise ProtocolViolation('topic_alias必须为非负整数')
        return pid, event['dup'], event.get('topic', ''), payload, \
            event['retained'], properties, alias

    def _on_publish(self, event, seq):
        gen = self._require_generation()
        pid, dup, raw_topic, payload, retained, properties, alias = \
            self._parse_publish(event)
        # 先解析连接内alias得到完整topic，再做过滤器判定
        topic = self.alias.resolve(raw_topic, alias)
        if not accepted(self.filters, topic):
            with self.store.transaction() as conn:
                self.store.append_journal(
                    conn, seq, gen, 'PUBLISH', canonical_json(event),
                    'ignored_filter')
            return {'event_seq': seq, 'type': 'PUBLISH', 'packet_id': pid,
                    'generation': gen, 'topic': topic,
                    'outcome': 'ignored_filter'}

        s = self.store
        # 事务段：所有持久化在COMMIT前完成，COMMIT前崩溃=整段回滚
        with s.transaction() as conn:
            active = s.find_active(conn, gen, pid)
            if active is not None:
                exchange_id, outcome = self._dup_publish_tx(
                    conn, seq, gen, pid, topic, payload, retained,
                    properties, event, active)
            else:
                if dup:
                    # 对已不存在交换的重传，是协议冲突
                    raise ProtocolViolation(
                        f'重复PUBLISH(packet_id={pid})无对应在途交换')
                # 容量判定在任何确认之前：达上限即停止接收，不先确认再丢样本
                if s.inflight_count(gen) >= self.inflight_limit:
                    raise CapacityReached(
                        f'入站在途交换已达上限 {self.inflight_limit}')
                if (s.stored_payload_bytes() + len(payload)
                        > self.disk_quota_bytes):
                    raise CapacityReached('磁盘配额不足，无法持久化新样本')
                identity = {'client_id': self.client_id,
                            'generation': gen,
                            'received_event_seq': seq}
                exchange_id = s.insert_exchange(
                    conn, gen, pid, topic, payload, retained,
                    canonical_json(properties), canonical_json(identity), seq)
                s.append_journal(conn, seq, gen, 'PUBLISH',
                                 canonical_json(event), 'stored')
                outcome = 'stored'
            self._exit('before_publish_commit')
        # 事务已提交：以下崩溃只影响确认，持久样本不会重复
        self._exit('after_publish_commit')
        # 先把确认意图持久化（与原始日志同事务），再经通道发出；
        # 崩溃在发送前则恢复后按台账重发，正式样本不受影响。
        if self.record_acks:
            last = s.last_ack(gen, pid, 'PUBREC')
            is_resend = outcome == 'duplicate_publish'
            s.record_ack(gen, pid, 'PUBREC', RC_SUCCESS, seq,
                         exchange_id=exchange_id,
                         resend_of=last['ack_id']
                         if is_resend and last else None)
        self._exit('before_pubrec_send')
        self.channel.send_pubrec(pid, RC_SUCCESS)
        self._exit('after_pubrec_send')
        return {'event_seq': seq, 'type': 'PUBLISH', 'packet_id': pid,
                'generation': gen, 'exchange_id': exchange_id,
                'topic': topic, 'outcome': outcome}

    def _dup_publish_tx(self, conn, seq, gen, pid, topic, payload,
                        retained, properties, event, active):
        # 同一在途ID的重复PUBLISH：载荷不同是协议冲突，绝不覆盖。
        # alias编码变化本身不算冲突，比较时剔除topic_alias。
        business = {k: v for k, v in properties.items()
                    if k != 'topic_alias'}
        stored = {k: v for k, v in
                  json.loads(active['properties']).items()
                  if k != 'topic_alias'}
        same = (topic == active['topic']
                and payload == bytes(active['payload'])
                and bool(retained) == bool(active['retained'])
                and business == stored)
        if not same:
            raise ProtocolViolation(
                f'packet_id={pid}重复PUBLISH载荷与首次不一致，拒绝覆盖')
        self.store.append_journal(conn, seq, gen, 'PUBLISH',
                                  canonical_json(event),
                                  'duplicate_publish')
        return active['exchange_id'], 'duplicate_publish'

    # ---- PUBREL ----
    def _on_pubrel(self, event, seq):
        gen = self._require_generation()
        pid = event.get('packet_id')
        if type(pid) is not int or not (1 <= pid <= 65535):
            raise ProtocolViolation(f'packet_id越界: {pid!r}')
        s = self.store
        with s.transaction() as conn:
            active = s.find_active(conn, gen, pid)
            if active is None:
                prior = s.find_any(conn, gen, pid)
                if prior is not None and prior['state'] == 'completed':
                    # 完成记录仍保留：重复PUBREL重发成功PUBCOMP，不重发样本
                    s.append_journal(conn, seq, gen, 'PUBREL',
                                     canonical_json(event),
                                     'duplicate_pubrel')
                    duplicate_completed = True
                else:
                    # 从未见过或已放弃世代（lost）：按约定0x92，不凭空生成样本
                    s.append_journal(conn, seq, gen, 'PUBREL',
                                     canonical_json(event), 'orphan_pubrel')
                    duplicate_completed = False
            else:
                # 一个本地事务：样本正式可见且交换推进到completed，原子发生
                s.mark_completed_and_publish_sample(conn, active, seq)
                s.append_journal(conn, seq, gen, 'PUBREL',
                                 canonical_json(event), 'released')
                duplicate_completed = None
                self._exit('before_rel_commit')
        if active is None:
            if duplicate_completed:
                # 先持久化重发意图，再发送；重复PUBREL不重发样本
                if self.record_acks:
                    last = s.last_ack(gen, pid, 'PUBCOMP')
                    s.record_ack(gen, pid, 'PUBCOMP', RC_SUCCESS, seq,
                                 exchange_id=prior['exchange_id'],
                                 resend_of=last['ack_id'] if last else None)
                self.channel.send_pubcomp(pid, RC_SUCCESS)
                return {'event_seq': seq, 'type': 'PUBREL',
                        'packet_id': pid, 'generation': gen,
                        'exchange_id': prior['exchange_id'],
                        'outcome': 'duplicate_pubrel'}
            # 孤立PUBREL：先持久化0x92意图再发送，绝不凭空生成样本
            if self.record_acks:
                s.record_ack(gen, pid, 'PUBCOMP',
                             RC_PACKET_ID_NOT_FOUND, seq)
            self.channel.send_pubcomp(pid, RC_PACKET_ID_NOT_FOUND)
            return {'event_seq': seq, 'type': 'PUBREL', 'packet_id': pid,
                    'generation': gen, 'outcome': 'orphan_pubrel',
                    'reason_code': RC_PACKET_ID_NOT_FOUND}
        # 事务已提交：以下崩溃只影响确认，正式样本已恰好可见一次
        self._exit('after_rel_commit')
        # 先持久化PUBCOMP意图，再经通道发出；发送前崩溃则恢复后重发
        if self.record_acks:
            last = s.last_ack(gen, pid, 'PUBCOMP')
            s.record_ack(gen, pid, 'PUBCOMP', RC_SUCCESS, seq,
                         exchange_id=active['exchange_id'],
                         resend_of=last['ack_id'] if last else None)
        self._exit('before_pubcomp_send')
        self.channel.send_pubcomp(pid, RC_SUCCESS)
        self._exit('after_pubcomp_send')
        return {'event_seq': seq, 'type': 'PUBREL', 'packet_id': pid,
                'generation': gen, 'exchange_id': active['exchange_id'],
                'outcome': 'released'}

    # ---- 只读盘点 ----
    def snapshot(self):
        return {
            'generation': self.generation,
            'inflight': self.store.inflight_count(self.generation)
            if self.generation is not None else 0,
            'stored_payload_bytes': self.store.stored_payload_bytes(),
            'samples': self.store.list_samples(),
            'exchanges': self.store.list_exchanges(),
            'acks': self.store.list_acks(),
            'journal': self.store.list_journal(),
            'generations': self.store.list_generations(),
            'channel': self.channel.transcript(),
        }
