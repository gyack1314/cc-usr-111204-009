"""回放入口：崩溃注入、进程重启语义与从原始事件日志重演。

附件是合成事件与虚拟确认通道，没有在线broker。run_feed按声明顺序喂事件，
CrashPlan命中时抛ProcessExit模拟进程退出；再次调用即新进程：重新打开
SQLite、alias清空，未确认的交换依据持久状态重发确认，正式样本只出现一次。
"""
import copy
import json

from .channel import AckChannel
from .engine import CRASH_POINTS, InboundReceiver, ProcessExit
from .store import Store


class CrashPlan:
    """崩溃计划：可在若干(事件索引, 边界)上各注入一次进程退出。

    构造参数为若干(event_index, point)元组；同一事件可在不同边界各触发一次。
    每个点只触发一次，因此重启后重放同一事件不会无限崩溃。
    """

    def __init__(self, *targets):
        if not targets:
            raise ValueError('至少需要一个崩溃点')
        self.targets = []
        for target in targets:
            event_index, point = target
            if point not in CRASH_POINTS:
                raise ValueError(f'未知崩溃边界: {point!r}')
            if type(event_index) is not int or event_index < 0:
                raise ValueError('event_index必须为非负整数')
            self.targets.append({'event_index': event_index,
                                 'point': point, 'fired': False})

    def hooks(self, position):
        for t in self.targets:
            if not t['fired'] and t['event_index'] == position:
                return {t['point']: True}
        return {}

    def mark_fired(self, position, point):
        for t in self.targets:
            if (not t['fired'] and t['event_index'] == position
                    and t['point'] == point):
                t['fired'] = True
                return
        raise RuntimeError(f'未登记的崩溃触发: {position} {point}')

    @property
    def all_fired(self):
        return all(t['fired'] for t in self.targets)


def _config(feed):
    return {
        'topic_filters': list(feed['topic_filters']),
        'inflight_limit': feed['inflight_limit'],
        'disk_quota_bytes': feed['disk_quota_bytes'],
        'topic_alias_maximum': feed['topic_alias_maximum'],
        'client_id': feed.get('client_id'),
    }


def run_feed(db_path, feed, crash_plan=None, start_index=0):
    """把整份feed喂入一个（可反复重启的）接收器。

    每次ProcessExit后以全新进程重开数据库继续；返回完整报告。若feed最终
    仍因异常终止，报告中记录error且保留已持久状态。
    """
    cfg = _config(feed)
    store = Store(db_path)
    channel = AckChannel()
    outcomes = []
    crashes = []
    error = None
    position = start_index
    receiver = InboundReceiver(
        store, channel=channel,
        crash_hooks=crash_plan.hooks(position) if crash_plan else None,
        **cfg)
    events = feed['events']
    while position < len(events):
        event = copy.deepcopy(events[position])
        try:
            outcomes.append(receiver.handle(event, position))
            position += 1
        except ProcessExit as exc:
            point = exc.args[0]
            crashes.append({'event_index': position, 'point': point})
            crash_plan.mark_fired(position, point)
            # 新进程：重开持久层（alias随之清空），同一事件重放
            store.close()
            store = Store(db_path)
            channel = AckChannel()
            receiver = InboundReceiver(store, channel=channel, **cfg)
            receiver.crash_hooks = crash_plan.hooks(position)
            continue
        except Exception as exc:  # 容量/协议冲突：停止接收
            error = {'event_index': position,
                     'type': type(exc).__name__, 'message': str(exc)}
            break
        if crash_plan is not None:
            receiver.crash_hooks = crash_plan.hooks(position)
    snapshot = receiver.snapshot()
    store.close()
    return {
        'config': cfg,
        'processed': position,
        'outcomes': outcomes,
        'crashes': crashes,
        'error': error,
        'sample_count': len(snapshot['samples']),
        'samples': snapshot['samples'],
        'exchanges': snapshot['exchanges'],
        'acks': snapshot['acks'],
        'journal': snapshot['journal'],
        'generations': snapshot['generations'],
        'channel': snapshot['channel'],
        'snapshot': snapshot,
    }


def rebuild_from_journal(source_db_path, feed):
    """从持久的raw_journal原始事件重演到全新内存库，核对状态可重建性。

    事件类条目走引擎重新处理，确认意图（PUBREC/PUBCOMP）按原始台账复原，
    因此崩溃前已持久、崩溃后未重发的确认也能精确重建。
    """
    source = Store(source_db_path)
    rows = source.list_journal()
    source.close()
    cfg = _config(feed)
    rebuilt = Store(':memory:')
    receiver = InboundReceiver(rebuilt, channel=AckChannel(),
                               record_acks=False, **cfg)
    for row in rows:
        if row['kind'] in ('PUBREC', 'PUBCOMP'):
            intent = json.loads(row['raw'])
            rebuilt.conn.execute(
                'INSERT INTO ack_ledger(generation,packet_id,kind,reason_code,'
                'exchange_id,event_seq,resend_of) VALUES(?,?,?,?,?,?,?)',
                (row['generation'], intent['packet_id'], row['kind'],
                 intent['reason_code'], intent['exchange_id'],
                 row['event_seq'], intent['resend_of']))
            rebuilt.conn.execute(
                'INSERT INTO raw_journal(event_seq,generation,kind,raw,outcome) '
                'VALUES(?,?,?,?,?)',
                (row['event_seq'], row['generation'], row['kind'],
                 row['raw'], row['outcome']))
            continue
        event = json.loads(row['raw'])
        receiver.handle(event, row['event_seq'])
    snapshot = receiver.snapshot()
    rebuilt.close()
    return snapshot


def assert_durable_state_equal(report_a, report_b):
    """比较两份报告中的持久状态（样本/交换/确认/世代/日志），忽略内存通道。"""
    keys = ('samples', 'exchanges', 'acks', 'generations', 'journal')
    for key in keys:
        if report_a[key] != report_b[key]:
            raise AssertionError(f'重演后持久状态不一致: {key}')


def ack_sequence(report):
    """确认顺序：按ack_id排列的(kind, packet_id, reason_code, resend_of)。"""
    return [(a['kind'], a['packet_id'], a['reason_code'], a['resend_of'])
            for a in report['acks']]


def sample_topics(report):
    return [(s['topic'], bytes(s['payload']).hex(), bool(s['retained']))
            for s in report['samples']]
