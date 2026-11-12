"""QoS2入站持久状态机的端到端语义测试。"""
import tempfile
import unittest
from pathlib import Path

from receiver.engine import (RC_PACKET_ID_NOT_FOUND, RC_SUCCESS, ProcessExit)
from receiver.errors import CapacityReached, ProtocolViolation
from receiver.replay import (CrashPlan, ack_sequence,
                             assert_durable_state_equal, rebuild_from_journal,
                             run_feed)

FILTERS = ['plant/+/pressure']


def pub(pid, topic, payload, *, dup=False, retained=False, alias=None):
    props = {} if alias is None else {'topic_alias': alias}
    return {'type': 'PUBLISH', 'packet_id': pid, 'qos': 2, 'dup': dup,
            'topic': topic, 'payload_hex': payload, 'retained': retained,
            'properties': props}


def rel(pid):
    return {'type': 'PUBREL', 'packet_id': pid}


def connack(sp):
    return {'type': 'CONNACK', 'session_present': sp}


DISC = {'type': 'DISCONNECT'}


def feed(events, *, inflight=4, quota=100_000, alias_max=8,
         filters=None):
    return {'client_id': 'archive-demo',
            'topic_filters': filters or FILTERS,
            'inflight_limit': inflight, 'disk_quota_bytes': quota,
            'topic_alias_maximum': alias_max, 'events': events}


class ReceiverTestBase(unittest.TestCase):
    def run_events(self, events, **kw):
        with tempfile.TemporaryDirectory() as d:
            self.db = str(Path(d) / 'state.sqlite')
            return run_feed(self.db, feed(events, **kw))

    def run_feed_obj(self, f, plan=None):
        with tempfile.TemporaryDirectory() as d:
            self.db = str(Path(d) / 'state.sqlite')
            return run_feed(self.db, f, crash_plan=plan)


class HappyPathTests(ReceiverTestBase):
    def test_full_qos2_one_sample_and_ack_order(self):
        r = self.run_events([connack(False), pub(10, 'plant/A/pressure', '0a'),
                      rel(10)])
        self.assertEqual(r['sample_count'], 1)
        self.assertEqual(r['error'], None)
        self.assertEqual(ack_sequence(r), [
            ('PUBREC', 10, RC_SUCCESS, None),
            ('PUBCOMP', 10, RC_SUCCESS, None)])
        s = r['samples'][0]
        self.assertEqual(bytes(s['payload']), b'\n')
        self.assertEqual(s['topic'], 'plant/A/pressure')

    def test_retained_flag_and_full_properties_persisted(self):
        ev = pub(10, 'plant/A/pressure', '0a', retained=True, alias=2)
        ev['properties']['user_property'] = ['unit', 'bar']
        # 正式可见发生在PUBREL事务；PUBLISH阶段先持久为未完成交换
        r = self.run_events([connack(False), ev, rel(10)])
        import json
        s = r['samples'][0]
        self.assertTrue(s['retained'])
        self.assertEqual(json.loads(s['properties']),
                         {'topic_alias': 2, 'user_property': ['unit', 'bar']})
        # 保存的是解析后完整topic，而非连接内alias编号
        self.assertEqual(s['topic'], 'plant/A/pressure')
        self.assertNotIn('2', s['topic'])

    def test_retained_is_not_topic_snapshot(self):
        # 两条不同topic的retained消息各自独立保留，不互相补全快照
        r = self.run_events([connack(False),
                      pub(1, 'plant/A/pressure', '01', retained=True),
                      rel(1),
                      pub(2, 'plant/B/pressure', '02', retained=True),
                      rel(2)])
        self.assertEqual(r['sample_count'], 2)
        self.assertEqual([s['topic'] for s in r['samples']],
                         ['plant/A/pressure', 'plant/B/pressure'])


class DuplicatePublishTests(ReceiverTestBase):
    def test_duplicate_publish_resends_pubrec_no_second_sample(self):
        r = self.run_events([connack(False),
                      pub(7, 'plant/A/pressure', '31322e35', retained=True,
                          alias=1),
                      pub(7, '', '31322e35', dup=True, retained=True,
                          alias=1),
                      rel(7)])
        self.assertEqual(r['sample_count'], 1)
        acks = ack_sequence(r)
        self.assertEqual([a[0] for a in acks],
                         ['PUBREC', 'PUBREC', 'PUBCOMP'])
        # 第二条PUBREC是重发，链接到首条
        self.assertEqual(acks[1][3], acks[0] and 1)
        self.assertTrue(all(a[2] == RC_SUCCESS for a in acks))

    def test_payload_conflict_is_protocol_violation_no_overwrite(self):
        with tempfile.TemporaryDirectory() as d:
            db = str(Path(d) / 's.sqlite')
            r = run_feed(db, feed([
                connack(False),
                pub(7, 'plant/A/pressure', '01'),
                pub(7, 'plant/A/pressure', '02', dup=True)]))
            self.assertIsInstance(r['error'], dict)
            self.assertEqual(r['error']['type'], 'ProtocolViolation')
            # 冲突前只有首条落盘，且未被覆盖
            ex = r['exchanges']
            self.assertEqual(len(ex), 1)
            self.assertEqual(bytes(ex[0]['payload']), b'\x01')
            self.assertEqual(r['sample_count'], 0)

    def test_alias_encoding_change_is_not_conflict(self):
        # 完整topic与载荷一致，仅alias编号变化不算冲突
        r = self.run_events([connack(False),
                      pub(7, 'plant/A/pressure', '01', alias=1),
                      pub(7, 'plant/A/pressure', '01', dup=True, alias=2)])
        self.assertIsNone(r['error'])
        self.assertEqual(r['outcomes'][-1]['outcome'], 'duplicate_publish')

    def test_dup_without_active_exchange_violates(self):
        r = self.run_events([connack(False),
                      pub(7, 'plant/A/pressure', '01', dup=True)])
        self.assertEqual(r['error']['type'], 'ProtocolViolation')


class PacketIdReuseTests(ReceiverTestBase):
    def test_id_reused_after_completion_is_new_exchange(self):
        r = self.run_events([connack(False),
                      pub(7, 'plant/A/pressure', '01'), rel(7),
                      pub(7, 'plant/A/pressure', '02'), rel(7)])
        self.assertEqual(r['sample_count'], 2)
        self.assertEqual([e['state'] for e in r['exchanges']],
                         ['completed', 'completed'])
        self.assertEqual([e['exchange_id'] for e in r['exchanges']], [1, 2])
        # 新交换的确认不是重发（resend_of为None）
        acks = ack_sequence(r)
        self.assertEqual(acks[2], ('PUBREC', 7, RC_SUCCESS, None))

    def test_identical_payload_different_exchanges_both_recorded(self):
        # 协议一次交换不证明同payload是同一业务事件：不做跨交换去重
        r = self.run_events([connack(False),
                      pub(7, 'plant/A/pressure', 'ab'), rel(7),
                      pub(8, 'plant/A/pressure', 'ab'), rel(8)])
        self.assertEqual(r['sample_count'], 2)
        self.assertEqual(len({e['exchange_id'] for e in r['exchanges']}), 2)


class PubrelTests(ReceiverTestBase):
    def test_duplicate_pubrel_no_second_pubcomp_sample(self):
        r = self.run_events([connack(False),
                      pub(7, 'plant/A/pressure', '01'), rel(7), rel(7)])
        self.assertEqual(r['sample_count'], 1)
        comps = [a for a in r['acks'] if a['kind'] == 'PUBCOMP']
        self.assertEqual(len(comps), 2)
        self.assertEqual(comps[1]['resend_of'], comps[0]['ack_id'])
        self.assertEqual(r['outcomes'][-1]['outcome'], 'duplicate_pubrel')

    def test_orphan_pubrel_returns_identifier_not_found(self):
        r = self.run_events([connack(False), rel(99)])
        self.assertEqual(r['sample_count'], 0)
        ack = r['acks'][-1]
        self.assertEqual(ack['kind'], 'PUBCOMP')
        self.assertEqual(ack['reason_code'], RC_PACKET_ID_NOT_FOUND)
        self.assertEqual(r['outcomes'][-1]['outcome'], 'orphan_pubrel')


class SessionTests(ReceiverTestBase):
    def test_session_present_resumes_generation(self):
        r = self.run_events([connack(False),
                      pub(7, 'plant/A/pressure', '01'),
                      DISC, connack(True), rel(7)])
        gens = r['generations']
        self.assertEqual(len(gens), 1)
        self.assertEqual(gens[0]['status'], 'active')
        self.assertEqual(r['sample_count'], 1)

    def test_session_absent_opens_generation_and_keeps_lost_exchange(self):
        r = self.run_events([connack(False),
                      pub(7, 'plant/A/pressure', '01'),  # 未完成
                      DISC, connack(False),
                      pub(7, 'plant/A/pressure', '02'), rel(7)])
        gens = r['generations']
        self.assertEqual([g['status'] for g in gens], ['lost', 'active'])
        # 旧交换保留为lost，可审阅但不继续、不补确认
        old, new = r['exchanges']
        self.assertEqual(old['state'], 'lost')
        self.assertEqual(new['state'], 'completed')
        self.assertEqual(r['sample_count'], 1)
        # 新世代的PUBREC/PUBCOMP不属于旧交换，无跨世代重发链
        self.assertTrue(all(a['generation'] == 2
                            for a in r['acks']
                            if a['exchange_id'] == new['exchange_id']))

    def test_orphan_pubrel_in_new_generation_after_loss(self):
        r = self.run_events([connack(False),
                      pub(7, 'plant/A/pressure', '01'),
                      DISC, connack(False), rel(7)])
        self.assertEqual(r['outcomes'][-1]['outcome'], 'orphan_pubrel')
        self.assertEqual(r['acks'][-1]['reason_code'],
                         RC_PACKET_ID_NOT_FOUND)
        self.assertEqual(r['sample_count'], 0)

    def test_session_present_without_local_session_violates(self):
        r = self.run_events([connack(True)])
        self.assertEqual(r['error']['type'], 'ProtocolViolation')


class AliasConnectionTests(ReceiverTestBase):
    def test_alias_cleared_on_reconnect(self):
        r = self.run_events([connack(False),
                      pub(1, 'plant/A/pressure', '01', alias=1), rel(1),
                      DISC, connack(True),
                      pub(2, '', '02', alias=1)])  # 重连后旧映射应失效
        self.assertEqual(r['error']['type'], 'ProtocolViolation')

    def test_alias_rebuilt_within_new_connection(self):
        r = self.run_events([connack(False),
                      pub(1, 'plant/A/pressure', '01', alias=1), rel(1),
                      DISC, connack(True),
                      pub(2, 'plant/B/pressure', '02', alias=1),
                      pub(3, '', '03', alias=1), rel(2), rel(3)])
        self.assertIsNone(r['error'])
        self.assertEqual(r['sample_count'], 3)

    def test_alias_out_of_range_rejected(self):
        r = self.run_events([connack(False),
                      pub(1, 'plant/A/pressure', '01', alias=9)],
                     alias_max=8)
        self.assertEqual(r['error']['type'], 'ProtocolViolation')


class FilterAndCapacityTests(ReceiverTestBase):
    def test_non_qos2_message_rejected(self):
        ev = pub(1, 'plant/A/pressure', '01')
        ev['qos'] = 1
        r = self.run_events([connack(False), ev])
        self.assertIsNotNone(r['error'])
        self.assertEqual(r['sample_count'], 0)
        self.assertEqual(r['acks'], [])

    def test_non_matching_topic_ignored_no_ack(self):
        r = self.run_events([connack(False),
                      pub(1, 'other/zone/pressure', '01')])
        self.assertEqual(r['sample_count'], 0)
        self.assertEqual(r['acks'], [])
        self.assertEqual(r['outcomes'][-1]['outcome'], 'ignored_filter')

    def test_inflight_limit_blocks_before_ack(self):
        events = [connack(False),
                  pub(1, 'plant/A/pressure', '01'),
                  pub(2, 'plant/A/pressure', '02')]
        r = self.run_events(events, inflight=1)
        self.assertEqual(r['error']['type'], 'CapacityReached')
        # 第二条既未确认也未落盘
        self.assertEqual(len(r['exchanges']), 1)
        self.assertEqual([a['packet_id'] for a in r['acks']], [1])

    def test_disk_quota_blocks_before_ack(self):
        events = [connack(False),
                  pub(1, 'plant/A/pressure', '0102'),
                  pub(2, 'plant/A/pressure', '0304')]
        r = self.run_events(events, quota=2)
        self.assertEqual(r['error']['type'], 'CapacityReached')
        self.assertEqual(len(r['exchanges']), 1)


class CrashRecoveryTests(ReceiverTestBase):
    def _feed(self):
        return feed([connack(False),
                     pub(7, 'plant/A/pressure', '31322e35', retained=True,
                         alias=1),
                     pub(7, '', '31322e35', dup=True, retained=True,
                         alias=1),
                     rel(7), rel(7),
                     DISC, connack(True),
                     pub(7, 'plant/A/pressure', '31332e30'), rel(7)])

    def test_every_boundary_keeps_single_samples_and_rebuilds(self):
        from receiver.engine import CRASH_POINTS
        targets = [(1, 'before_publish_commit'),
                   (1, 'after_publish_commit'),
                   (1, 'before_pubrec_send'),
                   (1, 'after_pubrec_send'),
                   (3, 'before_rel_commit'),
                   (3, 'after_rel_commit'),
                   (3, 'before_pubcomp_send'),
                   (3, 'after_pubcomp_send')]
        self.assertEqual({p for _, p in targets}, set(CRASH_POINTS))
        for idx, point in targets:
            with self.subTest(point=point):
                with tempfile.TemporaryDirectory() as d:
                    db = str(Path(d) / 's.sqlite')
                    r = run_feed(db, self._feed(),
                                 crash_plan=CrashPlan((idx, point)))
                    self.assertIsNone(r['error'])
                    self.assertEqual(len(r['crashes']), 1)
                    self.assertEqual(r['sample_count'], 2)
                    rebuilt = rebuild_from_journal(db, self._feed())
                    assert_durable_state_equal(
                        r, {'samples': rebuilt['samples'],
                            'exchanges': rebuilt['exchanges'],
                            'acks': rebuilt['acks'],
                            'journal': rebuilt['journal'],
                            'generations': rebuilt['generations']})

    def test_crash_before_publish_commit_leaves_no_trace(self):
        with tempfile.TemporaryDirectory() as d:
            db = str(Path(d) / 's.sqlite')
            r = run_feed(db, self._feed(),
                         crash_plan=CrashPlan((1, 'before_publish_commit')))
            # 回滚后重放：只有一次正式存储，无重复交换
            self.assertEqual(
                sum(1 for e in r['exchanges'] if e['state'] != 'lost'), 2)
            self.assertEqual(r['sample_count'], 2)

    def test_crash_after_pubrel_commit_resends_pubcomp_once_sample(self):
        with tempfile.TemporaryDirectory() as d:
            db = str(Path(d) / 's.sqlite')
            r = run_feed(db, self._feed(),
                         crash_plan=CrashPlan((3, 'after_rel_commit')))
            self.assertEqual(r['sample_count'], 2)
            # 首个PUBCOMP经历重发，台账可见resend链
            comps = [a for a in r['acks'] if a['kind'] == 'PUBCOMP']
            self.assertTrue(any(a['resend_of'] is not None for a in comps))

    def test_multiple_restarts_across_feed_still_exactly_once(self):
        # 同一feed在PUBREC前与PUBCOMP前各退出一次，两次重启后样本仍各一份
        with tempfile.TemporaryDirectory() as d:
            db = str(Path(d) / 's.sqlite')
            plan = CrashPlan((1, 'before_pubrec_send'),
                             (3, 'before_pubcomp_send'))
            r = run_feed(db, self._feed(), crash_plan=plan)
            self.assertIsNone(r['error'])
            self.assertEqual(len(r['crashes']), 2)
            self.assertTrue(plan.all_fired)
            self.assertEqual(r['sample_count'], 2)
            rebuilt = rebuild_from_journal(db, self._feed())
            assert_durable_state_equal(
                r, {'samples': rebuilt['samples'],
                    'exchanges': rebuilt['exchanges'],
                    'acks': rebuilt['acks'],
                    'journal': rebuilt['journal'],
                    'generations': rebuilt['generations']})


if __name__ == '__main__':
    unittest.main()
