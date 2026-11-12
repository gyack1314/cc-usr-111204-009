"""topic过滤与连接内alias的单元约定。"""
import unittest

from receiver.alias import AliasMap
from receiver.errors import ProtocolViolation
from receiver.filters import accepted, matches


class FilterTests(unittest.TestCase):
    def test_single_level_wildcard(self):
        self.assertTrue(matches('plant/+/pressure', 'plant/A/pressure'))
        self.assertFalse(matches('plant/+/pressure', 'plant/A/B/pressure'))
        self.assertFalse(matches('plant/+/pressure', 'plant/pressure'))

    def test_multi_level_wildcard(self):
        self.assertTrue(matches('plant/#', 'plant/A/pressure'))
        self.assertTrue(matches('plant/#', 'plant'))
        self.assertFalse(matches('plant/#', 'other/x'))
        with self.assertRaises(ValueError):
            matches('plant/#/x', 'plant/a')

    def test_dollar_system_topic(self):
        self.assertFalse(matches('#', '$SYS/uptime'))
        self.assertFalse(matches('+/x', '$SYS/x'))
        self.assertTrue(matches('$SYS/uptime', '$SYS/uptime'))

    def test_accepted_any_filter(self):
        self.assertTrue(accepted(['plant/+/pressure', 'other/#'],
                                 'other/a/b'))
        self.assertFalse(accepted(['plant/+/pressure'], 'other/a'))


class AliasTests(unittest.TestCase):
    def test_register_then_resolve(self):
        am = AliasMap(8)
        self.assertEqual(am.resolve('plant/A/pressure', 1),
                         'plant/A/pressure')
        # 仅alias无topic：解析为已登记完整topic
        self.assertEqual(am.resolve('', 1), 'plant/A/pressure')
        # 重映射合法（连接内编号语义可变）
        self.assertEqual(am.resolve('plant/B/pressure', 1),
                         'plant/B/pressure')
        self.assertEqual(am.resolve('', 1), 'plant/B/pressure')

    def test_first_use_without_topic_rejected(self):
        am = AliasMap(8)
        with self.assertRaises(ProtocolViolation):
            am.resolve('', 3)

    def test_alias_out_of_range(self):
        am = AliasMap(8)
        with self.assertRaises(ProtocolViolation):
            am.resolve('plant/A/pressure', 9)
        with self.assertRaises(ProtocolViolation):
            am.resolve('plant/A/pressure', 0)

    def test_empty_topic_without_alias_rejected(self):
        am = AliasMap(8)
        with self.assertRaises(ProtocolViolation):
            am.resolve('', None)

    def test_reset_clears_mapping(self):
        am = AliasMap(8)
        am.resolve('plant/A/pressure', 1)
        am.reset()
        with self.assertRaises(ProtocolViolation):
            am.resolve('', 1)

    def test_zero_maximum_rejects_alias(self):
        am = AliasMap(0)
        with self.assertRaises(ProtocolViolation):
            am.resolve('plant/A/pressure', 1)
        self.assertEqual(am.resolve('plant/A/pressure', None),
                         'plant/A/pressure')


if __name__ == '__main__':
    unittest.main()
