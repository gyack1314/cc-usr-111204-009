"""连接内Topic Alias临时映射：重连即清空，不进入持久层。"""
from .errors import ProtocolViolation

class AliasMap:
    """只保存在进程内存中的 alias->完整topic 映射。

    持久消息保存解析后的完整topic，alias编号本身从不入库作为业务身份。
    """

    def __init__(self, maximum):
        if type(maximum) is not int or maximum < 0:
            raise ValueError('topic_alias_maximum必须为非负整数')
        self.maximum = maximum
        self._mapping = {}

    def reset(self):
        self._mapping.clear()

    def resolve(self, topic, alias):
        """返回解析后的完整topic，必要时登记/更新映射。

        - 带完整topic且带alias：登记（允许重映射），alias必须在1..maximum。
        - 仅alias无topic：映射必须已存在，否则拒绝；alias超范围拒绝。
        - 无alias：直接使用完整topic。
        """
        if alias is not None:
            if type(alias) is not int or not (1 <= alias <= self.maximum):
                raise ProtocolViolation(
                    f'topic alias {alias!r} 超出范围 1..{self.maximum}')
        if topic:
            if alias is not None:
                self._mapping[alias] = topic
            return topic
        if alias is None:
            raise ProtocolViolation('空topic且未提供topic alias，无法解析')
        if alias not in self._mapping:
            raise ProtocolViolation(f'topic alias {alias} 首次出现但未携带完整topic')
        return self._mapping[alias]
