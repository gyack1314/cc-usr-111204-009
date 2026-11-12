"""接收器的协议与容量异常。"""

class ProtocolViolation(Exception):
    """协议冲突（载荷不一致、alias非法等）：断开当前连接，不覆盖已持久数据。"""

class CapacityReached(Exception):
    """在途上限或磁盘配额耗尽：先停止接收/断连，不得先确认再丢样本。"""
