"""虚拟确认通道：只记录PUBREC/PUBCOMP发送意图，不实际联网。

确认意图先在确认台账持久化（与原始日志同事务，构成一个崩溃边界），再经通道
“发出”（又一崩溃边界）；进程恢复后按台账重发，正式样本与确认顺序均可解释。
"""

class AckChannel:
    def __init__(self):
        self.sent = []

    def send_pubrec(self, packet_id, reason_code):
        self.sent.append({'kind': 'PUBREC', 'packet_id': packet_id,
                          'reason_code': reason_code})

    def send_pubcomp(self, packet_id, reason_code):
        self.sent.append({'kind': 'PUBCOMP', 'packet_id': packet_id,
                          'reason_code': reason_code})

    def transcript(self):
        return list(self.sent)
