"""清点源事件种类和原始payload长度。"""
from collections import Counter
from .io import integer

def summarize(v,root):
    kinds=Counter();payload=0
    for e in v['events']:
        if e['type'] not in ('CONNACK','PUBLISH','PUBREL','DISCONNECT'):raise ValueError('未知源事件')
        kinds[e['type']]+=1
        if e['type'] in ('PUBLISH','PUBREL'):
            pid=integer(e['packet_id'],1)
            if pid>65535:raise ValueError('报文身份越界')
        if e['type']=='PUBLISH':payload+=len(bytes.fromhex(e['payload_hex']))
    return {'event_counts':dict(sorted(kinds.items())),'raw_payload_bytes':payload,'inflight_limit':integer(v['inflight_limit'],1)}
