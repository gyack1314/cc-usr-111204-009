"""命令行：对合成事件资料执行QoS2持久回放、崩溃注入与状态重建。

资料为受信任的项目内合成文件；读取仍走source_data.io的严格JSON解析。
不联网、不编码报文、不操控设备。
"""
import argparse
import json
import tempfile
from pathlib import Path

from source_data.io import load

from .replay import (ack_sequence, rebuild_from_journal, run_feed,
                     sample_topics)


def _print(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def _summary(report):
    return {
        'processed': report['processed'],
        'sample_count': report['sample_count'],
        'crashes': report['crashes'],
        'error': report['error'],
        'samples': sample_topics(report),
        'ack_sequence': ack_sequence(report),
        'generation_status': [
            (g['generation'], g['status']) for g in report['generations']],
        'exchange_states': [
            (e['exchange_id'], e['generation'], e['packet_id'], e['state'])
            for e in report['exchanges']],
    }


def main(argv=None):
    p = argparse.ArgumentParser(
        description='MQTT5 QoS2入站持久收集的离线回放')
    p.add_argument('command', choices=('replay', 'rebuild'))
    p.add_argument('file', help='合成事件资料JSON路径')
    p.add_argument('--db', help='SQLite路径，缺省使用临时文件')
    p.add_argument('--crash-event', type=int,
                   help='在该事件索引（0基）上注入进程退出')
    p.add_argument('--crash-point',
                   help='崩溃边界，如before_publish_commit')
    args = p.parse_args(argv)

    feed, digest = load(args.file)
    crash_plan = None
    if args.crash_event is not None or args.crash_point is not None:
        if args.crash_event is None or args.crash_point is None:
            p.error('--crash-event与--crash-point必须同时给出')
        from .replay import CrashPlan
        crash_plan = CrashPlan((args.crash_event, args.crash_point))

    tmp = None
    if args.db:
        db_path = args.db
    else:
        tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        tmp.close()
        db_path = tmp.name
    try:
        report = run_feed(db_path, feed, crash_plan=crash_plan)
        result = {'source_sha256': digest, 'report': _summary(report)}
        if args.command == 'rebuild':
            rebuilt = rebuild_from_journal(db_path, feed)
            rebuilt_report = {'samples': rebuilt['samples'],
                              'exchanges': rebuilt['exchanges'],
                              'acks': rebuilt['acks'],
                              'journal': rebuilt['journal'],
                              'generations': rebuilt['generations']}
            for key, value in rebuilt_report.items():
                if value != report[key]:
                    raise SystemExit(f'日志重建与主库状态不一致: {key}')
            result['rebuild'] = 'durable_state_identical'
        _print(result)
        if report['error']:
            raise SystemExit(2)
    finally:
        if tmp is not None:
            Path(tmp.name).unlink(missing_ok=True)


if __name__ == '__main__':
    main()
