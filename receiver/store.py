"""SQLite持久层：世代、在途交换、正式样本、确认台账与原始事件日志。

只做状态存取，所有事务边界由引擎显式控制；isolation_level=None下自行管理
BEGIN IMMEDIATE/COMMIT，任何异常都会回滚，保证“要么完整落盘、要么毫无痕迹”。
"""
import json
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS generations (
  generation INTEGER PRIMARY KEY,
  client_id TEXT NOT NULL,
  opened_event_seq INTEGER NOT NULL,
  session_present INTEGER NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('active','lost'))
);
CREATE TABLE IF NOT EXISTS exchanges (
  exchange_id INTEGER PRIMARY KEY AUTOINCREMENT,
  generation INTEGER NOT NULL,
  packet_id INTEGER NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('received','completed','lost')),
  topic TEXT NOT NULL,
  payload BLOB NOT NULL,
  retained INTEGER NOT NULL,
  properties TEXT NOT NULL,
  received_identity TEXT NOT NULL,
  created_event_seq INTEGER NOT NULL,
  completed_event_seq INTEGER
);
-- 同一世代内一个packet_id只允许一个未完成交换；completed/lost不占键，ID可合法复用。
CREATE UNIQUE INDEX IF NOT EXISTS ux_active_exchange
  ON exchanges(generation, packet_id) WHERE state='received';
CREATE TABLE IF NOT EXISTS samples (
  exchange_id INTEGER PRIMARY KEY REFERENCES exchanges(exchange_id),
  generation INTEGER NOT NULL,
  packet_id INTEGER NOT NULL,
  topic TEXT NOT NULL,
  payload BLOB NOT NULL,
  retained INTEGER NOT NULL,
  properties TEXT NOT NULL,
  received_identity TEXT NOT NULL,
  visible_event_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS ack_ledger (
  ack_id INTEGER PRIMARY KEY AUTOINCREMENT,
  generation INTEGER NOT NULL,
  packet_id INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK(kind IN ('PUBREC','PUBCOMP')),
  reason_code INTEGER NOT NULL,
  exchange_id INTEGER,
  event_seq INTEGER NOT NULL,
  resend_of INTEGER
);
CREATE TABLE IF NOT EXISTS raw_journal (
  journal_id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_seq INTEGER NOT NULL,
  generation INTEGER,
  kind TEXT NOT NULL,
  raw TEXT NOT NULL,
  outcome TEXT NOT NULL
);
"""

class Store:
    def __init__(self, path):
        self.path = path
        self.conn = sqlite3.connect(path, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        if path != ':memory:':
            self.conn.execute('PRAGMA journal_mode=WAL')
            self.conn.execute('PRAGMA synchronous=FULL')
        self.conn.execute('PRAGMA foreign_keys=ON')
        self.conn.execute('PRAGMA temp_store=MEMORY')
        self.init_schema()

    def init_schema(self):
        # executescript自带事务语义（autocommit模式下先提交再执行），
        # 不额外套BEGIN，避免“no transaction is active”。
        self.conn.executescript(SCHEMA)

    def transaction(self):
        return _Transaction(self.conn)

    # ---- 世代 ----
    def open_generation(self, conn, generation, client_id, event_seq, session_present):
        conn.execute(
            'INSERT INTO generations(generation,client_id,opened_event_seq,'
            'session_present,status) VALUES(?,?,?,?,?)',
            (generation, client_id, event_seq, 1 if session_present else 0, 'active'))

    def mark_generation_lost(self, conn, generation):
        conn.execute("UPDATE generations SET status='lost' WHERE generation=?",
                     (generation,))
        conn.execute(
            "UPDATE exchanges SET state='lost' WHERE generation=? "
            "AND state='received'", (generation,))

    def active_generation(self):
        row = self.conn.execute(
            "SELECT * FROM generations WHERE status='active' "
            "ORDER BY generation DESC LIMIT 1").fetchone()
        return dict(row) if row else None

    def next_generation(self):
        row = self.conn.execute(
            'SELECT COALESCE(MAX(generation),0)+1 AS g FROM generations').fetchone()
        return row['g']

    # ---- 在途交换 ----
    def inflight_count(self, generation):
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM exchanges WHERE generation=? "
            "AND state='received'", (generation,)).fetchone()
        return row['n']

    def stored_payload_bytes(self):
        row = self.conn.execute(
            'SELECT COALESCE(SUM(LENGTH(payload)),0) AS n FROM exchanges').fetchone()
        return row['n']

    def find_active(self, conn, generation, packet_id):
        row = conn.execute(
            "SELECT * FROM exchanges WHERE generation=? AND packet_id=? "
            "AND state='received'",
            (generation, packet_id)).fetchone()
        return dict(row) if row else None

    def find_any(self, conn, generation, packet_id):
        row = conn.execute(
            'SELECT * FROM exchanges WHERE generation=? AND packet_id=? '
            'ORDER BY exchange_id DESC LIMIT 1',
            (generation, packet_id)).fetchone()
        return dict(row) if row else None

    def insert_exchange(self, conn, generation, packet_id, topic, payload,
                        retained, properties, identity, event_seq):
        cur = conn.execute(
            'INSERT INTO exchanges(generation,packet_id,state,topic,payload,'
            'retained,properties,received_identity,created_event_seq) '
            "VALUES(?,?, 'received', ?,?,?,?,?,?)",
            (generation, packet_id, topic, payload, 1 if retained else 0,
             properties, identity, event_seq))
        return cur.lastrowid

    def mark_completed_and_publish_sample(self, conn, exchange, event_seq):
        """同一本地事务：交换置completed并插入正式样本（正式可见只发生一次）。"""
        conn.execute(
            "UPDATE exchanges SET state='completed', completed_event_seq=? "
            'WHERE exchange_id=?', (event_seq, exchange['exchange_id']))
        conn.execute(
            'INSERT INTO samples(exchange_id,generation,packet_id,topic,payload,'
            'retained,properties,received_identity,visible_event_seq) '
            'VALUES(?,?,?,?,?,?,?,?,?)',
            (exchange['exchange_id'], exchange['generation'],
             exchange['packet_id'], exchange['topic'], exchange['payload'],
             exchange['retained'], exchange['properties'],
             exchange['received_identity'], event_seq))

    # ---- 确认台账 ----
    def record_ack(self, generation, packet_id, kind, reason_code,
                   event_seq, exchange_id=None, resend_of=None):
        """确认意图与原始日志在同一事务持久化，保证可从日志完整重演。"""
        self.conn.execute('BEGIN IMMEDIATE')
        try:
            cur = self.conn.execute(
                'INSERT INTO ack_ledger(generation,packet_id,kind,reason_code,'
                'exchange_id,event_seq,resend_of) VALUES(?,?,?,?,?,?,?)',
                (generation, packet_id, kind, reason_code, exchange_id,
                 event_seq, resend_of))
            raw = canonical_json({
                'type': kind, 'packet_id': packet_id,
                'reason_code': reason_code, 'exchange_id': exchange_id,
                'resend_of': resend_of})
            self.conn.execute(
                'INSERT INTO raw_journal(event_seq,generation,kind,raw,outcome) '
                "VALUES(?,?,?,?,'ack_sent')",
                (event_seq, generation, kind, raw))
            self.conn.execute('COMMIT')
            return cur.lastrowid
        except BaseException:
            self.conn.execute('ROLLBACK')
            raise

    def last_ack(self, generation, packet_id, kind):
        row = self.conn.execute(
            'SELECT * FROM ack_ledger WHERE generation=? AND packet_id=? AND kind=? '
            'ORDER BY ack_id DESC LIMIT 1',
            (generation, packet_id, kind)).fetchone()
        return dict(row) if row else None

    # ---- 原始事件日志 ----
    def append_journal(self, conn, event_seq, generation, kind, raw, outcome):
        conn.execute(
            'INSERT INTO raw_journal(event_seq,generation,kind,raw,outcome) '
            'VALUES(?,?,?,?,?)',
            (event_seq, generation, kind, raw, outcome))

    # ---- 只读盘点 ----
    def list_samples(self):
        return [dict(r) for r in self.conn.execute(
            'SELECT * FROM samples ORDER BY exchange_id')]

    def list_exchanges(self):
        return [dict(r) for r in self.conn.execute(
            'SELECT * FROM exchanges ORDER BY exchange_id')]

    def list_acks(self):
        return [dict(r) for r in self.conn.execute(
            'SELECT * FROM ack_ledger ORDER BY ack_id')]

    def list_journal(self):
        return [dict(r) for r in self.conn.execute(
            'SELECT * FROM raw_journal ORDER BY journal_id')]

    def list_generations(self):
        return [dict(r) for r in self.conn.execute(
            'SELECT * FROM generations ORDER BY generation')]

    def close(self):
        self.conn.close()


class _Transaction:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        self.conn.execute('BEGIN IMMEDIATE')
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.conn.execute('COMMIT')
            return False
        self.conn.execute('ROLLBACK')
        return False


def canonical_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'))
