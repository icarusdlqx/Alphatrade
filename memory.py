from __future__ import annotations
import os, json, datetime as dt
from typing import List, Dict, Optional, Any
import psycopg
from psycopg.rows import dict_row

DB_URL = os.getenv("DB_URL") or os.getenv("DATABASE_URL") or os.getenv("REPLIT_DB_URL")

def _conn():
    if not DB_URL:
        raise RuntimeError("No DB_URL / DATABASE_URL found.")
    return psycopg.connect(DB_URL, autocommit=True)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS episodes (
  id BIGSERIAL PRIMARY KEY,
  asof TIMESTAMPTZ NOT NULL,
  window_tag TEXT,
  equity NUMERIC,
  cash NUMERIC,
  notes TEXT,
  confidence NUMERIC,
  constraints JSONB,
  top_panel JSONB
);
CREATE TABLE IF NOT EXISTS picks (
  episode_id BIGINT REFERENCES episodes(id) ON DELETE CASCADE,
  symbol TEXT,
  weight NUMERIC,
  rationale TEXT
);
CREATE INDEX IF NOT EXISTS picks_episode_idx ON picks(episode_id);

CREATE TABLE IF NOT EXISTS orders (
  id BIGSERIAL PRIMARY KEY,
  episode_id BIGINT REFERENCES episodes(id) ON DELETE CASCADE,
  alpaca_order_id TEXT,
  symbol TEXT,
  side TEXT,
  notional NUMERIC,
  qty NUMERIC,
  status TEXT,
  submitted_at TIMESTAMPTZ,
  filled_qty NUMERIC,
  filled_avg_price NUMERIC,
  filled_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS orders_episode_idx ON orders(episode_id);

CREATE TABLE IF NOT EXISTS runlog (
  id BIGSERIAL PRIMARY KEY,
  at TIMESTAMPTZ NOT NULL DEFAULT now(),
  level TEXT,
  event TEXT,
  detail JSONB
);
CREATE INDEX IF NOT EXISTS runlog_at_idx ON runlog(at DESC);
"""

def init_db():
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)

def insert_episode(asof: dt.datetime, window_tag: str, equity: float, cash: float,
                   notes: str, confidence: float, constraints: Dict[str, Any],
                   top_panel: List[Dict[str, Any]]) -> int:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO episodes(asof, window_tag, equity, cash, notes, confidence, constraints, top_panel) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
            (asof, window_tag, equity, cash, notes, confidence, json.dumps(constraints), json.dumps(top_panel))
        )
        return int(cur.fetchone()[0])

def insert_picks(episode_id: int, picks: List[Dict[str, Any]]):
    with _conn() as conn, conn.cursor() as cur:
        for p in picks:
            cur.execute(
                "INSERT INTO picks(episode_id, symbol, weight, rationale) VALUES(%s,%s,%s,%s)",
                (p.get("symbol"), p.get("symbol"), p.get("weight"), p.get("rationale",""))
            )

def insert_order(episode_id: int, alpaca_order_id: str, symbol: str, side: str,
                 notional: Optional[float], qty: Optional[float], status: str,
                 submitted_at: Optional[dt.datetime]):
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO orders(episode_id, alpaca_order_id, symbol, side, notional, qty, status, submitted_at) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
            (episode_id, alpaca_order_id, symbol, side, notional, qty, status, submitted_at)
        )

def insert_log(level: str, event: str, detail: Dict[str, Any] | None = None):
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO runlog(level, event, detail) VALUES(%s,%s,%s)", (level, event, json.dumps(detail or {})))

def fetch_logs(limit: int = 400) -> List[Dict[str, Any]]:
    with _conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT at, level, event, detail FROM runlog ORDER BY at DESC LIMIT %s", (limit,))
        return cur.fetchall()

def fetch_orders(limit: int = 200) -> List[Dict[str, Any]]:
    with _conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM orders ORDER BY submitted_at DESC NULLS LAST, id DESC LIMIT %s", (limit,))
        return cur.fetchall()

def equity_series(limit: int = 300) -> List[Dict[str, Any]]:
    with _conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT asof, equity, cash FROM episodes ORDER BY asof ASC LIMIT %s", (limit,))
        return cur.fetchall()

def recent_episodes(n: int = 5) -> List[Dict[str, Any]]:
    with _conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM episodes ORDER BY asof DESC LIMIT %s", (n,))
        eps = cur.fetchall()
        out = []
        for e in eps:
            cur.execute("SELECT symbol, weight, rationale FROM picks WHERE episode_id=%s", (e["id"],))
            picks = cur.fetchall()
            e["picks"] = picks or []
            out.append(e)
        return out

def build_memory_context(n: int = 5) -> str:
    eps = recent_episodes(n)
    if not eps:
        return "No prior episodes."
    lines = []
    for e in eps[::-1]:
        when = e["asof"].strftime("%Y-%m-%d %H:%M")
        picks_str = ", ".join(f"{p['symbol']}:{float(p['weight']):.0%}" for p in e.get("picks", []) if p.get("symbol"))
        lines.append(f"{when} ({e.get('window_tag','?')}): {picks_str or 'no positions'}")
    return "Recent episodes → " + " | ".join(lines)
