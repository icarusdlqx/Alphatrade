from __future__ import annotations
import os, json
from typing import Dict, Any, Optional
import psycopg
from psycopg.rows import dict_row
from config import DEFAULTS

DB_URL = os.getenv("DB_URL") or os.getenv("DATABASE_URL") or os.getenv("REPLIT_DB_URL")

def _conn():
    if not DB_URL:
        raise RuntimeError("Missing DB_URL/DATABASE_URL. In Replit, create a Database and copy the connection string to `DB_URL`.")
    return psycopg.connect(DB_URL, autocommit=True)

SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value JSONB
);
"""

def init_settings_table():
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(SCHEMA)

def get_settings() -> Dict[str, Any]:
    # DB overrides env defaults
    d = DEFAULTS.copy()
    try:
        with _conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT key, value FROM settings")
            for r in cur.fetchall():
                d[r["key"]] = r["value"]
    except Exception:
        pass
    # Normalize types for bools stored as JSON
    d["USE_INTRADAY"] = bool(d.get("USE_INTRADAY"))
    d["EARNINGS_GATING"] = bool(d.get("EARNINGS_GATING"))
    d["REGIME_FILTER"] = bool(d.get("REGIME_FILTER"))
    d["ENABLED"] = bool(d.get("ENABLED"))
    return d

def set_settings(new_vals: Dict[str, Any]):
    with _conn() as conn, conn.cursor() as cur:
        for k, v in new_vals.items():
            cur.execute("INSERT INTO settings(key, value) VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value", (k, json.dumps(v)))
