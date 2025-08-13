import os, json, hashlib, time
from datetime import datetime
from sqlalchemy import create_engine, text
from typing import Optional

ENGINE = create_engine(os.getenv("etl_agent_MEMORY_URL", "sqlite:///etl_agent.db"))

def _exec(sql, **kw):
    with ENGINE.begin() as c:
        return c.execute(text(sql), kw)

def init():
    _exec("""
    CREATE TABLE IF NOT EXISTS etl_agent_runs (
      run_id TEXT PRIMARY KEY,
      started_at TIMESTAMP, ended_at TIMESTAMP,
      prompt TEXT, prompt_hash TEXT,
      plan_yaml TEXT,
      status TEXT, rows_written INTEGER,
      dq_json TEXT, verify_json TEXT, error TEXT
    );
    """)
    _exec("""
    CREATE TABLE IF NOT EXISTS etl_agent_state (
      key TEXT PRIMARY KEY,
      value_json TEXT,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    _exec("""
    CREATE TABLE IF NOT EXISTS etl_agent_source_schema (
      source_hash TEXT PRIMARY KEY,
      schema_json TEXT,
      sample_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

def prompt_hash(prompt: str) -> str:
    return hashlib.sha256(prompt.encode()).hexdigest()[:16]

def start_run(prompt: str, plan_yaml: str) -> str:
    rid = f"run_{int(time.time()*1000)}"
    _exec("""
      INSERT INTO etl_agent_runs(run_id, started_at, prompt, prompt_hash, plan_yaml, status)
      VALUES (:rid, :ts, :prompt, :ph, :plan, 'running')
    """, rid=rid, ts=datetime.utcnow(), prompt=prompt, ph=prompt_hash(prompt), plan=plan_yaml)
    return rid

def finish_run(
    run_id: str,
    status: str,
    rows_written: int = 0,
    dq_json: Optional[dict] = None,
    verify_json: Optional[dict] = None,
    error: Optional[str] = None,
):
  _exec("""
    UPDATE etl_agent_runs SET ended_at=:ts, status=:status, rows_written=:rows,
      dq_json=:dq, verify_json=:ver, error=:err WHERE run_id=:rid
  """, ts=datetime.utcnow(), status=status, rows=rows_written,
      dq=json.dumps(dq_json or {}), ver=json.dumps(verify_json or {}), err=error, rid=run_id)

def get_state(key: str, default=None):
    r = _exec("SELECT value_json FROM etl_agent_state WHERE key=:k", k=key).fetchone()
    return json.loads(r[0]) if r and r[0] else default

def set_state(key: str, value):
    _exec("""
      INSERT INTO etl_agent_state(key, value_json) VALUES (:k, :v)
      ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=CURRENT_TIMESTAMP
    """, k=key, v=json.dumps(value))