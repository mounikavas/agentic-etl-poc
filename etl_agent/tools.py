from agents.tool import function_tool
import pandas as pd, duckdb, io, json, base64, requests, re
from sqlalchemy import create_engine
from typing import Optional, List

# In-memory registry to store intermediate dataframes by handle
REG = {}

def _put(df, tag):
    h = f"df://{tag}-{len(REG)}"; REG[h] = df; return h

def _get(h):
    return REG[h]

@function_tool(description="Load CSV from local path or base64 bytes. Returns a dataframe handle.")
def load_csv(path: str = "", content_b64: str = "") -> str:
    df = pd.read_csv(path) if path else pd.read_csv(io.BytesIO(base64.b64decode(content_b64)))
    return _put(df, "csv")

@function_tool(description="Load JSON from local path. Returns a dataframe handle.")
def load_json(path: str, json_path: str = "") -> str:
    data = json.load(open(path))
    rows = eval(json_path) if json_path else data  # prefer jsonpath_ng in prod
    df = pd.json_normalize(rows)
    return _put(df, "json")

@function_tool(description="Fetch from REST API. Returns a dataframe handle.")
def fetch_api(url: str, params: dict = None, json_path: str = "") -> str:
    r = requests.get(url, params=params or {}, timeout=60); r.raise_for_status()
    data = r.json()
    rows = eval(json_path) if json_path else data
    df = pd.json_normalize(rows)
    return _put(df, "api")

@function_tool(description="Run SQL against an upstream database (SQLAlchemy conn string). Returns a dataframe handle.")
def fetch_db(conn_str: str, query: str) -> str:
    eng = create_engine(conn_str)
    df = pd.read_sql_query(query, eng)
    return _put(df, "db")

@function_tool(description="Run DuckDB SQL over a dataframe handle registered as input_df. Returns a new handle.")
def transform_sql(sql: str, handle: str) -> str:
    df = _get(handle)
    con = duckdb.connect()
    con.register("input_df", df)
    out = con.execute(sql).df()
    return _put(out, "xform")

@function_tool(description="Optional Python UDF transform: eval a simple expression per row.")
def python_udf(handle: str, expression: str, new_col: str) -> str:
    df = _get(handle).copy()
    df[new_col] = df.eval(expression)
    return _put(df, "udf")

@function_tool(description="Write dataframe handle to Postgres. Supports append/replace/upsert (key cols).")
def load_to_postgres(handle: str, conn_str: str, table: str, mode: str = "append", key_cols: Optional[List[str]] = None) -> str:
    df = _get(handle)
    eng = create_engine(conn_str)
    if mode == "replace":
        df.to_sql(table, eng, if_exists="replace", index=False)
    elif mode == "append":
        df.to_sql(table, eng, if_exists="append", index=False)
    elif mode == "upsert":
        # minimal demonstration: stage then merge via SQL (simplified)
        tmp = f"{table}_stg"
        df.to_sql(tmp, eng, if_exists="replace", index=False)
        keys = ",".join(key_cols or [])
        cols = ",".join(df.columns)
        upd = ",".join([f"{c}=EXCLUDED.{c}" for c in df.columns if c not in (key_cols or [])])
        with eng.begin() as conn:
            conn.exec_driver_sql(f"""
                CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM {tmp} WHERE 1=0;
                INSERT INTO {table} ({cols})
                SELECT {cols} FROM {tmp}
                ON CONFLICT ({keys}) DO UPDATE SET {upd};
            """)
    return f"wrote {len(df)} rows to {table}"

@function_tool(description="Write dataframe handle to a CSV file path.")
def write_csv(handle: str, path: str, include_header: bool = True) -> str:
    df = _get(handle)
    df.to_csv(path, index=False, header=include_header)
    return f"wrote {len(df)} rows to {path}"

@function_tool(description="Basic DQ checks and freshness window.")
def dq_check(handle: str, min_rows: int = 1, nonnull_cols: Optional[List[str]] = None, freshness_minutes: Optional[int] = None, timestamp_col: str = "") -> str:
    import pandas as pd
    df = _get(handle)
    ok_rows = len(df) >= min_rows
    ok_nn = all(df[c].notna().all() for c in (nonnull_cols or []))
    ok_fresh = True
    if freshness_minutes and timestamp_col and timestamp_col in df.columns:
        dt = pd.to_datetime(df[timestamp_col]).max()
        ok_fresh = (pd.Timestamp.utcnow() - dt).total_seconds() <= freshness_minutes*60
    status = ok_rows and ok_nn and ok_fresh
    return json.dumps({"rows": len(df), "nonnull_ok": ok_nn, "fresh_ok": ok_fresh, "status": status})

@function_tool(description="Send an alert stub (stdout/log/Slack webhook placeholder).")
def send_alert(channel: str, message: str) -> str:
    print(f"ALERT to {channel}: {message}")
    return "sent"

@function_tool(description="Report a run status (stdout/log).")
def report_status(step: str, detail: str) -> str:
    print(f"STATUS[{step}]: {detail}")
    return "ok"