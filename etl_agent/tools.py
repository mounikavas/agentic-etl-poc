from agents import function_tool
import pandas as pd, duckdb, io, json, base64, requests, re, os
from sqlalchemy import create_engine, text
from typing import Optional, List

# In-memory registry to store intermediate dataframes by handle
REG = {}

def _put(df, tag):
    h = f"df://{tag}-{len(REG)}"; REG[h] = df; return h

def _get(h):
    return REG[h]

@function_tool
def load_csv(path: str = "", content_b64: str = "") -> str:
    """Load CSV from a local path or base64 bytes. Returns a dataframe handle."""
    df = pd.read_csv(path) if path else pd.read_csv(io.BytesIO(base64.b64decode(content_b64)))
    return _put(df, "csv")

@function_tool
def fetch_api(url: str, params_json: str = "", json_path: str = "") -> str:
    """
    Fetch from REST API. `params_json` is a JSON string (e.g. '{"q":"tv","limit":10}').
    Returns a dataframe handle.
    """
    params = json.loads(params_json) if params_json else {}
    r = requests.get(url, params=params, timeout=120)
    r.raise_for_status()
    data = r.json()
    rows = eval(json_path) if json_path else data   # (consider jsonpath_ng in prod)
    df = pd.json_normalize(rows)
    return _put(df, "api")

@function_tool
def load_json(path: str, json_path: str = "") -> str:
    """Load JSON from local path. Returns a dataframe handle."""
    data = json.load(open(path))
    rows = eval(json_path) if json_path else data  # prefer jsonpath_ng in prod
    df = pd.json_normalize(rows)
    return _put(df, "json")

@function_tool
def fetch_db(conn_str: str, query: str) -> str:
    """Run SQL against an upstream database (SQLAlchemy conn string). Returns a dataframe handle."""
    eng = create_engine(conn_str)
    df = pd.read_sql_query(query, eng)
    return _put(df, "db")

@function_tool
def transform_sql(sql: str, handle: str) -> str:
    """Run DuckDB SQL over a dataframe handle registered as input_df. Returns a new handle."""
    df = _get(handle)
    con = duckdb.connect()
    con.register("input_df", df)
    out = con.execute(sql).df()
    return _put(out, "xform")

@function_tool
def python_udf(handle: str, expression: str, new_col: str) -> str:
    """Optional Python UDF transform: eval a simple expression per row."""
    df = _get(handle).copy()
    df[new_col] = df.eval(expression)
    return _put(df, "udf")

@function_tool
def load_to_postgres(handle: str, conn_str: str, table: str, mode: str = "append", key_cols: Optional[List[str]] = None) -> str:
    """Write dataframe handle to Postgres. Supports append/replace/upsert (key cols)."""
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

@function_tool
def write_csv(handle: str, path: str, include_header: bool = True) -> str:
    """Write dataframe handle to a CSV file path."""
    df = _get(handle)
    df.to_csv(path, index=False, header=include_header)
    return f"wrote {len(df)} rows to {path}"

@function_tool
def dq_check(handle: str, min_rows: int = 1, nonnull_cols: Optional[List[str]] = None, freshness_minutes: Optional[int] = None, timestamp_col: str = "") -> str:
    """Basic DQ checks and freshness window."""
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

@function_tool
def verify_table(conn_str: str, table: str, ts_col: str = "", max_lag_minutes: int = 180) -> str:
    """
    Verify that a target table exists and (optionally) is fresh.

    Args:
        conn_str: SQLAlchemy connection string (e.g., postgresql+psycopg2://user:pass@host/db)
        table:    Fully-qualified table name, e.g., schema.table
        ts_col:   Optional timestamp column to check freshness (MAX(ts_col))
        max_lag_minutes: Maximum allowed lag in minutes for freshness check

    Returns:
        JSON string like:
        {"rows": 123, "lag_minutes": 4.73, "lag_ok": true, "status": true}

    Notes:
        - If ts_col is empty or not present, freshness is skipped and considered OK.
        - This function returns a JSON string to stay schema-safe for the Agents SDK.
        - For dynamic identifiers, ensure `table` and `ts_col` are trusted (no user-supplied SQL).
    """
    try:
        eng = create_engine(conn_str)
    except Exception as e:
        return json.dumps({"error": f"engine_error: {e}", "status": False})

    rows = 0
    lag_min = None
    lag_ok = True

    try:
        with eng.connect() as c:
            # Count rows (will raise if table doesn't exist)
            rows = c.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar() or 0

            # Freshness check (optional)
            if ts_col:
                max_ts = c.execute(text(f"SELECT MAX({ts_col}) FROM {table}")).scalar()
                if max_ts is not None:
                    max_ts = pd.to_datetime(max_ts, utc=True, errors="coerce")
                    if pd.isna(max_ts):
                        return json.dumps({"rows": int(rows), "error": f"could_not_parse_ts({ts_col})", "status": False})
                    now = pd.Timestamp.utcnow()
                    lag_min = float((now - max_ts).total_seconds() / 60.0)
                    lag_ok = lag_min <= max_lag_minutes
    except Exception as e:
        return json.dumps({"rows": int(rows), "error": f"verify_error: {e}", "status": False})

    status = (rows > 0) and lag_ok
    return json.dumps({"rows": int(rows), "lag_minutes": lag_min, "lag_ok": lag_ok, "status": bool(status)})

@function_tool
def verify_csv(
    path: str,
    min_rows: int = 1,
    nonnull_cols: Optional[List[str]] = None,
    timestamp_col: str = "",
    max_lag_minutes: int = 180,
    delimiter: str = ",",
    encoding: str = "",
) -> str:
    """
    Verify a CSV sink:
      - file exists and non-zero size
      - has at least `min_rows`
      - specified columns have no nulls
      - if `timestamp_col` provided: MAX(timestamp_col) freshness <= max_lag_minutes
        otherwise, falls back to file mtime freshness.
    Returns JSON string: {"rows": N, "nonnull_ok": bool, "fresh_ok": bool, "lag_minutes": float|None, "status": bool, "error": str|None}
    """
    try:
        if not os.path.exists(path):
            return json.dumps({"error": f"file_not_found: {path}", "status": False})

        size = os.path.getsize(path)
        if size == 0:
            return json.dumps({"error": "empty_file", "status": False})

        rows_total = 0
        nonnull_ok = True
        fresh_ok = True
        lag_min = None

        # Only load columns we need
        usecols = None
        if nonnull_cols and timestamp_col:
            usecols = list(set(nonnull_cols + [timestamp_col]))
        elif nonnull_cols:
            usecols = nonnull_cols
        elif timestamp_col:
            usecols = [timestamp_col]

        parse_dates = [timestamp_col] if timestamp_col else None

        # Stream in chunks so large files don't blow memory
        need_dataframe_checks = bool(nonnull_cols or timestamp_col)
        if need_dataframe_checks:
            for chunk in pd.read_csv(
                path,
                sep=delimiter,
                encoding=(encoding or None),
                usecols=usecols,
                parse_dates=parse_dates,
                chunksize=200_000,
                low_memory=False,
            ):
                rows_total += len(chunk)
                if nonnull_cols:
                    for c in nonnull_cols:
                        if c in chunk.columns and not chunk[c].notna().all():
                            nonnull_ok = False
                # keep max timestamp across chunks
                if timestamp_col and timestamp_col in chunk.columns:
                    max_ts_chunk = pd.to_datetime(chunk[timestamp_col], errors="coerce")
                    if not max_ts_chunk.empty:
                        max_ts_val = max_ts_chunk.max()
                        if pd.notna(max_ts_val):
                            # track globally
                            lag_min = (
                                (pd.Timestamp.utcnow() - max_ts_val.tz_localize("UTC", nonexistent="NaT", ambiguous="NaT")
                                 if max_ts_val.tzinfo is None else pd.Timestamp.utcnow() - max_ts_val)
                                .total_seconds() / 60.0
                            ) if max_ts_val is not pd.NaT else lag_min
            # if timestamp_col present, evaluate freshness
            if timestamp_col and lag_min is not None:
                fresh_ok = lag_min <= max_lag_minutes
        else:
            # Fast line count without loading columns
            # (subtract header if present)
            with open(path, "r", encoding=(encoding or "utf-8"), errors="ignore") as f:
                rows_total = sum(1 for _ in f) - 1
            # freshness via file mtime as fallback
            mtime = os.path.getmtime(path)
            lag_min = (pd.Timestamp.utcnow() - pd.to_datetime(mtime, unit="s", utc=True)).total_seconds() / 60.0
            fresh_ok = lag_min <= max_lag_minutes

        status = (rows_total >= min_rows) and nonnull_ok and fresh_ok
        return json.dumps({
            "rows": int(rows_total),
            "nonnull_ok": bool(nonnull_ok),
            "fresh_ok": bool(fresh_ok),
            "lag_minutes": lag_min,
            "status": bool(status),
        })
    except Exception as e:
        return json.dumps({"error": f"verify_csv_error: {e}", "status": False})


@function_tool
def send_alert(channel: str, message: str) -> str:
    """Send an alert stub (stdout/log/Slack webhook placeholder)."""
    print(f"ALERT to {channel}: {message}")
    return "sent"

@function_tool
def report_status(step: str, detail: str) -> str:
    """Report a run status (stdout/log)."""
    print(f"STATUS[{step}]: {detail}")
    return "ok"

