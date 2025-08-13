import os, json
from typing import Optional, List
import pandas as pd

# --- simple in-memory registry for dataframes
_DF_REGISTRY: dict[str, pd.DataFrame] = {}

def registry_put(df: pd.DataFrame, tag: str) -> str:
    key = f"{tag}_{len(_DF_REGISTRY)+1}"
    _DF_REGISTRY[key] = df
    return key

def registry_get(handle: str) -> pd.DataFrame:
    if handle not in _DF_REGISTRY:
        raise KeyError(f"unknown dataframe handle: {handle}")
    return _DF_REGISTRY[handle]

# --- IO + checks

def load_csv_op(path: str, max_bytes: Optional[int] = 1_000_000_000) -> str:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    if max_bytes is not None and os.path.getsize(path) > max_bytes:
        raise ValueError(f"input too large: {os.path.getsize(path)} > {max_bytes}")
    df = pd.read_csv(path)
    return registry_put(df, "csv")

def write_csv_op(handle: str, path: str, include_header: bool = True) -> str:
    df = registry_get(handle)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df.to_csv(path, index=False, header=include_header)
    return f"wrote {len(df):,} rows to {path}"

def dq_check_op(handle: str, min_rows: int = 1, nonnull_cols: Optional[List[str]] = None) -> str:
    nonnull_cols = nonnull_cols or []
    df = registry_get(handle)
    ok = True
    err = None
    if len(df) < min_rows:
        ok = False
        err = f"min_rows check failed: {len(df)} < {min_rows}"
    for c in nonnull_cols:
        if c in df.columns and df[c].isna().any():
            ok = False
            err = f"nonnull check failed: {c}"
            break
    return json.dumps({"rows": int(len(df)), "status": bool(ok), "error": err})

def verify_csv_op(
    path: str,
    min_rows: int = 1,
    nonnull_cols: Optional[List[str]] = None,
    timestamp_col: str = "",
    max_lag_minutes: int = 180,
    delimiter: str = ",",
    encoding: str = "",
) -> str:
    import pandas as pd, os, json
    nonnull_cols = nonnull_cols or []
    if not os.path.exists(path):
        return json.dumps({"status": False, "error": f"file_not_found: {path}"})
    if os.path.getsize(path) == 0:
        return json.dumps({"status": False, "error": "empty_file"})

    # load only what we need, in chunks for safety
    usecols = None
    if nonnull_cols and timestamp_col:
        usecols = list(set(nonnull_cols + [timestamp_col]))
    elif nonnull_cols:
        usecols = nonnull_cols
    elif timestamp_col:
        usecols = [timestamp_col]

    parse_dates = [timestamp_col] if timestamp_col else None
    rows = 0
    nonnull_ok = True
    fresh_ok = True
    lag_min = None

    need_df = bool(nonnull_cols or timestamp_col)
    if need_df:
        for ch in pd.read_csv(
            path, sep=delimiter, encoding=(encoding or None),
            usecols=usecols, parse_dates=parse_dates,
            chunksize=200_000, low_memory=False
        ):
            rows += len(ch)
            for c in nonnull_cols:
                if c in ch.columns and not ch[c].notna().all():
                    nonnull_ok = False
            if timestamp_col and timestamp_col in ch.columns:
                mx = pd.to_datetime(ch[timestamp_col], errors="coerce")
                if not mx.empty and mx.notna().any():
                    m = mx.max()
                    now = pd.Timestamp.utcnow()
                    if m.tzinfo is None:
                        m = m.tz_localize("UTC")
                    lag_min = float((now - m).total_seconds()/60.0)
        if timestamp_col and lag_min is not None:
            fresh_ok = lag_min <= max_lag_minutes
    else:
        with open(path, "r", encoding=(encoding or "utf-8"), errors="ignore") as f:
            rows = sum(1 for _ in f) - 1
        mtime = os.path.getmtime(path)
        lag_min = (pd.Timestamp.utcnow() - pd.to_datetime(mtime, unit="s", utc=True)).total_seconds()/60.0
        fresh_ok = lag_min <= max_lag_minutes

    status = (rows >= min_rows) and nonnull_ok and fresh_ok
    return json.dumps({"rows": int(rows), "nonnull_ok": nonnull_ok, "fresh_ok": fresh_ok, "lag_minutes": lag_min, "status": status})
