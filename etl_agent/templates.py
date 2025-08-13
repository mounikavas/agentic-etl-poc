PLAN_SCHEMA_HINT = """
# plan.yaml schema
source: {kind: api|csv|json, api:{url,params,json_path}, csv:{path}, json:{path,json_path}}
transform: {sql: "SELECT ... FROM input_df"}
load: {conn_str: "postgresql+psycopg2://...", table: "schema.table", mode: append|replace|upsert, key_cols: [..]}
checks: {min_rows: int, nonnull_cols: [..], freshness_minutes: int, timestamp_col: str}
verify: {ts_col: str, max_lag_minutes: int}
alerts: {on_fail: "slack://#channel", webhook_url: "https://hooks.slack.com/..."}
limits: { max_input_bytes: 1073741824 }
"""

EXECUTOR_SNIPPET = """
from etl_agent.ops import (
    load_csv_op, write_csv_op, dq_check_op, verify_csv_op,
    registry_put, registry_get
)
# aliases so the rest of the snippet can keep using old names
load_csv   = load_csv_op
write_csv  = write_csv_op
dq_check   = dq_check_op
verify_csv = verify_csv_op

import yaml, json, re, os, duckdb, pandas as pd

def _to_yaml_map(text):
    s = str(text or "").strip()
    m = re.search(r\"```(?:yaml|yml)?\\s*\\n(.*?)\\n```\", s, flags=re.DOTALL|re.IGNORECASE)
    if m: s = m.group(1).strip()
    if s.startswith('mel <<EOF'):
        s = re.sub(r'^mel <<EOF\\n?(.*)\\nEOF\\s*$', r\"\\1\", s, flags=re.DOTALL)
    doc = yaml.safe_load(s)
    if not isinstance(doc, dict):
        raise ValueError(f\"Plan YAML must be a mapping; got {type(doc).__name__}\")
    return doc

def send_alert(*args, **kwargs): return "skipped"
def report_status(*args, **kwargs): return "ok"

def _infer_kind(src: dict) -> str:
    kind = src.get('kind','auto')
    if kind != 'auto':
        return kind
    # Heuristics: conn string -> db, http(s) -> api, file ext -> csv/json
    if 'db' in src and src['db'].get('conn_str'): return 'db'
    if 'api' in src and src['api'].get('url','').startswith(('http://','https://')): return 'api'
    if 'csv' in src and src['csv'].get('path','').lower().endswith('.csv'): return 'csv'
    if 'json' in src and src['json'].get('path','').lower().endswith(('.json','.ndjson')): return 'json'
    return 'api'  # conservative default


def run_from_plan(yml: str):
    plan = yaml.safe_load(yml)
    alerts = plan.get('alerts', {})
    # 1) Extract (choose tool based on kind or heuristics)
    src = plan['source']
    kind = _infer_kind(src)
    limits = plan.get('limits', {})
    max_bytes = limits.get('max_input_bytes', 1_000_000_000)
    if kind == 'csv':
        csvspec = src.get('csv', {})
        if 'paths' in csvspec:
            p = csvspec['paths']
            required = {'sales','features','stores'}
            if not required.issubset(p.keys()):
                raise ValueError("csv.paths must include keys: sales, features, stores")
            limits = plan.get('limits', {})
            max_bytes = limits.get('max_input_bytes', 1_000_000_000)
            total = sum(os.path.getsize(p[k]) for k in ('sales','features','stores'))
            if total > max_bytes:
                raise ValueError(f"input too large: {total} bytes > {max_bytes}")
            # Load each file separately; keep handles and dataframes
            h_sales   = load_csv(path=p['sales'],   max_bytes=max_bytes)
            h_features= load_csv(path=p['features'],max_bytes=max_bytes)
            h_stores  = load_csv(path=p['stores'],  max_bytes=max_bytes)

            df_sales    = registry_get(h_sales)
            df_features = registry_get(h_features)
            df_stores   = registry_get(h_stores)
            con = duckdb.connect()
            con.register("sales", df_sales)
            con.register("features", df_features)
            con.register("stores", df_stores)
        elif 'path' in csvspec:
            h = load_csv(path=csvspec['path'], max_bytes=max_bytes)
        else:
            raise ValueError("CSV source requires either csv.path or csv.paths{sales,features,stores}")
    elif kind == 'json':
        jp = src.get('json', {})
        h = load_json(path=jp['path'], json_path=jp.get('json_path', ''))
    elif kind == 'db':
        db = src['db']
        h = fetch_db(conn_str=db['conn_str'], query=db['query'])
    else:  # api
        ap = src['api']
        h = fetch_api(url=ap['url'], params=ap.get('params', {}), json_path=ap.get('json_path',''))

    # 2) Transform
    # h2 = transform_sql(sql=plan['transform']['sql'], handle=h)
    tr = plan.get('transform', {})
    steps = tr.get('steps')
    final_handle = None
    last_name = None

    if steps:
        for i, st in enumerate(steps):
            name = st['name']             # required
            sql  = st['sql']              # required
            out_df = con.execute(sql).df()
            # make this step available to later steps as a table
            con.register(name, out_df)
            # also store it in the registry for DQ/load
            final_handle = registry_put(out_df, name)
            last_name = name
    else:
        # Backward-compat: single SQL using sales/features/stores
        sql = tr.get('sql')
        if not sql:
            raise ValueError("Provide transform.steps[...].sql (preferred) or transform.sql.")
        out_df = con.execute(sql).df()
        final_handle = registry_put(out_df, "transform")
        last_name = "transform"

    # 3) DQ
    cks = plan.get('checks', {})
    # dq = dq_check(handle=final_handle, min_rows=cks.get('min_rows',1), nonnull_cols=cks.get('nonnull_cols',[]),
    #               freshness_minutes=cks.get('freshness_minutes'), timestamp_col=cks.get('timestamp_col',''))
    dq = dq_check(handle=final_handle, min_rows=cks.get('min_rows',1), nonnull_cols=cks.get('nonnull_cols',[]),
                  )
    dqj = json.loads(dq)
    if not dqj['status']:
        if alerts:
            send_alert(channel=alerts.get('on_fail',''), message=f"DQ failed: {dq}")
        return {"status":"failed", "dq": dqj}

    # 4) Load
    ld = plan['load']
    if ld.get('to','postgres') == 'csv':
        msg = write_csv(handle=final_handle, path=ld['file_path'], include_header=ld.get('include_header', True))
    else:
        msg = load_to_postgres(handle=final_handle, conn_str=ld['conn_str'], table=ld['table'], mode=ld.get('mode','append'), key_cols=ld.get('key_cols'))

    # 5) Verify
    vf = plan.get('verify', {})
    result = {"status": "ok", "dq": dqj, "message": msg}

    if ld.get('to', 'postgres') == 'csv':
        ver = verify_csv(
            path=ld['file_path'],
            min_rows=vf.get('min_rows', cks.get('min_rows', 1)),
            nonnull_cols=vf.get('nonnull_cols', cks.get('nonnull_cols', [])),
            timestamp_col=vf.get('ts_col', ''),
            max_lag_minutes=vf.get('max_lag_minutes', 180),
        )
    else:
        ver = verify_table(
            conn_str=ld['conn_str'],
            table=ld['table'],
            ts_col=vf.get('ts_col', ''),
            max_lag_minutes=vf.get('max_lag_minutes', 180),
        )

    vj = json.loads(ver)
    if not vj.get('status', False):
        if alerts:
            send_alert(channel=alerts.get('on_fail', ''), message=f"Verify failed: {ver}")
        return {"status": "failed", "verify": vj}

    result["verify"] = vj
    report_status(step='load', detail=msg)
    return result
"""