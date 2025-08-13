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
from etl_agent.tools import *
import yaml, json, re

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
        h = load_csv(path=src['csv']['path'])
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
    h2 = transform_sql(sql=plan['transform']['sql'], handle=h)

    # 3) DQ
    cks = plan.get('checks', {})
    dq = dq_check(handle=h2, min_rows=cks.get('min_rows',1), nonnull_cols=cks.get('nonnull_cols',[]),
                  freshness_minutes=cks.get('freshness_minutes'), timestamp_col=cks.get('timestamp_col',''))
    dqj = json.loads(dq)
    if not dqj['status']:
        if alerts:
            send_alert(channel=alerts.get('on_fail',''), message=f"DQ failed: {dq}")
        return {"status":"failed", "dq": dqj}

    # 4) Load
    ld = plan['load']
    if ld.get('to','postgres') == 'csv':
        msg = write_csv(handle=h2, path=ld['file_path'], include_header=ld.get('include_header', True))
    else:
        msg = load_to_postgres(handle=h2, conn_str=ld['conn_str'], table=ld['table'], mode=ld.get('mode','append'), key_cols=ld.get('key_cols'))

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