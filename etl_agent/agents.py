from agents import Agent
from etl_agent.tools import (load_csv, load_json, fetch_api, transform_sql, python_udf,
                       load_to_postgres, dq_check, verify_table, send_alert, report_status)
from etl_agent.templates import PLAN_SCHEMA_HINT

GREETING = (
    "Hey! I’m your data engineer agent.\n"
    "Capabilities: extract (CSV/API/DB), transform (SQL or NL like 'clean, aggregate by date'), "
    "load (Postgres/CSV), DQ checks, verification, and optional alerts.\n\n"
    "Prompt format (example):\n"
    "limits:\n"
    "  max_input_bytes: 1073741824  # optional 1 GiB cap\n"
    "Source: db conn_str=$POSTGRES_URL\n"
    "Query:\n"
    "  SELECT sku, name, price AS salePrice, updated_at AS itemUpdateDate FROM upstream.products;\n"
    "Transform:\n"
    "  clean data; aggregate numeric columns (sum, avg) grouped by date/name-like fields\n"
    "Load: conn_str=$POSTGRES_URL, table=analytics.products_db, mode=replace\n"
    "Checks: min_rows=2, nonnull_cols=[sku,name,sale_price]\n"
    "Verify: ts_col=loaded_at, max_lag_minutes=60\n"
    "Alerts:  # optional; omit if not set up\n"
)

PLANNER_SYS = (
    "You are the planning agent for etl_agent. Convert the user's natural-language request "
    "into strict YAML with keys: limits, source, transform, load, checks, verify, alerts. "
    "Always include limits.max_input_bytes (default 1073741824) unless the user specifies otherwise. "
    "For CSV triplets, expect source.csv.paths.{sales,features,stores}. "
    "For API sources, put query params under source.api.params and rows selector in source.api.json_path. "
    "For CSV output, set load.to=csv and load.file_path; for Postgres, set load.conn_str and load.table. "
    "Return ONLY YAML (no backticks)."
)

greeter = Agent(
    name="ETL-Agent-Greeter",
    instructions="Welcome the user. Show capabilities and the prompt schema succinctly.",
    tools=[report_status],
)

planner = Agent(
    name="ETL-Agent-Planner",
    instructions=(
        "You convert a natural-language ETL request into strict YAML matching the plan schema. "
        "Fill defaults conservatively. Use $ENV placeholders for secrets."
        "Always include `limits.max_input_bytes` (default 1_073_741_824). If omitted by the user, set it to that default."
    ),
    tools=[],
)

codegen = Agent(
    name="ETL-Agent-Codegenerator",
    instructions=(
        "Given a finalized YAML plan, generate Python code needed to execute it now. "
        "Return a module named run_from_plan(yml: str) exactly as in EXECUTOR_SNIPPET; do not invent new functions."
    ),
    tools=[],
)

executor = Agent(
    name="ETL-Agent-Executor",
    instructions=(
        "Execute the ETL by calling the exposed tools in order, honoring checks and stopping on failure."
    ),
    tools=[load_csv, load_json, fetch_api, transform_sql, python_udf,
           load_to_postgres, dq_check, verify_table, send_alert, report_status],
)

verifier = Agent(
    name="ETL-Agent-Verifier",
    instructions=(
        "After load, verify target table row-count and freshness. If stale or empty, send alert."
    ),
    tools=[verify_table, send_alert, report_status],
)