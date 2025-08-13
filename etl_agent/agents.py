from agents import Agent
from etl_agent.tools import (load_csv, load_json, fetch_api, transform_sql, python_udf,
                       load_to_postgres, dq_check, verify_table, send_alert, report_status)
from etl_agent.templates import PLAN_SCHEMA_HINT

GREETING = (
    "Hey there! I’m etl_agent, your Data Engineering Agent. I can provision ETL from API/CSV/JSON to Postgres, "
    "transform with SQL, schedule runs, verify freshness, and alert on issues."
    "Guardrail: inputs over 1 GB are rejected."
    "**Prompt format**: Source→Transform→Target (+Schedule/Checks/Verify)." + PLAN_SCHEMA_HINT
)

greeter = Agent(
    instructions="Welcome the user. Show capabilities and the prompt schema succinctly.",
    tools=[report_status],
)

planner = Agent(
    instructions=(
        "You convert a natural-language ETL request into strict YAML matching the plan schema. "
        "Fill defaults conservatively. Use $ENV placeholders for secrets."
        "Always include `limits.max_input_bytes` (default 1_073_741_824). If omitted by the user, set it to that default."
    ),
    tools=[],
)

codegen = Agent(
    instructions=(
        "Given a finalized YAML plan, generate Python code needed to execute it now. "
        "Return a module named run_from_plan(yml: str) exactly as in EXECUTOR_SNIPPET; do not invent new functions."
    ),
    tools=[],
)

executor = Agent(
    instructions=(
        "Execute the ETL by calling the exposed tools in order, honoring checks and stopping on failure."
    ),
    tools=[load_csv, load_json, fetch_api, transform_sql, python_udf,
           load_to_postgres, dq_check, verify_table, send_alert, report_status],
)

verifier = Agent(
    instructions=(
        "After load, verify target table row-count and freshness. If stale or empty, send alert."
    ),
    tools=[verify_table, send_alert, report_status],
)