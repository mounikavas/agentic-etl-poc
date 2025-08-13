from prefect import flow, task
from etl_agent.runtime import run_prompt

@task
def run_once(prompt: str):
    """Execute a single Prompt→Plan→ETL run and return the result dict."""
    return run_prompt(prompt)

@flow(name="etl-agent-pipeline-run", log_prints=True)
def pipeline_run(prompt: str):
    """Prefect flow wrapper so you can schedule/monitor runs in Prefect UI."""
    return run_once(prompt)

# Deploy with Prefect for schedules/alerts.
# prefect deployment build etl_agent/operator.py:pipeline_run -n etl_agent-hourly -p cron --cron "0 * * * *" --apply \
#   --param prompt="Hourly, fetch BestBuy products ... load to analytics.cheap_products ..."


if __name__ == "__main__":
    # Optional: run this flow directly from the CLI
    import argparse, sys, json
    ap = argparse.ArgumentParser(description="Run a Mel ETL prompt via Prefect flow")
    ap.add_argument("-p", "--prompt", help="Prompt text. If omitted, read from stdin.")
    args = ap.parse_args()
    prompt = args.prompt if args.prompt else sys.stdin.read()
    if not prompt.strip():
        raise SystemExit("No prompt provided. Use --prompt or pipe text via stdin.")
    result = pipeline_run(prompt)
    print(json.dumps(result, indent=2))
