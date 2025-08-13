import os
from agents import Runner, SQLiteSession
from etl_agent.agents import planner         # (and greeter if you use greet())
from etl_agent.templates import EXECUTOR_SNIPPET

def _expand_env(text: str) -> str:
    return os.path.expandvars(text or "")

def greet() -> str:
    sess = SQLiteSession("etl_agent_greeting")
    res = Runner.run_sync(planner, "hello", session=sess)
    return res.final_output

def run_prompt(prompt: str):
    prompt = os.path.expandvars(prompt or "")
    ns = {}; exec(EXECUTOR_SNIPPET, ns)

    # Offline if env set OR prompt already looks like YAML
    first = prompt.lstrip().lower()
    if os.getenv("ETL_AGENT_OFFLINE") == "1" or first.startswith(("limits:", "source:", "transform:", "load:", "checks:", "verify:", "alerts:")):
        plan_yaml = prompt
    else:
        sess = SQLiteSession("etl_agent_run")
        plan_yaml = Runner.run_sync(planner, prompt, session=sess).final_output

    return ns["run_from_plan"](plan_yaml)

