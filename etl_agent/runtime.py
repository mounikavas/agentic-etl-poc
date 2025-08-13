from etl_agent import memory
from etl_agent.templates import EXECUTOR_SNIPPET
from etl_agent.agents import planner
from agents import Session
from datetime import datetime
memory.init()

def run_prompt(prompt: str):
    s = Session()
    plan_yaml = planner.run(session=s, messages=[{"role":"user","content": prompt}]).output_text
    run_id = memory.start_run(prompt, plan_yaml)

    # optional: apply state placeholders like $STATE{bestbuy:last_since}
    from etl_agent.memory import get_state
    plan_yaml = plan_yaml.replace("$STATE{bestbuy:last_since}", get_state("bestbuy:last_since", "" ) or "")

    # execute
    code_text = EXECUTOR_SNIPPET
    ns = {}
    exec(code_text, ns)
    try:
        result = ns['run_from_plan'](plan_yaml)
        rows = (result.get('verify') or {}).get('rows') or 0
        memory.finish_run(run_id, status=result.get('status','ok'), rows_written=rows, dq_json=result.get('dq'), verify_json=result.get('verify'))
        # update incremental state example
        if 'bestbuy' in prompt.lower():
            # in real code, compute max(itemUpdateDate) from df; here we just set now()
            memory.set_state("bestbuy:last_since", datetime.utcnow().isoformat())
        return result
    except Exception as e:
        memory.finish_run(run_id, status='failed', error=str(e))
        raise