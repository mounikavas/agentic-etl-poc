#!/usr/bin/env python3
import sys, argparse, json, os
from pathlib import Path
from dotenv import load_dotenv
from etl_agent.runtime import run_prompt
from etl_agent.agents import GREETING

# load .env from repo root
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

def _read_prompt_arg(arg: str) -> str:
    # Treat values containing newlines as inline text, otherwise path if file exists
    if "\n" in arg:
        return arg
    try:
        p = Path(arg)
        return p.read_text() if (p.exists() and p.is_file()) else arg
    except OSError:
        return arg

def main():
    ap = argparse.ArgumentParser(description="ETL Agent — run prompt from terminal")
    ap.add_argument("-p", "--prompt", help="Prompt text OR path to a file containing the prompt.")
    ap.add_argument("--greet", action="store_true", help="Print greeting/capabilities and exit.")
    ap.add_argument("--no-greet", action="store_true", help="Do not print greeting on startup.")
    args = ap.parse_args()

    if args.greet:
        print(GREETING)
        return 0

    # Read prompt (flag or stdin)
    if args.prompt:
        prompt = _read_prompt_arg(args.prompt)
    else:
        prompt = sys.stdin.read()

    if not prompt.strip():
        # No prompt: show greeting to stdout and exit non-error
        print(GREETING)
        return 0

    # Show greeting to STDERR by default so stdout stays pure JSON
    if not args.no_greet and os.getenv("MEL_NO_GREETING", "0") != "1":
        print(GREETING, file=sys.stderr)

    result = run_prompt(prompt)
    try:
        print(json.dumps(result, indent=2))
    except Exception:
        print(result)

if __name__ == "__main__":
    sys.exit(main())
