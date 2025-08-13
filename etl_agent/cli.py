import sys, argparse, json
from etl_agent.runtime import run_prompt

def main():
    parser = argparse.ArgumentParser(description="Mel ETL Agent — run prompt from terminal")
    parser.add_argument("-p", "--prompt", help="ETL prompt text. If omitted, read from stdin.")
    args = parser.parse_args()

    prompt = args.prompt if args.prompt else sys.stdin.read()
    if not prompt or not prompt.strip():
        print("No prompt provided. Use --prompt or pipe text via stdin.", file=sys.stderr)
        raise SystemExit(2)

    result = run_prompt(prompt)
    # pretty JSON for CLI users
    try:
        print(json.dumps(result, indent=2))
    except Exception:
        print(result)

if __name__ == "__main__":
    main()