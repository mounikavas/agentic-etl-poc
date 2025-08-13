import sys, argparse, json, os
from pathlib import Path
from dotenv import load_dotenv
from etl_agent.runtime import run_prompt

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

def main():
    ap = argparse.ArgumentParser(description="ETL Agent — run prompt from terminal")
    ap.add_argument("-p", "--prompt", help="Prompt text OR path to a file containing the prompt.")
    args = ap.parse_args()

    if args.prompt:
        raw = args.prompt
        if "\n" in raw:
            prompt = raw                      # inline text → use as-is
        else:
            try:
                p = Path(raw)
                prompt = p.read_text() if (p.exists() and p.is_file()) else raw
            except OSError:
                prompt = raw
    else:
        prompt = sys.stdin.read()

    if not prompt.strip():
        print("No prompt provided. Use --prompt or pipe text via stdin.", file=sys.stderr)
        sys.exit(2)

    result = run_prompt(prompt)
    try:
        print(json.dumps(result, indent=2))
    except Exception:
        print(result)

if __name__ == "__main__":
    main()
