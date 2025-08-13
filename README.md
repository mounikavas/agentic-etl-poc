# agentic-etl-poc
An Agentic AI-powered ETL system that builds and runs data pipelines from the user prompts. It interprets user requests, ingests data from sources like APIs or databases, transforms it, loads it into PostgreSQL/Tableau-ready formats, and monitors performance with proactive alerts for seamless analytics.

Prompt→Pipeline ETL Agent

Turn natural-language prompts into ETL runs: extract (csv/api/db), transform (SQL), load (Postgres/CSV), DQ + verify, and alerts.

## Install
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
```