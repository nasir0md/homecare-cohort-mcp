# HomeCare Cohort MCP (Step 1)

FastMCP server that powers the TopGun HomeCare demo Step 1 agents. It exposes tools to surface the high‑risk cohort and produce the Step 1b care gap closure plan from the synthetic OMOP-like dataset.

## Prerequisites

- macOS / Linux shell
- Conda (recommended) or Python 3.11
- Repo cloned at `/Users/mdnasir/Documents/proj/TopGun/code/homecare-cohort-mcp`

## Environment Setup

```bash
conda create -n homecare-mcp python=3.11 -y
conda activate homecare-mcp
pip install -r requirements.txt
```

## Build DuckDB Dataset

The synthetic CSVs live in `../synthetic_data`. Rebuild the DuckDB file whenever the CSVs change:

```bash
cd /Users/mdnasir/Documents/proj/TopGun/code/homecare-cohort-mcp
python -c "from db import ensure_database; ensure_database(force_rebuild=True)"
```

This creates/overwrites `data/homecare.duckdb` and materializes helper views (`latest_sbp`, `latest_eye_exam`, etc.).

## Run the MCP Server Locally

```bash
cd /Users/mdnasir/Documents/proj/TopGun/code/homecare-cohort-mcp
uvicorn server:app --reload --port 8010
```

The Streamable HTTP transport is available at `http://127.0.0.1:8010/mcp`.

## Example Tool Calls

Using the MCP CLI (from the same conda env):

```bash
# Identify Step 1a cohort
mcp run server.py:mcp --call get_highrisk_cohort --data '{"limit": 6}'

# Build Step 1b care gap plan
mcp run server.py:mcp \
  --call care_gap_closure_plan \
  --data '{"patient_ids": ["PAT-00042", "PAT-00058"]}'
```

Or use MCP Inspector (`mcp dev server.py:mcp`) to interactively inspect Markdown and structured JSON.

## Smoke Test

`python smoke_test.py`

(Ensures both Step 1 tools execute and return non-empty results.)

## Deployment Notes

- Optional build step (if rebuilding DB on deploy):
  ```bash
  python -c "from db import ensure_database; ensure_database()"
  ```
- Configure `MCP_ALLOWED_HOSTS` / `MCP_ALLOWED_ORIGINS` and future API keys (CMS, HDI, Medical Research).

## Repository Structure

```
homecare-cohort-mcp/
├── api/index.py          # Vercel entrypoint
├── data/homecare.duckdb  # Generated DuckDB file (ignored by default)
├── db.py                 # DuckDB loader + helper views
├── requirements.txt
├── server.py             # FastMCP server with Step 1 tools
├── smoke_test.py         # Regression script
└── vercel.json
```
