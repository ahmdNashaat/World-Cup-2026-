# World Cup 2026 ETL Pipeline

Production-style ETL pipeline that extracts live FIFA World Cup 2026 match data from a public API, applies data quality validation, and loads it into PostgreSQL — fully orchestrated with Apache Airflow and containerized with Docker.

Built as a hands-on portfolio project to apply data engineering fundamentals (idempotency, data quality isolation, pipeline observability, orchestration) on a real, time-relevant dataset rather than a static tutorial dataset.

## Architecture
football-data.org API

│

▼

extract.py  ──► raw JSON snapshot (timestamped, immutable)

│

▼

transform.py ──► validation + classification

│

├──► matches table          (valid, fully known matches)

├──► pending_matches table  (slot reserved, teams not yet determined)

└──► quarantine table       (real data-quality failures, isolated)

│

▼

load.py    ──► idempotent UPSERT into PostgreSQL + pipeline_logs entry

│

▼

Orchestrated daily by an Airflow DAG (extract >> transform >> load)
## Why this isn't just "another CRUD script"

Most beginner ETL projects stop at "fetch API → insert into table." This project deliberately adds the layer that's actually evaluated in real data engineering roles:

- **Idempotency**: re-running the pipeline on identical data never creates duplicates. Verified empirically — running `load.py` twice against the same snapshot produced the same row count (72) both times, via `INSERT ... ON CONFLICT (match_id) DO UPDATE`.
- **Data quality isolation, not binary failure**: a single malformed record never crashes the run. Every match is classified into one of three buckets:
  - `matches` — both teams confirmed, ready for analysis
  - `pending_matches` — a real, expected state for early knockout-stage fixtures where teams aren't determined yet (distinguished from an actual data error)
  - `quarantine` — genuine validation failures (e.g. invalid enum values, missing required fields), logged with a reason and the original payload for debugging
- **Observability**: every pipeline run writes a row to `pipeline_logs` (start time, end time, duration, row counts per category, success/failure status) — so failures are diagnosable without digging through container logs.
- **Orchestration**: the three stages are wired into an Airflow DAG using XCom for inter-task data passing, scheduled to run daily, retriable on failure.

## Tech stack

- **Python 3** — `requests`, `psycopg2`
- **PostgreSQL 16** — UPSERT-based idempotent loading
- **Apache Airflow 2.9.3** (`LocalExecutor`) — DAG orchestration
- **Docker Compose** — multi-service local environment (Postgres + Airflow), single-command spin-up
- **football-data.org API** — live World Cup 2026 match data

## Real data quality finding

Out of 104 total matches in the competition, the live API returned 32 knockout-stage fixtures with both `homeTeam` and `awayTeam` fields set to `null` — these are real fixtures (date, stage, and bracket slot already defined) waiting on group-stage results to determine the participating teams. The pipeline correctly distinguishes this from an actual data defect (e.g. only one team missing, which would indicate a genuine API/data issue) and routes it to `pending_matches` instead of incorrectly quarantining valid future fixtures.

## Running it locally

```bash
git clone https://github.com/ahmdNashaat/World-Cup-2026-.git
cd World-Cup-2026-

# 1. Add your football-data.org API token
echo "FOOTBALL_API_TOKEN=your_token_here" > .env

# 2. Start Postgres + Airflow
docker compose up -d

# 3. Open Airflow UI
# http://localhost:8080  (user: admin / password: admin)
# Trigger the "worldcup_etl_pipeline" DAG manually, or wait for the daily schedule
```

## Project structure
.

├── pipeline/

│   ├── extract.py      # API extraction + rate-limit handling + raw snapshotting

│   ├── transform.py    # Validation, classification, data quality rules

│   └── load.py         # Idempotent UPSERT loading + pipeline_logs

├── dags/

│   └── worldcup_pipeline_dag.py   # Airflow DAG wiring the three stages together

├── docker-compose.yml   # Postgres + Airflow (LocalExecutor)

└── raw_data/             # Timestamped raw JSON snapshots (gitignored in production use)
## Database schema

| Table | Purpose |
|---|---|
| `matches` | Fully resolved matches with both teams and scores |
| `pending_matches` | Future fixtures awaiting team determination |
| `quarantine` | Rejected records with failure reason + raw payload |
| `pipeline_logs` | Run-level metadata: timing, row counts, status |

## Author

Ahmed Nashaat — transitioning into Data Engineering, building hands-on projects to apply theory to real, current datasets.