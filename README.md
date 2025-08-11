# Retail ETL Pipeline (PwC Data Engineering Challenge)

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5+-orange.svg)](https://spark.apache.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green.svg)](https://fastapi.tiangolo.com/)

## Overview

End-to-end data platform implementing the Medallion (Bronze/Silver/Gold) architecture with PySpark, a star schema warehouse (SQLite in dev, PostgreSQL-ready), and a FastAPI service to query curated sales data. Typesense vector search is prepared for product embeddings.

## Architecture

```mermaid
graph LR
  A[Raw Data (CSV)] --> B[Bronze (Delta)]
  B --> C[Silver (Delta)]
  C --> D[Gold (Star Schema)]
  D --> E[(SQLite/PostgreSQL)]
  E --> F[FastAPI]
  D --> G[Typesense]
  G --> F
```

## Tech Stack
- PySpark 3.5 with Delta Lake for transformations
- SQLModel + SQLAlchemy for warehouse schema
- FastAPI + Pydantic for API layer and validation
- Typesense for vector/text search (optional)
- Docker Compose for orchestration
- Pytest, Ruff, Black, Mypy for quality

## Prerequisites
- Python 3.10+
- Poetry 1.8+
- Docker & Docker Compose (recommended)
- 8GB+ RAM (Spark)

## Setup
1) Create `.env` (or copy from example):
```bash
cp .env.example .env
```
Minimal required values:
```
ENVIRONMENT=development
DATABASE_TYPE=sqlite
DATABASE_URL=sqlite:///./data/warehouse/retail.db
SPARK_MASTER=local[*]
BASIC_AUTH_USERNAME=admin
BASIC_AUTH_PASSWORD=changeme123
TYPESENSE_API_KEY=xyz123changeme
```

2) Provide input data in `data/raw/` (CSV). Example file:
```
data/raw/sample.csv
invoice_no,stock_code,description,quantity,unit_price,invoice_timestamp,customer_id,country
536365,85123A,White Hanging Heart T-Light Holder,6,2.55,2010-12-01T08:26:00,10001,United Kingdom
536366,71053,White Metal Lantern,6,3.39,2010-12-01T08:28:00,10002,France
```

Quick create (PowerShell):
```powershell
New-Item -ItemType Directory -Force data/raw | Out-Null; @(
  'invoice_no,stock_code,description,quantity,unit_price,invoice_timestamp,customer_id,country',
  '536365,85123A,White Hanging Heart T-Light Holder,6,2.55,2010-12-01T08:26:00,10001,United Kingdom',
  '536366,71053,White Metal Lantern,6,3.39,2010-12-01T08:28:00,10002,France'
) | Set-Content data/raw/sample.csv

Note: By default, Silver validation requires non-cancelled rows to have a numeric `customer_id`. Use numeric IDs like `10001`.
```

Optional: seed demo data (skips if facts exist):
```bash
poetry run python scripts/seed_data.py
```

## Quick Start

### A) Docker (recommended)
```bash
docker compose up -d --build typesense api
# If your Compose supports profiles directly
docker compose --profile etl run --rm etl-all
# Otherwise on PowerShell, enable the profile via env var
$env:COMPOSE_PROFILES = 'etl'
docker compose run --rm etl-all
```
Validate:
```bash
curl http://localhost:8000/api/v1/health
# On Windows PowerShell, use curl.exe or Invoke-RestMethod for Basic Auth
curl.exe -u admin:changeme123 "http://localhost:8000/api/v1/sales?page=1&size=10"
```

Tips:
- Subsequent builds are much faster. `.dockerignore` reduces build context.
- Tail API logs: `docker compose logs -f api`

### B) Local (Poetry)
```bash
poetry install
poetry run python scripts/run_etl.py   # Bronze -> Silver -> Gold
poetry run uvicorn de_challenge.api.main:app --host 0.0.0.0 --port 8000
```
Local ETL requires Java 17. Install Temurin JDK 17 and set JAVA_HOME in PowerShell for the session:
```powershell
winget install --id EclipseAdoptium.Temurin.17.JDK -e --accept-source-agreements --accept-package-agreements
$jdk = Get-ChildItem 'C:\\Program Files\\Eclipse Adoptium\\' -Directory | Where-Object { $_.Name -like 'jdk-17*' } | Select-Object -First 1 -ExpandProperty FullName
$env:JAVA_HOME = $jdk; $env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```
Validate:
```bash
curl http://127.0.0.1:8000/api/v1/health
curl -u admin:changeme123 "http://127.0.0.1:8000/api/v1/sales?page=1&size=10"
```

## ETL Pipeline
- Bronze: Ingest raw CSV to Delta at `data/bronze/sales/` (no transforms; adds metadata and partitions by `ingestion_date`)
- Silver: Clean, validate (Pydantic), deduplicate; write Delta at `data/silver/sales/`
- Gold: Build star schema and load `FactSale` + dimensions into the warehouse via JDBC

Make targets:
```bash
make etl-bronze
make etl-silver
make etl-gold
make etl-full     # scripts/run_etl.py
make run-api
```

## API
- Auth: HTTP Basic via `.env` (`BASIC_AUTH_USERNAME`, `BASIC_AUTH_PASSWORD`)
- Health: `GET /api/v1/health`
- Sales: `GET /api/v1/sales?page=1&size=10&product=85123A&country=United%20Kingdom`

More examples:
```bash
# Filter by date range (ISO8601)
curl -u admin:changeme123 "http://localhost:8000/api/v1/sales?date_from=2010-12-01T00:00:00&date_to=2010-12-02T00:00:00&page=1&size=20"
# Sort by total ascending
curl -u admin:changeme123 "http://localhost:8000/api/v1/sales?product=85123A&sort=total:asc&page=1&size=10"
```

Response excerpt:
```json
{
  "items": [
    {
      "invoice_no": "536365",
      "stock_code": "85123A",
      "description": "White Hanging Heart T-Light Holder",
      "quantity": 6,
      "invoice_date": "2010-12-01T08:26:00",
      "unit_price": 2.55,
      "customer_id": "CUST-0001",
      "country": "United Kingdom",
      "total": 15.3,
      "total_str": "15.30"
    }
  ],
  "total": 2,
  "page": 1,
  "size": 10
}
```

## Testing & Quality
```bash
make test
make lint
make type-check
```

## Troubleshooting
- 401 on `/api/v1/sales`: include `-u user:pass` with Basic Auth
- Docker first build slow on Windows/OneDrive → use `.dockerignore` and retry
- SQLite locked: avoid concurrent writers; close open DB viewers
 - Spark first run downloads JDBC drivers; re-run if it fails once
- PowerShell `curl`: use `curl.exe` or `Invoke-RestMethod` (as `curl` is an alias to `Invoke-WebRequest` and `-u` is ambiguous)
- Silver empty: your data likely failed validation (e.g., non-numeric `customer_id`). Use numeric IDs or relax the rule.

## Switch to PostgreSQL / Supabase
- Set in `.env`:
```
DATABASE_TYPE=postgresql
# Standard Postgres
DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
# Supabase (require SSL)
# DATABASE_URL=postgresql://USER:PASSWORD@HOST:6543/DBNAME?sslmode=require
```
- Spark JDBC drivers for Postgres are already configured. Gold will write to Postgres automatically via JDBC.
- Ensure the database is reachable from the container/host and the user has DDL/DML permissions. For Supabase, keep `sslmode=require`.

## License
Part of the PwC Data Engineering Challenge.