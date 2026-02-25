# Crypto Market Data ETL Pipeline

## Overview

An end-to-end Data Engineering pipeline that extracts, transforms, and loads cryptocurrency market data from the CoinGecko API into Google BigQuery, following the **Medallion Architecture** (Bronze → Silver → Gold).

The primary goal is to build a reliable, structured history of market metrics (price, volume, market cap, ROI) for Business Intelligence (BI) and Data Science applications.

## Architecture

<p align="center">
  <img src="./assets/architeture-diagram.png" alt="Project Architecture Diagram">
</p>

### Data Flow

1. **Extract → Bronze:** Fetches raw JSON from the CoinGecko API with retry/backoff strategy and stores it partitioned by date in GCS (`bronze/YYYY-MM-DD/`).
2. **Transform → Silver:** Normalizes the JSON, renames columns, enforces a PyArrow schema (with proper `NUMERIC`/`BIGNUMERIC` precision), and saves as Parquet in GCS (`silver/crypto_market/YYYY-MM-DD.parquet`).
3. **Load → Gold:** Loads the Parquet file into a partitioned and clustered BigQuery table, replacing only the target date partition (idempotent).

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12+ |
| Cloud Provider | Google Cloud Platform (GCP) |
| Data Lake | Google Cloud Storage (GCS) |
| Data Warehouse | Google BigQuery |
| Data Processing | Pandas + PyArrow |
| Logging | RotatingFileHandler + Google Cloud Logging |
| Orchestration | Prepared for Apache Airflow |
| Version Control | GitHub |

---

## Project Structure

```
crypto-api-to-bigquery-etl/
├── src/
│   ├── utils/
│   │   ├── config.py          # env vars, column mapping, decimal columns
│   │   ├── schema.py          # PyArrow schema, BigQuery schema, job config
│   │   ├── gcp_utils.py       # GCS and BigQuery client helpers
│   │   └── logging_config.py  # RotatingFileHandler + Cloud Logging setup
│   ├── extract.py             # API extraction with retry strategy
│   ├── transform.py           # Bronze → Silver transformation
│   ├── load.py                # Silver → Gold BigQuery load
│   └── main.py                # Pipeline entry point
├── assets/
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Module Details

### `main.py` — Entry Point
Orchestrates the full pipeline for a given `target_date`, ensuring **idempotency** and enabling backfill of historical dates.

### `extract.py` — Bronze Layer
- Implements `HTTPAdapter` with `Retry` strategy for rate limit (429) and server error resilience.
- Saves raw JSON to GCS with date partitioning.

### `transform.py` — Silver Layer
- Normalizes nested JSON (`roi_times`, `roi_currency`, `roi_percentage`) via `pd.json_normalize`.
- Renames columns to a standardized schema (`COLUMNS_MAPPING`).
- Converts decimal columns to `Decimal` with proper precision using `ROUND_HALF_UP`.
- Converts timestamp columns to UTC-aware datetime.
- Enforces PyArrow schema before writing Parquet to GCS.

### `load.py` — Gold Layer
- Loads Parquet from GCS directly into BigQuery (no intermediate download).
- Uses `WRITE_TRUNCATE` scoped to the target partition (`table$YYYYMMDD`) for safe re-runs.
- Table is partitioned by `reference_dt` (DAY) and clustered by `coin_id`.

---

## Schema Design

| Column | BigQuery Type | Notes |
|---|---|---|
| `coin_id` | STRING | |
| `market_cap_rank` | INT64 | |
| `current_price_usd` | BIGNUMERIC | High precision for low-value coins (e.g. SHIB) |
| `market_cap_usd` | NUMERIC | |
| `circulating_supply` | BIGNUMERIC | Large integer precision |
| `all_time_high_usd` | BIGNUMERIC | |
| `all_time_low_usd` | BIGNUMERIC | |
| `*_pct` columns | NUMERIC | Percentage changes |
| `*_date` columns | TIMESTAMP | UTC |
| `reference_dt` | DATE | Partition key |

---

## How to Run

### 1. Clone and install

```bash
git clone https://github.com/ndrero/crypto-api-to-bigquery-etl.git
cd crypto-api-to-bigquery-etl
pip install -r requirements.txt
```

### 2. Configure environment

Create a `.env` file based on `.env.example`:

```env
API_KEY=your_coingecko_key
GCP_PROJECT_ID=your_gcp_project
BUCKET_NAME=your_gcs_bucket
DATASET=your_bq_dataset
TABLE_NAME=your_bq_table
CREDENTIALS_PATH=path/to/service-account.json
```

### 3. Run

```bash
python src/main.py
```

Logs are written to `/logs/job.log` with automatic rotation (10MB, 3 backups) and streamed to Google Cloud Logging.

---

## Roadmap

- [ ] Full orchestration with **Apache Airflow**
- [ ] Infrastructure as Code with **Terraform**
- [ ] Data Quality checks with **Great Expectations**
- [ ] Unit tests with **pytest**