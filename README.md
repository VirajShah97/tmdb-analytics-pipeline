# TMDB Genre ROI Analytics Pipeline

An end-to-end data pipeline that ingests movie data from the TMDB API, loads it into Snowflake, and transforms it using dbt to answer the question: **which film genres deliver the highest return on investment?**

## Architecture

```
EventBridge (6am UTC)
        │
        ▼
AWS Lambda (Python)
        │  Fetches popular movies + financial data from TMDB API
        │  Filters movies with no financial data
        │  Uploads as NDJSON
        ▼
AWS S3 (raw/movies/YYYY-MM-DD/movies_HH-MM-SS.json)
        │
        ▼
Snowpipe (AUTO_INGEST via SQS event notification)
        │  Appends new files automatically on arrival
        ▼
Snowflake: TMDB.RAW.MOVIES (VARIANT column)
        │
        ▼
dbt Cloud (7am UTC, 1hr after ingestion)
        │
        ├── stg__movies (view)
        │     Parses VARIANT JSON → typed columns
        │     Deduplicates via QUALIFY ROW_NUMBER()
        │     Treats budget/revenue = 0 as NULL
        │     Calculates ROI and profit
        │
        └── mart__genre_roi (table)
              LATERAL FLATTEN to explode genres array
              Aggregates ROI metrics by genre
              Filters genres with < 10 movies
```

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | AWS EventBridge |
| Ingestion | AWS Lambda (Python 3.12) |
| Raw Storage | AWS S3 |
| Auto-Ingest | Snowflake Snowpipe |
| Data Warehouse | Snowflake |
| Transformation | dbt Cloud |
| Testing | dbt tests + dbt_utils |

## Business Question

**Which film genres deliver the best return on investment?**

`mart__genre_roi` aggregates movies by genre and surfaces:

- `avg_roi` / `median_roi` — average and median return per dollar invested
- `portfolio_roi` — treats all movies in a genre as a single investment pool
- `pct_profitable` — share of movies that returned more than they cost
- `min_roi` / `max_roi` — range of outcomes within the genre
- `roi_rank` / `portfolio_roi_rank` — genre rankings by each ROI measure
- Financial totals: `total_budget`, `total_revenue`, `total_profit`
- Audience signals: `avg_vote_average`, `avg_popularity`

Genres with fewer than 10 movies are excluded to avoid misleading ROI from small samples.

> Note: multi-genre movies are counted in every genre they belong to after LATERAL FLATTEN — totals across genres will exceed raw movie counts by design.

## Key Design Decisions

**VARIANT column at raw layer** — raw data is stored as a single VARIANT column in Snowflake rather than parsed at load time. This means Snowpipe never breaks on API schema changes; parsing happens in dbt where it is version-controlled and covered by tests. If TMDB renames a field, a `not_null` test fails on the next dbt run rather than silently producing bad data.

**S3 as raw landing zone** — storing raw NDJSON in S3 before loading into Snowflake decouples ingestion from transformation and makes the pipeline replayable without re-hitting the API. Raw data is append-only and never modified.

**Deduplication in dbt, not at ingest** — Snowpipe appends every file that lands in S3, so duplicate movie records are expected at the raw layer. `stg__movies` deduplicates using `QUALIFY ROW_NUMBER()`, keeping the most recent copy of each movie.

**IAM role for Snowflake-to-S3** — Snowflake connects to S3 via a storage integration (IAM role assumption with temporary credentials) rather than hardcoded IAM user access keys. This follows the principle of least privilege and avoids storing permanent credentials in a third-party system.

## Data Quality

`dbt build` runs tests on every production execution:

- `not_null` and `unique` on primary keys
- `not_null` on all business-critical columns
- `dbt_utils.accepted_range` on numeric fields (e.g. `vote_average` between 0–10, `pct_profitable` between 0–1, `total_movies` ≥ 10)

## Medallion Architecture

| Layer | Location | Description |
|---|---|---|
| Bronze | S3 + `TMDB.RAW.MOVIES` | Raw NDJSON exactly as received from TMDB API |
| Silver | `TMDB.STAGING.STG__MOVIES` | Parsed, deduplicated, typed, ROI calculated |
| Gold | `TMDB.MARTS.MART__GENRE_ROI` | Genre-level aggregations for analysis |

## Pipeline Schedule

| Step | Schedule | Description |
|---|---|---|
| Lambda ingestion | 6:00 AM UTC daily | Fetches 5 pages (~100 movies) from TMDB |
| dbt Cloud build | 7:00 AM UTC daily | Transforms and tests all models |

## Setup

### Prerequisites
- AWS account with S3 and Lambda access
- Snowflake account
- TMDB API key (free at [themoviedb.org](https://www.themoviedb.org/))
- dbt Cloud account (free Developer tier)

### 1. Local Development

```bash
git clone https://github.com/VirajShah97/tmdb-analytics-pipeline
cd tmdb-analytics-pipeline
pip install -r requirements.txt
```

Create a `.env` file in the project root:
```
TMDB_API_KEY=your_key_here
S3_BUCKET=your-bucket-name
```

Run the ingestion script locally:
```bash
python ingestion/extract_tmdb_movies.py
```

### 2. AWS

1. Create an S3 bucket for raw data storage
2. Create an IAM role for Lambda with S3 write access
3. Package `ingestion/extract_tmdb_movies.py` and its dependencies into a zip and deploy as a Lambda function (Python 3.12, handler: `extract_tmdb_movies.main`)
4. Set `TMDB_API_KEY` and `S3_BUCKET` as Lambda environment variables
5. Set the Lambda timeout to at least 3 minutes
6. Add an EventBridge trigger with cron `0 6 * * ? *` to run daily at 6am UTC

### 3. Snowflake

Run [`infrastructure/snowflake_setup.sql`](infrastructure/snowflake_setup.sql) in order. The script sets up the warehouse, database, raw table, file format, storage integration, stage, and Snowpipe. Two steps require a context switch to AWS — these are marked `[ACTION REQUIRED]` in the script.

### 4. dbt Cloud

1. Create a free Developer account at [cloud.getdbt.com](https://cloud.getdbt.com)
2. Connect to this GitHub repo and your Snowflake account
3. Set the project subdirectory to `tmdb_analytics_pipeline/`
4. Create a production environment targeting the `MARTS` schema
5. Create a daily job with command `dbt build` scheduled at 7am UTC (1 hour after Lambda ingests)