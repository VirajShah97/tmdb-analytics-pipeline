# TMDB Genre ROI Analytics Pipeline

Not all genres are created equal — a Horror film can turn a $5M budget into a $100M hit, while a big-budget Action film might barely break even. This pipeline ingests daily movie data from TMDB's popular movies feed, loads it into Snowflake, and transforms it using dbt to answer: **which film genres deliver the highest return on investment?** Because the data comes from TMDB's popular movies feed, the analysis reflects current trends rather than all-time history.

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
AWS S3 (raw/movies/YYYY-MM-DD/movies_HH-MM-SS.json)        ← Bronze layer
        │
        ▼
Snowpipe (AUTO_INGEST via SQS event notification)
        │  Appends new files automatically on arrival
        ▼
Snowflake: TMDB.RAW.MOVIES (VARIANT column)                 ← Bronze layer
        │
        ▼
dbt Cloud (7am UTC, 1hr after ingestion)
        │
        ├── stg__movies (view)                               ← Silver layer
        │     Parses VARIANT JSON → typed columns
        │     Deduplicates via QUALIFY ROW_NUMBER()
        │     Treats budget/revenue = 0 as NULL
        │     Calculates ROI and profit
        │     Tests: not_null, unique, accepted_range
        │
        └── mart__genre_roi (table)                          ← Gold layer
              LATERAL FLATTEN to explode genres array
              Aggregates ROI metrics by genre
              Filters genres with < 10 movies
              Tests: not_null, accepted_range
```

## Business Question

**Which film genres deliver the best return on investment?**

Each genre is evaluated across multiple ROI lenses — average ROI captures the typical film's performance, median ROI removes the distortion of outliers, and portfolio ROI treats the entire genre as a single investment to reflect how a studio allocating budget across a genre would actually perform. The share of movies that turned a profit rounds out the picture. Genres with fewer than 10 movies are excluded to keep the results statistically meaningful.

## Key Design Decisions

**VARIANT column at raw layer** — raw data is stored as a single VARIANT column in Snowflake rather than parsed at load time. This means Snowpipe never breaks on API schema changes; parsing happens in dbt where it is version-controlled and covered by tests. If TMDB renames a field, a `not_null` test fails on the next dbt run rather than silently producing bad data.

**S3 as raw landing zone** — storing raw NDJSON in S3 before loading into Snowflake decouples ingestion from transformation and makes the pipeline replayable without re-hitting the API. Raw data is append-only and never modified.

**Deduplication in dbt, not at ingest** — Snowpipe appends every file that lands in S3, so duplicate movie records are expected at the raw layer. `stg__movies` deduplicates using `QUALIFY ROW_NUMBER()`, keeping the most recent copy of each movie.

**IAM role for Snowflake-to-S3** — Snowflake connects to S3 via a storage integration (IAM role assumption with temporary credentials) rather than hardcoded IAM user access keys. This follows the principle of least privilege and avoids storing permanent credentials in a third-party system.

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