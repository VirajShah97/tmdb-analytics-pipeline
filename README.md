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