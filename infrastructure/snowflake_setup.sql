-- =============================================================================
-- TMDB Analytics Pipeline — Snowflake Setup
-- =============================================================================
-- Run these statements in order.
-- Steps marked [ACTION REQUIRED] require a context switch to AWS before proceeding.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Warehouse, Database & Schema
-- -----------------------------------------------------------------------------

CREATE WAREHOUSE IF NOT EXISTS TMDB_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for TMDB analytics pipeline';

CREATE DATABASE IF NOT EXISTS TMDB;
CREATE SCHEMA IF NOT EXISTS TMDB.RAW;

USE DATABASE TMDB;
USE SCHEMA RAW;
USE WAREHOUSE TMDB_WH;


-- -----------------------------------------------------------------------------
-- 2. Raw Table
-- -----------------------------------------------------------------------------

-- Single VARIANT column stores raw NDJSON exactly as received from Lambda.
-- Parsing happens downstream in dbt where it is version-controlled and tested.

CREATE TABLE IF NOT EXISTS TMDB.RAW.MOVIES (
    raw_data VARIANT,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- -----------------------------------------------------------------------------
-- 3. File Format
-- -----------------------------------------------------------------------------

-- STRIP_OUTER_ARRAY = FALSE: NDJSON has one JSON object per line, no outer array

CREATE OR REPLACE FILE FORMAT TMDB.RAW.NDJSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
    COMMENT = 'Format for newline-delimited JSON files from Lambda ingestion';


-- -----------------------------------------------------------------------------
-- 4. Storage Integration
-- -----------------------------------------------------------------------------

-- Connects Snowflake to S3 via IAM role assumption (temporary credentials).
-- Never store IAM user access keys directly in Snowflake.

CREATE OR REPLACE STORAGE INTEGRATION S3_TMDB_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<your_aws_account_id>:role/snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://<your_s3_bucket>/raw/movies/');

-- [ACTION REQUIRED] Then update the IAM role trust policy in AWS with the
-- STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID values returned below:
DESC INTEGRATION S3_TMDB_INTEGRATION;


-- -----------------------------------------------------------------------------
-- 5. External Stage
-- -----------------------------------------------------------------------------

CREATE OR REPLACE STAGE TMDB.RAW.S3_MOVIES_STAGE
    URL = 's3://<your_s3_bucket>/raw/movies/'
    STORAGE_INTEGRATION = S3_TMDB_INTEGRATION
    FILE_FORMAT = TMDB.RAW.NDJSON_FORMAT
    COMMENT = 'Stage pointing to S3 raw movies prefix';

-- Verify S3 connection and confirm files are visible:
LIST @TMDB.RAW.S3_MOVIES_STAGE;


-- -----------------------------------------------------------------------------
-- 6. Snowpipe
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PIPE TMDB.RAW.MOVIES_PIPE
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest pipe: loads new NDJSON files from S3 into TMDB.RAW.MOVIES'
    AS
    COPY INTO TMDB.RAW.MOVIES (raw_data)
    FROM (
        SELECT $1
        FROM @TMDB.RAW.S3_MOVIES_STAGE
    )
    FILE_FORMAT = (FORMAT_NAME = 'TMDB.RAW.NDJSON_FORMAT');

-- [ACTION REQUIRED] Then configure an S3 event notification (ObjectCreated)
-- on your bucket pointing to the SQS ARN returned in the notification_channel column:
SHOW PIPES;

-- After configuring the S3 event notification, backfill any files already in S3:
ALTER PIPE TMDB.RAW.MOVIES_PIPE REFRESH;

-- Verify pipe status:
SELECT SYSTEM$PIPE_STATUS('TMDB.RAW.MOVIES_PIPE');