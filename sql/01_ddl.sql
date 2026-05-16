-- Create database and schemas
CREATE OR REPLACE DATABASE uhc_tic;
CREATE OR REPLACE SCHEMA uhc_tic.staging;
CREATE OR REPLACE SCHEMA uhc_tic.analytics;

-- Staging table for raw loaded data
CREATE OR REPLACE TABLE uhc_tic.staging.rates_raw (
  plan_name STRING,
  plan_id STRING,
  plan_market_type STRING,
  reporting_entity_name STRING,
  last_updated_on DATE,
  billing_code_type STRING,
  billing_code STRING,
  service_name STRING,
  service_description STRING,
  negotiation_arrangement STRING,
  tin_type STRING,
  tin_value STRING,
  npi_count NUMBER,
  npi_sample STRING,
  negotiated_type STRING,
  negotiated_rate NUMBER(20,4),
  billing_class STRING,
  expiration_date DATE,
  service_codes STRING,
  modifiers STRING,
  source_file STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- File format for parquet
CREATE OR REPLACE FILE FORMAT uhc_tic.staging.pq_fmt
  TYPE = PARQUET;

-- Internal stage for parquet uploads (used if STORAGE_MODE=LOCAL)
CREATE OR REPLACE STAGE uhc_tic.staging.pq_stage
  FILE_FORMAT = uhc_tic.staging.pq_fmt;

-- ============================================================================
-- S3 EXTERNAL STAGE SETUP (only if STORAGE_MODE=S3)
-- ============================================================================
-- If using S3 storage mode:
-- 1. Replace YOUR_AWS_ACCOUNT_ID with your actual AWS account ID
-- 2. Replace your-uhc-bucket with your actual S3 bucket name
-- 3. Uncomment the SQL below and run it in Snowflake
-- 4. Update SF_S3_ROLE in .env with the created storage integration ARN
-- 5. Run: STORAGE_MODE=S3 python src/load_snowflake.py

-- CREATE OR REPLACE STORAGE INTEGRATION s3_int
--   TYPE = EXTERNAL_STAGE
--   STORAGE_PROVIDER = 'S3'
--   ENABLED = TRUE
--   STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/snowflake-s3-role'
--   STORAGE_ALLOWED_LOCATIONS = ('s3://your-uhc-bucket/parquet/');
--
-- CREATE OR REPLACE STAGE s3_stage
--   STORAGE_INTEGRATION = s3_int
--   URL = 's3://your-uhc-bucket/parquet/'
--   FILE_FORMAT = uhc_tic.staging.pq_fmt;
