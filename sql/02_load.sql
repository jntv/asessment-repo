-- Clean analytics tables (filter nulls, zero/negative rates, missing codes)
CREATE OR REPLACE TABLE uhc_tic.analytics.rates AS
SELECT *
FROM uhc_tic.staging.rates_raw
WHERE negotiated_rate IS NOT NULL
  AND negotiated_rate > 0
  AND billing_code IS NOT NULL;

-- Distinct billing codes for reference
CREATE OR REPLACE TABLE uhc_tic.analytics.codes AS
SELECT DISTINCT
  billing_code_type, billing_code, service_name, service_description
FROM uhc_tic.analytics.rates;

-- Row counts for validation
SELECT
  'staging.rates_raw' as table_name, COUNT(*) as row_count
FROM uhc_tic.staging.rates_raw
UNION ALL
SELECT
  'analytics.rates', COUNT(*)
FROM uhc_tic.analytics.rates
UNION ALL
SELECT
  'analytics.codes', COUNT(*)
FROM uhc_tic.analytics.codes;
