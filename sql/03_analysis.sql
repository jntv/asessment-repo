-- INSIGHT 1: Price Variation for Common Procedures
-- Shows how much negotiated rates vary for the same CPT code across providers

WITH common_codes AS (
  SELECT billing_code, COUNT(*) as cnt
  FROM uhc_tic.analytics.rates
  WHERE billing_code_type = 'CPT'
  GROUP BY 1
  HAVING COUNT(*) > 100
  ORDER BY cnt DESC
  LIMIT 50
)
SELECT
  r.billing_code,
  r.service_name,
  MIN(r.negotiated_rate) AS price_min,
  APPROX_PERCENTILE(r.negotiated_rate, 0.25) AS price_q1,
  APPROX_PERCENTILE(r.negotiated_rate, 0.5) AS price_median,
  APPROX_PERCENTILE(r.negotiated_rate, 0.75) AS price_q3,
  APPROX_PERCENTILE(r.negotiated_rate, 0.95) AS price_p95,
  MAX(r.negotiated_rate) AS price_max,
  MAX(r.negotiated_rate) / NULLIF(MIN(r.negotiated_rate), 0) AS ratio_max_min,
  COUNT(*) as provider_count
FROM uhc_tic.analytics.rates r
JOIN common_codes c ON r.billing_code = c.billing_code
GROUP BY 1, 2
ORDER BY ratio_max_min DESC;


-- INSIGHT 2: Negotiation Arrangement Mix
-- Breakdown of contract structures (Fee-for-Service vs Bundle vs Capitation)

SELECT
  negotiation_arrangement,
  COUNT(*) as row_count,
  COUNT(DISTINCT plan_id) as plan_count,
  COUNT(DISTINCT plan_id || '|' || billing_code) as code_plan_combinations,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_total
FROM uhc_tic.analytics.rates
GROUP BY 1
ORDER BY row_count DESC;


-- INSIGHT 3: Plan-Level Concentration
-- How many distinct codes and providers per plan

SELECT
  plan_name,
  plan_id,
  COUNT(DISTINCT billing_code) as distinct_codes,
  COUNT(DISTINCT npi_sample) as approx_providers,
  COUNT(*) as total_rates,
  APPROX_PERCENTILE(negotiated_rate, 0.5) as median_rate,
  MIN(negotiated_rate) as min_rate,
  MAX(negotiated_rate) as max_rate
FROM uhc_tic.analytics.rates
WHERE npi_sample IS NOT NULL
GROUP BY 1, 2
ORDER BY distinct_codes DESC;


-- INSIGHT 4: Outliers and Data Quality
-- Identify suspicious rates (very cheap, very expensive, or missing data)

SELECT
  CASE
    WHEN negotiated_rate < 1 THEN 'penny_rates (<$1)'
    WHEN negotiated_rate BETWEEN 1 AND 10 THEN 'low_rates ($1-$10)'
    WHEN negotiated_rate > 100000 THEN 'outlier_high (>$100k)'
    WHEN service_description IS NULL THEN 'missing_description'
    ELSE 'normal'
  END as category,
  COUNT(*) as count,
  COUNT(DISTINCT plan_id) as plans_affected,
  MIN(negotiated_rate) as rate_min,
  MAX(negotiated_rate) as rate_max
FROM uhc_tic.analytics.rates
GROUP BY 1
ORDER BY count DESC;
