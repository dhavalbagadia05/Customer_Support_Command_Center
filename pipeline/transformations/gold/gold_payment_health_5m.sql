-- Gold: payment gateway health per country x gateway x 5 minutes.
-- gateway_state is driven by gateway-side errors (not customer declines); revenue at risk only counts failures in degraded windows.
CREATE OR REFRESH MATERIALIZED VIEW gold_payment_health_5m
COMMENT 'Payment gateway health per country x gateway (Stripe, Adyen, PayPal, Apple IAP, Google Play) x 5 min with gateway_state and revenue_at_risk_usd.'
AS
WITH p AS (
  SELECT p.* EXCEPT (_ingested_at), r.region_group
  FROM bronze_payment_transactions p JOIN bronze_regions r ON p.country_code = r.country_code
),
b AS (SELECT country_code, gateway, sum(successes) / sum(attempts) AS baseline_success_rate
      FROM p WHERE date(ts) < '2026-10-24' GROUP BY ALL),
w AS (
  SELECT p.*, b.baseline_success_rate,
    sum(attempts) OVER x AS att_15m, sum(successes) OVER x AS succ_15m,
    sum(gateway_errors) OVER x AS gw_err_15m, sum(customer_declines) OVER x AS cust_dec_15m
  FROM p JOIN b USING (country_code, gateway)
  WINDOW x AS (PARTITION BY country_code, gateway ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
t AS (SELECT ts_5m AS ts, country_code, count(*) AS billing_tickets FROM silver_tickets WHERE issue_domain = 'Billing & Payments' GROUP BY ALL),
s AS (
  SELECT w.*,
    CASE WHEN gw_err_15m >= 4 AND gw_err_15m / att_15m >= 0.10 THEN 'Critical'
         WHEN gw_err_15m >= 3 AND gw_err_15m / att_15m >= 0.03 THEN 'Degraded' ELSE 'Healthy' END AS gateway_state
  FROM w
)
SELECT s.ts, s.country_code, s.region_group, s.gateway, attempts, successes, customer_declines, gateway_errors,
  round(successes / nullif(attempts, 0), 4) AS success_rate, round(succ_15m / nullif(att_15m, 0), 4) AS success_rate_15m,
  round(baseline_success_rate, 4) AS baseline_success_rate, round(gw_err_15m / nullif(att_15m, 0), 4) AS gateway_error_rate_15m,
  gw_err_15m, cust_dec_15m,
  CASE WHEN gateway_state <> 'Healthy' THEN round(gateway_errors * avg_price_usd, 2) ELSE 0 END AS revenue_at_risk_usd,
  coalesce(t.billing_tickets, 0) AS country_billing_tickets, gateway_state
FROM s LEFT JOIN t USING (ts, country_code);
