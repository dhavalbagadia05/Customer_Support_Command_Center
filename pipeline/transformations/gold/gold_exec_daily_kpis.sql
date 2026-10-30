-- Gold: executive daily KPIs by region_group
CREATE OR REFRESH MATERIALIZED VIEW gold_exec_daily_kpis
COMMENT 'Executive daily KPIs by region_group: tickets, contact rate, first response time, SLA, CSAT, reopen rate, peak streams, frustration, customer-detected minutes, payment success, revenue at risk, negative social share, incidents declared.'
AS
WITH t AS (
  SELECT date(created_at) AS d, region_group, count(*) AS tickets, avg(first_response_minutes) AS avg_first_response_min,
    percentile_approx(first_response_minutes, 0.9) AS p90_first_response_min, avg(CAST(sla_met AS INT)) AS sla_met_rate,
    avg(csat_score) AS avg_csat, avg(CAST(reopened AS INT)) AS reopen_rate,
    avg(CASE WHEN subscriber_tier IN ('Mega Fan', 'Ultimate Fan') THEN 1 ELSE 0 END) AS premium_ticket_share
  FROM silver_tickets GROUP BY ALL
),
s AS (SELECT date(s.ts) AS d, r.region_group, sum(s.streams) AS stream_intervals
      FROM silver_country_streams_5m s JOIN bronze_regions r ON s.country_code = r.country_code GROUP BY ALL),
sp AS (SELECT date(ts) AS d, scope AS region_group, max(streams) AS peak_concurrent_streams,
         avg(frustration_index) AS avg_frustration_index, max(frustration_index) AS max_frustration_index
       FROM gold_frustration_index_5m WHERE scope_type = 'Region' GROUP BY ALL),
h AS (SELECT date(ts) AS d, region_group,
        5 * sum(CASE WHEN health_state = 'Customer-Detected' THEN 1 ELSE 0 END) AS customer_detected_minutes,
        5 * sum(CASE WHEN health_state = 'Known Incident' THEN 1 ELSE 0 END) AS known_incident_minutes
      FROM gold_service_health_region_5m GROUP BY ALL),
p AS (SELECT date(ts) AS d, region_group, sum(successes) / sum(attempts) AS payment_success_rate, sum(revenue_at_risk_usd) AS revenue_at_risk_usd
      FROM gold_payment_health_5m GROUP BY ALL),
so AS (SELECT date(posted_at) AS d, region_group, count(*) AS social_posts, avg(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_social_share
       FROM gold_social_posts WHERE region_group IS NOT NULL GROUP BY ALL),
inc AS (SELECT date(declared_at) AS d, rg AS region_group, count(*) AS incidents_declared
        FROM bronze_incidents LATERAL VIEW explode(split(region_groups, ',')) x AS rg GROUP BY ALL)
SELECT t.d AS kpi_date, t.region_group, t.tickets, round(t.tickets / (s.stream_intervals / 1e4), 3) AS contacts_per_10k_stream_intervals,
  round(t.avg_first_response_min, 1) AS avg_first_response_min, round(t.p90_first_response_min, 1) AS p90_first_response_min,
  round(t.sla_met_rate, 3) AS sla_met_rate, round(t.avg_csat, 2) AS avg_csat, round(t.reopen_rate, 3) AS reopen_rate,
  round(t.premium_ticket_share, 3) AS premium_ticket_share,
  sp.peak_concurrent_streams, round(sp.avg_frustration_index, 1) AS avg_frustration_index, sp.max_frustration_index,
  coalesce(h.customer_detected_minutes, 0) AS customer_detected_minutes, coalesce(h.known_incident_minutes, 0) AS known_incident_minutes,
  round(p.payment_success_rate, 4) AS payment_success_rate, round(p.revenue_at_risk_usd, 2) AS revenue_at_risk_usd,
  coalesce(so.social_posts, 0) AS social_posts, round(so.negative_social_share, 3) AS negative_social_share,
  coalesce(inc.incidents_declared, 0) AS incidents_declared
FROM t
JOIN s ON t.d = s.d AND t.region_group = s.region_group
JOIN sp ON t.d = sp.d AND t.region_group = sp.region_group
LEFT JOIN h ON t.d = h.d AND t.region_group = h.region_group
LEFT JOIN p ON t.d = p.d AND t.region_group = p.region_group
LEFT JOIN so ON t.d = so.d AND t.region_group = so.region_group
LEFT JOIN inc ON t.d = inc.d AND t.region_group = inc.region_group;
