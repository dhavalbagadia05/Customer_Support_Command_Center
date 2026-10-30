-- Gold: country x 5-minute platform pulse for the global heatmap
CREATE OR REFRESH MATERIALIZED VIEW gold_region_heatmap_5m
COMMENT 'Country x 5-minute platform pulse: streams vs release-aware baseline, tickets vs expected, anomaly_level, login/playback error rates, payment success, revenue at risk, worst domain health state.'
AS
WITH h AS (
  SELECT ts, country_code, sum(tickets) AS tickets, sum(tickets_15m) AS tickets_15m, sum(expected_tickets_15m) AS expected_tickets_15m,
    max_by(domain, ticket_ratio) AS top_domain,
    max(CASE health_state WHEN 'Customer-Detected' THEN 5 WHEN 'Known Incident' THEN 4 WHEN 'Telemetry Alert' THEN 3
                          WHEN 'Recovering' THEN 2 WHEN 'Watch' THEN 1 ELSE 0 END) AS worst_state_rank,
    max(CASE WHEN domain = 'Authentication' THEN error_rate END) AS login_failure_rate,
    max(CASE WHEN domain = 'Playback & CDN' THEN error_rate END) AS playback_error_rate
  FROM gold_service_health_country_5m GROUP BY ALL
),
p AS (
  SELECT ts, country_code, sum(successes) / nullif(sum(attempts), 0) AS payment_success_rate, sum(revenue_at_risk_usd) AS revenue_at_risk_usd
  FROM gold_payment_health_5m GROUP BY ALL
)
SELECT s.ts, s.country_code, r.country_name, r.region_group, r.lat, r.lon, r.subscribers,
  s.streams, s.baseline_streams, round(s.streams / s.baseline_streams - 1, 4) AS stream_deviation_pct, s.active_release,
  h.tickets, h.tickets_15m, round(h.expected_tickets_15m, 1) AS expected_tickets_15m,
  round(h.tickets_15m / (r.subscribers / 1e5), 2) AS tickets_15m_per_100k_subs,
  round((h.tickets_15m + 1) / (h.expected_tickets_15m + 1), 2) AS ticket_deviation_ratio,
  round((h.tickets_15m - h.expected_tickets_15m) / sqrt(h.expected_tickets_15m + 1), 2) AS ticket_z,
  h.top_domain,
  element_at(array('Healthy', 'Watch', 'Recovering', 'Telemetry Alert', 'Known Incident', 'Customer-Detected'), h.worst_state_rank + 1) AS worst_health_state,
  round(h.login_failure_rate, 5) AS login_failure_rate, round(h.playback_error_rate, 5) AS playback_error_rate,
  round(p.payment_success_rate, 4) AS payment_success_rate, round(p.revenue_at_risk_usd, 2) AS revenue_at_risk_usd,
  CASE WHEN (h.tickets_15m + 1) / (h.expected_tickets_15m + 1) >= 4 AND h.tickets_15m >= 15 THEN 'Critical'
       WHEN (h.tickets_15m + 1) / (h.expected_tickets_15m + 1) >= 2 AND h.tickets_15m >= 8 THEN 'Elevated' ELSE 'Normal' END AS anomaly_level
FROM silver_country_streams_5m s
JOIN bronze_regions r ON s.country_code = r.country_code
JOIN h ON s.ts = h.ts AND s.country_code = h.country_code
LEFT JOIN p ON s.ts = p.ts AND s.country_code = p.country_code;
