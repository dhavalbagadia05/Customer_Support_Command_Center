-- Gold: service health matrix at region_group (NA, LATAM, EMEA, APAC) x engineering domain x 5 minutes.
-- Same rules as gold_service_health_country_5m, aggregated to region groups.
CREATE OR REFRESH MATERIALIZED VIEW gold_service_health_region_5m
COMMENT 'Service health matrix: engineering domain x region_group x 5 minutes. health_state: Healthy, Watch, Customer-Detected, Telemetry Alert, Known Incident, Recovering.'
AS
WITH sm AS (
  SELECT m.ts, r.region_group AS scope, m.domain, sum(m.request_count) AS req,
    sum(m.error_rate * m.request_count) / sum(m.request_count) AS error_rate, avg(m.p95_latency_ms) AS p95_latency_ms
  FROM bronze_service_metrics m JOIN bronze_regions r ON m.country_code = r.country_code GROUP BY ALL
),
st AS (SELECT s.ts, r.region_group AS scope, sum(s.streams) AS streams
       FROM silver_country_streams_5m s JOIN bronze_regions r ON s.country_code = r.country_code GROUP BY ALL),
tk AS (SELECT ts_5m AS ts, region_group AS scope, issue_domain AS domain, count(*) AS tickets FROM silver_tickets GROUP BY ALL),
j AS (SELECT sm.*, st.streams, coalesce(tk.tickets, 0) AS tickets
      FROM sm JOIN st USING (ts, scope) LEFT JOIN tk USING (ts, scope, domain)),
daily AS (SELECT scope, domain, date(ts) AS d, sum(tickets) / sum(streams) AS cr FROM j GROUP BY ALL),
bl AS (SELECT scope, domain, percentile_approx(cr, 0.5) AS contact_rate_baseline FROM daily GROUP BY ALL),
eb AS (SELECT scope, domain, percentile_approx(error_rate, 0.5) AS error_rate_baseline FROM j GROUP BY ALL),
r AS (
  SELECT j.*, bl.contact_rate_baseline, eb.error_rate_baseline,
    sum(tickets) OVER w AS tickets_15m, sum(streams) OVER w AS streams_15m, avg(error_rate) OVER w AS error_rate_15m
  FROM j JOIN bl USING (scope, domain) JOIN eb USING (scope, domain)
  WINDOW w AS (PARTITION BY scope, domain ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
m AS (
  SELECT *, streams_15m * contact_rate_baseline AS expected_tickets_15m,
    (tickets_15m + 1) / (streams_15m * contact_rate_baseline + 1) AS ticket_ratio,
    (tickets_15m - streams_15m * contact_rate_baseline) / sqrt(streams_15m * contact_rate_baseline + 1) AS ticket_z,
    error_rate_15m / error_rate_baseline AS error_ratio
  FROM r
),
h AS (
  SELECT m.*,
    (tickets_15m >= 8 AND ticket_ratio >= 2.5 AND ticket_z >= 4) AS customer_signal,
    (tickets_15m >= 20 AND ticket_ratio >= 5) AS customer_signal_severe,
    (error_ratio >= 3) AS telemetry_signal,
    (error_ratio >= 8) AS telemetry_signal_severe
  FROM m
),
hi AS (
  SELECT h.*,
    max_by(i.incident_id, i.declared_at) FILTER (WHERE h.ts >= i.declared_at AND h.ts < i.resolved_at) AS open_incident_id,
    max_by(i.incident_id, i.declared_at) FILTER (WHERE h.ts >= i.resolved_at AND h.ts < i.resolved_at + INTERVAL 30 MINUTES) AS recovering_incident_id
  FROM h LEFT JOIN bronze_incidents i
    ON i.domain = h.domain AND array_contains(split(i.region_groups, ','), h.scope)
   AND h.ts >= i.declared_at - INTERVAL 1 MINUTE AND h.ts < i.resolved_at + INTERVAL 30 MINUTES
  GROUP BY ALL
)
SELECT ts, scope AS region_group, domain, streams, req AS request_count, round(error_rate, 5) AS error_rate, round(error_rate_baseline, 5) AS error_rate_baseline,
  round(error_ratio, 2) AS error_ratio, round(p95_latency_ms, 1) AS p95_latency_ms, tickets, tickets_15m, round(expected_tickets_15m, 2) AS expected_tickets_15m,
  round(ticket_ratio, 2) AS ticket_ratio, round(ticket_z, 2) AS ticket_z, customer_signal, telemetry_signal, open_incident_id, recovering_incident_id,
  CASE WHEN open_incident_id IS NOT NULL THEN 'Known Incident'
       WHEN recovering_incident_id IS NOT NULL AND (customer_signal OR telemetry_signal) THEN 'Recovering'
       WHEN customer_signal AND NOT telemetry_signal THEN 'Customer-Detected'
       WHEN telemetry_signal THEN 'Telemetry Alert'
       WHEN tickets_15m >= 5 AND ticket_ratio >= 1.8 THEN 'Watch'
       ELSE 'Healthy' END AS health_state,
  CASE WHEN customer_signal_severe OR telemetry_signal_severe THEN 'Outage'
       WHEN customer_signal OR telemetry_signal THEN 'Degraded' ELSE 'Normal' END AS severity,
  greatest(0, round(100 - least(60, 12 * greatest(0, ticket_ratio - 1)) - least(40, 6 * greatest(0, error_ratio - 1)))) AS health_score
FROM hi;
