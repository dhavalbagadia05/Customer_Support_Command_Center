-- Silver: group consecutive anomalous 15-minute windows (gap <= 30 min) of the same signature into anomaly events.
-- anomaly_id is stable once an event starts (first window + signature hash), so downstream AI enrichment runs once per event.
CREATE OR REFRESH MATERIALIZED VIEW silver_anomaly_events
COMMENT 'Ticket-surge anomaly events per issue_signature with first/last seen, peak volume and baseline.'
AS
WITH f AS (SELECT * FROM gold_signature_trends_15m WHERE is_anomalous),
g AS (
  SELECT *, CASE WHEN lag(ts_15m) OVER (PARTITION BY issue_signature ORDER BY ts_15m) >= ts_15m - INTERVAL 30 MINUTES THEN 0 ELSE 1 END AS new_evt
  FROM f
),
e AS (SELECT *, sum(new_evt) OVER (PARTITION BY issue_signature ORDER BY ts_15m) AS evt_no FROM g)
SELECT concat('ANM-', date_format(min(ts_15m), 'MMdd-HHmm'), '-', substr(sha1(issue_signature), 1, 4)) AS anomaly_id,
  issue_signature, issue_domain, min(ts_15m) AS first_seen, max(ts_15m) + INTERVAL 15 MINUTES AS last_seen,
  max(tickets_15m) AS peak_tickets_15m, max(surge_ratio) AS peak_surge_ratio, max(z_score) AS peak_z,
  round(avg(baseline_15m), 2) AS baseline_15m, count(*) AS anomalous_windows
FROM e
GROUP BY issue_signature, issue_domain, evt_no;
