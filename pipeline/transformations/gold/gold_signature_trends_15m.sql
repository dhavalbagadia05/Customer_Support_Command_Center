-- Gold: ticket counts per issue_signature per 15 minutes with an hour-of-day baseline (other days), z-score and anomaly flag
CREATE OR REFRESH MATERIALIZED VIEW gold_signature_trends_15m
COMMENT 'Ticket counts per issue_signature per 15 minutes with hour-of-day baseline, z_score, surge_ratio and is_anomalous flag.'
AS
WITH t AS (
  SELECT CAST(from_unixtime(floor(unix_timestamp(created_at) / 900) * 900) AS TIMESTAMP) AS ts_15m, issue_signature, issue_domain, count(*) AS cnt
  FROM silver_tickets GROUP BY ALL
),
hd AS (SELECT issue_signature, date(ts_15m) AS d, hour(ts_15m) AS h, sum(cnt) AS c FROM t GROUP BY ALL),
ht AS (SELECT issue_signature, h, sum(c) AS c FROM hd GROUP BY ALL),
b AS (
  SELECT t.*, greatest(0.3, (ht.c - coalesce(hd.c, 0)) / (14 * 4.0)) AS baseline_15m
  FROM t
  JOIN ht ON ht.issue_signature = t.issue_signature AND ht.h = hour(t.ts_15m)
  LEFT JOIN hd ON hd.issue_signature = t.issue_signature AND hd.d = date(t.ts_15m) AND hd.h = hour(t.ts_15m)
)
SELECT ts_15m, issue_signature, issue_domain, cnt AS tickets_15m, round(baseline_15m, 2) AS baseline_15m,
  round((cnt - baseline_15m) / sqrt(baseline_15m), 2) AS z_score, round(cnt / baseline_15m, 1) AS surge_ratio,
  (cnt >= 10 AND cnt >= 4 * baseline_15m AND (cnt - baseline_15m) / sqrt(baseline_15m) >= 5) AS is_anomalous
FROM b;
