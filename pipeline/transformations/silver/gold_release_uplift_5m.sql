-- Expected traffic uplift from simulcast releases for every 5-minute interval (ramp-up 30 min, decay ~80 min, 6h horizon)
CREATE OR REFRESH MATERIALIZED VIEW gold_release_uplift_5m
COMMENT 'Release-calendar expected traffic uplift per 5-minute interval and the active release label.'
AS
WITH t AS (SELECT DISTINCT ts FROM bronze_concurrent_streams),
j AS (
  SELECT t.ts, r.show_title, r.episode_number, r.release_type, r.expected_uplift,
    (unix_timestamp(t.ts) - unix_timestamp(r.release_ts)) / 60.0 AS m
  FROM t LEFT JOIN bronze_release_schedule r
    ON t.ts >= r.release_ts AND t.ts <= r.release_ts + INTERVAL 360 MINUTES
)
SELECT ts,
  coalesce(sum(expected_uplift * least(1, (m + 5) / 30) * exp(-greatest(0, m - 30) / 80)), 0) AS expected_release_uplift,
  max_by(concat(show_title, ' E', episode_number, CASE WHEN release_type = 'Season Finale' THEN ' (Finale)' ELSE '' END), expected_uplift) AS active_release
FROM j
GROUP BY ts;
