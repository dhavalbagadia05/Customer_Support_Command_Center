-- Silver: concurrent streams per country x 5 minutes with a release-calendar-aware baseline
-- baseline = average de-seasonalised streams at the same time-of-day on all other days x (1 + expected uplift now)
CREATE OR REFRESH MATERIALIZED VIEW silver_country_streams_5m
COMMENT 'Country-level concurrent streams with release-aware baseline at 5-minute grain.'
AS
WITH s AS (
  SELECT ts, country_code, sum(concurrent_streams) AS streams, sum(playback_start_failures) AS playback_start_failures
  FROM bronze_concurrent_streams GROUP BY ALL
),
d AS (
  SELECT s.*, u.expected_release_uplift, u.active_release,
    s.streams / (1 + u.expected_release_uplift) AS deseason,
    hour(s.ts) * 60 + minute(s.ts) AS slot
  FROM s JOIN gold_release_uplift_5m u ON s.ts = u.ts
)
SELECT ts, country_code, streams, playback_start_failures, expected_release_uplift, active_release,
  round((sum(deseason) OVER (PARTITION BY country_code, slot) - deseason)
        / (count(*) OVER (PARTITION BY country_code, slot) - 1) * (1 + expected_release_uplift)) AS baseline_streams
FROM d;
