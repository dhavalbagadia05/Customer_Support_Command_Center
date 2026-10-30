-- Gold: real-time consumer frustration at Global / Region / Country scope.
-- frustration_index (0-100) = 55% contact-rate excess + 25% negative social volume + 20% stream drop vs baseline
-- divergence_flag = streams >= 8% below baseline while contacts >= 2x expected (users hitting breakage)
CREATE OR REFRESH MATERIALIZED VIEW gold_frustration_index_5m
COMMENT 'Consumer frustration index and stream/contact divergence by scope_type (Global, Region, Country) at 5-minute grain.'
AS
WITH c AS (
  SELECT 'Country' AS scope_type, country_code AS scope, ts, sum(streams) AS streams, sum(baseline_streams) AS baseline_streams,
    sum(tickets) AS tickets, sum(tickets_15m) AS tickets_15m, sum(expected_tickets_15m) AS expected_tickets_15m
  FROM gold_region_heatmap_5m GROUP BY ALL
  UNION ALL
  SELECT 'Region', region_group, ts, sum(streams), sum(baseline_streams), sum(tickets), sum(tickets_15m), sum(expected_tickets_15m)
  FROM gold_region_heatmap_5m GROUP BY ALL
  UNION ALL
  SELECT 'Global', 'Global', ts, sum(streams), sum(baseline_streams), sum(tickets), sum(tickets_15m), sum(expected_tickets_15m)
  FROM gold_region_heatmap_5m GROUP BY ALL
),
s AS (
  SELECT 'Country' AS scope_type, country_code AS scope, ts_15m, count(*) AS neg FROM gold_social_posts
  WHERE sentiment = 'negative' AND topic_domain <> 'Show hype / fandom' AND country_code IS NOT NULL GROUP BY ALL
  UNION ALL
  SELECT 'Region', region_group, ts_15m, count(*) FROM gold_social_posts
  WHERE sentiment = 'negative' AND topic_domain <> 'Show hype / fandom' GROUP BY ALL
  UNION ALL
  SELECT 'Global', 'Global', ts_15m, count(*) FROM gold_social_posts
  WHERE sentiment = 'negative' AND topic_domain <> 'Show hype / fandom' GROUP BY ALL
),
m AS (
  SELECT c.*, u.expected_release_uplift, u.active_release, coalesce(s.neg, 0) AS negative_social_15m,
    c.streams / c.baseline_streams - 1 AS stream_dev,
    c.tickets_15m / (c.streams / 1e4) / 3 AS contacts_per_10k_streams,
    c.expected_tickets_15m / (c.streams / 1e4) / 3 AS baseline_contacts_per_10k_streams,
    (c.tickets_15m + 1) / (c.expected_tickets_15m + 1) AS contact_ratio
  FROM c
  JOIN gold_release_uplift_5m u ON c.ts = u.ts
  LEFT JOIN s ON s.scope_type = c.scope_type AND s.scope = c.scope
    AND s.ts_15m = CAST(from_unixtime(floor(unix_timestamp(c.ts) / 900) * 900) AS TIMESTAMP)
)
SELECT scope_type, scope, ts, streams, baseline_streams, round(stream_dev, 4) AS stream_deviation_pct, expected_release_uplift, active_release,
  tickets, tickets_15m, round(expected_tickets_15m, 1) AS expected_tickets_15m,
  round(contacts_per_10k_streams, 3) AS contacts_per_10k_streams, round(baseline_contacts_per_10k_streams, 3) AS baseline_contacts_per_10k_streams,
  round(contact_ratio, 2) AS contact_ratio, negative_social_15m,
  round(least(100, 100 * (0.55 * least(1, greatest(0, contact_ratio - 1) / 4)
                        + 0.25 * least(1, negative_social_15m / 10.0)
                        + 0.20 * least(1, greatest(0, -stream_dev) / 0.25))), 1) AS frustration_index,
  (stream_dev <= -0.08 AND contact_ratio >= 2) AS divergence_flag
FROM m;
