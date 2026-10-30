-- Silver/gold: social posts enriched with AI sentiment and topic.
-- Streaming table, so ai_analyze_sentiment / ai_classify run ONCE per new post (never re-scored on later updates).
-- Name kept as gold_social_posts for compatibility with the Command Center app.
CREATE OR REFRESH STREAMING TABLE gold_social_posts (
  CONSTRAINT has_sentiment EXPECT (sentiment IS NOT NULL)
)
COMMENT 'Social listening posts from Reddit and X with AI sentiment (ai_analyze_sentiment) and AI topic_domain (ai_classify) mapped to engineering domains.'
AS SELECT p.post_id, p.posted_at, p.platform, p.author_handle, p.country_code, p.post_text, p.engagement, r.region_group,
  ai_analyze_sentiment(p.post_text) AS sentiment,
  ai_classify(p.post_text, ARRAY('Authentication', 'Billing & Payments', 'Playback & CDN', 'Content Ingestion', 'Subtitles & Localization',
                                 'Device Apps', 'Search & Discovery', 'Show hype / fandom', 'General feedback')) AS topic_domain,
  CAST(from_unixtime(floor(unix_timestamp(p.posted_at) / 900) * 900) AS TIMESTAMP) AS ts_15m
FROM STREAM(bronze_social_posts) p
LEFT JOIN bronze_regions r ON p.country_code = r.country_code;
