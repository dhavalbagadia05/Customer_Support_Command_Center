-- Bronze: social listening posts (Reddit, X)
CREATE OR REFRESH STREAMING TABLE bronze_social_posts (
  CONSTRAINT valid_post_id EXPECT (post_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_posted_at EXPECT (posted_at IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_text EXPECT (post_text IS NOT NULL AND length(post_text) > 0) ON VIOLATION DROP ROW
)
COMMENT 'Raw social posts mentioning Fanimation.'
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM(classic_stable_l0m1dn_catalog.support_command_center.social_posts);
