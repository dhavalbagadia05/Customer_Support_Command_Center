-- Bronze: deployments, config changes, content publishes and rollbacks
CREATE OR REFRESH STREAMING TABLE bronze_change_events (
  CONSTRAINT valid_change_id EXPECT (change_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_change_ts EXPECT (change_ts IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_domain EXPECT (domain IS NOT NULL)
)
COMMENT 'Raw change events by service and engineering domain.'
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM(classic_stable_l0m1dn_catalog.support_command_center.change_events);
