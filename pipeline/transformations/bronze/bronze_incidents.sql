-- Bronze: declared incidents (known issues)
CREATE OR REFRESH STREAMING TABLE bronze_incidents (
  CONSTRAINT valid_incident_id EXPECT (incident_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_declared_at EXPECT (declared_at IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_window EXPECT (resolved_at IS NULL OR resolved_at >= declared_at)
)
COMMENT 'Raw declared incidents with domain, countries, region groups and timeline.'
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM(classic_stable_l0m1dn_catalog.support_command_center.incidents);
