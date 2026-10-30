-- Bronze: concurrent streams per country x device x 5 minutes (append-only source, read incrementally)
CREATE OR REFRESH STREAMING TABLE bronze_concurrent_streams (
  CONSTRAINT valid_ts EXPECT (ts IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_keys EXPECT (country_code IS NOT NULL AND device IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_streams EXPECT (concurrent_streams >= 0)
)
COMMENT 'Raw concurrent streams by country, device and 5-minute interval.'
CLUSTER BY (ts, country_code)
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM(classic_stable_l0m1dn_catalog.support_command_center.concurrent_streams);
