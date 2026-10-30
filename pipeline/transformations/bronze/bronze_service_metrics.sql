-- Bronze: internal service telemetry per country x engineering domain x 5 minutes
CREATE OR REFRESH STREAMING TABLE bronze_service_metrics (
  CONSTRAINT valid_ts EXPECT (ts IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_keys EXPECT (country_code IS NOT NULL AND domain IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_error_rate EXPECT (error_rate BETWEEN 0 AND 1),
  CONSTRAINT positive_requests EXPECT (request_count > 0)
)
COMMENT 'Raw service metrics (request count, error rate, p95 latency) by country and domain.'
CLUSTER BY (ts, domain)
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM(classic_stable_l0m1dn_catalog.support_command_center.service_metrics);
