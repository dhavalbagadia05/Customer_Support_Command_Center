-- Bronze: support tickets (one row per ticket)
CREATE OR REFRESH STREAMING TABLE bronze_support_tickets (
  CONSTRAINT valid_ticket_id EXPECT (ticket_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_created_at EXPECT (created_at IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_country EXPECT (country_code IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_text EXPECT (subject IS NOT NULL OR description IS NOT NULL),
  CONSTRAINT valid_csat EXPECT (csat_score IS NULL OR csat_score BETWEEN 1 AND 5)
)
COMMENT 'Raw support tickets from the ticketing system.'
CLUSTER BY (created_at)
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM(classic_stable_l0m1dn_catalog.support_command_center.support_tickets);
