-- Bronze: link from each support ticket to the subscriber account that raised it (append-only)
CREATE OR REFRESH STREAMING TABLE bronze_ticket_accounts (
  CONSTRAINT valid_ticket_id EXPECT (ticket_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_subscriber_id EXPECT (subscriber_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Ticket to subscriber account link.'
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM(classic_stable_l0m1dn_catalog.support_command_center.ticket_accounts);
