-- Bronze: payment attempts per country x gateway x 5 minutes
CREATE OR REFRESH STREAMING TABLE bronze_payment_transactions (
  CONSTRAINT valid_ts EXPECT (ts IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_keys EXPECT (country_code IS NOT NULL AND gateway IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT consistent_counts EXPECT (attempts >= 0 AND successes >= 0 AND successes + customer_declines + gateway_errors = attempts)
)
COMMENT 'Raw payment gateway outcomes by country, gateway and 5-minute interval.'
CLUSTER BY (ts, country_code)
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM(classic_stable_l0m1dn_catalog.support_command_center.payment_transactions);
