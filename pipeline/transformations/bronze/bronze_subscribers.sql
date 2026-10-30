-- Reference: subscriber accounts from the CRM (Salesforce Customer 360 analog, synthetic). Snapshot dimension.
CREATE OR REFRESH MATERIALIZED VIEW bronze_subscribers (
  CONSTRAINT valid_subscriber_id EXPECT (subscriber_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_tier EXPECT (tier IN ('Free', 'Fan', 'Mega Fan', 'Ultimate Fan')),
  CONSTRAINT valid_price EXPECT (monthly_price_usd >= 0)
)
COMMENT 'Subscriber accounts (synthetic Salesforce Customer 360): tier, price, billing gateway, primary device, tenure, renewal, auto-renew, watch hours last vs prior 30 days.'
AS SELECT * FROM classic_stable_l0m1dn_catalog.support_command_center.subscribers;
