-- Gold: measurable customer outcomes per day and region - repeat contact rate and incident exposure
CREATE OR REFRESH MATERIALIZED VIEW gold_customer_outcomes_daily
COMMENT 'Daily customer outcomes by region_group: tickets, subscribers contacting support, repeat contacts (prior contact within 7 days), repeat_contact_rate, incident-linked tickets and subscribers.'
AS SELECT date(created_at) AS kpi_date, region_group,
  count(*) AS tickets,
  count(DISTINCT subscriber_id) AS subscribers_contacting,
  sum(CAST(is_repeat_contact AS INT)) AS repeat_contacts,
  round(avg(CAST(is_repeat_contact AS INT)), 4) AS repeat_contact_rate,
  count(anomaly_id) AS incident_linked_tickets,
  count(DISTINCT CASE WHEN anomaly_id IS NOT NULL THEN subscriber_id END) AS incident_exposed_subscribers,
  round(avg(CASE WHEN anomaly_id IS NOT NULL THEN CAST(is_repeat_contact AS INT) END), 4) AS incident_repeat_contact_rate
FROM gold_subscriber_ticket_history
GROUP BY ALL;
