-- Gold: every ticket with the subscriber who raised it, the anomaly/incident it belongs to, and a repeat-contact flag
-- repeat contact = the same subscriber already contacted support in the previous 7 days
CREATE OR REFRESH MATERIALIZED VIEW gold_subscriber_ticket_history
COMMENT 'Ticket-level support history per subscriber: issue domain/signature, SLA, CSAT, reopened, linked anomaly/incident, is_repeat_contact (prior contact within 7 days).'
AS
WITH t AS (
  SELECT a.subscriber_id, s.ticket_id, s.created_at, s.ts_5m, s.country_code, s.region_group, s.device, s.channel, s.subscriber_tier,
    s.subject, s.description, s.issue_domain, s.issue_signature, s.first_response_minutes, s.sla_met, s.resolved_at, s.csat_score, s.reopened,
    lower(concat(s.subject, ' ', s.description)) rlike 'cancel' AS cancel_intent
  FROM silver_tickets s
  JOIN bronze_ticket_accounts a ON s.ticket_id = a.ticket_id
),
l AS (
  SELECT t.*, lag(created_at) OVER (PARTITION BY subscriber_id ORDER BY created_at, ticket_id) AS prev_contact_at FROM t
)
SELECT l.*,
  (prev_contact_at IS NOT NULL AND prev_contact_at >= created_at - INTERVAL 7 DAYS) AS is_repeat_contact,
  an.anomaly_id, an.incident_id, an.ai_headline AS incident_headline
FROM l
LEFT JOIN gold_ticket_anomalies an
  ON l.issue_signature = an.issue_signature AND l.created_at >= an.first_seen AND l.created_at < an.last_seen;
