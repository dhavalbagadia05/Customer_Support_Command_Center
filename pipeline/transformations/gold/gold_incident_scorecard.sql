-- Gold: one row per incident with detection lead time (customers vs monitoring), impact and cost
CREATE OR REFRESH MATERIALIZED VIEW gold_incident_scorecard
COMMENT 'Incident scorecard: customer_detected_at, detection_lead_minutes, detection_winner, tickets, CSAT, SLA, estimated lost viewing hours, revenue at risk, negative social posts.'
AS
WITH i AS (SELECT * EXCEPT (_ingested_at) FROM bronze_incidents),
cs AS (
  SELECT i.incident_id, min(h.ts) AS first_customer_signal_at
  FROM i JOIN gold_service_health_country_5m h
    ON h.domain = i.domain AND array_contains(split(i.countries, ','), h.country_code)
   AND h.ts BETWEEN i.declared_at - INTERVAL 3 HOURS AND i.resolved_at AND h.customer_signal
  GROUP BY i.incident_id
),
an AS (
  SELECT incident_id, min(detected_at) AS first_ticket_anomaly_at, max_by(anomaly_id, total_tickets) AS anomaly_id,
    max_by(issue_signature, total_tickets) AS issue_signature, max_by(ai_headline, total_tickets) AS ai_headline
  FROM gold_ticket_anomalies GROUP BY incident_id
),
tk AS (
  SELECT i.incident_id, count(*) AS tickets_during_incident, avg(t.csat_score) AS csat_during_incident, avg(CAST(t.sla_met AS INT)) AS sla_met_rate
  FROM i JOIN silver_tickets t
    ON t.issue_domain = i.domain AND array_contains(split(i.countries, ','), t.country_code)
   AND t.created_at BETWEEN coalesce(i.first_alert_at, i.declared_at) - INTERVAL 90 MINUTES AND i.resolved_at
  GROUP BY i.incident_id
),
st AS (
  SELECT i.incident_id, sum(CASE WHEN s.streams < 0.95 * s.baseline_streams THEN s.baseline_streams - s.streams ELSE 0 END) AS lost_stream_intervals
  FROM i JOIN silver_country_streams_5m s
    ON array_contains(split(i.countries, ','), s.country_code) AND s.ts BETWEEN i.declared_at - INTERVAL 90 MINUTES AND i.resolved_at
  GROUP BY i.incident_id
),
pay AS (
  SELECT i.incident_id, sum(p.revenue_at_risk_usd) AS revenue_at_risk_usd
  FROM i JOIN gold_payment_health_5m p
    ON i.domain = 'Billing & Payments' AND array_contains(split(i.countries, ','), p.country_code)
   AND p.ts BETWEEN i.declared_at - INTERVAL 90 MINUTES AND i.resolved_at
  GROUP BY i.incident_id
),
so AS (
  SELECT i.incident_id, count(*) AS negative_social_posts
  FROM i JOIN gold_social_posts g
    ON g.sentiment = 'negative' AND g.posted_at BETWEEN i.declared_at - INTERVAL 90 MINUTES AND i.resolved_at
   AND array_contains(split(i.countries, ','), g.country_code)
  GROUP BY i.incident_id
)
SELECT i.*, cs.first_customer_signal_at, an.first_ticket_anomaly_at, an.anomaly_id, an.issue_signature, an.ai_headline,
  least(cs.first_customer_signal_at, an.first_ticket_anomaly_at) AS customer_detected_at,
  round((unix_timestamp(coalesce(i.first_alert_at, i.declared_at)) - unix_timestamp(least(cs.first_customer_signal_at, an.first_ticket_anomaly_at))) / 60) AS detection_lead_minutes,
  CASE WHEN least(cs.first_customer_signal_at, an.first_ticket_anomaly_at) < coalesce(i.first_alert_at, i.declared_at) THEN 'Customers first' ELSE 'Monitoring first' END AS detection_winner,
  round((unix_timestamp(i.resolved_at) - unix_timestamp(i.declared_at)) / 60) AS minutes_to_resolve,
  coalesce(tk.tickets_during_incident, 0) AS tickets_during_incident, round(tk.csat_during_incident, 2) AS csat_during_incident, round(tk.sla_met_rate, 3) AS sla_met_rate,
  round(st.lost_stream_intervals * 5 / 60) AS est_lost_viewing_hours, round(coalesce(pay.revenue_at_risk_usd, 0), 2) AS revenue_at_risk_usd,
  coalesce(so.negative_social_posts, 0) AS negative_social_posts
FROM i
LEFT JOIN cs ON i.incident_id = cs.incident_id
LEFT JOIN an ON i.incident_id = an.incident_id
LEFT JOIN tk ON i.incident_id = tk.incident_id
LEFT JOIN st ON i.incident_id = st.incident_id
LEFT JOIN pay ON i.incident_id = pay.incident_id
LEFT JOIN so ON i.incident_id = so.incident_id;
