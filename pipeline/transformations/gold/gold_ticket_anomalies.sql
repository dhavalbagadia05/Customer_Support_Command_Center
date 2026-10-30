-- Gold: ticket anomaly events + AI analysis (same columns as the notebook-built table used by the app).
-- AI columns come from silver_anomaly_ai, which calls the LLM only once per new anomaly_id.
CREATE OR REFRESH MATERIALIZED VIEW gold_ticket_anomalies
COMMENT 'AI-assisted ticket surge detection: one row per anomaly event with geography, devices, linked incident, suspected change, minutes_detected_before_incident and AI headline/summary/root cause/actions/customer macro.'
AS SELECT b.* EXCEPT (sample_tickets),
  ai.ai_headline, ai.ai_summary, ai.ai_likely_root_cause, ai.ai_recommended_actions, ai.ai_customer_macro,
  b.sample_tickets
FROM gold_ticket_anomalies_base b
LEFT JOIN silver_anomaly_ai ai ON b.anomaly_id = ai.anomaly_id;
