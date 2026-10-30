-- Gold (deterministic part): anomaly events enriched with geography, devices, CSAT/SLA, sample tickets,
-- the linked declared incident and the suspected change event. AI columns are joined in gold_ticket_anomalies.
CREATE OR REFRESH MATERIALIZED VIEW gold_ticket_anomalies_base
COMMENT 'Ticket anomaly events with top countries/devices, sample tickets, linked incident, detection lead and suspected change (no AI columns).'
AS
WITH tk AS (
  SELECT e.anomaly_id, t.*
  FROM silver_anomaly_events e
  JOIN silver_tickets t ON t.issue_signature = e.issue_signature AND t.created_at >= e.first_seen AND t.created_at < e.last_seen
),
agg AS (
  SELECT anomaly_id, count(*) AS total_tickets, count(DISTINCT country_code) AS n_countries,
    array_join(collect_set(region_group), ', ') AS region_groups,
    avg(CASE WHEN subscriber_tier IN ('Mega Fan', 'Ultimate Fan') THEN 1 ELSE 0 END) AS premium_share,
    avg(CAST(sla_met AS INT)) AS sla_met_rate, avg(csat_score) AS avg_csat,
    slice(collect_list(concat('[', country_code, '/', device, '/', language, '] ', subject, ': ', description)), 1, 12) AS sample_tickets
  FROM tk GROUP BY anomaly_id
),
cty AS (
  SELECT anomaly_id, array_join(slice(sort_array(collect_list(named_struct('k', -n, 'c', concat(country_code, ' (', n, ')')))), 1, 5).c, ', ') AS top_countries
  FROM (SELECT anomaly_id, country_code, count(*) AS n FROM tk GROUP BY ALL) GROUP BY anomaly_id
),
dev AS (
  SELECT anomaly_id, array_join(slice(sort_array(collect_list(named_struct('k', -n, 'c', concat(device, ' (', round(100 * n / tot), '%)')))), 1, 3).c, ', ') AS top_devices
  FROM (SELECT anomaly_id, device, count(*) AS n, sum(count(*)) OVER (PARTITION BY anomaly_id) AS tot FROM tk GROUP BY ALL) GROUP BY anomaly_id
),
inc AS (
  SELECT e.anomaly_id, max_by(i.incident_id, i.declared_at) AS incident_id, max(i.declared_at) AS incident_declared_at, max_by(i.title, i.declared_at) AS incident_title
  FROM silver_anomaly_events e
  -- also link incidents escalated after the surge peaked (declared up to 60 min after the last anomalous window)
  JOIN bronze_incidents i ON i.domain = e.issue_domain AND i.declared_at BETWEEN e.first_seen - INTERVAL 60 MINUTES AND e.last_seen + INTERVAL 60 MINUTES
  GROUP BY e.anomaly_id
),
chg AS (
  SELECT e.anomaly_id, max_by(c.change_id, c.change_ts) AS change_id,
    max_by(concat(c.service, ' ', c.version, ' — ', c.description), c.change_ts) AS change_summary, max(c.change_ts) AS change_ts
  FROM silver_anomaly_events e
  JOIN bronze_change_events c
    ON c.domain IN (e.issue_domain, CASE WHEN e.issue_domain = 'Subtitles & Localization' THEN 'Content Ingestion' END)
   AND c.change_ts BETWEEN e.first_seen - INTERVAL 120 MINUTES AND e.first_seen + INTERVAL 15 MINUTES
   AND c.change_type <> 'rollback' AND c.outcome <> 'success'
  GROUP BY e.anomaly_id
)
SELECT e.*, e.first_seen + INTERVAL 15 MINUTES AS detected_at, a.total_tickets, a.n_countries, cty.top_countries, dev.top_devices, a.region_groups,
  round(a.premium_share, 3) AS premium_share, round(a.sla_met_rate, 3) AS sla_met_rate, round(a.avg_csat, 2) AS avg_csat, a.sample_tickets,
  inc.incident_id, inc.incident_title, inc.incident_declared_at,
  round((unix_timestamp(inc.incident_declared_at) - unix_timestamp(e.first_seen + INTERVAL 15 MINUTES)) / 60) AS minutes_detected_before_incident,
  chg.change_id AS suspected_change_id, chg.change_summary AS suspected_change, chg.change_ts AS suspected_change_ts
FROM silver_anomaly_events e
JOIN agg a ON e.anomaly_id = a.anomaly_id
JOIN cty ON e.anomaly_id = cty.anomaly_id
JOIN dev ON e.anomaly_id = dev.anomaly_id
LEFT JOIN inc ON e.anomaly_id = inc.anomaly_id
LEFT JOIN chg ON e.anomaly_id = chg.anomaly_id
WHERE e.issue_signature NOT LIKE '% · general';
