-- Silver: tickets enriched with issue domain + issue signature extracted from multilingual text (EN/ES/PT/DE/FR).
-- Incremental: each update only processes newly ingested tickets (stream-static join with the country dimension).
CREATE OR REFRESH STREAMING TABLE silver_tickets (
  CONSTRAINT has_region EXPECT (region_group IS NOT NULL),
  CONSTRAINT has_signature EXPECT (issue_signature IS NOT NULL)
)
COMMENT 'Support tickets enriched with issue_domain, issue_signature, error_code, service_code, show_title, episode_number, subtitle_language, SLA met flag. One row per ticket.'
CLUSTER BY (ts_5m)
AS
WITH b AS (
  SELECT t.* EXCEPT (_ingested_at), r.region_group,
    CAST(from_unixtime(floor(unix_timestamp(t.created_at) / 300) * 300) AS TIMESTAMP) AS ts_5m,
    lower(concat(t.subject, ' ', t.description)) AS txt
  FROM STREAM(bronze_support_tickets) t
  JOIN bronze_regions r ON t.country_code = r.country_code
),
c AS (
  SELECT *,
    CASE WHEN txt rlike 'subt[ií]t|sous-titres|subtitle|subs |dub|audio' THEN 'Subtitles & Localization'
         WHEN txt rlike 'payment|charged|card|bill|invoice|zahlung|renew|google play|cancel|price' THEN 'Billing & Payments'
         WHEN txt rlike 'log|sign|password|auth-' THEN 'Authentication'
         WHEN txt rlike 'search' THEN 'Search & Discovery'
         WHEN txt rlike 'crash|freez' THEN 'Device Apps'
         WHEN txt rlike 'not available|new episode|episode not showing' THEN 'Content Ingestion'
         ELSE 'Playback & CDN' END AS issue_domain,
    nullif(regexp_extract(txt, '(?:error|erro|erreur)\\s*(?:code\\s*)?(\\d{4})', 1), '') AS error_code,
    nullif(upper(regexp_extract(txt, '((?:auth|bill)-\\d{3})', 1)), '') AS service_code,
    nullif(regexp_extract(concat(subject, ' ', description),
      '(Blade of the Crimson Moon|Spirit Ronin|Iron Samurai Gaiden|Moonlit Idol Stage|Mecha Academy Zero|Isekai Cafe Chronicles|Shadow Guild Hunters|Ocean Spirit Diaries)', 1), '') AS show_title,
    nullif(regexp_extract(txt, '(?:episode|episodio|épisode|ep)\\s*(\\d+)', 1), '') AS episode_number_str,
    CASE WHEN txt rlike 'spanish|español|espanol' THEN 'Spanish' WHEN txt rlike 'sous-titres|french' THEN 'French' END AS subtitle_language
  FROM b
)
SELECT ticket_id, created_at, ts_5m, country_code, region_group, language, device, channel, subscriber_tier, form_category,
  subject, description, issue_domain, error_code, service_code, show_title, CAST(episode_number_str AS INT) AS episode_number, subtitle_language,
  CASE
    WHEN error_code IS NOT NULL THEN concat('Error ', error_code)
    WHEN service_code IS NOT NULL THEN concat(service_code, ' · ', issue_domain)
    WHEN issue_domain = 'Subtitles & Localization' AND subtitle_language IS NOT NULL AND show_title IS NOT NULL
      THEN concat(CASE WHEN txt rlike 'sync|décal|timing|late|behind' THEN 'Subtitles out of sync · ' ELSE 'Subtitles missing · ' END,
                  show_title, ' E', episode_number_str, ' · ', subtitle_language)
    WHEN issue_domain = 'Billing & Payments' AND txt rlike 'declined|failed|fehlgeschlagen|abgelehnt|could not be processed'
      THEN concat('Payment failure · ', country_code)
    ELSE concat(issue_domain, ' · general') END AS issue_signature,
  first_response_minutes, sla_target_minutes, first_response_minutes <= sla_target_minutes AS sla_met,
  resolved_at, csat_score, reopened
FROM c;
