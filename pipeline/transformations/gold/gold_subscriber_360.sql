-- Gold: Subscriber 360 with a RULE-BASED churn risk score (not an ML model).
-- Signals over the 14 days before the latest ticket in the data (as_of):
--   cancellation intent 25 | watch time drop >=40% 20 (>=20% 10) | 3+ contacts 20 (2 contacts 10) | repeat contacts 10 each (max 20)
--   payment failures 15 | hit by a platform incident 15 | low CSAT (<=2) 15 | SLA missed 10 | reopened ticket 10 | paid renewal due in 14 days 10
-- churn_risk_score = min(100, sum); band High >= 60, Medium >= 35, Low otherwise. churn_driver = highest-weighted signal.
CREATE OR REFRESH MATERIALIZED VIEW gold_subscriber_360
COMMENT 'Subscriber 360: CRM profile, engagement trend, 14-day support history and rule-based churn_risk_score / churn_risk_band / churn_driver / churn_reasons / recommended_action.'
AS
WITH asof AS (SELECT max(created_at) AS as_of FROM gold_subscriber_ticket_history),
h AS (
  SELECT h.* FROM gold_subscriber_ticket_history h CROSS JOIN asof WHERE h.created_at > asof.as_of - INTERVAL 14 DAYS
),
agg AS (
  SELECT subscriber_id, count(*) AS tickets_14d, sum(CAST(is_repeat_contact AS INT)) AS repeat_contacts_14d,
    sum(CAST(reopened AS INT)) AS reopened_14d, sum(CASE WHEN csat_score <= 2 THEN 1 ELSE 0 END) AS low_csat_14d,
    sum(CASE WHEN NOT sla_met THEN 1 ELSE 0 END) AS sla_missed_14d, count(anomaly_id) AS incident_tickets_14d,
    sum(CASE WHEN issue_signature LIKE 'Payment failure%' OR issue_signature LIKE 'BILL-%' THEN 1 ELSE 0 END) AS payment_failure_tickets_14d,
    max(CAST(cancel_intent AS INT)) = 1 AS cancel_intent_14d, max(created_at) AS last_contact_at, avg(csat_score) AS avg_csat_14d,
    array_join(slice(array_distinct(filter(collect_list(incident_id), x -> x IS NOT NULL)), 1, 3), ', ') AS incidents_hit
  FROM h GROUP BY subscriber_id
),
dom AS (
  SELECT subscriber_id, max_by(issue_domain, n) AS primary_issue_domain
  FROM (SELECT subscriber_id, issue_domain, count(*) AS n FROM h GROUP BY ALL) GROUP BY subscriber_id
),
sig AS (
  SELECT subscriber_id, max_by(issue_signature, n) AS primary_issue_signature
  FROM (SELECT subscriber_id, issue_signature, count(*) + CASE WHEN issue_signature LIKE '% general' THEN 0 ELSE 1000 END AS n FROM h GROUP BY ALL)
  GROUP BY subscriber_id
),
b AS (
  SELECT s.*, r.region_group, r.country_name, asof.as_of,
    coalesce(a.tickets_14d, 0) AS tickets_14d, coalesce(a.repeat_contacts_14d, 0) AS repeat_contacts_14d, coalesce(a.reopened_14d, 0) AS reopened_14d,
    coalesce(a.low_csat_14d, 0) AS low_csat_14d, coalesce(a.sla_missed_14d, 0) AS sla_missed_14d, coalesce(a.incident_tickets_14d, 0) AS incident_tickets_14d,
    coalesce(a.payment_failure_tickets_14d, 0) AS payment_failure_tickets_14d, coalesce(a.cancel_intent_14d, false) AS cancel_intent_14d,
    a.last_contact_at, round(a.avg_csat_14d, 2) AS avg_csat_14d, a.incidents_hit, d.primary_issue_domain, g.primary_issue_signature,
    round(1 - s.watch_hours_last_30d / nullif(s.watch_hours_prior_30d, 0), 3) AS watch_time_change_pct_neg,
    datediff(s.renewal_date, CAST(asof.as_of AS DATE)) AS days_to_renewal
  FROM bronze_subscribers s
  JOIN bronze_regions r ON s.country_code = r.country_code
  CROSS JOIN asof
  LEFT JOIN agg a ON s.subscriber_id = a.subscriber_id
  LEFT JOIN dom d ON s.subscriber_id = d.subscriber_id
  LEFT JOIN sig g ON s.subscriber_id = g.subscriber_id
),
p AS (
  SELECT b.*, array(
    named_struct('p', CASE WHEN cancel_intent_14d THEN 25 ELSE 0 END, 'r', 'Cancellation intent'),
    named_struct('p', CASE WHEN watch_time_change_pct_neg >= 0.4 THEN 20 WHEN watch_time_change_pct_neg >= 0.2 THEN 10 ELSE 0 END, 'r', 'Watch time dropping'),
    named_struct('p', CASE WHEN tickets_14d >= 3 THEN 20 WHEN tickets_14d = 2 THEN 10 ELSE 0 END, 'r', 'High contact volume'),
    named_struct('p', least(20, 10 * repeat_contacts_14d), 'r', 'Repeat contacts'),
    named_struct('p', CASE WHEN payment_failure_tickets_14d > 0 THEN 15 ELSE 0 END, 'r', 'Payment failures'),
    named_struct('p', CASE WHEN incident_tickets_14d > 0 THEN 15 ELSE 0 END, 'r', 'Hit by a platform incident'),
    named_struct('p', CASE WHEN low_csat_14d > 0 THEN 15 ELSE 0 END, 'r', 'Low CSAT'),
    named_struct('p', CASE WHEN sla_missed_14d > 0 THEN 10 ELSE 0 END, 'r', 'Slow first response (SLA missed)'),
    named_struct('p', CASE WHEN reopened_14d > 0 THEN 10 ELSE 0 END, 'r', 'Reopened ticket'),
    named_struct('p', CASE WHEN monthly_price_usd > 0 AND days_to_renewal BETWEEN 0 AND 14 AND tickets_14d > 0 THEN 10 ELSE 0 END, 'r', 'Renewal due within 14 days')
  ) AS signals
  FROM b
),
s AS (
  SELECT p.* EXCEPT (signals),
    least(100, aggregate(signals, CAST(0 AS BIGINT), (acc, x) -> acc + x.p)) AS churn_risk_score,
    transform(filter(sort_array(signals, false), x -> x.p > 0), x -> x.r) AS churn_reasons
  FROM p
)
SELECT s.* EXCEPT (watch_time_change_pct_neg),
  round(-watch_time_change_pct_neg, 3) AS watch_time_change_pct,
  CASE WHEN churn_risk_score >= 60 THEN 'High' WHEN churn_risk_score >= 35 THEN 'Medium' ELSE 'Low' END AS churn_risk_band,
  element_at(churn_reasons, 1) AS churn_driver,
  CASE element_at(churn_reasons, 1)
    WHEN 'Cancellation intent' THEN 'Retention offer: route to save desk within 24h'
    WHEN 'Payment failures' THEN 'Confirm payment fixed, extend grace period, retry billing'
    WHEN 'Hit by a platform incident' THEN 'Send apology + incident resolution update; consider service credit'
    WHEN 'Repeat contacts' THEN 'Assign a single owner and close the loop proactively'
    WHEN 'High contact volume' THEN 'Assign a single owner and close the loop proactively'
    WHEN 'Watch time dropping' THEN 'Re-engagement: personalised picks for upcoming simulcasts'
    WHEN 'Low CSAT' THEN 'Supervisor follow-up on last interaction'
    WHEN 'Slow first response (SLA missed)' THEN 'Prioritise in queue; apologise for wait'
    WHEN 'Reopened ticket' THEN 'Escalate to tier 2 for root-cause fix'
    WHEN 'Renewal due within 14 days' THEN 'Pre-renewal check-in on open issues'
  END AS recommended_action,
  CASE WHEN churn_risk_score >= 35 THEN monthly_price_usd ELSE 0 END AS mrr_at_risk_usd
FROM s;
