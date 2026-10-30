-- Gold: at-risk subscribers (Medium + High churn risk) grouped by churn driver and the issue they faced
CREATE OR REFRESH MATERIALIZED VIEW gold_churn_risk_by_issue
COMMENT 'Churn-risk cohorts: subscribers with Medium/High rule-based churn risk grouped by churn_driver, primary_issue_domain and region_group, with MRR at risk and top issue signature.'
AS SELECT churn_driver, primary_issue_domain, region_group,
  count(*) AS at_risk_subscribers,
  sum(CASE WHEN churn_risk_band = 'High' THEN 1 ELSE 0 END) AS high_risk_subscribers,
  round(sum(mrr_at_risk_usd), 2) AS mrr_at_risk_usd,
  round(avg(churn_risk_score), 1) AS avg_churn_risk_score,
  max_by(primary_issue_signature, churn_risk_score) AS example_signature,
  sum(CASE WHEN tier IN ('Mega Fan', 'Ultimate Fan') THEN 1 ELSE 0 END) AS premium_subscribers,
  max(as_of) AS as_of
FROM gold_subscriber_360
WHERE churn_risk_band IN ('High', 'Medium')
GROUP BY ALL;
