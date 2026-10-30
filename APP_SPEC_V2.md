# Support Command Center - App spec V2 (persona nav, Customer 360, churn, Lakebase serving, architecture)

App: `support-command-center`. Source: `/Workspace/Users/dhaval.bagadia@databricks.com/support_command_center/app` (entry `command_center.py`, Streamlit, light theme, Vantage-style layout already in place). Keep existing look and feel. Service principal client id `7219a20a-d11b-421c-a628-71a6c1bafce9`. Deploy and verify every page renders without errors for all three personas.

## 1. Real serving story: read from Lakebase
All analytics reads move from the SQL warehouse to **Lakebase Postgres** (project `support-command-center`, branch `production`, db `databricks_postgres`, endpoint `projects/support-command-center/branches/production/endpoints/primary`). The SP already has USAGE + SELECT on PG schema `support_command_center_sdp`. Snapshot synced tables, refreshed by notebook `04_live_event_generator` after each pipeline update:

| PG table (schema support_command_center_sdp) | Use |
| --- | --- |
| lb_region_heatmap_5m | global heatmap |
| lb_service_health_region_5m | health matrix |
| lb_frustration_index_5m | streams vs baseline, frustration |
| lb_ticket_anomalies | anomaly cards, Incident Brief (ai_recommended_actions / sample_tickets are JSONB) |
| lb_incident_scorecard | exec scorecard, detection lead |
| lb_signature_trends_15m | sparklines |
| lb_payment_health_5m | payments |
| lb_social_posts | social feed |
| lb_exec_daily_kpis | exec trends |
| lb_change_events, lb_incidents | risky changes, incidents |
| lb_subscriber_360 | Customer 360 + churn lists (1M rows; indexes on subscriber_id PK, (churn_risk_band, churn_risk_score desc), email) |
| lb_subscriber_ticket_history | per-subscriber ticket timeline (index on subscriber_id, created_at) |
| lb_churn_risk_by_issue | churn cohorts by churn_driver x primary_issue_domain x region_group |
| lb_customer_outcomes_daily | repeat contact rate trend |

Use psycopg with OAuth token from `w.postgres.generate_database_credential(endpoint=...)`, user = `DATABRICKS_CLIENT_ID`; reconnect on token expiry. Push filters into SQL (never load whole tables): latest snapshot = `WHERE ts = (SELECT max(ts) FROM ...)`; time series = latest day only. Keep the SQL warehouse only as fallback. Ops write-back tables in schema `command_center_ops` stay as is. Show a small "Served from Lakebase · data as of HH:MM UTC" badge.

## 2. Remove the demo clock (go live)
Remove the replay clock, play/pause, speed and Jump-to-scenario controls. "Now" = latest `ts` in `lb_region_heatmap_5m`; today = that date. Auto-refresh every 60 s (or a Refresh button). New data arrives when the presenter runs notebook `04_live_event_generator` (it appends events, runs the streaming pipeline and re-syncs Lakebase). Incident Brief keeps its incident picker (all incidents, default "No incident selected").

## 3. Persona-based navigation (tabs and nav change with "Viewing as")
Top persona cards and the sidebar selector stay in sync. Each persona sees only its nav items; if the current page is not in the new persona's nav, go to Home.
* **C-Suite (Executive Strategy):** Home · Alerts · Dashboards: Executive Overview, Churn & Retention · AI powered research: Incident Brief, Genie
* **Support Operations (VP Support / Support Ops Manager):** Home · Alerts · Dashboards: Subscriber Experience, Customer 360, Churn & Retention, Payments & Social · AI powered research: Incident Brief, Genie, Ops Console (Lakebase)
* **Engineering / SRE (Platform Reliability):** Home · Alerts · Dashboards: Platform Pulse, Payments & Social · AI powered research: Incident Brief, Genie
Rename the nav section "Agentic-powered research" to **"AI powered research"** everywhere.
Home additions: C-Suite hero adds "MRR at risk (churn)" and "Repeat contact rate today"; Support Ops adds "At-risk subscribers contacting today".

## 4. Customer 360 page (new)
* Search by subscriber_id, email or name (ILIKE, limit 20). Default list: top 20 High-risk subscribers.
* Profile card from lb_subscriber_360: name, email, country, language, tier, monthly price, billing gateway, primary device, segment, tenure, signup date, renewal date + days to renewal, auto-renew, lifetime value, crm_source "Salesforce (synthetic)".
* Engagement: watch hours last 30d vs prior 30d (watch_time_change_pct).
* Churn risk: score gauge (0-100), band (High >= 60 / Medium >= 35 / Low), churn_driver, churn_reasons as chips, recommended_action, mrr_at_risk_usd. Label clearly "Rule-based risk score (not an ML model)" with a tooltip of the weights: cancellation intent 25; watch time drop >=40% 20 (>=20% 10); 3+ contacts in 14d 20 (2 contacts 10); repeat contacts 10 each (max 20); payment failures 15; hit by a platform incident 15; low CSAT <=2 15; SLA missed 10; reopened ticket 10; paid renewal due within 14 days 10; capped at 100.
* 14-day support signals: tickets_14d, repeat_contacts_14d, low_csat_14d, sla_missed_14d, reopened_14d, payment_failure_tickets_14d, incident_tickets_14d, incidents_hit.
* Ticket timeline from lb_subscriber_ticket_history: created_at, channel, issue_signature, subject, SLA met, CSAT, reopened, repeat-contact badge, linked incident (click opens Incident Brief for that anomaly).

## 5. Churn & Retention page (new) - customers with potential to churn grouped by issue / reasoning
* KPIs: at-risk subscribers (High+Medium), High, MRR at risk, repeat contact rate (latest day vs 7-day avg).
* Cohort view from lb_churn_risk_by_issue grouped by churn_driver, expandable by primary_issue_domain and region_group; bar of MRR at risk by driver coloured by issue faced.
* Selecting a cohort lists its subscribers from lb_subscriber_360 (top 200 by score; filters region, tier, band) with churn_reasons and recommended_action; clicking a row opens Customer 360.
* Repeat-contact trend from lb_customer_outcomes_daily (overall and incident-linked).

## 6. About page
Remove unmeasured claims. Outcomes must be measurable from the data: detection lead time (customers before monitoring), Customer-Detected minutes, SLA/CSAT during incidents, revenue at risk from gateway failures, lost viewing hours, repeat contact rate, at-risk subscribers and MRR at risk (rule-based). State that data is synthetic.

## 7. Architecture page - three diagrams (tabs), HTML cards like the provided screenshots
Style: column headers in small-caps cards, coloured top border per card type, dashed pink "Unity Catalog governance" perimeter, legend box (Person/persona blue, Platform capability red, Governed data gold, Views/metrics teal, AI & serving purple, Outcome green, UC perimeter dashed). Do NOT invent components - only what exists:

**A. Business Architecture** - columns WHO DOES THE WORK / WHAT THEY DO IN THE COMMAND CENTER / OUTCOMES; one row per persona; middle column lists that persona's nav pages (section 3) inside the UC perimeter. Outcomes:
* C-Suite: one view of subscriber impact - detection lead time vs monitoring, CSAT/SLA, revenue at risk, MRR at risk from churn-risk cohorts.
* Support Ops: catch ticket surges early, publish known-issue banners + AI customer macros, see which customers are at risk and why, track repeat contacts.
* Engineering/SRE: support as a second monitoring signal - customer-detected cells, telemetry vs contact-rate divergence, deploy/config-change correlation, AI root-cause summary.

**B. Platform & Data Flow** - columns SOURCES / LAKEHOUSE / INTELLIGENCE & SERVING / APPLICATION. Sources (synthetic; show as directional source cards, not every table): Support case tickets; Subscriber accounts (Salesforce Customer 360, synthetic); Product telemetry (concurrent streams, service metrics); Payment gateway transactions; Social posts (Reddit, X); Deploys, change events & declared incidents; Simulcast release calendar. Note "generated by notebooks 01/05, live events by 04". Lakehouse (inside UC perimeter): Lakeflow Spark Declarative Pipeline - streaming bronze tables with data-quality expectations -> silver (ticket issue signatures, release-aware stream baselines, AI-enriched social posts) -> gold (service health, heatmap, frustration, ticket anomalies, payment health, incident scorecard, Subscriber 360 & churn risk, customer outcomes). Intelligence & serving: AI Functions (ai_query with Claude Sonnet 4.6, ai_analyze_sentiment, ai_classify), AI/BI Genie, AI/BI dashboard, Lakebase (15 synced serving tables + app write-back: incident actions, known-issue banners, alert acks). Application: Databricks App (persona views, Customer 360, write-back actions, Genie). Arrows: sources -> pipeline -> gold -> {Genie, dashboard, Lakebase sync}; app reads Lakebase, writes ops tables, calls Genie, embeds dashboard.

**C. AI, Agents & Serving** - columns SEMANTIC LAYER / AI & SERVING SURFACES / CONSUMERS. Semantic layer: governed gold tables with column comments + Genie instructions and example SQL. Surfaces (purple): AI/BI Genie (natural language -> governed SQL); Claude Sonnet 4.6 via ai_query (incident headline, summary, likely root cause, recommended actions, customer macro - generated once per new anomaly); AI Functions ai_analyze_sentiment + ai_classify (sentiment and topic per social post). Separate grey card "Rule-based analytics (not AI)": ticket-surge detection (z-score vs hour-of-day baseline), health states, release-aware baselines, churn risk score. Consumers:
* C-Suite: Genie (detection lead, revenue/MRR at risk, CSAT vs last week), Incident Brief (Claude).
* Support Ops: AI anomaly cards + customer macro (Claude), social sentiment (AI Functions), Genie (what drives spikes, churn by issue), churn reasons (rule-based).
* Engineering/SRE: Incident Brief root cause + actions (Claude), Genie (why a region turned red, which deploys preceded incidents), suspected-change correlation (rule-based).
No agents, no ML models, no other LLMs - do not show them.

## 8. Genie
Space `01f1d226b70f1202b656c554ffc9bd38` now points at `classic_stable_l0m1dn_catalog.support_command_center_sdp` (incl. subscriber and churn tables). Suggested questions per persona: C-Suite add "Which issues are driving churn risk and how much MRR is at risk?"; Support add "Show high churn-risk Ultimate Fan subscribers hit by an incident"; Eng unchanged.

## 9. Executive dashboard
`01f1d2273e931b0e95acaa9a67663fd7` already repointed to the SDP schema with churn widgets; embed unchanged.
