"""AI incident analysis for ticket-surge anomaly events, computed ONCE per anomaly_id.

How LLM calls are limited to new anomalies:
  * The stream source is silver_tickets (streaming table), so each update only sees newly ingested tickets.
  * Those tickets are joined (stream-static) to gold_ticket_anomalies_base to find the anomaly events they belong to.
  * dropDuplicates(['anomaly_id']) keeps streaming state of anomaly ids already analysed, so an event that keeps
    growing across updates is never sent to the LLM again.
  * ai_query (databricks-claude-sonnet-4-6) therefore runs only for anomaly events seen for the first time.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

MODEL = "databricks-claude-sonnet-4-6"

PROMPT_SQL = """concat(
  'You are an incident analyst for Fanimation, an anime streaming service. A surge of support tickets was detected. ',
  'Produce: headline (max 12 words), summary (2-3 concise sentences, plain English, mention scale, geography, device and likely cause), ',
  'likely_root_cause (1 sentence), recommended_actions (3 short imperative strings for engineering + support), ',
  'customer_macro (1-2 sentence known-issue reply agents can send to customers, in English).',
  '\\nSignature: ', issue_signature, '\\nDomain: ', issue_domain,
  '\\nDetected at: ', cast(detected_at AS string), ' UTC; active until ', cast(last_seen AS string),
  '\\nTotal tickets: ', total_tickets, '; peak per 15 min: ', peak_tickets_15m, ' vs baseline ', baseline_15m,
  '\\nTop countries: ', top_countries, '\\nTop devices: ', top_devices,
  '\\nRecent change near onset: ', coalesce(suspected_change, 'none found'),
  '\\nSample tickets (multilingual): ', array_join(sample_tickets, ' || '))"""

RESPONSE_FORMAT = ("STRUCT<analysis:STRUCT<headline:STRING, summary:STRING, likely_root_cause:STRING, "
                   "recommended_actions:ARRAY<STRING>, customer_macro:STRING>>")
PARSE_SCHEMA = ("headline STRING, summary STRING, likely_root_cause STRING, "
                "recommended_actions ARRAY<STRING>, customer_macro STRING")


@dp.table(
    name="silver_anomaly_ai",
    comment="AI-generated headline, summary, root cause, recommended actions and customer macro per anomaly_id (LLM called once per new anomaly event).",
)
@dp.expect("has_headline", "ai_headline IS NOT NULL")
def silver_anomaly_ai():
    new_tickets = spark.readStream.table("silver_tickets").select(
        F.col("issue_signature").alias("t_signature"), F.col("created_at").alias("t_created_at"))
    events = spark.read.table("gold_ticket_anomalies_base")
    matched = (
        new_tickets.join(
            events,
            (new_tickets.t_signature == events.issue_signature)
            & (new_tickets.t_created_at >= events.first_seen)
            & (new_tickets.t_created_at < events.last_seen),
        )
        .select("anomaly_id", "issue_signature", "issue_domain", "detected_at", "last_seen", "total_tickets",
                "peak_tickets_15m", "baseline_15m", "top_countries", "top_devices", "suspected_change", "sample_tickets")
        .dropDuplicates(["anomaly_id"])
    )
    raw = F.expr(f"ai_query('{MODEL}', {PROMPT_SQL}, responseFormat => '{RESPONSE_FORMAT}')")
    return (
        matched.withColumn("raw", raw)
        .withColumn("j", F.from_json("raw", PARSE_SCHEMA))
        .select(
            "anomaly_id",
            "issue_signature",
            F.col("j.headline").alias("ai_headline"),
            F.col("j.summary").alias("ai_summary"),
            F.col("j.likely_root_cause").alias("ai_likely_root_cause"),
            F.col("j.recommended_actions").alias("ai_recommended_actions"),
            F.col("j.customer_macro").alias("ai_customer_macro"),
            F.lit(MODEL).alias("ai_model"),
            F.current_timestamp().alias("ai_generated_at"),
        )
    )
