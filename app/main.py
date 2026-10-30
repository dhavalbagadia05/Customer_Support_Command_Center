"""Fanimation Support Command Center - Databricks App (Streamlit, persona-driven layout).

Support tickets as a real-time signal of platform health. Replays the scripted demo day 2026-10-27 (UTC)
from Unity Catalog gold tables. Incident actions are written to Lakebase. Genie for natural-language Q&A.
"""
import os, time, json, html
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

st.set_page_config(page_title="Fanimation Support Command Center", page_icon="🛰️", layout="wide", initial_sidebar_state="expanded")

WH = os.getenv("WAREHOUSE_ID", "13bf44dc52395b21")
GENIE = os.getenv("GENIE_SPACE_ID", "01f1d226b70f1202b656c554ffc9bd38")
DASH = os.getenv("DASHBOARD_ID", "01f1d2273e931b0e95acaa9a67663fd7")
S = os.getenv("UC_SCHEMA", "classic_stable_l0m1dn_catalog.support_command_center")
LB_EP = os.getenv("LAKEBASE_ENDPOINT") or os.getenv("LAKEBASE_ENDPOINT_PATH", "projects/support-command-center/branches/production/endpoints/primary")
LB_DB = os.getenv("PGDATABASE") or os.getenv("LAKEBASE_DATABASE", "databricks_postgres")
WS_ID = os.getenv("WORKSPACE_ID", "")
DAY = datetime(2026, 10, 27)
END = DAY.replace(hour=23, minute=55)
DOMAINS = ["Authentication", "Billing & Payments", "Playback & CDN", "Content Ingestion",
           "Subtitles & Localization", "Device Apps", "Search & Discovery"]
REGIONS = ["NA", "LATAM", "EMEA", "APAC"]
STATE_COLORS = {"Healthy": "#1e8a52", "Watch": "#8a9a2b", "Recovering": "#2563eb", "Telemetry Alert": "#ea580c",
                "Known Incident": "#d97706", "Known (declared in app)": "#d97706", "Customer-Detected": "#e11d74"}
JUMPS = {"08:30 Start of demo": "08:30", "09:00 APAC login failures": "09:00", "14:00 Simulcast finale (benign)": "14:00",
         "15:40 Error 1002 · Brazil": "15:40", "18:10 Spirit Ronin E3 subtitles": "18:10", "20:05 Adyen payments · DE": "20:05"}
# scenario -> AI-detected anomaly it showcases (None = no incident in that scenario)
JUMP_ANOM = {"08:30 Start of demo": None, "09:00 APAC login failures": "ANM-1027-0900-e915", "14:00 Simulcast finale (benign)": None,
             "15:40 Error 1002 · Brazil": "ANM-1027-1530-47b0", "18:10 Spirit Ronin E3 subtitles": "ANM-1027-1800-e355",
             "20:05 Adyen payments · DE": "ANM-1027-2000-d79b"}
NO_INC = "— No incident selected —"

PERSONAS = {
    "exec": dict(label="C-Suite", tab="Executive", sub="Customer & Platform Health", tag="Executive Strategy",
                 about="Chief Customer / Content / Technology officers. Wants one view of subscriber impact, revenue at risk, and whether we hear about problems from customers before our own monitoring.",
                 blurb="Portfolio view for Fanimation leadership: subscriber impact, detection lead time, CSAT and revenue at risk, rolled up from the same data the operators drill into."),
    "support": dict(label="Support Operations", tab="Support Ops", sub="Queues & Customer Impact", tag="VP Support / Support Ops Manager",
                    about="Runs the global support org. Needs to spot ticket surges early, push known-issue banners and macros to agents, and protect SLA and CSAT during incidents.",
                    blurb="Live view of ticket surges, SLA and CSAT, with AI-written summaries and customer macros ready to publish to agents."),
    "eng": dict(label="Engineering / SRE", tab="Engineering", sub="Reliability & Incidents", tag="Platform Reliability / Incident Command",
                about="Owns authentication, playback/CDN, payments, content ingestion and localization. Uses support signal as a second monitoring system and correlates it with deploys.",
                blurb="Service health by domain and region, telemetry versus customer contact rate, and the deploys and config changes that preceded each surge."),
}

# ----------------------------------------------------------------------------- styling
st.markdown("""
<style>
header[data-testid="stHeader"] {display:none;}
.block-container {padding-top: 1.2rem; max-width: 1400px;}
.stApp {background:#f6f5f1;}
html, body, [class*="css"] {font-family: 'Inter', 'Segoe UI', sans-serif;}
[data-testid="stSidebar"] {background:#1c3a3f; min-width:290px; max-width:290px;}
[data-testid="stSidebar"] * {color:#dce8e6;}
[data-testid="stSidebar"] .stButton button {background:transparent; border:none; box-shadow:none; justify-content:flex-start;
   width:100%; padding:8px 14px; font-size:15px; border-radius:10px; color:#e7f0ee;}
[data-testid="stSidebar"] .stButton button:hover {background:#2a4b50;}
[data-testid="stSidebar"] .stButton button p {font-size:15px;}
[data-testid="stSidebar"] [data-baseweb="select"] > div {background:#2a4b50; border:none; border-radius:10px;}
[data-testid="stSidebar"] hr {border-color:#2f5156; margin:10px 0;}
.sb-brand {display:flex; gap:12px; align-items:center; padding:4px 4px 10px 4px;}
.sb-logo {width:40px; height:40px; border-radius:10px; background:#f47521; display:flex; align-items:center; justify-content:center; font-weight:900; color:#fff !important; font-size:20px;}
.sb-name {font-weight:700; font-size:16px; color:#fff !important; line-height:1.2;}
.sb-sub {font-size:13px; color:#b7c9c6 !important;}
.sb-powered {font-size:10px; letter-spacing:1.4px; color:#9fb5b2 !important; padding:6px 4px;}
.sb-powered b {color:#ff5f46 !important; letter-spacing:0; font-size:14px;}
.sb-section {font-size:11px; letter-spacing:1.4px; color:#9fb5b2 !important; text-transform:uppercase; padding:10px 14px 2px 14px;}
.sb-tag {font-size:12px; color:#9fb5b2 !important; padding:0 4px;}
.topbar-label {font-size:11px; letter-spacing:1.4px; color:#8a8a8a; text-transform:uppercase; padding-top:10px;}
.hero {background:#1b2f36; border-radius:16px; padding:30px 34px; color:#fff; margin:6px 0 18px 0;}
.hero .eyebrow {color:#ff6a3d; font-weight:700; font-size:15px;}
.hero h1 {color:#fff !important; font-size:32px; margin:4px 0 4px 0; padding:0;}
.hero .lead {color:#d7dee0; font-size:16px;}
.hero .kgrid {display:grid; grid-template-columns: repeat(4, 1fr); gap:20px; margin-top:28px;}
.hero .kl {font-size:12px; letter-spacing:1.2px; color:#b7c2c6; text-transform:uppercase;}
.hero .kv {font-size:38px; font-weight:800; line-height:1.15;}
.hero .ks {font-size:14px; color:#b7c2c6;}
.panel {background:#fff; border:1px solid #ece9e2; border-radius:14px; padding:18px 22px; margin-bottom:16px;}
.panel h3 {font-size:19px !important; margin:0 0 2px 0; color:#1f2a2e !important;}
.panel .sub {font-size:13px; color:#8a8a8a; margin-bottom:12px;}
.barrow {display:grid; grid-template-columns: 26px 210px 1fr; align-items:center; gap:10px; margin:9px 0; font-size:14px;}
.bartrack {background:#efede7; border-radius:999px; height:26px; position:relative;}
.barfill {height:26px; border-radius:999px; color:#fff; font-size:12px; font-weight:700; display:flex; align-items:center; padding-left:12px; min-width:60px;}
.blurb {color:#3b4448; font-size:15px; margin: 8px 0 14px 0; max-width: 900px;}
.matrix {display:grid; grid-template-columns: 180px repeat(4, 1fr); gap:6px;}
.mh {color:#8a8a8a; font-size:.72rem; text-transform:uppercase; text-align:center; padding:4px; letter-spacing:1px;}
.md {color:#1f2a2e; font-size:.85rem; padding:10px 6px; font-weight:600;}
.cell {border-radius:8px; padding:8px; min-height:56px; color:#fff; font-size:.72rem; line-height:1.25;}
.cell b {font-size:.76rem; display:block;}
.pulse {animation: pulse 1.2s infinite; border:2px solid #ff4fa3;}
@keyframes pulse {0% {box-shadow:0 0 0 0 #e11d74aa;} 70% {box-shadow:0 0 0 12px #e11d7400;} 100% {box-shadow:0 0 0 0 #e11d7400;}}
.card {background:#fff; border:1px solid #ece9e2; border-radius:12px; padding:14px 16px; margin-bottom:10px;}
.card.hot {border-color:#e11d74; box-shadow:0 0 0 2px #e11d7422;}
.badge {display:inline-block; padding:2px 9px; border-radius:999px; font-size:.7rem; font-weight:700; margin-right:6px; color:#fff;}
.post {border-left:3px solid #ccc; padding:4px 10px; margin:5px 0; font-size:.84rem; color:#2b3437; background:#fff; border-radius:0 8px 8px 0;}
.alertrow {display:flex; gap:12px; align-items:flex-start; padding:12px 0; border-bottom:1px solid #f0eee8;}
.dot {width:10px; height:10px; border-radius:50%; margin-top:6px; flex-shrink:0;}
.arch {display:grid; grid-template-columns: repeat(5, 1fr); gap:12px;}
.archcol {background:#fff; border:1px solid #ece9e2; border-radius:12px; padding:14px;}
.archcol h4 {font-size:14px !important; margin:0 0 6px 0; color:#1c3a3f !important;}
.archcol li {font-size:13px; color:#3b4448;}
.pill {display:inline-block; background:#efede7; border-radius:999px; padding:2px 10px; font-size:12px; margin:2px 4px 2px 0; color:#1f2a2e;}
</style>""", unsafe_allow_html=True)


def style_fig(fig, h=320, legend=True):
    fig.update_layout(height=h, margin=dict(l=0, r=0, t=10, b=0), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font=dict(color="#2b3437", size=12), showlegend=legend, legend=dict(orientation="h", y=-0.18, title=None))
    fig.update_xaxes(gridcolor="#eeebe4"); fig.update_yaxes(gridcolor="#eeebe4")
    return fig


# ----------------------------------------------------------------------------- data access
@st.cache_resource
def ws():
    return WorkspaceClient()


@st.cache_resource
def sql_conn():
    from databricks import sql
    cfg = Config()
    return sql.connect(server_hostname=cfg.host.replace("https://", ""), http_path=f"/sql/1.0/warehouses/{WH}",
                       credentials_provider=lambda: cfg.authenticate)


def q(query: str) -> pd.DataFrame:
    def run():
        with sql_conn().cursor() as c:
            c.execute(query)
            return c.fetchall_arrow().to_pandas()
    try:
        df = run()
    except Exception:
        sql_conn.clear(); df = run()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            s = pd.to_datetime(df[col])
            df[col] = s.dt.tz_convert(None) if s.dt.tz is not None else s
    return df


@st.cache_data(ttl=3600, show_spinner="Loading demo-day telemetry from Unity Catalog...")
def load_day():
    d0, d1 = "2026-10-27", "2026-10-28"
    w = f"ts >= '{d0}' AND ts < '{d1}'"
    return {
        "heat": q(f"SELECT * FROM {S}.gold_region_heatmap_5m WHERE {w}"),
        "health": q(f"SELECT * FROM {S}.gold_service_health_region_5m WHERE {w}"),
        "frus": q(f"SELECT * FROM {S}.gold_frustration_index_5m WHERE {w}"),
        "anom": q(f"SELECT * EXCEPT (sample_tickets), to_json(ai_recommended_actions) actions_json FROM {S}.gold_ticket_anomalies WHERE first_seen >= '{d0}' AND first_seen < '{d1}'"),
        "anom_all": q(f"SELECT * EXCEPT (sample_tickets), to_json(ai_recommended_actions) actions_json, to_json(sample_tickets) samples_json FROM {S}.gold_ticket_anomalies"),
        "sig": q(f"SELECT * FROM {S}.gold_signature_trends_15m WHERE ts_15m >= '{d0}' AND ts_15m < '{d1}'"),
        "pay": q(f"SELECT * FROM {S}.gold_payment_health_5m WHERE {w}"),
        "social": q(f"SELECT posted_at, platform, author_handle, country_code, region_group, post_text, engagement, sentiment, topic_domain, ts_15m FROM {S}.gold_social_posts WHERE posted_at >= '{d0}' AND posted_at < '{d1}'"),
        "tix": q(f"""SELECT ts_5m AS ts, region_group, issue_domain, count(*) n, sum(CAST(sla_met AS INT)) sla_met, sum(csat_score) csat_sum, count(csat_score) csat_n,
                       sum(CASE WHEN subscriber_tier IN ('Mega Fan','Ultimate Fan') THEN 1 ELSE 0 END) premium
                     FROM {S}.silver_tickets WHERE ts_5m >= '{d0}' AND ts_5m < '{d1}' GROUP BY ALL"""),
        "score": q(f"SELECT * FROM {S}.gold_incident_scorecard"),
        "kpi": q(f"SELECT * FROM {S}.gold_exec_daily_kpis"),
        "changes": q(f"SELECT * FROM {S}.change_events WHERE change_ts >= '{d0}' AND change_ts < '{d1}' AND (change_type <> 'deployment' OR outcome <> 'success')"),
    }


# ----------------------------------------------------------------------------- Lakebase ops store
class Ops:
    DDL = [
        "CREATE SCHEMA IF NOT EXISTS command_center_ops",
        """CREATE TABLE IF NOT EXISTS command_center_ops.incident_actions (id BIGSERIAL PRIMARY KEY, anomaly_id TEXT, issue_signature TEXT,
           domain TEXT, region_groups TEXT, action TEXT, actor TEXT, note TEXT, demo_clock TIMESTAMP, created_at TIMESTAMPTZ DEFAULT now())""",
        """CREATE TABLE IF NOT EXISTS command_center_ops.known_issue_banners (anomaly_id TEXT PRIMARY KEY, headline TEXT, macro TEXT,
           active BOOLEAN DEFAULT TRUE, created_at TIMESTAMPTZ DEFAULT now())""",
        """CREATE TABLE IF NOT EXISTS command_center_ops.alert_acks (id BIGSERIAL PRIMARY KEY, region_group TEXT, domain TEXT, health_state TEXT,
           actor TEXT, demo_clock TIMESTAMP, created_at TIMESTAMPTZ DEFAULT now())""",
    ]

    def __init__(self):
        self.ok, self.err = False, None
        try:
            with self.conn() as c:
                for d in self.DDL:
                    c.execute(d)
                c.commit()
            self.ok = True
        except Exception as e:
            self.err = str(e)[:300]

    def conn(self):
        import psycopg
        w = ws()
        host = os.getenv("PGHOST") or w.postgres.get_endpoint(name=LB_EP).status.hosts.host
        user = os.getenv("PGUSER") or os.getenv("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name
        tok = w.postgres.generate_database_credential(endpoint=LB_EP).token
        return psycopg.connect(host=host, dbname=LB_DB, user=user, password=tok, sslmode="require", connect_timeout=8)

    def _mem(self, k):
        return st.session_state.setdefault(f"ops_{k}", [])

    def _exec(self, sql_, params=None, fetch=False):
        with self.conn() as c:
            cur = c.execute(sql_, params)
            rows = cur.fetchall() if fetch else None
            c.commit()
            return rows

    def declare(self, a, actor, clock):
        if self.ok:
            try:
                self._exec("""INSERT INTO command_center_ops.incident_actions (anomaly_id, issue_signature, domain, region_groups, action, actor, note, demo_clock)
                              VALUES (%s,%s,%s,%s,'declare_incident',%s,'Declared from Support Command Center',%s)""",
                           (a["anomaly_id"], a["issue_signature"], a["issue_domain"], a["region_groups"], actor, clock))
                self._exec("""INSERT INTO command_center_ops.known_issue_banners (anomaly_id, headline, macro, active) VALUES (%s,%s,%s,TRUE)
                              ON CONFLICT (anomaly_id) DO UPDATE SET active=TRUE, macro=EXCLUDED.macro""", (a["anomaly_id"], a["ai_headline"], a["ai_customer_macro"]))
                return
            except Exception as e:
                self.err = str(e)[:300]
        self._mem("actions").append(dict(anomaly_id=a["anomaly_id"], domain=a["issue_domain"], region_groups=a["region_groups"], action="declare_incident", actor=actor, created_at=datetime.utcnow()))
        self._mem("banners").append(dict(anomaly_id=a["anomaly_id"], headline=a["ai_headline"], macro=a["ai_customer_macro"], created_at=datetime.utcnow()))

    def ack(self, rg, dom, state, actor, clock):
        if self.ok:
            try:
                self._exec("INSERT INTO command_center_ops.alert_acks (region_group, domain, health_state, actor, demo_clock) VALUES (%s,%s,%s,%s,%s)", (rg, dom, state, actor, clock))
                return
            except Exception as e:
                self.err = str(e)[:300]
        self._mem("acks").append(dict(region_group=rg, domain=dom, health_state=state, actor=actor, created_at=datetime.utcnow()))

    def frame(self, kind):
        sqls = {"actions": ("SELECT anomaly_id, domain, region_groups, action, actor, demo_clock, created_at FROM command_center_ops.incident_actions ORDER BY created_at DESC",
                            ["anomaly_id", "domain", "region_groups", "action", "actor", "demo_clock", "created_at"]),
                "banners": ("SELECT anomaly_id, headline, macro, created_at FROM command_center_ops.known_issue_banners WHERE active ORDER BY created_at DESC",
                            ["anomaly_id", "headline", "macro", "created_at"]),
                "acks": ("SELECT region_group, domain, health_state, actor, demo_clock, created_at FROM command_center_ops.alert_acks ORDER BY created_at DESC",
                         ["region_group", "domain", "health_state", "actor", "demo_clock", "created_at"])}
        sql_, cols = sqls[kind]
        if self.ok:
            try:
                return pd.DataFrame(self._exec(sql_, fetch=True), columns=cols)
            except Exception as e:
                self.err = str(e)[:300]
        m = self._mem(kind)
        return pd.DataFrame(m) if m else pd.DataFrame(columns=cols)

    def reset(self):
        if self.ok:
            try:
                self._exec("UPDATE command_center_ops.known_issue_banners SET active=FALSE")
                self._exec("DELETE FROM command_center_ops.incident_actions WHERE action='declare_incident'")
            except Exception as e:
                self.err = str(e)[:300]
        for k in ["actions", "banners", "acks"]:
            st.session_state[f"ops_{k}"] = []


@st.cache_resource
def ops():
    return Ops()


def user_email():
    try:
        return st.context.headers.get("X-Forwarded-Email") or ""
    except Exception:
        return ""


def first_name():
    try:
        n = st.context.headers.get("X-Forwarded-Preferred-Username") or user_email()
    except Exception:
        n = ""
    n = (n or "there").split("@")[0].split(".")[0]
    return n[:1].upper() + n[1:]


# ----------------------------------------------------------------------------- Genie
def genie_ask(question: str):
    w = ws()
    conv = st.session_state.get("genie_conv")
    if conv:
        msg = w.genie.create_message_and_wait(space_id=GENIE, conversation_id=conv, content=question, timeout=timedelta(minutes=3))
    else:
        msg = w.genie.start_conversation_and_wait(space_id=GENIE, content=question, timeout=timedelta(minutes=3))
        st.session_state["genie_conv"] = msg.conversation_id
    texts, tables, sqls = [], [], []
    for a in (msg.attachments or []):
        if a.text and a.text.content:
            texts.append(a.text.content)
        if a.query:
            sqls.append(a.query.query)
            try:
                r = w.genie.get_message_attachment_query_result(space_id=GENIE, conversation_id=msg.conversation_id,
                                                               message_id=msg.id, attachment_id=a.attachment_id)
                sr = r.statement_response
                cols = [c.name for c in sr.manifest.schema.columns]
                rows = (sr.result.data_array or []) if sr.result else []
                tables.append(pd.DataFrame(rows, columns=cols))
            except Exception:
                pass
    return dict(q=question, texts=texts, tables=tables, sqls=sqls)


GENIE_SUGGEST = {
    "exec": ["Which incidents were detected by customers before monitoring and by how many minutes?", "How much revenue was at risk from payment failures this week?",
             "Compare CSAT and SLA met rate today vs the previous 7 days by region"],
    "support": ["What is driving the ticket spike in LATAM today?", "What did social media say about Spirit Ronin episode 3?",
                "Which region and domain had the most Customer-Detected minutes today?"],
    "eng": ["Why did Brazil turn red at 15:45 UTC?", "Which deployments preceded incidents?", "Show payment success in Germany by gateway today"],
}


# ----------------------------------------------------------------------------- state & helpers
ss = st.session_state
ss.setdefault("persona", "exec"); ss.setdefault("page", "home")
ss.setdefault("clock", DAY.replace(hour=15, minute=45)); ss.setdefault("playing", False); ss.setdefault("speed", 3.0)


def go_page(p):
    ss.page = p


def set_persona(p):
    ss.persona = p
    ss.persona_sel = PERSONAS[p]["label"]
    ss.page = "home"


def at(df, clock, col="ts"):
    t = df.loc[df[col] <= clock, col].max()
    return df[df[col] == t]


def effective_health(D, clock):
    h = at(D["health"], clock).copy()
    declared = ops().frame("actions")
    if len(declared):
        an = D["anom"].set_index("anomaly_id")
        for aid in declared.anomaly_id.unique():
            if aid in an.index and an.loc[aid, "detected_at"] <= clock <= an.loc[aid, "last_seen"] + timedelta(minutes=30):
                rgs = [r.strip() for r in str(an.loc[aid, "region_groups"]).split(",")]
                m = (h.domain == an.loc[aid, "issue_domain"]) & h.region_group.isin(rgs) & h.health_state.isin(["Customer-Detected", "Telemetry Alert", "Watch"])
                h.loc[m, "health_state"] = "Known (declared in app)"
                h.loc[m, "open_incident_id"] = "declared in app"
    return h


def alerts_now(D, clock):
    h = effective_health(D, clock)
    items = []
    for r in h[h.health_state.isin(["Customer-Detected", "Telemetry Alert"])].itertuples():
        items.append(dict(sev="critical" if r.health_state == "Customer-Detected" else "high", title=f"{r.domain} · {r.region_group}: {r.health_state}",
                          detail=f"Tickets x{r.ticket_ratio:.1f} vs expected · internal errors x{r.error_ratio:.1f}" + (" · no internal alert yet" if r.health_state == "Customer-Detected" else "")))
    declared = set(ops().frame("actions").anomaly_id) if len(ops().frame("actions")) else set()
    an = D["anom"]; an = an[(an.detected_at <= clock) & (an.last_seen >= clock)]
    for a in an.itertuples():
        known = (isinstance(a.incident_declared_at, pd.Timestamp) and a.incident_declared_at <= clock) or a.anomaly_id in declared
        if not known:
            items.append(dict(sev="critical", title=f"Undeclared ticket surge: {a.issue_signature}", detail=f"{a.ai_headline} · {a.top_countries}"))
    p = at(D["pay"], clock)
    for r in p[p.gateway_state != "Healthy"].itertuples():
        items.append(dict(sev="high" if r.gateway_state == "Degraded" else "critical", title=f"Payment gateway {r.gateway} · {r.country_code}: {r.gateway_state}",
                          detail=f"Success {r.success_rate_15m*100:.1f}% vs {r.baseline_success_rate*100:.1f}% baseline"))
    return items


def panel_open(title, sub=""):
    st.markdown(f"<div class='panel'><h3>{html.escape(title)}</h3><div class='sub'>{html.escape(sub)}</div>", unsafe_allow_html=True)


def barlist(title, sub, rows, color="#1e8a52", fmt=lambda v: f"{v:,.0f}"):
    mx = max([v for _, v in rows] + [1])
    body = "".join(f"<div class='barrow'><div style='color:#8a8a8a'>{i+1}</div><div style='font-weight:600'>{html.escape(str(k))}</div>"
                   f"<div class='bartrack'><div class='barfill' style='width:{max(6, 100*v/mx):.1f}%;background:{color}'>{fmt(v)}</div></div></div>"
                   for i, (k, v) in enumerate(rows))
    st.markdown(f"<div class='panel'><h3>{html.escape(title)}</h3><div class='sub'>{html.escape(sub)}</div>{body}</div>", unsafe_allow_html=True)


def hero(eyebrow, lead, kpis):
    cells = "".join(f"<div><div class='kl'>{k}</div><div class='kv' style='color:{c}'>{v}</div><div class='ks'>{s}</div></div>" for k, v, s, c in kpis)
    hour = ss.clock.hour
    greet = "Good morning" if hour < 12 else ("Good afternoon" if hour < 18 else "Good evening")
    st.markdown(f"<div class='hero'><div class='eyebrow'>{eyebrow}</div><h1>{greet}, {html.escape(first_name())}</h1>"
                f"<div class='lead'>{lead}</div><div class='kgrid'>{cells}</div></div>", unsafe_allow_html=True)


# ----------------------------------------------------------------------------- sidebar
def sidebar(D):
    with st.sidebar:
        st.markdown("<div class='sb-brand'><div class='sb-logo'>C</div><div><div class='sb-name'>Fanimation</div><div class='sb-sub'>Support Command Center</div></div></div>"
                    "<div class='sb-powered'>POWERED BY &nbsp;<b>◆ databricks</b></div>", unsafe_allow_html=True)
        st.divider()
        st.markdown("<div class='sb-section' style='padding-left:4px'>Viewing as</div>", unsafe_allow_html=True)
        labels = {v["label"]: k for k, v in PERSONAS.items()}
        ss.setdefault("persona_sel", PERSONAS[ss.persona]["label"])
        st.selectbox("Viewing as", list(labels), key="persona_sel", label_visibility="collapsed",
                     on_change=lambda: set_persona(labels[ss.persona_sel]))
        st.markdown(f"<div class='sb-tag'>{PERSONAS[ss.persona]['tag']}</div>", unsafe_allow_html=True)
        with st.expander("👤 About this role"):
            st.write(PERSONAS[ss.persona]["about"])
        st.divider()
        n_alerts = len(alerts_now(D, ss.clock))
        nav = [("home", "Home", ":material/home:"), ("alerts", f"Alerts  :red-background[ {n_alerts} ]" if n_alerts else "Alerts", ":material/notifications:")]
        dash = [("pulse", "Platform Pulse", ":material/public:"), ("subscriber", "Subscriber Experience", ":material/support_agent:"),
                ("payments", "Payments & Social", ":material/payments:"), ("executive", "Executive Overview", ":material/monitoring:")]
        agent = [("genie", "Genie", ":material/auto_awesome:"), ("brief", "Incident Brief", ":material/description:"),
                 ("console", "Ops Console (Lakebase)", ":material/database:")]
        def items(lst):
            for key, label, icon in lst:
                st.button(label, key=f"nav_{key}", icon=icon, on_click=go_page, args=(key,), use_container_width=True)
        items(nav)
        st.markdown("<div class='sb-section'>Dashboards</div>", unsafe_allow_html=True)
        items(dash)
        st.markdown("<div class='sb-section'>Agentic-powered research</div>", unsafe_allow_html=True)
        items(agent)
        st.divider()
        o = ops()
        st.caption(f"Lakebase ops store: {'🟢 connected' if o.ok else '🟡 session-only'}")
        if st.button("Reset demo", key="reset", icon=":material/restart_alt:", use_container_width=True):
            o.reset(); ss.clock = DAY.replace(hour=8, minute=30); ss.playing = False
    # active nav + persona card highlight
    st.markdown(f"<style>.st-key-nav_{ss.page} button {{background:#2f5156 !important;}}"
                f".st-key-pc_{ss.persona} button {{background:#efede7 !important; box-shadow:0 2px 6px #0000000f !important;}}</style>", unsafe_allow_html=True)


# ----------------------------------------------------------------------------- top bar
def topbar():
    st.markdown("""<style>
    .st-key-topbar [data-testid="stHorizontalBlock"] {align-items:center;}
    .st-key-about button, .st-key-arch button, .st-key-genie_top button {background:transparent; border:none; box-shadow:none; color:#1f2a2e; font-size:15px;}
    .st-key-about button:hover, .st-key-arch button:hover {color:#f47521;}
    [class*="st-key-pc_"] button {background:#fff; border:1px solid #ece9e2; border-radius:12px; padding:14px 16px; height:66px;
        justify-content:flex-start; text-align:left; color:#1f2a2e;}
    [class*="st-key-pc_"] button p {text-align:left; font-size:14px; line-height:1.3;}
    </style>""", unsafe_allow_html=True)
    with st.container(key="topbar"):
        c = st.columns([0.9, 2.1, 0.8, 0.8, 0.8, 2.6, 0.7, 0.9, 0.4, 0.4])
        c[0].markdown("<div class='topbar-label'>Demo clock</div>", unsafe_allow_html=True)
        times = [DAY + timedelta(minutes=5 * i) for i in range(288)]
        ss.setdefault("clock_sel", ss.clock)
        if ss.clock_sel != ss.clock:
            ss.clock_sel = ss.clock
        def on_clock():
            ss.clock = ss.clock_sel; ss.playing = False
        c[1].select_slider("Demo clock", options=times, key="clock_sel", format_func=lambda t: f"{t:%H:%M} UTC", label_visibility="collapsed", on_change=on_clock)
        if c[2].button("⏸" if ss.playing else "▶", key="play", help="Play / pause replay", use_container_width=True):
            ss.playing = not ss.playing; st.rerun()
        if c[3].button("+15m", key="fwd", use_container_width=True):
            ss.clock = min(ss.clock + timedelta(minutes=15), END); ss.playing = False; st.rerun()
        c[4].selectbox("Speed", [1.0, 2.0, 3.0, 5.0], key="speed", format_func=lambda s: f"{s:g}s / 5m", label_visibility="collapsed")
        def on_jump():
            j = ss.get("jump")
            if not j or j not in JUMPS:          # scenario cleared -> keep clock, clear the selected incident
                ss.brief_id = None; ss.brief_sync = True
                return
            h, m = map(int, JUMPS[j].split(":")); ss.clock = DAY.replace(hour=h, minute=m); ss.playing = False
            ss.brief_id = JUMP_ANOM.get(j); ss.brief_sync = True
        c[5].selectbox("Jump to scenario", list(JUMPS), key="jump", index=None, placeholder="Jump to scenario…", label_visibility="collapsed", on_change=on_jump)
        c[6].button("About", key="about", on_click=go_page, args=("about",))
        c[7].button("Architecture", key="arch", on_click=go_page, args=("architecture",))
        c[8].button("", key="genie_top", icon=":material/auto_awesome:", help="Ask Genie", on_click=go_page, args=("genie",))
        init = (first_name() or "U")[:1].upper()
        c[9].markdown(f"<div style='width:36px;height:36px;border-radius:50%;background:#1b2f36;color:#fff;display:flex;align-items:center;justify-content:center;font-weight:700'>{init}</div>", unsafe_allow_html=True)
    st.markdown("<hr style='margin:4px 0 14px 0;border-color:#e7e4dc'>", unsafe_allow_html=True)


def persona_cards():
    c = st.columns(3)
    for i, (k, p) in enumerate(PERSONAS.items()):
        c[i].button(f"**{p['tab']}**  \n{p['sub']}", key=f"pc_{k}", icon=":material/bar_chart:", on_click=set_persona, args=(k,), use_container_width=True)
    st.markdown(f"<div class='blurb'>{PERSONAS[ss.persona]['blurb']}</div>", unsafe_allow_html=True)


# ----------------------------------------------------------------------------- shared widgets
def health_matrix(D, clock, ack=True):
    h = effective_health(D, clock)
    cells = ["<div class='matrix'><div></div>" + "".join(f"<div class='mh'>{r}</div>" for r in REGIONS)]
    hi = h.set_index(["domain", "region_group"])
    for d in DOMAINS:
        cells.append(f"<div class='md'>{d}</div>")
        for r in REGIONS:
            if (d, r) in hi.index:
                x = hi.loc[(d, r)]; s_ = x.health_state; col = STATE_COLORS.get(s_, "#1e8a52")
                sub = "no internal alert" if s_ == "Customer-Detected" else (x.open_incident_id if isinstance(x.open_incident_id, str) else "")
                cells.append(f"<div class='cell {'pulse' if s_=='Customer-Detected' else ''}' style='background:{col}'>"
                             f"<b>{html.escape(s_.upper() if s_=='Customer-Detected' else s_)}</b>{html.escape(sub)}<br>tix x{x.ticket_ratio:.1f} · err x{x.error_ratio:.1f}</div>")
            else:
                cells.append("<div class='cell' style='background:#ccc'>n/a</div>")
    cells.append("</div>")
    st.markdown("".join(cells), unsafe_allow_html=True)
    st.caption("Customer-Detected = support tickets spiking with no internal alert or open incident · tix = tickets vs expected · err = internal error rate vs baseline")
    hot = h[h.health_state.isin(["Customer-Detected", "Telemetry Alert"])]
    if ack and len(hot):
        opts = [f"{r.domain} · {r.region_group} ({r.health_state})" for r in hot.itertuples()]
        a, b = st.columns([3, 1])
        pick = a.selectbox("Acknowledge alert", opts, label_visibility="collapsed", key=f"ackpick_{ss.page}")
        if b.button("Acknowledge", key=f"ack_{ss.page}", use_container_width=True):
            r = hot.iloc[opts.index(pick)]; ops().ack(r.region_group, r.domain, r.health_state, user_email() or "demo-user", clock)
            st.toast("Acknowledged · logged to Lakebase")


def geo_map(D, clock, key="map"):
    heat = at(D["heat"], clock).copy()
    layer = st.radio("Layer", ["Support tickets vs expected", "Login failures", "Playback errors", "Payment success"], horizontal=True, label_visibility="collapsed", key=f"layer_{key}")
    if layer.startswith("Support"):
        heat["v"] = heat.ticket_deviation_ratio; cs, rng_, title = [[0, "#1e8a52"], [0.25, "#facc15"], [0.6, "#f97316"], [1, "#e11d74"]], (0.5, 8), "x expected"
    elif layer.startswith("Login"):
        heat["v"] = heat.login_failure_rate * 100; cs, rng_, title = "YlOrRd", (0, 5), "% login fail"
    elif layer.startswith("Playback"):
        heat["v"] = heat.playback_error_rate * 100; cs, rng_, title = "YlOrRd", (0, 3), "% playback err"
    else:
        heat["v"] = heat.payment_success_rate * 100; cs, rng_, title = "RdYlGn", (85, 99), "% pay success"
    heat["size"] = (heat.tickets_15m.clip(lower=1) ** 0.5) * 6 + 6
    heat["label"] = heat.apply(lambda r: f"<b>{r.country_name}</b><br>{r.worst_health_state}<br>Tickets 15m: {int(r.tickets_15m)} (exp {r.expected_tickets_15m:.0f})<br>Streams: {int(r.streams):,} ({r.stream_deviation_pct*100:+.1f}% vs baseline)<br>Top domain: {r.top_domain}", axis=1)
    fig = go.Figure()
    hot = heat[heat.worst_health_state == "Customer-Detected"]
    if len(hot):
        fig.add_trace(go.Scattergeo(lat=hot.lat, lon=hot.lon, mode="markers", hoverinfo="skip", showlegend=False,
                                    marker=dict(size=hot["size"] * 2.2, color="rgba(225,29,116,0.18)", line=dict(width=2, color="#e11d74"))))
    fig.add_trace(go.Scattergeo(lat=heat.lat, lon=heat.lon, text=heat.label, hoverinfo="text", mode="markers", showlegend=False,
                                marker=dict(size=heat["size"], color=heat["v"], colorscale=cs, cmin=rng_[0], cmax=rng_[1], line=dict(width=0.6, color="#fff"),
                                            colorbar=dict(title=title, thickness=10, len=0.6))))
    fig.update_geos(projection_type="natural earth", showland=True, landcolor="#e9e6df", showocean=True, oceancolor="#f6f5f1",
                    showcountries=True, countrycolor="#d6d2c8", coastlinecolor="#d6d2c8", bgcolor="rgba(0,0,0,0)", lataxis_range=[-50, 72])
    fig.update_layout(height=380, margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=key)


def telemetry_chart(D, clock, default="Global", key="tele", h=320):
    fr = D["frus"]
    scopes = ["Global"] + REGIONS + sorted(fr[fr.scope_type == "Country"].scope.unique().tolist())
    c1, c2 = st.columns([4, 1])
    with c2:
        scope = st.selectbox("Scope", scopes, index=scopes.index(default), key=f"scope_{key}")
        cur = at(fr[fr.scope == scope], clock)
        fi = float(cur.frustration_index.iloc[0]) if len(cur) else 0
        g = go.Figure(go.Indicator(mode="gauge+number", value=fi, title={"text": "Frustration index", "font": {"size": 13}},
                                   gauge={"axis": {"range": [0, 100]}, "bar": {"color": "#f47521"},
                                          "steps": [{"range": [0, 25], "color": "#dcf2e4"}, {"range": [25, 55], "color": "#fdf0cf"}, {"range": [55, 100], "color": "#fbd5e5"}]}))
        st.plotly_chart(style_fig(g, 200, False), use_container_width=True, config={"displayModeBar": False}, key=f"g_{key}")
        if len(cur) and bool(cur.divergence_flag.iloc[0]):
            st.error("Divergence: streams dropping while contacts spike — users are hitting breakage.")
        elif len(cur) and isinstance(cur.active_release.iloc[0], str):
            st.info(f"Simulcast in effect: {cur.active_release.iloc[0]} — traffic spike is expected.")
    with c1:
        s = fr[fr.scope == scope].sort_values("ts"); past = s[s.ts <= clock]
        f = go.Figure()
        f.add_trace(go.Scatter(x=s.ts, y=s.baseline_streams, name="Release-aware baseline", line=dict(color="#9ca3af", dash="dot")))
        f.add_trace(go.Scatter(x=past.ts, y=past.streams, name="Concurrent streams", line=dict(color="#1c3a3f", width=2.5)))
        f.add_trace(go.Scatter(x=past.ts, y=past.contacts_per_10k_streams, name="Contacts / 10k streams", yaxis="y2", line=dict(color="#f47521", width=2)))
        f.add_trace(go.Scatter(x=past.ts, y=past.baseline_contacts_per_10k_streams, name="Expected contacts", yaxis="y2", line=dict(color="#f4752166", dash="dash")))
        for t in past[past.divergence_flag].ts:
            f.add_vrect(x0=t - timedelta(minutes=2.5), x1=t + timedelta(minutes=2.5), fillcolor="#e11d74", opacity=0.15, line_width=0)
        for name, t in s.dropna(subset=["active_release"]).groupby("active_release").ts.min().items():
            if t <= clock:
                f.add_annotation(x=t, y=1.03, yref="paper", text=f"🎬 {name}", showarrow=False, font=dict(size=10, color="#7c3aed"))
        for r in D["changes"][D["changes"].change_ts <= clock].itertuples():
            f.add_vline(x=r.change_ts, line=dict(color="#d4a017", width=1, dash="dot"))
        f.add_vline(x=clock, line=dict(color="#f47521", width=2))
        f.update_layout(xaxis=dict(range=[DAY, DAY + timedelta(days=1)]), yaxis=dict(title="Streams"),
                        yaxis2=dict(title="Contacts / 10k", overlaying="y", side="right", showgrid=False))
        st.plotly_chart(style_fig(f, h), use_container_width=True, config={"displayModeBar": False}, key=f"t_{key}")
        st.caption("Pink = divergence · dotted gold = risky deploys/config changes · purple = simulcast releases (benign spikes)")


def anomaly_cards(D, clock, compact=False, limit=None):
    an = D["anom"]; an = an[an.detected_at <= clock].sort_values("detected_at", ascending=False)
    if limit:
        an = an.head(limit)
    acts = ops().frame("actions"); declared = set(acts.anomaly_id) if len(acts) else set()
    if an.empty:
        st.info("No ticket surges detected yet — advance the demo clock.")
    for a in an.to_dict("records"):
        active = clock <= a["last_seen"]
        inc_open = isinstance(a.get("incident_declared_at"), pd.Timestamp) and a["incident_declared_at"] <= clock
        in_app = a["anomaly_id"] in declared
        age = int((clock - a["detected_at"]).total_seconds() // 60)
        if not active:
            badge = "<span class='badge' style='background:#2563eb'>SUBSIDED</span>"
        elif inc_open:
            badge = f"<span class='badge' style='background:#d97706'>KNOWN · {a['incident_id']}</span>"
        elif in_app:
            badge = "<span class='badge' style='background:#d97706'>DECLARED IN APP</span>"
        else:
            badge = f"<span class='badge' style='background:#e11d74'>UNDECLARED · customers detected {age} min ago</span>"
        try:
            actions = json.loads(a.get("actions_json") or "[]")
        except Exception:
            actions = []
        extra = "" if compact else (f"<div style='margin-top:6px;font-size:.8rem;color:#475569'>📍 {html.escape(str(a['top_countries']))} &nbsp; 📺 {html.escape(str(a['top_devices']))}</div>"
                                    f"<div style='margin-top:4px;font-size:.8rem;color:#a16207'>🔧 Suspected change: {html.escape(str(a.get('suspected_change') or 'none found'))}</div>"
                                    f"<div style='margin-top:4px;font-size:.8rem'>✅ {' · '.join(html.escape(x) for x in actions)}</div>")
        st.markdown(f"""<div class='card {'hot' if (active and not inc_open and not in_app) else ''}'>{badge}<span class='badge' style='background:#64748b'>{html.escape(a['issue_domain'])}</span>
          <div style='font-weight:800;font-size:1rem;margin-top:6px;color:#1f2a2e'>{html.escape(str(a['ai_headline']))}</div>
          <div style='color:#8a8a8a;font-size:.78rem'>Signature <b style='color:#1f2a2e'>{html.escape(a['issue_signature'])}</b> · detected {a['detected_at']:%H:%M} UTC ·
             {int(a['total_tickets']):,} tickets · peak {int(a['peak_tickets_15m'])}/15m vs {float(a['baseline_15m']):.1f} baseline</div>
          <div style='margin-top:6px;font-size:.88rem;color:#2b3437'>{html.escape(str(a['ai_summary']))}</div>{extra}</div>""", unsafe_allow_html=True)
        if not compact:
            c1, c2 = st.columns([3, 1.3])
            sp = D["sig"]; sp = sp[(sp.issue_signature == a["issue_signature"]) & (sp.ts_15m <= clock)].sort_values("ts_15m")
            if len(sp):
                f = go.Figure(go.Bar(x=sp.ts_15m, y=sp.tickets_15m, marker_color=["#e11d74" if v else "#b8b4aa" for v in sp.is_anomalous]))
                f.update_layout(xaxis=dict(visible=False, range=[DAY, DAY + timedelta(days=1)]), yaxis=dict(visible=False))
                c1.plotly_chart(style_fig(f, 80, False), use_container_width=True, config={"displayModeBar": False}, key=f"sp_{ss.page}_{a['anomaly_id']}")
            if active and not inc_open and not in_app:
                if c2.button("📣 Declare incident + publish banner", key=f"dec_{ss.page}_{a['anomaly_id']}", use_container_width=True):
                    ops().declare(a, user_email() or "demo-user", clock); st.toast("Incident declared · known-issue banner published"); st.rerun()
            with c2.expander("Agent macro"):
                st.write(a["ai_customer_macro"])


def today_tix(D, clock):
    t = D["tix"]; return t[t.ts <= clock]


# ----------------------------------------------------------------------------- pages
def page_home(D, clock):
    persona_cards()
    tx = today_tix(D, clock); sc = D["score"]
    sc_day = sc[(sc.declared_at >= DAY) & (sc.declared_at <= clock)]
    h = effective_health(D, clock); heat = at(D["heat"], clock)
    cd_cells = int((h.health_state == "Customer-Detected").sum())
    rar = D["pay"].loc[D["pay"].ts <= clock, "revenue_at_risk_usd"].sum()
    csat = tx.csat_sum.sum() / max(tx.csat_n.sum(), 1); sla = tx.sla_met.sum() / max(tx.n.sum(), 1)
    lead = sc_day.detection_lead_minutes.mean() if len(sc_day) else float("nan")
    hc = D["health"]; hc = hc[hc.ts <= clock]
    cdm = hc[hc.health_state == "Customer-Detected"].groupby("region_group").size().reindex(REGIONS, fill_value=0) * 5
    gl = at(D["frus"][D["frus"].scope_type == "Global"], clock)
    fi = float(gl.frustration_index.iloc[0]) if len(gl) else 0
    an = D["anom"]; live = an[(an.detected_at <= clock) & (an.last_seen >= clock)]
    banners = ops().frame("banners")
    red, green, amber, white = "#ff5470", "#34d399", "#fbbf24", "#ffffff"
    if ss.persona == "exec":
        hero("Customer & Platform Health", "Where subscriber impact, detection speed and revenue at risk stand right now.", [
            ("Tickets today", f"{int(tx.n.sum()):,}", f"as of {clock:%H:%M} UTC", white),
            ("CSAT today", f"{csat:.2f}", "Target ≥ 4.0", green if csat >= 4 else amber),
            ("Customers beat monitoring", f"{lead:.0f} min" if lead == lead else "—", "avg detection lead today", red if lead == lead and lead > 15 else white),
            ("Revenue at risk", f"${rar:,.0f}", "failed payments, degraded gateways", red if rar > 500 else green)])
        c1, c2 = st.columns(2)
        with c1:
            barlist("Minutes in Customer-Detected state", "Today by region — time customers felt an issue before we declared it",
                    [(r, int(v)) for r, v in cdm.sort_values(ascending=False).items()], color="#e11d74", fmt=lambda v: f"{v:,.0f} min")
        with c2:
            rows = [(f"{r.incident_id} · {r.domain}", float(r.detection_lead_minutes)) for r in sc_day.sort_values("detection_lead_minutes", ascending=False).itertuples()]
            barlist("Detection lead time by incident", "Minutes support signal preceded internal monitoring (today, declared so far)",
                    rows or [("No incidents declared yet", 0)], color="#1c3a3f", fmt=lambda v: f"{v:.0f} min")
        kp = D["kpi"].copy()
        st.markdown("<div class='panel'><h3>15-day trend</h3><div class='sub'>Tickets and CSAT by region</div></div>", unsafe_allow_html=True)
        a, b = st.columns(2)
        a.plotly_chart(style_fig(px.bar(kp, x="kpi_date", y="tickets", color="region_group", color_discrete_sequence=["#1c3a3f", "#f47521", "#1e8a52", "#94a3b8"]), 280), use_container_width=True, key="exec_t")
        b.plotly_chart(style_fig(px.line(kp, x="kpi_date", y="avg_csat", color="region_group", markers=True, color_discrete_sequence=["#1c3a3f", "#f47521", "#1e8a52", "#94a3b8"]), 280), use_container_width=True, key="exec_c")
    elif ss.persona == "support":
        hero("Queues & Customer Impact", "Surges, SLA and CSAT across the global support operation.", [
            ("Tickets · last 15 min", f"{int(heat.tickets_15m.sum()):,}", f"expected {heat.expected_tickets_15m.sum():,.0f}", red if heat.tickets_15m.sum() > 1.5 * heat.expected_tickets_15m.sum() else white),
            ("SLA met today", f"{sla*100:.1f}%", "Target ≥ 80%", green if sla >= 0.8 else amber),
            ("Active ticket surges", f"{len(live)}", "AI-detected signatures", red if len(live) else green),
            ("Known-issue banners live", f"{len(banners)}", "published to agents", white)])
        c1, c2 = st.columns([1.2, 1])
        with c1:
            st.markdown("<div class='panel'><h3>Live ticket surges</h3><div class='sub'>AI-summarised · publish a known-issue banner to deflect repeat contacts</div></div>", unsafe_allow_html=True)
            anomaly_cards(D, clock, compact=True, limit=4)
            if st.button("Open Subscriber Experience →", key="go_sub"):
                go_page("subscriber"); st.rerun()
        with c2:
            by_dom = tx.groupby("issue_domain").n.sum().sort_values(ascending=False)
            barlist("Tickets today by domain", "Up to the demo clock", [(k, int(v)) for k, v in by_dom.items()], color="#f47521")
            by_rg = tx.groupby("region_group").agg(n=("n", "sum"), s=("sla_met", "sum"))
            barlist("SLA met % by region", "First-response SLA by subscriber tier", [(k, 100 * r.s / max(r.n, 1)) for k, r in by_rg.iterrows()],
                    color="#1e8a52", fmt=lambda v: f"{v:.1f}%")
    else:
        tel = int((h.health_state == "Telemetry Alert").sum()); known = int(h.health_state.str.startswith("Known").sum())
        hero("Reliability & Incidents", "Service health by domain and region, with support as a second monitoring system.", [
            ("Customer-detected cells", f"{cd_cells}", "no internal alert yet", red if cd_cells else green),
            ("Telemetry alerts", f"{tel}", "error rate ≥ 3x baseline", amber if tel else green),
            ("Known incidents", f"{known}", "cells with open incident", amber if known else white),
            ("Global frustration", f"{fi:.0f}", "index 0–100", red if fi > 40 else white)])
        st.markdown("<div class='panel'><h3>Service health matrix</h3><div class='sub'>Engineering domain × region at the demo clock</div></div>", unsafe_allow_html=True)
        health_matrix(D, clock)
        st.markdown("<div class='panel'><h3>Concurrent streams vs baseline</h3><div class='sub'>Support contact rate as a leading indicator</div></div>", unsafe_allow_html=True)
        telemetry_chart(D, clock, default="BR" if clock >= DAY.replace(hour=15, minute=30) and clock <= DAY.replace(hour=17, minute=45) else "Global", key="home_t", h=280)
        ch = D["changes"]; ch = ch[ch.change_ts <= clock].sort_values("change_ts", ascending=False)
        st.markdown("<div class='panel'><h3>Recent risky changes</h3><div class='sub'>Config changes, content publishes and rollbacks today</div></div>", unsafe_allow_html=True)
        st.dataframe(ch[["change_ts", "service", "domain", "change_type", "version", "description", "outcome"]], hide_index=True, use_container_width=True)


def page_alerts(D, clock):
    items = alerts_now(D, clock)
    st.markdown(f"## Alerts")
    st.caption(f"Active at {clock:%H:%M} UTC · {len(items)} open")
    if not items:
        st.success("All clear — no active alerts at this time.")
    col = {"critical": "#e11d74", "high": "#ea580c"}
    st.markdown("<div class='panel'>" + "".join(f"<div class='alertrow'><div class='dot' style='background:{col[i['sev']]}'></div><div><div style='font-weight:700;color:#1f2a2e'>{html.escape(i['title'])}</div>"
                f"<div style='font-size:13px;color:#64748b'>{html.escape(i['detail'])}</div></div></div>" for i in items) + "</div>", unsafe_allow_html=True)


def page_pulse(D, clock):
    st.markdown("## Platform Pulse"); st.caption("Global heatmap · service health matrix · live telemetry vs consumer frustration")
    a, b = st.columns([1.3, 1])
    with a:
        st.markdown("<div class='panel'><h3>Global pulse</h3><div class='sub'>Bubble = tickets in last 15 min · colour = selected signal · pink halo = customer-detected</div></div>", unsafe_allow_html=True)
        geo_map(D, clock, key="pulse_map")
    with b:
        st.markdown("<div class='panel'><h3>Service health matrix</h3><div class='sub'>Internal telemetry + support signal + known incidents</div></div>", unsafe_allow_html=True)
        health_matrix(D, clock)
    st.markdown("<div class='panel'><h3>Concurrent streams vs baseline · consumer frustration</h3><div class='sub'>When streams dip while contacts spike, users are hitting breakage before alerts fire</div></div>", unsafe_allow_html=True)
    telemetry_chart(D, clock, key="pulse_t")


def page_subscriber(D, clock):
    st.markdown("## Subscriber Experience & Support Command"); st.caption("AI ticket anomaly detection · declare incidents and publish known-issue banners (Lakebase)")
    banners = ops().frame("banners")
    if len(banners):
        st.warning("📣 Known-issue banners live for agents: " + " | ".join(banners.headline.astype(str).head(3)))
    anomaly_cards(D, clock)


def page_payments(D, clock):
    st.markdown("## Payments & Social Listening"); st.caption("Gateway success vs billing tickets · Reddit and X sentiment")
    a, b = st.columns([1.1, 1])
    with a:
        pay = D["pay"]; cs = sorted(pay.country_code.unique())
        ctry = st.selectbox("Country", cs, index=cs.index("DE"))
        p = pay[(pay.country_code == ctry) & (pay.ts <= clock)].sort_values("ts")
        latest = at(pay[pay.country_code == ctry], clock)
        colmap = {"Healthy": "#1e8a52", "Degraded": "#ea580c", "Critical": "#e11d74"}
        st.markdown("".join(f"<span class='badge' style='background:{colmap[r.gateway_state]}'>{r.gateway}: {r.success_rate_15m*100:.1f}%</span>" for r in latest.itertuples()), unsafe_allow_html=True)
        f = go.Figure()
        for gw_, gdf in p.groupby("gateway"):
            f.add_trace(go.Scatter(x=gdf.ts, y=gdf.success_rate_15m * 100, name=gw_, mode="lines"))
        bt = p.groupby("ts").country_billing_tickets.max().reset_index()
        f.add_trace(go.Bar(x=bt.ts, y=bt.country_billing_tickets, name="Billing tickets", yaxis="y2", marker_color="#f4752166"))
        f.update_layout(xaxis=dict(range=[DAY, DAY + timedelta(days=1)]), yaxis=dict(title="Success % (15m)", range=[60, 101]),
                        yaxis2=dict(title="Tickets", overlaying="y", side="right", showgrid=False))
        st.plotly_chart(style_fig(f, 340), use_container_width=True, config={"displayModeBar": False}, key="payc")
        st.metric(f"Revenue at risk today · {ctry}", f"${p.revenue_at_risk_usd.sum():,.0f}")
    with b:
        so = D["social"]; so = so[so.posted_at <= clock]
        neg = so[(so.sentiment == "negative") & (so.topic_domain != "Show hype / fandom")]
        if len(neg):
            nv = neg.groupby(["ts_15m", "topic_domain"]).size().reset_index(name="posts")
            f3 = px.bar(nv, x="ts_15m", y="posts", color="topic_domain")
            f3.update_layout(xaxis=dict(title=None, range=[DAY, DAY + timedelta(days=1)]), yaxis=dict(title="Negative posts"))
            st.plotly_chart(style_fig(f3, 200), use_container_width=True, config={"displayModeBar": False}, key="socc")
        colors = {"negative": "#e11d74", "positive": "#1e8a52", "neutral": "#94a3b8", "mixed": "#d4a017"}
        feed = so.sort_values("posted_at", ascending=False).head(14)
        st.markdown("".join(f"<div class='post' style='border-color:{colors.get(r.sentiment,'#ccc')}'><span style='color:#8a8a8a;font-size:.72rem'>{r.posted_at:%H:%M} · {r.platform} · {r.author_handle} · {r.country_code if isinstance(r.country_code, str) else ''} · {r.sentiment}</span><br>{html.escape(str(r.post_text))}</div>" for r in feed.itertuples()), unsafe_allow_html=True)


def page_executive(D, clock):
    host = Config().host.rstrip("/")
    st.markdown("## Executive Overview"); st.caption("Incident scorecard across 15 days · published AI/BI dashboard")
    sc = D["score"].sort_values("declared_at", ascending=False); cf = sc[sc.detection_winner == "Customers first"]
    st.markdown("<div class='hero'><div class='eyebrow'>15-day scorecard</div><div class='kgrid' style='margin-top:6px'>" + "".join(
        f"<div><div class='kl'>{k}</div><div class='kv'>{v}</div></div>" for k, v in [
            ("Incidents", len(sc)), ("Customers detected first", f"{len(cf)}/{len(sc)}"),
            ("Avg minutes before monitoring", f"{cf.detection_lead_minutes.astype(float).mean():.0f}"), ("Est. lost viewing hours", f"{sc.est_lost_viewing_hours.astype(float).sum():,.0f}")]) + "</div></div>", unsafe_allow_html=True)
    st.dataframe(sc[["incident_id", "title", "region_groups", "customer_detected_at", "first_alert_at", "declared_at", "detection_lead_minutes",
                     "minutes_to_resolve", "tickets_during_incident", "est_lost_viewing_hours", "revenue_at_risk_usd", "root_cause"]], use_container_width=True, hide_index=True)
    st.markdown(f"**AI/BI dashboard** · [open in a new tab]({host}/dashboardsv3/{DASH}/published)")
    st.caption("If the frame is blank, a workspace admin must allow dashboard embedding for the app domain (Settings → Security).")
    components.iframe(f"{host}/embed/dashboardsv3/{DASH}" + (f"?o={WS_ID}" if WS_ID else ""), height=900, scrolling=True)


def page_genie(D, clock):
    st.markdown("## Genie"); st.caption("Ask questions in plain English over the Command Center gold tables · powered by AI/BI Genie")
    c = st.columns(3)
    for i, s_ in enumerate(GENIE_SUGGEST[ss.persona]):
        if c[i].button(s_, key=f"gs_{i}", use_container_width=True):
            ss.genie_pending = s_
    prompt = st.chat_input("Ask Genie about tickets, incidents, payments, social sentiment…")
    if prompt:
        ss.genie_pending = prompt
    if ss.get("genie_pending"):
        qn = ss.pop("genie_pending"); ss.playing = False
        with st.spinner("Genie is thinking…"):
            try:
                ss.setdefault("genie_hist", []).append(genie_ask(qn))
            except Exception as e:
                ss.setdefault("genie_hist", []).append(dict(q=qn, texts=[f"Genie error: {e}"], tables=[], sqls=[]))
    if st.button("New conversation", key="gnew"):
        ss.pop("genie_conv", None); ss.genie_hist = []
    for hh in ss.get("genie_hist", []):
        with st.chat_message("user"):
            st.write(hh["q"])
        with st.chat_message("assistant", avatar=":material/auto_awesome:"):
            for t in hh["texts"]:
                st.markdown(t)
            for tb in hh["tables"]:
                st.dataframe(tb, use_container_width=True, hide_index=True)
            if hh["sqls"]:
                with st.expander("Generated SQL"):
                    st.code(hh["sqls"][0], language="sql")


def page_brief(D, clock):
    st.markdown("## Incident Brief"); st.caption("AI-generated brief for one incident (ai_query on the ticket cluster) · ready to paste into an incident channel or exec update")
    an = D["anom_all"].sort_values("detected_at", ascending=False).reset_index(drop=True)
    sc = D["score"].set_index("incident_id")
    if an.empty:
        st.info("No incidents found."); return

    lab = {r.anomaly_id: f"{r.detected_at:%a %d %b %H:%M} · {r.issue_signature} — {r.incident_id or 'undeclared'}" for r in an.itertuples()}
    id_of = {v: k for k, v in lab.items()}
    options = [NO_INC] + list(lab.values())
    # keep the picker in sync with the scenario chosen in the top bar
    if ss.pop("brief_sync", False) or "brief_pick" not in ss:
        ss.brief_pick = lab.get(ss.get("brief_id"), NO_INC)
    if ss.brief_pick not in options:
        ss.brief_pick = NO_INC

    def on_pick():
        ss.brief_id = id_of.get(ss.brief_pick)
    c1, c2 = st.columns([4, 1])
    c1.selectbox("Incident", options, key="brief_pick", on_change=on_pick)
    c2.metric("Incidents (15 days)", len(an))
    sel = id_of.get(ss.brief_pick)
    if sel is None:
        st.info("No incident selected. Pick one above, or choose a scenario in **Jump to scenario** — the brief follows the scenario.")
        ov = an[["detected_at", "issue_signature", "incident_id", "ai_headline", "top_countries", "total_tickets", "minutes_detected_before_incident"]].copy()
        ov.columns = ["Detected (UTC)", "Signature", "Incident", "Headline", "Top countries", "Tickets", "Minutes before declared"]
        st.dataframe(ov, hide_index=True, use_container_width=True)
        return
    rows = [an[an.anomaly_id == sel].iloc[0].to_dict()]
    for a in rows:
        if a["detected_at"] > clock:
            st.info(f"This incident happens later in the demo (detected {a['detected_at']:%d %b %H:%M} UTC) — showing the brief ahead of the replay clock.")
        try:
            actions = json.loads(a.get("actions_json") or "[]")
        except Exception:
            actions = []
        inc = a.get("incident_id"); s_ = sc.loc[inc] if isinstance(inc, str) and inc in sc.index else None
        impact = (f"<span class='pill'>{int(s_['tickets_during_incident']):,} tickets</span><span class='pill'>{float(s_['est_lost_viewing_hours']):,.0f} lost viewing hours</span>"
                  f"<span class='pill'>${float(s_['revenue_at_risk_usd']):,.0f} revenue at risk</span><span class='pill'>customers first by {float(s_['detection_lead_minutes']):.0f} min</span>") if s_ is not None else ""
        st.markdown(f"""<div class='panel'><h3>{html.escape(str(a['ai_headline']))}</h3>
          <div class='sub'>{html.escape(a['issue_signature'])} · detected {a['detected_at']:%H:%M} UTC · {html.escape(str(inc) if isinstance(inc, str) else 'not yet declared')}</div>
          <p style='color:#2b3437'>{html.escape(str(a['ai_summary']))}</p>
          <p style='color:#2b3437'><b>Likely root cause:</b> {html.escape(str(a['ai_likely_root_cause']))}</p>
          <p style='color:#2b3437'><b>Recommended actions:</b> {' · '.join(html.escape(x) for x in actions)}</p>
          <p style='color:#2b3437'><b>Customer macro:</b> <i>{html.escape(str(a['ai_customer_macro']))}</i></p>
          <p style='color:#2b3437'><b>Where:</b> {html.escape(str(a['top_countries']))} &nbsp;·&nbsp; <b>Devices:</b> {html.escape(str(a['top_devices']))}
             &nbsp;·&nbsp; <b>Suspected change:</b> {html.escape(str(a.get('suspected_change') or 'none found'))}</p>{impact}</div>""", unsafe_allow_html=True)
        try:
            samples = json.loads(a.get("samples_json") or "[]")
        except Exception:
            samples = []
        if samples:
            with st.expander(f"Sample customer tickets ({len(samples)})"):
                for s_t in samples:
                    st.markdown(f"- {s_t}")
        sig = q(f"SELECT ts_15m, tickets_15m, baseline_15m, is_anomalous FROM {S}.gold_signature_trends_15m WHERE issue_signature = '{a['issue_signature'].replace(chr(39), chr(39)*2)}' AND ts_15m BETWEEN '{a['first_seen'] - timedelta(hours=3)}' AND '{a['last_seen'] + timedelta(hours=2)}' ORDER BY ts_15m")
        if len(sig):
            f = go.Figure(go.Bar(x=sig.ts_15m, y=sig.tickets_15m, name="Tickets / 15 min", marker_color=["#e11d74" if v else "#b8b4aa" for v in sig.is_anomalous]))
            f.add_trace(go.Scatter(x=sig.ts_15m, y=sig.baseline_15m, name="Baseline", line=dict(color="#1c3a3f", dash="dot")))
            if isinstance(a.get("incident_declared_at"), pd.Timestamp):
                f.add_vline(x=a["incident_declared_at"], line=dict(color="#d97706", width=2))
                f.add_annotation(x=a["incident_declared_at"], y=1.05, yref="paper", text="Incident declared", showarrow=False, font=dict(color="#d97706", size=11))
            f.add_vline(x=a["detected_at"], line=dict(color="#e11d74", width=2, dash="dash"))
            f.add_annotation(x=a["detected_at"], y=1.12, yref="paper", text="Surge detected", showarrow=False, font=dict(color="#e11d74", size=11))
            st.markdown("<div class='panel'><h3>Ticket surge timeline</h3><div class='sub'>15-minute ticket volume for this signature vs baseline</div></div>", unsafe_allow_html=True)
            st.plotly_chart(style_fig(f, 260), use_container_width=True, config={"displayModeBar": False}, key="brief_sig")


def page_console(D, clock):
    o = ops()
    st.markdown("## Ops Console"); st.caption("Operational state written by this app to Lakebase (Postgres) · schema command_center_ops")
    st.markdown(f"Status: {'🟢 connected to Lakebase `support-command-center / production`' if o.ok else '🟡 session-only (Lakebase unavailable)'}")
    if o.err and not o.ok:
        st.caption(o.err)
    for k, t in [("actions", "Incident actions"), ("banners", "Known-issue banners (live)"), ("acks", "Alert acknowledgements")]:
        st.markdown(f"#### {t}"); st.dataframe(o.frame(k), use_container_width=True, hide_index=True)


def page_about(D, clock):
    st.markdown("## About the Support Command Center")
    st.markdown("""<div class='hero'><div class='eyebrow'>Why this exists</div><h1 style='font-size:26px'>Customers are the fastest monitoring system you have.</h1>
    <div class='lead'>When a server quietly drops sessions or a subtitle pack fails to publish, subscribers notice minutes — sometimes hours — before internal alerts fire.
    The Support Command Center fuses support tickets, Salesforce customer context, product telemetry, payments and social chatter into one control tower,
    so Fanimation can see breakage from the fan's point of view and act before it becomes churn.</div></div>""", unsafe_allow_html=True)
    c = st.columns(3)
    blocks = [("Business outcomes", ["Cut time-to-detect by using support as a second monitoring system", "Protect CSAT & SLA during incidents with known-issue banners and AI macros",
                                     "Quantify revenue at risk from payment-gateway failures", "Reduce repeat contacts and churn after bad experiences"]),
              ("Who uses it", [f"<b>{p['label']}</b> — {p['about']}" for p in PERSONAS.values()]),
              ("Key metrics", ["Customer-Detected minutes (issue felt, not yet declared)", "Detection lead time vs monitoring", "Frustration index (contacts, social, stream drop)",
                               "Tickets vs release-aware expected volume", "Gateway success rate & revenue at risk", "SLA met %, CSAT, reopen rate"])]
    for i, (t, items) in enumerate(blocks):
        c[i].markdown(f"<div class='panel'><h3>{t}</h3><ul>" + "".join(f"<li style='margin:6px 0;color:#2b3437'>{x}</li>" for x in items) + "</ul></div>", unsafe_allow_html=True)
    st.markdown("<div class='panel'><h3>Capabilities</h3><div class='sub'>What the app does</div>" + "".join(f"<span class='pill'>{x}</span>" for x in [
        "Global heatmap of regional spikes", "Service health matrix: known vs customer-detected", "Streams vs baseline with simulcast awareness",
        "AI ticket-surge detection (error codes, episodes, languages)", "AI incident summaries, root cause and customer macros", "Payment gateway health",
        "Social listening (Reddit, X) with AI sentiment", "Deploy / config-change correlation", "Declare incidents & publish banners (Lakebase)",
        "Genie natural-language Q&A", "Executive AI/BI dashboard", "Persona-based views"]) + "</div>", unsafe_allow_html=True)
    st.markdown("""<div class='panel'><h3>The demo day — Tue 27 Oct 2026 (UTC)</h3><div class='sub'>Scripted scenarios replayed by the demo clock</div>
    <ul style='color:#2b3437'>
    <li><b>09:00</b> APAC login failures (AUTH-503) after an auth token-cache change — monitoring catches it in 4 minutes.</li>
    <li><b>14:00</b> <i>Blade of the Crimson Moon</i> season finale simulcast — streams spike +45%, recognised as benign.</li>
    <li><b>15:30</b> Android TV player SDK v2.3.0 ships; from 15:36 Brazil / Mexico / Argentina hit <b>Error 1002</b>. Sessions drop quietly; tickets and Reddit light up.
        The matrix shows <b>Customer-Detected for ~45 minutes</b> before the incident is declared at 16:25.</li>
    <li><b>18:00</b> <i>Spirit Ronin</i> E3 ships without Spanish subtitles — only support sees it; declared via escalation at 19:40.</li>
    <li><b>20:00</b> Adyen EU endpoint migration — German and Austrian payments fail; revenue at risk climbs until rollback.</li></ul>
    <div class='sub'>All data is synthetic and generated for this demo.</div></div>""", unsafe_allow_html=True)


def page_architecture(D, clock):
    st.markdown("## Architecture"); st.caption("Built end-to-end on the Databricks Data Intelligence Platform")
    st.markdown("<div class='arch'>" + "".join(f"<div class='archcol'><h4>{t}</h4><ul>" + "".join(f"<li>{x}</li>" for x in items) + "</ul></div>" for t, items in [
        ("1 · Sources", ["Support cases (Zendesk / ServiceNow)", "Salesforce Customer 360", "Product telemetry: streams, logins, playback", "Payment gateways", "Social: Reddit, X", "Deploys & change events", "Simulcast release calendar"]),
        ("2 · Ingest & transform", ["Lakeflow Connect (Salesforce, ServiceNow)", "Auto Loader / Zerobus for streaming telemetry", "Lakeflow Spark Declarative Pipelines: bronze → silver → gold", "Expectations for data quality"]),
        ("3 · Intelligence", ["AI Functions: ai_classify, ai_analyze_sentiment, ai_query", "Release-aware baseline forecasting", "Signature surge detection (z-score / ratio)", "Health-state model: known vs customer-detected", "MLflow for model tracking"]),
        ("4 · Govern & serve", ["Unity Catalog: lineage, tags, access", "Lakebase Postgres: synced gold tables (reverse ETL)", "Lakebase OLTP: incident actions, banners, acks", "Databricks SQL warehouse (serverless)"]),
        ("5 · Experience", ["Databricks Apps (this app)", "AI/BI Dashboards (executive)", "AI/BI Genie (natural-language Q&A)", "Agent-ready: incident briefs & macros"])]) + "</div>", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<div class='panel'><h3>Data flow</h3><div class='sub'>From raw signals to action</div></div>", unsafe_allow_html=True)
    st.graphviz_chart("""
    digraph G { rankdir=LR; bgcolor="transparent"; node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11, color="#d6d2c8", fillcolor="white"];
      edge [color="#8a8a8a"];
      subgraph cluster_s { label="Sources"; style="rounded"; color="#d6d2c8"; tickets [label="Support tickets"]; sfdc [label="Salesforce C360"]; tele [label="Streams & service telemetry"]; pay [label="Payment gateways"]; social [label="Reddit / X"]; chg [label="Deploys & releases"]; }
      subgraph cluster_p { label="Lakeflow Spark Declarative Pipelines"; style="rounded"; color="#f47521"; bronze [label="Bronze\\nraw events", fillcolor="#f3e3d3"]; silver [label="Silver\\nsilver_tickets (domain, signature)\\nsilver_country_streams_5m (baseline)", fillcolor="#e8e8e8"]; gold [label="Gold\\nservice_health · heatmap · frustration\\npayment_health · incident_scorecard · exec_kpis", fillcolor="#fbefc7"]; }
      subgraph cluster_ai { label="AI & ML"; style="rounded"; color="#7c3aed"; aif [label="AI Functions\\nai_classify · ai_analyze_sentiment"]; anom [label="Surge detection\\nsignature z-score"]; llm [label="ai_query (Claude)\\nheadline · root cause · macro"]; }
      uc [label="Unity Catalog\\ngovernance & lineage", fillcolor="#dbeafe"];
      subgraph cluster_lb { label="Lakebase (Postgres)"; style="rounded"; color="#1e8a52"; synced [label="Synced tables\\nlb_service_health · lb_ticket_anomalies …"]; opsdb [label="command_center_ops\\nincident_actions · banners · acks"]; }
      subgraph cluster_x { label="Experience"; style="rounded"; color="#1c3a3f"; app [label="Databricks App\\nSupport Command Center", fillcolor="#1c3a3f", fontcolor="white"]; dash [label="AI/BI Dashboard"]; genie [label="AI/BI Genie"]; }
      {tickets sfdc tele pay social chg} -> bronze -> silver -> gold; social -> aif; silver -> anom -> llm -> gold; aif -> gold;
      gold -> uc; uc -> synced; uc -> dash; uc -> genie; synced -> app; app -> opsdb [label=" declare / ack", fontsize=9]; opsdb -> app; genie -> app; dash -> app;
    }""", use_container_width=True)
    c = st.columns(2)
    c[0].markdown("<div class='panel'><h3>Lakehouse layers</h3><div class='sub'>Unity Catalog · classic_stable_l0m1dn_catalog.support_command_center</div>" + "".join(f"<p style='margin:6px 0;color:#2b3437'><b>{k}</b> — {v}</p>" for k, v in [
        ("Raw", "support_tickets, concurrent_streams, service_metrics, payment_transactions, social_posts, change_events, incidents, release_schedule, regions"),
        ("Silver", "silver_tickets (issue domain + signature from multilingual text), silver_country_streams_5m (release-aware baseline), silver_anomaly_events"),
        ("Gold", "gold_service_health_region/country_5m, gold_region_heatmap_5m, gold_frustration_index_5m, gold_ticket_anomalies (AI), gold_payment_health_5m, gold_social_posts (AI), gold_incident_scorecard, gold_exec_daily_kpis")]) + "</div>", unsafe_allow_html=True)
    c[1].markdown("<div class='panel'><h3>Why Databricks</h3><div class='sub'>One platform, no copies</div><ul style='color:#2b3437'>"
                  "<li>Support, CRM, telemetry and payments joined in one governed lakehouse</li><li>AI Functions run LLMs in SQL — no separate ML infra for classification and summaries</li>"
                  "<li>Lakebase gives the app millisecond reads and transactional writes next to the lakehouse</li><li>Genie and AI/BI let execs self-serve without new dashboards</li>"
                  "<li>Databricks Apps hosts the UI with built-in SSO and service-principal security</li></ul>"
                  "<div class='sub'>Demo implementation: pipelines are authored as notebooks 01–03 in the workspace folder and map 1:1 to SDP datasets for production.</div></div>", unsafe_allow_html=True)


PAGES = {"home": page_home, "alerts": page_alerts, "pulse": page_pulse, "subscriber": page_subscriber, "payments": page_payments,
         "executive": page_executive, "genie": page_genie, "brief": page_brief, "console": page_console, "about": page_about, "architecture": page_architecture}

# ----------------------------------------------------------------------------- main
try:
    D = load_day()
except Exception as e:
    st.error(f"Could not load data from the SQL warehouse: {e}"); st.stop()

sidebar(D)
topbar()
try:
    PAGES.get(ss.page, page_home)(D, ss.clock)
except Exception as e:
    st.error(f"{ss.page} failed: {type(e).__name__}: {e}"); st.exception(e)

if ss.get("playing"):
    time.sleep(ss.get("speed", 3.0))
    if ss.clock < END:
        ss.clock += timedelta(minutes=5)
    else:
        ss.playing = False
    st.rerun()
