"""Fanimation Support Command Center - Databricks App (Streamlit) - V2.

Live mode: all analytics are served from Lakebase Postgres synced tables (schema support_command_center_sdp), which are
kept fresh by the streaming Lakeflow pipeline + snapshot syncs (run notebook 04_live_event_generator to add data).
Incident actions / banners / acks are written back to Lakebase (schema command_center_ops). Genie for NL Q&A.
"""
import os, time, json, html, threading
from datetime import datetime, timedelta
from decimal import Decimal

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
UC = os.getenv("UC_SCHEMA_SDP", "classic_stable_l0m1dn_catalog.support_command_center_sdp")
PG = os.getenv("PG_SCHEMA", "support_command_center_sdp")
LB_EP = os.getenv("LAKEBASE_ENDPOINT") or os.getenv("LAKEBASE_ENDPOINT_PATH", "projects/support-command-center/branches/production/endpoints/primary")
LB_DB = os.getenv("PGDATABASE") or os.getenv("LAKEBASE_DATABASE", "databricks_postgres")
WS_ID = os.getenv("WORKSPACE_ID", "")
DOMAINS = ["Authentication", "Billing & Payments", "Playback & CDN", "Content Ingestion", "Subtitles & Localization", "Device Apps", "Search & Discovery"]
REGIONS = ["NA", "LATAM", "EMEA", "APAC"]
STATE_COLORS = {"Healthy": "#1e8a52", "Watch": "#8a9a2b", "Recovering": "#2563eb", "Telemetry Alert": "#ea580c",
                "Known Incident": "#d97706", "Known (declared in app)": "#d97706", "Customer-Detected": "#e11d74"}
NO_INC = "— No incident selected —"
UC_NAME = {"change_events": "bronze_change_events", "incidents": "bronze_incidents"}   # lb_<x> -> UC table (fallback path)

PAGE_META = {  # key: (label, icon)
    "home": ("Home", ":material/home:"), "alerts": ("Alerts", ":material/notifications:"),
    "executive": ("Executive Overview", ":material/bar_chart:"), "churn": ("Churn & Retention", ":material/person_remove:"),
    "subscriber": ("Subscriber Experience", ":material/support_agent:"), "customer360": ("Customer 360", ":material/account_circle:"),
    "payments": ("Payments & Social", ":material/payments:"), "pulse": ("Platform Pulse", ":material/radar:"),
    "brief": ("Incident Brief", ":material/description:"), "genie": ("Genie", ":material/auto_awesome:"),
    "console": ("Ops Console (Lakebase)", ":material/database:"),
}
PERSONAS = {
    "exec": dict(label="C-Suite", tab="Executive", sub="Customer & Platform Health", tag="Executive Strategy",
                 about="Chief Customer / Content / Technology officers. One view of subscriber impact, revenue and MRR at risk, and whether customers notice problems before our own monitoring.",
                 blurb="Today's subscriber experience at a glance. Incident performance, customer experience, revenue impact and long-term trends live in Executive Overview.",
                 dashboards=["executive"], ai=["brief", "genie"]),
    "support": dict(label="Support Operations", tab="Support Ops", sub="Queues & Customer Impact", tag="Support VP/Manager",
                    about="Runs the global support org. Spots ticket surges early, publishes known-issue banners and macros to agents, protects SLA/CSAT and works the at-risk customer list.",
                    blurb="Live view of ticket surges, SLA and CSAT, AI-written summaries and customer macros, and the customers most likely to churn - grouped by what went wrong.",
                    dashboards=["subscriber", "customer360", "churn"], ai=["brief", "genie", "console"]),
    "eng": dict(label="Engineering / SRE", tab="Engineering", sub="Reliability & Incidents", tag="Platform Reliability / Incident Command",
                about="Owns authentication, playback/CDN, payments, content ingestion and localization. Uses support signal as a second monitoring system and correlates it with deploys.",
                blurb="Service health by domain and region, telemetry versus customer contact rate, and the deploys and config changes that preceded each surge.",
                dashboards=["pulse", "payments"], ai=["brief", "genie"]),
}


ROLE_ABOUT = {
    "exec": [("Role", "Chief Customer, Content and Technology officers accountable for subscriber experience, revenue and platform reliability."),
             ("Day-to-Day", "Reviews the subscriber impact of incidents, CSAT and SLA, revenue at risk from payment failures and churn-risk cohorts — and asks how quickly issues were detected versus our own monitoring."),
             ("How the Command Center Helps", "Home shows what is happening right now; Executive Overview covers incident performance, customer experience, revenue impact and 36-month trends — with AI-written Incident Briefs and Genie on the same governed data the operators use.")],
    "support": [("Role", "Support VP/Manager running Fanimation's global, multilingual support organization."),
                ("Day-to-Day", "Watches ticket surges by issue signature, protects SLA and CSAT, publishes known-issue banners and agent macros, and works the at-risk customer list."),
                ("How the Command Center Helps", "AI ticket-surge detection with summaries and customer macros, Customer 360 lookups served from Lakebase, churn cohorts grouped by what went wrong, and one-click incident declaration written back to Lakebase.")],
    "eng": [("Role", "Platform reliability and incident command for authentication, playback / CDN, payments, content ingestion and localization."),
            ("Day-to-Day", "Triages the service health matrix, compares internal telemetry with customer contact rate, and correlates surges with recent deploys and config changes."),
            ("How the Command Center Helps", "Support becomes a second monitoring signal: customer-detected cells before alerts fire, stream-vs-baseline divergence, suspected-change correlation and AI root-cause summaries.")],
}
LOGO_SVG = ("<svg width='28' height='28' viewBox='0 0 24 24' fill='none' stroke='#fff' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'>"
            "<circle cx='12' cy='12' r='9'/><path d='M10 8.5v7l5.5-3.5z' fill='#fff'/></svg>")
DB_SVG = ("<svg width='22' height='22' viewBox='0 0 24 24' fill='none' stroke='#ff3621' stroke-width='2.2' stroke-linejoin='round'>"
          "<path d='M12 3 2.5 8.2 12 13.4l9.5-5.2z'/><path d='M2.5 12.2 12 17.4l9.5-5.2'/><path d='M2.5 16.2 12 21.4l9.5-5.2'/></svg>")


def allowed_pages(p):
    return ["home", "alerts"] + PERSONAS[p]["dashboards"] + PERSONAS[p]["ai"] + ["about", "architecture"]


# ----------------------------------------------------------------------------- styling
st.markdown("""
<style>
header[data-testid="stHeader"] {display:none;}
.block-container {padding-top: 1.2rem; max-width: 1400px;}
.stApp {background:#f6f5f1;}
/* ---- side navigation ---- */
[data-testid="stSidebar"] {background:#1b3038; min-width:300px; max-width:300px; border-right:1px solid #24404a; font-family:'Inter', 'Source Sans Pro', sans-serif;}
[data-testid="stSidebar"] [data-testid="stSidebarHeader"] {display:none !important;}
[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {padding:26px 18px 24px 18px !important;}
[data-testid="stSidebar"] p, [data-testid="stSidebar"] div, [data-testid="stSidebar"] label {color:#e6edef; font-family:'Inter', 'Source Sans Pro', sans-serif;}
[data-testid="stSidebar"] [data-testid="stVerticalBlock"] {gap:0.2rem !important;}
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {margin:0 !important;}
[data-testid="stSidebar"] hr {border:none; border-top:1px solid #2c4650 !important; margin:12px -18px !important;}
/* nav buttons: left aligned icon + label */
[data-testid="stSidebar"] .stButton {width:100%;}
[data-testid="stSidebar"] .stButton button {display:flex !important; justify-content:flex-start !important; align-items:center !important; gap:14px !important; text-align:left !important;
   background:transparent !important; border:none !important; box-shadow:none !important; width:100% !important; padding:8px 14px !important; min-height:44px !important; border-radius:10px !important;}
[data-testid="stSidebar"] .stButton button > div, [data-testid="stSidebar"] .stButton button > span {justify-content:flex-start !important; margin:0 !important;}
[data-testid="stSidebar"] .stButton button [data-testid="stMarkdownContainer"] {text-align:left !important; width:auto !important;}
[data-testid="stSidebar"] .stButton button p {font-size:16px !important; font-weight:400 !important; color:#e6edef !important; text-align:left !important; margin:0 !important;}
[data-testid="stSidebar"] .stButton button [data-testid="stIconMaterial"] {font-size:22px !important; color:#b3c4c8 !important; margin:0 !important;}
[data-testid="stSidebar"] .stButton button:hover {background:#24404a !important;}
/* viewing-as select */
[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] {background:#2c454e !important; border:1px solid #36525c !important; border-radius:12px !important;}
[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] * {background:transparent !important; border-color:transparent !important; color:#ffffff !important;}
[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] > div {min-height:50px; padding-left:8px;}
[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] div[value], [data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] input {font-size:16.5px !important; font-weight:500 !important;}
[data-testid="stSidebar"] [data-testid="stSelectbox"] svg {fill:#c9d6d9 !important; color:#c9d6d9 !important;}
/* about-this-role: plain row, icon left, chevron right */
[data-testid="stSidebar"] [data-testid="stExpander"] details {border:none !important; background:transparent !important; border-radius:0 !important;}
[data-testid="stSidebar"] [data-testid="stExpander"] summary {background:transparent !important; padding:6px 2px !important;}
[data-testid="stSidebar"] [data-testid="stExpander"] summary:hover {background:transparent !important;}
[data-testid="stSidebar"] [data-testid="stExpander"] summary p {font-size:15.5px !important; font-weight:500 !important; color:#c9d6d9 !important;}
[data-testid="stSidebar"] [data-testid="stExpander"] summary [data-testid="stIconMaterial"] {color:#c9d6d9 !important;}
[data-testid="stSidebar"] [data-testid="stExpanderDetails"] {padding:4px 2px 4px 2px !important;}
.sb-brand {display:flex; flex-wrap:nowrap; gap:12px; align-items:center; padding:0 0 16px 0; border-bottom:1px solid #2c4650; margin-bottom:14px; width:100%;}
.sb-brand > div:last-child {display:flex; flex-direction:column; justify-content:center; min-width:0; flex:1;}
.sb-logo {width:42px; height:42px; border-radius:10px; background:#f47521; display:flex; align-items:center; justify-content:center; flex-shrink:0;}
.sb-logo svg {width:24px; height:24px;}
.sb-name {font-weight:700; font-size:17px; color:#fff !important; line-height:1.2; white-space:nowrap;}
.sb-sub {font-size:13px; color:#a9bcc1 !important; line-height:1.3; white-space:nowrap; margin-top:2px;}
.sb-powered {display:flex; flex-wrap:nowrap; align-items:center; gap:8px; font-size:10.5px; letter-spacing:1.8px; color:#a9bcc1 !important; font-weight:500; white-space:nowrap; line-height:1;}
.sb-powered .db {display:inline-flex; align-items:center; gap:5px; letter-spacing:0; font-size:16px; font-weight:700; color:#fff !important; white-space:nowrap;}
.sb-powered .db svg {width:18px; height:18px; flex-shrink:0;}
.sb-section {font-size:12px; letter-spacing:2px; color:#93a8ad !important; text-transform:uppercase; font-weight:500; padding:14px 14px 6px 14px;}
.sb-label {font-size:12px; letter-spacing:2px; color:#93a8ad !important; text-transform:uppercase; font-weight:500; padding:4px 0 10px 0;}
.sb-tag {font-size:13.5px; color:#93a8ad !important; padding:8px 0 2px 0;}
.sb-about h5 {font-size:14.5px !important; font-weight:700 !important; color:#e6edef !important; margin:10px 0 4px 0 !important; padding:0 !important;}
.sb-about p {font-size:14px; line-height:1.55; color:#a9bcc1 !important; margin:0 0 6px 0;}
.sb-foot {font-size:11.5px; color:#7f969c !important; padding:0 4px;}
.ph {font-size:34px; font-weight:800; color:#1f2a2e; margin:4px 0 16px 0; letter-spacing:-.3px;}
.why {border-left:3px solid #e5322d; padding:2px 0 2px 16px; margin-bottom:24px;}
.why .wl {color:#c8211b; font-weight:700; font-size:12.5px; letter-spacing:1.8px;}
.why .wt {color:#5b5f60; font-size:16.5px; margin-top:4px;}
.qi {color:#8a7f72; font-size:14px; letter-spacing:1.8px; font-weight:600; margin-bottom:12px;}
.qgrid {display:grid; grid-template-columns:repeat(auto-fit, minmax(300px, 1fr)); gap:18px; margin-bottom:22px;}
.qc {background:#fff; border-radius:16px; border-left:3px solid; padding:18px 20px; display:flex; gap:14px; align-items:flex-start; font-weight:600; font-size:16px; line-height:1.4; color:#1f2a2e; box-shadow:0 1px 4px #0000000d;}
.qc .ic {font-size:18px; line-height:1.4;}
.mgrid {display:grid; grid-template-columns:repeat(auto-fit, minmax(220px, 1fr)); gap:18px; margin:4px 0 18px 0;}
.mc {background:#fff; border-radius:16px; padding:20px 24px; box-shadow:0 1px 4px #0000000d;}
.mc .ml {font-size:13px; letter-spacing:1.6px; color:#7a7268; text-transform:uppercase;}
.mc .mv {font-size:40px; font-weight:800; color:#1b3038; line-height:1.15; margin-top:6px;}
.mc .ms {font-size:14px; color:#9a9187; margin-top:4px;}
.live {display:inline-flex; gap:8px; align-items:center; font-size:13px; color:#3b4448; padding-top:8px;}
.live .dot {width:9px; height:9px; border-radius:50%; background:#1e8a52; box-shadow:0 0 0 4px #1e8a5222;}
.hero {background:#1b2f36; border-radius:16px; padding:30px 34px; color:#fff; margin:6px 0 18px 0;}
.hero .eyebrow {color:#ff6a3d; font-weight:700; font-size:15px;}
.hero h1 {color:#fff !important; font-size:32px; margin:4px 0 4px 0; padding:0;}
.hero .lead {color:#d7dee0; font-size:16px;}
.hero .kgrid {display:grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap:20px; margin-top:26px;}
.hero .kl {font-size:12px; letter-spacing:1.2px; color:#b7c2c6; text-transform:uppercase;}
.hero .kv {font-size:34px; font-weight:800; line-height:1.15;}
.hero .ks {font-size:13px; color:#b7c2c6;}
.panel {background:#fff; border:1px solid #ece9e2; border-radius:14px; padding:18px 22px; margin-bottom:16px;}
.panel h3 {font-size:19px !important; margin:0 0 2px 0; color:#1f2a2e !important;}
.panel .sub {font-size:13px; color:#8a8a8a; margin-bottom:12px;}
.barrow {display:grid; grid-template-columns: 26px 230px 1fr; align-items:center; gap:10px; margin:9px 0; font-size:14px;}
.bartrack {background:#efede7; border-radius:999px; height:26px;}
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
.chip {display:inline-block; padding:3px 10px; border-radius:999px; font-size:.75rem; font-weight:600; margin:2px 4px 2px 0; background:#fde8f1; color:#9d174d; border:1px solid #f9c6dc;}
.post {border-left:3px solid #ccc; padding:4px 10px; margin:5px 0; font-size:.84rem; color:#2b3437; background:#fff; border-radius:0 8px 8px 0;}
.alertrow {display:flex; gap:12px; align-items:flex-start; padding:12px 0; border-bottom:1px solid #f0eee8;}
.adot {width:10px; height:10px; border-radius:50%; margin-top:6px; flex-shrink:0;}
.pill {display:inline-block; background:#efede7; border-radius:999px; padding:2px 10px; font-size:12px; margin:2px 4px 2px 0; color:#1f2a2e;}
.kv2 {display:grid; grid-template-columns: 150px 1fr; gap:4px 12px; font-size:14px; color:#2b3437;}
.kv2 div:nth-child(odd) {color:#8a8a8a;}
/* architecture diagrams */
.dg {display:grid; column-gap:0; row-gap:16px; margin-top:10px;}
.dg .hdr {background:#fff; border:1px solid #e6e3dc; border-radius:10px; padding:8px 10px; text-align:center; box-shadow:0 1px 3px #0000000d; margin:0 14px 10px 14px; align-self:end;}
.dg .hdr b {display:block; font-size:12px; letter-spacing:1.6px; color:#1f2a2e;}
.dg .hdr span {font-size:11px; color:#8a8a8a;}
.dg .cell {position:relative; z-index:1; display:flex; flex-direction:column; gap:14px; padding:0 14px; min-width:0;}
.dg .cell.stack {justify-content:center;}
.dg .cell.fill > .nd {flex:1;}
.dg .first {padding-top:34px;} .dg .last {padding-bottom:30px;}
.dg .ucbox {position:relative; z-index:0; border:2px dashed #f2a0a0; background:#fdf0ee; border-radius:16px;}
.dg .uct {position:absolute; top:-12px; left:16px; background:#fff; border:1px solid #f2a0a0; color:#e0503a; font-size:11px; letter-spacing:1.4px; font-weight:700; padding:2px 10px; border-radius:6px;}
.dg .ucf {position:absolute; bottom:7px; right:16px; font-size:11px; color:#c0365c; font-style:italic;}
.dg .cn {position:relative; z-index:2; display:flex; align-items:center; justify-content:center; min-width:0;}
.arw {position:relative; flex:1; height:0; border-top:2px solid #7b8790;}
.arw.dash {border-top-style:dashed;} .arw.green {border-top-color:#22a05a;} .arw.green::after {border-left-color:#22a05a;}
.arw::after {content:""; position:absolute; right:-4px; top:-7px; border-left:10px solid #7b8790; border-top:6px solid transparent; border-bottom:6px solid transparent;}
.arw span {position:absolute; bottom:5px; left:-12px; right:-12px; text-align:center; font-size:11px; color:#5b6569; white-space:nowrap;}
.brk {position:absolute; top:58px; bottom:44px; width:9px; border:2px solid #7b8790;}
.brk.l {left:2px; border-right:none; border-radius:9px 0 0 9px;} .brk.r {right:2px; border-left:none; border-radius:0 9px 9px 0;}
.varw {align-self:center; position:relative; height:30px; border-left:2px solid #7b8790; margin:-4px 0;}
.varw::after {content:""; position:absolute; left:-7px; bottom:-4px; border-top:10px solid #7b8790; border-left:6px solid transparent; border-right:6px solid transparent;}
.varw span {position:absolute; left:10px; top:6px; font-size:11px; color:#5b6569; white-space:nowrap;}
.nd {background:#fff; border:1px solid #ece9e2; border-radius:10px; padding:12px 14px; box-shadow:0 2px 6px #0000000a; border-top:4px solid #ccc;}
.nd .ty {display:inline-block; font-size:10px; letter-spacing:1px; background:#efede7; color:#5b6569; padding:1px 7px; border-radius:4px; margin-bottom:4px; text-transform:uppercase;}
.nd .t {font-weight:700; color:#1f2a2e; font-size:14px;}
.nd .s {font-size:12px; color:#6b7478; margin-top:2px; line-height:1.35;}
.nd ul {margin:6px 0 0 0; padding-left:0; list-style:none;}
.nd li {font-size:12px; color:#c2410c; background:#fff5ef; border-left:3px solid #f47521; padding:4px 8px; margin:3px 0; border-radius:0 4px 4px 0;}
.nd.persona {border-top-color:#3b82f6;} .nd.cap {border-top-color:#ef4444;} .nd.data {border-top-color:#c9a227;}
.nd.view {border-top-color:#14b8a6;} .nd.ai {border-top-color:#8b5cf6;} .nd.out {border-top-color:#22c55e;} .nd.rule {border-top-color:#94a3b8; background:#f8fafc;}
.legend {background:#fff; border:1px solid #ece9e2; border-radius:12px; padding:12px 16px; font-size:13px; color:#3b4448; display:flex; gap:18px; flex-wrap:wrap; margin-top:14px;}
.legend span {display:inline-flex; align-items:center; gap:6px;}
.legend i {width:12px; height:12px; border-radius:3px; display:inline-block;}
</style>""", unsafe_allow_html=True)


AXIS_LABELS = {"tickets": "Tickets", "avg_csat": "CSAT", "sla_met_rate": "SLA met", "repeat_contact_rate": "Repeat contact rate",
               "revenue_at_risk_usd": "Revenue at risk (USD)", "payment_success_rate": "Payment success", "posts": "Posts",
               "detection_lead_minutes": "Minutes", "mrr_at_risk_usd": "MRR at risk (USD)", "rate": "Rate"}
TIME_AXES = {"kpi_date", "ts", "ts_15m", "month", "date", "created_at", "label", "churn_driver"}


def style_fig(fig, h=320, legend=True):
    """House style for every chart: legend in its own row ABOVE the plot (never on the x-axis title), no raw column-name
    axis titles (dates need no title; measures get a readable label), light grid."""
    has_legend = legend and any(getattr(t, "showlegend", None) is not False and getattr(t, "name", None) for t in fig.data)
    fig.update_layout(height=h, margin=dict(l=0, r=0, t=36 if has_legend else 10, b=0), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font=dict(color="#2b3437", size=12), showlegend=legend,
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, title=None, font=dict(size=12), bgcolor="rgba(0,0,0,0)"))
    for ax in [a for a in fig.layout.to_plotly_json() if a.startswith("xaxis") or a.startswith("yaxis")]:
        t = fig.layout[ax].title.text
        if isinstance(t, str) and t and t == t.lower() and " " not in t:      # raw column name from plotly express
            fig.layout[ax].title.text = None if t in TIME_AXES or ax.startswith("xaxis") and t not in AXIS_LABELS else AXIS_LABELS.get(t, t.replace("_", " ").capitalize())
    fig.update_xaxes(gridcolor="#eeebe4"); fig.update_yaxes(gridcolor="#eeebe4")
    return fig


# ----------------------------------------------------------------------------- Lakebase data layer
@st.cache_resource
def ws():
    return WorkspaceClient()


@st.cache_resource
def _pg_state():
    return {"conn": None, "born": 0.0, "lock": threading.Lock(), "source": "Lakebase", "err": None}


def _pg_connect():
    import psycopg
    w = ws()
    host = os.getenv("PGHOST") or w.postgres.get_endpoint(name=LB_EP).status.hosts.host
    user = os.getenv("PGUSER") or os.getenv("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name
    tok = w.postgres.generate_database_credential(endpoint=LB_EP).token
    return psycopg.connect(host=host, dbname=LB_DB, user=user, password=tok, sslmode="require", connect_timeout=10, autocommit=True)


def _norm(df):
    for c in df.columns:
        if len(df) and isinstance(df[c].dropna().iloc[0] if df[c].notna().any() else None, Decimal):
            df[c] = df[c].astype(float)
        if pd.api.types.is_datetime64_any_dtype(df[c]) or (df[c].dtype == object and len(df) and isinstance(df[c].dropna().iloc[0] if df[c].notna().any() else None, datetime)):
            s = pd.to_datetime(df[c], utc=True, errors="coerce")
            df[c] = s.dt.tz_convert(None)
    return df


@st.cache_resource
def _sql_conn():
    from databricks import sql
    cfg = Config()
    return sql.connect(server_hostname=cfg.host.replace("https://", ""), http_path=f"/sql/1.0/warehouse/{WH}".replace("warehouse/", "warehouses/"),
                       credentials_provider=lambda: cfg.authenticate)


def _lit(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, (datetime, pd.Timestamp)):
        return f"TIMESTAMP'{pd.Timestamp(v):%Y-%m-%d %H:%M:%S}'"
    return "'" + str(v).replace("'", "''") + "'"


def _fallback(query, params):
    """Run the same query on the SQL warehouse against the UC gold tables (used only if Lakebase is unavailable)."""
    import re
    q = re.sub(rf"{PG}\.lb_(\w+)", lambda m: f"{UC}.{UC_NAME.get(m.group(1), 'gold_' + m.group(1))}", query)
    q = q.replace("ILIKE", "ILIKE")
    if params:
        parts = q.split("%s")
        q = "".join(p + (_lit(params[i]) if i < len(params) else "") for i, p in enumerate(parts))
    q = q.replace("::date", "")
    with _sql_conn().cursor() as c:
        c.execute(q)
        return c.fetchall_arrow().to_pandas()


def pg(query, params=None):
    s = _pg_state()
    with s["lock"]:
        for attempt in range(2):
            try:
                if s["conn"] is None or s["conn"].closed or time.time() - s["born"] > 2700:
                    s["conn"] = _pg_connect(); s["born"] = time.time()
                with s["conn"].cursor() as cur:
                    cur.execute(query, params)
                    cols = [d.name for d in cur.description]
                    rows = cur.fetchall()
                s["source"] = "Lakebase"
                return _norm(pd.DataFrame(rows, columns=cols))
            except Exception as e:
                s["err"] = str(e)[:300]
                try:
                    s["conn"].close()
                except Exception:
                    pass
                s["conn"] = None
    s["source"] = "SQL warehouse (fallback)"
    return _norm(_fallback(query, params))


T = lambda name: f"{PG}.lb_{name}"   # noqa: E731


# A surge is "active" if its latest anomalous 15-minute window started within the last 30 minutes
# (the newest 15-minute bucket is usually still incomplete, so last_seen lags "now" by up to ~25 minutes).
ACTIVE = timedelta(minutes=30)


@st.cache_data(ttl=30, show_spinner=False)
def data_now():
    df = pg(f"SELECT max(ts) AS now FROM {T('region_heatmap_5m')}")
    return pd.Timestamp(df.now.iloc[0])


@st.cache_data(ttl=20, show_spinner=False)
def data_version():
    """Fingerprint across the Lakebase tables the pages depend on. The generator re-syncs 15 tables in parallel, so the
    heatmap can land before ticket history or anomalies; reloading whenever ANY of these changes avoids caching a half-synced view."""
    v = pg(f"""SELECT (SELECT max(ts) FROM {T('region_heatmap_5m')})::text || '|' ||
                      (SELECT coalesce(max(created_at)::text, '') FROM {T('subscriber_ticket_history')}) || '|' ||
                      (SELECT count(*) FROM {T('ticket_anomalies')})::text || '|' ||
                      (SELECT coalesce(max(last_seen)::text, '') FROM {T('ticket_anomalies')}) || '|' ||
                      (SELECT coalesce(max(ts)::text, '') FROM {T('payment_health_5m')}) || '|' ||
                      (SELECT coalesce(max(as_of)::text, '') FROM {T('churn_risk_by_issue')}) AS v""") if _pg_state()["source"] == "Lakebase" else None
    return str(v.v.iloc[0]) if v is not None and len(v) else str(data_now())


@st.cache_data(ttl=600, show_spinner="Loading live command-center data from Lakebase…")
def load(now, ver=""):
    d0 = now - timedelta(hours=24)
    day = now.normalize()
    D = {
        "heat": pg(f"SELECT * FROM {T('region_heatmap_5m')} WHERE ts = %s", (now,)),
        "health": pg(f"SELECT * FROM {T('service_health_region_5m')} WHERE ts = %s", (now,)),
        "health_24h": pg(f"SELECT ts, region_group, domain, health_state FROM {T('service_health_region_5m')} WHERE ts > %s AND health_state NOT IN ('Healthy','Watch')", (d0,)),
        "frus": pg(f"SELECT * FROM {T('frustration_index_5m')} WHERE ts > %s", (d0,)),
        "anom": pg(f"SELECT * FROM {T('ticket_anomalies')} ORDER BY detected_at DESC"),
        "sig": pg(f"SELECT ts_15m, issue_signature, tickets_15m, baseline_15m, is_anomalous FROM {T('signature_trends_15m')} WHERE ts_15m > %s", (d0,)),
        "pay_now": pg(f"SELECT * FROM {T('payment_health_5m')} WHERE ts = %s", (now,)),
        "pay_rar_today": pg(f"SELECT country_code, sum(revenue_at_risk_usd) AS rar FROM {T('payment_health_5m')} WHERE ts >= %s GROUP BY 1", (day,)),
        "social": pg(f"SELECT * FROM {T('social_posts')} WHERE posted_at > %s", (d0,)),
        "score": pg(f"SELECT * FROM {T('incident_scorecard')}"),
        "kpi": pg(f"SELECT * FROM {T('exec_daily_kpis')} ORDER BY kpi_date"),
        "changes": pg(f"SELECT * FROM {T('change_events')} WHERE change_ts > %s AND (change_type <> 'deployment' OR outcome <> 'success') ORDER BY change_ts DESC", (d0,)),
        "tix_today": pg(f"""SELECT region_group, issue_domain, count(*) AS n, sum(CASE WHEN sla_met THEN 1 ELSE 0 END) AS sla_met,
                             sum(csat_score) AS csat_sum, count(csat_score) AS csat_n, sum(CASE WHEN is_repeat_contact THEN 1 ELSE 0 END) AS repeats
                           FROM {T('subscriber_ticket_history')} WHERE created_at >= %s GROUP BY 1, 2""", (day,)),
        "outcomes": pg(f"SELECT * FROM {T('customer_outcomes_daily')} ORDER BY kpi_date"),
        "churn": pg(f"SELECT * FROM {T('churn_risk_by_issue')}"),
        "at_risk_today": pg(f"""SELECT count(DISTINCT h.subscriber_id) AS n FROM {T('subscriber_ticket_history')} h
                               JOIN {T('subscriber_360')} s ON s.subscriber_id = h.subscriber_id
                               WHERE h.created_at >= %s AND s.churn_risk_band IN ('High','Medium')""", (day,)),
        "tix_prev7": pg(f"""SELECT date_trunc('day', created_at) AS d, count(*) AS n FROM {T('subscriber_ticket_history')}
                           WHERE created_at >= %s AND created_at < %s AND (created_at - date_trunc('day', created_at)) <= %s GROUP BY 1""",
                        (day - timedelta(days=7), day, now - day)),
    }
    for k in ["ai_recommended_actions", "sample_tickets"]:
        if k in D["anom"]:
            D["anom"][k] = D["anom"][k].apply(lambda v: v if isinstance(v, list) else (json.loads(v) if isinstance(v, str) and v.startswith("[") else []))
    return D


# ----------------------------------------------------------------------------- Lakebase ops store (write-back)
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
            with _pg_connect() as c:
                for d in self.DDL:
                    c.execute(d)
            self.ok = True
        except Exception as e:
            self.err = str(e)[:300]

    def _exec(self, sql_, params=None, fetch=False):
        with _pg_connect() as c:
            cur = c.execute(sql_, params)
            return cur.fetchall() if fetch else None

    def _mem(self, k):
        return st.session_state.setdefault(f"ops_{k}", [])

    def declare(self, a, actor, now):
        if self.ok:
            try:
                self._exec("""INSERT INTO command_center_ops.incident_actions (anomaly_id, issue_signature, domain, region_groups, action, actor, note, demo_clock)
                              VALUES (%s,%s,%s,%s,'declare_incident',%s,'Declared from Support Command Center',%s)""",
                           (a["anomaly_id"], a["issue_signature"], a["issue_domain"], a["region_groups"], actor, now))
                self._exec("""INSERT INTO command_center_ops.known_issue_banners (anomaly_id, headline, macro, active) VALUES (%s,%s,%s,TRUE)
                              ON CONFLICT (anomaly_id) DO UPDATE SET active=TRUE, macro=EXCLUDED.macro""", (a["anomaly_id"], a["ai_headline"], a["ai_customer_macro"]))
                return
            except Exception as e:
                self.err = str(e)[:300]
        self._mem("actions").append(dict(anomaly_id=a["anomaly_id"], domain=a["issue_domain"], region_groups=a["region_groups"], action="declare_incident", actor=actor, created_at=datetime.utcnow()))
        self._mem("banners").append(dict(anomaly_id=a["anomaly_id"], headline=a["ai_headline"], macro=a["ai_customer_macro"], created_at=datetime.utcnow()))

    def ack(self, rg, dom, state, actor, now):
        if self.ok:
            try:
                self._exec("INSERT INTO command_center_ops.alert_acks (region_group, domain, health_state, actor, demo_clock) VALUES (%s,%s,%s,%s,%s)", (rg, dom, state, actor, now))
                return
            except Exception as e:
                self.err = str(e)[:300]
        self._mem("acks").append(dict(region_group=rg, domain=dom, health_state=state, actor=actor, created_at=datetime.utcnow()))

    def frame(self, kind):
        sqls = {"actions": ("SELECT anomaly_id, domain, region_groups, action, actor, created_at FROM command_center_ops.incident_actions ORDER BY created_at DESC",
                            ["anomaly_id", "domain", "region_groups", "action", "actor", "created_at"]),
                "banners": ("SELECT anomaly_id, headline, macro, created_at FROM command_center_ops.known_issue_banners WHERE active ORDER BY created_at DESC",
                            ["anomaly_id", "headline", "macro", "created_at"]),
                "acks": ("SELECT region_group, domain, health_state, actor, created_at FROM command_center_ops.alert_acks ORDER BY created_at DESC",
                         ["region_group", "domain", "health_state", "actor", "created_at"])}
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
def genie_ask(question):
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
                r = w.genie.get_message_attachment_query_result(space_id=GENIE, conversation_id=msg.conversation_id, message_id=msg.id, attachment_id=a.attachment_id)
                sr = r.statement_response
                tables.append(pd.DataFrame((sr.result.data_array or []) if sr.result else [], columns=[c.name for c in sr.manifest.schema.columns]))
            except Exception:
                pass
    return dict(q=question, texts=texts, tables=tables, sqls=sqls)


GENIE_SUGGEST = {
    "exec": ["Which incidents were detected by customers before monitoring and by how many minutes?", "Which region had the lowest SLA met rate over the last 15 days?",
             "Compare CSAT and SLA met rate on the latest day vs the previous 7 days by region"],
    "support": ["What is driving the ticket spike in LATAM today?", "Show high churn-risk Ultimate Fan subscribers hit by an incident",
                "How did the repeat contact rate change on incident days?"],
    "eng": ["Why did Brazil turn red at 15:45 UTC on 2026-10-27?", "Which deployments preceded incidents?", "Show payment success in Germany by gateway on the latest day"],
}


# ----------------------------------------------------------------------------- state
ss = st.session_state
ss.setdefault("persona", "exec"); ss.setdefault("page", "home")


def go_page(p):
    ss.page = p


def set_persona(p):
    ss.persona = p
    ss.persona_sel = PERSONAS[p]["label"]
    if ss.page not in allowed_pages(p):
        ss.page = "home"


def open_customer(sid):
    ss.c360_id = sid
    if "customer360" in allowed_pages(ss.persona):
        ss.page = "customer360"


def open_brief(aid):
    ss.brief_id = aid; ss.brief_sync = True; ss.page = "brief"


def effective_health(D):
    h = D["health"].copy()
    acts = ops().frame("actions")
    if len(acts) and len(h):
        an = D["anom"].set_index("anomaly_id")
        now = D["now"]
        for aid in acts.anomaly_id.unique():
            if aid in an.index and an.loc[aid, "last_seen"] + timedelta(minutes=30) >= now:
                rgs = [r.strip() for r in str(an.loc[aid, "region_groups"]).split(",")]
                m = (h.domain == an.loc[aid, "issue_domain"]) & h.region_group.isin(rgs) & h.health_state.isin(["Customer-Detected", "Telemetry Alert", "Watch"])
                h.loc[m, "health_state"] = "Known (declared in app)"; h.loc[m, "open_incident_id"] = "declared in app"
    return h


def live_anomalies(D):
    a = D["anom"]
    return a[a.last_seen >= D["now"] - ACTIVE]


def alerts_now(D):
    items = []
    for r in effective_health(D).query("health_state in ['Customer-Detected','Telemetry Alert']").itertuples():
        items.append(dict(sev="critical" if r.health_state == "Customer-Detected" else "high", title=f"{r.domain} · {r.region_group}: {r.health_state}",
                          detail=f"Tickets x{r.ticket_ratio:.1f} vs expected · internal errors x{r.error_ratio:.1f}" + (" · no internal alert yet" if r.health_state == "Customer-Detected" else "")))
    acts = ops().frame("actions"); declared = set(acts.anomaly_id) if len(acts) else set()
    for a in live_anomalies(D).itertuples():
        known = (isinstance(a.incident_declared_at, pd.Timestamp) and a.incident_declared_at <= D["now"]) or a.anomaly_id in declared
        if not known:
            items.append(dict(sev="critical", title=f"Undeclared ticket surge: {a.issue_signature}", detail=f"{a.ai_headline} · {a.top_countries}"))
    for r in D["pay_now"].query("gateway_state != 'Healthy'").itertuples():
        items.append(dict(sev="high" if r.gateway_state == "Degraded" else "critical", title=f"Payment gateway {r.gateway} · {r.country_code}: {r.gateway_state}",
                          detail=f"Success {r.success_rate_15m*100:.1f}% vs {r.baseline_success_rate*100:.1f}% baseline"))
    return items


def barlist(title, sub, rows, color="#1e8a52", fmt=lambda v: f"{v:,.0f}"):
    mx = max([v for _, v in rows] + [1])
    body = "".join(f"<div class='barrow'><div style='color:#8a8a8a'>{i+1}</div><div style='font-weight:600'>{html.escape(str(k))}</div>"
                   f"<div class='bartrack'><div class='barfill' style='width:{max(6, 100*v/mx):.1f}%;background:{color}'>{fmt(v)}</div></div></div>" for i, (k, v) in enumerate(rows))
    st.markdown(f"<div class='panel'><h3>{html.escape(title)}</h3><div class='sub'>{html.escape(sub)}</div>{body}</div>", unsafe_allow_html=True)


def panel(title, sub=""):
    st.markdown(f"<div class='panel'><h3>{html.escape(title)}</h3><div class='sub'>{html.escape(sub)}</div></div>", unsafe_allow_html=True)


def hero(eyebrow, lead, kpis, now, title=None):
    cells = "".join(f"<div><div class='kl'>{k}</div><div class='kv' style='color:{c}'>{v}</div><div class='ks'>{s}</div></div>" for k, v, s, c in kpis)
    greet = title or (f"{'Good morning' if now.hour < 12 else ('Good afternoon' if now.hour < 18 else 'Good evening')}, {html.escape(first_name())}")
    st.markdown(f"<div class='hero'><div class='eyebrow'>{eyebrow}</div><h1>{greet}</h1><div class='lead'>{lead}</div><div class='kgrid'>{cells}</div></div>", unsafe_allow_html=True)


RED, GREEN, AMBER, WHITE = "#ff5470", "#34d399", "#fbbf24", "#ffffff"


# ----------------------------------------------------------------------------- layout chrome
def sidebar(D):
    p = ss.persona
    with st.sidebar:
        st.markdown("<style>@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');</style>", unsafe_allow_html=True)
        st.markdown(f"<div class='sb-brand'><div class='sb-logo'>{LOGO_SVG}</div><div><div class='sb-name'>Fanimation</div><div class='sb-sub'>Support Command Center</div></div></div>"
                    f"<div class='sb-powered'>POWERED BY <span class='db'>{DB_SVG}databricks</span></div>", unsafe_allow_html=True)
        st.divider()
        st.markdown("<div class='sb-label'>Viewing as</div>", unsafe_allow_html=True)
        labels = {v["label"]: k for k, v in PERSONAS.items()}
        ss.setdefault("persona_sel", PERSONAS[p]["label"])
        st.selectbox("Viewing as", list(labels), key="persona_sel", label_visibility="collapsed", on_change=lambda: set_persona(labels[ss.persona_sel]))
        st.markdown(f"<div class='sb-tag'>{PERSONAS[p]['tag']}</div>", unsafe_allow_html=True)
        st.divider()
        with st.expander("About this role", icon=":material/person:"):
            r = ROLE_ABOUT[p]
            st.markdown("<div class='sb-about'>" + "".join(f"<h5>{h}</h5><p>{html.escape(t)}</p>" for h, t in r) + "</div>", unsafe_allow_html=True)
        st.divider()
        n_alerts = len(alerts_now(D))

        def item(key):
            label, icon = PAGE_META[key]
            st.button(label, key=f"nav_{key}", icon=icon, on_click=go_page, args=(key,), use_container_width=True)
        item("home"); item("alerts")
        if n_alerts:
            st.markdown(f"<style>.st-key-nav_alerts button::after {{content:'{n_alerts}'; margin-left:auto; background:#d92d20; color:#fff; border-radius:999px;"
                        f" padding:1px 10px; font-size:13px; font-weight:700; line-height:20px;}}</style>", unsafe_allow_html=True)
        st.markdown("<div class='sb-section'>Dashboards</div>", unsafe_allow_html=True)
        for k in PERSONAS[p]["dashboards"]:
            item(k)
        st.markdown("<div class='sb-section'>AI powered research</div>", unsafe_allow_html=True)
        for k in PERSONAS[p]["ai"]:
            item(k)
        st.divider()
        o = ops()
        if st.button("Reset demo actions", key="reset", icon=":material/restart_alt:", use_container_width=True):
            o.reset()
        st.markdown(f"<div class='sb-foot'>Reads: {_pg_state()['source']} · write-back: {'Lakebase' if o.ok else 'session-only'}</div>", unsafe_allow_html=True)
    st.markdown(f"<style>[data-testid='stSidebar'] .st-key-nav_{ss.page} button {{background:#2c454e !important;}}"
                f" [data-testid='stSidebar'] .st-key-nav_{ss.page} button p {{color:#ffffff !important; font-weight:500 !important;}}"
                f" [data-testid='stSidebar'] .st-key-nav_{ss.page} button [data-testid='stIconMaterial'] {{color:#ffffff !important;}}"
                f" [data-testid='stSidebar'] .st-key-reset button p {{font-size:14px !important; color:#a9bcc1 !important;}}"
                f" [data-testid='stSidebar'] .st-key-reset button {{min-height:38px !important;}}</style>", unsafe_allow_html=True)


def topbar(D):
    st.markdown("""<style>
    /* style-only markdown blocks must not take up a row (they pushed the header strip down) */
    [data-testid="stMain"] [data-testid="stElementContainer"]:has(style):not(:has(.tb-left)) {display:none !important;}
    /* page content starts below the fixed 64px header */
    .block-container, [data-testid="stMainBlockContainer"] {padding-top:92px !important;}
    /* fixed header: spans from the sidebar edge to the window edge, 64px tall, hairline underneath */
    /* the container is a native horizontal flex row (st.container(horizontal=True)); only position + size it here */
    .st-key-topbar {position:fixed !important; top:0; left:300px; right:0; width:auto !important; height:64px; z-index:990; background:#ffffff;
        border-bottom:1px solid #ebe7df; padding:0 32px !important; margin:0 !important; box-sizing:border-box;
        flex-wrap:nowrap !important; align-items:center !important; column-gap:26px !important; overflow:hidden;}
    .st-key-topbar [data-testid="stElementContainer"] {margin:0 !important;}
    .st-key-topbar [data-testid="stMarkdownContainer"] p {margin:0 !important;}
    .st-key-topbar .stButton button {height:40px !important; min-height:40px !important; padding:0 2px !important; background:transparent !important;
        border:none !important; box-shadow:none !important; display:flex; align-items:center;}
    .tb-left {display:flex; align-items:center; gap:14px; white-space:nowrap; height:40px;}
    .tb-label {font-size:11.5px; letter-spacing:2px; color:#8a8580; font-weight:500; text-transform:uppercase; display:inline-flex; align-items:center; gap:8px;}
    .tb-label .dot {width:8px; height:8px; border-radius:50%; background:#1e8a52; box-shadow:0 0 0 3px #1e8a5222;}
    .tb-pill {font-size:16px; color:#1f2a2e; font-weight:500;}
    .tb-pill span {color:#8a8580; font-weight:400;}
    .st-key-refresh button p, .st-key-about button p, .st-key-arch button p {font-size:16px !important; color:#3b4448 !important; font-weight:400 !important; white-space:nowrap; margin:0 !important;}
    .st-key-refresh button:hover p, .st-key-about button:hover p, .st-key-arch button:hover p {color:#f47521 !important;}
    .st-key-genie_top {border-left:1px solid #e3dfd7; padding-left:24px;}
    .st-key-genie_top button [data-testid="stIconMaterial"] {color:#ff3621 !important; font-size:26px !important;}
    .st-key-genie_top button:hover [data-testid="stIconMaterial"] {color:#c8211b !important;}
    /* scoped under .st-key-topbar so it outranks the generic transparent top-bar button rule */
    .st-key-topbar .st-key-avatar button {width:40px !important; height:40px !important; min-height:40px !important; border-radius:50% !important; padding:0 !important;
        background:#1b3038 !important; justify-content:center !important;}
    .st-key-topbar .st-key-avatar button:hover {background:#24404a !important;}
    .st-key-topbar .st-key-avatar button p {color:#ffffff !important; font-weight:600 !important; font-size:16px !important; margin:0 !important;}
    </style>""", unsafe_allow_html=True)
    src = _pg_state()["source"]
    src_short = "Lakebase" if src == "Lakebase" else "SQL warehouse (fallback)"
    with st.container(key="topbar", horizontal=True, vertical_alignment="center", gap="medium"):
        st.markdown(f"<div class='tb-left'><span class='tb-label'><span class='dot'></span>Live data</span>"
                    f"<span class='tb-pill'>As of {D['now']:%a %d %b %H:%M} UTC <span>· {src_short}</span></span></div>", unsafe_allow_html=True, width="stretch")
        if st.button("Refresh", key="refresh", width="content"):
            data_now.clear(); data_version.clear(); load.clear(); st.rerun()
        st.button("About", key="about", on_click=go_page, args=("about",), width="content")
        st.button("Architecture", key="arch", on_click=go_page, args=("architecture",), width="content")
        st.button("", key="genie_top", icon=":material/auto_awesome:", help="Ask Genie", on_click=go_page, args=("genie",), width="content")
        st.button(first_name()[:1].upper(), key="avatar", help=user_email() or first_name(), width="content")


def persona_cards():
    """Only the current persona's view is shown; switch persona with 'Viewing as' in the sidebar."""
    p = PERSONAS[ss.persona]
    st.markdown(f"<div class='blurb'><b>{p['label']} view</b> · {p['sub']} — {p['blurb']}</div>", unsafe_allow_html=True)


def render_alerts(items, limit=None):
    col = {"critical": "#e11d74", "high": "#ea580c"}
    if not items:
        st.success("All clear — no active alerts right now.")
        return
    shown = items[:limit] if limit else items
    st.markdown("<div class='panel'>" + "".join(f"<div class='alertrow'><div class='adot' style='background:{col[i['sev']]}'></div><div><div style='font-weight:700;color:#1f2a2e'>{html.escape(i['title'])}</div>"
                f"<div style='font-size:13px;color:#64748b'>{html.escape(i['detail'])}</div></div></div>" for i in shown) + "</div>", unsafe_allow_html=True)
    if limit and len(items) > limit:
        st.caption(f"+ {len(items) - limit} more")


@st.fragment(run_every="60s")
def live_watcher():
    """Poll Lakebase for a newer snapshot; rerun the app when the pipeline + sync publish new data."""
    try:
        ver = data_version()
    except Exception:
        return
    if ss.get("seen_ver") is not None and ver != ss.seen_ver:
        data_now.clear(); load.clear(); st.rerun(scope="app")


# ----------------------------------------------------------------------------- shared widgets
def health_matrix(D, ack=True):
    h = effective_health(D)
    cells = ["<div class='matrix'><div></div>" + "".join(f"<div class='mh'>{r}</div>" for r in REGIONS)]
    hi = h.set_index(["domain", "region_group"])
    for d in DOMAINS:
        cells.append(f"<div class='md'>{d}</div>")
        for r in REGIONS:
            if (d, r) in hi.index:
                x = hi.loc[(d, r)]; s_ = x.health_state; col = STATE_COLORS.get(s_, "#1e8a52")
                sub = "no internal alert" if s_ == "Customer-Detected" else (x.open_incident_id if isinstance(x.open_incident_id, str) else "")
                cells.append(f"<div class='cell {'pulse' if s_ == 'Customer-Detected' else ''}' style='background:{col}'><b>{html.escape(s_.upper() if s_ == 'Customer-Detected' else s_)}</b>"
                             f"{html.escape(sub)}<br>tix x{x.ticket_ratio:.1f} · err x{x.error_ratio:.1f}</div>")
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
            r = hot.iloc[opts.index(pick)]; ops().ack(r.region_group, r.domain, r.health_state, user_email() or "demo-user", D["now"])
            st.toast("Acknowledged · written to Lakebase")


def geo_map(D, key="map"):
    heat = D["heat"].copy()
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
        fig.add_trace(go.Scattergeo(lat=hot.lat, lon=hot.lon, mode="markers", hoverinfo="skip", showlegend=False, marker=dict(size=hot["size"] * 2.2, color="rgba(225,29,116,0.18)", line=dict(width=2, color="#e11d74"))))
    fig.add_trace(go.Scattergeo(lat=heat.lat, lon=heat.lon, text=heat.label, hoverinfo="text", mode="markers", showlegend=False,
                                marker=dict(size=heat["size"], color=heat["v"], colorscale=cs, cmin=rng_[0], cmax=rng_[1], line=dict(width=0.6, color="#fff"), colorbar=dict(title=title, thickness=10, len=0.6))))
    fig.update_geos(projection_type="natural earth", showland=True, landcolor="#e9e6df", showocean=True, oceancolor="#f6f5f1", showcountries=True, countrycolor="#d6d2c8",
                    coastlinecolor="#d6d2c8", bgcolor="rgba(0,0,0,0)", lataxis_range=[-50, 72])
    fig.update_layout(height=380, margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=key)


def telemetry_chart(D, default="Global", key="tele", h=320):
    fr = D["frus"]; now = D["now"]
    scopes = ["Global"] + REGIONS + sorted(fr[fr.scope_type == "Country"].scope.unique().tolist())
    c1, c2 = st.columns([4, 1])
    with c2:
        scope = st.selectbox("Scope", scopes, index=scopes.index(default) if default in scopes else 0, key=f"scope_{key}")
        s = fr[fr.scope == scope].sort_values("ts"); cur = s.tail(1)
        fi = float(cur.frustration_index.iloc[0]) if len(cur) else 0
        g = go.Figure(go.Indicator(mode="gauge+number", value=fi, title={"text": "Frustration index", "font": {"size": 13}},
                                   gauge={"axis": {"range": [0, 100]}, "bar": {"color": "#f47521"}, "steps": [{"range": [0, 25], "color": "#dcf2e4"}, {"range": [25, 55], "color": "#fdf0cf"}, {"range": [55, 100], "color": "#fbd5e5"}]}))
        st.plotly_chart(style_fig(g, 200, False), use_container_width=True, config={"displayModeBar": False}, key=f"g_{key}")
        if len(cur) and bool(cur.divergence_flag.iloc[0]):
            st.error("Divergence: streams dropping while contacts spike — users are hitting breakage.")
        elif len(cur) and isinstance(cur.active_release.iloc[0], str):
            st.info(f"Simulcast in effect: {cur.active_release.iloc[0]} — traffic spike is expected.")
    with c1:
        f = go.Figure()
        f.add_trace(go.Scatter(x=s.ts, y=s.baseline_streams, name="Release-aware baseline", line=dict(color="#9ca3af", dash="dot")))
        f.add_trace(go.Scatter(x=s.ts, y=s.streams, name="Concurrent streams", line=dict(color="#1c3a3f", width=2.5)))
        f.add_trace(go.Scatter(x=s.ts, y=s.contacts_per_10k_streams, name="Contacts / 10k streams", yaxis="y2", line=dict(color="#f47521", width=2)))
        f.add_trace(go.Scatter(x=s.ts, y=s.baseline_contacts_per_10k_streams, name="Expected contacts", yaxis="y2", line=dict(color="#f4752166", dash="dash")))
        for t in s[s.divergence_flag.astype(bool)].ts:
            f.add_vrect(x0=t - timedelta(minutes=2.5), x1=t + timedelta(minutes=2.5), fillcolor="#e11d74", opacity=0.15, line_width=0)
        for name, t in s.dropna(subset=["active_release"]).groupby("active_release").ts.min().items():
            f.add_annotation(x=t, y=0.99, yref="paper", yanchor="top", text=f"🎬 {name}", showarrow=False, font=dict(size=10, color="#7c3aed"))
        for r in D["changes"].itertuples():
            f.add_vline(x=r.change_ts, line=dict(color="#d4a017", width=1, dash="dot"))
        f.update_layout(xaxis=dict(range=[now - timedelta(hours=24), now + timedelta(minutes=10)]), yaxis=dict(title="Streams"),
                        yaxis2=dict(title="Contacts / 10k", overlaying="y", side="right", showgrid=False))
        st.plotly_chart(style_fig(f, h), use_container_width=True, config={"displayModeBar": False}, key=f"t_{key}")
        st.caption("Last 24 hours · pink = divergence · dotted gold = risky deploys/config changes · purple = simulcast releases (benign spikes)")


def sparkline(sig_df, key):
    if len(sig_df):
        f = go.Figure(go.Bar(x=sig_df.ts_15m, y=sig_df.tickets_15m, marker_color=["#e11d74" if v else "#b8b4aa" for v in sig_df.is_anomalous]))
        f.update_layout(xaxis=dict(visible=False), yaxis=dict(visible=False))
        st.plotly_chart(style_fig(f, 80, False), use_container_width=True, config={"displayModeBar": False}, key=key)


def anomaly_cards(D, compact=False, limit=None, window_h=24):
    now = D["now"]; an = D["anom"]; an = an[an.detected_at > now - timedelta(hours=window_h)]
    if limit:
        an = an.head(limit)
    acts = ops().frame("actions"); declared = set(acts.anomaly_id) if len(acts) else set()
    if an.empty:
        st.info(f"No ticket surges detected in the last {window_h} hours. Run the live generator with a scenario to create one.")
    for a in an.to_dict("records"):
        active = a["last_seen"] >= now - ACTIVE
        inc_open = isinstance(a.get("incident_declared_at"), pd.Timestamp) and a["incident_declared_at"] <= now
        in_app = a["anomaly_id"] in declared
        age = int((now - a["detected_at"]).total_seconds() // 60)
        if inc_open:
            badge = f"<span class='badge' style='background:#d97706'>KNOWN · {a['incident_id']}</span>"
        elif in_app:
            badge = "<span class='badge' style='background:#d97706'>DECLARED IN APP</span>"
        elif active:
            badge = f"<span class='badge' style='background:#e11d74'>UNDECLARED · customers detected {age} min ago</span>"
        else:
            badge = "<span class='badge' style='background:#2563eb'>SUBSIDED</span>"
        actions = a.get("ai_recommended_actions") or []
        extra = "" if compact else (f"<div style='margin-top:6px;font-size:.8rem;color:#475569'>📍 {html.escape(str(a['top_countries']))} &nbsp; 📺 {html.escape(str(a['top_devices']))}</div>"
                                    f"<div style='margin-top:4px;font-size:.8rem;color:#a16207'>🔧 Suspected change: {html.escape(str(a.get('suspected_change') or 'none found'))}</div>"
                                    f"<div style='margin-top:4px;font-size:.8rem'>✅ {' · '.join(html.escape(str(x)) for x in actions)}</div>")
        st.markdown(f"""<div class='card {'hot' if (active and not inc_open and not in_app) else ''}'>{badge}<span class='badge' style='background:#64748b'>{html.escape(a['issue_domain'])}</span>
          <div style='font-weight:800;font-size:1rem;margin-top:6px;color:#1f2a2e'>{html.escape(str(a['ai_headline']))}</div>
          <div style='color:#8a8a8a;font-size:.78rem'>Signature <b style='color:#1f2a2e'>{html.escape(a['issue_signature'])}</b> · detected {a['detected_at']:%d %b %H:%M} UTC ·
             {int(a['total_tickets']):,} tickets · peak {int(a['peak_tickets_15m'])}/15m vs {float(a['baseline_15m']):.1f} baseline</div>
          <div style='margin-top:6px;font-size:.88rem;color:#2b3437'>{html.escape(str(a['ai_summary']))}</div>{extra}</div>""", unsafe_allow_html=True)
        if not compact:
            c1, c2 = st.columns([3, 1.3])
            with c1:
                sp = D["sig"]; sparkline(sp[sp.issue_signature == a["issue_signature"]].sort_values("ts_15m"), f"sp_{ss.page}_{a['anomaly_id']}")
            if active and not inc_open and not in_app:
                if c2.button("📣 Declare incident + publish banner", key=f"dec_{ss.page}_{a['anomaly_id']}", use_container_width=True):
                    ops().declare(a, user_email() or "demo-user", now); st.toast("Incident declared · banner published (Lakebase)"); st.rerun()
            c2.button("Open brief", key=f"ob_{ss.page}_{a['anomaly_id']}", on_click=open_brief, args=(a["anomaly_id"],), use_container_width=True)
            with c2.expander("Agent macro"):
                st.write(a["ai_customer_macro"])


def today_stats(D):
    t = D["tix_today"]
    n = t.n.sum()
    return dict(n=int(n), sla=(t.sla_met.sum() / n) if n else float("nan"), csat=(t.csat_sum.sum() / t.csat_n.sum()) if t.csat_n.sum() else float("nan"),
                repeat=(t.repeats.sum() / n) if n else float("nan"))


def fmt_pct(v, d=1):
    return "—" if v != v else f"{v*100:.{d}f}%"


# ----------------------------------------------------------------------------- pages
def page_home(D):
    persona_cards()
    now = D["now"]; tst = today_stats(D); sc = D["score"]; heat = D["heat"]
    sc_today = sc[sc.declared_at >= now.normalize()]
    h = effective_health(D); cd_cells = int((h.health_state == "Customer-Detected").sum())
    rar = D["pay_rar_today"].rar.sum() if len(D["pay_rar_today"]) else 0
    churn = D["churn"]; mrr = churn.mrr_at_risk_usd.sum(); at_risk = churn.at_risk_subscribers.sum()
    live = live_anomalies(D); banners = ops().frame("banners")
    gl = D["frus"][D["frus"].scope_type == "Global"].sort_values("ts").tail(1); fi = float(gl.frustration_index.iloc[0]) if len(gl) else 0
    if ss.persona == "exec":
        tp = D["tix_prev7"]; typical = float(tp.n.mean()) if len(tp) else float("nan")
        known, ai = issue_split(D)
        live_ai = ai[ai.last_seen >= now - ACTIVE] if len(ai) else ai
        hero("Right now", "Today's subscriber experience at a glance — trends and incident performance are in Executive Overview.", [
            ("Tickets today", f"{tst['n']:,}", "—" if typical != typical else f"vs {typical:,.0f} typical by {now:%H:%M} UTC", RED if typical == typical and tst["n"] > 1.3 * typical else WHITE),
            ("CSAT today", "—" if tst["csat"] != tst["csat"] else f"{tst['csat']:.2f}", "Target ≥ 4.0", GREEN if tst["csat"] >= 4 else AMBER),
            ("Revenue at risk today", f"${rar:,.0f}", "failed payments, degraded gateways", RED if rar > 500 else GREEN),
            ("Customer-reported, not yet declared", f"{len(live_ai)}", f"live ticket surges · {cd_cells} health cells customer-detected", RED if len(live_ai) or cd_cells else GREEN)], now)
        panel("Needs attention", "Open issues in the last 24 hours — headline and ticket count only; open Incident Brief for detail")
        c1, c2 = st.columns(2)
        with c1:
            issue_list("Known issues", "Declared incidents and in-app declarations", known, "#d97706")
        with c2:
            issue_list("Undeclared ticket surges", "AI-detected, not yet declared", ai, "#e11d74")
        b1, b2, _ = st.columns([1, 1, 3])
        b1.button("Executive Overview →", key="go_exec", on_click=go_page, args=("executive",), use_container_width=True)
        b2.button("Incident Brief →", key="go_brief", on_click=go_page, args=("brief",), use_container_width=True)
    elif ss.persona == "support":
        hero("Queues & Customer Impact", "Surges, SLA, CSAT and at-risk customers across the global support operation.", [
            ("Tickets · last 15 min", f"{int(heat.tickets_15m.sum()):,}", f"expected {heat.expected_tickets_15m.sum():,.0f}", RED if heat.tickets_15m.sum() > 1.5 * heat.expected_tickets_15m.sum() else WHITE),
            ("SLA met today", fmt_pct(tst["sla"]), "Target ≥ 80%", GREEN if tst["sla"] >= 0.8 else AMBER),
            ("Active ticket surges", f"{len(live)}", "AI-detected signatures", RED if len(live) else GREEN),
            ("Known-issue banners live", f"{len(banners)}", "published to agents", WHITE),
            ("At-risk subscribers contacting today", f"{int(D['at_risk_today'].n.iloc[0]):,}", "High/Medium churn risk", AMBER)], now)
        c1, c2 = st.columns([1.2, 1])
        with c1:
            known, ai = issue_split(D)
            issue_list("Known issues", "Declared incidents and in-app declarations · last 24 hours", known, "#d97706")
            issue_list("AI-detected issues", "Ticket surges not yet declared · last 24 hours", ai, "#e11d74")
            st.button("Open Subscriber Experience →", key="go_sub", on_click=go_page, args=("subscriber",))
        with c2:
            t = D["tix_today"]
            by_dom = t.groupby("issue_domain").n.sum().sort_values(ascending=False)
            barlist("Tickets today by domain", f"Since 00:00 UTC on {now:%d %b}", [(k, int(v)) for k, v in by_dom.items()], color="#f47521")
            by_rg = t.groupby("region_group").agg(n=("n", "sum"), s=("sla_met", "sum"))
            barlist("SLA met % by region", "First-response SLA by subscriber tier", [(k, 100 * r.s / max(r.n, 1)) for k, r in by_rg.iterrows()], color="#1e8a52", fmt=lambda v: f"{v:.1f}%")
    else:
        tel = int((h.health_state == "Telemetry Alert").sum()); known = int(h.health_state.str.startswith("Known").sum())
        hero("Reliability & Incidents", "Service health by domain and region, with support as a second monitoring system.", [
            ("Customer-detected cells", f"{cd_cells}", "no internal alert yet", RED if cd_cells else GREEN),
            ("Telemetry alerts", f"{tel}", "error rate ≥ 3x baseline", AMBER if tel else GREEN),
            ("Known incidents", f"{known}", "cells with open incident", AMBER if known else WHITE),
            ("Global frustration", f"{fi:.0f}", "index 0–100", RED if fi > 40 else WHITE)], now)
        panel("Service health matrix", f"Engineering domain × region at {now:%H:%M} UTC")
        health_matrix(D)
        panel("Concurrent streams vs baseline", "Support contact rate as a leading indicator")
        telemetry_chart(D, key="home_t", h=280)
        panel("Recent risky changes", "Config changes, content publishes and rollbacks · last 24 hours")
        st.dataframe(D["changes"][["change_ts", "service", "domain", "change_type", "version", "description", "outcome"]], hide_index=True, use_container_width=True)


def page_alerts(D):
    items = alerts_now(D)
    st.markdown("## Alerts"); st.caption(f"Active at {D['now']:%d %b %H:%M} UTC · {len(items)} open · shared across all personas")
    render_alerts(items)


def page_pulse(D):
    st.markdown("## Platform Pulse"); st.caption("Global heatmap · service health matrix · live telemetry vs consumer frustration")
    a, b = st.columns([1.3, 1])
    with a:
        panel("Global pulse", "Bubble = tickets in last 15 min · colour = selected signal · pink halo = customer-detected")
        geo_map(D, key="pulse_map")
    with b:
        panel("Service health matrix", "Internal telemetry + support signal + known incidents")
        health_matrix(D)
    panel("Concurrent streams vs baseline · consumer frustration", "When streams dip while contacts spike, users are hitting breakage before alerts fire")
    telemetry_chart(D, key="pulse_t")


def issue_split(D):
    now = D["now"]; an = D["anom"]; an = an[an.detected_at > now - timedelta(hours=24)]
    acts = ops().frame("actions"); declared = set(acts.anomaly_id) if len(acts) else set()
    is_known = an.apply(lambda r: (isinstance(r.incident_declared_at, pd.Timestamp) and r.incident_declared_at <= now) or r.anomaly_id in declared, axis=1) if len(an) else pd.Series(dtype=bool)
    return (an[is_known] if len(an) else an), (an[~is_known] if len(an) else an)


def issue_list(title, sub, df, color):
    rows = ""
    for r in df.sort_values("detected_at", ascending=False).itertuples():
        rows += (f"<div class='alertrow' style='align-items:center'><div class='adot' style='background:{color};margin-top:0'></div>"
                 f"<div style='flex:1;font-weight:600;color:#1f2a2e'>{html.escape(str(r.ai_headline))}"
                 f"<div style='font-size:12px;color:#8a8a8a;font-weight:400'>{html.escape(r.issue_signature)} · detected {r.detected_at:%d %b %H:%M} UTC"
                 f"{' · ' + html.escape(str(r.incident_id)) if isinstance(r.incident_id, str) else ''}</div></div>"
                 f"<span class='pill' style='font-weight:700'>{int(r.total_tickets):,} tickets</span></div>")
    if not rows:
        rows = "<div style='color:#8a8a8a;font-size:14px;padding:8px 0'>None right now.</div>"
    st.markdown(f"<div class='panel'><h3>{html.escape(title)} <span class='pill'>{len(df)}</span></h3><div class='sub'>{html.escape(sub)}</div>{rows}</div>", unsafe_allow_html=True)


def page_header(title, why, insights):
    cards = "".join(f"<div class='qc' style='border-left-color:{c}'><span class='ic' style='color:{c}'>{ic}</span><span>{html.escape(t)}</span></div>" for ic, t, c in insights)
    st.markdown(f"<div class='ph'>{html.escape(title)}</div><div class='why'><div class='wl'>WHY THIS MATTERS</div><div class='wt'>{html.escape(why)}</div></div>"
                f"<div class='qi'>QUICK INSIGHTS</div><div class='qgrid'>{cards}</div>", unsafe_allow_html=True)


def pill_tabs(options, key):
    st.markdown(f"""<style>
    .st-key-{key} [data-testid="stButtonGroup"] > div {{gap:12px !important; flex-wrap:wrap;}}
    .st-key-{key} button {{background:#ffffff !important; border:none !important; border-radius:12px !important; padding:10px 22px !important; box-shadow:0 1px 3px #0000000f !important; min-height:46px;}}
    .st-key-{key} button p {{font-size:16px !important; color:#4b5257 !important;}}
    .st-key-{key} button[data-testid="stBaseButton-segmented_controlActive"], .st-key-{key} button[kind="segmented_controlActive"] {{background:#1b3038 !important;}}
    .st-key-{key} button[data-testid="stBaseButton-segmented_controlActive"] p, .st-key-{key} button[kind="segmented_controlActive"] p {{color:#ffffff !important;}}
    </style>""", unsafe_allow_html=True)
    with st.container(key=key):
        v = st.segmented_control("Section", options, default=options[0], key=f"{key}_sel", label_visibility="collapsed")
    return v or options[0]


def page_subscriber(D):
    now = D["now"]; known, ai = issue_split(D)
    live_ai = ai[ai.last_seen >= now - ACTIVE] if len(ai) else ai
    if len(live_ai):
        top = live_ai.sort_values("total_tickets", ascending=False).iloc[0]
        i1 = ("⚠", f"{len(live_ai)} undeclared ticket surge{'s' if len(live_ai) > 1 else ''} live — {top.issue_signature}: {int(top.total_tickets):,} tickets", "#e11d74")
    else:
        i1 = ("✔", f"No undeclared surges — {len(known)} known issue{'s' if len(known) != 1 else ''} in the last 24h ({int(known.total_tickets.sum()) if len(known) else 0:,} tickets)", "#1e8a52")
    pn = D["pay_now"]; rar = D["pay_rar_today"].rar.sum() if len(D["pay_rar_today"]) else 0
    bad = pn[pn.gateway_state != "Healthy"].sort_values("success_rate_15m") if len(pn) else pn
    if len(bad):
        b = bad.iloc[0]
        i2 = ("↘", f"{b.gateway} in {b.country_code} at {b.success_rate_15m*100:.1f}% payment success — ${rar:,.0f} revenue at risk today", "#c2410c")
    else:
        i2 = ("✔", f"All payment gateways healthy — ${rar:,.0f} revenue at risk today", "#1e8a52")
    so = D["social"]
    if len(so):
        neg = so[(so.sentiment == "negative") & (so.topic_domain != "Show hype / fandom")]
        topic = neg.topic_domain.value_counts().index[0] if len(neg) else "none"
        i3 = ("◔", f"{len(neg)/len(so):.0%} of {len(so):,} social posts negative in the last 24h — top topic: {topic}", "#d97706" if len(neg) / len(so) > 0.15 else "#1e8a52")
    else:
        i3 = ("◔", "No social posts in the last 24h", "#94a3b8")
    page_header("Subscriber Experience", "Ticket surges, payment failures and social sentiment — the three places subscribers tell us something is broken", [i1, i2, i3])
    tab = pill_tabs(["Incidents", "Payments", "Social"], "sx_tab")
    if tab == "Incidents":
        banners = ops().frame("banners")
        if len(banners):
            st.warning("📣 Known-issue banners live for agents: " + " | ".join(banners.headline.astype(str).head(3)))
        win = st.segmented_control("Window", ["Last 24 hours", "Last 15 days"], default="Last 24 hours", key="anom_win")
        anomaly_cards(D, window_h=24 if win != "Last 15 days" else 24 * 16)
    elif tab == "Payments":
        panel("Payment gateway health", "15-minute success rate by gateway vs billing tickets · last 24 hours")
        payments_view(D)
    else:
        panel("Social listening", "Reddit and X · AI sentiment and topic per post · last 24 hours")
        social_view(D)


def page_payments(D):
    st.markdown("## Payments & Social Listening"); st.caption("Gateway success vs billing tickets · Reddit and X sentiment · last 24 hours")
    a, b = st.columns([1.1, 1])
    with a:
        payments_view(D)
    with b:
        social_view(D)


def payments_view(D):
    now = D["now"]
    if True:
        cs = sorted(D["pay_now"].country_code.unique()) or ["DE"]
        ctry = st.selectbox("Country", cs, index=cs.index("DE") if "DE" in cs else 0)
        p = pg(f"SELECT ts, gateway, success_rate_15m, country_billing_tickets, revenue_at_risk_usd FROM {T('payment_health_5m')} WHERE country_code = %s AND ts > %s ORDER BY ts", (ctry, now - timedelta(hours=24)))
        latest = D["pay_now"][D["pay_now"].country_code == ctry]
        colmap = {"Healthy": "#1e8a52", "Degraded": "#ea580c", "Critical": "#e11d74"}
        st.markdown("".join(f"<span class='badge' style='background:{colmap[r.gateway_state]}'>{r.gateway}: {r.success_rate_15m*100:.1f}%</span>" for r in latest.itertuples()), unsafe_allow_html=True)
        f = go.Figure()
        for gw_, gdf in p.groupby("gateway"):
            f.add_trace(go.Scatter(x=gdf.ts, y=gdf.success_rate_15m * 100, name=gw_, mode="lines"))
        bt = p.groupby("ts").country_billing_tickets.max().reset_index()
        f.add_trace(go.Bar(x=bt.ts, y=bt.country_billing_tickets, name="Billing tickets", yaxis="y2", marker_color="#f4752166"))
        f.update_layout(yaxis=dict(title="Success % (15m)", range=[60, 101]), yaxis2=dict(title="Tickets", overlaying="y", side="right", showgrid=False))
        st.plotly_chart(style_fig(f, 340), use_container_width=True, config={"displayModeBar": False}, key="payc")
        st.metric(f"Revenue at risk · last 24h · {ctry}", f"${p.revenue_at_risk_usd.sum():,.0f}")


def social_view(D):
    if True:
        so = D["social"]
        neg = so[(so.sentiment == "negative") & (so.topic_domain != "Show hype / fandom")]
        if len(neg):
            nv = neg.groupby(["ts_15m", "topic_domain"]).size().reset_index(name="posts")
            f3 = px.bar(nv, x="ts_15m", y="posts", color="topic_domain")
            f3.update_layout(xaxis=dict(title=None), yaxis=dict(title="Negative posts"))
            st.plotly_chart(style_fig(f3, 200), use_container_width=True, config={"displayModeBar": False}, key="socc")
        colors = {"negative": "#e11d74", "positive": "#1e8a52", "neutral": "#94a3b8", "mixed": "#d4a017"}
        feed = so.sort_values("posted_at", ascending=False).head(14)
        st.markdown("".join(f"<div class='post' style='border-color:{colors.get(r.sentiment, '#ccc')}'><span style='color:#8a8a8a;font-size:.72rem'>{r.posted_at:%d %b %H:%M} · {r.platform} · {r.author_handle} · {r.country_code if isinstance(r.country_code, str) else ''} · {r.sentiment}</span><br>{html.escape(str(r.post_text))}</div>" for r in feed.itertuples()), unsafe_allow_html=True)


def kpi_cards(items):
    st.markdown("<div class='mgrid'>" + "".join(f"<div class='mc'><div class='ml'>{html.escape(str(l))}</div><div class='mv'>{v}</div><div class='ms'>{html.escape(str(s))}</div></div>"
                                              for l, v, s in items) + "</div>", unsafe_allow_html=True)


def _wavg(df, col, w="tickets"):
    d = df.dropna(subset=[col])
    return float((d[col] * d[w]).sum() / d[w].sum()) if len(d) and d[w].sum() else float("nan")


def page_executive(D):
    host = Config().host.rstrip("/"); now = D["now"]; tst = today_stats(D); pal = ["#1c3a3f", "#f47521", "#1e8a52", "#94a3b8"]
    sc = D["score"].sort_values("declared_at", ascending=False); cf = sc[sc.detection_winner == "Customers first"]
    lead = float(cf.detection_lead_minutes.astype(float).mean()) if len(cf) else float("nan")
    kp = D["kpi"].copy(); kp["kpi_date"] = pd.to_datetime(kp.kpi_date); oc = D["outcomes"].copy(); oc["kpi_date"] = pd.to_datetime(oc.kpi_date)
    prev7 = kp[(kp.kpi_date < now.normalize()) & (kp.kpi_date >= now.normalize() - timedelta(days=7))]
    csat7 = _wavg(prev7, "avg_csat")
    rar15 = float(kp.revenue_at_risk_usd.sum()); by_rg = kp.groupby("region_group").revenue_at_risk_usd.sum().sort_values(ascending=False)
    i1 = ("⚠", f"Customers detected {len(cf)} of {len(sc)} incidents before monitoring — {lead:.0f} min earlier on average" if len(cf) else f"Monitoring caught all {len(sc)} incidents first", "#e5322d" if len(cf) else "#1e8a52")
    i2 = ("★", f"CSAT {tst['csat']:.2f} today vs {csat7:.2f} 7-day average" if tst["csat"] == tst["csat"] and csat7 == csat7 else "CSAT not available yet today",
          "#1e8a52" if tst["csat"] == tst["csat"] and csat7 == csat7 and tst["csat"] >= csat7 else "#d97706")
    i3 = ("↘", f"${rar15:,.0f} revenue at risk from payment failures in 15 days — {by_rg.index[0]} {by_rg.iloc[0] / rar15:.0%} of it" if rar15 else "No revenue at risk from payment failures in 15 days", "#c2410c")
    page_header("Executive Overview", "Incident performance, customer experience, revenue impact and long-term trends for Fanimation leadership", [i1, i2, i3])
    tab = pill_tabs(["Incident Performance", "Customer Experience", "Revenue Impact", "Long-term Trends"], "ex_tab")
    if tab == "Incident Performance":
        kpi_cards([("Incidents · 15 days", f"{len(sc)}", f"{int((sc.severity == 'SEV1').sum())} SEV1"),
                   ("Customers detected first", f"{len(cf)}/{len(sc)}", "before internal monitoring"),
                   ("Avg detection lead", "—" if lead != lead else f"{lead:.0f} min", "customers vs monitoring"),
                   ("Est. lost viewing hours", f"{sc.est_lost_viewing_hours.astype(float).sum():,.0f}", "across incidents")])
        d = sc.dropna(subset=["detection_lead_minutes"]).copy(); d["label"] = d.declared_at.dt.strftime("%d %b") + " · " + d.title.str.slice(0, 48)
        f = px.bar(d.sort_values("declared_at"), x="detection_lead_minutes", y="label", color="detection_winner", orientation="h",
                   color_discrete_map={"Customers first": "#e11d74", "Monitoring first": "#1e8a52"})
        f.update_layout(yaxis=dict(title=None), xaxis=dict(title="Minutes customers reported it before monitoring alerted"))
        panel("Detection lead by incident", "How far support signals ran ahead of internal monitoring")
        st.plotly_chart(style_fig(f, 320), use_container_width=True, config={"displayModeBar": False}, key="ex_lead")
        panel("Incident scorecard", "All incidents in the last 15 days")
        st.dataframe(sc[["incident_id", "title", "severity", "region_groups", "customer_detected_at", "first_alert_at", "declared_at", "detection_lead_minutes",
                         "minutes_to_resolve", "tickets_during_incident", "csat_during_incident", "est_lost_viewing_hours", "root_cause"]], use_container_width=True, hide_index=True)
    elif tab == "Customer Experience":
        rc = oc.repeat_contacts.sum() / max(oc.tickets.sum(), 1)
        kpi_cards([("Tickets · 15 days", f"{int(kp.tickets.sum()):,}", f"{int(kp.tickets.sum() / max(kp.kpi_date.nunique(), 1)):,} per day"),
                   ("CSAT · 15 days", f"{_wavg(kp, 'avg_csat'):.2f}", "Target ≥ 4.0"),
                   ("SLA met · 15 days", fmt_pct(_wavg(kp, "sla_met_rate")), "Target ≥ 80%"),
                   ("Repeat contact rate", fmt_pct(rc), "same subscriber within 7 days")])
        a, b = st.columns(2)
        with a:
            panel("Tickets by region", "Daily")
            st.plotly_chart(style_fig(px.bar(kp, x="kpi_date", y="tickets", color="region_group", color_discrete_sequence=pal), 280), use_container_width=True, key="ex_t")
            panel("SLA met rate by region", "First-response SLA by subscriber tier")
            f = px.line(kp, x="kpi_date", y="sla_met_rate", color="region_group", markers=True, color_discrete_sequence=pal); f.update_layout(yaxis=dict(tickformat=".0%"))
            st.plotly_chart(style_fig(f, 280), use_container_width=True, key="ex_sla")
        with b:
            panel("CSAT by region", "Daily average, 1–5")
            st.plotly_chart(style_fig(px.line(kp, x="kpi_date", y="avg_csat", color="region_group", markers=True, color_discrete_sequence=pal), 280), use_container_width=True, key="ex_c")
            panel("Repeat contact rate by region", "Share of tickets from subscribers who contacted us in the previous 7 days")
            f = px.line(oc, x="kpi_date", y="repeat_contact_rate", color="region_group", markers=True, color_discrete_sequence=pal); f.update_layout(yaxis=dict(tickformat=".0%"))
            st.plotly_chart(style_fig(f, 280), use_container_width=True, key="ex_rc")
    elif tab == "Revenue Impact":
        rar_today = D["pay_rar_today"].rar.sum() if len(D["pay_rar_today"]) else 0
        last = kp[kp.kpi_date == kp.kpi_date.max()]
        pay_succ = float(last.payment_success_rate.mean()) if len(last) else float("nan")
        kpi_cards([("Revenue at risk · 15 days", f"${rar15:,.0f}", "failed payments on degraded gateways"),
                   ("Revenue at risk · today", f"${rar_today:,.0f}", f"as of {now:%H:%M} UTC"),
                   ("Payment success · latest day", fmt_pct(pay_succ), "average across regions"),
                   ("MRR at risk (churn)", f"${D['churn'].mrr_at_risk_usd.sum():,.0f}", "rule-based · worked by Support Ops")])
        a, b = st.columns(2)
        with a:
            panel("Revenue at risk by region", "Daily, USD")
            st.plotly_chart(style_fig(px.bar(kp, x="kpi_date", y="revenue_at_risk_usd", color="region_group", color_discrete_sequence=pal), 300), use_container_width=True, key="ex_rar")
        with b:
            panel("Payment success rate by region", "Daily")
            f = px.line(kp, x="kpi_date", y="payment_success_rate", color="region_group", markers=True, color_discrete_sequence=pal); f.update_layout(yaxis=dict(tickformat=".1%"))
            st.plotly_chart(style_fig(f, 300), use_container_width=True, key="ex_ps")
    else:
        st.markdown(f"<div class='panel'><h3>Long-term trends</h3><div class='sub'>36-month history (synthetic) from gold history tables in the lakehouse, queried through the SQL warehouse by the AI/BI dashboard — not served from Lakebase.</div>"
                    f"<a href='{host}/dashboardsv3/{DASH}/published' target='_blank'>Open in AI/BI ↗</a></div>", unsafe_allow_html=True)
        st.caption("If the frame below is blank, a workspace admin must allow dashboard embedding for the app domain (Settings → Security).")
        # Cross-origin iframes cannot auto-size to their content, so size the frame for the tallest dashboard page and let the app page scroll.
        components.iframe(f"{host}/embed/dashboardsv3/{DASH}" + (f"?o={WS_ID}" if WS_ID else ""), height=1550, scrolling=False)


SCORE_HELP = ("Rule-based score over the 14 days before the latest ticket (not an ML model): cancellation intent 25 · watch time drop ≥40% 20 (≥20% 10) · "
              "3+ contacts 20 (2 contacts 10) · repeat contacts 10 each (max 20) · payment failures 15 · hit by a platform incident 15 · low CSAT (≤2) 15 · "
              "SLA missed 10 · reopened ticket 10 · paid renewal due within 14 days 10 · capped at 100. Band: High ≥ 60, Medium ≥ 35, Low otherwise.")
S360_COLS = ("subscriber_id, first_name, last_name, email, country_code, country_name, region_group, preferred_language, tier, monthly_price_usd, billing_gateway, "
             "primary_device, segment, tenure_months, signup_date, renewal_date, days_to_renewal, auto_renew, lifetime_value_usd, crm_source, "
             "watch_hours_prior_30d, watch_hours_last_30d, watch_time_change_pct, churn_risk_score, churn_risk_band, churn_driver, churn_reasons, "
             "recommended_action, mrr_at_risk_usd, tickets_14d, repeat_contacts_14d, low_csat_14d, sla_missed_14d, reopened_14d, "
             "payment_failure_tickets_14d, incident_tickets_14d, incidents_hit, primary_issue_domain, primary_issue_signature, last_contact_at, avg_csat_14d")


def _reasons(v):
    if isinstance(v, list):
        return v
    if isinstance(v, str) and v.startswith("["):
        try:
            return json.loads(v)
        except Exception:
            return []
    return []


def page_customer360(D):
    st.markdown("## Customer 360"); st.caption("Subscriber profile (synthetic Salesforce Customer 360) + support history + rule-based churn risk · served from Lakebase")
    qtxt = st.text_input("Search subscriber", placeholder="Subscriber ID (SUB-…), email or name", key="c360_q")
    if qtxt.strip():
        like = f"%{qtxt.strip()}%"
        res = pg(f"""SELECT subscriber_id, first_name, last_name, email, tier, country_code, churn_risk_score, churn_risk_band FROM {T('subscriber_360')}
                     WHERE subscriber_id = %s OR email ILIKE %s OR (first_name || ' ' || last_name) ILIKE %s ORDER BY churn_risk_score DESC LIMIT 20""", (qtxt.strip().upper(), like, like))
        st.caption(f"{len(res)} match(es)")
    else:
        res = pg(f"""SELECT subscriber_id, first_name, last_name, email, tier, country_code, churn_risk_score, churn_risk_band FROM {T('subscriber_360')}
                     WHERE churn_risk_band = 'High' ORDER BY churn_risk_score DESC, mrr_at_risk_usd DESC LIMIT 20""")
        st.caption("Top 20 High churn-risk subscribers — select a row or search")
    ev = st.dataframe(res, hide_index=True, use_container_width=True, on_select="rerun", selection_mode="single-row", key="c360_tbl", height=260)
    if ev and ev.selection and ev.selection.rows:
        ss.c360_id = res.iloc[ev.selection.rows[0]].subscriber_id
    sid = ss.get("c360_id") or (res.subscriber_id.iloc[0] if len(res) else None)
    if not sid:
        return
    p = pg(f"SELECT {S360_COLS} FROM {T('subscriber_360')} WHERE subscriber_id = %s", (sid,))
    if p.empty:
        st.warning(f"Subscriber {sid} not found."); return
    r = p.iloc[0]
    band_col = {"High": RED, "Medium": AMBER, "Low": GREEN}.get(r.churn_risk_band, WHITE)
    wt = r.watch_time_change_pct
    hero(f"{r.tier} · {r.country_name} · {r.segment}", f"{r.subscriber_id} · {r.email} · source: {r.crm_source}", [
        ("Churn risk", f"{int(r.churn_risk_score)}", f"{r.churn_risk_band} band", band_col),
        ("Top driver", html.escape(str(r.churn_driver or "—")), "highest-weighted signal", WHITE),
        ("Watch time", "—" if wt != wt else f"{wt*100:+.0f}%", f"{r.watch_hours_last_30d:.1f}h last 30d vs {r.watch_hours_prior_30d:.1f}h", RED if wt == wt and wt <= -0.2 else WHITE),
        ("Tickets · 14 days", f"{int(r.tickets_14d)}", f"{int(r.repeat_contacts_14d)} repeat contacts", AMBER if r.tickets_14d >= 2 else WHITE),
        ("MRR at risk", f"${r.mrr_at_risk_usd:,.2f}", f"plan ${r.monthly_price_usd:.2f}/mo", WHITE)], D["now"], title=html.escape(f"{r.first_name} {r.last_name}"))
    c1, c2, c3 = st.columns([1.1, 1, 1.1])
    with c1:
        renew = "—" if pd.isna(r.renewal_date) else f"{pd.Timestamp(r.renewal_date):%d %b %Y} ({int(r.days_to_renewal)} days)"
        rows = [("Tier / price", f"{r.tier} · ${r.monthly_price_usd:.2f}/mo"), ("Billing gateway", r.billing_gateway or "—"), ("Primary device", r.primary_device),
                ("Language", r.preferred_language), ("Region", f"{r.country_name} ({r.region_group})"), ("Tenure", f"{int(r.tenure_months)} months · since {pd.Timestamp(r.signup_date):%b %Y}"),
                ("Renewal", renew), ("Auto-renew", "Yes" if r.auto_renew else "No"), ("Lifetime value", f"${r.lifetime_value_usd:,.2f}")]
        st.markdown("<div class='panel'><h3>Profile</h3><div class='sub'>CRM account</div><div class='kv2'>" + "".join(f"<div>{k}</div><div>{html.escape(str(v))}</div>" for k, v in rows) + "</div></div>", unsafe_allow_html=True)
    with c2:
        g = go.Figure(go.Indicator(mode="gauge+number", value=float(r.churn_risk_score), title={"text": "Churn risk score", "font": {"size": 13}},
                                   gauge={"axis": {"range": [0, 100]}, "bar": {"color": "#1c3a3f"}, "steps": [{"range": [0, 35], "color": "#dcf2e4"}, {"range": [35, 60], "color": "#fdf0cf"}, {"range": [60, 100], "color": "#fbd5e5"}]}))
        st.plotly_chart(style_fig(g, 210, False), use_container_width=True, config={"displayModeBar": False}, key="c360_g")
        chips = "".join(f"<span class='chip'>{html.escape(x)}</span>" for x in _reasons(r.churn_reasons)) or "<span class='pill'>No risk signals</span>"
        st.markdown(f"<div class='panel'><h3>Why at risk</h3><div class='sub' title='{html.escape(SCORE_HELP)}'>Rule-based risk score (not an ML model) ⓘ</div>{chips}"
                    f"<p style='margin-top:10px;color:#2b3437'><b>Recommended action:</b> {html.escape(str(r.recommended_action or 'No action needed'))}</p></div>", unsafe_allow_html=True)
        with st.expander("How the score works"):
            st.caption(SCORE_HELP)
    with c3:
        sig = [("Tickets", r.tickets_14d), ("Repeat contacts", r.repeat_contacts_14d), ("Low CSAT (≤2)", r.low_csat_14d), ("SLA missed", r.sla_missed_14d),
               ("Reopened", r.reopened_14d), ("Payment failures", r.payment_failure_tickets_14d), ("Incident tickets", r.incident_tickets_14d)]
        st.markdown("<div class='panel'><h3>Support signals · 14 days</h3><div class='sub'>Primary issue: " + html.escape(str(r.primary_issue_signature or "—")) + "</div><div class='kv2'>"
                    + "".join(f"<div>{k}</div><div><b>{int(v)}</b></div>" for k, v in sig) + f"<div>Incidents hit</div><div>{html.escape(str(r.incidents_hit or '—'))}</div>"
                    + f"<div>Avg CSAT</div><div>{'—' if pd.isna(r.avg_csat_14d) else f'{r.avg_csat_14d:.1f}'}</div></div></div>", unsafe_allow_html=True)
    hist = pg(f"""SELECT created_at, channel, issue_signature, subject, sla_met, csat_score, reopened, is_repeat_contact, incident_id, anomaly_id
                  FROM {T('subscriber_ticket_history')} WHERE subscriber_id = %s ORDER BY created_at DESC""", (sid,))
    panel("Ticket timeline", f"{len(hist)} tickets · repeat = contacted again within 7 days")
    if len(hist):
        st.dataframe(hist.drop(columns=["anomaly_id"]), hide_index=True, use_container_width=True,
                     column_config={"is_repeat_contact": st.column_config.CheckboxColumn("repeat"), "sla_met": st.column_config.CheckboxColumn("SLA met"), "reopened": st.column_config.CheckboxColumn("reopened")})
        links = hist.dropna(subset=["anomaly_id"]).drop_duplicates("anomaly_id")
        for lr in links.itertuples():
            st.button(f"Open incident brief · {lr.incident_id or lr.anomaly_id} · {lr.issue_signature}", key=f"c360b_{lr.anomaly_id}", on_click=open_brief, args=(lr.anomaly_id,))
    else:
        st.info("No support tickets for this subscriber.")


def page_churn(D):
    st.markdown("## Churn & Retention"); st.caption("Subscribers with potential to churn, grouped by what went wrong · rule-based risk score (not an ML model) · served from Lakebase")
    churn = D["churn"]; oc = D["outcomes"]
    daily = oc.groupby("kpi_date").agg(t=("tickets", "sum"), r=("repeat_contacts", "sum")).reset_index()
    daily["rate"] = daily.r / daily.t
    last = daily.tail(1).rate.iloc[0] if len(daily) else float("nan"); prev7 = daily.iloc[-8:-1].rate.mean() if len(daily) > 1 else float("nan")
    hero("Churn-risk cohorts", "At-risk = High or Medium rule-based churn risk.", [
        ("At-risk subscribers", f"{int(churn.at_risk_subscribers.sum()):,}", "High + Medium", AMBER),
        ("High risk", f"{int(churn.high_risk_subscribers.sum()):,}", "score ≥ 60", RED),
        ("MRR at risk", f"${churn.mrr_at_risk_usd.sum():,.0f}", "monthly plan value of at-risk subscribers", WHITE),
        ("Repeat contact rate", fmt_pct(last), f"latest day · 7-day avg {fmt_pct(prev7)}", RED if last == last and prev7 == prev7 and last > prev7 * 1.2 else WHITE)], D["now"], title="Who might leave, and why")
    c1, c2 = st.columns([1, 1.2])
    with c1:
        cohort = churn.groupby("churn_driver").agg(at_risk=("at_risk_subscribers", "sum"), high=("high_risk_subscribers", "sum"), mrr_at_risk_usd=("mrr_at_risk_usd", "sum"),
                                                   avg_score=("avg_churn_risk_score", "mean")).sort_values("mrr_at_risk_usd", ascending=False).reset_index()
        panel("Cohorts by churn driver", "Select a cohort to list its subscribers")
        ev = st.dataframe(cohort.round(1), hide_index=True, use_container_width=True, on_select="rerun", selection_mode="single-row", key="cohort_tbl")
        if ev and ev.selection and ev.selection.rows:
            ss.churn_driver = cohort.iloc[ev.selection.rows[0]].churn_driver
    with c2:
        f = px.bar(churn.groupby(["churn_driver", "primary_issue_domain"]).mrr_at_risk_usd.sum().reset_index(), y="churn_driver", x="mrr_at_risk_usd", color="primary_issue_domain", orientation="h")
        f.update_layout(yaxis=dict(title=None, categoryorder="total ascending"), xaxis=dict(title="MRR at risk (USD)"))
        panel("MRR at risk by driver and issue faced", "Colour = primary issue domain of the subscriber's recent tickets")
        st.plotly_chart(style_fig(f, 330), use_container_width=True, config={"displayModeBar": False}, key="churn_bar")
    drv = ss.get("churn_driver") or (cohort.churn_driver.iloc[0] if len(cohort) else None)
    if drv:
        with st.expander(f"Breakdown of “{drv}” by issue faced and region", expanded=False):
            sub = churn[churn.churn_driver == drv].pivot_table(index="primary_issue_domain", columns="region_group", values="at_risk_subscribers", aggfunc="sum", fill_value=0)
            st.dataframe(sub, use_container_width=True)
        f1, f2, f3 = st.columns(3)
        reg = f1.multiselect("Region", REGIONS, default=REGIONS, key="ch_reg")
        tiers = f2.multiselect("Tier", ["Free", "Fan", "Mega Fan", "Ultimate Fan"], default=["Fan", "Mega Fan", "Ultimate Fan"], key="ch_tier")
        bands = f3.multiselect("Band", ["High", "Medium"], default=["High", "Medium"], key="ch_band")
        lst = pg(f"""SELECT subscriber_id, first_name, last_name, tier, country_code, churn_risk_score, churn_risk_band, primary_issue_domain, primary_issue_signature,
                       churn_reasons, recommended_action, mrr_at_risk_usd, days_to_renewal
                     FROM {T('subscriber_360')} WHERE churn_driver = %s AND region_group = ANY(%s) AND tier = ANY(%s) AND churn_risk_band = ANY(%s)
                     ORDER BY churn_risk_score DESC, mrr_at_risk_usd DESC LIMIT 200""", (drv, reg or REGIONS, tiers or ["Free"], bands or ["High"])) if _pg_state()["source"] == "Lakebase" else \
            pg(f"""SELECT subscriber_id, first_name, last_name, tier, country_code, churn_risk_score, churn_risk_band, primary_issue_domain, primary_issue_signature,
                       churn_reasons, recommended_action, mrr_at_risk_usd, days_to_renewal FROM {T('subscriber_360')} WHERE churn_driver = %s
                     ORDER BY churn_risk_score DESC LIMIT 200""", (drv,))
        lst["churn_reasons"] = lst.churn_reasons.apply(lambda v: ", ".join(_reasons(v)))
        panel(f"At-risk subscribers · {drv}", f"{len(lst)} shown (top 200 by score) · select a row to open Customer 360" if "customer360" in allowed_pages(ss.persona) else f"{len(lst)} shown (top 200 by score)")
        ev2 = st.dataframe(lst, hide_index=True, use_container_width=True, on_select="rerun", selection_mode="single-row", key="churn_list")
        if ev2 and ev2.selection and ev2.selection.rows and "customer360" in allowed_pages(ss.persona):
            open_customer(lst.iloc[ev2.selection.rows[0]].subscriber_id); st.rerun()
    panel("Repeat contact rate", "Share of tickets from a subscriber who already contacted support in the previous 7 days")
    oc2 = oc.groupby("kpi_date").apply(lambda g: pd.Series({"All tickets": g.repeat_contacts.sum() / max(g.tickets.sum(), 1),
                                                            "Incident-linked tickets": (g.incident_repeat_contact_rate * g.incident_linked_tickets).sum() / g.incident_linked_tickets.sum() if g.incident_linked_tickets.sum() else None})).reset_index()
    f = px.line(oc2.melt(id_vars="kpi_date", var_name="series", value_name="rate"), x="kpi_date", y="rate", color="series", markers=True, color_discrete_sequence=["#1c3a3f", "#e11d74"])
    f.update_layout(yaxis=dict(tickformat=".0%", title=None), xaxis=dict(title=None))
    st.plotly_chart(style_fig(f, 260), use_container_width=True, config={"displayModeBar": False}, key="rcr")
    st.caption("Early days of the base period have little prior history, so their repeat rate is understated.")


def page_genie(D):
    st.markdown("## Genie"); st.caption("Ask questions in plain English over the governed Command Center gold tables · AI/BI Genie")
    c = st.columns(3)
    for i, s_ in enumerate(GENIE_SUGGEST[ss.persona]):
        if c[i].button(s_, key=f"gs_{i}", use_container_width=True):
            ss.genie_pending = s_
    prompt = st.chat_input("Ask Genie about tickets, incidents, payments, churn risk…")
    if prompt:
        ss.genie_pending = prompt
    if ss.get("genie_pending"):
        qn = ss.pop("genie_pending")
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


def page_brief(D):
    st.markdown("## Incident Brief"); st.caption("AI-generated brief for one incident (ai_query with Claude Sonnet 4.6 on the ticket cluster, generated once per anomaly)")
    an = D["anom"].sort_values("detected_at", ascending=False).reset_index(drop=True); sc = D["score"].set_index("incident_id")
    if an.empty:
        st.info("No incidents found."); return
    lab = {r.anomaly_id: f"{r.detected_at:%a %d %b %H:%M} · {r.issue_signature} — {r.incident_id or 'undeclared'}" for r in an.itertuples()}
    id_of = {v: k for k, v in lab.items()}; options = [NO_INC] + list(lab.values())
    if ss.pop("brief_sync", False) or "brief_pick" not in ss:
        ss.brief_pick = lab.get(ss.get("brief_id"), NO_INC)
    if ss.brief_pick not in options:
        ss.brief_pick = NO_INC

    def on_pick():
        ss.brief_id = id_of.get(ss.brief_pick)
    c1, c2 = st.columns([4, 1])
    c1.selectbox("Incident", options, key="brief_pick", on_change=on_pick)
    c2.metric("Incidents detected", len(an))
    sel = id_of.get(ss.brief_pick)
    if sel is None:
        st.info("No incident selected. Pick one above (or open one from a surge card or a customer's timeline).")
        ov = an[["detected_at", "issue_signature", "incident_id", "ai_headline", "top_countries", "total_tickets", "minutes_detected_before_incident"]].copy()
        ov.columns = ["Detected (UTC)", "Signature", "Incident", "Headline", "Top countries", "Tickets", "Minutes before declared"]
        st.dataframe(ov, hide_index=True, use_container_width=True); return
    a = an[an.anomaly_id == sel].iloc[0].to_dict()
    actions = a.get("ai_recommended_actions") or []
    inc = a.get("incident_id"); s_ = sc.loc[inc] if isinstance(inc, str) and inc in sc.index else None
    impact = (f"<span class='pill'>{int(s_['tickets_during_incident']):,} tickets</span><span class='pill'>{float(s_['est_lost_viewing_hours']):,.0f} lost viewing hours</span>"
              f"<span class='pill'>${float(s_['revenue_at_risk_usd']):,.0f} revenue at risk</span><span class='pill'>customers first by {float(s_['detection_lead_minutes']):.0f} min</span>") if s_ is not None else ""
    st.markdown(f"""<div class='panel'><h3>{html.escape(str(a['ai_headline']))}</h3>
      <div class='sub'>{html.escape(a['issue_signature'])} · detected {a['detected_at']:%d %b %H:%M} UTC · {html.escape(str(inc) if isinstance(inc, str) else 'not yet declared')}</div>
      <p style='color:#2b3437'>{html.escape(str(a['ai_summary']))}</p>
      <p style='color:#2b3437'><b>Likely root cause:</b> {html.escape(str(a['ai_likely_root_cause']))}</p>
      <p style='color:#2b3437'><b>Recommended actions:</b> {' · '.join(html.escape(str(x)) for x in actions)}</p>
      <p style='color:#2b3437'><b>Customer macro:</b> <i>{html.escape(str(a['ai_customer_macro']))}</i></p>
      <p style='color:#2b3437'><b>Where:</b> {html.escape(str(a['top_countries']))} &nbsp;·&nbsp; <b>Devices:</b> {html.escape(str(a['top_devices']))} &nbsp;·&nbsp; <b>Suspected change:</b> {html.escape(str(a.get('suspected_change') or 'none found'))}</p>{impact}</div>""", unsafe_allow_html=True)
    samples = a.get("sample_tickets") or []
    if samples:
        with st.expander(f"Sample customer tickets ({len(samples)})"):
            for s_t in samples:
                st.markdown(f"- {s_t}")
    sig = pg(f"SELECT ts_15m, tickets_15m, baseline_15m, is_anomalous FROM {T('signature_trends_15m')} WHERE issue_signature = %s AND ts_15m BETWEEN %s AND %s ORDER BY ts_15m",
             (a["issue_signature"], a["first_seen"] - timedelta(hours=3), a["last_seen"] + timedelta(hours=2)))
    if len(sig):
        f = go.Figure(go.Bar(x=sig.ts_15m, y=sig.tickets_15m, name="Tickets / 15 min", marker_color=["#e11d74" if v else "#b8b4aa" for v in sig.is_anomalous]))
        f.add_trace(go.Scatter(x=sig.ts_15m, y=sig.baseline_15m.astype(float), name="Baseline", line=dict(color="#1c3a3f", dash="dot")))
        if isinstance(a.get("incident_declared_at"), pd.Timestamp):
            f.add_vline(x=a["incident_declared_at"], line=dict(color="#d97706", width=2))
            f.add_annotation(x=a["incident_declared_at"], y=0.99, yref="paper", yanchor="top", xanchor="left", text=" Incident declared", showarrow=False, font=dict(color="#d97706", size=11))
        f.add_vline(x=a["detected_at"], line=dict(color="#e11d74", width=2, dash="dash"))
        f.add_annotation(x=a["detected_at"], y=0.88, yref="paper", yanchor="top", xanchor="right", text="Surge detected ", showarrow=False, font=dict(color="#e11d74", size=11))
        panel("Ticket surge timeline", "15-minute ticket volume for this signature vs baseline")
        st.plotly_chart(style_fig(f, 260), use_container_width=True, config={"displayModeBar": False}, key="brief_sig")


def page_console(D):
    o = ops()
    st.markdown("## Ops Console"); st.caption("Operational state written back by this app to Lakebase (Postgres) · schema command_center_ops")
    st.markdown(f"Status: {'🟢 connected to Lakebase support-command-center / production' if o.ok else '🟡 session-only (Lakebase unavailable)'}")
    if o.err and not o.ok:
        st.caption(o.err)
    for k, t in [("actions", "Incident actions"), ("banners", "Known-issue banners (live)"), ("acks", "Alert acknowledgements")]:
        st.markdown(f"#### {t}"); st.dataframe(o.frame(k), use_container_width=True, hide_index=True)


def page_about(D):
    st.markdown("## About the Support Command Center")
    st.markdown("""<div class='hero'><div class='eyebrow'>Why this exists</div><h1 style='font-size:26px'>Customers are the fastest monitoring system you have.</h1>
    <div class='lead'>When a server quietly drops sessions or a subtitle pack fails to publish, subscribers notice before internal alerts fire. The Command Center
    combines support tickets, subscriber accounts, product telemetry, payments and social posts in one governed place, so Fanimation can see breakage from the fan's
    point of view, act on it, and see which customers are at risk because of it.</div></div>""", unsafe_allow_html=True)
    c = st.columns(3)
    blocks = [("Outcomes we measure", ["Detection lead time: minutes customers reported an issue before monitoring alerted", "Customer-Detected minutes by region and domain",
                                       "SLA met rate and CSAT, including during incidents", "Revenue at risk from payment-gateway failures", "Estimated lost viewing hours per incident",
                                       "Repeat contact rate (same subscriber within 7 days)", "At-risk subscribers and MRR at risk (rule-based churn risk)"]),
              ("Who uses it", [f"<b>{p['label']}</b> — {p['about']}" for p in PERSONAS.values()]),
              ("What it does", ["Global heatmap and service health matrix (known vs customer-detected)", "Streams vs release-aware baseline with contact-rate divergence",
                                "Ticket-surge detection with AI incident summaries, root cause and customer macros", "Declare incidents and publish known-issue banners (Lakebase write-back)",
                                "Customer 360 and churn-risk cohorts grouped by what went wrong", "Payment gateway health and social listening with AI sentiment", "Genie natural-language Q&A and an executive AI/BI dashboard"])]
    for i, (t, items) in enumerate(blocks):
        c[i].markdown(f"<div class='panel'><h3>{t}</h3><ul>" + "".join(f"<li style='margin:6px 0;color:#2b3437'>{x}</li>" for x in items) + "</ul></div>", unsafe_allow_html=True)
    st.markdown("""<div class='panel'><h3>Demo storyline</h3><div class='sub'>Base period 13–27 Oct 2026 (UTC) plus live events you generate</div>
    <ul style='color:#2b3437'>
    <li><b>27 Oct 09:00</b> APAC login failures (AUTH-503) after an auth token-cache change — monitoring catches it in 4 minutes.</li>
    <li><b>27 Oct 14:00</b> <i>Blade of the Crimson Moon</i> finale simulcast — streams spike, recognised as benign.</li>
    <li><b>27 Oct 15:36</b> Android TV <b>Error 1002</b> in Brazil / Mexico / Argentina after a player SDK deploy — Customer-Detected ~45 minutes before declaration.</li>
    <li><b>27 Oct 18:00</b> <i>Spirit Ronin</i> E3 ships without Spanish subtitles — only support sees it.</li>
    <li><b>27 Oct 20:00</b> Adyen EU endpoint migration — German and Austrian payments fail, revenue at risk climbs.</li>
    <li><b>Live</b> — run notebook <code>04_live_event_generator</code> with a scenario; the streaming pipeline and Lakebase sync update this app.</li></ul>
    <div class='sub'>All data is synthetic. The churn risk score is rule-based, not a trained model.</div></div>""", unsafe_allow_html=True)


# ----------------------------------------------------------------------------- architecture diagrams
def nd(kind, title, sub="", items=None, ty=None):
    li = "<ul>" + "".join(f"<li>{html.escape(x)}</li>" for x in items) + "</ul>" if items else ""
    tyh = f"<div class='ty'>{html.escape(ty)}</div>" if ty else ""
    return f"<div class='nd {kind}'>{tyh}<div class='t'>{html.escape(title)}</div><div class='s'>{sub}</div>{li}</div>"


def hdr(t, s):
    return f"<div class='hdr'><b>{t}</b><span>{s}</span></div>"


LEGEND = ("<div class='legend'><span><i style='background:#3b82f6'></i>Person / persona</span><span><i style='background:#ef4444'></i>Platform capability</span>"
          "<span><i style='background:#c9a227'></i>Governed data</span><span><i style='background:#14b8a6'></i>Views / metrics</span><span><i style='background:#8b5cf6'></i>AI &amp; serving</span>"
          "<span><i style='background:#22c55e'></i>Outcome</span><span><i style='background:#94a3b8'></i>Rule-based analytics (not AI)</span>"
          "<span><i style='border:2px dashed #f2a0a0;background:#fdf0ee'></i>Unity Catalog perimeter</span></div>")


def arrow(label, dash=False, green=False, left=False, right=False):
    """Horizontal connector. left/right brackets collect (fan-in) or distribute (fan-out) across a stacked column."""
    ml, mr = (11 if left else 4), (15 if right else 8)
    brk = ("<div class='brk l'></div>" if left else "") + ("<div class='brk r'></div>" if right else "")
    cls = "arw" + (" dash" if dash else "") + (" green" if green else "")
    return f"{brk}<div class='{cls}' style='margin-left:{ml}px;margin-right:{mr}px'><span>{html.escape(label)}</span></div>"


def varrow(label):
    return f"<div class='varw'><span>{html.escape(label)}</span></div>"


def render_dg(headers, bodies, conns, widths, rows=1, uc=None, gap=84):
    """CSS-grid diagram: header row + `rows` body rows; connector columns sit in the same grid rows so arrows align with cards.
    bodies[i] = list of html per row (or a single html string when rows == 1); conns[i] = list per row or one string spanning all rows.
    uc = (first_col, last_col, label, footer) draws the Unity Catalog perimeter behind those columns."""
    tmpl = " ".join(f"minmax(0,{w}fr)" + (f" {gap}px" if i < len(widths) - 1 else "") for i, w in enumerate(widths))
    out = [f"<div class='dg' style='grid-template-columns:{tmpl};grid-template-rows:auto repeat({rows}, auto)'>"]
    for i, (t, s) in enumerate(headers):
        out.append(f"<div class='hdr' style='grid-column:{2*i+1};grid-row:1'><b>{t}</b><span>{s}</span></div>")
    if uc:
        a, b, lab, foot = uc
        out.append(f"<div class='ucbox' style='grid-column:{2*a+1} / {2*b+2};grid-row:2 / {rows+2}'><div class='uct'>{lab}</div><div class='ucf'>{foot}</div></div>")
    edge = lambda r: (" first" if r == 0 else "") + (" last" if r == rows - 1 else "")   # noqa: E731
    for i, col in enumerate(bodies):
        cells = col if isinstance(col, list) else [col]
        mode = "fill" if rows > 1 else "stack"
        for r, c in enumerate(cells):
            out.append(f"<div class='cell {mode}{edge(r)}' style='grid-column:{2*i+1};grid-row:{r+2}'>{c}</div>")
    for i, cn in enumerate(conns):
        if isinstance(cn, list):
            for r, c in enumerate(cn):
                out.append(f"<div class='cn{edge(r)}' style='grid-column:{2*i+2};grid-row:{r+2}'>{c}</div>")
        else:
            out.append(f"<div class='cn first last' style='grid-column:{2*i+2};grid-row:2 / {rows+2}'>{cn}</div>")
    out.append("</div>")
    st.markdown("".join(out) + LEGEND, unsafe_allow_html=True)


def diagram_business():
    outcomes = {
        "exec": "One view of subscriber impact: detection lead time vs monitoring, CSAT / SLA, revenue at risk from payment failures, and 36-month trends in the AI/BI dashboard.",
        "support": "Catch ticket surges early, publish known-issue banners and AI customer macros, see which customers are at risk and why, and track the repeat contact rate.",
        "eng": "Support as a second monitoring signal: customer-detected cells, telemetry vs contact-rate divergence, deploy / config-change correlation and an AI root-cause summary.",
    }
    ks = list(PERSONAS)
    left = [nd("persona", PERSONAS[k]["label"], PERSONAS[k]["tag"], ty="persona") for k in ks]
    mid = [nd("cap", "Daily workflow", PERSONAS[k]["tag"], [PAGE_META[x][0] for x in ["home"] + PERSONAS[k]["dashboards"] + PERSONAS[k]["ai"]] + ["Alerts (all personas)"]) for k in ks]
    right = [nd("out", "Business value", html.escape(outcomes[k])) for k in ks]
    render_dg([("WHO DOES THE WORK", "Personas"), ("WHAT THEY DO IN THE COMMAND CENTER", "Persona-specific navigation"), ("OUTCOMES", "Measured in the data")],
              [left, mid, right], [[arrow("works in") for _ in ks], [arrow("delivers", dash=True, green=True) for _ in ks]],
              widths=[0.9, 1.2, 1.1], rows=3, uc=(1, 1, "UNITY CATALOG GOVERNANCE", "one governed copy · one security model · audit"))


def diagram_platform():
    src = "".join(nd("data", t, s, ty="source") for t, s in [
        ("Support case tickets", "multilingual tickets: EN / ES / PT / DE / FR"), ("Subscriber accounts", "Salesforce Customer 360 (synthetic) + ticket → account links"),
        ("Product telemetry", "concurrent streams, service metrics by domain"), ("Payment gateways", "attempts, declines, gateway errors"),
        ("Social posts", "Reddit and X"), ("Deploys, changes & incidents", "change events, declared incidents"), ("Simulcast release calendar", "expected traffic uplift"),
        ("KPI & churn history", "36 months, standalone gold_hist_* tables (not in the pipeline)")]) + \
        "<div class='s' style='font-size:11px;color:#8a8a8a'>All synthetic · generated by notebooks 01, 05 and 06; live events appended by 04</div>"
    lake = nd("cap", "Lakeflow Spark Declarative Pipeline", "streaming, incremental · serverless", ["Bronze: streaming tables + data-quality expectations",
        "Silver: ticket issue signatures, release-aware stream baselines, AI-enriched social posts", "Gold: materialized views"]) + varrow("aggregate") + \
        nd("view", "Gold tables", "service health · heatmap · frustration · ticket anomalies · payment health · incident scorecard · Subscriber 360 & churn risk · customer outcomes")
    intel = nd("ai", "AI Functions", "ai_query (Claude Sonnet 4.6) · ai_analyze_sentiment · ai_classify — run inside the pipeline") + \
        nd("ai", "AI/BI Genie", "natural language → governed SQL") + nd("ai", "AI/BI dashboard", "36-month trends, YoY and churn history via the SQL warehouse · embedded in Executive Overview") + \
        nd("ai", "Lakebase (Postgres)", "15 synced serving tables · app write-back: incident actions, known-issue banners, alert acks")
    app = nd("persona", "Support Command Center app", "Streamlit · SSO · service principal", ty="app") + \
        nd("persona", "Persona views", "C-Suite · Support Ops · Engineering / SRE") + nd("persona", "Customer 360 & churn", "millisecond lookups from Lakebase") + \
        nd("persona", "Actions", "declare incident · publish banner · acknowledge alert → Lakebase")
    render_dg([("SOURCES", "Synthetic operational feeds"), ("LAKEHOUSE", "Unity Catalog-governed"), ("INTELLIGENCE & SERVING", "Reads the governed layer"), ("APPLICATION", "Databricks Apps")],
              [src, lake, intel, app], [arrow("stream", left=True), arrow("serve · sync", right=True), arrow("read · ask · embed", left=True, right=True)],
              widths=[1, 1.15, 1.15, 1], uc=(1, 2, "UNITY CATALOG — ONE GOVERNANCE BOUNDARY", "one governed copy · one security model · audit"))


def diagram_ai():
    sem = nd("view", "Gold tables", "rich table and column descriptions in Unity Catalog") + \
        nd("view", "Genie instructions", "business definitions (Customer-Detected, detection lead, at-risk) + example SQL")
    surf = nd("ai", "AI/BI Genie", "natural language → SQL → governed answers") + \
        nd("ai", "Claude Sonnet 4.6 via ai_query", "incident headline, summary, likely root cause, recommended actions, customer macro — generated once per new anomaly", ty="Anthropic") + \
        nd("ai", "AI Functions", "ai_analyze_sentiment + ai_classify: sentiment and topic for every new social post") + \
        nd("rule", "Rule-based analytics (not AI)", "ticket-surge detection (z-score vs hour-of-day baseline) · health states · release-aware baselines · churn risk score")
    use = {"exec": ["Genie: detection lead, revenue at risk, CSAT vs last week", "Incident Brief (Claude)"],
           "support": ["AI surge cards + customer macro (Claude)", "Social sentiment (AI Functions)", "Genie: what drives spikes, churn by issue", "Churn reasons (rule-based)"],
           "eng": ["Incident Brief root cause + actions (Claude)", "Genie: why a region turned red, deploys before incidents", "Suspected-change correlation (rule-based)"]}
    cons = "".join(nd("persona", PERSONAS[k]["label"], PERSONAS[k]["tag"], use[k], ty="persona") for k in PERSONAS)
    render_dg([("SEMANTIC LAYER", "Governed, shared"), ("AI & SERVING SURFACES", "Foundation Model API · Genie"), ("CONSUMERS", "Who asks")],
              [sem, surf, cons], [arrow("grounds", right=True), arrow("answers · briefs", left=True, right=True)],
              widths=[0.9, 1.2, 1.1], uc=(0, 1, "UNITY CATALOG SECURITY BOUNDARY", "governed by Unity Catalog permissions"))
    st.caption("No agents, trained ML models or other LLMs are used in this demo.")


def page_architecture(D):
    st.markdown("## Architecture"); st.caption("Built on the Databricks Data Intelligence Platform · only components that exist in this demo are shown")
    t1, t2, t3 = st.tabs(["Business Architecture", "Platform & Data Flow", "AI & Serving"])
    with t1:
        st.caption("EXEC · BUSINESS — who does the work, what they do, and the outcomes")
        diagram_business()
    with t2:
        st.caption("PLATFORM · DATA FLOW — for platform architects and data engineers")
        diagram_platform()
    with t3:
        st.caption("AI · SERVING — which AI features each persona uses")
        diagram_ai()


PAGES = {"home": page_home, "alerts": page_alerts, "pulse": page_pulse, "subscriber": page_subscriber, "customer360": page_customer360, "churn": page_churn,
         "payments": page_payments, "executive": page_executive, "genie": page_genie, "brief": page_brief, "console": page_console, "about": page_about, "architecture": page_architecture}

# ----------------------------------------------------------------------------- main
try:
    now = data_now()
    ver = data_version()
    D = load(now, ver)
    D["now"] = now
    ss.seen_now = now; ss.seen_ver = ver
except Exception as e:
    st.error(f"Could not load data from Lakebase or the SQL warehouse: {e}"); st.stop()

if ss.page not in allowed_pages(ss.persona):
    ss.page = "home"
sidebar(D)
topbar(D)
live_watcher()
try:
    PAGES.get(ss.page, page_home)(D)
except Exception as e:
    st.error(f"{ss.page} failed: {type(e).__name__}: {e}"); st.exception(e)
