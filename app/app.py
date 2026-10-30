"""Fanimation Support Command Center - Databricks App (Streamlit).

Support tickets as a real-time signal of platform health. Replays the scripted demo day 2026-10-27 (UTC)
from gold tables in Unity Catalog; incident actions are written to Lakebase; Genie side panel for Q&A.
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

st.set_page_config(page_title="Fanimation Support Command Center", page_icon="🛰️", layout="wide")

WH = os.getenv("WAREHOUSE_ID", "13bf44dc52395b21")
GENIE = os.getenv("GENIE_SPACE_ID", "01f1d226b70f1202b656c554ffc9bd38")
DASH = os.getenv("DASHBOARD_ID", "01f1d2273e931b0e95acaa9a67663fd7")
S = os.getenv("UC_SCHEMA", "classic_stable_l0m1dn_catalog.support_command_center")
LB_EP = os.getenv("LAKEBASE_ENDPOINT") or os.getenv("LAKEBASE_ENDPOINT_PATH", "projects/support-command-center/branches/production/endpoints/primary")
LB_DB = os.getenv("PGDATABASE") or os.getenv("LAKEBASE_DATABASE", "databricks_postgres")
WS_ID = os.getenv("WORKSPACE_ID", "")
DAY = datetime(2026, 10, 27)
DOMAINS = ["Authentication", "Billing & Payments", "Playback & CDN", "Content Ingestion",
           "Subtitles & Localization", "Device Apps", "Search & Discovery"]
REGIONS = ["NA", "LATAM", "EMEA", "APAC"]
STATE_COLORS = {"Healthy": "#1f7a4d", "Watch": "#6b7d2a", "Recovering": "#2563eb", "Telemetry Alert": "#ea580c",
                "Known Incident": "#d97706", "Known (declared in app)": "#d97706", "Customer-Detected": "#e11d74"}
JUMPS = [("09:00 APAC login", "09:00"), ("14:00 Simulcast finale", "14:00"), ("15:40 Error 1002 Brazil", "15:40"),
         ("18:10 Spirit Ronin subs", "18:10"), ("20:05 Adyen DE", "20:05")]

# ----------------------------------------------------------------------------- styling
st.markdown("""
<style>
.stApp {background: radial-gradient(circle at 20% 0%, #14122b 0%, #0b0b14 60%);}
h1, h2, h3, h4 {color: #f5f5f7 !important;}
.cr-title {font-size: 1.6rem; font-weight: 800; letter-spacing: .5px;}
.cr-title span {color: #f47521;}
.kpi {background: #15152a; border: 1px solid #2a2a4a; border-radius: 10px; padding: 10px 14px;}
.kpi .v {font-size: 1.6rem; font-weight: 800; color: #fff;}
.kpi .l {font-size: .75rem; color: #9ca3af; text-transform: uppercase; letter-spacing: .6px;}
.kpi.alert {border-color: #e11d74; box-shadow: 0 0 14px #e11d7466;}
.matrix {display: grid; grid-template-columns: 190px repeat(4, 1fr); gap: 6px;}
.mh {color: #9ca3af; font-size: .75rem; text-transform: uppercase; text-align: center; padding: 4px;}
.md {color: #e5e7eb; font-size: .85rem; padding: 10px 6px; font-weight: 600;}
.cell {border-radius: 8px; padding: 8px; min-height: 58px; color: #fff; font-size: .72rem; line-height: 1.25;}
.cell b {font-size: .78rem; display: block;}
.pulse {animation: pulse 1.2s infinite; border: 2px solid #ff4fa3;}
@keyframes pulse {0% {box-shadow: 0 0 0 0 #e11d74aa;} 70% {box-shadow: 0 0 0 12px #e11d7400;} 100% {box-shadow: 0 0 0 0 #e11d7400;}}
.card {background: #15152a; border: 1px solid #2a2a4a; border-radius: 10px; padding: 12px 14px; margin-bottom: 10px;}
.card.hot {border-color: #e11d74; box-shadow: 0 0 12px #e11d7455;}
.badge {display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: .7rem; font-weight: 700; margin-right: 6px;}
.post {border-left: 3px solid #555; padding: 4px 10px; margin: 4px 0; font-size: .82rem; color: #e5e7eb;}
.clock {font-size: 1.9rem; font-weight: 800; color: #f47521; font-variant-numeric: tabular-nums;}
</style>""", unsafe_allow_html=True)


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
    try:
        with sql_conn().cursor() as c:
            c.execute(query)
            df = c.fetchall_arrow().to_pandas()
    except Exception:
        sql_conn.clear()
        with sql_conn().cursor() as c:
            c.execute(query)
            df = c.fetchall_arrow().to_pandas()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            s = pd.to_datetime(df[col])
            df[col] = s.dt.tz_convert(None) if s.dt.tz is not None else s
    return df


@st.cache_data(ttl=3600, show_spinner="Loading demo-day telemetry...")
def load_day():
    d0, d1 = "2026-10-27", "2026-10-28"
    data = {
        "heat": q(f"SELECT * FROM {S}.gold_region_heatmap_5m WHERE ts >= '{d0}' AND ts < '{d1}'"),
        "health": q(f"SELECT * FROM {S}.gold_service_health_region_5m WHERE ts >= '{d0}' AND ts < '{d1}'"),
        "health_c": q(f"SELECT ts, country_code, domain, health_state, ticket_ratio, error_ratio, tickets_15m FROM {S}.gold_service_health_country_5m WHERE ts >= '{d0}' AND ts < '{d1}' AND health_state NOT IN ('Healthy')"),
        "frus": q(f"SELECT * FROM {S}.gold_frustration_index_5m WHERE ts >= '{d0}' AND ts < '{d1}'"),
        "anom": q(f"SELECT * EXCEPT (sample_tickets), to_json(ai_recommended_actions) actions_json FROM {S}.gold_ticket_anomalies WHERE first_seen >= '{d0}' AND first_seen < '{d1}'"),
        "sig": q(f"SELECT * FROM {S}.gold_signature_trends_15m WHERE ts_15m >= '{d0}' AND ts_15m < '{d1}'"),
        "pay": q(f"SELECT * FROM {S}.gold_payment_health_5m WHERE ts >= '{d0}' AND ts < '{d1}'"),
        "social": q(f"SELECT posted_at, platform, author_handle, country_code, region_group, post_text, engagement, sentiment, topic_domain, ts_15m FROM {S}.gold_social_posts WHERE posted_at >= '{d0}' AND posted_at < '{d1}'"),
        "score": q(f"SELECT * FROM {S}.gold_incident_scorecard"),
        "kpi": q(f"SELECT * FROM {S}.gold_exec_daily_kpis"),
        "changes": q(f"SELECT * FROM {S}.change_events WHERE change_ts >= '{d0}' AND change_ts < '{d1}' AND change_type <> 'deployment' OR (change_ts >= '{d0}' AND change_ts < '{d1}' AND outcome <> 'success')"),
    }
    return data


# ----------------------------------------------------------------------------- Lakebase (ops state)
class Ops:
    """Incident actions / banners / acks in Lakebase; falls back to session memory if Lakebase is unavailable."""
    DDL = [
        "CREATE SCHEMA IF NOT EXISTS command_center_ops",
        """CREATE TABLE IF NOT EXISTS command_center_ops.incident_actions (id BIGSERIAL PRIMARY KEY, anomaly_id TEXT, issue_signature TEXT,
           domain TEXT, region_groups TEXT, action TEXT, actor TEXT, note TEXT, demo_clock TIMESTAMP, created_at TIMESTAMPTZ DEFAULT now())""",
        """CREATE TABLE IF NOT EXISTS command_center_ops.known_issue_banners (anomaly_id TEXT PRIMARY KEY, headline TEXT, macro TEXT,
           active BOOLEAN DEFAULT TRUE, created_at TIMESTAMPTZ DEFAULT now())""",
        """CREATE TABLE IF NOT EXISTS command_center_ops.alert_acks (id BIGSERIAL PRIMARY KEY, region_group TEXT, domain TEXT, health_state TEXT,
           actor TEXT, demo_clock TIMESTAMP, created_at TIMESTAMPTZ DEFAULT now())""",
        """CREATE TABLE IF NOT EXISTS command_center_ops.demo_state (actor TEXT PRIMARY KEY, demo_clock TIMESTAMP, updated_at TIMESTAMPTZ DEFAULT now())""",
    ]

    def __init__(self):
        self.ok, self.err = False, None
        try:
            with self.conn() as c:
                for d in self.DDL:
                    c.execute(d)
                c.commit()
            self.ok = True
        except Exception as e:  # noqa
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

    def declare(self, a, actor, clock, note):
        row = dict(anomaly_id=a["anomaly_id"], issue_signature=a["issue_signature"], domain=a["issue_domain"],
                   region_groups=a["region_groups"], action="declare_incident", actor=actor, note=note, demo_clock=clock)
        if self.ok:
            try:
                with self.conn() as c:
                    c.execute("""INSERT INTO command_center_ops.incident_actions (anomaly_id, issue_signature, domain, region_groups, action, actor, note, demo_clock)
                                 VALUES (%(anomaly_id)s,%(issue_signature)s,%(domain)s,%(region_groups)s,%(action)s,%(actor)s,%(note)s,%(demo_clock)s)""", row)
                    c.execute("""INSERT INTO command_center_ops.known_issue_banners (anomaly_id, headline, macro, active) VALUES (%s,%s,%s,TRUE)
                                 ON CONFLICT (anomaly_id) DO UPDATE SET active=TRUE, macro=EXCLUDED.macro""", (a["anomaly_id"], a["ai_headline"], a["ai_customer_macro"]))
                    c.commit()
                return
            except Exception as e:
                self.err = str(e)[:300]
        self._mem("actions").append(row)
        self._mem("banners").append(dict(anomaly_id=a["anomaly_id"], headline=a["ai_headline"], macro=a["ai_customer_macro"], active=True))

    def ack(self, rg, dom, state, actor, clock):
        if self.ok:
            try:
                with self.conn() as c:
                    c.execute("INSERT INTO command_center_ops.alert_acks (region_group, domain, health_state, actor, demo_clock) VALUES (%s,%s,%s,%s,%s)",
                              (rg, dom, state, actor, clock))
                    c.commit()
                return
            except Exception as e:
                self.err = str(e)[:300]
        self._mem("acks").append(dict(region_group=rg, domain=dom, health_state=state, actor=actor))

    def declared(self) -> pd.DataFrame:
        if self.ok:
            try:
                with self.conn() as c:
                    cur = c.execute("SELECT DISTINCT ON (anomaly_id) anomaly_id, domain, region_groups, actor, created_at FROM command_center_ops.incident_actions WHERE action='declare_incident' ORDER BY anomaly_id, created_at DESC")
                    rows = cur.fetchall()
                return pd.DataFrame(rows, columns=["anomaly_id", "domain", "region_groups", "actor", "created_at"])
            except Exception as e:
                self.err = str(e)[:300]
        return pd.DataFrame(self._mem("actions"), columns=["anomaly_id", "domain", "region_groups", "actor", "note"]) if self._mem("actions") else pd.DataFrame(columns=["anomaly_id", "domain", "region_groups"])

    def banners(self) -> pd.DataFrame:
        if self.ok:
            try:
                with self.conn() as c:
                    rows = c.execute("SELECT anomaly_id, headline, macro, created_at FROM command_center_ops.known_issue_banners WHERE active ORDER BY created_at DESC").fetchall()
                return pd.DataFrame(rows, columns=["anomaly_id", "headline", "macro", "created_at"])
            except Exception as e:
                self.err = str(e)[:300]
        return pd.DataFrame(self._mem("banners"))

    def reset(self):
        if self.ok:
            try:
                with self.conn() as c:
                    c.execute("UPDATE command_center_ops.known_issue_banners SET active=FALSE")
                    c.execute("DELETE FROM command_center_ops.incident_actions WHERE action='declare_incident'")
                    c.commit()
            except Exception as e:
                self.err = str(e)[:300]
        for k in ["actions", "banners", "acks"]:
            st.session_state[f"ops_{k}"] = []


@st.cache_resource
def ops():
    return Ops()


def actor():
    try:
        return st.context.headers.get("X-Forwarded-Email") or "demo-user"
    except Exception:
        return "demo-user"


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


def genie_panel():
    with st.sidebar:
        st.markdown("### 🧞 Ask Genie")
        st.caption("Natural-language questions over the Command Center gold tables.")
        sugg = ["Why did Brazil turn red at 15:45 UTC?", "Which incidents were detected by customers before monitoring?",
                "What did social media say about Spirit Ronin episode 3?", "Show payment success in Germany by gateway today"]
        for i, s_ in enumerate(sugg):
            if st.button(s_, key=f"sg{i}", use_container_width=True):
                st.session_state["genie_pending"] = s_
        with st.form("genie_form", clear_on_submit=True):
            qtxt = st.text_area("Your question", height=80, placeholder="e.g. Which region had the most customer-detected minutes today?")
            if st.form_submit_button("Ask", use_container_width=True) and qtxt.strip():
                st.session_state["genie_pending"] = qtxt.strip()
        if st.session_state.get("genie_pending"):
            question = st.session_state.pop("genie_pending")
            st.session_state["playing"] = False
            with st.spinner("Genie is thinking..."):
                try:
                    st.session_state.setdefault("genie_hist", []).insert(0, genie_ask(question))
                except Exception as e:
                    st.session_state.setdefault("genie_hist", []).insert(0, dict(q=question, texts=[f"Genie error: {e}"], tables=[], sqls=[]))
        if st.button("New conversation", use_container_width=True):
            st.session_state.pop("genie_conv", None); st.session_state["genie_hist"] = []
        for h in st.session_state.get("genie_hist", [])[:6]:
            st.markdown(f"**Q:** {h['q']}")
            for t in h["texts"]:
                st.markdown(t)
            for tb in h["tables"]:
                st.dataframe(tb, use_container_width=True, hide_index=True, height=min(260, 38 + 35 * len(tb)))
            if h["sqls"]:
                with st.expander("SQL"):
                    st.code(h["sqls"][0], language="sql")
            st.divider()


# ----------------------------------------------------------------------------- clock
def clock_bar():
    ss = st.session_state
    ss.setdefault("clock", DAY.replace(hour=8, minute=30))
    ss.setdefault("playing", False)
    ss.setdefault("speed", 3.0)
    c1, c2, c3, c4 = st.columns([2.2, 1.6, 4.6, 2.2])
    with c1:
        st.markdown(f"<div class='cr-title'>FANIMATION <span>SUPPORT COMMAND CENTER</span></div>", unsafe_allow_html=True)
        st.markdown(f"<div class='clock'>{ss.clock:%H:%M} UTC</div><div style='color:#9ca3af;font-size:.75rem'>Replay · {ss.clock:%a %d %b %Y}</div>", unsafe_allow_html=True)
    with c2:
        b1, b2 = st.columns(2)
        if b1.button("⏸ Pause" if ss.playing else "▶ Play", use_container_width=True):
            ss.playing = not ss.playing
        if b2.button("⏭ +15m", use_container_width=True):
            ss.clock = min(ss.clock + timedelta(minutes=15), DAY.replace(hour=23, minute=55))
        ss.speed = st.select_slider("Speed (sec per 5 min)", options=[1.0, 2.0, 3.0, 5.0, 10.0], value=ss.speed)
    with c3:
        mins = int((ss.clock - DAY).total_seconds() // 60)
        new = st.slider("Demo clock", 0, 23 * 60 + 55, mins, step=5, format="%d min", label_visibility="collapsed")
        if new != mins:
            ss.clock = DAY + timedelta(minutes=new)
        jc = st.columns(len(JUMPS))
        for i, (lab, hm) in enumerate(JUMPS):
            if jc[i].button(lab, use_container_width=True, key=f"j{i}"):
                h, m = map(int, hm.split(":")); ss.clock = DAY.replace(hour=h, minute=m); ss.playing = False
    with c4:
        o = ops()
        st.markdown(f"<div style='font-size:.75rem;color:#9ca3af'>Lakebase ops store: {'🟢 connected' if o.ok else '🟡 session-only'}</div>", unsafe_allow_html=True)
        if not o.ok and o.err:
            st.caption(o.err[:140])
        if st.button("Reset demo", use_container_width=True):
            o.reset(); ss.clock = DAY.replace(hour=8, minute=30); ss.playing = False
    return ss.clock


def at(df, clock, col="ts"):
    """Latest snapshot at or before the clock."""
    t = df.loc[df[col] <= clock, col].max()
    return df[df[col] == t]


# ----------------------------------------------------------------------------- views
def kpi(label, value, alert=False):
    return f"<div class='kpi {'alert' if alert else ''}'><div class='l'>{label}</div><div class='v'>{value}</div></div>"


def effective_health(D, clock, declared):
    h = at(D["health"], clock).copy()
    if len(declared):
        an = D["anom"].set_index("anomaly_id")
        for aid in declared.anomaly_id:
            if aid in an.index and an.loc[aid, "detected_at"] <= clock <= an.loc[aid, "last_seen"] + timedelta(minutes=30):
                rgs = [r.strip() for r in str(an.loc[aid, "region_groups"]).split(",")]
                m = (h.domain == an.loc[aid, "issue_domain"]) & h.region_group.isin(rgs) & h.health_state.isin(["Customer-Detected", "Telemetry Alert", "Watch"])
                h.loc[m, "health_state"] = "Known (declared in app)"
                h.loc[m, "open_incident_id"] = "declared in app"
    return h


def platform_pulse(D, clock):
    declared = ops().declared()
    h = effective_health(D, clock, declared)
    heat = at(D["heat"], clock)
    g = D["frus"]; gl = at(g[g.scope_type == "Global"], clock)
    cd_cells = int((h.health_state == "Customer-Detected").sum())
    sc = D["score"]; sc_day = sc[(sc.declared_at >= DAY) & (sc.declared_at <= clock)]
    lead = f"{sc_day.detection_lead_minutes.mean():.0f} min" if len(sc_day) else "—"
    rar = D["pay"].loc[D["pay"].ts <= clock, "revenue_at_risk_usd"].sum()
    k = st.columns(6)
    vals = [("Customer-detected cells", cd_cells, cd_cells > 0), ("Global frustration index", f"{gl.frustration_index.iloc[0]:.0f}" if len(gl) else "—", len(gl) and gl.frustration_index.iloc[0] > 40),
            ("Tickets · last 15 min", f"{int(heat.tickets_15m.sum()):,}", False), ("Concurrent streams", f"{int(heat.streams.sum()):,}", False),
            ("Revenue at risk today", f"${rar:,.0f}", rar > 500), ("Customers beat monitoring by", lead, False)]
    for i, (l, v, a) in enumerate(vals):
        k[i].markdown(kpi(l, v, bool(a)), unsafe_allow_html=True)
    banners = ops().banners()
    if len(banners):
        st.warning("📣 Known-issue banners live for agents: " + " | ".join(banners.headline.astype(str).head(3)))

    left, right = st.columns([1.35, 1])
    with left:
        st.markdown("#### 🌍 Global pulse")
        layer = st.radio("Layer", ["Support tickets vs expected", "Login failures", "Playback errors", "Payment success"], horizontal=True, label_visibility="collapsed")
        hm = heat.copy()
        if layer.startswith("Support"):
            color, cs, rng_, title = "ticket_deviation_ratio", [[0, "#1f7a4d"], [0.25, "#facc15"], [0.6, "#f97316"], [1, "#e11d74"]], (0.5, 8), "x expected"
        elif layer.startswith("Login"):
            hm["v"] = hm.login_failure_rate * 100; color, cs, rng_, title = "v", "YlOrRd", (0, 5), "% login fail"
        elif layer.startswith("Playback"):
            hm["v"] = hm.playback_error_rate * 100; color, cs, rng_, title = "v", "YlOrRd", (0, 3), "% playback err"
        else:
            hm["v"] = hm.payment_success_rate * 100; color, cs, rng_, title = "v", "RdYlGn", (85, 99), "% pay success"
        hm["size"] = (hm.tickets_15m.clip(lower=1) ** 0.5) * 6 + 6
        hm["label"] = hm.apply(lambda r: f"<b>{r.country_name}</b><br>{r.worst_health_state}<br>Tickets 15m: {int(r.tickets_15m)} (exp {r.expected_tickets_15m:.0f})<br>Streams: {int(r.streams):,} ({r.stream_deviation_pct*100:+.1f}% vs baseline)<br>Top domain: {r.top_domain}", axis=1)
        fig = go.Figure()
        hot = hm[hm.worst_health_state == "Customer-Detected"]
        if len(hot):
            fig.add_trace(go.Scattergeo(lat=hot.lat, lon=hot.lon, mode="markers", hoverinfo="skip", showlegend=False,
                                        marker=dict(size=hot["size"] * 2.2, color="rgba(225,29,116,0.25)", line=dict(width=2, color="#ff4fa3"))))
        fig.add_trace(go.Scattergeo(lat=hm.lat, lon=hm.lon, text=hm.label, hoverinfo="text", mode="markers+text", textposition="top center",
                                    textfont=dict(color="#d1d5db", size=10), showlegend=False,
                                    marker=dict(size=hm["size"], color=hm[color], colorscale=cs, cmin=rng_[0], cmax=rng_[1], line=dict(width=0.5, color="#111"),
                                                colorbar=dict(title=title, thickness=10, len=0.6))))
        fig.data[-1].text = hm.label
        fig.data[-1].mode = "markers"
        fig.update_geos(projection_type="natural earth", showland=True, landcolor="#1c1c30", showocean=True, oceancolor="#0b0b14",
                        showcountries=True, countrycolor="#2f2f4f", coastlinecolor="#2f2f4f", bgcolor="rgba(0,0,0,0)", lataxis_range=[-50, 72])
        fig.update_layout(height=390, margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    with right:
        st.markdown("#### 🧩 Service health matrix")
        cells = ["<div class='matrix'><div></div>" + "".join(f"<div class='mh'>{r}</div>" for r in REGIONS)]
        hi = h.set_index(["domain", "region_group"])
        for d in DOMAINS:
            cells.append(f"<div class='md'>{d}</div>")
            for r in REGIONS:
                if (d, r) in hi.index:
                    x = hi.loc[(d, r)]
                    s_ = x.health_state; col = STATE_COLORS.get(s_, "#1f7a4d")
                    sub = "no internal alert" if s_ == "Customer-Detected" else (x.open_incident_id if isinstance(x.open_incident_id, str) else "")
                    cells.append(f"<div class='cell {'pulse' if s_=='Customer-Detected' else ''}' style='background:{col}' title='tickets x{x.ticket_ratio:.1f} · errors x{x.error_ratio:.1f}'>"
                                 f"<b>{html.escape(s_.upper() if s_=='Customer-Detected' else s_)}</b>{html.escape(sub)}<br>tix x{x.ticket_ratio:.1f} · err x{x.error_ratio:.1f}</div>")
                else:
                    cells.append("<div class='cell' style='background:#222'>n/a</div>")
        cells.append("</div>")
        st.markdown("".join(cells), unsafe_allow_html=True)
        hot_cells = h[h.health_state.isin(["Customer-Detected", "Telemetry Alert"])]
        if len(hot_cells):
            opts = [f"{r.domain} · {r.region_group} ({r.health_state})" for r in hot_cells.itertuples()]
            c_a, c_b = st.columns([3, 1])
            pick = c_a.selectbox("Acknowledge alert", opts, label_visibility="collapsed")
            if c_b.button("Acknowledge", use_container_width=True):
                r = hot_cells.iloc[opts.index(pick)]
                ops().ack(r.region_group, r.domain, r.health_state, actor(), clock)
                st.toast("Acknowledged and logged to Lakebase")

    st.markdown("#### 📈 Concurrent streams vs baseline · consumer frustration")
    fr = D["frus"]
    scopes = ["Global"] + REGIONS + sorted(fr[fr.scope_type == "Country"].scope.unique().tolist())
    c1, c2 = st.columns([4, 1])
    with c2:
        st.session_state.setdefault("scope", "Global")
        scope = st.selectbox("Scope", scopes, key="scope")
        cur = at(fr[fr.scope == scope], clock)
        fi = float(cur.frustration_index.iloc[0]) if len(cur) else 0
        gauge = go.Figure(go.Indicator(mode="gauge+number", value=fi, title={"text": "Frustration index", "font": {"size": 13, "color": "#ddd"}},
                                       gauge={"axis": {"range": [0, 100]}, "bar": {"color": "#f47521"},
                                              "steps": [{"range": [0, 25], "color": "#14532d"}, {"range": [25, 55], "color": "#713f12"}, {"range": [55, 100], "color": "#831843"}]}))
        gauge.update_layout(height=210, margin=dict(l=10, r=10, t=40, b=0), paper_bgcolor="rgba(0,0,0,0)", font_color="#ddd")
        st.plotly_chart(gauge, use_container_width=True, config={"displayModeBar": False})
        if len(cur) and bool(cur.divergence_flag.iloc[0]):
            st.error("⚠️ DIVERGENCE: streams dropping while contacts spike — users are hitting breakage.")
        elif len(cur) and isinstance(cur.active_release.iloc[0], str):
            st.info(f"🎬 Simulcast in effect: {cur.active_release.iloc[0]} — traffic spike is expected.")
    with c1:
        s = fr[fr.scope == scope].sort_values("ts"); past = s[s.ts <= clock]
        f2 = go.Figure()
        f2.add_trace(go.Scatter(x=s.ts, y=s.baseline_streams, name="Release-aware baseline", line=dict(color="#6b7280", dash="dot")))
        f2.add_trace(go.Scatter(x=past.ts, y=past.streams, name="Concurrent streams", line=dict(color="#38bdf8", width=2.5)))
        f2.add_trace(go.Scatter(x=past.ts, y=past.contacts_per_10k_streams, name="Contacts per 10k streams", yaxis="y2", line=dict(color="#f47521", width=2)))
        f2.add_trace(go.Scatter(x=past.ts, y=past.baseline_contacts_per_10k_streams, name="Expected contacts", yaxis="y2", line=dict(color="#f4752155", dash="dash")))
        div = past[past.divergence_flag]
        for t in div.ts:
            f2.add_vrect(x0=t - timedelta(minutes=2.5), x1=t + timedelta(minutes=2.5), fillcolor="#e11d74", opacity=0.18, line_width=0)
        rel = s.dropna(subset=["active_release"]).groupby("active_release").ts.min()
        for name, t in rel.items():
            if t <= clock:
                f2.add_annotation(x=t, y=1.02, yref="paper", text=f"🎬 {name}", showarrow=False, font=dict(size=10, color="#a78bfa"))
        ch = D["changes"]
        for r in ch[ch.change_ts <= clock].itertuples():
            f2.add_vline(x=r.change_ts, line=dict(color="#facc15", width=1, dash="dot"))
        f2.add_vline(x=clock, line=dict(color="#f47521", width=2))
        f2.update_layout(height=320, margin=dict(l=0, r=0, t=20, b=0), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#ddd",
                         legend=dict(orientation="h", y=-0.15), xaxis=dict(range=[DAY, DAY + timedelta(days=1)], gridcolor="#222"),
                         yaxis=dict(title="Streams", gridcolor="#222"), yaxis2=dict(title="Contacts / 10k", overlaying="y", side="right", showgrid=False))
        st.plotly_chart(f2, use_container_width=True, config={"displayModeBar": False})
        st.caption("Pink bands = divergence (streams ≥8% below baseline while contacts ≥2x). Yellow lines = risky deploys/config changes. Purple = simulcast releases.")


def subscriber_command(D, clock):
    an = D["anom"]; an = an[an.detected_at <= clock].sort_values("detected_at", ascending=False)
    declared = ops().declared(); declared_ids = set(declared.anomaly_id) if len(declared) else set()
    left, right = st.columns([1.25, 1])
    with left:
        st.markdown("#### 🚨 AI ticket anomaly detection")
        if an.empty:
            st.info("No ticket surges detected yet. Advance the clock.")
        sig = D["sig"]
        for a in an.to_dict("records"):
            active = clock <= a["last_seen"]
            inc_open = isinstance(a.get("incident_declared_at"), pd.Timestamp) and a["incident_declared_at"] <= clock
            in_app = a["anomaly_id"] in declared_ids
            age = int((clock - a["detected_at"]).total_seconds() // 60)
            if inc_open:
                badge = f"<span class='badge' style='background:#d97706'>KNOWN · {a['incident_id']}</span>"
            elif in_app:
                badge = "<span class='badge' style='background:#d97706'>DECLARED IN APP</span>"
            else:
                badge = f"<span class='badge' style='background:#e11d74'>UNDECLARED · customers detected {age} min ago</span>"
            if not active:
                badge = "<span class='badge' style='background:#2563eb'>RESOLVED / SUBSIDED</span>"
            try:
                actions = json.loads(a.get("actions_json") or "[]")
            except Exception:
                actions = []
            st.markdown(f"""<div class='card {'hot' if (active and not inc_open and not in_app) else ''}'>
              {badge}<span class='badge' style='background:#374151'>{html.escape(a['issue_domain'])}</span>
              <div style='font-weight:800;font-size:1.02rem;margin-top:6px'>{html.escape(str(a['ai_headline']))}</div>
              <div style='color:#9ca3af;font-size:.78rem'>Signature <b style='color:#fff'>{html.escape(a['issue_signature'])}</b> · detected {a['detected_at']:%H:%M} UTC ·
                 {int(a['total_tickets']):,} tickets · peak {int(a['peak_tickets_15m'])}/15m vs {a['baseline_15m']:.1f} baseline</div>
              <div style='margin-top:6px;font-size:.86rem'>{html.escape(str(a['ai_summary']))}</div>
              <div style='margin-top:6px;font-size:.78rem;color:#cbd5e1'>📍 {html.escape(str(a['top_countries']))} &nbsp; 📺 {html.escape(str(a['top_devices']))}</div>
              <div style='margin-top:4px;font-size:.78rem;color:#facc15'>🔧 Suspected change: {html.escape(str(a.get('suspected_change') or 'none found'))}</div>
              <div style='margin-top:4px;font-size:.78rem'>✅ {' · '.join(html.escape(x) for x in actions)}</div></div>""", unsafe_allow_html=True)
            c1, c2 = st.columns([3, 1.2])
            sp = sig[(sig.issue_signature == a["issue_signature"]) & (sig.ts_15m <= clock)].sort_values("ts_15m")
            if len(sp):
                f = go.Figure(go.Bar(x=sp.ts_15m, y=sp.tickets_15m, marker_color=["#e11d74" if v else "#6b7280" for v in sp.is_anomalous]))
                f.update_layout(height=90, margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                xaxis=dict(visible=False, range=[DAY, DAY + timedelta(days=1)]), yaxis=dict(visible=False))
                c1.plotly_chart(f, use_container_width=True, config={"displayModeBar": False}, key=f"sp_{a['anomaly_id']}")
            if active and not inc_open and not in_app:
                if c2.button("📣 Declare incident + publish banner", key=f"dec_{a['anomaly_id']}", use_container_width=True):
                    ops().declare(a, actor(), clock, "Declared from Support Command Center")
                    st.toast("Incident declared · known-issue banner published (Lakebase)")
                    st.rerun()
            with c2.expander("Agent macro"):
                st.write(a["ai_customer_macro"])
    with right:
        st.markdown("#### 💳 Payment gateway health")
        pay = D["pay"]
        ctry = st.selectbox("Country", sorted(pay.country_code.unique()), index=sorted(pay.country_code.unique()).index("DE"))
        p = pay[(pay.country_code == ctry) & (pay.ts <= clock)].sort_values("ts")
        latest = at(pay[pay.country_code == ctry], clock)
        badges = "".join(f"<span class='badge' style='background:{ {'Healthy':'#1f7a4d','Degraded':'#ea580c','Critical':'#e11d74'}[r.gateway_state]}'>{r.gateway}: {r.success_rate_15m*100:.1f}%</span>" for r in latest.itertuples())
        st.markdown(badges, unsafe_allow_html=True)
        f = go.Figure()
        for gw_, gdf in p.groupby("gateway"):
            f.add_trace(go.Scatter(x=gdf.ts, y=gdf.success_rate_15m * 100, name=gw_, mode="lines"))
        bt = p.groupby("ts").country_billing_tickets.max().reset_index()
        f.add_trace(go.Bar(x=bt.ts, y=bt.country_billing_tickets, name="Billing tickets", yaxis="y2", marker_color="#f4752188"))
        f.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#ddd",
                        legend=dict(orientation="h", y=-0.2), xaxis=dict(range=[DAY, DAY + timedelta(days=1)], gridcolor="#222"),
                        yaxis=dict(title="Success % (15m)", range=[60, 101], gridcolor="#222"), yaxis2=dict(title="Tickets", overlaying="y", side="right", showgrid=False))
        st.plotly_chart(f, use_container_width=True, config={"displayModeBar": False})
        st.metric(f"Revenue at risk today · {ctry}", f"${p.revenue_at_risk_usd.sum():,.0f}")

        st.markdown("#### 📡 Social listening · Reddit & X")
        so = D["social"]; so = so[so.posted_at <= clock]
        neg = so[(so.sentiment == "negative") & (so.topic_domain != "Show hype / fandom")]
        if len(neg):
            nv = neg.groupby(["ts_15m", "topic_domain"]).size().reset_index(name="posts")
            f3 = px.bar(nv, x="ts_15m", y="posts", color="topic_domain")
            f3.update_layout(height=180, margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#ddd",
                             legend=dict(orientation="h", y=-0.35, title=None), xaxis=dict(title=None, range=[DAY, DAY + timedelta(days=1)]), yaxis=dict(title="Negative posts"))
            st.plotly_chart(f3, use_container_width=True, config={"displayModeBar": False})
        colors = {"negative": "#e11d74", "positive": "#22c55e", "neutral": "#6b7280", "mixed": "#facc15"}
        feed = so.sort_values("posted_at", ascending=False).head(14)
        st.markdown("".join(f"<div class='post' style='border-color:{colors.get(r.sentiment,'#555')}'><span style='color:#9ca3af;font-size:.72rem'>{r.posted_at:%H:%M} · {r.platform} · {r.author_handle} · {r.country_code or ''} · {r.sentiment}</span><br>{html.escape(str(r.post_text))}</div>" for r in feed.itertuples()), unsafe_allow_html=True)


def executive(D, clock):
    host = Config().host.rstrip("/")
    st.markdown("#### 🏛️ Executive view")
    sc = D["score"].sort_values("declared_at", ascending=False)
    k = st.columns(4)
    cf = sc[sc.detection_winner == "Customers first"]
    k[0].markdown(kpi("Incidents (15 days)", len(sc)), unsafe_allow_html=True)
    k[1].markdown(kpi("Detected by customers first", f"{len(cf)}/{len(sc)}", True), unsafe_allow_html=True)
    k[2].markdown(kpi("Avg minutes before monitoring", f"{cf.detection_lead_minutes.mean():.0f}"), unsafe_allow_html=True)
    k[3].markdown(kpi("Est. lost viewing hours", f"{sc.est_lost_viewing_hours.sum():,.0f}"), unsafe_allow_html=True)
    kp = D["kpi"]
    c1, c2 = st.columns(2)
    f = px.bar(kp, x="kpi_date", y="tickets", color="region_group", title="Daily tickets by region")
    f.update_layout(height=300, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#ddd")
    c1.plotly_chart(f, use_container_width=True)
    f = px.line(kp, x="kpi_date", y="customer_detected_minutes", color="region_group", markers=True, title="Minutes in Customer-Detected state")
    f.update_layout(height=300, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#ddd")
    c2.plotly_chart(f, use_container_width=True)
    st.markdown("##### Incident scorecard")
    st.dataframe(sc[["incident_id", "title", "region_groups", "customer_detected_at", "first_alert_at", "declared_at", "detection_lead_minutes",
                     "detection_winner", "minutes_to_resolve", "tickets_during_incident", "est_lost_viewing_hours", "revenue_at_risk_usd", "root_cause"]],
                 use_container_width=True, hide_index=True)
    st.markdown("##### AI/BI executive dashboard")
    url = f"{host}/embed/dashboardsv3/{DASH}" + (f"?o={WS_ID}" if WS_ID else "")
    st.markdown(f"[Open the dashboard in a new tab]({host}/dashboardsv3/{DASH}/published)")
    st.caption("If the frame below is blank or refuses to connect, an admin must allow embedding for the app domain (Settings > Security > Embed dashboards). Use the link above meanwhile.")
    components.iframe(url, height=900, scrolling=True)


# ----------------------------------------------------------------------------- main
genie_panel()
clock = clock_bar()
try:
    D = load_day()
except Exception as e:
    st.error(f"Could not load data from the SQL warehouse: {e}")
    st.info("Make sure the app service principal has CAN_USE on the warehouse and SELECT on the schema.")
    st.stop()

tab1, tab2, tab3 = st.tabs(["🛰️ Platform Pulse", "🎧 Subscriber Experience & Support Command", "🏛️ Executive"])
def safe(fn, *args):
    try:
        fn(*args)
    except Exception as e:  # show full trace in-app so issues are diagnosable without logs
        st.error(f"{fn.__name__} failed: {type(e).__name__}: {e}")
        st.exception(e)


with tab1:
    safe(platform_pulse, D, clock)
with tab2:
    safe(subscriber_command, D, clock)
with tab3:
    safe(executive, D, clock)

if st.session_state.get("playing"):
    time.sleep(st.session_state.get("speed", 3.0))
    if st.session_state.clock < DAY.replace(hour=23, minute=55):
        st.session_state.clock += timedelta(minutes=5)
    else:
        st.session_state.playing = False
    st.rerun()
