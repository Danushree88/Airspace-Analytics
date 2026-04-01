import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time
from datetime import datetime

# ─────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────
st.set_page_config(
    page_title="Airspace Intelligence",
    page_icon="✈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────
# CUSTOM CSS
# ─────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap');

html, body, [class*="css"] {
    font-family: 'Syne', sans-serif;
    background-color: #050d1a;
    color: #e0eaff;
}

.stApp { background-color: #050d1a; }

/* Header */
.dash-header {
    background: linear-gradient(135deg, #0a1628 0%, #0d2040 100%);
    border: 1px solid #1a3a6e;
    border-radius: 12px;
    padding: 24px 32px;
    margin-bottom: 24px;
    display: flex;
    align-items: center;
    gap: 16px;
}
.dash-title {
    font-family: 'Syne', sans-serif;
    font-weight: 800;
    font-size: 2rem;
    color: #4fc3f7;
    margin: 0;
    letter-spacing: -0.5px;
}
.dash-subtitle {
    font-family: 'Space Mono', monospace;
    font-size: 0.75rem;
    color: #5a7fa8;
    margin: 4px 0 0 0;
}
.live-dot {
    width: 10px;
    height: 10px;
    background: #00e676;
    border-radius: 50%;
    animation: pulse 1.5s infinite;
    display: inline-block;
    margin-right: 6px;
}
@keyframes pulse {
    0%   { box-shadow: 0 0 0 0 rgba(0,230,118,0.6); }
    70%  { box-shadow: 0 0 0 8px rgba(0,230,118,0); }
    100% { box-shadow: 0 0 0 0 rgba(0,230,118,0); }
}

/* Metric cards */
.metric-card {
    background: linear-gradient(135deg, #0a1628, #0d2040);
    border: 1px solid #1a3a6e;
    border-radius: 10px;
    padding: 20px 24px;
    text-align: center;
}
.metric-value {
    font-family: 'Space Mono', monospace;
    font-size: 2.2rem;
    font-weight: 700;
    color: #4fc3f7;
    line-height: 1;
}
.metric-label {
    font-size: 0.75rem;
    color: #5a7fa8;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-top: 6px;
}

/* Section headers */
.section-header {
    font-family: 'Space Mono', monospace;
    font-size: 0.7rem;
    color: #5a7fa8;
    text-transform: uppercase;
    letter-spacing: 2px;
    border-bottom: 1px solid #1a3a6e;
    padding-bottom: 8px;
    margin-bottom: 16px;
}

/* Alert badges */
.alert-high {
    background: rgba(244,67,54,0.15);
    border: 1px solid #f44336;
    border-radius: 6px;
    padding: 8px 14px;
    color: #ef9a9a;
    font-family: 'Space Mono', monospace;
    font-size: 0.8rem;
    margin: 4px 0;
}
.alert-medium {
    background: rgba(255,152,0,0.15);
    border: 1px solid #ff9800;
    border-radius: 6px;
    padding: 8px 14px;
    color: #ffcc80;
    font-family: 'Space Mono', monospace;
    font-size: 0.8rem;
    margin: 4px 0;
}
.no-alert {
    background: rgba(0,230,118,0.1);
    border: 1px solid #00e676;
    border-radius: 6px;
    padding: 8px 14px;
    color: #69f0ae;
    font-family: 'Space Mono', monospace;
    font-size: 0.8rem;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: #050d1a !important;
    border-right: 1px solid #1a3a6e;
}
section[data-testid="stSidebar"] .stMarkdown { color: #5a7fa8; }

/* Plotly chart bg */
.js-plotly-plot { background: transparent !important; }

/* Dataframe */
.stDataFrame { border: 1px solid #1a3a6e; border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────
# CASSANDRA CONNECTION
# ─────────────────────────────────────────
@st.cache_resource
def get_cassandra_session():
    try:
        cluster = Cluster(['127.0.0.1'], port=9042)
        session = cluster.connect('airspace')
        return session
    except Exception as e:
        st.error(f"Cannot connect to Cassandra: {e}")
        return None

# ─────────────────────────────────────────
# DATA LOADERS
# ─────────────────────────────────────────
def load_flight_events(session, limit=500):
    rows = session.execute(f"SELECT * FROM flight_events LIMIT {limit}")
    return pd.DataFrame(rows)

def load_region_metrics(session, limit=200):
    rows = session.execute(f"SELECT * FROM region_metrics LIMIT {limit}")
    return pd.DataFrame(rows)

def load_country_metrics(session, limit=100):
    rows = session.execute(f"SELECT * FROM country_metrics LIMIT {limit}")
    return pd.DataFrame(rows)

def load_anomaly_logs(session, limit=100):
    rows = session.execute(f"SELECT * FROM anomaly_logs LIMIT {limit}")
    return pd.DataFrame(rows)

# ─────────────────────────────────────────
# PLOTLY THEME
# ─────────────────────────────────────────
CHART_BG    = "rgba(0,0,0,0)"
GRID_COLOR  = "#1a3a6e"
FONT_COLOR  = "#8ab4d4"
ACCENT      = "#4fc3f7"
ACCENT2     = "#00e676"
DANGER      = "#f44336"
WARNING     = "#ff9800"

def dark_layout(fig, title=""):
    fig.update_layout(
        title=dict(text=title, font=dict(color=FONT_COLOR, size=13, family="Space Mono")),
        paper_bgcolor=CHART_BG,
        plot_bgcolor=CHART_BG,
        font=dict(color=FONT_COLOR, family="Syne"),
        margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=FONT_COLOR)),
        xaxis=dict(gridcolor=GRID_COLOR, zerolinecolor=GRID_COLOR),
        yaxis=dict(gridcolor=GRID_COLOR, zerolinecolor=GRID_COLOR),
    )
    return fig

# ─────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────
with st.sidebar:
    st.markdown("### ✈ Airspace Intel")
    st.markdown("---")
    auto_refresh = st.toggle("Auto Refresh", value=True)
    refresh_rate = st.slider("Refresh interval (s)", 10, 60, 20)
    st.markdown("---")
    st.markdown("**Data Limits**")
    flight_limit   = st.slider("Flight events",   100, 2000, 500)
    region_limit   = st.slider("Region metrics",  50,  500,  200)
    country_limit  = st.slider("Country metrics", 20,  200,  100)
    st.markdown("---")
    if st.button("🔄 Refresh Now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.markdown(f"""
    <div style='font-family:Space Mono;font-size:0.65rem;color:#2a4a6e;margin-top:16px;'>
    Last updated<br>{datetime.now().strftime('%H:%M:%S')}
    </div>
    """, unsafe_allow_html=True)

# ─────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────
st.markdown("""
<div class="dash-header">
  <div>
    <p class="dash-title">✈ &nbsp;Airspace Intelligence Platform</p>
    <p class="dash-subtitle"><span class="live-dot"></span>LIVE · Real-Time ADS-B Pipeline · Kafka → Spark → Cassandra</p>
  </div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────
# CONNECT + LOAD
# ─────────────────────────────────────────
session = get_cassandra_session()
if session is None:
    st.stop()

with st.spinner("Loading data from Cassandra..."):
    df_flights  = load_flight_events(session, flight_limit)
    df_regions  = load_region_metrics(session, region_limit)
    df_countries = load_country_metrics(session, country_limit)
    df_anomalies = load_anomaly_logs(session)

# ─────────────────────────────────────────
# KPI METRICS
# ─────────────────────────────────────────
total_flights   = len(df_flights)
total_countries = df_flights["country"].nunique() if not df_flights.empty else 0
total_anomalies = len(df_anomalies)
avg_altitude    = round(df_flights["altitude"].mean(), 0) if not df_flights.empty else 0
congested       = len(df_regions[df_regions["aci"] > 0.8]) if not df_regions.empty else 0

c1, c2, c3, c4, c5 = st.columns(5)
for col_obj, val, label in [
    (c1, f"{total_flights:,}", "Total Aircraft"),
    (c2, total_countries,      "Countries"),
    (c3, f"{avg_altitude:,.0f}m", "Avg Altitude"),
    (c4, total_anomalies,      "Anomalies"),
    (c5, congested,            "Congested Regions"),
]:
    with col_obj:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{val}</div>
            <div class="metric-label">{label}</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────
# ROW 1: MAP + CONGESTION ALERTS
# ─────────────────────────────────────────
col_map, col_alerts = st.columns([3, 1])

with col_map:
    st.markdown('<div class="section-header">Live Aircraft Map</div>', unsafe_allow_html=True)
    if not df_flights.empty:
        df_map = df_flights.dropna(subset=["latitude", "longitude"])

        # Altitude band for color
        def alt_band(a):
            if a < 2000:   return "LOW"
            elif a < 10000: return "MEDIUM"
            else:           return "HIGH"

        df_map = df_map.copy()
        df_map["band"] = df_map["altitude"].apply(alt_band)

        color_map = {"LOW": "#ff9800", "MEDIUM": "#4fc3f7", "HIGH": "#00e676"}

        fig_map = px.scatter_mapbox(
            df_map,
            lat="latitude", lon="longitude",
            color="band",
            color_discrete_map=color_map,
            hover_data={"callsign": True, "country": True,
                        "altitude": True, "velocity": True,
                        "latitude": False, "longitude": False},
            zoom=1, height=420,
            title=""
        )
        fig_map.update_layout(
            mapbox_style="carto-darkmatter",
            paper_bgcolor=CHART_BG,
            margin=dict(l=0, r=0, t=0, b=0),
            legend=dict(
                title="Altitude Band",
                bgcolor="rgba(5,13,26,0.8)",
                font=dict(color=FONT_COLOR)
            )
        )
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.info("No flight data yet.")

with col_alerts:
    st.markdown('<div class="section-header">Congestion Alerts</div>', unsafe_allow_html=True)
    if not df_regions.empty:
        latest = df_regions.sort_values("timestamp", ascending=False).drop_duplicates("region")
        high   = latest[latest["aci"] > 1.2]
        medium = latest[(latest["aci"] > 0.8) & (latest["aci"] <= 1.2)]

        if len(high) == 0 and len(medium) == 0:
            st.markdown('<div class="no-alert">✅ All regions normal</div>', unsafe_allow_html=True)
        else:
            for _, row in high.iterrows():
                st.markdown(f'<div class="alert-high">🔴 {row["region"]}<br>ACI: {row["aci"]:.2f}</div>', unsafe_allow_html=True)
            for _, row in medium.iterrows():
                st.markdown(f'<div class="alert-medium">🟡 {row["region"]}<br>ACI: {row["aci"]:.2f}</div>', unsafe_allow_html=True)
    else:
        st.info("No region data yet.")

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Anomaly Log</div>', unsafe_allow_html=True)
    if not df_anomalies.empty:
        st.dataframe(
            df_anomalies[["icao24", "reason"]].head(8),
            use_container_width=True, hide_index=True
        )
    else:
        st.markdown('<div class="no-alert">✅ No anomalies detected</div>', unsafe_allow_html=True)

# ─────────────────────────────────────────
# ROW 2: COUNTRY RANKINGS + ALTITUDE DIST
# ─────────────────────────────────────────
col_country, col_alt = st.columns(2)

with col_country:
    st.markdown('<div class="section-header">Top Countries by Traffic</div>', unsafe_allow_html=True)
    if not df_countries.empty:
        latest_c = df_countries.sort_values("timestamp", ascending=False).drop_duplicates("country")
        top20    = latest_c.nlargest(20, "total_flights")

        fig_bar = go.Figure(go.Bar(
            x=top20["total_flights"],
            y=top20["country"],
            orientation="h",
            marker=dict(
                color=top20["total_flights"],
                colorscale=[[0, "#1a3a6e"], [0.5, "#4fc3f7"], [1, "#00e676"]],
                showscale=False
            ),
            text=top20["total_flights"],
            textposition="outside",
            textfont=dict(color=FONT_COLOR, size=11)
        ))
        fig_bar = dark_layout(fig_bar)
        fig_bar.update_layout(height=400, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("No country data yet.")

with col_alt:
    st.markdown('<div class="section-header">Altitude Distribution</div>', unsafe_allow_html=True)
    if not df_flights.empty:
        fig_hist = go.Figure(go.Histogram(
            x=df_flights["altitude"].dropna(),
            nbinsx=40,
            marker=dict(
                color=ACCENT,
                opacity=0.8,
                line=dict(color="#0a1628", width=0.5)
            )
        ))
        fig_hist = dark_layout(fig_hist)
        fig_hist.update_layout(
            height=400,
            xaxis_title="Altitude (m)",
            yaxis_title="Aircraft Count"
        )
        st.plotly_chart(fig_hist, use_container_width=True)
    else:
        st.info("No flight data yet.")

# ─────────────────────────────────────────
# ROW 3: REGION ACI HEATMAP + VELOCITY
# ─────────────────────────────────────────
col_aci, col_vel = st.columns(2)

with col_aci:
    st.markdown('<div class="section-header">Airspace Congestion Index (ACI) by Region</div>', unsafe_allow_html=True)
    if not df_regions.empty:
        latest_r = df_regions.sort_values("timestamp", ascending=False).drop_duplicates("region")
        top_regions = latest_r.nlargest(25, "aircraft_count")

        fig_aci = go.Figure(go.Bar(
            x=top_regions["region"],
            y=top_regions["aci"],
            marker=dict(
                color=top_regions["aci"],
                colorscale=[
                    [0,   "#00e676"],
                    [0.5, "#ff9800"],
                    [1,   "#f44336"]
                ],
                showscale=True,
                colorbar=dict(
                    title="ACI",
                    tickfont=dict(color=FONT_COLOR)
                )
            )
        ))
        fig_aci = dark_layout(fig_aci)
        fig_aci.update_layout(
            height=360,
            xaxis_tickangle=-45,
            yaxis_title="ACI Score"
        )
        # Add threshold lines
        fig_aci.add_hline(y=0.8, line_dash="dash", line_color=WARNING,
                          annotation_text="Medium threshold",
                          annotation_font_color=WARNING)
        fig_aci.add_hline(y=1.2, line_dash="dash", line_color=DANGER,
                          annotation_text="High threshold",
                          annotation_font_color=DANGER)
        st.plotly_chart(fig_aci, use_container_width=True)
    else:
        st.info("No region data yet.")

with col_vel:
    st.markdown('<div class="section-header">Speed Distribution (km/h)</div>', unsafe_allow_html=True)
    if not df_flights.empty:
        df_flights["speed_kmh"] = df_flights["velocity"] * 3.6
        fig_vel = go.Figure()
        fig_vel.add_trace(go.Violin(
            y=df_flights["speed_kmh"].dropna(),
            box_visible=True,
            meanline_visible=True,
            fillcolor="rgba(79,195,247,0.2)",
            line_color=ACCENT,
            name="Speed"
        ))
        fig_vel = dark_layout(fig_vel)
        fig_vel.update_layout(height=360, yaxis_title="Speed (km/h)", showlegend=False)
        st.plotly_chart(fig_vel, use_container_width=True)
    else:
        st.info("No flight data yet.")

# ─────────────────────────────────────────
# ROW 4: FLIGHT STATUS PIE + AVG ALTITUDE
# ─────────────────────────────────────────
col_status, col_avg_alt = st.columns(2)

with col_status:
    st.markdown('<div class="section-header">Flight Status Breakdown</div>', unsafe_allow_html=True)
    if not df_flights.empty:
        def flight_status(vr):
            if pd.isna(vr):    return "STABLE"
            if vr > 5:         return "CLIMB"
            elif vr < -5:      return "DESCENT"
            else:              return "STABLE"

        df_flights["status"] = df_flights["vertical_rate"].apply(flight_status)
        status_counts = df_flights["status"].value_counts()

        fig_pie = go.Figure(go.Pie(
            labels=status_counts.index,
            values=status_counts.values,
            hole=0.55,
            marker=dict(colors=["#4fc3f7", "#f44336", "#00e676"]),
            textfont=dict(color="#ffffff"),
        ))
        fig_pie = dark_layout(fig_pie)
        fig_pie.update_layout(height=320)
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("No flight data yet.")

with col_avg_alt:
    st.markdown('<div class="section-header">Avg Altitude by Country (Top 15)</div>', unsafe_allow_html=True)
    if not df_countries.empty:
        latest_c2 = df_countries.sort_values("timestamp", ascending=False).drop_duplicates("country")
        top15 = latest_c2.nlargest(15, "avg_altitude")

        fig_alt = go.Figure(go.Bar(
            x=top15["country"],
            y=top15["avg_altitude"],
            marker=dict(
                color=top15["avg_altitude"],
                colorscale=[[0, "#1a3a6e"], [1, ACCENT]],
                showscale=False
            )
        ))
        fig_alt = dark_layout(fig_alt)
        fig_alt.update_layout(
            height=320,
            xaxis_tickangle=-40,
            yaxis_title="Avg Altitude (m)"
        )
        st.plotly_chart(fig_alt, use_container_width=True)
    else:
        st.info("No country data yet.")

# ─────────────────────────────────────────
# RAW DATA TABLE
# ─────────────────────────────────────────
with st.expander("📋 Raw Flight Events (latest 50)"):
    if not df_flights.empty:
        display_cols = ["icao24", "callsign", "country", "altitude", "velocity", "latitude", "longitude", "vertical_rate", "timestamp"]
        available = [c for c in display_cols if c in df_flights.columns]
        st.dataframe(df_flights[available].head(50), use_container_width=True, hide_index=True)
    else:
        st.info("No data yet.")

# ─────────────────────────────────────────
# AUTO REFRESH
# ─────────────────────────────────────────
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()