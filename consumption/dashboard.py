"""
Data Consumption: Barcelona Climate Dashboard.

Reads pre-computed KPIs from ClickHouse exploitation.temperature_kpis and
displays interactive temperature charts for Barcelona (1924–present).
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import clickhouse_connect

CH_HOST = "clickhouse"
CH_PORT = 8123

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
SEASON_ORDER  = ["Winter", "Spring", "Summer", "Autumn"]
SEASON_COLORS = {"Winter": "#5B9BD5", "Spring": "#70AD47",
                 "Summer": "#FF0000", "Autumn": "#ED7D31"}


@st.cache_resource
def get_client():
    return clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT)


@st.cache_data(ttl=300)
def load_kpis() -> pd.DataFrame:
    return get_client().query_df(
        "SELECT * FROM exploitation.temperature_kpis ORDER BY year, month"
    )


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Barcelona Climate Dashboard",
    page_icon="🌡️",
    layout="wide",
)

st.title("🌡️ Barcelona Climate Dashboard")
st.caption("Historical temperature data · NOAA (1924–present) + OpenWeatherMap")

# ── Load data ─────────────────────────────────────────────────────────────────
try:
    df = load_kpis()
except Exception as e:
    st.error(f"Could not connect to ClickHouse: {e}")
    st.stop()

if df.empty:
    st.warning("No KPI data found. Run the Airflow pipeline first.")
    st.stop()

monthly  = df[df["granularity"] == "monthly"].copy()
seasonal = df[df["granularity"] == "seasonal"].copy()

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.header("Filters")
year_min = int(monthly["year"].min())
year_max = int(monthly["year"].max())
year_range = st.sidebar.slider(
    "Year range", year_min, year_max, (max(year_min, 1950), year_max)
)

monthly  = monthly[(monthly["year"] >= year_range[0]) & (monthly["year"] <= year_range[1])]
seasonal = seasonal[(seasonal["year"] >= year_range[0]) & (seasonal["year"] <= year_range[1])]

st.sidebar.markdown("---")
st.sidebar.markdown(f"**Records shown:** {year_range[0]}–{year_range[1]}")
st.sidebar.markdown(f"**Monthly rows:** {len(monthly):,}")
st.sidebar.markdown(f"**Seasonal rows:** {len(seasonal):,}")

# ── KPI cards ─────────────────────────────────────────────────────────────────
seasonal_avg = (
    seasonal.groupby("season")
    .agg(avg=("avg_temp_c", "mean"), lo=("min_temp_c", "min"), hi=("max_temp_c", "max"))
    .reindex(SEASON_ORDER)
)

c1, c2, c3, c4 = st.columns(4)
icons = {"Winter": "❄️", "Spring": "🌸", "Summer": "☀️", "Autumn": "🍂"}
for col, season in zip([c1, c2, c3, c4], SEASON_ORDER):
    if season in seasonal_avg.index:
        row = seasonal_avg.loc[season]
        col.metric(
            f"{icons[season]} {season}",
            f"{row['avg']:.1f} °C",
            f"{row['lo']:.1f} / {row['hi']:.1f} °C  min/max",
        )

st.markdown("---")

# ── Yearly trend ──────────────────────────────────────────────────────────────
st.subheader("Annual Temperature Trend")

yearly = (
    monthly.groupby("year")
    .agg(avg=("avg_temp_c", "mean"), lo=("min_temp_c", "min"), hi=("max_temp_c", "max"))
    .reset_index()
)

fig_trend = go.Figure()
fig_trend.add_trace(go.Scatter(
    x=yearly["year"], y=yearly["hi"],
    mode="lines", name="Max", line=dict(color="rgba(255,80,80,0.3)", width=1),
    fill=None,
))
fig_trend.add_trace(go.Scatter(
    x=yearly["year"], y=yearly["lo"],
    mode="lines", name="Min", line=dict(color="rgba(80,130,255,0.3)", width=1),
    fill="tonexty", fillcolor="rgba(200,220,255,0.2)",
))
fig_trend.add_trace(go.Scatter(
    x=yearly["year"], y=yearly["avg"],
    mode="lines", name="Avg", line=dict(color="#E06C00", width=2),
))
fig_trend.update_layout(
    xaxis_title="Year", yaxis_title="Temperature (°C)",
    hovermode="x unified", height=380,
    legend=dict(orientation="h", yanchor="bottom", y=1.02),
)
st.plotly_chart(fig_trend, use_container_width=True)

# ── Monthly profile + Seasonal bars ──────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Average Monthly Profile")
    monthly_profile = (
        monthly.groupby("month")
        .agg(avg=("avg_temp_c", "mean"), lo=("min_temp_c", "min"), hi=("max_temp_c", "max"))
        .reset_index()
    )
    monthly_profile["month_name"] = monthly_profile["month"].apply(
        lambda m: MONTH_NAMES[m - 1]
    )
    fig_monthly = go.Figure()
    fig_monthly.add_trace(go.Bar(
        x=monthly_profile["month_name"], y=monthly_profile["avg"],
        name="Avg", marker_color="#E06C00",
        error_y=dict(
            type="data", symmetric=False,
            array=(monthly_profile["hi"] - monthly_profile["avg"]).tolist(),
            arrayminus=(monthly_profile["avg"] - monthly_profile["lo"]).tolist(),
        ),
    ))
    fig_monthly.update_layout(
        xaxis_title="Month", yaxis_title="°C", height=340, showlegend=False
    )
    st.plotly_chart(fig_monthly, use_container_width=True)

with col_right:
    st.subheader("Seasonal Averages")
    seasonal_profile = (
        seasonal.groupby("season")
        .agg(avg=("avg_temp_c", "mean"), lo=("min_temp_c", "min"), hi=("max_temp_c", "max"))
        .reindex(SEASON_ORDER)
        .reset_index()
    )
    fig_seasonal = go.Figure()
    fig_seasonal.add_trace(go.Bar(
        x=seasonal_profile["season"],
        y=seasonal_profile["avg"],
        marker_color=[SEASON_COLORS[s] for s in seasonal_profile["season"]],
        error_y=dict(
            type="data", symmetric=False,
            array=(seasonal_profile["hi"] - seasonal_profile["avg"]).tolist(),
            arrayminus=(seasonal_profile["avg"] - seasonal_profile["lo"]).tolist(),
        ),
    ))
    fig_seasonal.update_layout(
        xaxis_title="Season", yaxis_title="°C", height=340, showlegend=False
    )
    st.plotly_chart(fig_seasonal, use_container_width=True)

# ── Heatmap ───────────────────────────────────────────────────────────────────
st.subheader("Monthly Temperature Heatmap")
heatmap_data = (
    monthly.groupby(["year", "month"])["avg_temp_c"]
    .mean()
    .reset_index()
    .pivot(index="year", columns="month", values="avg_temp_c")
)
heatmap_data.columns = [MONTH_NAMES[m - 1] for m in heatmap_data.columns]

fig_heat = px.imshow(
    heatmap_data,
    color_continuous_scale="RdBu_r",
    aspect="auto",
    labels=dict(color="°C"),
)
fig_heat.update_layout(height=500, xaxis_title="Month", yaxis_title="Year")
st.plotly_chart(fig_heat, use_container_width=True)
