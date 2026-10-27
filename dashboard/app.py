import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st

import data

st.set_page_config(page_title="Warsaw Transit Pulse", layout="wide")

TIME_WINDOWS = {
    "All day": (0, 24),
    "Morning peak (7-10)": (7, 10),
    "Midday (10-15)": (10, 15),
    "Afternoon peak (15-19)": (15, 19),
    "Evening (19-24)": (19, 24),
    "Night (0-7)": (0, 7),
}

# ---------- Sidebar filters ----------
dates = data.available_dates()
if not dates:
    st.error("No gold data yet.")
    st.stop()

st.sidebar.header("Filters")
picked = st.sidebar.date_input(
    "Dates (UTC)", value=(dates[0], dates[-1]), min_value=dates[0], max_value=dates[-1],
)
# While the user is picking a range, Streamlit returns only the start date
start, end = (picked[0], picked[-1]) if isinstance(picked, (list, tuple)) else (picked, picked)
days_with_data = [d for d in dates if start <= d <= end]

# ---------- Header ----------
st.title("Warsaw Transit Pulse")
st.caption(
    "Live bus and tram positions from the City of Warsaw, processed with Kafka, "
    "Spark Structured Streaming and Delta Lake, orchestrated by Airflow."
)
st.info(
    f"Showing **{start:%d %b} to {end:%d %b %Y}**: {len(days_with_data)} day(s) with data. "
    "Charts combine all selected days. Use the sidebar to pick a single day or a range."
)

h = data.headline()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Positions collected (all time)", f"{h['records']:,}")
c2.metric("Days of data (all time)", h["days"])
c3.metric("Vehicles tracked (all time)", f"{h['vehicles']:,}")
c4.metric("Last update (UTC)", h["last_update"].strftime("%Y-%m-%d %H:%M"))

# ---------- Map ----------
st.subheader("Where are buses and trams slow?")
m1, m2 = st.columns(2)
window = m1.selectbox("Time of day (Warsaw time)", list(TIME_WINDOWS), index=1)
types = m2.multiselect("Vehicles", ["bus", "tram"], default=["bus", "tram"])

if types:
    hour_from, hour_to = TIME_WINDOWS[window]
    grid = data.speed_grid(start, end, tuple(sorted(types)), hour_from, hour_to)
    if grid.empty:
        st.warning("No data for this selection.")
    else:
        # Colour: red = slow, green = fast, clipped to 5..35 km/h
        t = ((grid["speed_kmh"] - 5) / 30).clip(0, 1)
        grid["color"] = [[int(255 * (1 - x)), int(190 * x), 60, 170] for x in t]
        grid["polygon"] = [
            [[lon, lat], [lon + data.CELL_LON, lat],
             [lon + data.CELL_LON, lat + data.CELL_LAT], [lon, lat + data.CELL_LAT]]
            for lat, lon in zip(grid["cell_lat"], grid["cell_lon"])
        ]
        grid["speed_kmh"] = grid["speed_kmh"].round(1)

        layer = pdk.Layer(
            "PolygonLayer", grid,
            get_polygon="polygon", get_fill_color="color",
            stroked=False, pickable=True,
        )
        st.pydeck_chart(pdk.Deck(
            layers=[layer],
            initial_view_state=pdk.ViewState(latitude=52.23, longitude=21.01, zoom=10.5),
            map_provider="carto",
            map_style="light",
            tooltip={"text": "{speed_kmh} km/h\n{segments} segments"},
        ))
        st.caption(
            "Each square is about 275 m wide. Red = slow (5 km/h or less), green = fast (35 km/h or more). "
            "Commercial speed: stops at bus stops and traffic lights count, layovers at terminals don't. "
            "Squares with fewer than 30 segments are hidden."
        )
else:
    st.warning("Pick at least one vehicle type.")

# ---------- Speed by hour ----------
st.subheader("Commercial speed by hour of day")
st.caption(
    "Total distance ÷ total time in service, layovers at terminals excluded. Warsaw time. "
    "Gaps in the line are hours without data. Hover a point to see how many hours it is based on."
)
sbh = data.speed_by_hour(start, end)
if sbh.empty:
    st.warning("No data for the selected dates.")
else:
    # Fill in all 24 hours so missing hours show as breaks instead of straight lines
    grid_index = pd.MultiIndex.from_product(
        [range(24), sorted(sbh.vehicle_type.unique())], names=["hour", "vehicle_type"]
    )
    sbh = sbh.set_index(["hour", "vehicle_type"]).reindex(grid_index).reset_index()
    fig = px.line(
        sbh, x="hour", y="speed_kmh", color="vehicle_type", markers=True,
        hover_data=["hours_observed"],
        labels={
            "hour": "Hour of day", "speed_kmh": "km/h", "vehicle_type": "",
            "hours_observed": "Hours of data",
        },
    )
    fig.update_xaxes(dtick=1, range=[-0.5, 23.5])
    st.plotly_chart(fig, width="stretch")

# ---------- Slowest and fastest lines ----------
st.subheader("Slowest and fastest lines")
lines = data.line_speeds(start, end)
if lines.empty:
    st.warning("Not enough data for the selected dates.")
else:
    left, right = st.columns(2)
    left.markdown("**Slowest 10**")
    left.dataframe(lines.head(10).round(1), hide_index=True, width="stretch")
    right.markdown("**Fastest 10**")
    right.dataframe(lines.tail(10).iloc[::-1].round(1), hide_index=True, width="stretch")

# ---------- Rain vs dry ----------
st.subheader("Buses: dry vs wet hours")
rd = data.rain_vs_dry(start, end)
dry_hours = int(rd[rd.weather == "dry"].hours_observed.sum())
wet_hours = int(rd[rd.weather == "wet"].hours_observed.sum())
st.caption(
    "Each hour of the day is split by its weather, so 8:00 on dry days is compared with 8:00 on wet days. "
    f"Based on {dry_hours} dry and {wet_hours} wet hours in the selected dates."
)
if rd.empty:
    st.warning("No hours with both speed and weather data for the selected dates.")
else:
    fig = px.bar(
        rd, x="hour", y="speed_kmh", color="weather", barmode="group",
        hover_data=["hours_observed"],
        labels={
            "hour": "Hour of day", "speed_kmh": "km/h", "weather": "",
            "hours_observed": "Hours of data",
        },
    )
    fig.update_xaxes(dtick=1, range=[-0.5, 23.5])
    st.plotly_chart(fig, width="stretch")

# ---------- Pipeline health ----------
st.subheader("Pipeline health")

st.markdown("**Data collection: polls per hour (UTC)**")
st.caption("About 290 polls an hour is normal. Missing bars are hours where nothing was collected.")
pph = data.polls_per_hour(start, end)
if not pph.empty:
    full_range = pd.date_range(pph.hour_utc.min(), pph.hour_utc.max(), freq="h")
    pph = pph.set_index("hour_utc").reindex(full_range, fill_value=0).rename_axis("hour_utc").reset_index()
    fig = px.bar(pph, x="hour_utc", y="polls", labels={"hour_utc": "", "polls": "Polls"})
    st.plotly_chart(fig, width="stretch")

checks = data.latest_checks()
# Count before adding icons, since the counts compare the plain status values
passing = (checks.status == "pass").sum()
failing = (checks.status == "fail").sum()
st.markdown(
    f"**{passing} / {len(checks)} checks passing** "
    f"(last run {checks.checked_at.max():%Y-%m-%d %H:%M} UTC)"
    + (f" · ⚠️ {failing} failing" if failing else "")
)
icons = {"pass": "✅ pass", "warn": "⚠️ warn", "fail": "❌ fail"}
checks["status"] = checks["status"].map(icons)
st.dataframe(
    checks.drop(columns="checked_at"), hide_index=True, width="stretch",
    height=(len(checks) + 1) * 35 + 3,
)

st.caption(
    "Data: Miasto Stołeczne Warszawa (dane.um.warszawa.pl) · "
    "Weather: Open-Meteo.com (CC BY 4.0) · Map: © OpenStreetMap contributors, © CARTO"
)