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
MAX_COMPARED_LINES = 8

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

line_options = data.available_lines(start, end)
line_labels = {row.line: f"{row.line} ({row.vehicle_type})" for row in line_options.itertuples()}
selected_lines = st.sidebar.multiselect(
    "Lines",
    options=list(line_labels),
    format_func=line_labels.get,
    placeholder="All lines",
    help="Leave empty for all lines. Pick one or more to focus every chart on them.",
)
lines = tuple(selected_lines)

compare = False
if len(lines) > 1:
    compare = st.sidebar.toggle(
        "Compare lines separately", value=len(lines) <= MAX_COMPARED_LINES,
        help="On: one curve per line. Off: the selected lines combined into one.",
    )
    if compare and len(lines) > MAX_COMPARED_LINES:
        st.sidebar.caption(f"Tip: with more than {MAX_COMPARED_LINES} lines the chart gets crowded.")

lines_text = ", ".join(lines) if lines else "all lines"
lines_label = f"line {lines[0]}" if len(lines) == 1 else f"lines {lines_text}"

# ---------- Header ----------
st.title("Warsaw Transit Pulse")
st.caption(
    "Live bus and tram positions from the City of Warsaw, processed with Kafka, "
    "Spark Structured Streaming and Delta Lake, orchestrated by Airflow."
)
st.info(
    f"Showing **{start:%d %b} to {end:%d %b %Y}** ({len(days_with_data)} day(s) with data), "
    f"**{lines_label if lines else lines_text}**. Charts combine all selected days. Change dates and lines in the sidebar."
)

h = data.headline()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Positions collected (all time)", f"{int(h['records']):,}")
c2.metric("Days of data (all time)", int(h["days"]))
c3.metric(
    "Vehicles in service (busiest hour, last 24 h)",
    f"{int(h['vehicles']):,}" if pd.notna(h["vehicles"]) else "–",
)
c4.metric("Latest hour of data (UTC)", h["last_update"].strftime("%Y-%m-%d %H:00"))

# ---------- Map ----------
st.subheader("Where is it slow?" if not lines else f"Where is it slow? {lines_label.capitalize()}")
m1, m2 = st.columns(2)
window = m1.selectbox("Time of day (Warsaw time)", list(TIME_WINDOWS), index=1)
if lines:
    # The selected lines already decide the vehicle types
    types = sorted(line_options[line_options.line.isin(lines)].vehicle_type.unique())
    m2.markdown(f"**Vehicles:** {', '.join(types)} (from the selected lines)")
else:
    types = m2.multiselect("Vehicles", ["bus", "tram"], default=["bus", "tram"])

if types:
    hour_from, hour_to = TIME_WINDOWS[window]
    # A single line has far fewer segments per square, so show squares with less data
    min_segments = 30 if not lines else 5
    grid = data.speed_grid(start, end, tuple(types), lines, hour_from, hour_to, min_segments)
    if grid.empty:
        st.warning("No data for this selection.")
    else:
        # Colour scale: fixed for the whole city, relative to the selection for single lines,
        # so a slow line still shows where along its route it is slowest
        if lines:
            low, high = grid["speed_kmh"].quantile([0.1, 0.9])
        else:
            low, high = 5.0, 35.0
        t = ((grid["speed_kmh"] - low) / max(high - low, 1)).clip(0, 1)

        def speed_color(x: float) -> list[int]:
            """0 = red, 0.5 = yellow, 1 = green."""
            if x < 0.5:
                return [230, int(60 + 330 * x), 50, 190]          # red -> yellow
            return [int(230 - 380 * (x - 0.5)), 200, 50, 190]     # yellow -> green

        grid["color"] = [speed_color(x) for x in t]
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
        if lines:
            scale_text = (
                f"Colours are relative to the selected {'line' if len(lines) == 1 else 'lines'}: "
                f"red = {low:.0f} km/h or less (their slowest 10%), yellow = in between, "
                f"green = {high:.0f} km/h or more (their fastest 10%). "
            )
        else:
            scale_text = "Red = 5 km/h or less, yellow = about 20 km/h, green = 35 km/h or more. "
        st.caption(
            "Each square is about 275 m wide. " + scale_text +
            "Commercial speed: stops at bus stops and traffic lights count, layovers at terminals don't. "
            f"Squares with fewer than {min_segments} segments are hidden."
        )
else:
    st.warning("Pick at least one vehicle type.")

# ---------- Speed by hour ----------
st.subheader("Commercial speed by hour of day")
st.caption(
    "Total distance ÷ total time in service, layovers at terminals excluded. Warsaw time. "
    "Gaps in the line are hours without data. Hover a point to see how much data it is based on."
)
if not lines:
    group_by = "vehicle_type"          # all lines: buses vs trams
elif len(lines) == 1 or compare:
    group_by = "line"                  # one curve per selected line
else:
    group_by = "combined"              # selected lines as one curve
sbh = data.speed_by_hour(start, end, lines, group_by)
if sbh.empty:
    st.warning("No data for this selection.")
else:
    # Fill in all 24 hours so missing hours show as breaks instead of straight lines
    grid_index = pd.MultiIndex.from_product(
        [range(24), sorted(sbh.series.unique(), key=str)], names=["hour", "series"]
    )
    sbh = sbh.set_index(["hour", "series"]).reindex(grid_index).reset_index()
    fig = px.line(
        sbh, x="hour", y="speed_kmh", color="series", markers=True,
        hover_data=["hours_observed", "segments"],
        labels={
            "hour": "Hour of day", "speed_kmh": "km/h", "series": "",
            "hours_observed": "Hours of data", "segments": "Segments",
        },
    )
    fig.update_xaxes(dtick=1, range=[-0.5, 23.5])
    st.plotly_chart(fig, width="stretch")

# ---------- Lines ----------
lines_df = data.line_speeds(start, end, lines)
if lines:
    st.subheader("Selected lines")
    if lines_df.empty:
        st.warning("No data for the selected lines and dates.")
    else:
        st.dataframe(lines_df.round(1), hide_index=True, width="stretch")
else:
    st.subheader("Slowest and fastest lines")
    if lines_df.empty:
        st.warning("Not enough data for the selected dates.")
    else:
        left, right = st.columns(2)
        left.markdown("**Slowest 10**")
        left.dataframe(lines_df.head(10).round(1), hide_index=True, width="stretch")
        right.markdown("**Fastest 10**")
        right.dataframe(lines_df.tail(10).iloc[::-1].round(1), hide_index=True, width="stretch")

# ---------- Rain vs dry ----------
st.subheader("Dry vs wet hours: " + ("buses" if not lines else (lines_label if len(lines) == 1 else f"{lines_label}, combined")))
rd = data.rain_vs_dry(start, end, lines)
dry_hours = int(rd[rd.weather == "dry"].hours_observed.sum())
wet_hours = int(rd[rd.weather == "wet"].hours_observed.sum())
st.caption(
    "Each hour of the day is split by its weather, so 8:00 on dry days is compared with 8:00 on wet days. "
    f"Based on {dry_hours} dry and {wet_hours} wet hours in the selection."
)
if rd.empty:
    st.warning("No hours with both speed and weather data for this selection.")
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
st.caption("Not affected by the line filter: this is about the pipeline itself.")

st.markdown("**Data collection: polls per hour (UTC)**")
st.caption(
    "About 290 (daytime) to 360 (night) polls an hour is normal. "
    "Missing bars are hours where nothing was collected."
)
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