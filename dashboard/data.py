import os
import re
import tempfile
import urllib.request
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st


def _setting(name: str, default: str | None = None) -> str | None:
    """Environment variable first, then Streamlit secrets (used on Streamlit Cloud)."""
    value = os.getenv(name)
    if value:
        return value
    try:
        return st.secrets.get(name, default)
    except Exception: 
        return default


DATA_SOURCE = _setting("DATA_SOURCE", "lake")
LAKE = _setting("LAKE", "/home/ykhalil/projects/warsaw-transit-pipeline/data/lake")
DATA_REPO = _setting("DASHBOARD_DATA_REPO", "")
DATA_URL = _setting("DATA_URL", f"https://raw.githubusercontent.com/{DATA_REPO}/main")
TZ = "Europe/Warsaw"
TABLES = ["line_hourly", "speed_grid", "collection_hourly", "dq_results"]


CELL_LAT = 0.0025
CELL_LON = 0.004

LOCAL_HOUR = f"hour(timezone('{TZ}', hour_utc::TIMESTAMPTZ))"
TIME_WINDOW_SQL = """
    CASE
        WHEN h < 7  THEN 'night'
        WHEN h < 10 THEN 'morning_peak'
        WHEN h < 15 THEN 'midday'
        WHEN h < 19 THEN 'afternoon_peak'
        ELSE 'evening'
    END
"""


def _table(name: str) -> str:
    return name


def _dates(column: str, start: date, end: date) -> str:
    return f"{column} BETWEEN DATE '{start}' AND DATE '{end}'"


def _lines(lines: tuple[str, ...]) -> str:
    """Extra WHERE condition for a line filter. Empty tuple = all lines."""
    if not lines:
        return ""
    quoted = ", ".join("'" + line.replace("'", "''") + "'" for line in lines)
    return f"AND line IN ({quoted})"


def _lake_views(con) -> None:
    def gold(name: str) -> str:
        return f"delta_scan('{LAKE}/gold/{name}')"

    con.execute("INSTALL delta; LOAD delta;")
    for name in ["line_hourly", "collection_hourly", "dq_results"]:
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM {gold(name)}")
    con.execute(f"""
        CREATE VIEW speed_grid AS
        SELECT event_date, {TIME_WINDOW_SQL} AS time_window, vehicle_type, line,
               cell_lat, cell_lon, total_distance_m, total_seconds, segments
        FROM (SELECT *, {LOCAL_HOUR} AS h FROM {gold('speed_grid_hourly')})
    """)


def _published_views(con) -> None:
    """Download the published files once (a few MB) and query them locally."""
    folder = Path(tempfile.mkdtemp(prefix="wtp_"))
    for name in TABLES:
        path = folder / f"{name}.parquet"
        with urllib.request.urlopen(f"{DATA_URL}/{name}.parquet", timeout=60) as response:
            path.write_bytes(response.read())
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{path}')")


@st.cache_resource(ttl=3600 if DATA_SOURCE == "published" else None)
def _con():
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC';")
    if DATA_SOURCE == "published":
        _published_views(con)
    else:
        _lake_views(con)
    return con


def _query(sql: str) -> pd.DataFrame:
    return _con().execute(sql).df()


# ---------- Overview ----------

@st.cache_data(ttl=600)
def headline() -> dict:
    row = _query(f"""
        SELECT sum(records)               AS records,
               count(DISTINCT event_date) AS days,
               max(live_vehicles) FILTER (
                   WHERE hour_utc >= (SELECT max(hour_utc) FROM {_table('collection_hourly')}) - INTERVAL 24 HOUR
               ) AS vehicles,
               max(hour_utc)              AS last_update
        FROM {_table('collection_hourly')}
    """).iloc[0]
    return row.to_dict()


@st.cache_data(ttl=600)
def available_dates() -> list[date]:
    """UTC dates that have line speed data."""
    df = _query(f"SELECT DISTINCT event_date FROM {_table('line_hourly')} ORDER BY 1")
    return [d.date() if hasattr(d, "date") else d for d in df["event_date"]]


def _natural_key(line: str):
    """Sort 2, 10, 128, E-2, L19, N61 the way people expect: numbers by value, then letters."""
    m = re.fullmatch(r"(\D*)(\d*)(.*)", line)
    prefix, number, rest = m.groups()
    return (prefix != "", prefix, int(number) if number else 0, rest)


@st.cache_data(ttl=600)
def available_lines(start: date, end: date) -> pd.DataFrame:
    """Lines with data in the selected dates, sorted naturally."""
    df = _query(f"""
        SELECT line, any_value(vehicle_type) AS vehicle_type
        FROM {_table('line_hourly')}
        WHERE {_dates('event_date', start, end)}
        GROUP BY line
    """)
    df["sort_key"] = df["line"].map(_natural_key)
    return df.sort_values(["vehicle_type", "sort_key"]).drop(columns="sort_key").reset_index(drop=True)


# ---------- Line performance ----------

@st.cache_data(ttl=600)
def speed_by_hour(start: date, end: date, lines: tuple[str, ...], group_by: str) -> pd.DataFrame:
    """Commercial speed per hour of day (Warsaw time), one series per group:
    "vehicle_type" (bus / tram), "line" (one per line), or "combined" (all selected lines as one)."""
    series = {
        "vehicle_type": "vehicle_type",
        "line": "line",
        "combined": "'selected lines'",
    }[group_by]
    return _query(f"""
        SELECT {LOCAL_HOUR} AS hour,
               {series} AS series,
               sum(total_distance_m) / sum(total_seconds) * 3.6 AS speed_kmh,
               count(DISTINCT hour_utc) AS hours_observed,
               sum(segments) AS segments
        FROM {_table('line_hourly')}
        WHERE {_dates('event_date', start, end)} {_lines(lines)}
        GROUP BY ALL
        ORDER BY hour
    """)


@st.cache_data(ttl=600)
def line_speeds(start: date, end: date, lines: tuple[str, ...], min_segments: int = 2000) -> pd.DataFrame:
    """Speed per line over the selected dates. With a line filter, every selected line is shown."""
    min_seg = 0 if lines else min_segments
    return _query(f"""
        SELECT vehicle_type, line,
               sum(total_distance_m) / sum(total_seconds) * 3.6 AS speed_kmh,
               sum(segments) AS segments
        FROM {_table('line_hourly')}
        WHERE {_dates('event_date', start, end)} {_lines(lines)}
        GROUP BY ALL
        HAVING sum(segments) >= {min_seg}
        ORDER BY speed_kmh
    """)


@st.cache_data(ttl=600)
def rain_vs_dry(start: date, end: date, lines: tuple[str, ...]) -> pd.DataFrame:
    """Speed per hour of day, dry vs wet, so each hour is compared with itself.
    Without a line filter: buses only. With one: the selected lines, combined."""
    vehicle_filter = "" if lines else "AND vehicle_type = 'bus'"
    return _query(f"""
        SELECT {LOCAL_HOUR} AS hour,
               CASE WHEN precip_category = 'dry' THEN 'dry' ELSE 'wet' END AS weather,
               sum(total_distance_m) / sum(total_seconds) * 3.6 AS speed_kmh,
               count(DISTINCT hour_utc) AS hours_observed
        FROM {_table('line_hourly')}
        WHERE precip_category IS NOT NULL
          AND {_dates('event_date', start, end)}
          {vehicle_filter} {_lines(lines)}
        GROUP BY ALL
        ORDER BY hour
    """)


# ---------- Map ----------

@st.cache_data(ttl=600)
def speed_grid(
    start: date,
    end: date,
    vehicle_types: tuple[str, ...],
    lines: tuple[str, ...],
    time_window: str | None,
    min_segments: int = 30,
) -> pd.DataFrame:
    """Commercial speed per map square, summed over the selected days, lines and time window.
    time_window None = all day."""
    types = ", ".join(f"'{t}'" for t in vehicle_types)
    window_filter = f"AND time_window = '{time_window}'" if time_window else ""
    return _query(f"""
        SELECT cell_lat, cell_lon,
               sum(total_distance_m) / sum(total_seconds) * 3.6 AS speed_kmh,
               sum(segments) AS segments
        FROM {_table('speed_grid')}
        WHERE {_dates('event_date', start, end)}
          AND vehicle_type IN ({types})
          {_lines(lines)}
          {window_filter}
        GROUP BY ALL
        HAVING sum(segments) >= {min_segments}
    """)


# ---------- Pipeline health ----------

@st.cache_data(ttl=600)
def polls_per_hour(start: date, end: date) -> pd.DataFrame:
    """How many polls reached bronze each hour (UTC). Gaps in collection show up as zeros."""
    return _query(f"""
        SELECT hour_utc, polls
        FROM {_table('collection_hourly')}
        WHERE {_dates('event_date', start, end)}
        ORDER BY hour_utc
    """)


@st.cache_data(ttl=300)
def latest_checks() -> pd.DataFrame:
    return _query(f"""
        SELECT check_name, layer, status, round(observed, 2) AS observed,
               warn_threshold, fail_threshold, checked_at
        FROM {_table('dq_results')}
        WHERE checked_at = (SELECT max(checked_at) FROM {_table('dq_results')})
        ORDER BY CASE status WHEN 'fail' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END, check_name
    """)