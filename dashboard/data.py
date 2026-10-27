"""All data access for the dashboard. The app never queries tables directly.

Reads gold only:
    gold/line_hourly         one line, one hour: speed sums + weather
    gold/speed_grid_hourly   one map square, line and hour: speed sums
    gold/collection_hourly   one hour of data collection: polls, records, vehicles
    gold/dq_results          one check, one run
"""

import os
import re
from datetime import date

import duckdb
import pandas as pd
import streamlit as st

LAKE = os.getenv("LAKE", "/home/ykhalil/projects/warsaw-transit-pipeline/data/lake")
TZ = "Europe/Warsaw"

# Map grid size. Keep in sync with spark/jobs/gold_speeds.py
CELL_LAT = 0.0025
CELL_LON = 0.004


# Warsaw hour of a UTC timestamp. The ::TIMESTAMPTZ cast makes this correct whether the column
# arrives with a time zone (Delta) or without one (plain Parquet), since the session runs in UTC.
LOCAL_HOUR = f"hour(timezone('{TZ}', hour_utc::TIMESTAMPTZ))"


def _table(name: str) -> str:
    return f"delta_scan('{LAKE}/gold/{name}')"


def _dates(column: str, start: date, end: date) -> str:
    return f"{column} BETWEEN DATE '{start}' AND DATE '{end}'"


def _lines(lines: tuple[str, ...]) -> str:
    """Extra WHERE condition for a line filter. Empty tuple = all lines."""
    if not lines:
        return ""
    quoted = ", ".join("'" + line.replace("'", "''") + "'" for line in lines)
    return f"AND line IN ({quoted})"


@st.cache_resource
def _con():
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta; SET TimeZone = 'UTC';")
    return con


def _query(sql: str) -> pd.DataFrame:
    return _con().execute(sql).df()


# ---------- Overview ----------

@st.cache_data(ttl=600)
def headline() -> dict:
    row = _query(f"""
        SELECT sum(records)               AS records,
               count(DISTINCT event_date) AS days,
               max(live_vehicles) FILTER (WHERE hour_utc >= now() - INTERVAL 24 HOUR) AS vehicles,
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
    hour_from: int,
    hour_to: int,
    min_segments: int = 30,
) -> pd.DataFrame:
    """Commercial speed per map square, summed over the selected days, lines and hours."""
    types = ", ".join(f"'{t}'" for t in vehicle_types)
    return _query(f"""
        SELECT cell_lat, cell_lon,
               sum(total_distance_m) / sum(total_seconds) * 3.6 AS speed_kmh,
               sum(segments) AS segments
        FROM {_table('speed_grid_hourly')}
        WHERE {_dates('event_date', start, end)}
          AND vehicle_type IN ({types})
          {_lines(lines)}
          AND {LOCAL_HOUR} >= {hour_from}
          AND {LOCAL_HOUR} < {hour_to}
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