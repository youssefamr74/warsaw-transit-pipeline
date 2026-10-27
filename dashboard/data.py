"""All data access for the dashboard. The app never queries tables directly."""

import os
from datetime import date

import duckdb
import pandas as pd
import streamlit as st

LAKE = os.getenv("LAKE", "/home/ykhalil/projects/warsaw-transit-pipeline/data/lake")
TZ = "Europe/Warsaw"

# Map grid: about 275 m x 275 m at Warsaw's latitude
CELL_LAT = 0.0025
CELL_LON = 0.004


def _table(path: str) -> str:
    return f"delta_scan('{LAKE}/{path}')"


def _dates(column: str, start: date, end: date) -> str:
    return f"{column} BETWEEN DATE '{start}' AND DATE '{end}'"


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
        SELECT
            (SELECT count(*) FROM {_table('bronze/vehicle_positions')})                       AS records,
            (SELECT count(DISTINCT ingest_date) FROM {_table('bronze/vehicle_positions')})    AS days,
            (SELECT count(DISTINCT vehicle_number) FROM {_table('silver/vehicle_positions')}) AS vehicles,
            (SELECT max(ingested_ts) FROM {_table('silver/vehicle_positions')})               AS last_update
    """).iloc[0]
    return row.to_dict()


@st.cache_data(ttl=600)
def available_dates() -> list[date]:
    """UTC dates that have gold speed data."""
    df = _query(f"""
        SELECT DISTINCT event_date FROM {_table('gold/line_hourly_speed')} ORDER BY 1
    """)
    return [d.date() if hasattr(d, "date") else d for d in df["event_date"]]


# ---------- Speeds ----------

@st.cache_data(ttl=600)
def speed_by_hour(start: date, end: date) -> pd.DataFrame:
    """Commercial speed per hour of day (Warsaw time), with how many hours each point is based on."""
    return _query(f"""
        SELECT hour(timezone('{TZ}', hour_utc)) AS hour,
               vehicle_type,
               sum(total_distance_m) / sum(total_seconds) * 3.6 AS speed_kmh,
               count(DISTINCT hour_utc) AS hours_observed
        FROM {_table('gold/city_hourly_weather')}
        WHERE {_dates('event_date', start, end)}
        GROUP BY ALL
        ORDER BY hour
    """)


@st.cache_data(ttl=600)
def line_speeds(start: date, end: date, min_segments: int = 2000) -> pd.DataFrame:
    return _query(f"""
        SELECT vehicle_type, line,
               sum(total_distance_m) / sum(total_seconds) * 3.6 AS speed_kmh,
               sum(segments) AS segments
        FROM {_table('gold/line_hourly_speed')}
        WHERE {_dates('event_date', start, end)}
        GROUP BY ALL
        HAVING sum(segments) >= {min_segments}
        ORDER BY speed_kmh
    """)


@st.cache_data(ttl=600)
def rain_vs_dry(start: date, end: date) -> pd.DataFrame:
    """Bus speed per hour of day, dry vs wet, so each hour is compared with itself."""
    return _query(f"""
        SELECT hour(timezone('{TZ}', hour_utc)) AS hour,
               CASE WHEN precip_category = 'dry' THEN 'dry' ELSE 'wet' END AS weather,
               sum(total_distance_m) / sum(total_seconds) * 3.6 AS speed_kmh,
               count(DISTINCT hour_utc) AS hours_observed
        FROM {_table('gold/city_hourly_weather')}
        WHERE precip_category IS NOT NULL
          AND vehicle_type = 'bus'
          AND {_dates('event_date', start, end)}
        GROUP BY ALL
        ORDER BY hour
    """)


# ---------- Map ----------

@st.cache_data(ttl=600)
def speed_grid(
    start: date,
    end: date,
    vehicle_types: tuple[str, ...],
    hour_from: int,
    hour_to: int,
    min_segments: int = 30,
) -> pd.DataFrame:
    """Commercial speed per map cell. Each segment is placed at the midpoint of its two readings."""
    types = ", ".join(f"'{t}'" for t in vehicle_types)
    return _query(f"""
        WITH seg AS (
            SELECT (prev_lat + lat) / 2 AS mid_lat,
                   (prev_lon + lon) / 2 AS mid_lon,
                   distance_m,
                   gap_seconds
            FROM {_table('gold/vehicle_segments')}
            WHERE {_dates('event_date', start, end)}
              AND NOT is_layover
              AND vehicle_type IN ({types})
              AND hour(timezone('{TZ}', event_time_utc)) >= {hour_from}
              AND hour(timezone('{TZ}', event_time_utc)) < {hour_to}
        )
        SELECT floor(mid_lat / {CELL_LAT}) * {CELL_LAT} AS cell_lat,
               floor(mid_lon / {CELL_LON}) * {CELL_LON} AS cell_lon,
               sum(distance_m) / sum(gap_seconds) * 3.6 AS speed_kmh,
               count(*) AS segments
        FROM seg
        GROUP BY ALL
        HAVING count(*) >= {min_segments}
    """)


# ---------- Pipeline health ----------

@st.cache_data(ttl=600)
def polls_per_hour(start: date, end: date) -> pd.DataFrame:
    """How many polls reached bronze each hour (UTC). Gaps in collection show up as zeros."""
    return _query(f"""
        SELECT date_trunc('hour', kafka_timestamp) AS hour_utc,
               count(DISTINCT poll_id) AS polls
        FROM {_table('bronze/vehicle_positions')}
        WHERE {_dates('ingest_date', start, end)}
        GROUP BY ALL
        ORDER BY 1
    """)


@st.cache_data(ttl=300)
def latest_checks() -> pd.DataFrame:
    return _query(f"""
        SELECT check_name, layer, status, round(observed, 2) AS observed,
               warn_threshold, fail_threshold, checked_at
        FROM {_table('gold/dq_results')}
        WHERE checked_at = (SELECT max(checked_at) FROM {_table('gold/dq_results')})
        ORDER BY CASE status WHEN 'fail' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END, check_name
    """)