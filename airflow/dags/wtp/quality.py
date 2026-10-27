"""Data quality checks on the lake, run with DuckDB."""

from dataclasses import dataclass
from datetime import datetime, timezone

from wtp.alerts import DataQualityFailed, send_alert


@dataclass
class Check:
    name: str
    layer: str
    sql: str
    warn: float
    fail: float
    bad_when: str  # "above": high values are bad, "below": low values are bad


# Seconds between our ingestion and the vehicle's own timestamp (Warsaw local time, no offset)
STALENESS_SQL = (
    "epoch(TRY_CAST(ingested_at AS TIMESTAMPTZ))"
    " - epoch(timezone('Europe/Warsaw', TRY_CAST(\"Time\" AS TIMESTAMP)))"
)
# ZTM service area, same box as the silver filter
IN_SERVICE_AREA_SQL = '"Lat" BETWEEN 51.8 AND 52.8 AND "Lon" BETWEEN 20.3 AND 21.9'


def build_checks(lake: str) -> list[Check]:
    bronze = f"delta_scan('{lake}/bronze/vehicle_positions')"
    silver = f"delta_scan('{lake}/silver/vehicle_positions')"
    gold = f"delta_scan('{lake}/gold/line_hourly_speed')"
    gold_weather = f"delta_scan('{lake}/gold/line_hourly')"
    weather = f"delta_scan('{lake}/silver/weather_hourly')"

    recent_bronze = "ingest_date >= current_date - 1"
    recent_silver = "event_date >= current_date - 1"
    last_hour_bronze = f"{recent_bronze} AND kafka_timestamp >= now() - INTERVAL 1 HOUR"
    last_hour_silver = f"{recent_silver} AND ingested_ts >= now() - INTERVAL 1 HOUR"

    return [
        # ---------- Freshness ----------
        Check(
            "bronze_freshness_minutes", "bronze",
            f"SELECT epoch(now() - max(kafka_timestamp)) / 60 FROM {bronze} WHERE {recent_bronze}",
            warn=12, fail=30, bad_when="above",
        ),
        Check(
            "silver_lag_minutes", "silver",
            f"""SELECT epoch(
                    (SELECT max(kafka_timestamp) FROM {bronze} WHERE {recent_bronze})
                  - (SELECT max(ingested_ts) FROM {silver} WHERE {recent_silver})
                ) / 60""",
            warn=12, fail=30, bad_when="above",
        ),
        Check(
            "gold_freshness_minutes", "gold",
            f"SELECT epoch(now() - max(hour_utc)) / 60 FROM {gold} WHERE {recent_silver}",
            warn=90, fail=180, bad_when="above",
        ),
        Check(
            "weather_freshness_minutes", "silver",
            f"SELECT epoch(now() - (max(hour_utc) + INTERVAL 1 HOUR)) / 60 FROM {weather}",
            warn=90, fail=180, bad_when="above",
        ),

        # ---------- Volume ----------
        Check(
            "polls_last_hour", "bronze",
            f"SELECT count(DISTINCT poll_id) FROM {bronze} WHERE {last_hour_bronze}",
            warn=240, fail=120, bad_when="below",
        ),
        Check(
            "live_vehicles_last_hour", "silver",
            f"SELECT count(DISTINCT vehicle_number) FROM {silver} WHERE {last_hour_silver}",
            warn=20, fail=20, bad_when="below",
        ),

        # ---------- Validity ----------
        Check(
            "bronze_unparsed_pct", "bronze",
            f"""SELECT 100.0 * count(*) FILTER (
                    WHERE "VehicleNumber" IS NULL OR "Lat" IS NULL OR "Lon" IS NULL OR "Time" IS NULL
                ) / nullif(count(*), 0)
                FROM {bronze} WHERE {last_hour_bronze}""",
            warn=0.1, fail=5, bad_when="above",
        ),
        Check(
            # Live vehicles only: ghosts parked far away (factories, abroad) are removed in silver anyway
            "live_invalid_coords_vehicles", "bronze",
            f"""SELECT count(DISTINCT "VehicleNumber") FROM {bronze}
                WHERE {last_hour_bronze}
                  AND {STALENESS_SQL} < 120
                  AND NOT ({IN_SERVICE_AREA_SQL})""",
            warn=3, fail=20, bad_when="above",
        ),
        Check(
            "bronze_ghost_pct", "bronze",
            f"""SELECT 100.0 * count(*) FILTER (WHERE {STALENESS_SQL} >= 120) / nullif(count(*), 0)
                FROM {bronze} WHERE {last_hour_bronze}""",
            warn=60, fail=85, bad_when="above",
        ),
        Check(
            # Last finished hour; the newest hour in gold is still in progress
            "city_speed_low_kmh", "gold",
            f"""SELECT sum(total_distance_m) / nullif(sum(total_seconds), 0) * 3.6
                FROM {gold}
                WHERE {recent_silver}
                  AND hour_utc = (SELECT max(hour_utc) - INTERVAL 1 HOUR FROM {gold} WHERE {recent_silver})""",
            warn=10, fail=5, bad_when="below",
        ),
        Check(
            "city_speed_high_kmh", "gold",
            f"""SELECT sum(total_distance_m) / nullif(sum(total_seconds), 0) * 3.6
                FROM {gold}
                WHERE {recent_silver}
                  AND hour_utc = (SELECT max(hour_utc) - INTERVAL 1 HOUR FROM {gold} WHERE {recent_silver})""",
            warn=40, fail=60, bad_when="above",
        ),

        # ---------- Uniqueness ----------
        Check(
            "silver_duplicates", "silver",
            f"""SELECT count(*) FROM (
                    SELECT vehicle_number, event_time_utc
                    FROM {silver}
                    WHERE {recent_silver} AND ingested_ts >= now() - INTERVAL 2 HOUR
                    GROUP BY ALL HAVING count(*) > 1
                )""",
            warn=0, fail=0, bad_when="above",
        ),
        Check(
            "bronze_duplicate_offsets", "bronze",
            f"""SELECT count(*) FROM (
                    SELECT kafka_partition, kafka_offset
                    FROM {bronze}
                    WHERE {recent_bronze} AND kafka_timestamp >= now() - INTERVAL 2 HOUR
                    GROUP BY ALL HAVING count(*) > 1
                )""",
            warn=0, fail=0, bad_when="above",
        ),

        # ---------- Consistency ----------
        Check(
            "weather_missing_hours_48h", "silver",
            f"""SELECT (epoch(max(hour_utc) - min(hour_utc)) / 3600 + 1) - count(DISTINCT hour_utc)
                FROM {weather}
                WHERE hour_utc >= now() - INTERVAL 48 HOUR""",
            warn=0, fail=3, bad_when="above",
        ),
        Check(
            # Excludes the newest hour, which has no observed weather yet; empty = nothing to check
            "weather_join_coverage_pct", "gold",
            f"""SELECT coalesce(
                    100.0 * count(*) FILTER (WHERE precipitation IS NOT NULL) / nullif(count(*), 0),
                    100)
                FROM {gold_weather}
                WHERE {recent_silver}
                  AND hour_utc >= now() - INTERVAL 24 HOUR
                  AND hour_utc < (SELECT max(hour_utc) FROM {gold_weather} WHERE {recent_silver})""",
            warn=99, fail=90, bad_when="below",
        ),
    ]


def evaluate(value, check: Check) -> str:
    """No value at all (empty table, missing data) always counts as a failure."""
    if value is None:
        return "fail"
    if check.bad_when == "above":
        if value > check.fail:
            return "fail"
        return "warn" if value > check.warn else "pass"
    if value < check.fail:
        return "fail"
    return "warn" if value < check.warn else "pass"


def previous_statuses(con, results_path: str) -> dict:
    """Latest status of each check from earlier runs ({} on the very first run)."""
    try:
        rows = con.execute(
            f"SELECT check_name, arg_max(status, checked_at) FROM delta_scan('{results_path}') GROUP BY check_name"
        ).fetchall()
        return dict(rows)
    except Exception:
        return {}


def run_all(lake: str, results_path: str) -> list[dict]:
    import duckdb
    import pandas as pd
    from deltalake import write_deltalake

    con = duckdb.connect()
    con.execute("SET extension_directory = '/tmp/duckdb_extensions'")
    con.execute("SET memory_limit = '1GB'")
    con.execute("SET TimeZone = 'UTC'")
    con.execute("INSTALL delta; LOAD delta;")

    checked_at = datetime.now(timezone.utc)
    previous = previous_statuses(con, results_path)

    results = []
    for check in build_checks(lake):
        try:
            value = con.execute(check.sql).fetchone()[0]
            value = None if value is None else float(value)
            status = evaluate(value, check)
            error = None
        except Exception as e:
            value, status, error = None, "fail", str(e)[:500]
        results.append({
            "check_name": check.name,
            "layer": check.layer,
            "status": status,
            "observed": value,
            "warn_threshold": float(check.warn),
            "fail_threshold": float(check.fail),
            "error": error,
            "checked_at": checked_at,
        })

    # Store every result, forever
    df = pd.DataFrame(results)
    df["checked_at"] = df["checked_at"].astype("datetime64[us, UTC]")
    df["check_date"] = checked_at.date()
    write_deltalake(results_path, df, mode="append", partition_by=["check_date"])

    # Alert only when the status changes
    for r in results:
        before = previous.get(r["check_name"])
        if r["status"] == "fail" and before != "fail":
            send_alert(
                title=f"DQ FAIL: {r['check_name']}",
                message=f"observed={r['observed']} fail threshold={r['fail_threshold']} {r['error'] or ''}",
            )
        elif r["status"] != "fail" and before == "fail":
            send_alert(
                title=f"DQ recovered: {r['check_name']}",
                message=f"observed={r['observed']}",
                priority="default",
                tags="white_check_mark",
            )

    failed = [r["check_name"] for r in results if r["status"] == "fail"]
    if failed:
        raise DataQualityFailed(f"failing checks: {', '.join(failed)}")
    return results