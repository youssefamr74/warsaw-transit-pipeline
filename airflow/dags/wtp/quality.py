"""Data quality checks on the lake."""

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
    bad_when: str


def build_checks(lake: str) -> list[Check]:
    bronze = f"delta_scan('{lake}/bronze/vehicle_positions')"
    silver = f"delta_scan('{lake}/silver/vehicle_positions')"
    gold = f"delta_scan('{lake}/gold/line_hourly_speed')"
    weather = f"delta_scan('{lake}/silver/weather_hourly')"
    recent_bronze = "ingest_date >= current_date - 1"
    recent_silver = "event_date >= current_date - 1"

    return [
        Check(
            "bronze_freshness_minutes", "bronze",
            f"SELECT epoch(now() - max(kafka_timestamp)) / 60 FROM {bronze} WHERE {recent_bronze}",
            warn=10, fail=30, bad_when="above",
        ),
        Check(
            "silver_lag_minutes", "silver",
            f"""SELECT epoch(
                    (SELECT max(kafka_timestamp) FROM {bronze} WHERE {recent_bronze})
                  - (SELECT max(ingested_ts) FROM {silver} WHERE {recent_silver})
                ) / 60""",
            warn=5, fail=15, bad_when="above",
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
        Check(
            "polls_last_hour", "bronze",
            f"""SELECT count(DISTINCT poll_id) FROM {bronze}
                WHERE {recent_bronze} AND kafka_timestamp >= now() - INTERVAL 1 HOUR""",
            warn=240, fail=120, bad_when="below",
        ),
        Check(
            "live_vehicles_last_hour", "silver",
            f"""SELECT count(DISTINCT vehicle_number) FROM {silver}
                WHERE {recent_silver} AND ingested_ts >= now() - INTERVAL 1 HOUR""",
            warn=20, fail=20, bad_when="below",
        ),
    ]


def evaluate(value, check: Check) -> str:
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

    df = pd.DataFrame(results)
    df["checked_at"] = df["checked_at"].astype("datetime64[us, UTC]")
    df["check_date"] = checked_at.date()
    write_deltalake(results_path, df, mode="append", partition_by=["check_date"])


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