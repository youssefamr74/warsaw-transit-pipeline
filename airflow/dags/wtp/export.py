"""
Export the gold tables the dashboard needs as small Parquet files, and publish them
to a separate public GitHub repo that the online dashboard reads from.

The published repo always holds exactly one commit: each publish replaces the previous one,
so the repo never grows with daily history.
"""

import base64
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

TZ = "Europe/Warsaw"

TIME_WINDOW_SQL = """
    CASE
        WHEN h < 7  THEN 'night'
        WHEN h < 10 THEN 'morning_peak'
        WHEN h < 15 THEN 'midday'
        WHEN h < 19 THEN 'afternoon_peak'
        ELSE 'evening'
    END
"""

DQ_DAYS = 7

README = """# Warsaw Transit Pulse: dashboard data

Aggregated data published nightly by the
[warsaw-transit-pipeline](https://github.com/{owner}/warsaw-transit-pipeline) project,
read by its public dashboard. This repo is overwritten on every export.

| File | One row = |
|---|---|
| `line_hourly.parquet` | one line, one hour: speed sums and weather |
| `speed_grid.parquet` | one map square (~275 m), line, day and time window: speed sums |
| `collection_hourly.parquet` | one hour of data collection |
| `dq_results.parquet` | one data quality check run (last {dq_days} days) |
| `manifest.json` | export time, date range and row counts |

Speeds are stored as sums (`total_distance_m`, `total_seconds`), so any filter or roll-up
is computed correctly as total distance / total time.

**Sources:** Źródłem pochodzenia danych jest Miasto Stołeczne Warszawa (dane.um.warszawa.pl).
Weather data by Open-Meteo.com (CC BY 4.0).
"""


def export_files(lake: str, out_dir: str) -> dict:
    """Write the dashboard files to out_dir (replacing its contents). Returns the manifest."""
    import duckdb

    out = Path(out_dir)
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    con = duckdb.connect()
    con.execute("SET extension_directory = '/tmp/duckdb_extensions'")
    con.execute("SET memory_limit = '2GB'")
    con.execute("SET TimeZone = 'UTC'")
    con.execute("INSTALL delta; LOAD delta;")

    def gold(name: str) -> str:
        return f"delta_scan('{lake}/gold/{name}')"

    def write(sql: str, filename: str) -> int:
        path = out / filename
        con.execute(f"COPY ({sql}) TO '{path}' (FORMAT parquet, COMPRESSION zstd)")
        return con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]

    counts = {}
    counts["line_hourly.parquet"] = write(f"""
        SELECT event_date, hour_utc, vehicle_type, line,
               total_distance_m, total_seconds, segments, precip_category
        FROM {gold('line_hourly')}
    """, "line_hourly.parquet")

    counts["speed_grid.parquet"] = write(f"""
        SELECT event_date, time_window, vehicle_type, line, cell_lat, cell_lon,
               sum(total_distance_m)::FLOAT AS total_distance_m,
               sum(total_seconds)::INTEGER  AS total_seconds,
               sum(segments)::INTEGER       AS segments
        FROM (
            SELECT *, {TIME_WINDOW_SQL} AS time_window
            FROM (
                SELECT *, hour(timezone('{TZ}', hour_utc::TIMESTAMPTZ)) AS h
                FROM {gold('speed_grid_hourly')}
            )
        )
        GROUP BY ALL
    """, "speed_grid.parquet")

    counts["collection_hourly.parquet"] = write(
        f"SELECT * FROM {gold('collection_hourly')}", "collection_hourly.parquet"
    )

    counts["dq_results.parquet"] = write(f"""
        SELECT *
        FROM {gold('dq_results')}
        WHERE check_date >= current_date - INTERVAL {DQ_DAYS} DAY
    """, "dq_results.parquet")

    first, last = con.execute(
        f"SELECT min(event_date), max(event_date) FROM {gold('line_hourly')}"
    ).fetchone()

    manifest = {
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "first_date": str(first),
        "last_date": str(last),
        "rows": counts,
        "bytes": {f.name: f.stat().st_size for f in out.iterdir()},
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def publish_to_github(folder: str, repo: str, token: str, branch: str = "main") -> str:
    """Replace the repo's content with the files in folder, as one commit with no history.
    Uses the GitHub Git Data API, so no git installation is needed."""
    import requests

    api = f"https://api.github.com/repos/{repo}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    def call(method: str, url: str, payload: dict) -> dict:
        response = requests.request(method, url, json=payload, headers=headers, timeout=300)
        if response.status_code >= 400:

            raise RuntimeError(f"GitHub API {method} {url} -> {response.status_code}: {response.text[:300]}")
        return response.json()

    owner = repo.split("/")[0]
    files = {f.name: f.read_bytes() for f in sorted(Path(folder).iterdir()) if f.is_file()}
    files["README.md"] = README.format(owner=owner, dq_days=DQ_DAYS).encode()

    tree = []
    for name, content in files.items():
        blob = call("POST", f"{api}/git/blobs", {
            "content": base64.b64encode(content).decode(),
            "encoding": "base64",
        })
        tree.append({"path": name, "mode": "100644", "type": "blob", "sha": blob["sha"]})

    tree_sha = call("POST", f"{api}/git/trees", {"tree": tree})["sha"]
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    commit_sha = call("POST", f"{api}/git/commits", {
        "message": f"Data export {stamp}",
        "tree": tree_sha,
        "parents": [],
    })["sha"]
    call("PATCH", f"{api}/git/refs/heads/{branch}", {"sha": commit_sha, "force": True})
    return commit_sha
