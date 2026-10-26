import json
from datetime import datetime, timezone

import requests

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARS = [
    "temperature_2m", "precipitation", "rain", "showers", "snowfall",
    "weather_code", "wind_speed_10m", "relative_humidity_2m",
]


def fetch_to_bronze(bronze_path: str, latitude: float, longitude: float, past_days: int) -> str:
    """Call Open-Meteo and append the raw response as one row to the bronze table."""
    import pyarrow as pa
    from deltalake import write_deltalake

    fetched_at = datetime.now(timezone.utc)
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(HOURLY_VARS),
        "past_days": past_days,
        "forecast_days": 1,
        "timezone": "UTC",
    }
    response = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    response.raise_for_status()

    row = pa.table({
        "fetched_at": pa.array([fetched_at], pa.timestamp("us", tz="UTC")),
        "fetch_date": pa.array([fetched_at.date()], pa.date32()),
        "source": ["open-meteo"],
        "request_params": [json.dumps(params)],
        "raw_json": [response.text],
    })
    write_deltalake(bronze_path, row, mode="append", partition_by=["fetch_date"])
    return fetched_at.isoformat()


def bronze_to_hourly(bronze):
    """Pure transformation: bronze rows (fetched_at, raw_json) -> one row per observed hour."""
    import pandas as pd

    frames = []
    for fetch in bronze.itertuples(index=False):
        hourly = pd.DataFrame(json.loads(fetch.raw_json)["hourly"])
        # Open-Meteo labels an hour by its END; we use the START, like the speed tables
        hourly["hour_utc"] = pd.to_datetime(hourly.pop("time"), utc=True) - pd.Timedelta(hours=1)
        hourly["fetched_at"] = pd.to_datetime(fetch.fetched_at, utc=True)
        frames.append(hourly)

    hours = pd.concat(frames, ignore_index=True)

    hours = hours[hours["hour_utc"] + pd.Timedelta(hours=1) <= hours["fetched_at"]]

    # Same hour fetched many times: keep the newest value
    silver = (
        hours.sort_values("fetched_at")
        .drop_duplicates("hour_utc", keep="last")
        .sort_values("hour_utc")
        .reset_index(drop=True)
    )

    # Delta stores timestamps in microseconds
    for col in ("hour_utc", "fetched_at"):
        silver[col] = silver[col].astype("datetime64[us, UTC]")
    return silver


def rebuild_silver(bronze_path: str, silver_path: str) -> int:
    """Read all of bronze, transform, and overwrite silver. Idempotent."""
    from deltalake import DeltaTable, write_deltalake

    bronze = DeltaTable(bronze_path).to_pandas(columns=["fetched_at", "raw_json"])
    silver = bronze_to_hourly(bronze)
    write_deltalake(silver_path, silver, mode="overwrite", schema_mode="overwrite")
    return len(silver)