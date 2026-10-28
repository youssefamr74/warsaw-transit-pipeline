from datetime import datetime

import pytest
from pyspark.sql import functions as F

from silver_positions import clean_positions, deduplicate

BRONZE_SCHEMA = (
    "VehicleNumber string, Lines string, Brigade string, Time string, Lat double, Lon double, "
    "ingested_at string, vehicle_type string, poll_id string, kafka_partition int, kafka_offset long"
)


def bronze_row(time, ingested_at, vehicle="1001", lat=52.23, lon=21.01, brigade="016", line=" 128 "):
    return (vehicle, line, brigade, time, lat, lon, ingested_at, "bus", "p1", 0, 0)


def clean(spark, rows):
    return clean_positions(spark.createDataFrame(rows, BRONZE_SCHEMA)).collect()


# ---------- Time zones ----------

@pytest.mark.parametrize(
    "local_time, ingested_at, expected_utc",
    [
        # Winter: UTC+1
        ("2026-01-15 08:00:00", "2026-01-15T07:00:20+00:00", datetime(2026, 1, 15, 7, 0)),
        # Summer: UTC+2
        ("2026-07-01 08:00:00", "2026-07-01T06:00:20+00:00", datetime(2026, 7, 1, 6, 0)),
        # Clocks go back on 25 Oct 2026: 02:30 happens twice
        ("2026-10-25 02:30:00", "2026-10-25T00:30:20+00:00", datetime(2026, 10, 25, 0, 30)),
        ("2026-10-25 02:30:00", "2026-10-25T01:30:20+00:00", datetime(2026, 10, 25, 1, 30)),
        # Just before and after the ambiguous hour
        ("2026-10-25 01:59:00", "2026-10-24T23:59:20+00:00", datetime(2026, 10, 24, 23, 59)),
        ("2026-10-25 03:00:00", "2026-10-25T02:00:20+00:00", datetime(2026, 10, 25, 2, 0)),
        # Clocks go forward on 29 Mar 2026: 03:00 local = 01:00 UTC
        ("2026-03-29 03:00:00", "2026-03-29T01:00:20+00:00", datetime(2026, 3, 29, 1, 0)),
    ],
)
def test_local_time_to_utc(spark, local_time, ingested_at, expected_utc):
    rows = clean(spark, [bronze_row(local_time, ingested_at)])
    assert len(rows) == 1
    assert rows[0].event_time_utc == expected_utc


def test_second_pass_of_ambiguous_hour_is_not_dropped_as_stale(spark):
    rows = clean(spark, [bronze_row("2026-10-25 02:10:00", "2026-10-25T01:10:30+00:00")])
    assert len(rows) == 1
    assert rows[0].staleness_seconds == 30


# ---------- Live filter ----------

@pytest.mark.parametrize(
    "local_time, kept",
    [
        ("2026-01-15 08:00:00", True),    # 20 s old
        ("2026-01-15 07:58:30", True),    # 110 s old
        ("2026-01-15 07:57:00", False),   # 200 s old: not live
        ("2024-11-01 08:00:00", False),   # parked for two years
        ("2026-01-15 08:02:00", False),   # 100 s in the future
    ],
)
def test_live_filter(spark, local_time, kept):
    rows = clean(spark, [bronze_row(local_time, "2026-01-15T07:00:20+00:00")])
    assert (len(rows) == 1) is kept


# ---------- Validity ----------

@pytest.mark.parametrize(
    "lat, lon, kept",
    [
        (52.23, 21.01, True),     # city centre
        (0.0, 0.0, False),        # null island
        (52.23, 19.0, False),     # west of the service area
        (53.5, 21.01, False),     # north of the service area
    ],
)
def test_service_area(spark, lat, lon, kept):
    rows = clean(spark, [bronze_row("2026-01-15 08:00:00", "2026-01-15T07:00:20+00:00", lat=lat, lon=lon)])
    assert (len(rows) == 1) is kept


def test_missing_vehicle_number_is_dropped(spark):
    rows = clean(spark, [bronze_row("2026-01-15 08:00:00", "2026-01-15T07:00:20+00:00", vehicle=None)])
    assert rows == []


@pytest.mark.parametrize("raw, expected", [("016", "16"), ("1", "1"), ("0", "0"), ("100", "100")])
def test_brigade_leading_zeros(spark, raw, expected):
    rows = clean(spark, [bronze_row("2026-01-15 08:00:00", "2026-01-15T07:00:20+00:00", brigade=raw)])
    assert rows[0].brigade == expected


def test_line_is_trimmed(spark):
    rows = clean(spark, [bronze_row("2026-01-15 08:00:00", "2026-01-15T07:00:20+00:00", line=" 128 ")])
    assert rows[0].line == "128"


# ---------- Deduplication (streaming only, so it runs as a small stream) ----------

def test_deduplication(spark, tmp_path):
    rows = [
        # The same reading seen in two polls 10 s apart: one observation
        bronze_row("2026-01-15 08:00:00", "2026-01-15T07:00:20+00:00"),
        bronze_row("2026-01-15 08:00:00", "2026-01-15T07:00:30+00:00"),
        # Same vehicle, same place, new timestamp: a new observation, kept
        bronze_row("2026-01-15 08:00:40", "2026-01-15T07:00:50+00:00"),
        # Another vehicle with the same timestamp: kept
        bronze_row("2026-01-15 08:00:00", "2026-01-15T07:00:20+00:00", vehicle="2002"),
    ]
    source = spark.createDataFrame(rows, BRONZE_SCHEMA)
    source.write.parquet(str(tmp_path / "bronze"))

    stream = spark.readStream.schema(source.schema).parquet(str(tmp_path / "bronze"))
    query = (
        deduplicate(clean_positions(stream))
        .writeStream.format("memory").queryName("dedup_test")
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    result = spark.table("dedup_test").groupBy("vehicle_number").agg(F.count("*").alias("n"))
    assert {r.vehicle_number: r.n for r in result.collect()} == {"1001": 2, "2002": 1}
