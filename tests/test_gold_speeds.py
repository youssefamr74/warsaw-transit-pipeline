from datetime import date, datetime, timedelta

import pytest

from gold_speeds import (
    build_line_hourly_speed,
    build_segment_quality,
    build_segments,
    classify_pairs,
    flag_layovers,
)

SILVER_COLUMNS = ["vehicle_number", "vehicle_type", "line", "brigade", "lat", "lon", "event_time_utc", "event_date"]
START = datetime(2026, 1, 15, 7, 0, 0)
METERS_PER_DEGREE_LAT = 111_195


def reading(seconds, lat, lon=21.0, vehicle="1001", line="128"):
    t = START + timedelta(seconds=seconds)
    return (vehicle, "bus", line, "1", lat, lon, t, t.date())


def silver(spark, rows):
    return spark.createDataFrame(rows, SILVER_COLUMNS)


def north(meters):
    """Latitude this many meters north of 52.0."""
    return 52.0 + meters / METERS_PER_DEGREE_LAT


def statuses(spark, rows):
    pairs = classify_pairs(silver(spark, rows)).orderBy("event_time_utc").collect()
    return [p.status for p in pairs]


# ---------- Distance ----------

def test_distance_matches_haversine(spark):
    rows = [reading(0, 52.0), reading(60, north(500))]
    segment = build_segments(silver(spark, rows)).collect()[0]
    assert segment.distance_m == pytest.approx(500, abs=0.5)
    assert segment.speed_kmh == pytest.approx(30, abs=0.05)


# ---------- Pair rules ----------

@pytest.mark.parametrize(
    "second_reading, expected",
    [
        (reading(30, north(200)), "ok"),
        (reading(3, north(10)), "gap_too_short"),
        (reading(30 * 60, north(5000)), "gap_too_long"),
        (reading(10, north(2000)), "too_fast"),                # 720 km/h: GPS jump
        (reading(30, north(200), line="N61"), "line_change"),
    ],
)
def test_pair_rules(spark, second_reading, expected):
    assert statuses(spark, [reading(0, 52.0), second_reading]) == ["no_previous", expected]


def test_pairs_never_mix_vehicles(spark):
    rows = [reading(0, 52.0, vehicle="A"), reading(30, north(200), vehicle="B")]
    assert statuses(spark, rows) == ["no_previous", "no_previous"]


def test_segment_counts_in_the_hour_of_its_second_reading(spark):
    rows = [reading(-20, 52.0), reading(20, north(300))]
    segment = build_segments(silver(spark, rows)).collect()[0]
    assert segment.hour_utc == datetime(2026, 1, 15, 7, 0)


# ---------- Layovers ----------

def drive_then_stand(stand_seconds):
    """Drive 3 readings, stand still for stand_seconds (readings every 30 s), drive again."""
    rows = [reading(0, 52.0), reading(30, north(300)), reading(60, north(600))]
    t = 60
    while t < 60 + stand_seconds:
        t += 30
        rows.append(reading(t, north(600)))
    rows.append(reading(t + 30, north(900)))
    return rows


def layover_flags(spark, rows):
    segments = flag_layovers(build_segments(silver(spark, rows))).orderBy("event_time_utc").collect()
    return [s.is_layover for s in segments]


def test_short_stop_is_not_a_layover(spark):
    flags = layover_flags(spark, drive_then_stand(120))
    assert not any(flags)


def test_long_standstill_is_a_layover(spark):
    flags = layover_flags(spark, drive_then_stand(600))
    standing = flags[2:-1]
    assert all(standing)
    assert not flags[0] and not flags[1] and not flags[-1]


# ---------- Aggregation ----------

def test_speed_is_total_distance_over_total_time(spark):
    rows = [
        reading(0, 52.0), reading(100, north(1000)),
        reading(0, 52.5, vehicle="B"), reading(50, 52.5 + 100 / METERS_PER_DEGREE_LAT, vehicle="B"),
    ]
    segments = flag_layovers(build_segments(silver(spark, rows)))
    hour = build_line_hourly_speed(segments).collect()[0]
    assert hour.total_distance_m == pytest.approx(1100, abs=1)
    assert hour.total_seconds == 150
    assert hour.avg_speed_kmh == pytest.approx(26.4, abs=0.05)


def test_segment_quality_accounts_for_every_pair(spark):
    rows = [
        reading(0, 52.0), reading(30, north(200)), reading(33, north(210)),
        reading(60 * 60, north(5000)),
    ]
    df = silver(spark, rows)
    segments = flag_layovers(build_segments(df))
    quality = {r.status: r.pairs for r in build_segment_quality(df, segments).collect()}
    assert quality == {"no_previous": 1, "used": 1, "gap_too_short": 1, "gap_too_long": 1}


def test_rerun_gives_identical_output(spark):
    rows = drive_then_stand(600) + [reading(5, 52.3, vehicle="B"), reading(35, 52.302, vehicle="B")]

    def run():
        segments = flag_layovers(build_segments(silver(spark, rows)))
        return sorted(build_line_hourly_speed(segments).collect())

    assert run() == run()
    assert run()[0].event_date == date(2026, 1, 15)
