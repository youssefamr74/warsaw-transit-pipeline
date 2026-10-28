import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

LAKE_MARKER = "/opt/lake/.lake-root"
SILVER_PATH = "/opt/lake/silver/vehicle_positions"
LINE_HOURLY_SPEED_PATH = "/opt/lake/gold/line_hourly_speed"
SPEED_GRID_PATH = "/opt/lake/gold/speed_grid_hourly"
SEGMENT_QUALITY_PATH = "/opt/lake/gold/segment_quality"

EARTH_RADIUS_M = 6_371_000
MIN_GAP_SECONDS = 5
MAX_GAP_SECONDS = 120
MAX_SPEED_KMH = 100
STATIONARY_DISTANCE_M = 15
MAX_DWELL_SECONDS = 300

CELL_LAT = 0.0025
CELL_LON = 0.004


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="", help="UTC date YYYY-MM-DD (default: date of one hour ago)")
    args = parser.parse_args()
    if not args.date:
        args.date = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%d")
    return args


def haversine_m(lat1, lon1, lat2, lon2):
    """Distance in meters between two points, as a Spark column expression."""
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_M * F.asin(F.sqrt(a))


def classify_pairs(silver_day):
    """Pair each reading with the same vehicle's previous one and label every pair.
    status: no_previous, line_change, gap_too_short, gap_too_long, too_fast or ok."""
    by_vehicle = Window.partitionBy("vehicle_number").orderBy("event_time_utc")

    pairs = (
        silver_day
        .withColumn("prev_time", F.lag("event_time_utc").over(by_vehicle))
        .withColumn("prev_lat", F.lag("lat").over(by_vehicle))
        .withColumn("prev_lon", F.lag("lon").over(by_vehicle))
        .withColumn("prev_line", F.lag("line").over(by_vehicle))
        .withColumn(
            "gap_seconds",
            F.col("event_time_utc").cast("long") - F.col("prev_time").cast("long"),
        )
        .withColumn(
            "distance_m",
            haversine_m(F.col("prev_lat"), F.col("prev_lon"), F.col("lat"), F.col("lon")),
        )
        .withColumn("speed_kmh", F.col("distance_m") / F.col("gap_seconds") * 3.6)
    )
    status = (
        F.when(F.col("prev_time").isNull(), "no_previous")
        .when(F.col("line") != F.col("prev_line"), "line_change")
        .when(F.col("gap_seconds") < MIN_GAP_SECONDS, "gap_too_short")
        .when(F.col("gap_seconds") > MAX_GAP_SECONDS, "gap_too_long")
        .when(F.col("speed_kmh") > MAX_SPEED_KMH, "too_fast")
        .otherwise("ok")
    )
    return pairs.withColumn("status", status)


def build_segments(silver_day):
    """Valid pairs only, with the hour each one counts in."""
    return (
        classify_pairs(silver_day)
        .filter(F.col("status") == "ok")
        .withColumn("hour_utc", F.date_trunc("hour", "event_time_utc"))
        .select(
            "event_date", "hour_utc", "vehicle_number", "vehicle_type", "line", "brigade",
            "prev_time", "event_time_utc", "prev_lat", "prev_lon", "lat", "lon",
            "gap_seconds", "distance_m", "speed_kmh",
        )
    )


def build_segment_quality(silver_day, segments):
    """One row per day and outcome: how many pairs, and how many seconds they cover.
    Outcomes: used, layover, or the reason a pair was rejected."""
    rejected = (
        classify_pairs(silver_day)
        .filter(F.col("status") != "ok")
        .select("event_date", "status", "gap_seconds")
    )
    valid = segments.select(
        "event_date",
        F.when(F.col("is_layover"), "layover").otherwise("used").alias("status"),
        "gap_seconds",
    )
    return (
        rejected.unionByName(valid)
        .groupBy("event_date", "status")
        .agg(
            F.count("*").alias("pairs"),
            F.coalesce(F.sum("gap_seconds"), F.lit(0)).alias("seconds"),
        )
    )


def flag_layovers(segments):
    """Mark long stationary stretches (e.g. breaks at the terminal) as layovers."""
    by_vehicle = (
        Window.partitionBy("vehicle_number")
        .orderBy("event_time_utc")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    by_run = Window.partitionBy("vehicle_number", "run_id")

    return (
        segments
        .withColumn("is_moving", F.col("distance_m") >= STATIONARY_DISTANCE_M)
        .withColumn("run_id", F.sum(F.col("is_moving").cast("int")).over(by_vehicle))
        .withColumn(
            "dwell_seconds",
            F.sum(F.when(~F.col("is_moving"), F.col("gap_seconds")).otherwise(0)).over(by_run),
        )
        .withColumn(
            "is_layover",
            ~F.col("is_moving") & (F.col("dwell_seconds") > MAX_DWELL_SECONDS),
        )
        .drop("is_moving", "run_id")
    )


def build_line_hourly_speed(segments):
    """Sums per line per hour. Speed = total distance / total time, layovers excluded."""
    return (
        segments
        .filter(~F.col("is_layover"))
        .groupBy("event_date", "hour_utc", "vehicle_type", "line")
        .agg(
            F.sum("distance_m").alias("total_distance_m"),
            F.sum("gap_seconds").alias("total_seconds"),
            F.count("*").alias("segments"),
            F.countDistinct("vehicle_number").alias("vehicles"),
        )
        .withColumn(
            "avg_speed_kmh",
            F.round(F.col("total_distance_m") / F.col("total_seconds") * 3.6, 2),
        )
    )


def build_speed_grid(segments):
    """Sums per map square, line and hour. Each segment counts at the midpoint of its two readings.
    Only sums are stored, so the dashboard can combine any lines and hours correctly."""
    mid_lat = (F.col("prev_lat") + F.col("lat")) / 2
    mid_lon = (F.col("prev_lon") + F.col("lon")) / 2
    return (
        segments
        .filter(~F.col("is_layover"))
        # round() avoids keys like 52.222500000000004 that would split one square in two
        .withColumn("cell_lat", F.round(F.floor(mid_lat / CELL_LAT) * CELL_LAT, 4))
        .withColumn("cell_lon", F.round(F.floor(mid_lon / CELL_LON) * CELL_LON, 4))
        .groupBy("event_date", "hour_utc", "vehicle_type", "line", "cell_lat", "cell_lon")
        .agg(
            F.sum("distance_m").alias("total_distance_m"),
            F.sum("gap_seconds").alias("total_seconds"),
            F.count("*").alias("segments"),
        )
    )


def overwrite_day(df, path, date):
    """Replace exactly one day's partition, so reruns never duplicate data."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"event_date = '{date}'")
        .partitionBy("event_date")
        .save(path)
    )


def main():
    if not os.path.exists(LAKE_MARKER):
        sys.exit(f"{LAKE_MARKER} not found: the lake is not mounted, refusing to start")

    args = parse_args()
    spark = (
        SparkSession.builder
        .appName(f"gold-speeds-{args.date}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    silver_day = (
        spark.read.format("delta").load(SILVER_PATH)
        .filter(F.col("event_date") == args.date)
    )

    started = time.monotonic()
    segments = flag_layovers(build_segments(silver_day)).cache()

    overwrite_day(build_line_hourly_speed(segments), LINE_HOURLY_SPEED_PATH, args.date)
    overwrite_day(build_speed_grid(segments), SPEED_GRID_PATH, args.date)
    overwrite_day(build_segment_quality(silver_day, segments), SEGMENT_QUALITY_PATH, args.date)

    print(
        f"gold speeds done for {args.date}: segments={segments.count()} "
        f"in {time.monotonic() - started:.0f} s"
    )
    spark.stop()


if __name__ == "__main__":
    main()
