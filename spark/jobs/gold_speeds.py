import argparse
from datetime import datetime, timezone, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

SILVER_PATH = "/opt/lake/silver/vehicle_positions"
SEGMENTS_PATH = "/opt/lake/gold/vehicle_segments"
LINE_HOURLY_PATH = "/opt/lake/gold/line_hourly_speed"

LOCAL_TZ = "Europe/Warsaw"
EARTH_RADIUS_M = 6_371_000
MIN_GAP_SECONDS = 5
MAX_GAP_SECONDS = 120
MAX_SPEED_KMH = 100
STATIONARY_DISTANCE_M = 15
MAX_DWELL_SECONDS = 300


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


def build_segments(silver_day):
    """Pair each reading with the same vehicle's previous one and compute speed."""
    by_vehicle = Window.partitionBy("vehicle_number").orderBy("event_time_utc")

    return (
        silver_day
        .withColumn("prev_time", F.lag("event_time_utc").over(by_vehicle))
        .withColumn("prev_lat", F.lag("lat").over(by_vehicle))
        .withColumn("prev_lon", F.lag("lon").over(by_vehicle))
        .withColumn("prev_line", F.lag("line").over(by_vehicle))
        # first reading of the day has no previous one
        .filter(F.col("prev_time").isNotNull())
        # same trip only
        .filter(F.col("line") == F.col("prev_line"))
        .withColumn(
            "gap_seconds",
            F.col("event_time_utc").cast("long") - F.col("prev_time").cast("long"),
        )
        .filter(F.col("gap_seconds").between(MIN_GAP_SECONDS, MAX_GAP_SECONDS))
        .withColumn(
            "distance_m",
            haversine_m(F.col("prev_lat"), F.col("prev_lon"), F.col("lat"), F.col("lon")),
        )
        .withColumn("speed_kmh", F.col("distance_m") / F.col("gap_seconds") * 3.6)
        .filter(F.col("speed_kmh") <= MAX_SPEED_KMH)
        .withColumn("hour_utc", F.date_trunc("hour", "event_time_utc"))
        .select(
            "event_date", "hour_utc", "vehicle_number", "vehicle_type", "line", "brigade",
            "prev_time", "event_time_utc", "prev_lat", "prev_lon", "lat", "lon",
            "gap_seconds", "distance_m", "speed_kmh",
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
        # running count of moving segments: stays the same while standing still
        .withColumn("run_id", F.sum(F.col("is_moving").cast("int")).over(by_vehicle))
        # total seconds spent standing still within this run
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

def build_line_hourly(segments):
    """Commercial speed per line per hour: total distance / total time."""
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
        .withColumn("hour_local", F.from_utc_timestamp("hour_utc", LOCAL_TZ))
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

    segments = flag_layovers(build_segments(silver_day)).cache()
    line_hourly = build_line_hourly(segments)

    overwrite_day(segments, SEGMENTS_PATH, args.date)
    overwrite_day(line_hourly, LINE_HOURLY_PATH, args.date)

    print(f"gold done for {args.date}: segments={segments.count()} line_hours={line_hourly.count()}")
    spark.stop()


if __name__ == "__main__":
    main()