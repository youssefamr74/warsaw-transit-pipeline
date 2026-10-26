import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LAKE_MARKER = "/opt/lake/.lake-root"
LINE_HOURLY_PATH = "/opt/lake/gold/line_hourly_speed"
WEATHER_SILVER_PATH = "/opt/lake/silver/weather_hourly"
LINE_WEATHER_PATH = "/opt/lake/gold/line_hourly_speed_weather"
CITY_WEATHER_PATH = "/opt/lake/gold/city_hourly_weather"

LIGHT_RAIN_MM = 1.0


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="", help="UTC date YYYY-MM-DD (default: date of one hour ago)")
    args = parser.parse_args()
    if not args.date:
        args.date = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%d")
    return args


def add_weather_categories(df):
    """Business rules: how we label an hour's weather."""
    return (
        df
        .withColumn(
            "precip_category",
            F.when(F.col("precipitation").isNull(), F.lit(None))
            .when(F.col("precipitation") == 0, F.lit("dry"))
            .when(F.col("precipitation") < LIGHT_RAIN_MM, F.lit("light"))
            .otherwise(F.lit("rain")),
        )
        .withColumn("is_snow", F.col("snowfall") > 0)
    )


def overwrite_day(df, path, date):
    (
        df.write.format("delta")
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
        .appName(f"gold-speed-weather-{args.date}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    speeds = (
        spark.read.format("delta").load(LINE_HOURLY_PATH)
        .filter(F.col("event_date") == args.date)
    )
    weather = spark.read.format("delta").load(WEATHER_SILVER_PATH).select(
        "hour_utc", "temperature_2m", "precipitation", "rain", "showers",
        "snowfall", "wind_speed_10m", "relative_humidity_2m", "weather_code",
    )

    # Left join: keep every speed hour, even if its weather is missing
    line_weather = add_weather_categories(speeds.join(weather, on="hour_utc", how="left"))

    city_weather = add_weather_categories(
        speeds.groupBy("event_date", "hour_utc", "hour_local", "vehicle_type")
        .agg(
            F.sum("total_distance_m").alias("total_distance_m"),
            F.sum("total_seconds").alias("total_seconds"),
            F.sum("vehicles").alias("vehicle_line_count"),
        )
        .withColumn("avg_speed_kmh", F.round(F.col("total_distance_m") / F.col("total_seconds") * 3.6, 2))
        .join(weather, on="hour_utc", how="left")
    )

    overwrite_day(line_weather, LINE_WEATHER_PATH, args.date)
    overwrite_day(city_weather, CITY_WEATHER_PATH, args.date)
    print(f"gold weather done for {args.date}")
    spark.stop()


if __name__ == "__main__":
    main()