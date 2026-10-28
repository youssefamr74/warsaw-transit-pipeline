from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_PATH = "/opt/lake/bronze/vehicle_positions"
SILVER_PATH = "/opt/lake/silver/vehicle_positions"
CHECKPOINT_PATH = "/opt/lake/_checkpoints/silver_vehicle_positions"

SOURCE_TIMEZONE = "Europe/Warsaw"
LIVE_THRESHOLD_SECONDS = 120
FUTURE_TOLERANCE_SECONDS = 60
DEDUP_WATERMARK = "10 minutes"
SERVICE_AREA = {"lat_min": 51.8, "lat_max": 52.8, "lon_min": 20.3, "lon_max": 21.9}


def local_to_utc(local_ts, reference_utc):
    first = F.to_utc_timestamp(local_ts, SOURCE_TIMEZONE)
    second = first + F.expr("INTERVAL 1 HOUR")
    second_is_valid = F.from_utc_timestamp(second, SOURCE_TIMEZONE) == local_ts
    second_is_closer = (
        F.abs(reference_utc.cast("long") - second.cast("long"))
        < F.abs(reference_utc.cast("long") - first.cast("long"))
    )
    return F.when(second_is_valid & second_is_closer, second).otherwise(first)


def clean_positions(bronze_df):
    """Stateless silver rules: types, UTC, service area, live filter, clean names."""
    return (
        bronze_df
        .withColumn("ingested_ts", F.col("ingested_at").cast("timestamp"))
        .withColumn(
            "event_time_utc",
            local_to_utc(F.to_timestamp("Time", "yyyy-MM-dd HH:mm:ss"), F.col("ingested_ts")),
        )
        .withColumn(
            "staleness_seconds",
            F.col("ingested_ts").cast("long") - F.col("event_time_utc").cast("long"),
        )

        .filter(
            F.col("VehicleNumber").isNotNull()
            & F.col("event_time_utc").isNotNull()
            & F.col("Lat").between(SERVICE_AREA["lat_min"], SERVICE_AREA["lat_max"])
            & F.col("Lon").between(SERVICE_AREA["lon_min"], SERVICE_AREA["lon_max"])
        )

        .filter(
            (F.col("staleness_seconds") < LIVE_THRESHOLD_SECONDS)
            & (F.col("staleness_seconds") > -FUTURE_TOLERANCE_SECONDS)
        )

        # Clean names and values
        .select(
            F.col("VehicleNumber").alias("vehicle_number"),
            F.col("vehicle_type"),
            F.trim(F.col("Lines")).alias("line"),
            F.regexp_replace("Brigade", r"^0+(?=.)", "").alias("brigade"),
            F.col("Lat").alias("lat"),
            F.col("Lon").alias("lon"),
            F.col("event_time_utc"),
            F.col("ingested_ts"),
            F.col("staleness_seconds"),
            F.col("poll_id"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
        )
    )


def deduplicate(df):
    """Same vehicle + same API timestamp = the same reading, seen in two polls.
    A vehicle reporting the same place later has a new timestamp, so it is kept.
    The watermark only limits how long Spark remembers keys: a duplicate always arrives
    within seconds (next poll), far inside 10 minutes."""
    return (
        df.withWatermark("ingested_ts", DEDUP_WATERMARK)
        .dropDuplicatesWithinWatermark(["vehicle_number", "event_time_utc"])
    )


def build_silver(bronze_df):
    """All silver rules. Input: bronze rows (stream). Output: clean, live, unique positions."""
    return (
        deduplicate(clean_positions(bronze_df))
        .withColumn("event_date", F.to_date("event_time_utc"))
    )


def main():
    spark = (
        SparkSession.builder
        .appName("silver-vehicle-positions")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = spark.readStream.format("delta").load(BRONZE_PATH)

    silver_df = build_silver(bronze_df)

    query = (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("event_date")
        .trigger(processingTime="5 minutes")
        .start(SILVER_PATH)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
