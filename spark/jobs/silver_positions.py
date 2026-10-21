from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_PATH = "/opt/lake/bronze/vehicle_positions"
SILVER_PATH = "/opt/lake/silver/vehicle_positions"
CHECKPOINT_PATH = "/opt/lake/_checkpoints/silver_vehicle_positions"

SOURCE_TIMEZONE = "Europe/Warsaw"
LIVE_THRESHOLD_SECONDS = 120     
FUTURE_TOLERANCE_SECONDS = 60    
DEDUP_WATERMARK = "10 minutes"


def build_silver(bronze_df):
    """All silver rules in one place. Input: bronze rows. Output: clean, live positions."""
    return (
        bronze_df
        # Types and UTC
        .withColumn(
            "event_time_utc",
            F.to_utc_timestamp(F.to_timestamp("Time", "yyyy-MM-dd HH:mm:ss"), SOURCE_TIMEZONE),
        )
        .withColumn("ingested_ts", F.col("ingested_at").cast("timestamp"))
        .withColumn(
            "staleness_seconds",
            F.col("ingested_ts").cast("long") - F.col("event_time_utc").cast("long"),
        )

        # Drop rows that can't be used at all
        .filter(
            F.col("VehicleNumber").isNotNull()
            & F.col("event_time_utc").isNotNull()
            & F.col("Lat").isNotNull()
            & F.col("Lon").isNotNull()
        )

        # Keep only live readings (no ghosts & no readings from the future)
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

        # Same vehicle + same reading seen in two polls = one observation
        .withWatermark("ingested_ts", DEDUP_WATERMARK)
        .dropDuplicatesWithinWatermark(["vehicle_number", "event_time_utc"])

        # Partition column
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

    # Source: the bronze Delta table, read as a stream
    bronze_df = spark.readStream.format("delta").load(BRONZE_PATH)

    silver_df = build_silver(bronze_df)

    # Sink: the silver Delta table
    query = (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("event_date")
        .trigger(processingTime="1 minute")
        .start(SILVER_PATH)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()