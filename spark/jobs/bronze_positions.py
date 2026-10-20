from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "vehicle-positions"

BRONZE_PATH = "/opt/lake/bronze/vehicle_positions"
CHECKPOINT_PATH = "/opt/lake/_checkpoints/bronze_vehicle_positions"

POSITION_SCHEMA = StructType([
    StructField("Lines", StringType()),
    StructField("Lon", DoubleType()),
    StructField("VehicleNumber", StringType()),
    StructField("Time", StringType()),
    StructField("Lat", DoubleType()),
    StructField("Brigade", StringType()),
    StructField("ingested_at", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("poll_id", StringType()),
])


def main():
    # 1. Session, with Delta enabled
    spark = (
        SparkSession.builder
        .appName("bronze-vehicle-positions")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 2. Source: Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 200000)
        .load()
    )

    # 3. Transform: raw string + parsed fields + Kafka metadata
    raw_df = kafka_df.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

    bronze_df = (
        raw_df
        .withColumn("data", F.from_json(F.col("raw_value"), POSITION_SCHEMA))
        .select(
            "data.*",
            "raw_value",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
        .withColumn("ingest_date", F.to_date(F.col("kafka_timestamp")))
    )

    # 4. Sink: Delta table
    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("ingest_date")
        .trigger(processingTime="1 minute")
        .start(BRONZE_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()