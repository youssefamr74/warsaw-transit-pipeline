from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "vehicle-positions"
position_schema = StructType([
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
    # Session
    spark = (
        SparkSession.builder
        .appName("kafka-parse-console")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Source: Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    # Columns: key, value, topic, partition, offset, timestamp, timestampType

    raw_df = kafka_df.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

    parsed_df = raw_df.withColumn(
        "data", F.from_json(F.col("raw_value"), position_schema)
    )

    result_df = parsed_df.select(
        "data.*",
        "raw_value",
        "partition",
        "offset",
        "kafka_timestamp",
    )

    result_df.printSchema()

    # Sink: console
    query = (
        result_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", True)
        .option("numRows", 5)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
