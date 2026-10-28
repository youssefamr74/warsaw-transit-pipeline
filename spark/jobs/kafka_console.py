from pyspark.sql import SparkSession
from pyspark.sql import functions as F

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "vehicle-positions"


def main():
    # 1. Session
    spark = (
        SparkSession.builder
        .appName("kafka-to-console")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 2. Source: Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    # Columns: key, value, topic, partition, offset, timestamp, timestampType

    # 3. Transform: turn the bytes into readable strings
    result_df = kafka_df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp"),
    )

    # 4. Sink: console
    query = (
        result_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("numRows", 5)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
