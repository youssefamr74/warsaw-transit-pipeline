"""
Nightly maintenance for all Delta tables in the lake.

OPTIMIZE: merges many small files into few large ones. Only partitions before today (UTC),
          so it never competes with the streaming jobs that are still writing today's data.
VACUUM:   deletes data files that no table version needs anymore, once they are older than
          the retention period. Keeps 7 days, so time travel and streaming readers stay safe.
"""

import os
import sys

from pyspark.sql import SparkSession

LAKE = "/opt/lake"
LAKE_MARKER = f"{LAKE}/.lake-root"
RETAIN_HOURS = 168
RETAIN_HOURS_GOLD = 24

# Table path -> its partition column (None = not partitioned)
TABLES = {
    "bronze/vehicle_positions": "ingest_date",
    "silver/vehicle_positions": "event_date",
    "gold/line_hourly_speed": "event_date",
    "gold/speed_grid_hourly": "event_date",
    "gold/line_hourly": "event_date",
    "gold/collection_hourly": "event_date",
    "gold/dq_results": "check_date",
    "gold/segment_quality": "event_date",
    "bronze/weather_raw": "fetch_date",
    "silver/weather_hourly": None,
}


def main():
    if not os.path.exists(LAKE_MARKER):
        sys.exit(f"{LAKE_MARKER} not found: the lake is not mounted, refusing to start")

    spark = (
        SparkSession.builder
        .appName("lake-maintenance")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    failures = []
    for table, partition_col in TABLES.items():
        path = f"{LAKE}/{table}"
        if not os.path.exists(f"{path}/_delta_log"):
            print(f"skip {table}: not a Delta table (yet)")
            continue
        try:
            where = f" WHERE {partition_col} < current_date()" if partition_col else ""
            result = spark.sql(f"OPTIMIZE delta.`{path}`{where}").collect()[0]
            metrics = result["metrics"]
            print(
                f"optimize {table}: removed {metrics['numFilesRemoved']} files, "
                f"added {metrics['numFilesAdded']}"
            )
            hours = RETAIN_HOURS_GOLD if table.startswith("gold/") else RETAIN_HOURS
            spark.sql(f"VACUUM delta.`{path}` RETAIN {hours} HOURS")
            print(f"vacuum {table}: done")
        except Exception as e:  # one table failing shouldn't stop the others
            print(f"FAILED {table}: {e}")
            failures.append(table)

    spark.stop()
    if failures:
        sys.exit(f"maintenance failed for: {', '.join(failures)}")


if __name__ == "__main__":
    main()
