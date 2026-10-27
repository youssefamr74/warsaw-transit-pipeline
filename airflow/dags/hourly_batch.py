from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from wtp.alerts import task_failure_callback

LAKE = "/opt/lake"
WEATHER_BRONZE = f"{LAKE}/bronze/weather_raw"
WEATHER_SILVER = f"{LAKE}/silver/weather_hourly"
WARSAW_LAT, WARSAW_LON = 52.2297, 21.0122

SPARK_CONF = {
    "spark.jars.ivy": "/tmp/.ivy2",
    "spark.cores.max": "2",
    "spark.executor.memory": "1g",
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
}

with DAG(
    dag_id="hourly_batch",
    description="Hourly batch: weather, gold speed tables, map grid and collection stats",
    schedule="5 * * * *",
    start_date=datetime(2026, 10, 26),
    catchup=False,
    max_active_runs=1,
    params={
        "date": Param(None, type=["null", "string"], description="UTC date for gold, empty = automatic"),
        "weather_past_days": Param(2, type="integer", minimum=1, maximum=92,
                                   description="How many past days of weather to fetch"),
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": task_failure_callback,
    },
    tags=["weather", "gold"],
) as dag:

    @task
    def fetch_weather(past_days: int) -> str:
        from wtp.weather import fetch_to_bronze
        return fetch_to_bronze(WEATHER_BRONZE, WARSAW_LAT, WARSAW_LON, int(past_days))

    @task
    def build_weather_silver(_fetched_at: str) -> int:
        from wtp.weather import rebuild_silver
        return rebuild_silver(WEATHER_BRONZE, WEATHER_SILVER)

    @task
    def collection_stats(date_param: str) -> int:
        from wtp.collection import build_collection_hourly
        day = date_param or (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%d")
        return build_collection_hourly(LAKE, day)

    # segments, line_hourly_speed, speed_grid_hourly
    gold_speeds = SparkSubmitOperator(
        task_id="gold_speeds",
        conn_id="spark_default",
        application="/opt/jobs/gold_speeds.py",
        name="gold-speeds",
        packages="io.delta:delta-spark_2.13:4.0.1",
        application_args=["--date", "{{ params.date or '' }}"],
        conf=SPARK_CONF,
    )

    # line_hourly = line speeds + weather
    gold_line_hourly = SparkSubmitOperator(
        task_id="gold_line_hourly",
        conn_id="spark_default",
        application="/opt/jobs/gold_speed_weather.py",
        name="gold-line-hourly",
        packages="io.delta:delta-spark_2.13:4.0.1",
        application_args=["--date", "{{ params.date or '' }}"],
        conf=SPARK_CONF,
    )

    weather_silver = build_weather_silver(fetch_weather("{{ params.weather_past_days }}"))
    collection = collection_stats("{{ params.date or '' }}")

    [gold_speeds, weather_silver] >> gold_line_hourly
    # Not a data dependency: runs last so it doesn't compete with Spark for memory
    gold_line_hourly >> collection