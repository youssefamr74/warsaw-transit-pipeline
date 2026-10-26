from datetime import datetime, timedelta

from airflow.sdk import DAG, Param, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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
    description="Hourly batch: weather bronze/silver and gold speed tables",
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

    gold_speeds = SparkSubmitOperator(
        task_id="gold_speeds",
        conn_id="spark_default",
        application="/opt/jobs/gold_speeds.py",
        name="gold-speeds",
        packages="io.delta:delta-spark_2.13:4.0.1",
        application_args=["--date", "{{ params.date or '' }}"],
        conf=SPARK_CONF,
    )

    weather_silver = build_weather_silver(fetch_weather("{{ params.weather_past_days }}"))

    gold_speed_weather = SparkSubmitOperator(
        task_id="gold_speed_weather",
        conn_id="spark_default",
        application="/opt/jobs/gold_speed_weather.py",
        name="gold-speed-weather",
        packages="io.delta:delta-spark_2.13:4.0.1",
        application_args=["--date", "{{ params.date or '' }}"],
        conf=SPARK_CONF,
    )

    [gold_speeds, weather_silver] >> gold_speed_weather