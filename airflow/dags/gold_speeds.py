from datetime import datetime, timedelta

from airflow.sdk import DAG, Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="gold_speeds_hourly",
    description="Recompute gold speed tables for the current UTC day",
    schedule="5 * * * *",               # 5 minutes past every hour
    start_date=datetime(2026, 10, 21),
    catchup=False,                      
    max_active_runs=1,                  
    params={
        "date": Param("", type="string", description="UTC date YYYY-MM-DD, empty = automatic"),
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["gold", "spark"],
) as dag:

    SparkSubmitOperator(
        task_id="gold_speeds",
        conn_id="spark_default",
        application="/opt/jobs/gold_speeds.py",
        name="gold-speeds",
        packages="io.delta:delta-spark_2.13:4.0.1",
        application_args=["--date", "{{ params.date }}"],
        conf={
            "spark.jars.ivy": "/tmp/.ivy2",
            "spark.cores.max": "2",
            "spark.executor.memory": "1g",
            "spark.driver.host": "airflow-scheduler",
            "spark.driver.bindAddress": "0.0.0.0",
        },
    )