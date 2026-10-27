from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from wtp.alerts import task_failure_callback

with DAG(
    dag_id="maintenance",
    description="Nightly OPTIMIZE (merge small files) and VACUUM (delete old files) on all Delta tables",
    schedule="30 2 * * *",          
    start_date=datetime(2026, 10, 27),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=2),
        "on_failure_callback": task_failure_callback,
    },
    tags=["maintenance"],
) as dag:

    SparkSubmitOperator(
        task_id="optimize_and_vacuum",
        conn_id="spark_default",
        application="/opt/jobs/maintenance.py",
        name="lake-maintenance",
        packages="io.delta:delta-spark_2.13:4.0.1",
        conf={
            "spark.jars.ivy": "/tmp/.ivy2",
            "spark.cores.max": "2",
            "spark.executor.memory": "1g",
            "spark.driver.host": "airflow-scheduler",
            "spark.driver.bindAddress": "0.0.0.0",
        },
    )