from datetime import datetime

from airflow.sdk import DAG, task

from wtp.alerts import task_failure_callback

LAKE = "/opt/lake"
DQ_RESULTS = f"{LAKE}/gold/dq_results"

with DAG(
    dag_id="data_quality",
    description="Freshness and volume checks on the lake, alerts to ntfy",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 10, 27),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "on_failure_callback": task_failure_callback},
    tags=["quality"],
) as dag:

    @task
    def run_checks() -> int:
        from wtp.quality import run_all
        return len(run_all(LAKE, DQ_RESULTS))

    run_checks()
