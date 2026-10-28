import os
from datetime import datetime, timedelta

from airflow.sdk import DAG, task

from wtp.alerts import task_failure_callback

LAKE = "/opt/lake"
EXPORT_DIR = f"{LAKE}/serving/dashboard"

with DAG(
    dag_id="publish_dashboard",
    description="Export the dashboard's gold tables as Parquet and publish them to the public data repo",
    schedule="30 3 * * *",
    start_date=datetime(2026, 10, 28),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": task_failure_callback,
    },
    tags=["dashboard"],
) as dag:

    @task
    def export() -> dict:
        from wtp.export import export_files
        manifest = export_files(LAKE, EXPORT_DIR)
        total_mb = sum(manifest["bytes"].values()) / 1_000_000
        print(f"exported {manifest['first_date']} to {manifest['last_date']}, {total_mb:.1f} MB")
        for name, size in manifest["bytes"].items():
            if size > 90_000_000:
                raise ValueError(f"{name} is {size / 1e6:.0f} MB, too close to GitHub's 100 MB limit")
        return manifest

    @task
    def publish(_manifest: dict) -> str:
        from wtp.export import publish_to_github
        repo = os.environ["DASHBOARD_DATA_REPO"]
        token = os.environ["DASHBOARD_DATA_TOKEN"]
        sha = publish_to_github(EXPORT_DIR, repo, token)
        print(f"published commit {sha[:7]} to {repo}")
        return sha

    publish(export())
