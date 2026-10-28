"""Push alerts to a phone via ntfy.sh."""

import logging
import os

import requests

log = logging.getLogger(__name__)
NTFY_URL = "https://ntfy.sh"


class DataQualityFailed(Exception):
    """Raised when checks fail. Alerts for these are sent by the checks themselves."""


def send_alert(title: str, message: str, priority: str = "high", tags: str = "rotating_light") -> None:
    topic = os.environ.get("NTFY_TOPIC")
    if not topic:
        log.warning("NTFY_TOPIC not set, alert not sent: %s", title)
        return
    try:
        requests.post(
            f"{NTFY_URL}/{topic}",
            data=message.encode("utf-8"),
            headers={"Title": title, "Priority": priority, "Tags": tags},
            timeout=10,
        )
    except requests.RequestException as e:
        log.error("could not send alert: %s", e)


def task_failure_callback(context) -> None:
    """Airflow calls this when a task fails for good (after its retries)."""
    exc = context.get("exception")
    if isinstance(exc, DataQualityFailed):
        return
    ti = context["task_instance"]
    send_alert(
        title=f"Task failed: {ti.dag_id}.{ti.task_id}",
        message=f"{type(exc).__name__}: {exc}",
    )
