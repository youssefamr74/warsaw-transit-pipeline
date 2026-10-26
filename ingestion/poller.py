"""
Warsaw transit poller.
Polls the Warsaw City API for live bus and tram positions
and publishes each vehicle as one message to Kafka.
"""

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
import signal
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

API_TOKEN = os.getenv("WARSAW_API_TOKEN")
API_URL = "https://dane.um.warszawa.pl/api/action/get_ztm_lokalizacja_pojazdow"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "vehicle-positions")
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "10"))

VEHICLE_TYPES = {1: "bus", 2: "tram"}  # API type code -> our label
MAX_RETRIES = 3
REQUEST_TIMEOUT_SECONDS = 10

FAILURES_BEFORE_BACKOFF = 3
MAX_BACKOFF_SECONDS = 600

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("poller")

# Counts delivery results reported by Kafka (filled by the callback)
delivery_stats = {"ok": 0, "failed": 0}

def safe(error: Exception) -> str:
    """Return the error message with the API token removed."""
    text = str(error)
    return text.replace(API_TOKEN, "***") if API_TOKEN else text
# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
def fetch_vehicles(type_code: int) -> list[dict]:
    headers = {"Authorization": API_TOKEN}
    body = {"type": type_code}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(API_URL, json=body, headers=headers,
                                     timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            data = response.json()

            # New API returns a plain list; keep the old {"result": [...]} shape working too
            result = data.get("result") if isinstance(data, dict) else data
            if not isinstance(result, list):
                raise ValueError(f"API returned an error: {data}")
            return result
        except (requests.RequestException, ValueError) as e:
            if attempt == MAX_RETRIES:
                raise
            log.warning("fetch type=%s attempt %s/%s failed: %s",
                        type_code, attempt, MAX_RETRIES, safe(e))
            time.sleep(2 ** (attempt - 1))


def enrich(records: list[dict], vehicle_type: str, ingested_at: str, poll_id: str) -> list[dict]:
    return [{**r, "ingested_at": ingested_at, "vehicle_type": vehicle_type, "poll_id": poll_id} for r in records]


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
def build_producer() -> Producer:
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "zstd",
        "linger.ms": 100,
        "client.id": "warsaw-poller",
    }
    return Producer(config)


def on_delivery(err, msg):
    if err is not None:
        delivery_stats["failed"] += 1
        log.error("delivery failed key=%s error=%s", msg.key().decode(), err)
    else:
        delivery_stats["ok"] += 1


def publish(producer: Producer, records: list[dict]) -> None:
    for record in records:
        kwargs = {
            "topic": KAFKA_TOPIC,
            "key": record["VehicleNumber"].encode(),
            "value": json.dumps(record).encode(),
            "callback": on_delivery,
        }
        try:
            producer.produce(**kwargs)
        except BufferError:
            log.warning("producer queue full, waiting")
            producer.poll(1)
            producer.produce(**kwargs)
        producer.poll(0)
        


# ---------------------------------------------------------------------------
# One cycle
# ---------------------------------------------------------------------------
def poll_once(producer: Producer) -> bool:
    ingested_at = datetime.now(timezone.utc).isoformat()
    poll_id = str(uuid.uuid4())
    counts = {}

    for type_code, vehicle_type in VEHICLE_TYPES.items():
        try:
            fetched = fetch_vehicles(type_code)
            enriched = enrich(fetched, vehicle_type, ingested_at, poll_id)
            publish(producer, enriched)
            counts[vehicle_type] = len(enriched)
        except Exception as e:
            log.error("poll failed for %s: %s", vehicle_type, safe(e))
            counts[vehicle_type] = 0
    producer.flush(10)
    failed_types = [t for t, c in counts.items() if c == 0]
    level = logging.WARNING if failed_types else logging.INFO
    status = "partial" if failed_types else "ok"
    log.log(
        level,
        "poll %s buses=%s trams=%s delivered=%s failed=%s",
        status, counts.get("bus"), counts.get("tram"),
        delivery_stats["ok"], delivery_stats["failed"],
    )
    return any(count > 0 for count in counts.values())


def wait_time(consecutive_failures: int) -> float:
    """Normal interval while healthy, growing waits during an outage."""
    if consecutive_failures < FAILURES_BEFORE_BACKOFF:
        return POLL_INTERVAL_SECONDS
    extra = consecutive_failures - FAILURES_BEFORE_BACKOFF
    return min(60 * 2 ** extra, MAX_BACKOFF_SECONDS)

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def handle_sigterm(signum, frame):
    raise KeyboardInterrupt

def main() -> None:
    signal.signal(signal.SIGTERM, handle_sigterm)
    if not API_TOKEN:
        log.error("WARSAW_API_TOKEN is not set, exiting")
        sys.exit(1)

    log.info(
        "starting poller topic=%s servers=%s interval=%ss",
        KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS, POLL_INTERVAL_SECONDS,
    )
    producer = build_producer()

    try:
        consecutive_failures = 0
        while True:
            started = time.monotonic()
            ok = False
            try:
                ok = poll_once(producer)
            except Exception:
                log.exception("unexpected error in poll cycle")

            if ok:
                if consecutive_failures >= FAILURES_BEFORE_BACKOFF:
                    log.info("API is back after %s failed polls", consecutive_failures)
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                if consecutive_failures >= FAILURES_BEFORE_BACKOFF:
                    log.warning("API unavailable, %s failed polls in a row, next try in %ss",
                                consecutive_failures, wait_time(consecutive_failures))

            elapsed = time.monotonic() - started
            time.sleep(max(0, wait_time(consecutive_failures) - elapsed))
    except KeyboardInterrupt:
        log.info("shutting down, flushing remaining messages")
    finally:
        producer.flush(10)
        log.info("stopped")


if __name__ == "__main__":
    main()