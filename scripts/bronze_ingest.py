import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)

OPENSKY_URL = "https://opensky-network.org/api/states/all"


def _get_auth() -> tuple | None:
    """Return (user, password) if env vars are set, else None (anonymous)."""
    user = os.getenv("OPENSKY_USER")
    password = os.getenv("OPENSKY_PASS")
    return (user, password) if user and password else None


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=5, max=60),
    reraise=True,
)
def _fetch_opensky() -> tuple[dict, dict]:
    auth = _get_auth()
    request_started = datetime.now(timezone.utc)
    log.info("Fetching OpenSky states (auth=%s)", "yes" if auth else "anonymous")
    response = requests.get(OPENSKY_URL, auth=auth, timeout=30)
    request_finished = datetime.now(timezone.utc)
    response.raise_for_status()
    payload = response.json()
    request_meta = {
        "requested_at_utc": request_started.isoformat(),
        "received_at_utc": request_finished.isoformat(),
        "response_time_ms": int((request_finished - request_started).total_seconds() * 1000),
        "http_status": response.status_code,
        "auth_mode": "authenticated" if auth else "anonymous",
    }
    return payload, request_meta


def run_bronze_ingestion(**context) -> str:
    """
    Ingest raw OpenSky flight states to the bronze layer.

    Filename is keyed on data_interval_start so re-runs are idempotent
    (same DAG run always writes to the same file path).
    """
    interval_start: datetime = context["data_interval_start"]
    timestamp = interval_start.strftime("%Y%m%d_%H%M%S")

    bronze_path = Path("/opt/airflow/data/bronze")
    bronze_path.mkdir(parents=True, exist_ok=True)

    output_file = bronze_path / f"flights_bronze_{timestamp}.json"

    data, request_meta = _fetch_opensky()

    state_count = len(data.get("states") or [])
    data["_pipeline_ingestion"] = {
        "window_start_utc": interval_start.isoformat(),
        "state_count": state_count,
        **request_meta,
    }

    log.info(
        "OpenSky returned %d states for window starting %s (response_time_ms=%d)",
        state_count,
        interval_start.isoformat(),
        request_meta["response_time_ms"],
    )

    if state_count < 100:
        log.warning(
            "Low OpenSky state volume detected (%d states). This may indicate upstream throttling,"
            " temporary partial outage, or reduced coverage.",
            state_count,
        )

    with open(output_file, "w") as f:
        json.dump(data, f)

    log.info("Bronze file written: %s", output_file)
    return str(output_file)