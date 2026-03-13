import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow.exceptions import AirflowSkipException

log = logging.getLogger(__name__)

STALE_THRESHOLD_SECONDS = 120
LOW_VELOCITY_THRESHOLD_MS = 70.0

# All 17 (or 18) fields returned by the OpenSky /states/all endpoint
OPENSKY_COLUMNS_17 = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
]
OPENSKY_COLUMNS_18 = OPENSKY_COLUMNS_17 + ["category"]

# Columns to keep in silver (analytically useful; aggregation is gold's job)
SILVER_COLUMNS = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "geo_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "squawk",
    "position_source",
    "window_start",
    "position_age_sec",
    "contact_age_sec",
    "is_stale_contact",
    "is_stale_position",
    "is_low_velocity",
]


def run_silver_transform(**context) -> None:
    """
    Transform raw bronze JSON into a clean, typed silver CSV.

    Key decisions:
    - Skip cleanly (AirflowSkipException) if bronze produced no states.
    - Add window_start column (DAG data_interval_start) as the temporal PK anchor.
    - Cast types: on_ground → bool, velocity → float (NaN-safe).
    - Filename keyed on data_interval_start for idempotency.
    """
    interval_start: datetime = context["data_interval_start"]
    timestamp = interval_start.strftime("%Y%m%d_%H%M%S")

    bronze_file = context["ti"].xcom_pull(
        key="return_value",
        task_ids="bronze_ingest",
    )
    if not bronze_file:
        raise ValueError("Bronze file path not found in XCom")

    log.info("Reading bronze file: %s", bronze_file)
    with open(bronze_file) as f:
        raw = json.load(f)

    states = raw.get("states")
    if not states:
        log.warning("OpenSky returned no states — skipping silver transform")
        raise AirflowSkipException("No flight states in bronze payload")

    # Assign column names based on payload width
    n_cols = len(states[0])
    if n_cols == 18:
        columns = OPENSKY_COLUMNS_18
    elif n_cols == 17:
        columns = OPENSKY_COLUMNS_17
    else:
        raise ValueError(f"Unexpected OpenSky payload width: {n_cols}")

    df = pd.DataFrame(states, columns=columns)

    # --- Normalize core identifiers ---
    df["icao24"] = df["icao24"].fillna("").astype(str).str.lower().str.strip()
    df["callsign"] = df["callsign"].fillna("").astype(str).str.strip()
    df["origin_country"] = df["origin_country"].fillna("Unknown").astype(str).str.strip()

    # Remove malformed rows lacking aircraft key
    df = df[df["icao24"] != ""]
    if df.empty:
        raise AirflowSkipException("No valid aircraft keys in bronze payload")

    # --- Type casting ---
    numeric_columns = [
        "time_position",
        "last_contact",
        "longitude",
        "latitude",
        "baro_altitude",
        "geo_altitude",
        "velocity",
        "true_track",
        "vertical_rate",
    ]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["on_ground"] = df["on_ground"].fillna(False).astype(bool)

    # Deduplicate aircraft snapshots by latest contact seen in this window
    df = (
        df.sort_values(["icao24", "last_contact"], ascending=[True, False], na_position="last")
        .drop_duplicates(subset=["icao24"], keep="first")
        .reset_index(drop=True)
    )

    # --- Add temporal anchor ---
    df["window_start"] = interval_start.strftime("%Y-%m-%d %H:%M:%S")
    window_start_epoch = int(interval_start.timestamp())

    df["position_age_sec"] = (window_start_epoch - df["time_position"]).where(df["time_position"].notna())
    df["contact_age_sec"] = (window_start_epoch - df["last_contact"]).where(df["last_contact"].notna())
    df["position_age_sec"] = df["position_age_sec"].clip(lower=0)
    df["contact_age_sec"] = df["contact_age_sec"].clip(lower=0)

    # Operational reliability and movement flags
    df["is_stale_contact"] = df["contact_age_sec"].fillna(STALE_THRESHOLD_SECONDS + 1) > STALE_THRESHOLD_SECONDS
    df["is_stale_position"] = df["position_age_sec"].fillna(STALE_THRESHOLD_SECONDS + 1) > STALE_THRESHOLD_SECONDS
    df["is_low_velocity"] = (~df["on_ground"]) & (df["velocity"].fillna(0.0) < LOW_VELOCITY_THRESHOLD_MS)

    # --- Keep only silver columns ---
    df = df[SILVER_COLUMNS]

    silver_path = Path("/opt/airflow/data/silver")
    silver_path.mkdir(parents=True, exist_ok=True)
    output_file = silver_path / f"flights_silver_{timestamp}.csv"

    df.to_csv(output_file, index=False)
    log.info("Silver file written: %s  (%d rows)", output_file, len(df))

    context["ti"].xcom_push(key="silver_file", value=str(output_file))