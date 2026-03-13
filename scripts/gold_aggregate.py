import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow.exceptions import AirflowSkipException

log = logging.getLogger(__name__)

CLIMBING_VERTICAL_RATE_THRESHOLD = 2.0
DESCENDING_VERTICAL_RATE_THRESHOLD = -2.0

GOLD_COLUMNS = [
    "window_start",
    "origin_country",
    "total_flights",
    "active_flights",
    "on_ground",
    "pct_grounded",
    "avg_velocity_active",
    "p90_velocity_active",
    "median_geo_altitude_active",
    "avg_abs_vertical_rate_active",
    "stale_contact_rate",
    "low_velocity_rate",
    "climbing_flights",
    "descending_flights",
    "data_completeness_score",
    "operational_stress_index",
]


def run_gold_aggregate(**context) -> None:
    """
        Aggregate silver CSV into per-window, per-country executive KPI gold CSV.

        The output is designed for senior operational decisions (capacity stress,
        disruption early warning, and data confidence).
    """
    interval_start: datetime = context["data_interval_start"]
    timestamp = interval_start.strftime("%Y%m%d_%H%M%S")

    silver_file = context["ti"].xcom_pull(
        key="silver_file",
        task_ids="silver_transform",
    )
    if not silver_file:
        log.warning("No silver file in XCom — skipping gold aggregate")
        raise AirflowSkipException("No silver file to aggregate")

    log.info("Reading silver file: %s", silver_file)
    df = pd.read_csv(silver_file)

    if df.empty:
        log.warning("Silver file is empty — skipping gold aggregate")
        raise AirflowSkipException("Silver file is empty")

    required_columns = {
        "window_start",
        "origin_country",
        "icao24",
        "on_ground",
        "velocity",
        "vertical_rate",
        "geo_altitude",
        "last_contact",
        "is_stale_contact",
        "is_low_velocity",
    }
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"Silver file missing required columns for gold aggregation: {sorted(missing_columns)}"
        )

    # Normalize booleans in case CSV reader infers object/string
    for col in ["on_ground", "is_stale_contact", "is_low_velocity"]:
        if df[col].dtype != bool:
            df[col] = df[col].astype(str).str.lower().map({"true": True, "false": False}).fillna(False)

    # Enriched derived fields for operational analytics
    df["is_active"] = (~df["on_ground"]).astype(int)
    df["velocity_active"] = df["velocity"].where(~df["on_ground"])
    df["geo_altitude_active"] = df["geo_altitude"].where(~df["on_ground"])
    df["vertical_rate_abs_active"] = df["vertical_rate"].abs().where(~df["on_ground"])
    df["is_climbing"] = (
        (~df["on_ground"]) & (df["vertical_rate"] > CLIMBING_VERTICAL_RATE_THRESHOLD)
    ).astype(int)
    df["is_descending"] = (
        (~df["on_ground"]) & (df["vertical_rate"] < DESCENDING_VERTICAL_RATE_THRESHOLD)
    ).astype(int)
    df["record_quality_flag"] = (
        df["icao24"].fillna("").astype(str).str.len().gt(0)
        & df["origin_country"].fillna("").astype(str).str.len().gt(0)
        & df["velocity"].notna()
        & df["last_contact"].notna()
    ).astype(int)

    agg = (
        df.groupby(["window_start", "origin_country"])
        .agg(
            total_flights=("icao24", "count"),
            active_flights=("is_active", "sum"),
            on_ground=("on_ground", "sum"),
            avg_velocity_active=("velocity_active", "mean"),
            p90_velocity_active=("velocity_active", lambda s: s.quantile(0.90)),
            median_geo_altitude_active=("geo_altitude_active", "median"),
            avg_abs_vertical_rate_active=("vertical_rate_abs_active", "mean"),
            stale_contact_rate=("is_stale_contact", "mean"),
            low_velocity_rate=("is_low_velocity", "mean"),
            climbing_flights=("is_climbing", "sum"),
            descending_flights=("is_descending", "sum"),
            data_completeness_score=("record_quality_flag", "mean"),
        )
        .reset_index()
    )

    # Null-safe defaults for countries with sparse active-flight measurements
    float_columns = [
        "avg_velocity_active",
        "p90_velocity_active",
        "median_geo_altitude_active",
        "avg_abs_vertical_rate_active",
        "stale_contact_rate",
        "low_velocity_rate",
        "data_completeness_score",
    ]
    agg[float_columns] = agg[float_columns].fillna(0.0)

    int_columns = ["total_flights", "active_flights", "on_ground", "climbing_flights", "descending_flights"]
    agg[int_columns] = agg[int_columns].astype(int)

    agg["pct_grounded"] = (100.0 * agg["on_ground"] / agg["total_flights"]).round(2)
    agg["stale_contact_rate"] = (agg["stale_contact_rate"] * 100.0).round(2)
    agg["low_velocity_rate"] = (agg["low_velocity_rate"] * 100.0).round(2)
    agg["data_completeness_score"] = (agg["data_completeness_score"] * 100.0).round(2)

    agg["avg_velocity_active"] = agg["avg_velocity_active"].round(4)
    agg["p90_velocity_active"] = agg["p90_velocity_active"].round(4)
    agg["median_geo_altitude_active"] = agg["median_geo_altitude_active"].round(4)
    agg["avg_abs_vertical_rate_active"] = agg["avg_abs_vertical_rate_active"].round(4)

    # Composite operational stress score (0-100): higher means potential disruption risk
    agg["operational_stress_index"] = (
        0.45 * agg["pct_grounded"]
        + 0.25 * agg["stale_contact_rate"]
        + 0.20 * agg["low_velocity_rate"]
        + 0.10 * (100.0 - agg["data_completeness_score"])
    ).clip(lower=0.0, upper=100.0).round(2)

    # Column order must exactly match Snowflake MERGE source SELECT
    agg = agg[GOLD_COLUMNS]

    gold_path = Path("/opt/airflow/data/gold")
    gold_path.mkdir(parents=True, exist_ok=True)
    output_file = gold_path / f"flights_gold_{timestamp}.csv"

    # Push XCom before writing so downstream tasks can distinguish between
    # "file was never created" vs "file write failed"
    context["ti"].xcom_push(key="gold_file", value=str(output_file))

    agg.to_csv(output_file, index=False)
    log.info(
        "Gold file written: %s  (%d country rows)",
        output_file,
        len(agg),
    )