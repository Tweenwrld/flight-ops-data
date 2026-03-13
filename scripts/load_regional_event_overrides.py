import logging
import os
from pathlib import Path

import pandas as pd
import requests
import snowflake.connector
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

OVERRIDES_FILE = Path("/opt/airflow/data/reference/regional_event_overrides.csv")

EXPECTED_COLUMNS = {
    "region_type",
    "region_value",
    "event_type",
    "source",
    "notes",
    "severity_score",
    "min_disruption_band",
    "event_start_utc",
    "event_end_utc",
    "is_active",
}

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS FLIGHTS.KPI.REGIONAL_EVENT_OVERRIDES (
    event_id               NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    region_type            TEXT            NOT NULL DEFAULT 'country',
    region_value           TEXT            NOT NULL,
    event_type             TEXT            NOT NULL,
    source                 TEXT,
    notes                  TEXT,
    severity_score         FLOAT           NOT NULL,
    min_disruption_band    TEXT            DEFAULT 'normal',
    event_start_utc        TIMESTAMP       NOT NULL,
    event_end_utc          TIMESTAMP       NOT NULL,
    is_active              BOOLEAN         DEFAULT TRUE,
    created_at             TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    updated_at             TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
)
"""

MERGE_SQL = """
MERGE INTO FLIGHTS.KPI.REGIONAL_EVENT_OVERRIDES tgt
USING (
    SELECT
        %(region_type)s AS REGION_TYPE,
        %(region_value)s AS REGION_VALUE,
        %(event_type)s AS EVENT_TYPE,
        %(source)s AS SOURCE,
        %(notes)s AS NOTES,
        %(severity_score)s AS SEVERITY_SCORE,
        %(min_disruption_band)s AS MIN_DISRUPTION_BAND,
        TO_TIMESTAMP(%(event_start_utc)s, 'YYYY-MM-DD HH24:MI:SS') AS EVENT_START_UTC,
        TO_TIMESTAMP(%(event_end_utc)s, 'YYYY-MM-DD HH24:MI:SS') AS EVENT_END_UTC,
        %(is_active)s AS IS_ACTIVE
) src
ON  LOWER(tgt.REGION_TYPE) = LOWER(src.REGION_TYPE)
AND UPPER(tgt.REGION_VALUE) = UPPER(src.REGION_VALUE)
AND LOWER(tgt.EVENT_TYPE) = LOWER(src.EVENT_TYPE)
AND tgt.EVENT_START_UTC = src.EVENT_START_UTC
AND tgt.EVENT_END_UTC = src.EVENT_END_UTC
WHEN MATCHED THEN UPDATE SET
    SOURCE = src.SOURCE,
    NOTES = src.NOTES,
    SEVERITY_SCORE = src.SEVERITY_SCORE,
    MIN_DISRUPTION_BAND = src.MIN_DISRUPTION_BAND,
    IS_ACTIVE = src.IS_ACTIVE,
    UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    REGION_TYPE,
    REGION_VALUE,
    EVENT_TYPE,
    SOURCE,
    NOTES,
    SEVERITY_SCORE,
    MIN_DISRUPTION_BAND,
    EVENT_START_UTC,
    EVENT_END_UTC,
    IS_ACTIVE,
    CREATED_AT,
    UPDATED_AT
)
VALUES (
    src.REGION_TYPE,
    src.REGION_VALUE,
    src.EVENT_TYPE,
    src.SOURCE,
    src.NOTES,
    src.SEVERITY_SCORE,
    src.MIN_DISRUPTION_BAND,
    src.EVENT_START_UTC,
    src.EVENT_END_UTC,
    src.IS_ACTIVE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
)
"""


def _build_connection() -> snowflake.connector.SnowflakeConnection:
    conn = BaseHook.get_connection("flight_snowflake")
    return snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson["account"],
        warehouse=conn.extra_dejson.get("warehouse"),
        database=conn.extra_dejson.get("database", "FLIGHTS"),
        schema=conn.extra_dejson.get("schema", "KPI"),
        role=conn.extra_dejson.get("role"),
    )


def _to_bool(value) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _normalize_band(value: str) -> str:
    band = str(value).strip().lower()
    if band not in {"normal", "watch", "high", "critical"}:
        return "normal"
    return band


def _extract_api_rows(payload) -> list[dict]:
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        preferred_key = os.getenv("EVENT_OVERRIDES_API_PAYLOAD_KEY", "overrides").strip()
        if preferred_key and isinstance(payload.get(preferred_key), list):
            return payload[preferred_key]
        if isinstance(payload.get("data"), list):
            return payload["data"]
        if EXPECTED_COLUMNS.issubset(set(payload.keys())):
            return [payload]

    return []


def _load_from_api() -> tuple[pd.DataFrame, str]:
    api_url = os.getenv("EVENT_OVERRIDES_API_URL", "").strip()
    if not api_url:
        raise ValueError("EVENT_OVERRIDES_API_URL is not set")

    timeout_sec = int(os.getenv("EVENT_OVERRIDES_API_TIMEOUT_SEC", "20"))
    verify_tls = _to_bool(os.getenv("EVENT_OVERRIDES_API_VERIFY_TLS", "true"))

    headers = {}
    api_token = os.getenv("EVENT_OVERRIDES_API_TOKEN", "").strip()
    if api_token:
        token_header = os.getenv("EVENT_OVERRIDES_API_TOKEN_HEADER", "Authorization").strip() or "Authorization"
        token_prefix = os.getenv("EVENT_OVERRIDES_API_TOKEN_PREFIX", "Bearer").strip()
        headers[token_header] = f"{token_prefix} {api_token}".strip() if token_prefix else api_token

    response = requests.get(
        api_url,
        headers=headers or None,
        timeout=timeout_sec,
        verify=verify_tls,
    )
    response.raise_for_status()

    rows = _extract_api_rows(response.json())
    if not rows:
        raise ValueError("Event override API response did not contain override rows")

    return pd.DataFrame(rows), f"api:{api_url}"


def _load_from_csv() -> tuple[pd.DataFrame, str]:
    if not OVERRIDES_FILE.exists():
        raise FileNotFoundError(f"Overrides file not found ({OVERRIDES_FILE})")

    df = pd.read_csv(OVERRIDES_FILE)
    if df.empty:
        raise AirflowSkipException("No rows in regional_event_overrides.csv")

    return df, f"csv:{OVERRIDES_FILE}"


def _load_overrides_dataframe() -> tuple[pd.DataFrame, str]:
    api_url = os.getenv("EVENT_OVERRIDES_API_URL", "").strip()
    prefer_api = _to_bool(os.getenv("EVENT_OVERRIDES_PREFER_API", "true"))
    fallback_to_csv = _to_bool(os.getenv("EVENT_OVERRIDES_API_FALLBACK_TO_CSV", "true"))

    if prefer_api and api_url:
        try:
            return _load_from_api()
        except Exception as api_error:
            if not fallback_to_csv:
                raise
            log.warning(
                "Event override API load failed (%s). Falling back to CSV if available.",
                api_error,
            )

    if OVERRIDES_FILE.exists():
        return _load_from_csv()

    if api_url:
        return _load_from_api()

    raise AirflowSkipException(
        "No event override source configured. Set EVENT_OVERRIDES_API_URL or provide regional_event_overrides.csv"
    )


def _normalize_overrides(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(col).strip() for col in df.columns]

    missing_columns = EXPECTED_COLUMNS - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"Event overrides payload is missing required columns: {sorted(missing_columns)}"
        )

    df = df.copy()
    df["region_type"] = df["region_type"].fillna("country").astype(str).str.strip().str.lower()
    df["region_value"] = df["region_value"].fillna("").astype(str).str.strip()
    df["event_type"] = df["event_type"].fillna("generic_disruption").astype(str).str.strip().str.lower()
    df["source"] = df["source"].fillna("external_intel").astype(str).str.strip()
    df["notes"] = df["notes"].fillna("").astype(str).str.strip()
    df["severity_score"] = pd.to_numeric(df["severity_score"], errors="coerce").fillna(0.0).clip(0.0, 100.0)
    df["min_disruption_band"] = df["min_disruption_band"].apply(_normalize_band)
    df["is_active"] = df["is_active"].apply(_to_bool)

    for col in ["event_start_utc", "event_end_utc"]:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        if df[col].isna().any():
            raise ValueError(f"Invalid datetime found in {col} column")

    invalid_ranges = df[df["event_end_utc"] < df["event_start_utc"]]
    if not invalid_ranges.empty:
        raise ValueError("Some override rows have event_end_utc earlier than event_start_utc")

    df = df[df["region_value"] != ""]
    if df.empty:
        raise AirflowSkipException("No valid region_value rows in event overrides payload")

    df["event_start_utc"] = df["event_start_utc"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S")
    df["event_end_utc"] = df["event_end_utc"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S")

    return df


def load_regional_event_overrides(**context) -> None:
    """Load event override intelligence into Snowflake for risk band adjustments."""
    df_raw, source_name = _load_overrides_dataframe()
    df = _normalize_overrides(df_raw)

    rows = [
        {
            "region_type": row["region_type"],
            "region_value": row["region_value"],
            "event_type": row["event_type"],
            "source": row["source"],
            "notes": row["notes"],
            "severity_score": float(row["severity_score"]),
            "min_disruption_band": row["min_disruption_band"],
            "event_start_utc": row["event_start_utc"],
            "event_end_utc": row["event_end_utc"],
            "is_active": bool(row["is_active"]),
        }
        for _, row in df.iterrows()
    ]

    sf_conn = _build_connection()
    try:
        with sf_conn.cursor() as cursor:
            cursor.execute(TABLE_DDL)
            cursor.executemany(MERGE_SQL, rows)
        sf_conn.commit()
        log.info(
            "Loaded %d regional event override rows into FLIGHTS.KPI.REGIONAL_EVENT_OVERRIDES from %s",
            len(rows),
            source_name,
        )
    finally:
        sf_conn.close()
