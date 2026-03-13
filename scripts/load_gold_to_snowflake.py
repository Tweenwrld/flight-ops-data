import logging
from pathlib import Path

import pandas as pd
import snowflake.connector
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

EXPECTED_GOLD_COLUMNS = {
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
}

TARGET_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS FLIGHTS.KPI.FLIGHT_KPIS (
    window_start                    TIMESTAMP       NOT NULL,
    origin_country                  TEXT            NOT NULL,
    total_flights                   INT             NOT NULL,
    active_flights                  INT             NOT NULL,
    on_ground                       INT             NOT NULL,
    pct_grounded                    FLOAT,
    avg_velocity_active             FLOAT,
    p90_velocity_active             FLOAT,
    median_geo_altitude_active      FLOAT,
    avg_abs_vertical_rate_active    FLOAT,
    stale_contact_rate              FLOAT,
    low_velocity_rate               FLOAT,
    climbing_flights                INT,
    descending_flights              INT,
    data_completeness_score         FLOAT,
    operational_stress_index        FLOAT,
    load_time                       TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (window_start, origin_country)
)
"""

TABLE_EVOLUTION_DDL = [
    "ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS ACTIVE_FLIGHTS INT",
    "ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS PCT_GROUNDED FLOAT",
    "ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS AVG_VELOCITY_ACTIVE FLOAT",
    "ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS P90_VELOCITY_ACTIVE FLOAT",
    "ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS MEDIAN_GEO_ALTITUDE_ACTIVE FLOAT",
    "ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS AVG_ABS_VERTICAL_RATE_ACTIVE FLOAT",
    "ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS STALE_CONTACT_RATE FLOAT",
    "ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS LOW_VELOCITY_RATE FLOAT",
    "ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS CLIMBING_FLIGHTS INT",
    "ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS DESCENDING_FLIGHTS INT",
    "ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS DATA_COMPLETENESS_SCORE FLOAT",
    "ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS OPERATIONAL_STRESS_INDEX FLOAT",
]

MERGE_SQL = """
    MERGE INTO FLIGHTS.KPI.FLIGHT_KPIS tgt
    USING (
        SELECT
            TO_TIMESTAMP(%(window_start)s,  'YYYY-MM-DD HH24:MI:SS') AS WINDOW_START,
            %(origin_country)s                                        AS ORIGIN_COUNTRY,
            %(total_flights)s                                         AS TOTAL_FLIGHTS,
            %(active_flights)s                                        AS ACTIVE_FLIGHTS,
            %(on_ground)s                                             AS ON_GROUND,
            %(pct_grounded)s                                          AS PCT_GROUNDED,
            %(avg_velocity_active)s                                   AS AVG_VELOCITY_ACTIVE,
            %(p90_velocity_active)s                                   AS P90_VELOCITY_ACTIVE,
            %(median_geo_altitude_active)s                            AS MEDIAN_GEO_ALTITUDE_ACTIVE,
            %(avg_abs_vertical_rate_active)s                          AS AVG_ABS_VERTICAL_RATE_ACTIVE,
            %(stale_contact_rate)s                                    AS STALE_CONTACT_RATE,
            %(low_velocity_rate)s                                     AS LOW_VELOCITY_RATE,
            %(climbing_flights)s                                      AS CLIMBING_FLIGHTS,
            %(descending_flights)s                                    AS DESCENDING_FLIGHTS,
            %(data_completeness_score)s                               AS DATA_COMPLETENESS_SCORE,
            %(operational_stress_index)s                              AS OPERATIONAL_STRESS_INDEX
    ) src
    ON  tgt.WINDOW_START    = src.WINDOW_START
    AND tgt.ORIGIN_COUNTRY  = src.ORIGIN_COUNTRY
    WHEN MATCHED THEN UPDATE SET
        TOTAL_FLIGHTS                 = src.TOTAL_FLIGHTS,
        ACTIVE_FLIGHTS                = src.ACTIVE_FLIGHTS,
        ON_GROUND                     = src.ON_GROUND,
        PCT_GROUNDED                  = src.PCT_GROUNDED,
        AVG_VELOCITY_ACTIVE           = src.AVG_VELOCITY_ACTIVE,
        P90_VELOCITY_ACTIVE           = src.P90_VELOCITY_ACTIVE,
        MEDIAN_GEO_ALTITUDE_ACTIVE    = src.MEDIAN_GEO_ALTITUDE_ACTIVE,
        AVG_ABS_VERTICAL_RATE_ACTIVE  = src.AVG_ABS_VERTICAL_RATE_ACTIVE,
        STALE_CONTACT_RATE            = src.STALE_CONTACT_RATE,
        LOW_VELOCITY_RATE             = src.LOW_VELOCITY_RATE,
        CLIMBING_FLIGHTS              = src.CLIMBING_FLIGHTS,
        DESCENDING_FLIGHTS            = src.DESCENDING_FLIGHTS,
        DATA_COMPLETENESS_SCORE       = src.DATA_COMPLETENESS_SCORE,
        OPERATIONAL_STRESS_INDEX      = src.OPERATIONAL_STRESS_INDEX,
        LOAD_TIME                     = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (
            WINDOW_START,
            ORIGIN_COUNTRY,
            TOTAL_FLIGHTS,
            ACTIVE_FLIGHTS,
            ON_GROUND,
            PCT_GROUNDED,
            AVG_VELOCITY_ACTIVE,
            P90_VELOCITY_ACTIVE,
            MEDIAN_GEO_ALTITUDE_ACTIVE,
            AVG_ABS_VERTICAL_RATE_ACTIVE,
            STALE_CONTACT_RATE,
            LOW_VELOCITY_RATE,
            CLIMBING_FLIGHTS,
            DESCENDING_FLIGHTS,
            DATA_COMPLETENESS_SCORE,
            OPERATIONAL_STRESS_INDEX,
            LOAD_TIME
        )
    VALUES
        (
            src.WINDOW_START,
            src.ORIGIN_COUNTRY,
            src.TOTAL_FLIGHTS,
            src.ACTIVE_FLIGHTS,
            src.ON_GROUND,
            src.PCT_GROUNDED,
            src.AVG_VELOCITY_ACTIVE,
            src.P90_VELOCITY_ACTIVE,
            src.MEDIAN_GEO_ALTITUDE_ACTIVE,
            src.AVG_ABS_VERTICAL_RATE_ACTIVE,
            src.STALE_CONTACT_RATE,
            src.LOW_VELOCITY_RATE,
            src.CLIMBING_FLIGHTS,
            src.DESCENDING_FLIGHTS,
            src.DATA_COMPLETENESS_SCORE,
            src.OPERATIONAL_STRESS_INDEX,
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


def _none_if_nan(value):
    return None if pd.isna(value) else value


def _ensure_target_schema(cursor) -> None:
    cursor.execute(TARGET_TABLE_DDL)
    for ddl in TABLE_EVOLUTION_DDL:
        cursor.execute(ddl)


def load_gold_to_snowflake(**context) -> None:
    """
    Batch-upsert gold KPI rows into Snowflake FLIGHT_KPIS via MERGE.

    Key decisions:
    - window_start is read from the gold CSV (data-driven), not from context —
      correct even if gold covers multiple windows in future.
    - executemany() sends all rows in a single round-trip instead of N loops.
    - Explicit commit() — Snowflake connector does NOT auto-commit DML.
    - try/finally guarantees sf_conn.close() runs even on exception.
    - AirflowSkipException propagates cleanly if upstream was skipped.
    """
    gold_file = context["ti"].xcom_pull(
        key="gold_file",
        task_ids="gold_aggregate",
    )
    if not gold_file:
        log.warning("No gold file in XCom — skipping Snowflake load")
        raise AirflowSkipException("No gold file to load")

    if not Path(gold_file).exists():
        raise FileNotFoundError(f"Gold file not found on disk: {gold_file}")

    df = pd.read_csv(gold_file)
    if df.empty:
        log.warning("Gold file is empty — nothing to load")
        raise AirflowSkipException("Gold file is empty")

    missing_columns = EXPECTED_GOLD_COLUMNS - set(df.columns)
    if missing_columns:
        raise ValueError(f"Gold file missing required columns for Snowflake load: {sorted(missing_columns)}")

    log.info("Preparing to load %d rows from %s", len(df), gold_file)

    # Build list-of-dicts for named-parameter executemany
    rows = [
        {
            "window_start": str(row["window_start"]),
            "origin_country": str(row["origin_country"]),
            "total_flights": int(row["total_flights"]),
            "active_flights": int(row["active_flights"]),
            "on_ground": int(row["on_ground"]),
            "pct_grounded": float(row["pct_grounded"]),
            "avg_velocity_active": float(row["avg_velocity_active"]),
            "p90_velocity_active": _none_if_nan(row["p90_velocity_active"]),
            "median_geo_altitude_active": _none_if_nan(row["median_geo_altitude_active"]),
            "avg_abs_vertical_rate_active": _none_if_nan(row["avg_abs_vertical_rate_active"]),
            "stale_contact_rate": float(row["stale_contact_rate"]),
            "low_velocity_rate": float(row["low_velocity_rate"]),
            "climbing_flights": int(row["climbing_flights"]),
            "descending_flights": int(row["descending_flights"]),
            "data_completeness_score": float(row["data_completeness_score"]),
            "operational_stress_index": float(row["operational_stress_index"]),
        }
        for _, row in df.iterrows()
    ]

    sf_conn = _build_connection()
    try:
        with sf_conn.cursor() as cursor:
            _ensure_target_schema(cursor)
            cursor.executemany(MERGE_SQL, rows)
        sf_conn.commit()
        log.info("Successfully committed %d enriched KPI rows to FLIGHTS.KPI.FLIGHT_KPIS", len(rows))
    finally:
        sf_conn.close()