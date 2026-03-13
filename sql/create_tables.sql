-- =============================================================
-- Flight Ops Medallion Pipeline — Snowflake DDL
-- Database: FLIGHTS  |  Schema: KPI
-- Run once during project bootstrap
-- =============================================================

CREATE DATABASE IF NOT EXISTS FLIGHTS;

CREATE SCHEMA IF NOT EXISTS FLIGHTS.KPI;

-- ------------------------------------------------------------
-- Core KPI table
-- PK: (window_start, origin_country)
--   → window_start  = DAG data_interval_start (30-min bucket)
--   → origin_country = ISO country string from OpenSky
--
-- Populated via MERGE (upsert) so re-runs are idempotent.
-- Enriched for executive decision support (disruption monitoring,
-- operational stress, and data confidence).
-- ------------------------------------------------------------
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
);

-- ------------------------------------------------------------
-- Optional compatibility alters for existing deployments
-- (safe to run repeatedly)
-- ------------------------------------------------------------
ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS ACTIVE_FLIGHTS INT;
ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS PCT_GROUNDED FLOAT;
ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS AVG_VELOCITY_ACTIVE FLOAT;
ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS P90_VELOCITY_ACTIVE FLOAT;
ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS MEDIAN_GEO_ALTITUDE_ACTIVE FLOAT;
ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS AVG_ABS_VERTICAL_RATE_ACTIVE FLOAT;
ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS STALE_CONTACT_RATE FLOAT;
ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS LOW_VELOCITY_RATE FLOAT;
ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS CLIMBING_FLIGHTS INT;
ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS DESCENDING_FLIGHTS INT;
ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS DATA_COMPLETENESS_SCORE FLOAT;
ALTER TABLE FLIGHTS.KPI.FLIGHT_KPIS ADD COLUMN IF NOT EXISTS OPERATIONAL_STRESS_INDEX FLOAT;

-- ------------------------------------------------------------
-- External event/intelligence overrides
-- Used to inject known disruptions (airspace restrictions, conflict,
-- NOTAM-heavy events) so risk bands reflect real operations context.
-- ------------------------------------------------------------
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
);

-- ------------------------------------------------------------
-- Decision-support view: disruption risk signal (z-score based)
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW FLIGHTS.KPI.V_COUNTRY_DISRUPTION_RISK AS
WITH base AS (
    SELECT
        window_start,
        origin_country,
        total_flights,
        pct_grounded,
        avg_velocity_active,
        stale_contact_rate,
        low_velocity_rate,
        data_completeness_score,
        operational_stress_index,
        AVG(pct_grounded) OVER (
            PARTITION BY origin_country
            ORDER BY window_start
            ROWS BETWEEN 48 PRECEDING AND 1 PRECEDING
        ) AS baseline_pct_grounded,
        STDDEV_SAMP(pct_grounded) OVER (
            PARTITION BY origin_country
            ORDER BY window_start
            ROWS BETWEEN 48 PRECEDING AND 1 PRECEDING
        ) AS baseline_pct_grounded_sd,
        AVG(operational_stress_index) OVER (
            PARTITION BY origin_country
            ORDER BY window_start
            ROWS BETWEEN 48 PRECEDING AND 1 PRECEDING
        ) AS baseline_stress,
        STDDEV_SAMP(operational_stress_index) OVER (
            PARTITION BY origin_country
            ORDER BY window_start
            ROWS BETWEEN 48 PRECEDING AND 1 PRECEDING
        ) AS baseline_stress_sd,
        AVG(total_flights) OVER (
            PARTITION BY origin_country
            ORDER BY window_start
            ROWS BETWEEN 48 PRECEDING AND 1 PRECEDING
        ) AS baseline_total_flights,
        STDDEV_SAMP(total_flights) OVER (
            PARTITION BY origin_country
            ORDER BY window_start
            ROWS BETWEEN 48 PRECEDING AND 1 PRECEDING
        ) AS baseline_total_flights_sd
    FROM FLIGHTS.KPI.FLIGHT_KPIS
), scored AS (
    SELECT
        *,
        COALESCE((pct_grounded - baseline_pct_grounded) / NULLIF(baseline_pct_grounded_sd, 0), 0) AS grounded_z,
        COALESCE((operational_stress_index - baseline_stress) / NULLIF(baseline_stress_sd, 0), 0) AS stress_z,
        COALESCE((baseline_total_flights - total_flights) / NULLIF(baseline_total_flights_sd, 0), 0) AS capacity_drop_z,
        GREATEST(0.0, COALESCE(100.0 * (1 - total_flights / NULLIF(baseline_total_flights, 0)), 0.0)) AS capacity_drop_pct
    FROM base
), event_candidates AS (
    SELECT
        s.window_start,
        s.origin_country,
        e.event_type,
        e.source AS event_source,
        e.notes AS event_notes,
        e.severity_score AS event_severity_score,
        LOWER(COALESCE(e.min_disruption_band, 'normal')) AS min_disruption_band,
        ROW_NUMBER() OVER (
            PARTITION BY s.window_start, s.origin_country
            ORDER BY e.severity_score DESC, e.event_start_utc DESC
        ) AS rn
    FROM scored s
    JOIN FLIGHTS.KPI.REGIONAL_EVENT_OVERRIDES e
      ON e.is_active = TRUE
     AND LOWER(e.region_type) = 'country'
     AND UPPER(e.region_value) = UPPER(s.origin_country)
     AND s.window_start BETWEEN e.event_start_utc AND e.event_end_utc
), events AS (
    SELECT
        window_start,
        origin_country,
        event_type,
        event_source,
        event_notes,
        event_severity_score,
        min_disruption_band
    FROM event_candidates
    WHERE rn = 1
), combined AS (
    SELECT
        s.window_start,
        s.origin_country,
        s.total_flights,
        s.pct_grounded,
        s.avg_velocity_active,
        s.stale_contact_rate,
        s.low_velocity_rate,
        s.data_completeness_score,
        s.operational_stress_index,
        s.capacity_drop_pct,
        ROUND(0.45 * s.stress_z + 0.25 * s.grounded_z + 0.30 * s.capacity_drop_z, 4) AS telemetry_risk_zscore,
        COALESCE(e.event_severity_score, 0.0) AS event_severity_score,
        e.event_type,
        e.event_source,
        e.event_notes,
        IFF(e.event_severity_score IS NULL, FALSE, TRUE) AS active_event_override,
        COALESCE(e.min_disruption_band, 'normal') AS min_disruption_band
    FROM scored s
    LEFT JOIN events e
      ON s.window_start = e.window_start
     AND s.origin_country = e.origin_country
), banded AS (
    SELECT
        *,
        GREATEST(telemetry_risk_zscore, event_severity_score / 25.0) AS disruption_risk_zscore,
        CASE
            WHEN GREATEST(telemetry_risk_zscore, event_severity_score / 25.0) >= 2.0 THEN 'critical'
            WHEN GREATEST(telemetry_risk_zscore, event_severity_score / 25.0) >= 1.0 THEN 'high'
            WHEN GREATEST(telemetry_risk_zscore, event_severity_score / 25.0) >= 0.2 THEN 'watch'
            ELSE 'normal'
        END AS computed_band
    FROM combined
)
SELECT
    window_start,
    origin_country,
    total_flights,
    pct_grounded,
    avg_velocity_active,
    stale_contact_rate,
    low_velocity_rate,
    data_completeness_score,
    operational_stress_index,
    ROUND(capacity_drop_pct, 2) AS capacity_drop_pct,
    ROUND(telemetry_risk_zscore, 4) AS telemetry_risk_zscore,
    ROUND(disruption_risk_zscore, 4) AS disruption_risk_zscore,
    ROUND(event_severity_score, 2) AS event_severity_score,
    event_type,
    event_source,
    event_notes,
    active_event_override,
    CASE
        WHEN min_disruption_band = 'critical' THEN 'critical'
        WHEN min_disruption_band = 'high' AND computed_band IN ('normal', 'watch') THEN 'high'
        WHEN min_disruption_band = 'watch' AND computed_band = 'normal' THEN 'watch'
        ELSE computed_band
    END AS disruption_band
FROM banded;

-- ------------------------------------------------------------
-- Verification queries
-- ------------------------------------------------------------
-- Full table scan:
--   SELECT * FROM FLIGHTS.KPI.FLIGHT_KPIS ORDER BY window_start DESC, total_flights DESC;

-- Country filter (example):
--   SELECT * FROM FLIGHTS.KPI.FLIGHT_KPIS WHERE origin_country = 'Kenya';

-- Dashboard: top 20 countries by avg active flights per window
--   SELECT
--       origin_country,
--       ROUND(AVG(total_flights), 1)              AS avg_flights_per_window,
--       ROUND(AVG(avg_velocity_active), 1)        AS avg_airborne_speed_ms,
--       ROUND(AVG(pct_grounded), 2)               AS avg_pct_grounded,
--       ROUND(AVG(operational_stress_index), 2)   AS avg_operational_stress
--   FROM FLIGHTS.KPI.FLIGHT_KPIS
--   GROUP BY origin_country
--   ORDER BY avg_flights_per_window DESC
--   LIMIT 20;

-- Current disruption watchlist:
--   SELECT *
--   FROM FLIGHTS.KPI.V_COUNTRY_DISRUPTION_RISK
--   QUALIFY ROW_NUMBER() OVER (PARTITION BY origin_country ORDER BY window_start DESC) = 1
--   ORDER BY disruption_risk_zscore DESC;

-- Active event overrides now:
--   SELECT *
--   FROM FLIGHTS.KPI.REGIONAL_EVENT_OVERRIDES
--   WHERE is_active = TRUE
--     AND CURRENT_TIMESTAMP() BETWEEN event_start_utc AND event_end_utc
--   ORDER BY severity_score DESC;
