-- =============================================================
-- Flight Ops Decision Intelligence Pack (Dashboard-ready Views)
-- Purpose: create reusable objects for disruption monitoring,
--          capacity planning, and confidence-aware decisions.
-- =============================================================

USE DATABASE FLIGHTS;
USE SCHEMA KPI;

-- -------------------------------------------------------------
-- 1) Executive Global Risk Board (latest available window)
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW FLIGHTS.KPI.V_EXEC_GLOBAL_RISK_BOARD_LATEST AS
WITH latest AS (
    SELECT MAX(window_start) AS latest_window
    FROM FLIGHTS.KPI.V_COUNTRY_DISRUPTION_RISK
)
SELECT
    r.window_start,
    r.origin_country,
    r.total_flights,
    ROUND(r.pct_grounded, 2)             AS pct_grounded,
    ROUND(r.capacity_drop_pct, 2)        AS capacity_drop_pct,
    ROUND(r.avg_velocity_active, 2)      AS avg_velocity_active_ms,
    ROUND(r.stale_contact_rate, 2)       AS stale_contact_rate_pct,
    ROUND(r.low_velocity_rate, 2)        AS low_velocity_rate_pct,
    ROUND(r.data_completeness_score, 2)  AS data_completeness_score,
    ROUND(r.operational_stress_index, 2) AS operational_stress_index,
    ROUND(r.telemetry_risk_zscore, 3)    AS telemetry_risk_zscore,
    ROUND(r.disruption_risk_zscore, 3)   AS disruption_risk_zscore,
    ROUND(r.event_severity_score, 2)     AS event_severity_score,
    r.active_event_override,
    r.event_type,
    r.event_source,
    r.disruption_band,
    CASE
        WHEN r.active_event_override THEN 'External disruption event active: execute contingency playbook'
        WHEN r.disruption_band = 'critical' THEN 'Immediate escalation to network control center'
        WHEN r.disruption_band = 'high' THEN 'Pre-position operations and customer comms resources'
        WHEN r.disruption_band = 'watch' THEN 'Monitor next 1-2 windows and prepare fallback plans'
        ELSE 'Normal operations'
    END AS recommended_action
FROM FLIGHTS.KPI.V_COUNTRY_DISRUPTION_RISK r
JOIN latest l
  ON r.window_start = l.latest_window;


-- -------------------------------------------------------------
-- 2) Country Deep Dive Trend (filter by country in dashboard)
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW FLIGHTS.KPI.V_COUNTRY_DEEP_DIVE_TREND AS
WITH trend AS (
    SELECT
        window_start,
        origin_country,
        total_flights,
        pct_grounded,
        capacity_drop_pct,
        avg_velocity_active,
        stale_contact_rate,
        data_completeness_score,
        operational_stress_index,
        telemetry_risk_zscore,
        disruption_risk_zscore,
        event_severity_score,
        active_event_override,
        event_type,
        event_source,
        AVG(operational_stress_index) OVER (
            PARTITION BY origin_country
            ORDER BY window_start
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS stress_ma_6,
        AVG(total_flights) OVER (
            PARTITION BY origin_country
            ORDER BY window_start
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS flights_ma_6
    FROM FLIGHTS.KPI.V_COUNTRY_DISRUPTION_RISK
)
SELECT
    window_start,
    origin_country,
    total_flights,
    ROUND(flights_ma_6, 2)               AS flights_ma_6,
    ROUND(pct_grounded, 2)               AS pct_grounded,
    ROUND(capacity_drop_pct, 2)          AS capacity_drop_pct,
    ROUND(avg_velocity_active, 2)        AS avg_velocity_active_ms,
    ROUND(stale_contact_rate, 2)         AS stale_contact_rate_pct,
    ROUND(data_completeness_score, 2)    AS data_completeness_score,
    ROUND(operational_stress_index, 2)   AS operational_stress_index,
    ROUND(stress_ma_6, 2)                AS stress_ma_6,
    ROUND(telemetry_risk_zscore, 3)      AS telemetry_risk_zscore,
    ROUND(disruption_risk_zscore, 3)     AS disruption_risk_zscore,
    ROUND(event_severity_score, 2)       AS event_severity_score,
    active_event_override,
    event_type,
    event_source
FROM trend;


-- -------------------------------------------------------------
-- 3) Worsening Risk Detection (latest 3 windows acceleration)
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW FLIGHTS.KPI.V_WORSENING_RISK_DETECTION AS
WITH latest_country_windows AS (
    SELECT
        origin_country,
        window_start,
        total_flights,
        operational_stress_index,
        disruption_risk_zscore,
        ROW_NUMBER() OVER (
            PARTITION BY origin_country
            ORDER BY window_start DESC
        ) AS rn
    FROM FLIGHTS.KPI.V_COUNTRY_DISRUPTION_RISK
), pivoted AS (
    SELECT
        origin_country,
        MAX(IFF(rn = 1, window_start, NULL)) AS latest_window,
        MAX(IFF(rn = 1, total_flights, NULL)) AS latest_total_flights,
        MAX(IFF(rn = 1, operational_stress_index, NULL)) AS stress_t0,
        MAX(IFF(rn = 2, operational_stress_index, NULL)) AS stress_t1,
        MAX(IFF(rn = 3, operational_stress_index, NULL)) AS stress_t2,
        MAX(IFF(rn = 1, disruption_risk_zscore, NULL)) AS risk_z_t0
    FROM latest_country_windows
    WHERE rn <= 3
    GROUP BY origin_country
)
SELECT
    latest_window,
    origin_country,
    latest_total_flights,
    ROUND(stress_t0, 2)                                        AS stress_now,
    ROUND(stress_t1, 2)                                        AS stress_prev,
    ROUND(stress_t2, 2)                                        AS stress_prev2,
    ROUND(stress_t0 - stress_t1, 2)                            AS stress_delta_1_window,
    ROUND(stress_t0 - COALESCE(stress_t2, stress_t1), 2)       AS stress_delta_2_windows,
    ROUND(risk_z_t0, 3)                                        AS risk_z_now,
    CASE
        WHEN risk_z_t0 >= 1.5 AND (stress_t0 - stress_t1) >= 5 THEN 'urgent trend deterioration'
        WHEN risk_z_t0 >= 1.0 AND (stress_t0 - stress_t1) >= 2 THEN 'elevated deterioration'
        ELSE 'stable_or_improving'
    END AS trend_signal
FROM pivoted
WHERE latest_total_flights >= 20;


-- -------------------------------------------------------------
-- 4) High-Impact Capacity Allocation Candidates (latest window)
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW FLIGHTS.KPI.V_CAPACITY_ALLOCATION_CANDIDATES_LATEST AS
WITH latest AS (
    SELECT MAX(window_start) AS latest_window
    FROM FLIGHTS.KPI.V_COUNTRY_DISRUPTION_RISK
), scored AS (
    SELECT
        r.origin_country,
        r.total_flights,
        r.capacity_drop_pct,
        r.disruption_risk_zscore,
        r.telemetry_risk_zscore,
        r.event_severity_score,
        r.active_event_override,
        r.event_type,
        r.event_source,
        r.operational_stress_index,
        r.pct_grounded,
        (r.total_flights * GREATEST(r.disruption_risk_zscore, 0)) AS impact_score
    FROM FLIGHTS.KPI.V_COUNTRY_DISRUPTION_RISK r
    JOIN latest l ON r.window_start = l.latest_window
)
SELECT
    origin_country,
    total_flights,
    ROUND(capacity_drop_pct, 2) AS capacity_drop_pct,
    ROUND(telemetry_risk_zscore, 3) AS telemetry_risk_zscore,
    ROUND(disruption_risk_zscore, 3) AS disruption_risk_zscore,
    ROUND(event_severity_score, 2) AS event_severity_score,
    active_event_override,
    event_type,
    event_source,
    ROUND(operational_stress_index, 2) AS operational_stress_index,
    ROUND(pct_grounded, 2) AS pct_grounded,
    ROUND(impact_score, 2) AS impact_score
FROM scored
WHERE total_flights >= 30;


-- -------------------------------------------------------------
-- 5) Decision Confidence / Data Trust Monitor
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW FLIGHTS.KPI.V_DECISION_CONFIDENCE_MONITOR AS
SELECT
    window_start,
    ROUND(AVG(data_completeness_score), 2)          AS avg_data_completeness_score,
    ROUND(AVG(stale_contact_rate), 2)               AS avg_stale_contact_rate_pct,
    COUNT_IF(data_completeness_score < 90)          AS countries_below_quality_threshold,
    COUNT_IF(stale_contact_rate > 25)               AS countries_with_stale_signal,
    COUNT(*)                                        AS total_countries_observed
FROM FLIGHTS.KPI.FLIGHT_KPIS
GROUP BY window_start;


-- -------------------------------------------------------------
-- 6) Active Event Overrides (ground-truth context)
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW FLIGHTS.KPI.V_ACTIVE_EVENT_OVERRIDES AS
SELECT
        region_type,
        region_value,
        event_type,
        source,
        notes,
        severity_score,
        min_disruption_band,
        event_start_utc,
        event_end_utc,
        is_active,
        updated_at
FROM FLIGHTS.KPI.REGIONAL_EVENT_OVERRIDES
WHERE is_active = TRUE
    AND CURRENT_TIMESTAMP() BETWEEN event_start_utc AND event_end_utc;


-- =============================================================
-- Run these SELECTs one-by-one for piece-by-piece insights
-- =============================================================

-- 1) Global risk board
SELECT *
FROM FLIGHTS.KPI.V_EXEC_GLOBAL_RISK_BOARD_LATEST
ORDER BY disruption_risk_zscore DESC, total_flights DESC;

-- 2) Country deep dive (replace country)
SELECT *
FROM FLIGHTS.KPI.V_COUNTRY_DEEP_DIVE_TREND
WHERE origin_country = 'United States'
ORDER BY window_start DESC;

-- 3) Worsening risk detection
SELECT *
FROM FLIGHTS.KPI.V_WORSENING_RISK_DETECTION
ORDER BY risk_z_now DESC, stress_delta_1_window DESC;

-- 4) Capacity allocation candidates (top 20)
SELECT *
FROM FLIGHTS.KPI.V_CAPACITY_ALLOCATION_CANDIDATES_LATEST
ORDER BY impact_score DESC
LIMIT 20;

-- 5) Decision confidence monitor
SELECT *
FROM FLIGHTS.KPI.V_DECISION_CONFIDENCE_MONITOR
ORDER BY window_start DESC;

-- 6) Active event overrides in effect now
SELECT *
FROM FLIGHTS.KPI.V_ACTIVE_EVENT_OVERRIDES
ORDER BY severity_score DESC, event_start_utc DESC;
