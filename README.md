# Flight Ops Decision Intelligence

A data pipeline that converts OpenSky telemetry into actionable disruption intelligence for airline operations.

## Business Outcome

- Detect disruption earlier using quantified risk.
- Prioritize intervention by country for network control, capacity allocation, and customer communications.
- Blend telemetry with external event context to avoid false “normal” signals.

## Architecture

OpenSky API → Bronze → Silver → Gold → Snowflake

- Airflow DAG: `flights_ops_medallion_pipe` (30-minute cadence)
- Core table: `FLIGHTS.KPI.FLIGHT_KPIS`
- Risk engine view: `FLIGHTS.KPI.V_COUNTRY_DISRUPTION_RISK`
- Event context: `FLIGHTS.KPI.REGIONAL_EVENT_OVERRIDES`, `FLIGHTS.KPI.V_ACTIVE_EVENT_OVERRIDES`

## Decision Model

Country-window risk is derived from:

- telemetry stress,
- grounding anomaly,
- capacity-drop anomaly,
- optional event overrides.

Risk bands: `normal`, `watch`, `high`, `critical`.

## Analytics Products

- SQL DDL / model bootstrap: `sql/create_tables.sql`
- Executive query pack / dashboard views: `sql/business_decision_intelligence.sql`
- Includes: global risk board, country trend, worsening acceleration, capacity-impact ranking, confidence monitor.

## Runbook

1. Start stack: `docker compose up -d`
2. Run Snowflake setup: `sql/create_tables.sql`
3. Run BI views pack: `sql/business_decision_intelligence.sql`
4. Trigger DAG `flights_ops_medallion_pipe`

## Event Override Ingestion

- Source priority: API-first (`EVENT_OVERRIDES_API_URL`) with CSV fallback (`data/reference/regional_event_overrides.csv`).
- Fallback exists for resilience during external API outages and for incident simulation/backfills.

## Scope Note

This is a telemetry-driven decision system. For official operational status, combine with schedule/cancellation/NOTAM-grade sources.
