import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Path bootstrap — allows `scripts/` to be imported as a package from any
# Airflow worker without installing the project as a package.
# ---------------------------------------------------------------------------
AIRFLOW_HOME = Path("/opt/airflow")
if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_ingest import run_bronze_ingestion
from scripts.silver_transform import run_silver_transform
from scripts.gold_aggregate import run_gold_aggregate
from scripts.load_gold_to_snowflake import load_gold_to_snowflake
from scripts.load_regional_event_overrides import load_regional_event_overrides

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default args — applied to every task unless overridden at task level
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
    "email_on_retry": False,
}


def _on_failure_callback(context: dict) -> None:
    task_id = context["task_instance"].task_id
    dag_id  = context["dag"].dag_id
    run_id  = context["run_id"]
    log.error(
        "Task FAILED  dag=%s  task=%s  run=%s",
        dag_id, task_id, run_id,
    )
    # ↳ Extend here: Slack webhook, PagerDuty, email, etc.


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="flights_ops_medallion_pipe",
    description=(
        "OpenSky Network → Bronze (raw JSON) → Silver (cleaned CSV) "
        "→ Gold (KPI aggregates) → Snowflake FLIGHT_KPIS MERGE"
    ),
    doc_md="""
## Flight Ops Medallion Pipeline

Fetches live global flight state vectors from the
[OpenSky Network REST API](https://opensky-network.org/apidoc/rest.html)
every 30 minutes and loads aggregated per-country KPIs into Snowflake.

### Layers
| Layer  | Format | Location                     | Grain                     |
|--------|--------|------------------------------|---------------------------|
| Bronze | JSON   | `data/bronze/`               | Raw API snapshot          |
| Silver | CSV    | `data/silver/`               | One row per aircraft      |
| Gold   | CSV    | `data/gold/`                 | One row per country/window|
| Target | Table  | `FLIGHTS.KPI.FLIGHT_KPIS`    | MERGE upsert              |

### Connections required
- `flight_snowflake` — Snowflake connection (account, warehouse, database, schema, role in Extra JSON)

### Optional env vars
- `OPENSKY_USER` / `OPENSKY_PASS` — unlock higher OpenSky rate limits

### Optional decision-intel input
- `data/reference/regional_event_overrides.csv` — manual/automated event overrides
    (country, severity, min risk band, time window) loaded by
    `load_regional_event_overrides` task.
    """,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["flights", "medallion", "opensky", "snowflake"],
    on_failure_callback=_on_failure_callback,
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze_ingestion,
    )

    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=run_silver_transform,
    )

    gold = PythonOperator(
        task_id="gold_aggregate",
        python_callable=run_gold_aggregate,
    )

    load_to_snowflake = PythonOperator(
        task_id="load_gold_to_snowflake",
        python_callable=load_gold_to_snowflake,
    )

    load_event_overrides = PythonOperator(
        task_id="load_regional_event_overrides",
        python_callable=load_regional_event_overrides,
    )

    bronze >> silver >> gold >> load_to_snowflake >> load_event_overrides