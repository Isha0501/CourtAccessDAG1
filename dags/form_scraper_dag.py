"""
dags/form_scraper_dag.py

Airflow DAG — form_scraper_dag
Scheduled: every Monday at 06:00 UTC
"""

import logging
import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Make sure the scripts/ folder is importable from inside Airflow.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.scrape_forms import run_scrape

logger = logging.getLogger(__name__)

# ── DAG default args ──────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "courtaccess",
    "depends_on_past":  False,
    "retries":          1,           # Retry once on transient failures.
    "retry_delay":      60,          # Wait 60 s before retry (seconds).
    "email_on_failure": False,       # Set to True + add email when ready.
    "email_on_retry":   False,
}

# ══════════════════════════════════════════════════════════════════════════════
# Task functions
# ══════════════════════════════════════════════════════════════════════════════

def task_scrape_and_classify(**context) -> dict:
    """
    Task 1 — Run the scraper, classify every form, update the catalog.
    Pushes the summary dict to XCom so downstream tasks can read it.
    """
    logger.info("Starting weekly mass.gov form scrape.")
    result = run_scrape()
    logger.info("Scrape finished. Summary: %s", result["counts"])

    # Push to XCom so the next task can read pretranslation_queue.
    context["ti"].xcom_push(key="scrape_result", value=result)
    return result


def task_trigger_pretranslation(**context) -> None:
    """
    Task 2 — For every form_id that needs translation, trigger
    form_pretranslation_dag via the Airflow REST API (or TriggerDagRunOperator).

    Current implementation: log the queue.
    When form_pretranslation_dag is ready, uncomment the TriggerDagRunOperator
    block below and remove the placeholder.
    """
    ti     = context["ti"]
    result = ti.xcom_pull(task_ids="scrape_and_classify", key="scrape_result")

    if result is None:
        logger.warning("No scrape result found in XCom — nothing to trigger.")
        return

    queue: list[str] = result.get("pretranslation_queue", [])

    if not queue:
        logger.info("No forms need pre-translation this cycle.")
        return

    logger.info(
        "Triggering form_pretranslation_dag for %d form(s): %s",
        len(queue),
        queue,
    )

    # ── Uncomment this block once form_pretranslation_dag is implemented ──────
    # from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    # for form_id in queue:
    #     TriggerDagRunOperator(
    #         task_id=f"trigger_pretranslation_{form_id}",
    #         trigger_dag_id="form_pretranslation_dag",
    #         conf={"form_id": form_id},
    #         dag=dag,
    #     ).execute(context)
    # ─────────────────────────────────────────────────────────────────────────

    # Placeholder: just log for now.
    for form_id in queue:
        logger.info(
            "[PLACEHOLDER] Would trigger form_pretranslation_dag "
            "with conf={'form_id': '%s'}",
            form_id,
        )


def task_log_summary(**context) -> None:
    """
    Task 3 — Write the final audit-style summary to the Airflow log.
    In production this would also write to Cloud Logging / Cloud SQL audit_logs.
    """
    ti     = context["ti"]
    result = ti.xcom_pull(task_ids="scrape_and_classify", key="scrape_result")

    if result is None:
        logger.warning("No scrape result available for summary.")
        return

    c = result["counts"]
    total = sum(c.values())

    logger.info(
        "══ Weekly Form Scrape Summary ══\n"
        "  Total forms checked : %d\n"
        "  New                 : %d\n"
        "  Updated             : %d\n"
        "  Archived (404)      : %d\n"
        "  Renamed             : %d\n"
        "  No change           : %d\n"
        "  Pre-translation jobs: %d",
        total,
        c["new"],
        c["updated"],
        c["deleted"],
        c["renamed"],
        c["no_change"],
        len(result.get("pretranslation_queue", [])),
    )


# ══════════════════════════════════════════════════════════════════════════════
# DAG definition
# ══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="form_scraper_dag",
    description="Weekly scrape of mass.gov court forms — classify & update catalog",
    schedule="0 6 * * 1",            # Every Monday at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["courtaccess", "forms", "scraping"],
) as dag:

    t1_scrape = PythonOperator(
        task_id="scrape_and_classify",
        python_callable=task_scrape_and_classify,
    )

    t2_trigger = PythonOperator(
        task_id="trigger_pretranslation",
        python_callable=task_trigger_pretranslation,
    )

    t3_summary = PythonOperator(
        task_id="log_summary",
        python_callable=task_log_summary,
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    # scrape → trigger pre-translation → log summary
    t1_scrape >> t2_trigger >> t3_summary