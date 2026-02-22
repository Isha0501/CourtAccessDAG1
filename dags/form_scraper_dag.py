"""
dags/form_scraper_dag.py

Airflow DAG — form_scraper_dag
Scheduled: every Monday at 06:00 UTC

Pipeline flow:
  scrape_and_classify → preprocess_data → validate_catalog → detect_anomalies
      → detect_bias → trigger_pretranslation → log_summary → dvc_version_data
"""

import json
import logging
import subprocess

from airflow import DAG
from datetime import datetime
from pathlib import Path
from airflow.providers.standard.operators.python import PythonOperator

from src.scrape_forms import run_scrape
from src.preprocess_forms import run_preprocessing
from src.bias_detection import run_bias_detection

logger = logging.getLogger(__name__)

# ── Paths (inside Docker container) ──────────────────────────────────────────
CATALOG_PATH       = "/opt/airflow/dags/data/form_catalog.json"
FORMS_DIR          = "/opt/airflow/forms"
METRICS_PATH       = "/opt/airflow/dags/data/catalog_metrics.json"
ANOMALY_REPORT_PATH    = "/opt/airflow/dags/data/anomaly_report.json"
PREPROCESS_REPORT_PATH = "/opt/airflow/dags/data/preprocess_report.json"
BIAS_REPORT_PATH       = "/opt/airflow/dags/data/bias_report.json"
PROJECT_ROOT           = "/opt/airflow"

# ── Anomaly thresholds ───────────────────────────────────────────────────────
THRESHOLD_FORM_DROP_PCT       = 20     # Alert if active forms drop >20%
THRESHOLD_MASS_NEW_FORMS      = 50     # Alert if >50 new forms in one run
THRESHOLD_DOWNLOAD_FAIL_PCT   = 10     # Alert if >10% of forms fail to download
THRESHOLD_MIN_PDF_SIZE_BYTES  = 1024   # Alert if PDF is <1KB (likely error page)
THRESHOLD_MAX_PDF_SIZE_BYTES  = 50 * 1024 * 1024  # Alert if PDF is >50MB
THRESHOLD_SCHEMA_ERRORS       = 0      # Alert if any schema validation errors

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


def task_preprocess_data(**context) -> dict:
    """
    Task 2 — Clean, normalize, and validate the scraped data.

    Preprocessing steps:
      1. File type detection — verify .pdf files are actually PDFs (magic bytes)
      2. PDF integrity       — check files aren't corrupt/truncated
      3. Empty file removal  — flag 0-byte or near-empty downloads
      4. Form name cleanup   — trim whitespace, normalize casing
      5. Slug normalization  — ensure clean URL-safe slugs
      6. Duplicate detection — flag different form_ids with identical content_hash

    Modifies the catalog in-place and re-saves it.
    """
    logger.info("Starting data preprocessing.")

    # Load catalog
    catalog_path = Path(CATALOG_PATH)
    if not catalog_path.exists():
        logger.error("Catalog file not found — cannot preprocess.")
        return {"error": "catalog_missing"}

    with open(catalog_path, "r", encoding="utf-8") as f:
        catalog = json.load(f)

    # Run preprocessing (modifies catalog entries in-place)
    report = run_preprocessing(catalog, FORMS_DIR)

    # Save updated catalog (with normalized names, slugs, flags)
    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)
    logger.info("Catalog saved with preprocessing updates.")

    # Save preprocessing report
    report_path = Path(PREPROCESS_REPORT_PATH)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    logger.info("Preprocessing report saved to %s", PREPROCESS_REPORT_PATH)

    # Push to XCom for log_summary
    context["ti"].xcom_push(key="preprocess_report", value=report)
    return report


def task_validate_catalog(**context) -> dict:
    """
    Task 3 — Validate the catalog JSON after preprocessing.
    Checks required fields, data types, PDF existence, and generates metrics.
    """
    from collections import Counter

    logger.info("Starting catalog validation.")

    REQUIRED_TOP_FIELDS = {
        "form_id", "form_name", "form_slug", "source_url", "status",
        "content_hash", "current_version", "needs_human_review",
        "created_at", "last_scraped_at", "appearances", "versions",
    }
    REQUIRED_VERSION_FIELDS = {
        "version", "content_hash", "file_path_original",
        "file_path_es", "file_path_pt", "created_at",
    }
    REQUIRED_APPEARANCE_FIELDS = {"division", "section_heading"}

    errors = []
    warnings = []

    # Load catalog
    catalog_path = Path(CATALOG_PATH)
    if not catalog_path.exists():
        logger.error("Catalog file not found: %s", CATALOG_PATH)
        return {"valid": False, "errors": 1}

    with open(catalog_path, "r", encoding="utf-8") as f:
        catalog = json.load(f)

    status_counts = Counter()
    division_counts = Counter()
    forms_with_es = 0
    forms_with_pt = 0
    missing_pdfs = 0
    total_versions = 0
    seen_ids  = set()
    seen_urls = set()

    for i, entry in enumerate(catalog):
        prefix = f"Entry [{i}]"

        # Check required fields
        missing = REQUIRED_TOP_FIELDS - set(entry.keys())
        if missing:
            errors.append(f"{prefix}: missing fields {missing}")
            continue

        # Duplicate checks
        fid = entry["form_id"]
        if fid in seen_ids:
            errors.append(f"{prefix}: duplicate form_id '{fid}'")
        seen_ids.add(fid)

        url = entry["source_url"]
        if url in seen_urls:
            errors.append(f"{prefix}: duplicate source_url '{url}'")
        seen_urls.add(url)

        # Status validation
        if entry["status"] not in ("active", "archived"):
            errors.append(f"{prefix}: invalid status '{entry['status']}'")
        status_counts[entry["status"]] += 1

        # Version validation
        if not isinstance(entry["current_version"], int) or entry["current_version"] < 1:
            errors.append(f"{prefix}: invalid current_version {entry['current_version']}")

        # Appearances validation
        for j, app in enumerate(entry.get("appearances", [])):
            app_missing = REQUIRED_APPEARANCE_FIELDS - set(app.keys())
            if app_missing:
                errors.append(f"{prefix} appearance[{j}]: missing {app_missing}")
            else:
                division_counts[app["division"]] += 1

        # Versions validation
        versions = entry.get("versions", [])
        if len(versions) == 0:
            errors.append(f"{prefix}: no versions")
        total_versions += len(versions)

        for j, ver in enumerate(versions):
            ver_missing = REQUIRED_VERSION_FIELDS - set(ver.keys())
            if ver_missing:
                errors.append(f"{prefix} version[{j}]: missing {ver_missing}")
                continue

            # Check latest version PDF exists
            if j == 0 and entry["status"] == "active":
                fp = ver["file_path_original"]
                if fp and not Path(fp).exists():
                    missing_pdfs += 1
                    warnings.append(f"{prefix}: PDF not found at '{fp}'")

            # Track translations (latest version)
            if j == 0:
                if ver.get("file_path_es"):
                    forms_with_es += 1
                if ver.get("file_path_pt"):
                    forms_with_pt += 1

    # Build metrics
    active   = status_counts.get("active", 0)
    archived = status_counts.get("archived", 0)

    metrics = {
        "valid":              len(errors) == 0,
        "total_forms":        len(catalog),
        "active_forms":       active,
        "archived_forms":     archived,
        "total_versions":     total_versions,
        "forms_with_es":      forms_with_es,
        "forms_with_pt":      forms_with_pt,
        "unique_divisions":   len(division_counts),
        "division_breakdown": dict(division_counts),
        "missing_pdfs":       missing_pdfs,
        "errors":             len(errors),
        "warnings":           len(warnings),
    }

    # Save metrics to JSON (DVC tracks this)
    metrics_path = Path(METRICS_PATH)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    logger.info(
        "Validation complete: %d forms, %d active, %d archived, "
        "%d errors, %d warnings, %d missing PDFs, "
        "%d with ES, %d with PT",
        len(catalog), active, archived,
        len(errors), len(warnings), missing_pdfs,
        forms_with_es, forms_with_pt,
    )

    if errors:
        for e in errors[:10]:
            logger.error("  %s", e)
        if len(errors) > 10:
            logger.error("  ... and %d more errors", len(errors) - 10)

    # Push metrics to XCom for downstream tasks
    context["ti"].xcom_push(key="validation_metrics", value=metrics)
    return metrics


def task_detect_anomalies(**context) -> dict:
    """
    Task 4 — Detect data anomalies by comparing current run metrics against
    thresholds and previous run metrics. Generates an anomaly report.

    Anomaly checks:
      1. Form count drop     — active forms decreased >20% from previous run
      2. Mass new forms      — >50 new forms in a single run (scraper bug?)
      3. Download failure     — >10% of expected forms not downloaded
      4. Tiny/huge PDFs      — files <1KB (error pages) or >50MB (wrong file)
      5. Schema violations   — any errors from validate_catalog
      6. Missing PDFs        — files in catalog but not on disk
    """
    ti      = context["ti"]
    result  = ti.xcom_pull(task_ids="scrape_and_classify", key="scrape_result")
    metrics = ti.xcom_pull(task_ids="validate_catalog", key="validation_metrics")

    if not result or not metrics:
        logger.warning("Missing scrape result or validation metrics — skipping anomaly detection.")
        return {"anomalies": [], "severity": "skipped"}

    anomalies = []
    counts = result["counts"]

    # ── Load previous metrics (from last run) ─────────────────────────────────
    prev_metrics = None
    prev_metrics_path = Path(ANOMALY_REPORT_PATH).parent / "prev_catalog_metrics.json"
    metrics_path = Path(METRICS_PATH)
    if prev_metrics_path.exists():
        try:
            with open(prev_metrics_path, "r", encoding="utf-8") as f:
                prev_metrics = json.load(f)
        except (json.JSONDecodeError, IOError):
            logger.warning("Could not load previous metrics — first run or corrupted file.")

    # ── Check 1: Form count drop ─────────────────────────────────────────────
    if prev_metrics and prev_metrics.get("active_forms", 0) > 0:
        prev_active = prev_metrics["active_forms"]
        curr_active = metrics.get("active_forms", 0)
        drop_pct = ((prev_active - curr_active) / prev_active) * 100

        if drop_pct > THRESHOLD_FORM_DROP_PCT:
            anomalies.append({
                "check":    "form_count_drop",
                "severity": "CRITICAL",
                "message":  f"Active forms dropped by {drop_pct:.1f}% "
                            f"({prev_active} → {curr_active}). "
                            f"Threshold: {THRESHOLD_FORM_DROP_PCT}%",
                "prev":     prev_active,
                "current":  curr_active,
            })

    # ── Check 2: Mass new forms ───────────────────────────────────────────────
    new_count = counts.get("new", 0)
    if new_count > THRESHOLD_MASS_NEW_FORMS:
        anomalies.append({
            "check":    "mass_new_forms",
            "severity": "WARNING",
            "message":  f"{new_count} new forms detected in a single run. "
                        f"Threshold: {THRESHOLD_MASS_NEW_FORMS}. "
                        f"Possible scraper bug or mass.gov restructure.",
            "count":    new_count,
        })

    # ── Check 3: Download failure rate ────────────────────────────────────────
    total_checked = sum(counts.values())
    total_new_updated = counts.get("new", 0) + counts.get("updated", 0)
    total_expected = metrics.get("total_forms", 0)
    if total_expected > 0 and total_checked > 0:
        # If we found significantly fewer forms than last time
        if prev_metrics:
            prev_total = prev_metrics.get("total_forms", 0)
            if prev_total > 0:
                found_pct = (total_checked / prev_total) * 100
                fail_pct = 100 - found_pct
                if fail_pct > THRESHOLD_DOWNLOAD_FAIL_PCT:
                    anomalies.append({
                        "check":    "download_failure_rate",
                        "severity": "WARNING",
                        "message":  f"Only {total_checked}/{prev_total} forms found "
                                    f"({fail_pct:.1f}% failure rate). "
                                    f"Threshold: {THRESHOLD_DOWNLOAD_FAIL_PCT}%",
                        "found":    total_checked,
                        "expected": prev_total,
                    })

    # ── Check 4: Tiny or huge PDFs ────────────────────────────────────────────
    forms_dir = Path(FORMS_DIR)
    tiny_pdfs  = []
    huge_pdfs  = []
    if forms_dir.exists():
        for pdf_file in forms_dir.rglob("*.pdf"):
            size = pdf_file.stat().st_size
            if size < THRESHOLD_MIN_PDF_SIZE_BYTES:
                tiny_pdfs.append({"file": str(pdf_file), "size_bytes": size})
            elif size > THRESHOLD_MAX_PDF_SIZE_BYTES:
                huge_pdfs.append({"file": str(pdf_file), "size_bytes": size})

    if tiny_pdfs:
        anomalies.append({
            "check":    "tiny_pdfs",
            "severity": "WARNING",
            "message":  f"{len(tiny_pdfs)} PDF(s) are under {THRESHOLD_MIN_PDF_SIZE_BYTES} bytes "
                        f"— likely HTML error pages saved as .pdf",
            "count":    len(tiny_pdfs),
            "files":    tiny_pdfs[:10],  # Cap at 10 to avoid huge report
        })

    if huge_pdfs:
        anomalies.append({
            "check":    "huge_pdfs",
            "severity": "WARNING",
            "message":  f"{len(huge_pdfs)} PDF(s) exceed "
                        f"{THRESHOLD_MAX_PDF_SIZE_BYTES // (1024*1024)}MB",
            "count":    len(huge_pdfs),
            "files":    huge_pdfs[:10],
        })

    # ── Check 5: Schema validation errors ─────────────────────────────────────
    schema_errors = metrics.get("errors", 0)
    if schema_errors > THRESHOLD_SCHEMA_ERRORS:
        anomalies.append({
            "check":    "schema_violations",
            "severity": "CRITICAL",
            "message":  f"{schema_errors} schema validation error(s) found in catalog. "
                        f"Data integrity is compromised.",
            "count":    schema_errors,
        })

    # ── Check 6: Missing PDFs ─────────────────────────────────────────────────
    missing_pdfs = metrics.get("missing_pdfs", 0)
    if missing_pdfs > 0:
        anomalies.append({
            "check":    "missing_pdfs",
            "severity": "WARNING",
            "message":  f"{missing_pdfs} PDF(s) referenced in catalog but not found on disk.",
            "count":    missing_pdfs,
        })

    # ── Determine overall severity ────────────────────────────────────────────
    has_critical = any(a["severity"] == "CRITICAL" for a in anomalies)
    has_warning  = any(a["severity"] == "WARNING" for a in anomalies)

    if has_critical:
        overall_severity = "CRITICAL"
    elif has_warning:
        overall_severity = "WARNING"
    else:
        overall_severity = "OK"

    # ── Build anomaly report ──────────────────────────────────────────────────
    report = {
        "timestamp":      datetime.utcnow().isoformat() + "Z",
        "severity":       overall_severity,
        "anomaly_count":  len(anomalies),
        "anomalies":      anomalies,
        "thresholds": {
            "form_drop_pct":      THRESHOLD_FORM_DROP_PCT,
            "mass_new_forms":     THRESHOLD_MASS_NEW_FORMS,
            "download_fail_pct":  THRESHOLD_DOWNLOAD_FAIL_PCT,
            "min_pdf_size_bytes": THRESHOLD_MIN_PDF_SIZE_BYTES,
            "max_pdf_size_bytes": THRESHOLD_MAX_PDF_SIZE_BYTES,
            "schema_errors":      THRESHOLD_SCHEMA_ERRORS,
        },
    }

    # Save anomaly report
    report_path = Path(ANOMALY_REPORT_PATH)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    # Save current metrics as "previous" for next run comparison
    if metrics_path.exists():
        import shutil
        shutil.copy2(str(metrics_path), str(prev_metrics_path))

    # ── Log alerts ────────────────────────────────────────────────────────────
    if not anomalies:
        logger.info("No anomalies detected. All checks passed.")
    else:
        for a in anomalies:
            if a["severity"] == "CRITICAL":
                logger.critical("ANOMALY [%s]: %s", a["check"], a["message"])
            else:
                logger.warning("ANOMALY [%s]: %s", a["check"], a["message"])

        logger.info(
            "Anomaly detection complete: %d anomaly(ies), severity: %s",
            len(anomalies), overall_severity,
        )

    # Push to XCom for log_summary
    context["ti"].xcom_push(key="anomaly_report", value=report)
    return report


def task_detect_bias(**context) -> dict:
    """
    Task 5 — Detect data coverage bias using data slicing.

    Slices the catalog across:
      1. Division      — form count and translation coverage per court department
      2. Language       — overall ES vs PT coverage, gap analysis
      3. Section heading — forms per section, translation availability
      4. Version        — update frequency distribution

    Flags imbalances that indicate unequal access for LEP individuals.
    """
    logger.info("Starting bias detection / data slicing.")

    # Load catalog
    catalog_path = Path(CATALOG_PATH)
    if not catalog_path.exists():
        logger.error("Catalog file not found — cannot detect bias.")
        return {"error": "catalog_missing"}

    with open(catalog_path, "r", encoding="utf-8") as f:
        catalog = json.load(f)

    # Run bias detection
    report = run_bias_detection(catalog)

    # Save bias report
    report_path = Path(BIAS_REPORT_PATH)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    logger.info("Bias report saved to %s", BIAS_REPORT_PATH)

    # Push to XCom for log_summary
    context["ti"].xcom_push(key="bias_report", value=report)
    return report


def task_trigger_pretranslation(**context) -> None:
    """
    Task 6 — For every form_id that needs translation, trigger
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
    Task 7 — Write the final audit-style summary to the Airflow log.
    Includes scrape results, preprocessing report, validation metrics,
    anomaly report, and bias report.
    """
    ti      = context["ti"]
    result  = ti.xcom_pull(task_ids="scrape_and_classify", key="scrape_result")
    preproc = ti.xcom_pull(task_ids="preprocess_data", key="preprocess_report")
    metrics = ti.xcom_pull(task_ids="validate_catalog", key="validation_metrics")
    anomaly = ti.xcom_pull(task_ids="detect_anomalies", key="anomaly_report")
    bias    = ti.xcom_pull(task_ids="detect_bias", key="bias_report")

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
        total, c["new"], c["updated"], c["deleted"],
        c["renamed"], c["no_change"],
        len(result.get("pretranslation_queue", [])),
    )

    if preproc:
        logger.info(
            "══ Preprocessing Report ══\n"
            "  Forms processed   : %d\n"
            "  Names normalized  : %d\n"
            "  Slugs normalized  : %d\n"
            "  Mislabeled files  : %d\n"
            "  Empty files       : %d\n"
            "  Corrupt files     : %d\n"
            "  Duplicate hashes  : %d",
            preproc.get("total_processed", 0),
            preproc.get("names_normalized", 0),
            preproc.get("slugs_normalized", 0),
            preproc.get("mislabeled_files", 0),
            preproc.get("empty_files", 0),
            preproc.get("corrupt_files", 0),
            preproc.get("duplicate_hashes", 0),
        )

    if metrics:
        logger.info(
            "══ Validation Metrics ══\n"
            "  Active forms   : %d\n"
            "  Archived forms : %d\n"
            "  Forms with ES  : %d\n"
            "  Forms with PT  : %d\n"
            "  Missing PDFs   : %d\n"
            "  Errors         : %d\n"
            "  Valid          : %s",
            metrics.get("active_forms", 0),
            metrics.get("archived_forms", 0),
            metrics.get("forms_with_es", 0),
            metrics.get("forms_with_pt", 0),
            metrics.get("missing_pdfs", 0),
            metrics.get("errors", 0),
            metrics.get("valid", False),
        )

    if anomaly:
        logger.info(
            "══ Anomaly Report ══\n"
            "  Severity       : %s\n"
            "  Anomalies found: %d",
            anomaly.get("severity", "unknown"),
            anomaly.get("anomaly_count", 0),
        )
        for a in anomaly.get("anomalies", []):
            logger.info("    [%s] %s: %s", a["severity"], a["check"], a["message"])
    else:
        logger.info("══ Anomaly Report ══\n  No anomaly data available.")

    if bias:
        logger.info(
            "══ Bias Detection Report ══\n"
            "  Active forms analyzed: %d\n"
            "  Divisions analyzed   : %d\n"
            "  Bias flags           : %d",
            bias.get("total_active_forms", 0),
            len(bias.get("slices", {}).get("by_division", {}).get("data", {})),
            bias.get("bias_count", 0),
        )
        lang = bias.get("slices", {}).get("by_language", {}).get("data", {})
        if lang:
            logger.info(
                "  Spanish coverage     : %s%%\n"
                "  Portuguese coverage  : %s%%",
                lang.get("Spanish", {}).get("coverage_pct", 0),
                lang.get("Portuguese", {}).get("coverage_pct", 0),
            )
        for flag in bias.get("bias_flags", []):
            logger.info("    [%s] %s: %s", flag["severity"], flag["type"], flag["detail"])
    else:
        logger.info("══ Bias Detection Report ══\n  No bias data available.")


def task_dvc_version_data(**context) -> None:
    """
    Task 8 — Version the scraper outputs using DVC.
    Runs 'dvc add' on the catalog and forms directory so changes are tracked.
    Then pushes to the DVC remote storage.
    """
    import os

    logger.info("Starting DVC data versioning.")

    # Run from the project root where .dvc/ lives
    dvc_env = os.environ.copy()

    def _run_dvc(cmd: list[str]) -> bool:
        """Run a DVC command and return True on success."""
        try:
            result = subprocess.run(
                cmd,
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                env=dvc_env,
                timeout=120,
            )
            if result.returncode == 0:
                logger.info("DVC command succeeded: %s", " ".join(cmd))
                if result.stdout.strip():
                    logger.debug("  stdout: %s", result.stdout.strip())
                return True
            else:
                logger.warning(
                    "DVC command failed (rc=%d): %s\n  stderr: %s",
                    result.returncode, " ".join(cmd), result.stderr.strip(),
                )
                return False
        except subprocess.TimeoutExpired:
            logger.warning("DVC command timed out: %s", " ".join(cmd))
            return False
        except FileNotFoundError:
            logger.error("DVC not found. Is it installed in the Docker image?")
            return False

    # Track catalog JSON with DVC
    catalog_tracked = _run_dvc(["dvc", "add", "dags/data/form_catalog.json"])

    # Track forms directory with DVC
    forms_tracked = _run_dvc(["dvc", "add", "forms"])

    # Push to remote storage
    if catalog_tracked or forms_tracked:
        pushed = _run_dvc(["dvc", "push"])
        if pushed:
            logger.info("DVC push completed — data versioned and stored.")
        else:
            logger.warning("DVC push failed — data is tracked locally but not pushed to remote.")
    else:
        logger.info("No DVC changes to push.")

    logger.info("DVC versioning task complete.")


# ══════════════════════════════════════════════════════════════════════════════
# DAG definition
# ══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="form_scraper_dag",
    description="Weekly scrape of mass.gov court forms — preprocess, validate, detect anomalies, version & update catalog",
    schedule="0 6 * * 1",            # Every Monday at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["courtaccess", "forms", "scraping", "preprocessing", "dvc", "anomaly-detection", "bias-detection"],
) as dag:

    t1_scrape = PythonOperator(
        task_id="scrape_and_classify",
        python_callable=task_scrape_and_classify,
    )

    t2_preprocess = PythonOperator(
        task_id="preprocess_data",
        python_callable=task_preprocess_data,
    )

    t3_validate = PythonOperator(
        task_id="validate_catalog",
        python_callable=task_validate_catalog,
    )

    t4_anomaly = PythonOperator(
        task_id="detect_anomalies",
        python_callable=task_detect_anomalies,
    )

    t5_bias = PythonOperator(
        task_id="detect_bias",
        python_callable=task_detect_bias,
    )

    t6_trigger = PythonOperator(
        task_id="trigger_pretranslation",
        python_callable=task_trigger_pretranslation,
    )

    t7_summary = PythonOperator(
        task_id="log_summary",
        python_callable=task_log_summary,
    )

    t8_dvc = PythonOperator(
        task_id="dvc_version_data",
        python_callable=task_dvc_version_data,
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    # scrape → preprocess → validate → detect anomalies → detect bias → trigger → summary → DVC
    t1_scrape >> t2_preprocess >> t3_validate >> t4_anomaly >> t5_bias >> t6_trigger >> t7_summary >> t8_dvc