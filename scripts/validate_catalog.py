"""
scripts/validate_catalog.py

Validates the form_catalog.json after each scraper run.
Generates catalog_metrics.json for DVC metrics tracking.

Run standalone:  python scripts/validate_catalog.py
Called by DVC:  dvc repro (stage: validate_catalog)
"""

import json
import logging
import sys
from pathlib import Path
from collections import Counter

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
CATALOG_PATH = Path("dags/data/form_catalog.json")
FORMS_DIR    = Path("forms")
METRICS_PATH = Path("dags/data/catalog_metrics.json")

# ── Required fields per entry ─────────────────────────────────────────────────
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


def validate() -> dict:
    """Validate the catalog and return a metrics dict."""
    errors = []
    warnings = []

    # ── 1. Check catalog file exists ──────────────────────────────────────────
    if not CATALOG_PATH.exists():
        logger.error("Catalog file not found: %s", CATALOG_PATH)
        return {"valid": False, "errors": 1, "error_details": ["Catalog file missing"]}

    # ── 2. Load and parse JSON ────────────────────────────────────────────────
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            catalog = json.load(f)
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in catalog: %s", e)
        return {"valid": False, "errors": 1, "error_details": [f"Invalid JSON: {e}"]}

    if not isinstance(catalog, list):
        errors.append("Catalog root is not a list")
        return {"valid": False, "errors": 1, "error_details": errors}

    total_forms = len(catalog)
    logger.info("Catalog contains %d forms", total_forms)

    # ── 3. Validate each entry ────────────────────────────────────────────────
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

        # Check required top-level fields
        missing = REQUIRED_TOP_FIELDS - set(entry.keys())
        if missing:
            errors.append(f"{prefix}: missing fields {missing}")
            continue

        # Check for duplicate form_id
        fid = entry["form_id"]
        if fid in seen_ids:
            errors.append(f"{prefix}: duplicate form_id '{fid}'")
        seen_ids.add(fid)

        # Check for duplicate source_url
        url = entry["source_url"]
        if url in seen_urls:
            errors.append(f"{prefix}: duplicate source_url '{url}'")
        seen_urls.add(url)

        # Check status is valid
        if entry["status"] not in ("active", "archived"):
            errors.append(f"{prefix}: invalid status '{entry['status']}'")
        status_counts[entry["status"]] += 1

        # Check current_version is positive
        if not isinstance(entry["current_version"], int) or entry["current_version"] < 1:
            errors.append(f"{prefix}: invalid current_version {entry['current_version']}")

        # ── Validate appearances ──────────────────────────────────────────────
        appearances = entry.get("appearances", [])
        if not isinstance(appearances, list):
            errors.append(f"{prefix}: appearances is not a list")
        elif len(appearances) == 0:
            warnings.append(f"{prefix}: no appearances (form not linked to any division)")
        else:
            for j, app in enumerate(appearances):
                app_missing = REQUIRED_APPEARANCE_FIELDS - set(app.keys())
                if app_missing:
                    errors.append(f"{prefix} appearance[{j}]: missing fields {app_missing}")
                else:
                    division_counts[app["division"]] += 1

        # ── Validate versions ─────────────────────────────────────────────────
        versions = entry.get("versions", [])
        if not isinstance(versions, list):
            errors.append(f"{prefix}: versions is not a list")
        elif len(versions) == 0:
            errors.append(f"{prefix}: no versions (form has no files)")
        else:
            total_versions += len(versions)
            for j, ver in enumerate(versions):
                ver_missing = REQUIRED_VERSION_FIELDS - set(ver.keys())
                if ver_missing:
                    errors.append(f"{prefix} version[{j}]: missing fields {ver_missing}")
                    continue

                # Check PDF exists on disk (only for latest version of active forms)
                if j == 0 and entry["status"] == "active":
                    fp = ver["file_path_original"]
                    if fp and not Path(fp).exists():
                        missing_pdfs += 1
                        warnings.append(f"{prefix}: PDF not found at '{fp}'")

                # Track translation availability (latest version)
                if j == 0:
                    if ver.get("file_path_es"):
                        forms_with_es += 1
                    if ver.get("file_path_pt"):
                        forms_with_pt += 1

    # ── 4. Build metrics ──────────────────────────────────────────────────────
    active = status_counts.get("active", 0)
    archived = status_counts.get("archived", 0)

    metrics = {
        "valid":              len(errors) == 0,
        "total_forms":        total_forms,
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

    # Log summary
    logger.info("── Validation Summary ──")
    logger.info("  Active forms    : %d", active)
    logger.info("  Archived forms  : %d", archived)
    logger.info("  Total versions  : %d", total_versions)
    logger.info("  Forms with ES   : %d", forms_with_es)
    logger.info("  Forms with PT   : %d", forms_with_pt)
    logger.info("  Divisions       : %d", len(division_counts))
    logger.info("  Missing PDFs    : %d", missing_pdfs)
    logger.info("  Errors          : %d", len(errors))
    logger.info("  Warnings        : %d", len(warnings))

    if errors:
        logger.error("── Errors ──")
        for e in errors[:20]:  # Cap at 20 to avoid log flood
            logger.error("  %s", e)
        if len(errors) > 20:
            logger.error("  ... and %d more errors", len(errors) - 20)

    if warnings:
        logger.warning("── Warnings ──")
        for w in warnings[:10]:
            logger.warning("  %s", w)
        if len(warnings) > 10:
            logger.warning("  ... and %d more warnings", len(warnings) - 10)

    return metrics


def main():
    metrics = validate()

    # Write metrics JSON for DVC tracking
    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(METRICS_PATH, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    logger.info("Metrics written to %s", METRICS_PATH)

    # Exit with error code if validation failed
    if not metrics["valid"]:
        logger.error("VALIDATION FAILED — %d error(s) found", metrics["errors"])
        sys.exit(1)

    logger.info("VALIDATION PASSED")


if __name__ == "__main__":
    main()