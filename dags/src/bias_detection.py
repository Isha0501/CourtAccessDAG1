"""
dags/src/bias_detection.py

Bias detection and data slicing for the form catalog.
Analyzes coverage equity across court divisions, languages, and section headings.

For the CourtAccess AI system, "bias" means unequal access:
  - If Division A has 40 translated forms and Division B has 0,
    LEP individuals in Division B get worse service.
  - If Spanish has 80% coverage but Portuguese has 10%,
    Portuguese speakers are underserved.

This module slices the catalog data across categorical dimensions
and flags significant imbalances.
"""

import logging
import math
from collections import Counter, defaultdict

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
# A division is "underserved" if its form count is below this fraction of the mean.
UNDERSERVED_THRESHOLD = 0.5  # Less than 50% of average → flagged

# A language is "underserved" if translation coverage is below this percentage.
TRANSLATION_COVERAGE_THRESHOLD = 20  # Less than 20% coverage → flagged


def _compute_stats(values: list[float]) -> dict:
    """Compute basic statistics for a list of numbers."""
    if not values:
        return {"count": 0, "mean": 0, "median": 0, "min": 0, "max": 0, "std_dev": 0}

    n = len(values)
    mean = sum(values) / n
    sorted_vals = sorted(values)
    median = sorted_vals[n // 2] if n % 2 == 1 else (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
    variance = sum((x - mean) ** 2 for x in values) / n if n > 1 else 0
    std_dev = math.sqrt(variance)

    return {
        "count": n,
        "mean": round(mean, 2),
        "median": median,
        "min": min(values),
        "max": max(values),
        "std_dev": round(std_dev, 2),
    }


def run_bias_detection(catalog: list[dict]) -> dict:
    """
    Analyze the form catalog for coverage bias across divisions,
    languages, and section headings.

    Args:
        catalog: The full form catalog (list of dicts).

    Returns:
        A bias report dict with sliced analysis and flagged imbalances.
    """
    # Only analyze active forms
    active_forms = [f for f in catalog if f.get("status") == "active"]
    total_active = len(active_forms)

    if total_active == 0:
        logger.warning("No active forms in catalog — skipping bias detection.")
        return {"total_active": 0, "slices": {}, "bias_flags": []}

    bias_flags: list[dict] = []

    # ══════════════════════════════════════════════════════════════════════════
    # Slice 1: By Division
    # How many forms appear under each division? Are translations equal?
    # ══════════════════════════════════════════════════════════════════════════

    division_forms: dict[str, int] = Counter()
    division_es: dict[str, int] = Counter()
    division_pt: dict[str, int] = Counter()
    division_form_ids: dict[str, set] = defaultdict(set)

    for form in active_forms:
        for app in form.get("appearances", []):
            div = app.get("division", "Unknown")
            fid = form.get("form_id")
            division_forms[div] += 1
            division_form_ids[div].add(fid)

            # Check latest version for translations
            versions = form.get("versions", [])
            if versions:
                latest = versions[0]
                if latest.get("file_path_es"):
                    division_es[div] += 1
                if latest.get("file_path_pt"):
                    division_pt[div] += 1

    # Build per-division slice
    division_slice = {}
    form_counts = list(division_forms.values())
    mean_forms = sum(form_counts) / len(form_counts) if form_counts else 0

    for div in sorted(division_forms.keys()):
        total = division_forms[div]
        es_count = division_es.get(div, 0)
        pt_count = division_pt.get(div, 0)
        es_pct = round((es_count / total) * 100, 1) if total > 0 else 0
        pt_pct = round((pt_count / total) * 100, 1) if total > 0 else 0

        division_slice[div] = {
            "total_forms": total,
            "unique_forms": len(division_form_ids[div]),
            "forms_with_es": es_count,
            "forms_with_pt": pt_count,
            "es_coverage_pct": es_pct,
            "pt_coverage_pct": pt_pct,
        }

        # Flag underserved divisions (form count)
        if total < mean_forms * UNDERSERVED_THRESHOLD:
            bias_flags.append({
                "type": "underserved_division",
                "dimension": "division",
                "slice": div,
                "severity": "WARNING",
                "detail": f"'{div}' has {total} forms — below {UNDERSERVED_THRESHOLD * 100:.0f}% "
                          f"of the mean ({mean_forms:.0f}). May indicate incomplete scraping.",
                "value": total,
                "threshold": round(mean_forms * UNDERSERVED_THRESHOLD, 1),
            })

        # Flag divisions with low ES translation coverage
        if total > 0 and es_pct < TRANSLATION_COVERAGE_THRESHOLD:
            bias_flags.append({
                "type": "low_translation_coverage",
                "dimension": "division × language",
                "slice": f"{div} → Spanish",
                "severity": "WARNING",
                "detail": f"'{div}' has {es_pct}% Spanish coverage ({es_count}/{total}). "
                          f"LEP Spanish speakers in this division are underserved.",
                "value": es_pct,
                "threshold": TRANSLATION_COVERAGE_THRESHOLD,
            })

        # Flag divisions with low PT translation coverage
        if total > 0 and pt_pct < TRANSLATION_COVERAGE_THRESHOLD:
            bias_flags.append({
                "type": "low_translation_coverage",
                "dimension": "division × language",
                "slice": f"{div} → Portuguese",
                "severity": "WARNING",
                "detail": f"'{div}' has {pt_pct}% Portuguese coverage ({pt_count}/{total}). "
                          f"LEP Portuguese speakers in this division are underserved.",
                "value": pt_pct,
                "threshold": TRANSLATION_COVERAGE_THRESHOLD,
            })

    division_stats = _compute_stats(form_counts)

    # ══════════════════════════════════════════════════════════════════════════
    # Slice 2: By Language (overall translation coverage)
    # ══════════════════════════════════════════════════════════════════════════

    total_with_es = sum(
        1 for f in active_forms
        if f.get("versions") and f["versions"][0].get("file_path_es")
    )
    total_with_pt = sum(
        1 for f in active_forms
        if f.get("versions") and f["versions"][0].get("file_path_pt")
    )

    es_overall_pct = round((total_with_es / total_active) * 100, 1) if total_active > 0 else 0
    pt_overall_pct = round((total_with_pt / total_active) * 100, 1) if total_active > 0 else 0

    language_slice = {
        "English": {
            "total_forms": total_active,
            "coverage_pct": 100.0,
        },
        "Spanish": {
            "total_forms": total_with_es,
            "coverage_pct": es_overall_pct,
        },
        "Portuguese": {
            "total_forms": total_with_pt,
            "coverage_pct": pt_overall_pct,
        },
    }

    # Flag overall language gaps
    es_pt_gap = abs(es_overall_pct - pt_overall_pct)
    if es_pt_gap > 30:
        higher = "Spanish" if es_overall_pct > pt_overall_pct else "Portuguese"
        lower = "Portuguese" if higher == "Spanish" else "Spanish"
        bias_flags.append({
            "type": "language_coverage_gap",
            "dimension": "language",
            "slice": f"{higher} vs {lower}",
            "severity": "WARNING",
            "detail": f"{higher} has {max(es_overall_pct, pt_overall_pct)}% coverage "
                      f"while {lower} has {min(es_overall_pct, pt_overall_pct)}%. "
                      f"Gap of {es_pt_gap:.1f}% indicates unequal language support.",
            "value": es_pt_gap,
            "threshold": 30,
        })

    # ══════════════════════════════════════════════════════════════════════════
    # Slice 3: By Section Heading
    # Which sections have the most/fewest forms?
    # ══════════════════════════════════════════════════════════════════════════

    section_forms: dict[str, int] = Counter()
    section_es: dict[str, int] = Counter()
    section_pt: dict[str, int] = Counter()

    for form in active_forms:
        for app in form.get("appearances", []):
            section = app.get("section_heading", "Unknown")
            section_forms[section] += 1

            versions = form.get("versions", [])
            if versions:
                latest = versions[0]
                if latest.get("file_path_es"):
                    section_es[section] += 1
                if latest.get("file_path_pt"):
                    section_pt[section] += 1

    section_slice = {}
    for section in sorted(section_forms.keys()):
        total = section_forms[section]
        es_count = section_es.get(section, 0)
        pt_count = section_pt.get(section, 0)
        section_slice[section] = {
            "total_forms": total,
            "forms_with_es": es_count,
            "forms_with_pt": pt_count,
            "es_coverage_pct": round((es_count / total) * 100, 1) if total > 0 else 0,
            "pt_coverage_pct": round((pt_count / total) * 100, 1) if total > 0 else 0,
        }

    section_counts = list(section_forms.values())
    section_stats = _compute_stats(section_counts)

    # ══════════════════════════════════════════════════════════════════════════
    # Slice 4: Version distribution
    # Are some forms getting more updates than others?
    # ══════════════════════════════════════════════════════════════════════════

    version_counts = [f.get("current_version", 1) for f in active_forms]
    version_stats = _compute_stats(version_counts)

    multi_version_forms = sum(1 for v in version_counts if v > 1)

    # ══════════════════════════════════════════════════════════════════════════
    # Build final report
    # ══════════════════════════════════════════════════════════════════════════

    report = {
        "total_active_forms": total_active,
        "slices": {
            "by_division": {
                "data": division_slice,
                "stats": division_stats,
            },
            "by_language": {
                "data": language_slice,
            },
            "by_section_heading": {
                "data": section_slice,
                "stats": section_stats,
            },
            "by_version": {
                "stats": version_stats,
                "multi_version_forms": multi_version_forms,
            },
        },
        "bias_flags": bias_flags,
        "bias_count": len(bias_flags),
        "thresholds": {
            "underserved_division_pct": UNDERSERVED_THRESHOLD * 100,
            "translation_coverage_min_pct": TRANSLATION_COVERAGE_THRESHOLD,
            "language_gap_max_pct": 30,
        },
    }

    # ── Log summary ───────────────────────────────────────────────────────────
    logger.info("Bias detection complete:")
    logger.info("  Active forms analyzed: %d", total_active)
    logger.info("  Divisions: %d", len(division_slice))
    logger.info("  Section headings: %d", len(section_slice))
    logger.info("  Spanish coverage: %s%%", es_overall_pct)
    logger.info("  Portuguese coverage: %s%%", pt_overall_pct)
    logger.info("  Bias flags: %d", len(bias_flags))

    if bias_flags:
        logger.warning("── Bias Flags ──")
        for flag in bias_flags:
            logger.warning(
                "  [%s] %s — %s",
                flag["severity"], flag["type"], flag["detail"],
            )
    else:
        logger.info("  No bias flags — coverage is balanced across all slices.")

    return report
