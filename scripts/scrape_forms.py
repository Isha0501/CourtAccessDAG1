"""
scripts/scrape_forms.py

Core scraping logic for the form_scraper_dag.
Kept separate from the DAG file so it can be unit-tested independently.

Storage: local JSON file (data/form_catalog.json)
Later migration: swap _load_catalog / _save_catalog for Cloud SQL queries.
"""

import hashlib
import json
import logging
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR        = Path(__file__).resolve().parent.parent
CATALOG_PATH    = BASE_DIR / "data" / "form_catalog.json"
FORMS_DIR       = BASE_DIR / "forms"
FORMS_DIR.mkdir(parents=True, exist_ok=True)

# ── mass.gov URLs to scrape ───────────────────────────────────────────────────
# Each entry is a human-readable label + the page that lists PDFs for that division.
COURT_FORM_PAGES = [
    {
        "division": "Trial Court Forms",
        "url": "https://www.mass.gov/lists/trial-court-forms",
    },
    {
        "division": "District Court Forms",
        "url": "https://www.mass.gov/lists/district-court-forms",
    },
    # Add more divisions here as needed.
]

# HTTP timeout (seconds) used for every request.
REQUEST_TIMEOUT = 30


# ══════════════════════════════════════════════════════════════════════════════
# Catalog helpers  (swap these two functions to migrate to Cloud SQL later)
# ══════════════════════════════════════════════════════════════════════════════

def _load_catalog() -> list[dict]:
    """Return the full catalog as a list of form-dicts."""
    if not CATALOG_PATH.exists():
        logger.info("Catalog file not found — starting with empty catalog.")
        return []
    with open(CATALOG_PATH, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _save_catalog(catalog: list[dict]) -> None:
    """Persist the catalog back to disk."""
    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CATALOG_PATH, "w", encoding="utf-8") as fh:
        json.dump(catalog, fh, indent=2, ensure_ascii=False)
    logger.debug("Catalog saved to %s", CATALOG_PATH)


def _now() -> str:
    """ISO-8601 UTC timestamp string."""
    return datetime.now(timezone.utc).isoformat()


def _find_by_url(catalog: list[dict], url: str) -> Optional[dict]:
    """Return the catalog entry whose source_url matches, or None."""
    return next((f for f in catalog if f["source_url"] == url), None)


def _find_by_hash(catalog: list[dict], content_hash: str) -> Optional[dict]:
    """Return the first catalog entry whose content_hash matches, or None."""
    return next((f for f in catalog if f["content_hash"] == content_hash), None)


# ══════════════════════════════════════════════════════════════════════════════
# PDF downloading & hashing
# ══════════════════════════════════════════════════════════════════════════════

def _download_pdf(url: str) -> Optional[bytes]:
    """
    Download a PDF from *url*.
    Returns raw bytes on success, None if the server returns 4xx/5xx,
    and raises on network-level errors (so the caller can distinguish
    a genuine 404 from a transient outage).
    """
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
    except requests.exceptions.RequestException as exc:
        # Network error, timeout, DNS failure, etc. — treat as transient.
        logger.warning("Network error fetching %s: %s", url, exc)
        raise   # Re-raise so the DAG task can decide how to handle it.

    if resp.status_code == 404:
        logger.info("404 received for %s", url)
        return None                 # Sentinel: confirmed deletion.

    if resp.status_code >= 400:
        # 5xx or unexpected 4xx → transient; raise so we don't archive.
        resp.raise_for_status()

    return resp.content


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ══════════════════════════════════════════════════════════════════════════════
# Local file-system helpers  (mirrors GCS paths for now)
# ══════════════════════════════════════════════════════════════════════════════

def _form_dir(form_id: str) -> Path:
    d = FORMS_DIR / form_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _archive_dir(form_id: str, version: int) -> Path:
    d = FORMS_DIR / form_id / "archive" / f"v{version}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _save_original(form_id: str, pdf_bytes: bytes) -> str:
    """
    Write the PDF to forms/{form_id}/original.pdf and return the
    local path string (placeholder for GCS path).
    """
    dest = _form_dir(form_id) / "original.pdf"
    dest.write_bytes(pdf_bytes)
    return str(dest)


def _archive_old_versions(form_id: str, old_version: int) -> None:
    """
    Move original.pdf (and any translated copies) into the archive folder
    before writing the new version.
    """
    src_dir  = _form_dir(form_id)
    arch_dir = _archive_dir(form_id, old_version)

    for fname in ("original.pdf", "es.pdf", "pt.pdf"):
        src = src_dir / fname
        if src.exists():
            shutil.copy2(src, arch_dir / fname)
            logger.debug("Archived %s → %s", src, arch_dir / fname)


# ══════════════════════════════════════════════════════════════════════════════
# mass.gov page scraper
# ══════════════════════════════════════════════════════════════════════════════

def _scrape_page(page_url: str) -> list[dict]:
    """
    Fetch a mass.gov court-forms listing page and return a list of:
        {"name": <str>, "url": <str>}
    for every PDF link found.
    """
    try:
        resp = requests.get(page_url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        logger.error("Failed to fetch listing page %s: %s", page_url, exc)
        return []

    soup  = BeautifulSoup(resp.text, "lxml")
    found = []

    # mass.gov wraps PDF links in <a> tags whose href ends with ".pdf".
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if not href.lower().endswith(".pdf"):
            continue

        # Make relative URLs absolute.
        if href.startswith("http"):
            pdf_url = href
        else:
            pdf_url = "https://www.mass.gov" + href

        form_name = tag.get_text(strip=True) or Path(href).stem
        found.append({"name": form_name, "url": pdf_url})

    logger.info("Found %d PDF links on %s", len(found), page_url)
    return found


# ══════════════════════════════════════════════════════════════════════════════
# The five scenario handlers
# ══════════════════════════════════════════════════════════════════════════════

def _handle_new_form(
    catalog: list[dict],
    form_name: str,
    pdf_url: str,
    pdf_bytes: bytes,
    pretranslation_queue: list[str],
) -> None:
    """Scenario A — brand-new URL not in catalog."""
    form_id    = str(uuid.uuid4())
    hash_val   = _sha256(pdf_bytes)
    local_path = _save_original(form_id, pdf_bytes)
    ts         = _now()

    entry = {
        "form_id":           form_id,
        "form_name":         form_name,
        "source_url":        pdf_url,
        "content_hash":      hash_val,
        "version":           1,
        "status":            "active",
        "languages_available": [],
        "gcs_path_original": local_path,   # Will be a real GCS path later.
        "gcs_path_es":       None,
        "gcs_path_pt":       None,
        "needs_human_review": True,
        "last_scraped_at":   ts,
        "last_updated_at":   ts,
        "created_at":        ts,
    }
    catalog.append(entry)
    pretranslation_queue.append(form_id)
    logger.info("Scenario A — New form: '%s' | form_id=%s", form_name, form_id)


def _handle_updated_form(
    entry: dict,
    pdf_bytes: bytes,
    new_hash: str,
    pretranslation_queue: list[str],
) -> None:
    """Scenario B — URL exists but content hash has changed."""
    old_version = entry["version"]
    _archive_old_versions(entry["form_id"], old_version)

    local_path = _save_original(entry["form_id"], pdf_bytes)
    ts         = _now()

    entry["version"]           += 1
    entry["content_hash"]       = new_hash
    entry["needs_human_review"] = True
    entry["gcs_path_original"]  = local_path
    entry["gcs_path_es"]        = None
    entry["gcs_path_pt"]        = None
    entry["languages_available"] = []
    entry["last_updated_at"]    = ts
    entry["last_scraped_at"]    = ts
    entry["status"]             = "active"

    pretranslation_queue.append(entry["form_id"])
    logger.info(
        "Scenario B — Updated form: '%s' | v%d → v%d",
        entry["form_name"], old_version, entry["version"],
    )


def _handle_deleted_form(entry: dict) -> None:
    """Scenario C — URL returned HTTP 404."""
    entry["status"]         = "archived"
    entry["last_scraped_at"] = _now()
    logger.info(
        "Scenario C — Form archived (404): '%s' | form_id=%s",
        entry["form_name"], entry["form_id"],
    )


def _handle_renamed_form(entry: dict, new_name: str, new_url: str) -> None:
    """Scenario D — same hash, but name or URL has changed."""
    old_name = entry["form_name"]
    entry["form_name"]      = new_name
    entry["source_url"]     = new_url
    entry["last_scraped_at"] = _now()
    logger.info(
        "Scenario D — Form renamed: '%s' → '%s'", old_name, new_name,
    )


def _handle_no_change(entry: dict) -> None:
    """Scenario E — everything is the same."""
    entry["last_scraped_at"] = _now()
    logger.debug("Scenario E — No change: '%s'", entry["form_name"])


# ══════════════════════════════════════════════════════════════════════════════
# Public entry-point called by the DAG
# ══════════════════════════════════════════════════════════════════════════════

def run_scrape() -> dict:
    """
    Main function.  Scrapes all configured mass.gov pages, classifies
    every PDF into one of 5 scenarios, and updates the catalog.

    Returns a summary dict with counts per scenario and the list of
    form_ids that need pre-translation (for the DAG to act on).
    """
    catalog = _load_catalog()

    # Build a set of URLs already tracked so we can detect deletions.
    tracked_urls = {f["source_url"] for f in catalog if f["status"] == "active"}
    scraped_urls  = set()

    counts = {"new": 0, "updated": 0, "deleted": 0, "renamed": 0, "no_change": 0}
    pretranslation_queue: list[str] = []

    # ── 1. Scrape every configured listing page ───────────────────────────────
    scraped_forms: list[dict] = []
    seen_urls: set[str] = set()
    for page in COURT_FORM_PAGES:
        for form in _scrape_page(page["url"]):
            if form["url"] not in seen_urls:
                seen_urls.add(form["url"])
                scraped_forms.append(form)

    # ── 2. Process each scraped PDF link ─────────────────────────────────────
    for form_info in scraped_forms:
        pdf_url   = form_info["url"]
        form_name = form_info["name"]
        scraped_urls.add(pdf_url)

        # Download the PDF (None = 404, exception = transient error).
        try:
            pdf_bytes = _download_pdf(pdf_url)
        except requests.exceptions.RequestException:
            logger.warning(
                "Transient error downloading %s — skipping this cycle.", pdf_url
            )
            continue

        if pdf_bytes is None:
            # 404 on a PDF we've never seen before — just skip it.
            existing = _find_by_url(catalog, pdf_url)
            if existing:
                _handle_deleted_form(existing)
                counts["deleted"] += 1
            continue

        new_hash  = _sha256(pdf_bytes)
        existing  = _find_by_url(catalog, pdf_url)

        if existing is None:
            # Could be a rename (same hash, different URL).
            same_hash_entry = _find_by_hash(catalog, new_hash)
            if same_hash_entry and same_hash_entry["source_url"] != pdf_url:
                _handle_renamed_form(same_hash_entry, form_name, pdf_url)
                counts["renamed"] += 1
            else:
                # Genuinely new form.
                _handle_new_form(
                    catalog, form_name, pdf_url, pdf_bytes, pretranslation_queue
                )
                counts["new"] += 1
        elif existing["content_hash"] != new_hash:
            # Same URL, different content.
            _handle_updated_form(existing, pdf_bytes, new_hash, pretranslation_queue)
            counts["updated"] += 1
        else:
            # Same URL, same hash — check for a name change.
            if existing["form_name"] != form_name:
                _handle_renamed_form(existing, form_name, pdf_url)
                counts["renamed"] += 1
            else:
                _handle_no_change(existing)
                counts["no_change"] += 1

    # ── 3. Detect deletions for URLs we track but didn't see this run ─────────
    # Only check URLs that were never returned by the scraper at all this cycle.
    # URLs in scraped_urls were already fully processed in Step 2 above.
    for entry in catalog:
        if entry["status"] != "active":
            continue
        if entry["source_url"] in scraped_urls:
            continue   # Already handled above — do not double-process.

        # URL was tracked but missing from scraped results this cycle.
        # Re-request directly to confirm it's a real 404 vs a scrape miss.
        # If the URL appeared anywhere in scraped_urls it was already handled.
        if entry["source_url"] in scraped_urls:
            continue

        try:
            resp_bytes = _download_pdf(entry["source_url"])
        except requests.exceptions.RequestException:
            logger.warning(
                "Transient error re-checking %s — not archiving.", entry["source_url"]
            )
            continue

        if resp_bytes is None:   # Confirmed 404.
            _handle_deleted_form(entry)
            counts["deleted"] += 1

    # ── 4. Persist ────────────────────────────────────────────────────────────
    _save_catalog(catalog)

    total = sum(counts.values())
    logger.info(
        "Weekly form scrape completed. %d forms checked. "
        "%d new, %d updated, %d archived, %d renamed, %d no-change.",
        total,
        counts["new"],
        counts["updated"],
        counts["deleted"],
        counts["renamed"],
        counts["no_change"],
    )

    return {
        "counts":                counts,
        "pretranslation_queue":  pretranslation_queue,
    }