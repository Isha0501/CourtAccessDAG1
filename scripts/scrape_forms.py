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

import time
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR        = Path(__file__).resolve().parent.parent
CATALOG_PATH    = BASE_DIR / "data" / "form_catalog.json"
FORMS_DIR       = BASE_DIR / "forms"
FORMS_DIR.mkdir(parents=True, exist_ok=True)

# ── Batch download settings ───────────────────────────────────────────────────
BATCH_SIZE        = 10    # Number of PDFs to download per batch
BATCH_SLEEP_SEC   = 15    # Seconds to sleep between batches
PRE_DOWNLOAD_SLEEP = 60   # Seconds to sleep after scraping before downloading
REQUEST_TIMEOUT = 30
HEADERS = {
    # Mimic a real browser so mass.gov doesn't block us with a 403.
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── Known court form pages ────────────────────────────────────────────────────
# Source: https://www.mass.gov/guides/court-forms-by-court-department
# These are all official Massachusetts Trial Court and Appellate Court
# form listing pages. Add new ones here if mass.gov adds a new department.
COURT_FORM_PAGES = [
    # Appellate Courts
    {"division": "Appeals Court",               "url": "https://www.mass.gov/lists/appeals-court-forms"},
    # Trial Court Departments
    {"division": "Boston Municipal Court",      "url": "https://www.mass.gov/lists/boston-municipal-court-forms"},
    {"division": "District Court",              "url": "https://www.mass.gov/lists/district-court-forms"},
    {"division": "Housing Court",               "url": "https://www.mass.gov/lists/housing-court-forms"},
    {"division": "Juvenile Court",              "url": "https://www.mass.gov/lists/juvenile-court-forms"},
    {"division": "Land Court",                  "url": "https://www.mass.gov/lists/land-court-forms"},
    {"division": "Superior Court",              "url": "https://www.mass.gov/lists/superior-court-forms"},
    # Cross-department form collections
    {"division": "Attorney Forms",              "url": "https://www.mass.gov/lists/attorney-court-forms"},
    {"division": "Criminal Matter Forms",       "url": "https://www.mass.gov/lists/court-forms-for-criminal-matters"},
    {"division": "Criminal Records Forms",      "url": "https://www.mass.gov/lists/court-forms-for-criminal-records"},
    {"division": "Trial Court eFiling Forms",   "url": "https://www.mass.gov/lists/trial-court-efiling-forms"},
]


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

def _download_pdf_playwright(url: str) -> Optional[bytes]:
    """
    Download a PDF using Playwright — needed because mass.gov blocks
    plain requests() calls with 403. Playwright has the full browser
    session and cookies so the download succeeds.
    Returns bytes on success, None on 404, raises on other errors.
    """
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(accept_downloads=True)
            page    = context.new_page()

            # Intercept the download instead of opening it in the browser.
            with page.expect_download(timeout=30000) as dl_info:
                page.goto(url, timeout=30000)

            download = dl_info.value
            path     = download.path()

            if path is None:
                browser.close()
                return None

            data = Path(path).read_bytes()
            browser.close()
            return data

    except Exception as exc:
        logger.warning("Playwright download failed for %s: %s", url, exc)
        raise requests.exceptions.RequestException(str(exc))


def _download_pdf(url: str) -> Optional[bytes]:
    """
    Download a PDF from *url*.
    First tries plain requests (fast). Falls back to Playwright if blocked.
    Returns raw bytes on success, None on 404, raises on transient errors.
    """
    try:
        resp = requests.get(
            url,
            timeout=REQUEST_TIMEOUT,
            headers=HEADERS,
            allow_redirects=True,
        )

        if resp.status_code == 404:
            logger.info("404 received for %s", url)
            return None

        if resp.status_code == 403:
            # mass.gov blocks plain requests — fall through to Playwright.
            logger.debug("403 on %s — retrying with Playwright", url)
            raise requests.exceptions.HTTPError("403")

        if resp.status_code >= 400:
            resp.raise_for_status()

        return resp.content

    except requests.exceptions.HTTPError:
        # Fall back to Playwright for 403s.
        return _download_pdf_playwright(url)

    except requests.exceptions.RequestException as exc:
        logger.warning("Network error fetching %s: %s", url, exc)
        raise


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
    dest = _form_dir(form_id) / "original.pdf"
    dest.write_bytes(pdf_bytes)
    return str(dest)


def _archive_old_versions(form_id: str, old_version: int) -> None:
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

def _scrape_and_download_page(page_url: str) -> list[dict]:
    """
    Scrape a mass.gov form listing page AND download all PDFs in a single
    Playwright browser session. This is much faster than launching a new
    browser for each PDF download.

    Returns a list of:
        {"name": <str>, "url": <str>, "bytes": <bytes>}
    """
    results = []
    seen    = set()

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(accept_downloads=True)
            page    = context.new_page()

            # Load the listing page.
            page.goto(page_url, wait_until="networkidle", timeout=30000)

            # Scroll to trigger lazy loading.
            for _ in range(5):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(1500)

            # Extract all form links.
            links = page.query_selector_all("a.ma__download-link__file-link")
            forms = []
            for link in links:
                href = link.get_attribute("href") or ""
                if not href:
                    continue
                if not href.startswith("http"):
                    href = "https://www.mass.gov" + href
                if href in seen:
                    continue
                seen.add(href)

                name_span = link.query_selector("span:not(.ma__visually-hidden)")
                name = name_span.inner_text().strip() if name_span else ""
                if not name:
                    slug = href.rstrip("/").split("/")[-2]
                    name = slug.replace("-", " ").title()

                forms.append({"name": name, "url": href})

            logger.info("Found %d forms on %s — downloading PDFs...", len(forms), page_url)

            logger.info(
                "Found %d forms on %s — sleeping %ds before downloading...",
                len(forms), page_url, PRE_DOWNLOAD_SLEEP
            )
            time.sleep(PRE_DOWNLOAD_SLEEP)

            # Download PDFs in batches with a sleep between each batch.
            total    = len(forms)
            batches  = [forms[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

            for batch_num, batch in enumerate(batches, start=1):
                logger.info(
                    "Downloading batch %d/%d (%d forms)...",
                    batch_num, len(batches), len(batch)
                )
                for form in batch:
                    try:
                        api_response = context.request.get(
                            form["url"],
                            timeout=30000,
                        )
                        if api_response.status == 200:
                            pdf_bytes = api_response.body()
                            results.append({
                                "name":  form["name"],
                                "url":   form["url"],
                                "bytes": pdf_bytes,
                            })
                            logger.debug(
                                "Downloaded: %s (%d bytes)",
                                form["name"], len(pdf_bytes)
                            )
                        elif api_response.status == 404:
                            logger.info("404 for %s — skipping", form["url"])
                        else:
                            logger.warning(
                                "HTTP %d for %s", api_response.status, form["url"]
                            )
                    except Exception as exc:
                        logger.warning("Failed to download %s: %s", form["url"], exc)

                # Sleep between batches — skip sleep after the last batch.
                if batch_num < len(batches):
                    logger.info(
                        "Batch %d complete. Sleeping %ds before next batch...",
                        batch_num, BATCH_SLEEP_SEC
                    )
                    time.sleep(BATCH_SLEEP_SEC)

            browser.close()

    except Exception as exc:
        logger.error("Playwright error on %s: %s", page_url, exc)

    logger.info(
        "Downloaded %d/%d PDFs from %s",
        len(results), len(seen), page_url
    )
    return results
    """
    Fetch a mass.gov court-forms listing page using Playwright (headless Chrome)
    and return a list of {"name": <str>, "url": <str>} for every form found.

    mass.gov renders forms dynamically via JavaScript. Links use the class
    'ma__download-link__file-link' and point to /doc/.../download endpoints.
    We scroll the full page to trigger lazy loading before extracting links.
    """
    found = []
    seen  = set()

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page    = browser.new_page()
            page.goto(page_url, wait_until="networkidle", timeout=30000)

            # Scroll repeatedly to trigger lazy-loaded content.
            for _ in range(5):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(1500)

            # Extract all form download links.
            links = page.query_selector_all("a.ma__download-link__file-link")

            for link in links:
                href = link.get_attribute("href") or ""
                if not href:
                    continue

                # Make relative URLs absolute.
                if not href.startswith("http"):
                    href = "https://www.mass.gov" + href

                # Skip duplicates.
                if href in seen:
                    continue
                seen.add(href)

                # Form name is in the span that is NOT the visually-hidden one.
                name_span = link.query_selector("span:not(.ma__visually-hidden)")
                name = name_span.inner_text().strip() if name_span else ""

                # Fall back to slug from URL if name is empty.
                if not name:
                    slug  = href.rstrip("/").split("/")[-2]
                    name  = slug.replace("-", " ").title()

                found.append({"name": name, "url": href})

            browser.close()

    except Exception as exc:
        logger.error("Playwright error scraping %s: %s", page_url, exc)

    logger.info("Found %d forms on %s", len(found), page_url)
    return found


# ══════════════════════════════════════════════════════════════════════════════
# The five scenario handlers
# ══════════════════════════════════════════════════════════════════════════════

def _handle_new_form(catalog, form_name, pdf_url, pdf_bytes, pretranslation_queue):
    """Scenario A — brand-new URL not in catalog."""
    form_id    = str(uuid.uuid4())
    hash_val   = _sha256(pdf_bytes)
    local_path = _save_original(form_id, pdf_bytes)
    ts         = _now()

    entry = {
        "form_id":            form_id,
        "form_name":          form_name,
        "source_url":         pdf_url,
        "content_hash":       hash_val,
        "version":            1,
        "status":             "active",
        "languages_available": [],
        "gcs_path_original":  local_path,
        "gcs_path_es":        None,
        "gcs_path_pt":        None,
        "needs_human_review": True,
        "last_scraped_at":    ts,
        "last_updated_at":    ts,
        "created_at":         ts,
    }
    catalog.append(entry)
    pretranslation_queue.append(form_id)
    logger.info("Scenario A — New form: '%s' | form_id=%s", form_name, form_id)


def _handle_updated_form(entry, pdf_bytes, new_hash, pretranslation_queue):
    """Scenario B — URL exists but content hash has changed."""
    old_version = entry["version"]
    _archive_old_versions(entry["form_id"], old_version)

    local_path = _save_original(entry["form_id"], pdf_bytes)
    ts         = _now()

    entry["version"]            += 1
    entry["content_hash"]        = new_hash
    entry["needs_human_review"]  = True
    entry["gcs_path_original"]   = local_path
    entry["gcs_path_es"]         = None
    entry["gcs_path_pt"]         = None
    entry["languages_available"] = []
    entry["last_updated_at"]     = ts
    entry["last_scraped_at"]     = ts
    entry["status"]              = "active"

    pretranslation_queue.append(entry["form_id"])
    logger.info(
        "Scenario B — Updated form: '%s' | v%d → v%d",
        entry["form_name"], old_version, entry["version"],
    )


def _handle_deleted_form(entry):
    """Scenario C — URL returned HTTP 404."""
    entry["status"]          = "archived"
    entry["last_scraped_at"] = _now()
    logger.info(
        "Scenario C — Form archived (404): '%s' | form_id=%s",
        entry["form_name"], entry["form_id"],
    )


def _handle_renamed_form(entry, new_name, new_url):
    """Scenario D — same hash, but name or URL has changed."""
    old_name = entry["form_name"]
    entry["form_name"]       = new_name
    entry["source_url"]      = new_url
    entry["last_scraped_at"] = _now()
    logger.info("Scenario D — Form renamed: '%s' → '%s'", old_name, new_name)


def _handle_no_change(entry):
    """Scenario E — everything is the same."""
    entry["last_scraped_at"] = _now()
    logger.debug("Scenario E — No change: '%s'", entry["form_name"])


# ══════════════════════════════════════════════════════════════════════════════
# Public entry-point called by the DAG
# ══════════════════════════════════════════════════════════════════════════════

def run_scrape() -> dict:
    """
    Main function. Scrapes all court form pages, classifies every PDF
    into one of 5 scenarios, and updates the catalog.
    Returns a summary dict with counts and the pretranslation queue.
    """
    catalog = _load_catalog()
    tracked_urls = {f["source_url"] for f in catalog if f["status"] == "active"}
    scraped_urls  = set()

    counts = {"new": 0, "updated": 0, "deleted": 0, "renamed": 0, "no_change": 0}
    pretranslation_queue: list[str] = []

    # ── 1. Scrape every court form page and download PDFs in one session ──────
    scraped_forms: list[dict] = []
    seen_urls: set[str] = set()
    for page in COURT_FORM_PAGES:
        for form in _scrape_and_download_page(page["url"]):
            if form["url"] not in seen_urls:
                seen_urls.add(form["url"])
                scraped_forms.append(form)

    logger.info("Total unique forms downloaded: %d", len(scraped_forms))

    # ── 2. Process each downloaded form ───────────────────────────────────────
    for form_info in scraped_forms:
        pdf_url   = form_info["url"]
        form_name = form_info["name"]
        pdf_bytes = form_info["bytes"]
        scraped_urls.add(pdf_url)

        new_hash = _sha256(pdf_bytes)
        existing = _find_by_url(catalog, pdf_url)

        if existing is None:
            same_hash_entry = _find_by_hash(catalog, new_hash)
            if same_hash_entry and same_hash_entry["source_url"] != pdf_url:
                _handle_renamed_form(same_hash_entry, form_name, pdf_url)
                counts["renamed"] += 1
            else:
                _handle_new_form(catalog, form_name, pdf_url, pdf_bytes, pretranslation_queue)
                counts["new"] += 1
        elif existing["content_hash"] != new_hash:
            _handle_updated_form(existing, pdf_bytes, new_hash, pretranslation_queue)
            counts["updated"] += 1
        else:
            if existing["form_name"] != form_name:
                _handle_renamed_form(existing, form_name, pdf_url)
                counts["renamed"] += 1
            else:
                _handle_no_change(existing)
                counts["no_change"] += 1

    # ── 3. Detect deletions — URLs we track but didn't see this run ───────────
    for entry in catalog:
        if entry["status"] != "active":
            continue
        if entry["source_url"] in scraped_urls:
            continue

        try:
            resp_bytes = _download_pdf(entry["source_url"])
        except requests.exceptions.RequestException:
            logger.warning("Transient error re-checking %s — not archiving.", entry["source_url"])
            continue

        if resp_bytes is None:
            _handle_deleted_form(entry)
            counts["deleted"] += 1

    # ── 4. Persist ────────────────────────────────────────────────────────────
    _save_catalog(catalog)

    total = sum(counts.values())
    logger.info(
        "Weekly form scrape completed. %d forms checked. "
        "%d new, %d updated, %d archived, %d renamed, %d no-change.",
        total,
        counts["new"], counts["updated"], counts["deleted"],
        counts["renamed"], counts["no_change"],
    )

    return {
        "counts":               counts,
        "pretranslation_queue": pretranslation_queue,
    }