"""
dags/src/scrape_forms.py

Core scraping logic for the form_scraper_dag.
Kept separate from the DAG file so it can be unit-tested independently.

Storage: local JSON file (dags/data/form_catalog.json)
Later migration: swap _load_catalog / _save_catalog for Cloud SQL queries.

JSON schema per form:
{
    "form_id":          <uuid>,
    "form_name":        <str>,
    "form_slug":        <str>,
    "source_url":       <str>,
    "status":           "active" | "archived",
    "content_hash":     <str>,          ← latest version's hash (denormalized for fast lookup)
    "current_version":  <int>,
    "needs_human_review": <bool>,
    "created_at":       <iso8601>,
    "last_scraped_at":  <iso8601>,
    "appearances":      [{"division": <str>, "section_heading": <str>}, ...],
    "versions":         [{"version": <int>, "content_hash": <str>,
                          "file_path_original": <str>, "file_path_es": <str|null>,
                          "file_path_pt": <str|null>, "created_at": <iso8601>}, ...]
}
"""

import hashlib
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from playwright.sync_api import sync_playwright

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
# File lives at:  dags/src/scrape_forms.py
#   .parent     = dags/src/
#   .parent x2  = dags/           ← DAGS_DIR (catalog data lives here)
#   .parent x3  = project root    ← PROJECT_DIR (forms/ lives here)
#
# In Docker:
#   dags/  = /opt/airflow/dags/   (volume mount)
#   forms/ = /opt/airflow/forms/  (volume mount)

DAGS_DIR = Path(__file__).resolve().parent.parent
PROJECT_DIR = DAGS_DIR.parent
CATALOG_PATH = DAGS_DIR / "data" / "form_catalog.json"
FORMS_DIR = PROJECT_DIR / "forms"
FORMS_DIR.mkdir(parents=True, exist_ok=True)

# ── Batch download settings ───────────────────────────────────────────────────
BATCH_SIZE = 10  # Number of PDFs to download per batch
BATCH_SLEEP_SEC = 15  # Seconds to sleep between batches
PRE_DOWNLOAD_SLEEP = 60  # Seconds to sleep after scraping before downloading
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
    {"division": "Appeals Court", "url": "https://www.mass.gov/lists/appeals-court-forms"},

    # Trial Court Departments
    {"division": "Boston Municipal Court", "url": "https://www.mass.gov/lists/boston-municipal-court-forms"},
    {"division": "District Court", "url": "https://www.mass.gov/lists/district-court-forms"},
    {"division": "Housing Court", "url": "https://www.mass.gov/lists/housing-court-forms"},
    {"division": "Juvenile Court", "url": "https://www.mass.gov/lists/juvenile-court-forms"},
    {"division": "Land Court", "url": "https://www.mass.gov/lists/land-court-forms"},
    {"division": "Superior Court", "url": "https://www.mass.gov/lists/superior-court-forms"},

    # Cross-department form collections
    {"division": "Attorney Forms", "url": "https://www.mass.gov/lists/attorney-court-forms"},
    {"division": "Criminal Matter Forms", "url": "https://www.mass.gov/lists/court-forms-for-criminal-matters"},
    {"division": "Criminal Records Forms", "url": "https://www.mass.gov/lists/court-forms-for-criminal-records"},
    {"division": "Trial Court eFiling Forms", "url": "https://www.mass.gov/lists/trial-court-efiling-forms"},
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
    """Return the first catalog entry whose content_hash matches, or None.
    content_hash is stored at the top level (denormalized from latest version)."""
    return next((f for f in catalog if f["content_hash"] == content_hash), None)


# ══════════════════════════════════════════════════════════════════════════════
# Slug & path helpers
# ══════════════════════════════════════════════════════════════════════════════

def _slug_from_url(url: str) -> str:
    """Extract a human-readable slug from a mass.gov download URL.
    https://www.mass.gov/doc/affidavit-of-indigency/download → affidavit-of-indigency
    """
    # Strip trailing /download, then take the last path segment.
    path = url.rstrip("/")
    if path.endswith("/download"):
        path = path[: -len("/download")]
    return path.rstrip("/").split("/")[-1]


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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
            page = context.new_page()

            # Intercept the download instead of opening it in the browser.
            with page.expect_download(timeout=30000) as dl_info:
                page.goto(url, timeout=30000)

            download = dl_info.value
            path = download.path()

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


# ══════════════════════════════════════════════════════════════════════════════
# Local file-system helpers  (mirrors GCS paths for now)
#
# File path convention:
#   forms/{form_id}/v{version}/{slug}.pdf            ← original English
#   forms/{form_id}/v{version}/{slug}_es.pdf          ← Spanish
#   forms/{form_id}/v{version}/{slug}_pt.pdf          ← Portuguese
#
# Folder uses form_id (UUID — never changes).
# Filename uses slug (human-readable — cosmetic only).
# ══════════════════════════════════════════════════════════════════════════════

def _version_dir(form_id: str, version: int) -> Path:
    """Return (and create) the directory forms/{form_id}/v{version}/."""
    d = FORMS_DIR / form_id / f"v{version}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _save_original(form_id: str, version: int, slug: str, pdf_bytes: bytes) -> str:
    """Save a PDF to forms/{form_id}/v{version}/{slug}.pdf and return the path."""
    dest = _version_dir(form_id, version) / f"{slug}.pdf"
    dest.write_bytes(pdf_bytes)
    return str(dest)


def _save_translation(form_id: str, version: int, slug: str,
                      lang: str, pdf_bytes: bytes) -> str:
    """Save a translation PDF to forms/{form_id}/v{version}/{slug}_{lang}.pdf.
    lang should be 'es' or 'pt'."""
    dest = _version_dir(form_id, version) / f"{slug}_{lang}.pdf"
    dest.write_bytes(pdf_bytes)
    return str(dest)


# ══════════════════════════════════════════════════════════════════════════════
# mass.gov page scraper
# ══════════════════════════════════════════════════════════════════════════════

def _scrape_and_download_page(page_url: str, division: str) -> list[dict]:
    """
    Scrape a mass.gov form listing page AND download all PDFs (including
    existing Spanish/Portuguese translations) in a single Playwright
    browser session.

    Returns a list of:
        {"name": <str>, "url": <str>, "bytes": <bytes>,
         "division": <str>, "section_heading": <str>,
         "es_bytes": <bytes|None>, "pt_bytes": <bytes|None>}
    """
    results = []
    seen = set()

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()

            # Load the listing page.
            page.goto(page_url, wait_until="networkidle", timeout=30000)

            # Scroll to trigger lazy loading.
            for _ in range(5):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(1500)

            # ── Extract form links WITH section headings AND translation URLs ─
            # Walk all h2, h3, and download links in DOM order.
            # Track the "current heading" — each link inherits the heading
            # that most recently preceded it in the document.
            # For each main link, also look inside the parent container for
            # translation links (Español, Português).
            form_data = page.evaluate("""() => {
                const results = [];
                let currentHeading = "General";
                const elements = document.querySelectorAll(
                    'h2, h3, a.ma__download-link__file-link'
                );
                for (const el of elements) {
                    if (el.tagName === 'H2' || el.tagName === 'H3') {
                        currentHeading = el.textContent.trim();
                    } else if (el.classList.contains('ma__download-link__file-link')) {
                        const href = el.getAttribute('href') || '';
                        const nameSpan = el.querySelector('span:not(.ma__visually-hidden)');
                        const name = nameSpan ? nameSpan.textContent.trim() : '';

                        // Find translation links in the parent container.
                        let esUrl = null;
                        let ptUrl = null;
                        const container = el.closest('.ma__download-link') || el.parentElement;
                        if (container) {
                            const tLinks = container.querySelectorAll('a:not(.ma__download-link__file-link)');
                            for (const tl of tLinks) {
                                const text = tl.textContent.trim().toLowerCase();
                                const tHref = tl.getAttribute('href') || '';
                                if (!tHref) continue;
                                if (text.includes('español') || text.includes('espanol') || text === 'spanish') {
                                    esUrl = tHref;
                                } else if (text.includes('português') || text.includes('portugues') || text.includes('portugal') || text === 'portuguese') {
                                    ptUrl = tHref;
                                }
                            }
                        }

                        results.push({
                            name: name,
                            href: href,
                            section_heading: currentHeading,
                            es_url: esUrl,
                            pt_url: ptUrl
                        });
                    }
                }
                return results;
            }""")

            forms = []
            for item in form_data:
                href = item["href"]
                if not href:
                    continue
                if not href.startswith("http"):
                    href = "https://www.mass.gov" + href
                if href in seen:
                    continue
                seen.add(href)

                name = item["name"]
                if not name:
                    slug = href.rstrip("/").split("/")[-2]
                    name = slug.replace("-", " ").title()

                # Make translation URLs absolute (if present).
                es_url = item.get("es_url")
                pt_url = item.get("pt_url")
                if es_url and not es_url.startswith("http"):
                    es_url = "https://www.mass.gov" + es_url
                if pt_url and not pt_url.startswith("http"):
                    pt_url = "https://www.mass.gov" + pt_url

                forms.append({
                    "name": name,
                    "url": href,
                    "section_heading": item["section_heading"],
                    "es_url": es_url,
                    "pt_url": pt_url,
                })

            logger.info(
                "Found %d forms on %s — sleeping %ds before downloading...",
                len(forms), page_url, PRE_DOWNLOAD_SLEEP
            )
            time.sleep(PRE_DOWNLOAD_SLEEP)

            # Download PDFs in batches with a sleep between each batch.
            # For each form: download English, then Spanish/Portuguese if available.
            total = len(forms)
            batches = [forms[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

            for batch_num, batch in enumerate(batches, start=1):
                logger.info(
                    "Downloading batch %d/%d (%d forms)...",
                    batch_num, len(batches), len(batch)
                )
                for form in batch:
                    try:
                        # ── Download English original ──
                        api_response = context.request.get(
                            form["url"],
                            timeout=30000,
                        )
                        if api_response.status != 200:
                            if api_response.status == 404:
                                logger.info("404 for %s — skipping", form["url"])
                            else:
                                logger.warning(
                                    "HTTP %d for %s", api_response.status, form["url"]
                                )
                            continue

                        pdf_bytes = api_response.body()
                        logger.debug(
                            "Downloaded: %s (%d bytes)",
                            form["name"], len(pdf_bytes)
                        )

                        # ── Download Spanish translation (if available) ──
                        es_bytes = None
                        if form["es_url"]:
                            try:
                                es_resp = context.request.get(
                                    form["es_url"], timeout=30000,
                                )
                                if es_resp.status == 200:
                                    es_bytes = es_resp.body()
                                    logger.debug(
                                        "Downloaded ES: %s (%d bytes)",
                                        form["name"], len(es_bytes)
                                    )
                                else:
                                    logger.debug(
                                        "ES translation HTTP %d for %s",
                                        es_resp.status, form["es_url"]
                                    )
                            except Exception as exc:
                                logger.debug("Failed to download ES for %s: %s", form["name"], exc)

                        # ── Download Portuguese translation (if available) ──
                        pt_bytes = None
                        if form["pt_url"]:
                            try:
                                pt_resp = context.request.get(
                                    form["pt_url"], timeout=30000,
                                )
                                if pt_resp.status == 200:
                                    pt_bytes = pt_resp.body()
                                    logger.debug(
                                        "Downloaded PT: %s (%d bytes)",
                                        form["name"], len(pt_bytes)
                                    )
                                else:
                                    logger.debug(
                                        "PT translation HTTP %d for %s",
                                        pt_resp.status, form["pt_url"]
                                    )
                            except Exception as exc:
                                logger.debug("Failed to download PT for %s: %s", form["name"], exc)

                        results.append({
                            "name": form["name"],
                            "url": form["url"],
                            "bytes": pdf_bytes,
                            "es_bytes": es_bytes,
                            "pt_bytes": pt_bytes,
                            "division": division,
                            "section_heading": form["section_heading"],
                        })

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


# ══════════════════════════════════════════════════════════════════════════════
# Appearances helper
# ══════════════════════════════════════════════════════════════════════════════

def _merge_appearances(entry: dict, new_appearances: list[dict]) -> None:
    """Add any new division+heading pairs to the entry's appearances list.
    Skips duplicates (same division already present)."""
    existing_divisions = {a["division"] for a in entry["appearances"]}
    for app in new_appearances:
        if app["division"] not in existing_divisions:
            entry["appearances"].append(app)
            existing_divisions.add(app["division"])
            logger.debug(
                "Added appearance: '%s' → division '%s', heading '%s'",
                entry["form_name"], app["division"], app["section_heading"],
            )


# ══════════════════════════════════════════════════════════════════════════════
# The five scenario handlers
# ══════════════════════════════════════════════════════════════════════════════

def _handle_new_form(catalog, form_name, pdf_url, pdf_bytes,
                     es_bytes, pt_bytes, appearances, pretranslation_queue):
    """Scenario A — brand-new URL not in catalog."""
    form_id = str(uuid.uuid4())
    hash_val = _sha256(pdf_bytes)
    slug = _slug_from_url(pdf_url)
    version = 1
    ts = _now()

    file_path = _save_original(form_id, version, slug, pdf_bytes)

    # Save existing translations from mass.gov (if available).
    file_path_es = None
    if es_bytes:
        file_path_es = _save_translation(form_id, version, slug, "es", es_bytes)
        logger.info("  ↳ Saved existing ES translation for '%s'", form_name)

    file_path_pt = None
    if pt_bytes:
        file_path_pt = _save_translation(form_id, version, slug, "pt", pt_bytes)
        logger.info("  ↳ Saved existing PT translation for '%s'", form_name)

    entry = {
        "form_id": form_id,
        "form_name": form_name,
        "form_slug": slug,
        "source_url": pdf_url,
        "status": "active",
        "content_hash": hash_val,
        "current_version": version,
        "needs_human_review": True,
        "created_at": ts,
        "last_scraped_at": ts,
        "appearances": list(appearances),
        "versions": [
            {
                "version": version,
                "content_hash": hash_val,
                "file_path_original": file_path,
                "file_path_es": file_path_es,
                "file_path_pt": file_path_pt,
                "created_at": ts,
            }
        ],
    }
    catalog.append(entry)
    pretranslation_queue.append(form_id)
    logger.info("Scenario A — New form: '%s' | form_id=%s", form_name, form_id)


def _handle_updated_form(entry, pdf_bytes, new_hash, es_bytes, pt_bytes,
                         pretranslation_queue):
    """Scenario B — URL exists but content hash has changed."""
    old_version = entry["current_version"]
    new_version = old_version + 1
    slug = entry["form_slug"]
    ts = _now()

    file_path = _save_original(entry["form_id"], new_version, slug, pdf_bytes)

    # Save existing translations from mass.gov (if available).
    file_path_es = None
    if es_bytes:
        file_path_es = _save_translation(entry["form_id"], new_version, slug, "es", es_bytes)
        logger.info("  ↳ Saved existing ES translation for '%s'", entry["form_name"])

    file_path_pt = None
    if pt_bytes:
        file_path_pt = _save_translation(entry["form_id"], new_version, slug, "pt", pt_bytes)
        logger.info("  ↳ Saved existing PT translation for '%s'", entry["form_name"])

    # Append new version at the front (newest first). Old versions stay untouched.
    entry["versions"].insert(0, {
        "version": new_version,
        "content_hash": new_hash,
        "file_path_original": file_path,
        "file_path_es": file_path_es,
        "file_path_pt": file_path_pt,
        "created_at": ts,
    })

    entry["current_version"] = new_version
    entry["content_hash"] = new_hash
    entry["needs_human_review"] = True
    entry["last_scraped_at"] = ts
    entry["status"] = "active"

    pretranslation_queue.append(entry["form_id"])
    logger.info(
        "Scenario B — Updated form: '%s' | v%d → v%d",
        entry["form_name"], old_version, new_version,
    )


def _handle_deleted_form(entry):
    """Scenario C — URL returned HTTP 404."""
    entry["status"] = "archived"
    entry["last_scraped_at"] = _now()
    logger.info(
        "Scenario C — Form archived (404): '%s' | form_id=%s",
        entry["form_name"], entry["form_id"],
    )


def _handle_renamed_form(entry, new_name, new_url):
    """Scenario D — same hash, but name or URL has changed."""
    old_name = entry["form_name"]
    entry["form_name"] = new_name
    entry["form_slug"] = _slug_from_url(new_url)
    entry["source_url"] = new_url
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
    scraped_urls = set()

    counts = {"new": 0, "updated": 0, "deleted": 0, "renamed": 0, "no_change": 0}
    pretranslation_queue: list[str] = []

    # ── 1. Scrape every court form page ──────────────────────────────────────
    #    Collect unique forms (first occurrence gets the bytes) and ALL
    #    appearances across departments (for the appearances array).
    scraped_forms: list[dict] = []  # unique forms with bytes
    all_appearances: dict[str, list] = {}  # url → [{division, section_heading}, ...]
    seen_urls: set[str] = set()

    for page_info in COURT_FORM_PAGES:
        for form in _scrape_and_download_page(page_info["url"], page_info["division"]):
            url = form["url"]

            # Track every appearance (even for duplicate URLs across departments).
            if url not in all_appearances:
                all_appearances[url] = []
            all_appearances[url].append({
                "division": form["division"],
                "section_heading": form["section_heading"],
            })

            # Only keep first occurrence for download/processing.
            if url not in seen_urls:
                seen_urls.add(url)
                scraped_forms.append(form)

    logger.info("Total unique forms downloaded: %d", len(scraped_forms))

    # ── 2. Process each downloaded form ───────────────────────────────────────
    for form_info in scraped_forms:
        pdf_url = form_info["url"]
        form_name = form_info["name"]
        pdf_bytes = form_info["bytes"]
        es_bytes = form_info.get("es_bytes")
        pt_bytes = form_info.get("pt_bytes")
        form_appearances = all_appearances.get(pdf_url, [])
        scraped_urls.add(pdf_url)

        new_hash = _sha256(pdf_bytes)
        existing = _find_by_url(catalog, pdf_url)

        if existing is None:
            # URL not in catalog — check if same content exists under a different URL.
            same_hash_entry = _find_by_hash(catalog, new_hash)
            if same_hash_entry and same_hash_entry["source_url"] != pdf_url:
                _handle_renamed_form(same_hash_entry, form_name, pdf_url)
                _merge_appearances(same_hash_entry, form_appearances)
                counts["renamed"] += 1
            else:
                _handle_new_form(
                    catalog, form_name, pdf_url, pdf_bytes,
                    es_bytes, pt_bytes,
                    form_appearances, pretranslation_queue,
                )
                counts["new"] += 1
        elif existing["content_hash"] != new_hash:
            _handle_updated_form(
                existing, pdf_bytes, new_hash,
                es_bytes, pt_bytes, pretranslation_queue,
            )
            _merge_appearances(existing, form_appearances)
            counts["updated"] += 1
        else:
            # Content unchanged — still merge appearances (form may have been
            # added to a new department page since last run).
            _merge_appearances(existing, form_appearances)
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
        "counts": counts,
        "pretranslation_queue": pretranslation_queue,
    }
