"""
tests/test_form_scraper.py

Unit tests for scripts/scrape_forms.py

Run with:  pytest tests/test_form_scraper.py -v
"""

import hashlib
import json
import shutil
import sys
import tempfile
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ── Make scripts/ importable ──────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import scripts.scrape_forms as sf


# ══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════════════════

@pytest.fixture(autouse=True)
def tmp_dirs(tmp_path, monkeypatch):
    """
    Redirect CATALOG_PATH and FORMS_DIR to a temp directory so every test
    starts with a clean slate and never touches real files.
    """
    catalog_file = tmp_path / "form_catalog.json"
    forms_dir    = tmp_path / "forms"
    forms_dir.mkdir()

    monkeypatch.setattr(sf, "CATALOG_PATH", catalog_file)
    monkeypatch.setattr(sf, "FORMS_DIR",    forms_dir)
    yield tmp_path


def _make_pdf(content: str = "fake pdf bytes") -> bytes:
    """Return fake bytes that represent a unique PDF."""
    return content.encode()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _make_catalog_entry(**overrides) -> dict:
    """Build a minimal valid catalog entry."""
    form_id = str(uuid.uuid4())
    ts      = "2024-01-01T00:00:00+00:00"
    base    = {
        "form_id":            form_id,
        "form_name":          "Test Form",
        "source_url":         "https://www.mass.gov/test-form.pdf",
        "content_hash":       _sha256(b"original bytes"),
        "version":            1,
        "status":             "active",
        "languages_available": [],
        "gcs_path_original":  f"forms/{form_id}/original.pdf",
        "gcs_path_es":        None,
        "gcs_path_pt":        None,
        "needs_human_review": True,
        "last_scraped_at":    ts,
        "last_updated_at":    ts,
        "created_at":         ts,
    }
    base.update(overrides)
    return base


# ══════════════════════════════════════════════════════════════════════════════
# Catalog helpers
# ══════════════════════════════════════════════════════════════════════════════

class TestCatalogHelpers:
    def test_load_empty_when_missing(self):
        assert sf._load_catalog() == []

    def test_save_and_load_roundtrip(self):
        data = [{"form_id": "abc", "form_name": "Test"}]
        sf._save_catalog(data)
        assert sf._load_catalog() == data

    def test_find_by_url_hit(self):
        entry   = _make_catalog_entry(source_url="https://example.com/a.pdf")
        catalog = [entry]
        found   = sf._find_by_url(catalog, "https://example.com/a.pdf")
        assert found is entry

    def test_find_by_url_miss(self):
        catalog = [_make_catalog_entry(source_url="https://example.com/a.pdf")]
        assert sf._find_by_url(catalog, "https://example.com/b.pdf") is None

    def test_find_by_hash_hit(self):
        h     = _sha256(b"hello")
        entry = _make_catalog_entry(content_hash=h)
        assert sf._find_by_hash([entry], h) is entry

    def test_find_by_hash_miss(self):
        entry = _make_catalog_entry(content_hash=_sha256(b"hello"))
        assert sf._find_by_hash([entry], _sha256(b"world")) is None


# ══════════════════════════════════════════════════════════════════════════════
# SHA-256 helper
# ══════════════════════════════════════════════════════════════════════════════

class TestSha256:
    def test_known_value(self):
        result = sf._sha256(b"")
        assert result == hashlib.sha256(b"").hexdigest()

    def test_different_inputs_differ(self):
        assert sf._sha256(b"a") != sf._sha256(b"b")


# ══════════════════════════════════════════════════════════════════════════════
# Scenario A — new form
# ══════════════════════════════════════════════════════════════════════════════

class TestScenarioA:
    def test_adds_entry_to_catalog(self):
        catalog = []
        queue   = []
        pdf     = _make_pdf("new form")
        sf._handle_new_form(catalog, "New Form", "https://mass.gov/new.pdf", pdf, queue)

        assert len(catalog) == 1
        e = catalog[0]
        assert e["form_name"]         == "New Form"
        assert e["source_url"]        == "https://mass.gov/new.pdf"
        assert e["version"]           == 1
        assert e["status"]            == "active"
        assert e["needs_human_review"] is True
        assert e["content_hash"]      == _sha256(pdf)

    def test_adds_form_id_to_pretranslation_queue(self):
        catalog = []
        queue   = []
        sf._handle_new_form(catalog, "F", "https://mass.gov/f.pdf", _make_pdf(), queue)
        assert len(queue) == 1
        assert queue[0] == catalog[0]["form_id"]

    def test_writes_pdf_to_disk(self, tmp_dirs):
        catalog = []
        queue   = []
        pdf     = _make_pdf("disk test")
        sf._handle_new_form(catalog, "F", "https://mass.gov/f.pdf", pdf, queue)
        saved_path = Path(catalog[0]["gcs_path_original"])
        assert saved_path.exists()
        assert saved_path.read_bytes() == pdf


# ══════════════════════════════════════════════════════════════════════════════
# Scenario B — updated form
# ══════════════════════════════════════════════════════════════════════════════

class TestScenarioB:
    def test_increments_version(self):
        entry = _make_catalog_entry(version=1, content_hash=_sha256(b"old"))
        # Write the old PDF so archive logic has a file to copy.
        old_path = sf._form_dir(entry["form_id"]) / "original.pdf"
        old_path.write_bytes(b"old")
        entry["gcs_path_original"] = str(old_path)

        new_pdf  = _make_pdf("new content")
        new_hash = _sha256(new_pdf)
        queue    = []
        sf._handle_updated_form(entry, new_pdf, new_hash, queue)

        assert entry["version"]      == 2
        assert entry["content_hash"] == new_hash
        assert entry["needs_human_review"] is True

    def test_queues_for_pretranslation(self):
        entry = _make_catalog_entry(version=1)
        old_path = sf._form_dir(entry["form_id"]) / "original.pdf"
        old_path.write_bytes(b"old")
        entry["gcs_path_original"] = str(old_path)

        queue = []
        sf._handle_updated_form(entry, _make_pdf(), _sha256(b"x"), queue)
        assert entry["form_id"] in queue

    def test_archives_old_pdf(self):
        entry = _make_catalog_entry(version=1)
        form_dir = sf._form_dir(entry["form_id"])
        old_path = form_dir / "original.pdf"
        old_path.write_bytes(b"old content")
        entry["gcs_path_original"] = str(old_path)

        sf._handle_updated_form(entry, _make_pdf("new"), _sha256(b"new"), [])

        archive = form_dir / "archive" / "v1" / "original.pdf"
        assert archive.exists()


# ══════════════════════════════════════════════════════════════════════════════
# Scenario C — deleted form (404)
# ══════════════════════════════════════════════════════════════════════════════

class TestScenarioC:
    def test_marks_as_archived(self):
        entry = _make_catalog_entry(status="active")
        sf._handle_deleted_form(entry)
        assert entry["status"] == "archived"

    def test_updates_last_scraped_at(self):
        entry = _make_catalog_entry(last_scraped_at="2000-01-01T00:00:00+00:00")
        sf._handle_deleted_form(entry)
        assert entry["last_scraped_at"] != "2000-01-01T00:00:00+00:00"

    def test_does_not_delete_file(self, tmp_dirs):
        entry    = _make_catalog_entry()
        pdf_path = sf._form_dir(entry["form_id"]) / "original.pdf"
        pdf_path.write_bytes(b"keep me")
        entry["gcs_path_original"] = str(pdf_path)

        sf._handle_deleted_form(entry)
        assert pdf_path.exists()   # File must still be there.


# ══════════════════════════════════════════════════════════════════════════════
# Scenario D — renamed form
# ══════════════════════════════════════════════════════════════════════════════

class TestScenarioD:
    def test_updates_name(self):
        entry = _make_catalog_entry(form_name="Old Name")
        sf._handle_renamed_form(entry, "New Name", entry["source_url"])
        assert entry["form_name"] == "New Name"

    def test_updates_url_when_changed(self):
        entry = _make_catalog_entry(source_url="https://mass.gov/old.pdf")
        sf._handle_renamed_form(entry, entry["form_name"], "https://mass.gov/new.pdf")
        assert entry["source_url"] == "https://mass.gov/new.pdf"

    def test_does_not_queue_pretranslation(self):
        """Rename should NOT touch the pretranslation queue."""
        entry = _make_catalog_entry()
        # _handle_renamed_form doesn't accept a queue — confirm no queue param.
        import inspect
        sig = inspect.signature(sf._handle_renamed_form)
        assert "pretranslation_queue" not in sig.parameters


# ══════════════════════════════════════════════════════════════════════════════
# Scenario E — no change
# ══════════════════════════════════════════════════════════════════════════════

class TestScenarioE:
    def test_updates_last_scraped_at(self):
        entry = _make_catalog_entry(last_scraped_at="2000-01-01T00:00:00+00:00")
        sf._handle_no_change(entry)
        assert entry["last_scraped_at"] != "2000-01-01T00:00:00+00:00"

    def test_does_not_change_version(self):
        entry = _make_catalog_entry(version=3)
        sf._handle_no_change(entry)
        assert entry["version"] == 3

    def test_does_not_change_status(self):
        entry = _make_catalog_entry(status="active")
        sf._handle_no_change(entry)
        assert entry["status"] == "active"


# ══════════════════════════════════════════════════════════════════════════════
# _download_pdf — network behaviour
# ══════════════════════════════════════════════════════════════════════════════

class TestDownloadPdf:
    def test_returns_bytes_on_200(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content     = b"pdf data"
        with patch("scripts.scrape_forms.requests.get", return_value=mock_resp):
            assert sf._download_pdf("https://example.com/a.pdf") == b"pdf data"

    def test_returns_none_on_404(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        with patch("scripts.scrape_forms.requests.get", return_value=mock_resp):
            assert sf._download_pdf("https://example.com/a.pdf") is None

    def test_raises_on_500(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.raise_for_status.side_effect = Exception("500 Server Error")
        with patch("scripts.scrape_forms.requests.get", return_value=mock_resp):
            with pytest.raises(Exception):
                sf._download_pdf("https://example.com/a.pdf")

    def test_raises_on_network_error(self):
        import requests as req_lib
        with patch(
            "scripts.scrape_forms.requests.get",
            side_effect=req_lib.exceptions.ConnectionError("no network"),
        ):
            with pytest.raises(req_lib.exceptions.ConnectionError):
                sf._download_pdf("https://example.com/a.pdf")


# ══════════════════════════════════════════════════════════════════════════════
# run_scrape — integration-level (all network calls mocked)
# ══════════════════════════════════════════════════════════════════════════════

class TestRunScrape:
    """
    These tests mock _scrape_page and _download_pdf so no real HTTP
    requests are made.  They verify the counts returned by run_scrape.
    """

    def _run(self, scraped_forms, download_map):
        """
        Helper: patch _scrape_page to return scraped_forms,
        patch _download_pdf to return values from download_map keyed by URL,
        then call run_scrape() and return the result.
        """
        def fake_scrape(page_url):
            return scraped_forms

        def fake_download(url):
            val = download_map.get(url)
            if isinstance(val, Exception):
                raise val
            return val  # bytes or None

        with patch("scripts.scrape_forms._scrape_page",  side_effect=fake_scrape), \
             patch("scripts.scrape_forms._download_pdf", side_effect=fake_download):
            return sf.run_scrape()

    def test_new_form_increments_count(self):
        pdf = _make_pdf("brand new")
        result = self._run(
            scraped_forms=[{"name": "Form A", "url": "https://mass.gov/a.pdf"}],
            download_map={"https://mass.gov/a.pdf": pdf},
        )
        assert result["counts"]["new"] == 1
        assert len(result["pretranslation_queue"]) == 1

    def test_no_change_increments_count(self, tmp_dirs):
        pdf  = _make_pdf("same content")
        hash_val = _sha256(pdf)

        # Pre-populate catalog with this form.
        entry = _make_catalog_entry(
            source_url="https://mass.gov/a.pdf",
            content_hash=hash_val,
            form_name="Form A",
        )
        sf._save_catalog([entry])

        result = self._run(
            scraped_forms=[{"name": "Form A", "url": "https://mass.gov/a.pdf"}],
            download_map={"https://mass.gov/a.pdf": pdf},
        )
        assert result["counts"]["no_change"] == 1
        assert result["counts"]["new"]       == 0

    def test_updated_form_increments_count(self, tmp_dirs):
        old_pdf = _make_pdf("old content")
        new_pdf = _make_pdf("new content")

        entry = _make_catalog_entry(
            source_url="https://mass.gov/a.pdf",
            content_hash=_sha256(old_pdf),
        )
        # Write old PDF to disk so archive step doesn't fail.
        fpath = sf._form_dir(entry["form_id"]) / "original.pdf"
        fpath.write_bytes(old_pdf)
        entry["gcs_path_original"] = str(fpath)
        sf._save_catalog([entry])

        result = self._run(
            scraped_forms=[{"name": "Form A", "url": "https://mass.gov/a.pdf"}],
            download_map={"https://mass.gov/a.pdf": new_pdf},
        )
        assert result["counts"]["updated"] == 1
        assert len(result["pretranslation_queue"]) == 1

    def test_deleted_form_increments_count(self, tmp_dirs):
        entry = _make_catalog_entry(
            source_url="https://mass.gov/gone.pdf",
            status="active",
        )
        sf._save_catalog([entry])

        # Scrape returns nothing; direct re-check returns None (404).
        result = self._run(
            scraped_forms=[],
            download_map={"https://mass.gov/gone.pdf": None},
        )
        assert result["counts"]["deleted"] == 1

    def test_transient_error_does_not_archive(self, tmp_dirs):
        import requests as req_lib
        entry = _make_catalog_entry(
            source_url="https://mass.gov/flaky.pdf",
            status="active",
        )
        sf._save_catalog([entry])

        result = self._run(
            scraped_forms=[],
            download_map={
                "https://mass.gov/flaky.pdf": req_lib.exceptions.ConnectionError("timeout")
            },
        )
        # Connection error → do not archive.
        assert result["counts"]["deleted"] == 0
        catalog = sf._load_catalog()
        assert catalog[0]["status"] == "active"

    def test_renamed_form_increments_count(self, tmp_dirs):
        pdf      = _make_pdf("same content")
        hash_val = _sha256(pdf)

        entry = _make_catalog_entry(
            source_url="https://mass.gov/old-url.pdf",
            content_hash=hash_val,
            form_name="Old Name",
        )
        sf._save_catalog([entry])

        # New URL, same hash → rename.
        result = self._run(
            scraped_forms=[{"name": "New Name", "url": "https://mass.gov/new-url.pdf"}],
            download_map={"https://mass.gov/new-url.pdf": pdf},
        )
        assert result["counts"]["renamed"] == 1
        assert result["counts"]["new"]     == 0