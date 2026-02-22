import hashlib
import sys
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))

import src.scrape_forms as sf


# ══════════════════════════════════════════════════════════════════════════════
# Fixtures & helpers
# ══════════════════════════════════════════════════════════════════════════════

@pytest.fixture(autouse=True)
def tmp_dirs(tmp_path, monkeypatch):
    """
    Redirect CATALOG_PATH and FORMS_DIR to a temp directory so every test
    starts with a clean slate and never touches real files.
    """
    catalog_file = tmp_path / "data" / "form_catalog.json"
    forms_dir = tmp_path / "forms"
    forms_dir.mkdir()

    monkeypatch.setattr(sf, "CATALOG_PATH", catalog_file)
    monkeypatch.setattr(sf, "FORMS_DIR", forms_dir)
    yield tmp_path


def _make_pdf(content: str = "fake pdf bytes") -> bytes:
    """Return fake bytes that represent a unique PDF."""
    return content.encode()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _make_catalog_entry(**overrides) -> dict:
    """Build a minimal valid catalog entry matching the new JSON schema."""
    form_id = str(uuid.uuid4())
    ts = "2024-01-01T00:00:00+00:00"
    pdf = b"original bytes"
    h = _sha256(pdf)
    base = {
        "form_id": form_id,
        "form_name": "Test Form",
        "form_slug": "test-form",
        "source_url": "https://www.mass.gov/doc/test-form/download",
        "status": "active",
        "content_hash": h,
        "current_version": 1,
        "needs_human_review": True,
        "created_at": ts,
        "last_scraped_at": ts,
        "appearances": [
            {"division": "District Court", "section_heading": "General"},
        ],
        "versions": [
            {
                "version": 1,
                "content_hash": h,
                "file_path_original": f"forms/{form_id}/v1/test-form.pdf",
                "file_path_es": None,
                "file_path_pt": None,
                "created_at": ts,
            }
        ],
    }
    base.update(overrides)
    return base


# ══════════════════════════════════════════════════════════════════════════════
# Catalog helpers
# ══════════════════════════════════════════════════════════════════════════════

class TestCatalogHelpers:
    def test_load_empty_when_missing(self):
        """Catalog file doesn't exist → returns empty list."""
        assert sf._load_catalog() == []

    def test_save_and_load_roundtrip(self):
        """Save then load returns the same data."""
        data = [{"form_id": "abc", "form_name": "Test"}]
        sf._save_catalog(data)
        assert sf._load_catalog() == data

    def test_save_creates_parent_directory(self, tmp_path):
        """_save_catalog creates the data/ dir if missing."""
        # CATALOG_PATH is already set to tmp_path/data/form_catalog.json
        # The data/ subdir doesn't exist yet — save should create it.
        sf._save_catalog([{"test": True}])
        assert sf.CATALOG_PATH.exists()

    def test_find_by_url_hit(self):
        entry = _make_catalog_entry(source_url="https://example.com/a.pdf")
        catalog = [entry]
        found = sf._find_by_url(catalog, "https://example.com/a.pdf")
        assert found is entry

    def test_find_by_url_miss(self):
        catalog = [_make_catalog_entry(source_url="https://example.com/a.pdf")]
        assert sf._find_by_url(catalog, "https://example.com/b.pdf") is None

    def test_find_by_hash_hit(self):
        h = _sha256(b"hello")
        entry = _make_catalog_entry(content_hash=h)
        assert sf._find_by_hash([entry], h) is entry

    def test_find_by_hash_miss(self):
        entry = _make_catalog_entry(content_hash=_sha256(b"hello"))
        assert sf._find_by_hash([entry], _sha256(b"world")) is None


# ══════════════════════════════════════════════════════════════════════════════
# Slug extraction
# ══════════════════════════════════════════════════════════════════════════════

class TestSlugFromUrl:
    def test_standard_download_url(self):
        url = "https://www.mass.gov/doc/affidavit-of-indigency/download"
        assert sf._slug_from_url(url) == "affidavit-of-indigency"

    def test_url_without_download_suffix(self):
        url = "https://www.mass.gov/doc/some-form"
        assert sf._slug_from_url(url) == "some-form"

    def test_url_with_trailing_slash(self):
        url = "https://www.mass.gov/doc/some-form/download/"
        assert sf._slug_from_url(url) == "some-form"

    def test_deeply_nested_url(self):
        url = "https://www.mass.gov/a/b/c/my-form/download"
        assert sf._slug_from_url(url) == "my-form"


# ══════════════════════════════════════════════════════════════════════════════
# SHA-256 helper
# ══════════════════════════════════════════════════════════════════════════════

class TestSha256:
    def test_known_value(self):
        result = sf._sha256(b"")
        assert result == hashlib.sha256(b"").hexdigest()

    def test_different_inputs_differ(self):
        assert sf._sha256(b"a") != sf._sha256(b"b")

    def test_deterministic(self):
        assert sf._sha256(b"hello") == sf._sha256(b"hello")


# ══════════════════════════════════════════════════════════════════════════════
# File system helpers
# ══════════════════════════════════════════════════════════════════════════════

class TestFileHelpers:
    def test_version_dir_creates_directory(self):
        d = sf._version_dir("test-id", 1)
        assert d.exists()
        assert d.name == "v1"

    def test_save_original_writes_file(self):
        pdf = _make_pdf("test content")
        path = sf._save_original("test-id", 1, "my-form", pdf)
        assert Path(path).exists()
        assert Path(path).read_bytes() == pdf
        assert path.endswith("my-form.pdf")

    def test_save_original_path_convention(self):
        """Path follows forms/{form_id}/v{version}/{slug}.pdf."""
        path = sf._save_original("abc-123", 2, "some-form", b"data")
        assert "abc-123" in path
        assert "/v2/" in path
        assert path.endswith("some-form.pdf")

    def test_save_translation_spanish(self):
        pdf = _make_pdf("spanish content")
        path = sf._save_translation("test-id", 1, "my-form", "es", pdf)
        assert Path(path).exists()
        assert Path(path).read_bytes() == pdf
        assert path.endswith("my-form_es.pdf")

    def test_save_translation_portuguese(self):
        pdf = _make_pdf("portuguese content")
        path = sf._save_translation("test-id", 1, "my-form", "pt", pdf)
        assert path.endswith("my-form_pt.pdf")

    def test_save_translation_same_directory_as_original(self):
        """Original and translations live in the same version directory."""
        orig = sf._save_original("test-id", 1, "form", b"en")
        es = sf._save_translation("test-id", 1, "form", "es", b"es")
        assert Path(orig).parent == Path(es).parent


# ══════════════════════════════════════════════════════════════════════════════
# Appearances helper
# ══════════════════════════════════════════════════════════════════════════════

class TestMergeAppearances:
    def test_adds_new_division(self):
        entry = _make_catalog_entry(appearances=[
            {"division": "District Court", "section_heading": "General"}
        ])
        sf._merge_appearances(entry, [
            {"division": "Housing Court", "section_heading": "Indigency"}
        ])
        assert len(entry["appearances"]) == 2
        assert entry["appearances"][1]["division"] == "Housing Court"

    def test_skips_duplicate_division(self):
        entry = _make_catalog_entry(appearances=[
            {"division": "District Court", "section_heading": "General"}
        ])
        sf._merge_appearances(entry, [
            {"division": "District Court", "section_heading": "General"}
        ])
        assert len(entry["appearances"]) == 1

    def test_adds_multiple_new_divisions(self):
        entry = _make_catalog_entry(appearances=[])
        sf._merge_appearances(entry, [
            {"division": "District Court", "section_heading": "General"},
            {"division": "Housing Court", "section_heading": "Indigency"},
            {"division": "Land Court", "section_heading": "Forms"},
        ])
        assert len(entry["appearances"]) == 3

    def test_mixed_new_and_existing(self):
        entry = _make_catalog_entry(appearances=[
            {"division": "District Court", "section_heading": "General"}
        ])
        sf._merge_appearances(entry, [
            {"division": "District Court", "section_heading": "General"},  # dup
            {"division": "Housing Court", "section_heading": "Indigency"},  # new
        ])
        assert len(entry["appearances"]) == 2


# ══════════════════════════════════════════════════════════════════════════════
# Scenario A — new form
# ══════════════════════════════════════════════════════════════════════════════

class TestScenarioA:
    def test_adds_entry_to_catalog(self):
        catalog = []
        queue = []
        pdf = _make_pdf("new form")
        appearances = [{"division": "District Court", "section_heading": "General"}]
        sf._handle_new_form(
            catalog, "New Form",
            "https://www.mass.gov/doc/new-form/download",
            pdf, None, None, appearances, queue,
        )

        assert len(catalog) == 1
        e = catalog[0]
        assert e["form_name"] == "New Form"
        assert e["form_slug"] == "new-form"
        assert e["source_url"] == "https://www.mass.gov/doc/new-form/download"
        assert e["current_version"] == 1
        assert e["status"] == "active"
        assert e["needs_human_review"] is True
        assert e["content_hash"] == _sha256(pdf)

    def test_creates_version_entry(self):
        catalog = []
        queue = []
        pdf = _make_pdf("versioned")
        sf._handle_new_form(
            catalog, "F", "https://www.mass.gov/doc/f/download",
            pdf, None, None, [], queue,
        )
        versions = catalog[0]["versions"]
        assert len(versions) == 1
        assert versions[0]["version"] == 1
        assert versions[0]["content_hash"] == _sha256(pdf)

    def test_stores_appearances(self):
        catalog = []
        queue = []
        apps = [
            {"division": "District Court", "section_heading": "General"},
            {"division": "Housing Court", "section_heading": "Indigency"},
        ]
        sf._handle_new_form(
            catalog, "F", "https://www.mass.gov/doc/f/download",
            _make_pdf(), None, None, apps, queue,
        )
        assert len(catalog[0]["appearances"]) == 2

    def test_adds_form_id_to_pretranslation_queue(self):
        catalog = []
        queue = []
        sf._handle_new_form(
            catalog, "F", "https://www.mass.gov/doc/f/download",
            _make_pdf(), None, None, [], queue,
        )
        assert len(queue) == 1
        assert queue[0] == catalog[0]["form_id"]

    def test_writes_pdf_to_disk(self):
        catalog = []
        queue = []
        pdf = _make_pdf("disk test")
        sf._handle_new_form(
            catalog, "F", "https://www.mass.gov/doc/f/download",
            pdf, None, None, [], queue,
        )
        path = catalog[0]["versions"][0]["file_path_original"]
        assert Path(path).exists()
        assert Path(path).read_bytes() == pdf

    def test_saves_spanish_translation_when_available(self):
        catalog = []
        queue = []
        es_pdf = _make_pdf("spanish")
        sf._handle_new_form(
            catalog, "F", "https://www.mass.gov/doc/f/download",
            _make_pdf(), es_pdf, None, [], queue,
        )
        es_path = catalog[0]["versions"][0]["file_path_es"]
        assert es_path is not None
        assert Path(es_path).exists()
        assert Path(es_path).read_bytes() == es_pdf

    def test_saves_portuguese_translation_when_available(self):
        catalog = []
        queue = []
        pt_pdf = _make_pdf("portuguese")
        sf._handle_new_form(
            catalog, "F", "https://www.mass.gov/doc/f/download",
            _make_pdf(), None, pt_pdf, [], queue,
        )
        pt_path = catalog[0]["versions"][0]["file_path_pt"]
        assert pt_path is not None
        assert Path(pt_path).exists()

    def test_translation_paths_null_when_not_available(self):
        catalog = []
        queue = []
        sf._handle_new_form(
            catalog, "F", "https://www.mass.gov/doc/f/download",
            _make_pdf(), None, None, [], queue,
        )
        v = catalog[0]["versions"][0]
        assert v["file_path_es"] is None
        assert v["file_path_pt"] is None


# ══════════════════════════════════════════════════════════════════════════════
# Scenario B — updated form
# ══════════════════════════════════════════════════════════════════════════════

class TestScenarioB:
    def test_increments_version(self):
        entry = _make_catalog_entry(current_version=1, content_hash=_sha256(b"old"))
        new_pdf = _make_pdf("new content")
        new_hash = _sha256(new_pdf)
        queue = []
        sf._handle_updated_form(entry, new_pdf, new_hash, None, None, queue)

        assert entry["current_version"] == 2
        assert entry["content_hash"] == new_hash
        assert entry["needs_human_review"] is True

    def test_appends_new_version_at_front(self):
        entry = _make_catalog_entry(current_version=1)
        new_pdf = _make_pdf("v2 content")
        new_hash = _sha256(new_pdf)
        sf._handle_updated_form(entry, new_pdf, new_hash, None, None, [])

        assert len(entry["versions"]) == 2
        assert entry["versions"][0]["version"] == 2  # newest first
        assert entry["versions"][1]["version"] == 1  # old version preserved

    def test_old_version_untouched(self):
        entry = _make_catalog_entry(current_version=1)
        old_path = entry["versions"][0]["file_path_original"]
        sf._handle_updated_form(entry, _make_pdf("v2"), _sha256(b"v2"), None, None, [])
        assert entry["versions"][1]["file_path_original"] == old_path

    def test_queues_for_pretranslation(self):
        entry = _make_catalog_entry(current_version=1)
        queue = []
        sf._handle_updated_form(entry, _make_pdf(), _sha256(b"x"), None, None, queue)
        assert entry["form_id"] in queue

    def test_writes_new_version_to_disk(self):
        entry = _make_catalog_entry(current_version=1)
        new_pdf = _make_pdf("v2 on disk")
        sf._handle_updated_form(entry, new_pdf, _sha256(new_pdf), None, None, [])
        path = entry["versions"][0]["file_path_original"]
        assert Path(path).exists()
        assert Path(path).read_bytes() == new_pdf
        assert "/v2/" in path

    def test_saves_translations_on_update(self):
        entry = _make_catalog_entry(current_version=1)
        es_pdf = _make_pdf("es v2")
        pt_pdf = _make_pdf("pt v2")
        sf._handle_updated_form(
            entry, _make_pdf("v2"), _sha256(b"v2"), es_pdf, pt_pdf, [],
        )
        v = entry["versions"][0]
        assert v["file_path_es"] is not None
        assert v["file_path_pt"] is not None
        assert Path(v["file_path_es"]).exists()
        assert Path(v["file_path_pt"]).exists()

    def test_translation_null_when_not_available_on_update(self):
        entry = _make_catalog_entry(current_version=1)
        sf._handle_updated_form(entry, _make_pdf("v2"), _sha256(b"v2"), None, None, [])
        v = entry["versions"][0]
        assert v["file_path_es"] is None
        assert v["file_path_pt"] is None


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

    def test_preserves_versions(self):
        """Deleting should NOT remove version history."""
        entry = _make_catalog_entry()
        original_versions = list(entry["versions"])
        sf._handle_deleted_form(entry)
        assert entry["versions"] == original_versions

    def test_preserves_appearances(self):
        entry = _make_catalog_entry()
        original_apps = list(entry["appearances"])
        sf._handle_deleted_form(entry)
        assert entry["appearances"] == original_apps


# ══════════════════════════════════════════════════════════════════════════════
# Scenario D — renamed form
# ══════════════════════════════════════════════════════════════════════════════

class TestScenarioD:
    def test_updates_name(self):
        entry = _make_catalog_entry(form_name="Old Name")
        sf._handle_renamed_form(entry, "New Name", entry["source_url"])
        assert entry["form_name"] == "New Name"

    def test_updates_url(self):
        entry = _make_catalog_entry(source_url="https://www.mass.gov/doc/old/download")
        sf._handle_renamed_form(entry, entry["form_name"], "https://www.mass.gov/doc/new/download")
        assert entry["source_url"] == "https://www.mass.gov/doc/new/download"

    def test_updates_slug(self):
        entry = _make_catalog_entry(form_slug="old-form")
        sf._handle_renamed_form(entry, "New Form", "https://www.mass.gov/doc/new-form/download")
        assert entry["form_slug"] == "new-form"

    def test_does_not_queue_pretranslation(self):
        """Rename should NOT touch the pretranslation queue."""
        import inspect
        sig = inspect.signature(sf._handle_renamed_form)
        assert "pretranslation_queue" not in sig.parameters

    def test_does_not_change_versions(self):
        entry = _make_catalog_entry()
        original_versions = list(entry["versions"])
        sf._handle_renamed_form(entry, "New", "https://www.mass.gov/doc/new/download")
        assert entry["versions"] == original_versions


# ══════════════════════════════════════════════════════════════════════════════
# Scenario E — no change
# ══════════════════════════════════════════════════════════════════════════════

class TestScenarioE:
    def test_updates_last_scraped_at(self):
        entry = _make_catalog_entry(last_scraped_at="2000-01-01T00:00:00+00:00")
        sf._handle_no_change(entry)
        assert entry["last_scraped_at"] != "2000-01-01T00:00:00+00:00"

    def test_does_not_change_version(self):
        entry = _make_catalog_entry(current_version=3)
        sf._handle_no_change(entry)
        assert entry["current_version"] == 3

    def test_does_not_change_status(self):
        entry = _make_catalog_entry(status="active")
        sf._handle_no_change(entry)
        assert entry["status"] == "active"

    def test_does_not_change_content_hash(self):
        h = _sha256(b"stable")
        entry = _make_catalog_entry(content_hash=h)
        sf._handle_no_change(entry)
        assert entry["content_hash"] == h


# ══════════════════════════════════════════════════════════════════════════════
# _download_pdf — network behaviour
# ══════════════════════════════════════════════════════════════════════════════

class TestDownloadPdf:
    def test_returns_bytes_on_200(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"pdf data"
        with patch("src.scrape_forms.requests.get", return_value=mock_resp):
            assert sf._download_pdf("https://example.com/a.pdf") == b"pdf data"

    def test_returns_none_on_404(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        with patch("src.scrape_forms.requests.get", return_value=mock_resp):
            assert sf._download_pdf("https://example.com/a.pdf") is None

    def test_raises_on_500(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.raise_for_status.side_effect = Exception("500 Server Error")
        with patch("src.scrape_forms.requests.get", return_value=mock_resp):
            with pytest.raises(Exception):
                sf._download_pdf("https://example.com/a.pdf")

    def test_raises_on_network_error(self):
        import requests as req_lib
        with patch(
                "src.scrape_forms.requests.get",
                side_effect=req_lib.exceptions.ConnectionError("no network"),
        ):
            with pytest.raises(req_lib.exceptions.ConnectionError):
                sf._download_pdf("https://example.com/a.pdf")

    def test_falls_back_to_playwright_on_403(self):
        """403 from requests should trigger Playwright fallback."""
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        with patch("src.scrape_forms.requests.get", return_value=mock_resp), \
                patch("src.scrape_forms._download_pdf_playwright", return_value=b"pw data") as pw_mock:
            result = sf._download_pdf("https://example.com/a.pdf")
            assert result == b"pw data"
            pw_mock.assert_called_once()


# ══════════════════════════════════════════════════════════════════════════════
# run_scrape — integration-level (all network calls mocked)
# ══════════════════════════════════════════════════════════════════════════════

class TestRunScrape:
    """
    These tests mock _scrape_and_download_page and _download_pdf so
    no real HTTP requests are made. They verify the counts returned
    by run_scrape and the resulting catalog state.
    """

    def _run(self, scraped_forms, download_map=None):
        """
        Helper: patch _scrape_and_download_page to return scraped_forms
        (same list for every department page), and _download_pdf for
        deletion re-checks.
        """
        if download_map is None:
            download_map = {}

        def fake_scrape(page_url, division):
            return scraped_forms

        def fake_download(url):
            val = download_map.get(url)
            if isinstance(val, Exception):
                raise val
            return val  # bytes or None

        with patch("src.scrape_forms._scrape_and_download_page", side_effect=fake_scrape), \
                patch("src.scrape_forms._download_pdf", side_effect=fake_download):
            return sf.run_scrape()

    def test_new_form_increments_count(self):
        pdf = _make_pdf("brand new")
        result = self._run(scraped_forms=[{
            "name": "Form A",
            "url": "https://mass.gov/doc/a/download",
            "bytes": pdf,
            "es_bytes": None,
            "pt_bytes": None,
            "division": "District Court",
            "section_heading": "General",
        }])
        assert result["counts"]["new"] == 1
        assert len(result["pretranslation_queue"]) == 1

    def test_no_change_increments_count(self):
        pdf = _make_pdf("same content")
        hash_val = _sha256(pdf)

        entry = _make_catalog_entry(
            source_url="https://mass.gov/doc/a/download",
            content_hash=hash_val,
            form_name="Form A",
        )
        sf._save_catalog([entry])

        result = self._run(scraped_forms=[{
            "name": "Form A",
            "url": "https://mass.gov/doc/a/download",
            "bytes": pdf,
            "es_bytes": None,
            "pt_bytes": None,
            "division": "District Court",
            "section_heading": "General",
        }])
        assert result["counts"]["no_change"] == 1
        assert result["counts"]["new"] == 0

    def test_updated_form_increments_count(self):
        old_pdf = _make_pdf("old content")
        new_pdf = _make_pdf("new content")

        entry = _make_catalog_entry(
            source_url="https://mass.gov/doc/a/download",
            content_hash=_sha256(old_pdf),
        )
        sf._save_catalog([entry])

        result = self._run(scraped_forms=[{
            "name": "Form A",
            "url": "https://mass.gov/doc/a/download",
            "bytes": new_pdf,
            "es_bytes": None,
            "pt_bytes": None,
            "division": "District Court",
            "section_heading": "General",
        }])
        assert result["counts"]["updated"] == 1
        assert len(result["pretranslation_queue"]) == 1

    def test_deleted_form_increments_count(self):
        entry = _make_catalog_entry(
            source_url="https://mass.gov/doc/gone/download",
            status="active",
        )
        sf._save_catalog([entry])

        # Scrape returns nothing; direct re-check returns None (404).
        result = self._run(
            scraped_forms=[],
            download_map={"https://mass.gov/doc/gone/download": None},
        )
        assert result["counts"]["deleted"] == 1

    def test_transient_error_does_not_archive(self):
        import requests as req_lib
        entry = _make_catalog_entry(
            source_url="https://mass.gov/doc/flaky/download",
            status="active",
        )
        sf._save_catalog([entry])

        result = self._run(
            scraped_forms=[],
            download_map={
                "https://mass.gov/doc/flaky/download":
                    req_lib.exceptions.ConnectionError("timeout")
            },
        )
        assert result["counts"]["deleted"] == 0
        catalog = sf._load_catalog()
        assert catalog[0]["status"] == "active"

    def test_renamed_form_increments_count(self):
        pdf = _make_pdf("same content")
        hash_val = _sha256(pdf)

        entry = _make_catalog_entry(
            source_url="https://mass.gov/doc/old-url/download",
            content_hash=hash_val,
            form_name="Old Name",
        )
        sf._save_catalog([entry])

        result = self._run(scraped_forms=[{
            "name": "New Name",
            "url": "https://mass.gov/doc/new-url/download",
            "bytes": pdf,
            "es_bytes": None,
            "pt_bytes": None,
            "division": "District Court",
            "section_heading": "General",
        }])
        assert result["counts"]["renamed"] == 1
        assert result["counts"]["new"] == 0

    def test_appearances_merged_across_departments(self):
        """The same form appearing on two department pages should have 2 appearances."""
        pdf = _make_pdf("shared form")
        form_template = {
            "name": "Shared Form",
            "url": "https://mass.gov/doc/shared/download",
            "bytes": pdf,
            "es_bytes": None,
            "pt_bytes": None,
            "section_heading": "General",
        }

        def fake_scrape(page_url, division):
            # Use the actual division passed in — mirrors real behavior
            return [{**form_template, "division": division}]

        def fake_download(url):
            return None

        test_pages = [
            {"division": "District Court", "url": "https://mass.gov/lists/district-court-forms"},
            {"division": "Housing Court", "url": "https://mass.gov/lists/housing-court-forms"},
        ]

        with patch("src.scrape_forms._scrape_and_download_page", side_effect=fake_scrape), \
                patch("src.scrape_forms._download_pdf", side_effect=fake_download), \
                patch("src.scrape_forms.COURT_FORM_PAGES", test_pages):
            result = sf.run_scrape()

        catalog = sf._load_catalog()
        assert len(catalog) == 1
        assert result["counts"]["new"] == 1
        assert len(catalog[0]["appearances"]) == 2
        divisions = {a["division"] for a in catalog[0]["appearances"]}
        assert divisions == {"District Court", "Housing Court"}

    def test_empty_scrape_no_crash(self):
        """If mass.gov returns nothing, the pipeline should still complete."""
        result = self._run(scraped_forms=[])
        assert result["counts"]["new"] == 0
        assert result["counts"]["deleted"] == 0

    def test_new_form_with_translations_saves_both(self):
        """New form with ES/PT translations from mass.gov should save all 3 files."""
        pdf = _make_pdf("english")
        es_pdf = _make_pdf("spanish")
        pt_pdf = _make_pdf("portuguese")
        result = self._run(scraped_forms=[{
            "name": "Translated Form",
            "url": "https://mass.gov/doc/translated/download",
            "bytes": pdf,
            "es_bytes": es_pdf,
            "pt_bytes": pt_pdf,
            "division": "District Court",
            "section_heading": "General",
        }])
        catalog = sf._load_catalog()
        v = catalog[0]["versions"][0]
        assert v["file_path_es"] is not None
        assert v["file_path_pt"] is not None
        assert Path(v["file_path_original"]).exists()
        assert Path(v["file_path_es"]).exists()
        assert Path(v["file_path_pt"]).exists()
