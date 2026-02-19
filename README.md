# CourtAccess AI — Form Scraper DAG

Part of the CourtAccess AI system for the Massachusetts Trial Court Language Access initiative.

## What This Does

This pipeline automatically scrapes Massachusetts court forms from mass.gov every Monday, tracks changes, and queues new or updated forms for translation into Spanish and Portuguese.

### The 5 Scenarios It Handles

| Scenario | How Detected | Action |
|---|---|---|
| New form | URL not in catalog | Download, add to catalog, trigger translation |
| Updated form | Same URL, different hash | Archive old version, update catalog, trigger translation |
| Deleted form | URL returns 404 | Mark archived, keep in system with notice |
| Renamed form | Same hash, different name/URL | Update name/URL, no re-translation needed |
| No change | Hash matches | Update last_scraped_at timestamp, done |

---

## Project Structure

```
CourtAccessDAG1/
├── dags/
│   ├── form_scraper_dag.py         # Airflow DAG — runs every Monday at 6am UTC
│   └── form_pretranslation_dag.py  # Placeholder — translation pipeline (coming soon)
├── scripts/
│   ├── scrape_forms.py             # Core scraping logic and scenario handlers
│   └── __init__.py
├── tests/
│   └── test_form_scraper.py        # 33 unit tests (all mocked, no real HTTP calls)
├── data/
│   └── form_catalog.json           # Local catalog — 235 forms tracked
├── requirements.txt
└── README.md
```

---

## Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/Isha0501/CourtAccessDAG1.git
cd CourtAccessDAG1
```

### 2. Create and Activate Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Airflow
```bash
export AIRFLOW_HOME=$(pwd)/airflow
pip install "apache-airflow==3.1.7" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.7/constraints-3.13.txt"
```

### 4. Install Remaining Dependencies
```bash
pip install -r requirements.txt
```

### 5. Install Playwright Browser
```bash
python -m playwright install chromium
```

### 6. Run the Tests
```bash
pytest tests/test_form_scraper.py -v
```
Expected output: `33 passed`

---

## Running the Pipeline

### Every Time You Open a New Terminal
```bash
cd CourtAccessDAG1
source venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
```

### Start Airflow
```bash
airflow standalone
```

Visit `http://localhost:8080`. Password is in `airflow/simple_auth_manager_passwords.json.generated`.

### Trigger the DAG Manually
In a second terminal (with venv active and AIRFLOW_HOME set):
```bash
airflow dags unpause form_scraper_dag
airflow dags trigger form_scraper_dag
```

---

## How the Scraper Works

mass.gov renders court forms dynamically via JavaScript and blocks plain HTTP requests with 403 errors. The scraper uses **Playwright** (headless Chromium) to:

1. Load each court department's form listing page in a real browser
2. Scroll the page to trigger lazy-loaded content
3. Extract all form links using the `ma__download-link__file-link` CSS class
4. Sleep 60 seconds before downloading (to avoid rate limiting)
5. Download PDFs in batches of 10 with 15 second sleeps between batches
6. Use the browser's session/cookies for downloads so mass.gov doesn't block them

### Court Departments Scraped (11 total)

| Department | Forms Found |
|---|---|
| Appeals Court | 22 |
| Boston Municipal Court | 34 |
| District Court | 42 |
| Housing Court | 14 |
| Juvenile Court | 90 |
| Land Court | 46 |
| Superior Court | 30 |
| Attorney Forms | 3 |
| Criminal Matter Forms | 28 |
| Criminal Records Forms | 6 |
| Trial Court eFiling Forms | 5 |
| **Total** | **320 found, 235 unique** |

---

## How the Catalog Works

All form metadata is stored in `data/form_catalog.json`. Each entry tracks:

- `form_id` — unique identifier
- `form_name` — display name
- `source_url` — mass.gov URL
- `content_hash` — SHA-256 hash of the PDF bytes
- `version` — increments every time content changes
- `status` — `active` or `archived`
- `needs_human_review` — true until a court translator verifies the translation
- `last_scraped_at` — timestamp of last check

> **Note:** `data/form_catalog.json` is the local development database. This will be migrated to Cloud SQL on GCP in a later phase. Downloaded PDFs are stored in `forms/` locally and will move to Google Cloud Storage (GCS) in production.

---

## Confirmed DAG Run

The DAG was confirmed working on 2026-02-19:
- **Duration:** 20 minutes 55 seconds
- **Forms checked:** 235
- **Result:** 235 no-change (catalog already populated from manual run)
- **Schedule:** Every Monday at 6am UTC — next run 2026-02-23

---

## Roadmap

- [x] Form scraper DAG with 5-scenario classification
- [x] SHA-256 content hashing and version tracking
- [x] Playwright-based scraping for JavaScript-rendered pages
- [x] Batch download with sleep between batches (rate limit safe)
- [x] 235 forms cataloged across 11 departments
- [x] DAG confirmed running in Airflow — 20 min 55 sec duration
- [x] Unit tests — 33 passing
- [ ] Build `form_pretranslation_dag` (NLLB-200 translation pipeline)
- [ ] Add DVC for data versioning
- [ ] Migrate catalog storage to Cloud SQL on GCP
- [ ] Migrate PDF storage to Google Cloud Storage (GCS)
- [ ] Add Evidently AI drift monitoring