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

## Full Setup Instructions (Start Here)

Follow every step in order. Do not skip steps.

---

### Step 1 — Clone the Repository

```bash
git clone https://github.com/Isha0501/CourtAccessDAG1.git
cd CourtAccessDAG1
```

---

### Step 2 — Create a Virtual Environment

A virtual environment keeps all project dependencies isolated from your system Python.

```bash
python3 -m venv venv
```

**Activate it:**
```bash
source venv/bin/activate
```

Your terminal prompt will now show `(venv)` at the start. This means it worked.

> ⚠️ **Important:** You must activate the venv every time you open a new terminal window before running any project commands. If you don't see `(venv)` in your prompt, run `source venv/bin/activate` again.

**Troubleshooting:**
- If `python3 -m venv venv` fails with `command not found` → install Python 3.10+ from python.org
- If you see `(venv)` disappear after opening a new terminal → that's normal, just run `source venv/bin/activate` again

---

### Step 3 — Set Airflow Home

This tells Airflow to store all its files inside your project folder instead of your home directory.

```bash
export AIRFLOW_HOME=$(pwd)/airflow
```

> ⚠️ This command only lasts for the current terminal session. You must run it every time you open a new terminal. See the "Every Time You Open a New Terminal" section below for a shortcut.

---

### Step 4 — Install Airflow

Airflow requires a special installation command with a constraint file. Do not just `pip install apache-airflow` — it will cause dependency conflicts.

```bash
pip install "apache-airflow==3.1.7" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.7/constraints-3.13.txt"
```

This takes 3-5 minutes. Wait for it to fully finish.

**Verify it worked:**
```bash
airflow version
```
Should print `3.1.7`.

**Troubleshooting:**
- If you get `No matching distribution found` → your Python version is incompatible. Airflow 3.1.7 requires Python 3.10, 3.11, 3.12, or 3.13
- If you get dependency conflict warnings about `requests` → run `pip install "requests>=2.32.0"` afterwards

---

### Step 5 — Install Remaining Dependencies

```bash
pip install -r requirements.txt
```

**Verify:**
```bash
python -c "import requests; import bs4; import playwright; print('all good')"
```
Should print `all good`.

---

### Step 6 — Install Playwright Browser

Playwright needs to download a real Chromium browser to scrape mass.gov (which blocks regular HTTP requests).

```bash
python -m playwright install chromium
```

This downloads ~150MB. Wait for it to finish.

> ⚠️ **Important:** Use `python -m playwright install chromium` not just `playwright install chromium`. The `python -m` prefix ensures it installs inside your venv.

---

### Step 7 — Run the Tests

Verify everything is working before starting Airflow.

```bash
pytest tests/test_form_scraper.py -v
```

Expected output: `33 passed`

**Troubleshooting:**
- If you see `ModuleNotFoundError` → your venv is not activated. Run `source venv/bin/activate` first
- If tests fail → paste the error output and check the specific test name that failed

---

### Step 8 — Initialize and Start Airflow

```bash
airflow standalone
```

Wait 30-60 seconds. When you see this line in the terminal output:

```
standalone | Airflow is ready
```

Open your browser and go to `http://localhost:8080`.

**Finding your login password:**

Airflow generates a random password on first run. Find it with:
```bash
cat airflow/simple_auth_manager_passwords.json.generated
```

> ⚠️ Run this in a **second terminal** (with venv activated and AIRFLOW_HOME set) — do not stop the `airflow standalone` terminal.

Log in with:
- **Username:** `admin`
- **Password:** whatever is printed in that file

**Troubleshooting:**
- If `http://localhost:8080` shows 404 → Airflow is still starting up. Wait 30 more seconds and refresh
- If the password file doesn't exist → check `ls airflow/` and look for a `.json` file with "password" in the name
- If you forgot the password and can't find the file → stop Airflow with `Ctrl+C`, delete the database (`rm airflow/airflow.db airflow/airflow.db-shm airflow/airflow.db-wal`), and restart with `airflow standalone` — it will generate a new password

---

### Step 9 — Fix the DAGs List (Remove Example DAGs)

When you first open the Airflow UI you will see ~80 example DAGs that are not part of this project. Fix this once and it stays fixed.

**In a second terminal (venv activated, AIRFLOW_HOME set):**

```bash
# Turn off example DAGs
sed -i '' 's/load_examples = True/load_examples = False/' airflow/airflow.cfg

# Confirm the dags_folder points to your project
grep "dags_folder" airflow/airflow.cfg
```

The `dags_folder` line should show your project path like:
```
dags_folder = /Users/yourname/CourtAccessDAG1/dags
```

If it doesn't, set it manually:
```bash
sed -i '' "s|dags_folder = .*|dags_folder = $(pwd)/dags|" airflow/airflow.cfg
```

**Then restart Airflow** — stop it with `Ctrl+C` in Terminal 1, delete the old database, and restart:

```bash
rm airflow/airflow.db airflow/airflow.db-shm airflow/airflow.db-wal
export AIRFLOW_HOME=$(pwd)/airflow
airflow standalone
```

After restarting, the UI will only show `form_scraper_dag`.

**Verify in Terminal 2:**
```bash
airflow dags list
```
Should show only `form_scraper_dag`.

---

### Step 10 — Trigger the DAG

```bash
airflow dags unpause form_scraper_dag
airflow dags trigger form_scraper_dag
```

Go to `http://localhost:8080` → click **DAGs** → click **form_scraper_dag** → click the run → click `scrape_and_classify` task → click **Logs** tab.

> ⚠️ The full run takes **40-50 minutes** because it scrapes 11 departments and downloads 235 PDFs with sleep intervals between batches to avoid rate limiting.

---

## Every Time You Open a New Terminal

You must run these three commands before doing anything:

```bash
cd ~/CourtAccessDAG1
source venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
```

**Shortcut — add an alias so you only type one word:**
```bash
echo 'alias courtaccess="cd ~/CourtAccessDAG1 && source venv/bin/activate && export AIRFLOW_HOME=$(pwd)/airflow"' >> ~/.zshrc
source ~/.zshrc
```

From now on just type `courtaccess` in any terminal to set everything up.

---

## Two Terminals — Always

| Terminal | What runs | Never stop it |
|---|---|---|
| **Terminal 1** | `airflow standalone` | Keep running always |
| **Terminal 2** | All other commands (pytest, git, airflow dags trigger, etc.) | Open/close freely |

---

## How the Scraper Works

mass.gov renders court forms dynamically via JavaScript and blocks plain HTTP requests with 403 errors. The scraper uses **Playwright** (headless Chromium) to:

1. Load each court department's form listing page in a real browser
2. Scroll the page to trigger lazy-loaded content
3. Extract all form links using the `ma__download-link__file-link` CSS class
4. Sleep 60 seconds before downloading (to avoid rate limiting)
5. Download PDFs in batches of 10 with 15 second sleeps between batches
6. Use the browser's session and cookies for downloads so mass.gov doesn't block them

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

The 85 difference is due to deduplication — many forms are listed on multiple department pages but point to the same URL. Each unique URL is only downloaded once.

---

## How the Catalog Works

All form metadata is stored in `data/form_catalog.json`. Each entry tracks:

- `form_id` — unique identifier
- `form_name` — display name
- `source_url` — mass.gov URL
- `content_hash` — SHA-256 hash of the **actual PDF file bytes** (not the URL or filename)
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
- [ ] Add Docker + Docker Compose for production-ready deployment
- [ ] Add DVC for data versioning
- [ ] Migrate catalog storage to Cloud SQL on GCP
- [ ] Migrate PDF storage to Google Cloud Storage (GCS)
- [ ] Add Evidently AI drift monitoring