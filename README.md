# WFS Escalation + PO Flip Tool

**Built from your 3-hour BQ research session spec (2026-03-25)**

Consolidates **Seller Center + Scheduler 2.0 + Tableau TUP** into one FastAPI + HTMX + Tailwind web app.

---

## 🚀 Quick Start

### 1. Install dependencies

```bash
cd "/Users/t0t0ech/Documents/Code Puppy/wfs-escalation-tool"
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt --index-url https://pypi.ci.artifacts.walmart.com/artifactory/api/pypi/external-pypi/simple --allow-insecure-host pypi.ci.artifacts.walmart.com
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env if needed (defaults should work)
```

### 3. Authenticate with GCloud

Make sure you're logged in:

```bash
"/Users/t0t0ech/Documents/gCloud CLI/google-cloud-sdk/bin/gcloud" auth application-default login
```

### 4. Run the app

```bash
uvicorn main:app --reload --port 8765
```

Then open: **http://localhost:8765**

---

## 🎯 Modes

### Mode 1: Escalation Lookup
- **Input:** WFA PO number(s)
- **Output:** ESCALATE / BORDERLINE / DON'T + full reasoning
- **BQ Tables:** 5 tables, joins verified live on 2026-03-25
- **Logic:** Hero/Mosaic + WOS < 2.0 + arrived + not LTL

### Mode 2: PO Flip Request
- **Input:** WFA PO → auto-fills all fields from BQ
- **Output:** Writes to SharePoint PO Flip spreadsheet (via msgraph)
- **Placed Orders:** Auto-calculates current PO count at requested FC for that WM week

### Mode 3: Flip Status Monitor
- **Scheduled:** 9am, 1pm, 5pm EST
- **Action:** Reads SharePoint cols T (Approved) + U (Comment)
- **Notification:** Teams DM when status changes

---

## 🛠️ Architecture

```
main.py                      # FastAPI app + scheduler
config.py                    # Settings (gcloud path, BQ projects, etc.)
app/
  database.py                # SQLite flip tracker
  routes/
    escalation.py            # Mode 1
    po_flip.py               # Mode 2
    monitor.py               # Mode 3
  services/
    bigquery.py              # Master query + placed orders query
    escalation_logic.py      # ESCALATE / BORDERLINE / DON'T reasoning
    graph.py                 # SharePoint + Teams via MS Graph
  templates/                 # HTMX + Tailwind UI
```

---

## ⚙️ TODO

- [ ] Wire up MS Graph auth for SharePoint write + Teams DMs
- [ ] Test with more live PO numbers
- [ ] Deploy to VDI for 24/7 monitoring
- [ ] Add Walmart SSO if sharing with other AMs

---

## 🐶 Built by Dave (Code Puppy)

SQL query tested live with PO `6577303WFA` on 2026-03-25.
All 5 BQ table joins validated. PID used in 3/5 joins.

Happy escalating! 🚀
