# WFS Escalation + PO Flip Tool

Consolidates **Seller Center + Scheduler 2.0 + Tableau TUP** into one FastAPI + HTMX web app.
Access from any browser — hosted on a VDI, used by the whole team.

---

## ✅ What's Working

| Feature | Status |
|---------|--------|
| Escalation Lookup (BQ) | ✅ Fully functional |
| PO Flip pre-fill from BQ | ✅ All fields auto-populated |
| L3 category (all items) | ✅ Via preproc_offer_detl |
| SharePoint Excel write | ✅ Real writes — finds next available row |
| VLOOKUP formulas (Q & R) | ✅ WM Week + Placed Orders auto-calculated |
| Auto token refresh | ✅ Silent refresh, browser re-auth if needed |
| SQLite flip tracker | ✅ Local history of all requests |
| Flip Status Monitor | ✅ Dashboard with scheduled polling |
| Teams notifications | ✅ Configured (9am/1pm/5pm EST) |

---

## 🚀 Setup (First Time on a New Machine)

### 1. Prerequisites
- **Code Puppy** installed (get it at [puppy.walmart.com](https://puppy.walmart.com))
- **gcloud CLI** installed and authenticated
- **Python 3.11+**

### 2. Clone and install

```bash
git clone <repo-url>
cd wfs-escalation-tool
uv venv
source .venv/bin/activate        # Mac/Linux
.venv\Scripts\activate           # Windows
uv pip install -r requirements.txt --index-url https://pypi.ci.artifacts.walmart.com/artifactory/api/pypi/external-pypi/simple --allow-insecure-host pypi.ci.artifacts.walmart.com
```

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env — most defaults are fine, add Teams IDs if you have them
```

### 4. Authenticate

```bash
# BigQuery
gcloud auth application-default login

# Microsoft Graph (SharePoint writes)
# In Code Puppy, run:
/msgraph_auth
```

### 5. Run

```bash
./run.sh                              # Mac/Linux
uvicorn main:app --port 8765         # Windows
```

Then open: **http://localhost:8765**

Team members access via: **http://<vdi-ip>:8765**

---

## 🔐 Auth Notes

- **BigQuery:** Uses your personal gcloud ADC credentials
- **SharePoint:** Uses Microsoft Graph tokens stored by Code Puppy (`~/.code_puppy/msgraph.json`)
  - Tokens auto-refresh silently (hourly)
  - If fully expired (~90 days), browser re-auth launches automatically
  - All SharePoint writes appear as whoever ran `/msgraph_auth` on the host machine
- **Team members** access via browser — no auth needed on their end

---

## 📊 Teams Channel

- **Team ID:** `eec3e859-6399-46a1-9d17-78b9421be03c`
- **Channel ID:** `19:433a8ab43f8744ed9b898e4f356ff76f@thread.tacv2`

[Open in Teams](https://teams.cloud.microsoft/l/channel/19%3A433a8ab43f8744ed9b898e4f356ff76f%40thread.tacv2/alerts?groupId=eec3e859-6399-46a1-9d17-78b9421be03c&tenantId=3cbcc3d3-094d-4006-9849-0d11d61f484d&allowXTenantAccess=False)

---

## 🐶 Built with Code Puppy