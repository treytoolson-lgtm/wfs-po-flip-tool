# WFS Escalation + PO Flip Tool - Updated README

**Built from your 3-hour BQ research session spec (2026-03-25)**

Consolidates **Seller Center + Scheduler 2.0 + Tableau TUP** into one FastAPI + HTMX + Tailwind web app.

---

## ✅ What Works RIGHT NOW

### Mode 1: Escalation Lookup ✅ FULLY FUNCTIONAL
- Enter WFA PO numbers
- Queries BigQuery with validated SQL (tested 2026-03-25)
- Returns ESCALATE / BORDERLINE / DON'T with full reasoning
- Shows Hero/Mosaic items, WOS, trailer status

### Mode 2: PO Flip Request ✅ MOSTLY FUNCTIONAL
- ✅ Auto-fills all fields from BigQuery
- ✅ Calculates placed orders at requested FC
- ✅ Saves to local SQLite database
- ⚠️ SharePoint write: Needs Azure AD app auth (see below)

### Mode 3: Flip Status Monitor ✅ DASHBOARD WORKS
- ✅ Dashboard shows all flip requests with status badges
- ✅ Scheduled polling configured (9am/1pm/5pm EST)
- ✅ Teams channel created: "WFS PO Flip Tool" #alerts
- ✅ Teams notification logic complete (logs for now)
- 💡 See `docs/SHAREPOINT_INTEGRATION.md` for enabling real SharePoint reads

---

## 🚀 Quick Start

### 1. Install dependencies

```bash
cd "/Users/t0t0ech/Documents/Code Puppy/wfs-escalation-tool"
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt --index-url https://pypi.ci.artifacts.walmart.com/artifactory/api/pypi/external-pypi/simple --allow-insecure-host pypi.ci.artifacts.walmart.com
```

### 2. Authenticate with GCloud

```bash
"/Users/t0t0ech/Documents/gCloud CLI/google-cloud-sdk/bin/gcloud" auth application-default login
```

### 3. Run the app

```bash
./run.sh
# Or: uvicorn main:app --reload --port 8765
```

Then open: **http://localhost:8765**

---

## 🔧 What Needs Azure AD Auth (Optional)

To enable **SharePoint writes** and **Teams notifications**, you need to register an Azure AD application:

### Steps:
1. Go to Azure Portal → App Registrations
2. Create new app: "WFS PO Flip Tool"
3. Get `client_id` + `client_secret`
4. Add API permissions:
   - `Sites.ReadWrite.All` (SharePoint)
   - `ChannelMessage.Send` (Teams)
5. Update code to use OAuth Device Flow

**For now:** The app logs what it WOULD post to Teams/SharePoint. You can manually verify via msgraph:

```bash
# In Code Puppy
/agent msgraph
# Then post manually to test
```

---

## 📊 Teams Channel

Already created and ready:
- **Team:** WFS PO Flip Tool (Private)
- **Channel:** #alerts
- **Team ID:** `eec3e859-6399-46a1-9d17-78b9421be03c`
- **Channel ID:** `19:433a8ab43f8744ed9b898e4f356ff76f@thread.tacv2`

[Open in Teams](https://teams.cloud.microsoft/l/channel/19%3A433a8ab43f8744ed9b898e4f356ff76f%40thread.tacv2/alerts?groupId=eec3e859-6399-46a1-9d17-78b9421be03c&tenantId=3cbcc3d3-094d-4006-9849-0d11d61f484d&allowXTenantAccess=False)

---

## 🎯 Current Capabilities

| Feature | Status | Notes |
|---------|--------|-------|
| BQ Escalation Query | ✅ Works | Validated with PO 6577303WFA |
| BQ PO Flip Pre-fill | ✅ Works | All fields auto-populated |
| SQLite Flip Tracker | ✅ Works | Stores all requests locally |
| Dashboard UI | ✅ Works | Shows pending/approved/denied |
| Scheduler (3x/day) | ✅ Works | Runs at 9am/1pm/5pm EST |
| SharePoint Write | ⚠️ Logs Only | Needs Azure AD app |
| Teams Notifications | ⚠️ Logs Only | Needs Azure AD app |

---

## 🐶 Built by Dave (Code Puppy)

SQL query tested live with PO `6577303WFA` on 2026-03-25.
All 5 BQ table joins validated. Teams channel created and tested.

Happy escalating! 🚀
