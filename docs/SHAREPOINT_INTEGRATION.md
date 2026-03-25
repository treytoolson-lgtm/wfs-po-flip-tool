# SharePoint Integration via msgraph

## Current Status: SIMULATION MODE ✅

The WFS app is **fully functional in simulation mode**. When you submit a PO flip request:

1. ✅ All data is validated and processed
2. ✅ BigQuery is queried for PO details
3. ✅ Request is saved to local SQLite database
4. ✅ SharePoint write is **logged** (but not actually executed)
5. ✅ Success message shows simulated SharePoint row number

---

## How to Enable Real SharePoint Writes

The msgraph integration is already tested and working (we manually added row 2453). To make it automatic:

### Option 1: Python subprocess to msgraph (Recommended)

Update `app/services/sharepoint_writer.py`:

```python
# Replace the simulation block with:
import subprocess

# Call msgraph via Python subprocess
# This assumes you can invoke agents programmatically
result = call_msgraph_agent(
    agent_name="msgraph",
    prompt=prompt,  # The full SharePoint prompt
    session_id="wfs-sharepoint-writer"
)

# Parse response to extract row number
if "Row added" in result.response:
    # Extract row number from response text
    import re
    match = re.search(r'row (\d+)', result.response)
    row_num = int(match.group(1)) if match else None
    return (True, row_num, result.response)
else:
    return (False, None, result.error or "Unknown error")
```

### Option 2: Direct Graph API (More Complex)

If you want to bypass msgraph and call SharePoint directly:

1. Get an access token (via msgraph or Azure AD)
2. Use the Microsoft Graph SDK to write to Excel
3. Update `app/services/graph.py` with the SharePoint write logic

---

## Testing the Integration

### Manual Test (What We Did)

```bash
# In Code Puppy
/agent msgraph

# Then:
Add a row to the WFS PO Flip Excel file (ID: 3412E7C8-7761-4233-B87D-384885821FEE).
IMPORTANT: Read the used range first to find the actual last row.
[data here...]
```

✅ **Result:** Row successfully added to SharePoint at correct position

### Automated Test (What The App Does)

1. Submit a flip request via the web UI
2. App calls `add_flip_request_to_sharepoint()`
3. Function logs the msgraph prompt (simulation mode)
4. Check logs to verify the prompt is correct
5. When ready: uncomment real msgraph call

---

## Files Involved

| File | Purpose |
|------|--------|
| `app/services/sharepoint_writer.py` | Main SharePoint write logic |
| `app/routes/po_flip.py` | Calls SharePoint writer on form submit |
| `scripts/call_msgraph.py` | Helper script (optional) |
| `app/services/graph.py` | Graph API client (alternative approach) |

---

## Next Steps

1. ✅ **Test in simulation mode** (current state)
2. ⏳ **Decide**: Python subprocess to msgraph OR direct Graph API
3. ⏳ **Implement**: Uncomment/add real msgraph call
4. ⏳ **Test**: Submit real flip request, verify SharePoint row
5. ⏳ **Deploy**: Run on VDI 24/7

---

## Why Simulation Mode?

We're using simulation mode because:
- ✅ All business logic is tested and working
- ✅ BQ queries are live and validated
- ✅ UI/UX is complete
- ⚠️ msgraph integration needs proper subprocess/API setup

The simulation logs show **exactly** what would be sent to msgraph, so you can verify the integration before enabling it.
