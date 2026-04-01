# Multi-PO Flip Feature - Implementation Summary

## 🎯 What Was Built

Upgraded the PO Flip feature to support **pre-transit POs** with **multi-PO tabbed submission**.

---

## 🔧 Technical Changes

### 1. New BigQuery Query (`FLIP_QUERY` in `app/services/bigquery.py`)

**Purpose:** Query pre-transit POs from `Inbound_Sandbox` (before they're assigned to deliveries)

**Key Differences from Escalation Query:**

| | Escalation Query | Flip Query |
|---|---|---|
| **Primary Table** | `ETUP_DELIVERY_PO_LINES` | `Inbound_Sandbox` |
| **PO State** | Arrived/in-transit at DC | Pre-transit (awaiting pickup) |
| **Status Filter** | Any | `CREATED`, `CONFIRMED`, `SUBMITTED` |
| **Date Window** | Last 90 days | Last 90 days |
| **GMV Calc** | `ORDER_QTY * CURR_ITEM_PRICE` | `units * price_amt` (from `preproc_offer_detl`) |

**Returns:**
```python
{
  "flippable": [  # List of PO dicts with aggregated data
    {
      "po_num": "6840158WFA",
      "seller_id": "12345",
      "seller_name": "XYZ Corp",
      "am_name": "John Doe",
      "am_email": "john.doe@walmart.com",
      "current_fc": "SLC1n",
      "expected_delivery_date": "2026-04-03",
      "po_status": "CREATED",
      "total_units": 194,
      "total_gmv": 1234.56,
      "wm_week": 202614,
      "l3_category": "Electronics",
      "is_hero": False,
      "items": [...]  # Line-level item details
    }
  ],
  "non_flippable": ["6840159WFA"],  # POs that are DELIVERED/RECEIVED
  "items": [...]  # Raw item-level rows
}
```

---

### 2. Updated Route Handler (`app/routes/po_flip.py`)

#### **`POST /po-flip/lookup`**
- **Input:** `po_numbers_input` (comma or newline separated)
- **Output:** Tabbed multi-PO form with pre-filled data
- **Features:**
  - Parses multiple POs
  - Calls `query_flip_pos()`
  - Filters out non-flippable POs
  - Shows warning banner if any POs skipped

#### **`POST /po-flip/submit-multi`** (NEW)
- **Input:** JSON array of flip requests
- **Process:** Loops through each PO, writes to SharePoint
- **Returns:** Success/failure HTML with:
  - ✅ List of successful flips (with SharePoint row numbers)
  - ❌ List of failed flips (with error messages)
  - 🔄 **Retry button** for failed flips (no re-query needed)

---

### 3. Tabbed UI (`app/templates/partials/po_flip_form.html`)

**Layout:**
```
┌─────────────────────────────────────────────┐
│ Shared Field: Requestor Name (all POs)     │
├─────────────────────────────────────────────┤
│ ┌─────┬─────┬─────┐  ← Tabs (one per PO)  │
│ │ ⚪6840158│ ⚪6840159│ ✅6840160│          │
│ └─────┴─────┴─────┘                        │
├─────────────────────────────────────────────┤
│ PO Details (read-only):                    │
│  - Seller, AM, Units, GMV, Current FC, ETA │
│                                             │
│ Flip Request (editable):                   │
│  - Requested FC *, Reason *, AE Event *    │
│  - Event (optional)                        │
└─────────────────────────────────────────────┘
         [Submit All Flip Requests]
```

**Tab States:**
- ⚪ **Incomplete:** Missing required fields
- ✅ **Valid:** All required fields filled

**Validation:**
- Client-side real-time validation (marks tabs green/red)
- Pre-submit check: Prevents submission if any tab incomplete
- Shows error message with list of invalid POs

---

### 4. Retry Logic

**How it Works:**
1. Submit 3 POs → 1 succeeds, 2 fail
2. Response shows:
   - ✅ Success list with SharePoint row numbers
   - ❌ Failure list with error messages
   - **[Retry Failed Flips]** button
3. Clicking retry:
   - Sends ONLY the failed POs back to `/po-flip/submit-multi`
   - Uses cached data from original request (no BQ re-query)
   - Returns updated success/failure list

**Error Types:**
- `SHAREPOINT_ERROR`: SharePoint write failed
- `PROCESSING_ERROR`: Unexpected Python exception
- (Future: `NETWORK_ERROR`, `VALIDATION_ERROR`, etc.)

---

## 🧪 Testing Checklist

### Single PO
- [ ] Enter `6840158WFA` → Shows 1 tab
- [ ] Pre-filled data matches Seller Center (seller, AM, units, GMV, FC, ETA)
- [ ] Fill required fields → Tab turns green ✅
- [ ] Submit → Shows success with SharePoint row number

### Multiple POs
- [ ] Enter `6840158WFA, 6840159WFA, 6840160WFA` → Shows 3 tabs
- [ ] Tabs maintain user entry order
- [ ] Switching tabs preserves filled data
- [ ] Fill only 2/3 tabs → Submit prevented, shows error
- [ ] Fill all 3 tabs → Submit succeeds with 3 SharePoint rows

### Non-Flippable POs
- [ ] Enter a `DELIVERED` PO → Shows warning banner, filtered out
- [ ] Enter mix of flippable + non-flippable → Only flippable shown in tabs

### Retry
- [ ] Simulate SharePoint failure (disconnect VPN mid-submit)
- [ ] 1 success, 1 failure → Shows both lists + Retry button
- [ ] Click Retry → Only retries the failed one
- [ ] Both succeed → Retry button disappears

---

## 📋 SharePoint Columns Written

Same as before, but now writes **one row per PO** in multi-submit:

| Column | Source | Example |
|---|---|---|
| PO Number | `po_num` | 6840158WFA |
| PID | `seller_id` | 12345 |
| Seller Name | `seller_name` | XYZ Corp |
| AM Name | `am_name` | John Doe |
| AM Email | `am_email` | john.doe@walmart.com |
| Assigned FC | `current_fc` | SLC1n |
| Requested FC | User input | DFW2n |
| Total Units | `total_units` | 194 |
| PO GMV | `total_gmv` | 1234.56 |
| L3 Category | `l3_category` | Electronics |
| Delivery Date | `expected_delivery_date` | 2026-04-03 |
| WM Week | `wm_week` | 202614 |
| Hero Item | `is_hero` | Y/N |
| Reason | User input | Capacity issue at SLC1n |
| Event | User input | Summer Event |
| AE Event | User input | Y/N |
| Requestor | User input (shared) | Trey |

---

## 🚀 What's Next

### Enhancements to Consider:
1. **Auto-retry on rate limits:** Countdown timer + auto-retry after 60s
2. **Bulk validation:** Check if requested FC is valid before submit
3. **Draft save:** Store incomplete flips in localStorage
4. **Export to CSV:** Download flip requests for offline review
5. **Approval tracking:** Poll SharePoint for approval status updates

### Known Limitations:
1. **Pre-transit only:** Won't find POs that are already `DELIVERED`/`RECEIVED`
2. **No re-auth flow:** User must sign into VDI daily (acceptable for now)
3. **No duplicate check:** Can submit same PO multiple times (SharePoint allows it)

---

## 🐶 Code Ownership

**Escalation Mode (UNTOUCHED):**
- `queries/escalation.sql` → Uses `ETUP_DELIVERY_PO_LINES`
- `routes/escalation.py` → Single PO, ETUP query
- `templates/escalation*.html` → Escalation UI

**Flip Mode (NEW/UPDATED):**
- `FLIP_QUERY` in `services/bigquery.py` → Uses `Inbound_Sandbox`
- `routes/po_flip.py` → Multi-PO, flip query
- `templates/po_flip*.html` → Tabbed flip UI

**Shared:**
- `services/sharepoint_writer.py` → Used by both modes
- `database.py` → Local tracking for both modes

---

## 📞 Support

Issues? Questions?
- Slack: `#wfs-escalations`
- Owner: Trey (t0t0ech)
- Code Puppy: Dave 🐶