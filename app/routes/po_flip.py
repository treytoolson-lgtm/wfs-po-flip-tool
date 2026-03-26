from __future__ import annotations
import logging
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.services.bigquery import query_po_numbers, query_placed_orders
from app.services.escalation_logic import _safe_float
from app.services.sharepoint_writer import add_flip_request_to_sharepoint
from app import database as db

log = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

# All 31 WFS FCs from the build spec
WFS_FCS = [
    "ATL1s","ATL2n","ATL3","BNA1n","CVG1n","DFW2n","DFW5s","DFW6s",
    "IND2T","IND2n","IND3s","KY1","LAX1s","LAX2n","LAX2T","MCI1n",
    "MCO1s","MEM1s","NJ3","NJ3T","ORD1s","PHL1s","PHL2n","PHL4n",
    "PHL5s","PHX1s","SLC1n","SMF1n","SMF1T",
]


@router.get("/po-flip", response_class=HTMLResponse)
async def po_flip_page(request: Request):
    return templates.TemplateResponse(
        "po_flip.html", {"request": request, "wfs_fcs": WFS_FCS}
    )


@router.post("/po-flip/lookup", response_class=HTMLResponse)
async def po_flip_lookup(
    request: Request,
    po_number: Annotated[str, Form()],
):
    """Pre-fill flip form fields by querying BQ for a single PO."""
    po_number = po_number.strip().upper()
    if not po_number:
        return HTMLResponse('<div class="text-red-600 p-4">Enter a PO number first.</div>')

    try:
        rows = query_po_numbers([po_number])
    except Exception as e:
        log.error("BQ error: %s", e)
        return HTMLResponse(
            f'<div class="bg-red-50 border border-red-300 rounded p-4 text-red-700">'
            f'<strong>BQ Error:</strong> {e}</div>'
        )

    if not rows:
        return HTMLResponse(
            f'<div class="bg-yellow-50 border border-yellow-300 rounded p-4 text-yellow-800">'
            f'No data found for {po_number}.</div>'
        )

    sample = rows[0]
    total_units = sum(float(r.get("TOTAL_UNITS") or 0) for r in rows)
    total_gmv = sum(_safe_float(r.get("GMV_IMPACT")) for r in rows)
    l3 = next((r.get("L3_CATEGORY") for r in rows if r.get("L3_CATEGORY")), "")
    wm_week = sample.get("WM_YR_WK_NBR") or ""
    is_hero = any(int(r.get("IS_HERO_ITEM") or 0) == 1 for r in rows)

    prefill = {
        "po_number": po_number,
        "pid": sample.get("PID", ""),
        "seller_name": sample.get("SELLER_NAME", ""),
        "am_name": sample.get("AM_NAME", ""),
        "am_email": sample.get("AM_EMAIL", ""),
        "assigned_fc": sample.get("FC_NAME", ""),
        "total_units": int(total_units),
        "total_gmv": round(total_gmv, 2),
        "l3_category": l3,
        "delivery_date": str(sample.get("APPOINTMENT_DATE", ""))[:10],
        "wm_week": wm_week,
        "hero_item": "Y" if is_hero else "N",
        "wfs_fcs": WFS_FCS,
    }

    return templates.TemplateResponse(
        "partials/po_flip_form.html", {"request": request, **prefill}
    )


@router.post("/po-flip/submit", response_class=HTMLResponse)
async def po_flip_submit(
    request: Request,
    po_number: Annotated[str, Form()],
    pid: Annotated[str, Form()],
    seller_name: Annotated[str, Form()],
    am_name: Annotated[str, Form()],
    am_email: Annotated[str, Form()],
    assigned_fc: Annotated[str, Form()],
    request_fc: Annotated[str, Form()],
    total_units: Annotated[int, Form()],
    total_gmv: Annotated[float, Form()],
    l3_category: Annotated[str, Form()] = "",
    delivery_date: Annotated[str, Form()] = "",
    wm_week: Annotated[int, Form()] = 0,
    hero_item: Annotated[str, Form()] = "N",
    reason: Annotated[str, Form()] = "",
    event: Annotated[str, Form()] = "",
    ae_event: Annotated[str, Form()] = "N",
):
    try:
        # Get placed orders at requested FC for that WM week
        placed = query_placed_orders(request_fc, wm_week) if wm_week else {}

        # Write to SharePoint via msgraph
        success, sp_row_num, sp_message = add_flip_request_to_sharepoint(
            po_number=po_number,
            pid=pid,
            seller_name=seller_name,
            am_name=am_name,
            am_email=am_email,
            assigned_fc=assigned_fc,
            request_fc=request_fc,
            total_units=total_units,
            total_gmv=total_gmv,
            l3_category=l3_category,
            delivery_date=delivery_date,
            wm_week=wm_week,
            hero_item=hero_item,
            reason=reason,
            event=event,
            ae_event=ae_event,
        )
        
        if not success:
            log.warning("SharePoint write failed: %s", sp_message)
            sp_row_num = None
        else:
            log.info("PO Flip submitted: %s -> %s (SharePoint row %s)", po_number, request_fc, sp_row_num)

        request_id = await db.add_flip_request(
            po_number=po_number, seller_name=seller_name, am_name=am_name,
            am_email=am_email, assigned_fc=assigned_fc, request_fc=request_fc,
            sharepoint_row=sp_row_num,
        )

        placed_info = (
            f"{placed.get('placed_po_count', 0)} POs / {placed.get('total_cases', 0)} cases"
            if placed else "Not available"
        )

        sp_row_info = f"SharePoint row: {sp_row_num}" if sp_row_num else "(SharePoint write simulated)"
        
        return HTMLResponse(f"""
            <div class="bg-green-50 border border-green-200 rounded-lg p-6">
                <div class="text-4xl mb-3">&#x2705;</div>
                <h3 class="text-lg font-bold text-green-800">Flip Request Submitted!</h3>
                <p class="text-green-700 mt-1">PO <strong>{po_number}</strong> → <strong>{request_fc}</strong></p>
                <p class="text-sm text-green-600 mt-1">Current orders at {request_fc} (WK{wm_week}): {placed_info}</p>
                <p class="text-xs text-green-500 mt-2">Tracking ID: #{request_id} | {sp_row_info}</p>
                <p class="text-xs text-gray-500 mt-1">You\'ll be notified when approved/denied.</p>
                <button onclick="window.location.reload()" class="mt-4 text-sm text-green-700 underline">New Request</button>
            </div>
        """)

    except Exception as e:
        log.error("PO flip submit error: %s", e)
        return HTMLResponse(
            f'<div class="bg-red-50 border border-red-300 rounded p-4 text-red-700">'
            f'<strong>Error:</strong> {e}</div>'
        )
