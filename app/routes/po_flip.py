from __future__ import annotations
import logging
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.services.bigquery import query_flip_pos, query_placed_orders
from app.services.capacity_service import (
    get_all_fc_statuses,
    get_all_fc_uph,
    is_capacity_cached,
)
from app.services.escalation_logic import _safe_float
from app.services.sharepoint_writer import add_flip_request_to_sharepoint
from app import database as db

log = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _get_capacity_with_uph() -> list[dict]:
    """Return capacity rows with avg_ib_uph merged in.

    Both the SSR path (fc_capacity_page) and the HTMX data endpoint
    (fc_capacity_data) must go through here so the template always gets
    a row with avg_ib_uph — missing key = Jinja2 Undefined ≠ None crash.
    """
    capacity_data = get_all_fc_statuses()
    uph_lookup = {
        (r.get("fc_name_raw") or "").lower(): r
        for r in get_all_fc_uph()
    }
    for row in capacity_data:
        fc_lower = (row.get("fc_name") or "").lower()
        row["avg_ib_uph"] = (uph_lookup.get(fc_lower) or {}).get("avg_ib_uph")
    return capacity_data

# All 31 WFS FCs from the build spec
# FC names must match exactly what BigQuery returns in fc_name (case-insensitive
# matching is handled by the lookup below — but these names are what show in the UI).
WFS_FCS = [
    "ATL1s","ATL2n","ATL3n","ATL3s","BNA1s","CVG1n","DFW2n","DFW5s","DFW6s",
    "IND2T","IND2n","IND3s","KY1","LAX1s","LAX2n","LAX2T","MCI1n",
    "MCO1s","MEM1s","NJ3","NJ3T","ORD1s","PHL1s","PHL2n","PHL4n",
    "PHL5s","PHX1s","SLC1n","SMF1n","SMF1T",
]


@router.get("/po-flip", response_class=HTMLResponse)
async def po_flip_page(request: Request):
    return templates.TemplateResponse(
        "po_flip.html", {
            "request": request, 
            "wfs_fcs": WFS_FCS
        }
    )

@router.get("/fc-capacity", response_class=HTMLResponse)
async def fc_capacity_page(request: Request):
    if is_capacity_cached():
        return templates.TemplateResponse(
            "fc_capacity.html", {"request": request, "capacity_data": _get_capacity_with_uph()}
        )
    # Cache cold — render shell; HTMX loads data once cache warms
    return templates.TemplateResponse(
        "fc_capacity.html", {"request": request}
    )

@router.get("/fc-capacity/data", response_class=HTMLResponse)
async def fc_capacity_data(request: Request):
    return templates.TemplateResponse(
        "partials/fc_capacity_table.html",
        {"request": request, "capacity_data": _get_capacity_with_uph()},
    )


@router.post("/po-flip/lookup", response_class=HTMLResponse)
async def po_flip_lookup(
    request: Request,
    po_numbers_input: Annotated[str, Form()],
):
    """Pre-fill flip form fields by querying BQ for one or more POs (multi-PO support)."""
    # Parse input: comma or newline separated
    raw_pos = po_numbers_input.strip().upper()
    if not raw_pos:
        return HTMLResponse('<div class="text-red-600 p-4">Enter at least one PO number.</div>')
    
    # Split by comma or newline
    po_list = [po.strip() for po in raw_pos.replace('\n', ',').split(',') if po.strip()]
    
    if not po_list:
        return HTMLResponse('<div class="text-red-600 p-4">No valid PO numbers entered.</div>')
    
    log.info("Flip lookup for %d POs: %s", len(po_list), po_list)

    try:
        result = query_flip_pos(po_list)
    except Exception as e:
        log.error("BQ error: %s", e)
        return HTMLResponse(
            f'<div class="bg-red-50 border border-red-300 rounded p-4 text-red-700">'
            f'<strong>BQ Error:</strong> {e}</div>'
        )

    flippable = result["flippable"]
    non_flippable = result["non_flippable"]
    
    if not flippable:
        msg = f'<div class="bg-yellow-50 border border-yellow-300 rounded p-4 text-yellow-800">'
        if non_flippable:
            msg += f'<strong>No flippable POs found.</strong><br>'
            msg += f'The following POs cannot be flipped (already delivered or not in system): '
            msg += f'{", ".join(non_flippable)}'
        else:
            msg += f'No data found for: {", ".join(po_list)}'
        msg += '</div>'
        return HTMLResponse(msg)
    
    # Show warning if some POs were filtered out
    warning_html = ""
    if non_flippable:
        warning_html = f'''
        <div class="bg-amber-50 border border-amber-300 rounded p-3 mb-4 text-amber-800">
            <strong>⚠️ {len(non_flippable)} PO(s) skipped</strong> (already delivered or not found): 
            {', '.join(non_flippable)}
        </div>
        '''
    
    # Case-insensitive lookup: BQ returns mixed-case FC names (e.g. "IND2t")
    # but WFS_FCS uses canonical casing (e.g. "IND2T"). Lower both sides.
    capacity_data = get_all_fc_statuses()
    capacity_lookup = {item["fc_name"].lower(): item for item in capacity_data}
    return templates.TemplateResponse(
        "partials/po_flip_form.html", 
        {
            "request": request,
            "pos": flippable,
            "warning": warning_html,
            "wfs_fcs": WFS_FCS,
            "capacity_lookup": capacity_lookup,
        }
    )


@router.post("/po-flip/submit-multi", response_class=HTMLResponse)
async def po_flip_submit_multi(
    request: Request,
    flip_requests: Annotated[str, Form()],
):
    """Submit multiple flip requests at once."""
    import json
    
    try:
        requests_data = json.loads(flip_requests)
    except json.JSONDecodeError as e:
        log.error("JSON decode error: %s", e)
        return HTMLResponse(
            f'<div class="bg-red-50 border border-red-300 rounded p-4 text-red-700">'
            f'<strong>Error:</strong> Invalid request data</div>'
        )
    
    log.info("Processing %d flip requests", len(requests_data))
    
    successful = []
    failed = []
    
    for req in requests_data:
        try:
            po_num = req["po_num"]
            wm_week = req.get("wm_week") or 0
            request_fc = req["request_fc"]
            
            # Get placed orders at requested FC
            placed = query_placed_orders(request_fc, wm_week) if wm_week else {}
            
            # Write to SharePoint
            success, sp_row_num, sp_message = add_flip_request_to_sharepoint(
                po_number=po_num,
                pid=req["seller_id"],
                seller_name=req["seller_name"],
                am_name=req.get("am_name", ""),
                am_email=req.get("am_email", ""),
                assigned_fc=req["current_fc"],
                request_fc=request_fc,
                total_units=int(req["total_units"]),
                total_gmv=float(req["total_gmv"]),
                l3_category=req.get("l3_category", ""),
                delivery_date=req.get("delivery_date", ""),
                wm_week=wm_week,
                hero_item=req.get("hero_item", "N"),
                reason=req.get("reason", ""),
                event=req.get("event", ""),
                ae_event=req.get("ae_event", "N"),
            )
            
            if not success:
                log.warning("SharePoint write failed for %s: %s", po_num, sp_message)
                failed.append({
                    "po_num": po_num,
                    "error": sp_message,
                    "error_code": "SHAREPOINT_ERROR"
                })
            else:
                log.info("Flip submitted: %s -> %s (row %s)", po_num, request_fc, sp_row_num)
                
                # Log to local DB
                request_id = await db.add_flip_request(
                    po_number=po_num,
                    seller_name=req["seller_name"],
                    am_name=req.get("am_name", ""),
                    am_email=req.get("am_email", ""),
                    assigned_fc=req["current_fc"],
                    request_fc=request_fc,
                    sharepoint_row=sp_row_num,
                )
                
                successful.append({
                    "po_num": po_num,
                    "row": sp_row_num,
                    "request_id": request_id,
                    "placed_info": f"{placed.get('placed_po_count', 0)} POs / {placed.get('total_cases', 0)} cases" if placed else "N/A"
                })
        
        except Exception as e:
            log.error("Error processing flip for %s: %s", req.get("po_num", "unknown"), e)
            failed.append({
                "po_num": req.get("po_num", "unknown"),
                "error": str(e),
                "error_code": "PROCESSING_ERROR"
            })
    
    # Build result HTML
    result_html = ""
    
    if successful:
        result_html += f'''
        <div class="bg-green-50 border border-green-200 rounded-lg p-6 mb-4">
            <div class="text-4xl mb-3">✅</div>
            <h3 class="text-lg font-bold text-green-800">Successfully Flipped {len(successful)} PO(s)!</h3>
            <ul class="mt-3 space-y-2">
        '''
        for s in successful:
            result_html += f'''
                <li class="text-green-700">
                    <strong>{s["po_num"]}</strong> → SharePoint row <strong>{s["row"]}</strong> 
                    <span class="text-sm text-green-600">(Tracking #{s["request_id"]})</span>
                    <br>
                    <span class="text-sm">Current orders: {s["placed_info"]}</span>
                </li>
            '''
        result_html += '</ul></div>'
    
    if failed:
        result_html += f'''
        <div class="bg-red-50 border border-red-300 rounded-lg p-6">
            <div class="text-4xl mb-3">❌</div>
            <h3 class="text-lg font-bold text-red-800">Failed to Flip {len(failed)} PO(s):</h3>
            <ul class="mt-3 space-y-2">
        '''
        for f in failed:
            result_html += f'''
                <li class="text-red-700">
                    <strong>{f["po_num"]}</strong>: {f["error"]}
                    <br>
                    <span class="text-sm text-red-600">Error code: {f["error_code"]}</span>
                </li>
            '''
        result_html += '</ul>'
        
        # Add retry button if there are failures
        retry_data = json.dumps([req for req in requests_data if any(f["po_num"] == req["po_num"] for f in failed)])
        result_html += f'''
            <button
                onclick='retryFailedFlips({retry_data})'
                class="mt-4 bg-red-600 hover:bg-red-700 text-white font-bold py-2 px-6 rounded transition"
            >
                Retry Failed Flips
            </button>
        </div>
        <script>
        function retryFailedFlips(failedRequests) {{
            const submitBtn = event.target;
            submitBtn.disabled = true;
            submitBtn.innerHTML = '<svg class="animate-spin h-5 w-5 inline mr-2" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" fill="none"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg>Retrying...';
            
            htmx.ajax('POST', '/po-flip/submit-multi', {{
                target: '#submit-result',
                swap: 'innerHTML',
                values: {{flip_requests: JSON.stringify(failedRequests)}}
            }}).then(() => {{
                submitBtn.disabled = false;
                submitBtn.innerHTML = 'Retry Failed Flips';
            }});
        }}
        </script>
        '''
    
    if successful and not failed:
        result_html += '''
        <div class="mt-4">
            <button onclick="window.location.reload()" class="text-green-700 underline">Submit New Flip Request</button>
        </div>
        '''
    
    return HTMLResponse(result_html)


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

        if sp_row_num:
            sp_row_badge = f'<span class="text-green-500">✅ SharePoint row: {sp_row_num}</span>'
        else:
            sp_row_badge = (
                f'<span class="text-amber-600">'
                f'⚠️ SharePoint write failed: {sp_message}'
                f'</span>'
            )

        return HTMLResponse(f"""
            <div class="bg-green-50 border border-green-200 rounded-lg p-6">
                <div class="text-4xl mb-3">&#x2705;</div>
                <h3 class="text-lg font-bold text-green-800">Flip Request Submitted!</h3>
                <p class="text-green-700 mt-1">PO <strong>{po_number}</strong> &rarr; <strong>{request_fc}</strong></p>
                <p class="text-sm text-green-600 mt-1">Current orders at {request_fc} (WK{wm_week}): {placed_info}</p>
                <p class="text-xs mt-2">Tracking ID: #{request_id} | {sp_row_badge}</p>
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
