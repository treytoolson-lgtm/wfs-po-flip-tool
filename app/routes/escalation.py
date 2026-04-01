from __future__ import annotations
import logging
from typing import Annotated

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.services.bigquery import query_po_numbers
from app.services.capacity_service import get_fc_status
from app.services.escalation_logic import analyze_escalation
from app.services.trailer_service import get_trailer_context

log = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/escalation/trailer-context/{trailer_id}", response_class=HTMLResponse)
async def escalation_trailer_context(request: Request, trailer_id: str, po_num: str = ""):
    """HTMX lazy-load endpoint: trailer co-load GMV context panel."""
    try:
        ctx = get_trailer_context(trailer_id, current_po_num=po_num)
    except Exception as e:
        log.warning("Trailer context endpoint error: %s", e)
        ctx = None
    return templates.TemplateResponse(
        "partials/trailer_context.html",
        {"request": request, "ctx": ctx, "trailer_id": trailer_id},
    )


@router.get("/escalation", response_class=HTMLResponse)
async def escalation_page(request: Request):
    return templates.TemplateResponse("escalation.html", {"request": request})


@router.post("/escalation/lookup", response_class=HTMLResponse)
async def escalation_lookup(
    request: Request,
    po_input: Annotated[str, Form()],
):
    """Accept one or more WFA PO numbers (comma or newline separated)."""
    raw = po_input.replace("\n", ",").replace(" ", "")
    po_numbers = [p.strip().upper() for p in raw.split(",") if p.strip()]

    if not po_numbers:
        return HTMLResponse(
            '<div class="text-red-600 font-semibold p-4">Please enter at least one PO number.</div>'
        )

    try:
        rows = query_po_numbers(po_numbers)
    except Exception as e:
        log.error("BQ error: %s", e)
        return HTMLResponse(
            f'<div class="bg-red-50 border border-red-300 rounded p-4 text-red-700">'
            f'<strong>BQ Error:</strong> {e}</div>'
        )

    if not rows:
        return HTMLResponse(
            '<div class="bg-yellow-50 border border-yellow-300 rounded p-4 text-yellow-800">'
            f'No data found for PO(s): {", ".join(po_numbers)}. '
            f'Check the PO number or try a wider date range.</div>'
        )

    # Group rows by PO number
    by_po: dict[str, list[dict]] = {}
    for row in rows:
        by_po.setdefault(row["PO_NUM"], []).append(row)

    results = []
    for po_num, po_rows in by_po.items():
        # Pull FC status from cache (never blocks the submission — returns None if cold)
        fc_name = po_rows[0].get("FC_NAME", "") if po_rows else ""
        fc_status = get_fc_status(fc_name)

        analysis = analyze_escalation(po_rows, fc_status=fc_status)
        results.append({
            "po_num":    po_num,
            "rows":      po_rows,
            "analysis":  analysis,
            "fc_status": fc_status,
        })

    return templates.TemplateResponse(
        "partials/escalation_results.html",
        {"request": request, "results": results},
    )
