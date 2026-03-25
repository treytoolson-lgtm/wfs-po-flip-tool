from __future__ import annotations
import logging
from typing import Annotated

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.services.bigquery import query_po_numbers
from app.services.escalation_logic import analyze_escalation

log = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


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
        analysis = analyze_escalation(po_rows)
        results.append({"po_num": po_num, "rows": po_rows, "analysis": analysis})

    return templates.TemplateResponse(
        "partials/escalation_results.html",
        {"request": request, "results": results},
    )
