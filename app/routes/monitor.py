from __future__ import annotations
import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app import database as db

log = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/monitor", response_class=HTMLResponse)
async def monitor_page(request: Request):
    """Mode 3: Flip status dashboard."""
    all_requests = await db.get_all_requests(limit=100)
    pending = [r for r in all_requests if r["status"] == "PENDING"]
    approved = [r for r in all_requests if r["status"] == "APPROVED"]
    denied = [r for r in all_requests if r["status"] == "DENIED"]

    return templates.TemplateResponse(
        "monitor.html",
        {
            "request": request,
            "all_requests": all_requests,
            "pending": pending,
            "approved": approved,
            "denied": denied,
        },
    )


@router.get("/monitor/refresh", response_class=HTMLResponse)
async def monitor_refresh(request: Request):
    """HTMX endpoint to refresh just the status table."""
    all_requests = await db.get_all_requests(limit=100)
    return templates.TemplateResponse(
        "partials/monitor_table.html",
        {"request": request, "all_requests": all_requests},
    )
