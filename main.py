import contextlib
import logging
import os
import threading

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import app.database as db
from app.routes import escalation, monitor, po_flip
from app.services.capacity_service import refresh_capacity_cache, refresh_uph_cache
from app.services.teams_notifier import post_teams_notification
from config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

settings = get_settings()
settings.configure_gcloud_path()

templates = Jinja2Templates(directory="app/templates")
scheduler = AsyncIOScheduler(timezone=pytz.timezone("US/Eastern"))


async def monitor_poll_job():
    """Scheduled job: check SharePoint for flip approvals, send Teams DMs."""
    log.info("[MONITOR] Scheduled poll starting...")
    try:
        pending = await db.get_pending_requests()
        if not pending:
            log.info("[MONITOR] No pending requests.")
            return

        # TODO: Wire up SharePoint read + Teams DM once msgraph auth is configured.
        # Shape:
        #   1. Read SharePoint rows via GraphClient / SharePointService
        #   2. For each pending request, check col T (Approved) and col U (Comment)
        #   3. If changed — update DB + send Teams DM via TeamsService
        log.info("[MONITOR] %d pending request(s) checked.", len(pending))
    except Exception as e:
        log.error("[MONITOR] Poll failed: %s", e)


def _sync_refresh_capacity():
    """Thin sync wrapper so APScheduler can call the synchronous refresh."""
    refresh_capacity_cache()


def _sync_refresh_uph():
    """Thin sync wrapper so APScheduler can call the synchronous UPH refresh."""
    refresh_uph_cache()


@contextlib.asynccontextmanager
async def lifespan(application: FastAPI):
    await db.init_db()

    # —— Monitor poll — 9am, 1pm, 5pm EST ——————————————————————————
    for hour in settings.monitor_hours:
        scheduler.add_job(
            monitor_poll_job,
            CronTrigger(hour=hour, minute=0),
            id=f"monitor_poll_{hour}",
            replace_existing=True,
        )

    # —— Proactive FC capacity refresh — every 10 min —————————————————
    # Keeps cache warm before anyone needs it. Prevents cold-start latency and
    # eliminates the concurrent BQ double-fire on the first load of the day.
    scheduler.add_job(
        _sync_refresh_capacity,
        "interval",
        minutes=10,
        id="fc_capacity_refresh",
        replace_existing=True,
    )

    # —— Proactive IB UPH refresh — every 6 hours ——————————————————
    # UPH is weekly data — 4 queries/day instead of 96. Zero meaningful info lost.
    scheduler.add_job(
        _sync_refresh_uph,
        "interval",
        hours=6,
        id="uph_refresh",
        replace_existing=True,
    )

    scheduler.start()
    log.info("Scheduler started. Monitor hours (EST): %s", settings.monitor_hours)

    # Warm both caches in background — app responds immediately,
    # cache fills in ~20s without blocking any requests.
    def _background_warm() -> None:
        try:
            log.info("[STARTUP] Warming FC capacity cache...")
            _sync_refresh_capacity()
            log.info("[STARTUP] FC capacity cache warmed successfully.")
        except Exception as e:
            log.error("[STARTUP] FC capacity warm FAILED: %s", e, exc_info=True)
        try:
            log.info("[STARTUP] Warming UPH cache...")
            _sync_refresh_uph()
            log.info("[STARTUP] UPH cache warmed successfully.")
        except Exception as e:
            log.error("[STARTUP] UPH warm FAILED: %s", e, exc_info=True)

    threading.Thread(target=_background_warm, daemon=True, name="cache-warm").start()
    log.info("Cache warming started in background — app ready immediately.")

    yield
    scheduler.shutdown()


app = FastAPI(
    title="WFS Escalation + PO Flip Tool",
    lifespan=lifespan,
)

app.mount("/static", StaticFiles(directory="static"), name="static")

# Routers
app.include_router(escalation.router)
app.include_router(po_flip.router)
app.include_router(monitor.router)


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
