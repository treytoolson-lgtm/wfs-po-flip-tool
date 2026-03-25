import contextlib
import logging
import os

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import app.database as db
from app.routes import escalation, monitor, po_flip
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


@contextlib.asynccontextmanager
async def lifespan(application: FastAPI):
    await db.init_db()

    # Schedule monitor job at 9am, 1pm, 5pm EST
    for hour in settings.monitor_hours:
        scheduler.add_job(
            monitor_poll_job,
            CronTrigger(hour=hour, minute=0),
            id=f"monitor_poll_{hour}",
            replace_existing=True,
        )

    scheduler.start()
    log.info("Scheduler started. Monitor hours (EST): %s", settings.monitor_hours)
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
