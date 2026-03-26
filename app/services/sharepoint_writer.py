"""SharePoint Excel writer — calls scripts/sharepoint_write.py via code-puppy Python."""
from __future__ import annotations
import json
import logging
import subprocess
from datetime import date
from pathlib import Path

log = logging.getLogger(__name__)

# Absolute paths so the app can be run from any cwd
CODE_PUPPY_PYTHON = Path.home() / ".code-puppy-venv/bin/python3"
SCRIPT_PATH       = Path(__file__).resolve().parents[2] / "scripts" / "sharepoint_write.py"


def add_flip_request_to_sharepoint(
    po_number: str,
    pid: str,
    seller_name: str,
    am_name: str,
    am_email: str,
    assigned_fc: str,
    request_fc: str,
    total_units: int,
    total_gmv: float,
    l3_category: str = "",
    delivery_date: str = "",
    wm_week: int = 0,
    hero_item: str = "N",
    reason: str = "",
    event: str = "",
    ae_event: str = "N",
) -> tuple[bool, int | None, str]:
    """
    Add a PO flip request row to SharePoint via code-puppy's MSGraphClient.

    Returns:
        (success, row_number, message)
    """
    try:
        today = date.today().strftime("%-m/%-d/%Y")

        # Format delivery date from YYYY-MM-DD to M/D/YYYY
        if delivery_date:
            try:
                y, m, d = delivery_date.split("-")
                delivery_date = f"{int(m)}/{int(d)}/{y}"
            except Exception:
                pass  # keep as-is if parsing fails

        payload = json.dumps({
            "date":          today,
            "am_name":       am_name,
            "pid":           str(pid),
            "seller_name":   seller_name,
            "po_number":     po_number,
            "total_units":   total_units,
            "assigned_fc":   assigned_fc,
            "request_fc":    request_fc,
            "total_gmv":     total_gmv,
            "l3_category":   l3_category,
            "reason":        reason,
            "delivery_date": delivery_date,
            "event":         event,
            "hero_item":     hero_item,
            "ae_event":      ae_event,
            "wm_week":       wm_week,
        })

        log.info("Calling SharePoint write script for PO %s...", po_number)

        result = subprocess.run(
            [str(CODE_PUPPY_PYTHON), str(SCRIPT_PATH), payload],
            capture_output=True,
            text=True,
            timeout=30,
        )

        response = json.loads(result.stdout.strip())

        if response.get("success"):
            row_num = response["row_number"]
            log.info("SharePoint row %d written for PO %s", row_num, po_number)
            return (True, row_num, f"Row {row_num} written to SharePoint")
        else:
            error = response.get("error", "Unknown error")
            log.warning("SharePoint write failed: %s", error)
            return (False, None, error)

    except subprocess.TimeoutExpired:
        log.error("SharePoint write timed out for PO %s", po_number)
        return (False, None, "SharePoint write timed out (30s)")
    except Exception as e:
        log.error("SharePoint write error: %s", e)
        return (False, None, f"Error: {e}")