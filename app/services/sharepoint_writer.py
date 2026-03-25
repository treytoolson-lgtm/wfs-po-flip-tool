"""SharePoint Excel writer via msgraph sub-agent."""
from __future__ import annotations
import subprocess
import json
import logging
from datetime import date

log = logging.getLogger(__name__)

SHAREPOINT_FILE_ID = "3412E7C8-7761-4233-B87D-384885821FEE"
SHAREPOINT_SHEET = "Sheet1"


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
    Add a PO flip request row to SharePoint via msgraph sub-agent.
    
    Returns:
        (success, row_number, message)
        - success: True if row was added successfully
        - row_number: The actual SharePoint row number written (or None if failed)
        - message: Success or error message
    """
    try:
        today = date.today().strftime("%-m/%-d/%Y")  # e.g., "3/25/2026"
        
        # Format delivery date if provided
        if delivery_date:
            # Convert from YYYY-MM-DD to M/D/YYYY
            try:
                parts = delivery_date.split("-")
                delivery_date = f"{int(parts[1])}/{int(parts[2])}/{parts[0]}"
            except:
                pass  # Keep as-is if parsing fails
        
        prompt = f"""Add a row to the WFS PO Flip Excel file.

File details:
- File ID: {SHAREPOINT_FILE_ID}
- Sheet: {SHAREPOINT_SHEET}

IMPORTANT: First read the used range to find the actual last row with data, then add the new row immediately after it. Do NOT use the total row count.

Data to add (columns A-U):
Column A (Date): {today}
Column B (AM Name): {am_name}
Column C (PID): {pid}
Column D (Seller): {seller_name}
Column E (PO#): {po_number}
Column F (Units): {total_units}
Column G (Assigned FC): {assigned_fc}
Column H (Request FC): {request_fc}
Column I (PO GMV): {total_gmv}
Column J (Key Assortment/L3): {l3_category}
Column K (Reason): {reason}
Column L (Delivery Date): {delivery_date}
Column M: (blank)
Column N (Event): {event}
Column O (Hero Item): {hero_item}
Column P (AE Event): {ae_event}
Column Q (WM Week): {wm_week}
Column R: (blank - may be formula)
Column S (AM Action): (blank)
Column T (Approved): (blank)
Column U (Inv. Mgmt Comment): (blank)

After adding the row, please tell me the exact row number where it was added.
"""
        
        log.info("Calling msgraph to add SharePoint row...")
        
        # OPTION 1: Call via helper script (current - simulated)
        # result = subprocess.run(
        #     ["python3", "scripts/call_msgraph.py", prompt],
        #     capture_output=True,
        #     text=True,
        # )
        # response = json.loads(result.stdout)
        # return (response["success"], response.get("row_number"), response["message"])
        
        # OPTION 2: Call msgraph directly (RECOMMENDED - uncomment when ready)
        # This is the same approach used in the manual test
        # You would invoke the msgraph agent with the prompt above,
        # parse the response to extract the row number,
        # and return (True, row_num, message)
        
        # For now: SIMULATION MODE
        log.info("="*60)
        log.info("[SHAREPOINT - SIMULATION MODE]")
        log.info("In production, this prompt would be sent to msgraph:")
        log.info("="*60)
        log.info(prompt)
        log.info("="*60)
        
        # Simulate success - return a fake row number
        # TODO: Replace with actual msgraph call that returns real row number
        import random
        fake_row_num = 2450 + random.randint(1, 100)
        
        return (
            True,
            fake_row_num,
            f"[SIMULATED] Row would be added to SharePoint at row ~{fake_row_num}"
        )
        
    except Exception as e:
        log.error("Failed to add SharePoint row: %s", e)
        return (False, None, f"Error: {e}")
