#!/Users/t0t0ech/.code-puppy-venv/bin/python3
"""Write a row to the WFS PO Flip SharePoint Excel file.

Called as a subprocess from the FastAPI app.
Uses code-puppy's MSGraphClient + cached auth tokens.

Usage:
    python3 scripts/sharepoint_write.py '{"po_number": "...", ...}'

Returns JSON to stdout:
    {"success": true, "row_number": 2453}
    {"success": false, "error": "..."}
"""
from __future__ import annotations
import sys
import json
import re

SITE_ID   = "teams.wal-mart.com,e3a0eb98-6815-4f78-903b-622909921022,1227cc7c-7338-43ee-ab55-17682c982812"
FILE_ID   = "3412E7C8-7761-4233-B87D-384885821FEE"
SHEET     = "Sheet1"
BASE_PATH = f"/sites/{SITE_ID}/drive/items/{FILE_ID}/workbook/worksheets/{SHEET}"


def get_valid_token_or_reauth() -> str:
    """Get a valid Graph token, attempting silent refresh first.
    If refresh fails, launch the browser auth flow automatically.
    Returns the access token or raises RuntimeError.
    """
    from code_puppy.plugins.walmart_specific.msgraph_tokens import get_valid_access_token

    # Step 1: Try silent refresh (uses refresh_token under the hood)
    token = get_valid_access_token()
    if token:
        return token

    # Step 2: Silent refresh failed — launch browser auth flow
    print(json.dumps({"_log": "Token expired, launching browser re-auth..."}), file=sys.stderr)
    try:
        from code_puppy.plugins.walmart_specific.msgraph_auth import handle_msgraph_auth_command
        result = handle_msgraph_auth_command("/msgraph_auth", "msgraph_auth")
        if result and "successful" in result.lower():
            token = get_valid_access_token()
            if token:
                return token
    except Exception as e:
        raise RuntimeError(
            f"Auto re-auth failed: {e}. Please run /msgraph_auth in Code Puppy."
        ) from e

    raise RuntimeError(
        "Microsoft Graph authentication required. "
        "Please run /msgraph_auth in Code Puppy and try again."
    )


def get_next_row(client) -> int:
    """Find the actual last row with data, return next available row number."""
    result = client.get(f"{BASE_PATH}/usedRange(valuesOnly=true)")
    address = result.get("address", "")
    # Address looks like "Sheet1!A1:Y2452" — [A-Za-z]+ avoids greedy digit eating
    match = re.search(r":([A-Za-z]+)(\d+)$", address)
    if match:
        return int(match.group(2)) + 1
    raise ValueError(f"Could not parse used range address: {address}")


def write_row(client, row_num: int, data: dict) -> None:
    """Write the flip request data to the specified row.

    Uses 'formulas' so columns Q and R get proper VLOOKUPs:
      Q: WM Week of arrival at current FC (from delivery date)
      R: Current placed orders at requested FC (from WM Week)
    """
    # Formulas referencing the target row number
    wm_week_formula     = f"=VLOOKUP(L{row_num},FY26_Dates!A:B,2,FALSE)"
    placed_orders_formula = f"=VLOOKUP(Q{row_num},Weekly_Inbound_Deliveries!H:J,3,FALSE)"

    formulas = [[
        data.get("date", ""),          # A
        data.get("am_name", ""),       # B
        data.get("pid", ""),           # C
        data.get("seller_name", ""),   # D
        data.get("po_number", ""),     # E
        data.get("total_units", ""),   # F
        data.get("assigned_fc", ""),   # G
        data.get("request_fc", ""),    # H
        data.get("total_gmv", ""),     # I
        data.get("l3_category", ""),   # J
        data.get("reason", ""),        # K
        data.get("delivery_date", ""), # L
        "",                            # M - blank
        data.get("event", ""),         # N
        data.get("hero_item", "N"),    # O
        data.get("ae_event", "N"),     # P
        wm_week_formula,               # Q - WM Week of arrival at current FC
        placed_orders_formula,         # R - Current placed orders at request FC
        "",                            # S - AM Action
        "",                            # T - Approved
        "",                            # U - Inv. Mgmt Comment
    ]]

    address = f"A{row_num}:U{row_num}"
    client.patch(
        f"{BASE_PATH}/range(address='{address}')",
        json={"formulas": formulas},
    )


def main():
    if len(sys.argv) < 2:
        print(json.dumps({"success": False, "error": "No data provided"}))
        sys.exit(1)

    try:
        data = json.loads(sys.argv[1])
    except json.JSONDecodeError as e:
        print(json.dumps({"success": False, "error": f"Invalid JSON: {e}"}))
        sys.exit(1)

    try:
        # Ensure valid token — auto-refreshes, or launches browser if fully expired
        get_valid_token_or_reauth()

        from code_puppy.plugins.walmart_specific.msgraph_client import MSGraphClient
        client = MSGraphClient()

        row_num = get_next_row(client)
        write_row(client, row_num, data)

        print(json.dumps({"success": True, "row_number": row_num}))

    except Exception as e:
        print(json.dumps({"success": False, "error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    main()