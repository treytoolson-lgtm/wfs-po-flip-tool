"""Microsoft Graph API client for SharePoint + Teams."""
from __future__ import annotations
import logging
import httpx

log = logging.getLogger(__name__)

GRAPH_BASE = "https://graph.microsoft.com/v1.0"


class GraphClient:
    """Thin wrapper around MS Graph using a bearer token."""

    def __init__(self, access_token: str):
        self._token = access_token
        self._headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

    def _get(self, url: str, **kwargs) -> dict:
        with httpx.Client(timeout=30) as client:
            resp = client.get(url, headers=self._headers, **kwargs)
            resp.raise_for_status()
            return resp.json()

    def _post(self, url: str, json: dict) -> dict:
        with httpx.Client(timeout=30) as client:
            resp = client.post(url, headers=self._headers, json=json)
            resp.raise_for_status()
            return resp.json()


class SharePointService:
    """Read/write the WFS PO Flip Excel file via Graph API."""

    def __init__(self, client: GraphClient, site_id: str, file_id: str, sheet: str):
        self._client = client
        self._site_id = site_id
        self._file_id = file_id
        self._sheet = sheet
        self._base = f"{GRAPH_BASE}/sites/{site_id}/drive/items/{file_id}/workbook/worksheets/{sheet}"

    def get_used_range(self) -> dict:
        url = f"{self._base}/usedRange"
        return self._client._get(url)

    def read_row(self, row_index: int, col_count: int = 21) -> list:
        """Read a specific row (1-based). Returns list of cell values A-U."""
        # Graph uses 0-based row/col in rangeAddress notation
        range_addr = f"A{row_index}:U{row_index}"
        url = f"{self._base}/range(address='{range_addr}')"
        data = self._client._get(url)
        values = data.get("values", [[]])
        return values[0] if values else []

    def append_row(self, values: list) -> int:
        """Append a row after the last used row. Returns 1-based row index written."""
        used = self.get_used_range()
        row_count = used.get("rowCount", 1)
        next_row = row_count + 1
        range_addr = f"A{next_row}:U{next_row}"
        url = f"{self._base}/range(address='{range_addr}')"
        self._client._post(url, json={"values": [values]})
        log.info("SharePoint: wrote row %d", next_row)
        return next_row

    def get_all_rows(self) -> list[list]:
        """Return all data rows (skipping header row 1)."""
        data = self.get_used_range()
        values = data.get("values", [])
        return values[1:] if len(values) > 1 else []  # skip header


class TeamsService:
    """Post to Teams channels via Graph API."""

    def __init__(self, client: GraphClient):
        self._client = client

    def post_to_channel(self, team_id: str, channel_id: str, message: str):
        """Post a message to a Teams channel."""
        try:
            url = f"{GRAPH_BASE}/teams/{team_id}/channels/{channel_id}/messages"
            self._client._post(url, json={"body": {"content": message}})
            log.info("Teams channel message posted successfully")
        except Exception as e:
            log.error("Failed to post to Teams channel: %s", e)
            raise

    def send_dm(self, to_email: str, message: str):
        """Send a Teams DM to a user by their email/UPN."""
        try:
            # Get user ID
            user_data = self._client._get(f"{GRAPH_BASE}/users/{to_email}")
            user_id = user_data["id"]

            # Create or get existing chat
            me = self._client._get(f"{GRAPH_BASE}/me")
            my_id = me["id"]

            chat = self._client._post(
                f"{GRAPH_BASE}/chats",
                json={
                    "chatType": "oneOnOne",
                    "members": [
                        {"@odata.type": "#microsoft.graph.aadUserConversationMember",
                         "roles": ["owner"],
                         "user@odata.bind": f"{GRAPH_BASE}/users('{my_id}')"},
                        {"@odata.type": "#microsoft.graph.aadUserConversationMember",
                         "roles": ["owner"],
                         "user@odata.bind": f"{GRAPH_BASE}/users('{user_id}')"},
                    ],
                },
            )
            chat_id = chat["id"]

            # Send message
            self._client._post(
                f"{GRAPH_BASE}/chats/{chat_id}/messages",
                json={"body": {"content": message}},
            )
            log.info("Teams DM sent to %s", to_email)
        except Exception as e:
            log.error("Failed to send Teams DM to %s: %s", to_email, e)
            raise
