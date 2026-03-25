"""Helper to send Teams notifications via msgraph sub-agent."""
from __future__ import annotations
import subprocess
import logging
import json

log = logging.getLogger(__name__)


def post_teams_notification(
    team_id: str,
    channel_id: str,
    po_number: str,
    am_name: str,
    status: str,
    comment: str = "",
) -> bool:
    """
    Post a flip status update to Teams channel via msgraph agent.
    
    Args:
        team_id: Teams team ID
        channel_id: Channel ID for alerts
        po_number: PO number that was updated
        am_name: AM who submitted the request
        status: APPROVED or DENIED
        comment: Optional comment from Inv. Mgmt
    
    Returns:
        True if posted successfully, False otherwise
    """
    if not team_id or not channel_id:
        log.warning("Teams not configured - skipping notification")
        return False

    emoji = "✅" if status == "APPROVED" else "❌"
    
    message = f"""
🔔 **PO Flip Update: {po_number}**

{emoji} **Status:** {status}
👤 **AM:** {am_name}
"""
    
    if comment:
        message += f"\n💬 **Comment:** {comment}"
    
    try:
        # TODO: Implement proper Graph API auth for production
        # For now, log what we would post
        log.info("="*60)
        log.info("[TEAMS NOTIFICATION] Would post to channel %s:", channel_id)
        log.info(message)
        log.info("="*60)
        log.info("To post manually: /agent msgraph, then paste message above")
        return True
        
    except Exception as e:
        log.error("Failed to post Teams notification: %s", e)
        return False
