#!/usr/bin/env python3
"""CLI script to post a Teams notification via msgraph agent.

Usage:
    python3 post_teams_message.py <team_id> <channel_id> <message>
"""
import sys
import os

# Add parent dir to path so we can import from app/
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import get_settings


def post_message(team_id: str, channel_id: str, message: str) -> bool:
    """
    Post message to Teams channel by invoking msgraph sub-agent.
    
    Since we can't easily subprocess back to code-puppy from within code-puppy,
    we'll use the Graph API directly via the stored msgraph credentials.
    """
    try:
        # Import the GraphClient from our services
        from app.services.graph import GraphClient, TeamsService
        
        # TODO: Get the msgraph access token
        # For now, this is a placeholder until we wire up token retrieval
        # The msgraph agent stores tokens somewhere on disk - we need to find that location
        
        # Once we have the token:
        # client = GraphClient(access_token=token)
        # teams = TeamsService(client)
        # teams.post_to_channel(team_id, channel_id, message)
        
        print(f"[TEAMS] Would post to {channel_id}: {message}", file=sys.stderr)
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to post: {e}", file=sys.stderr)
        return False


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: post_teams_message.py <team_id> <channel_id> <message>", file=sys.stderr)
        sys.exit(1)
    
    team_id = sys.argv[1]
    channel_id = sys.argv[2]
    message = sys.argv[3]
    
    success = post_message(team_id, channel_id, message)
    sys.exit(0 if success else 1)
