import os
import sys
from functools import lru_cache
from pydantic_settings import BaseSettings


def _default_gcloud_path() -> str:
    """Best-guess gCloud SDK path per platform.
    Override via GCLOUD_SDK_PATH in .env if installed elsewhere."""
    home = os.path.expanduser("~")
    if sys.platform == "win32":
        # Common Windows install locations
        for candidate in [
            os.path.join(os.environ.get("LOCALAPPDATA", ""), "Google", "Cloud SDK", "google-cloud-sdk"),
            os.path.join(home, "AppData", "Local", "Google", "Cloud SDK", "google-cloud-sdk"),
        ]:
            if os.path.isdir(candidate):
                return candidate
        return ""  # already on PATH from installer
    else:
        # Mac — check common install locations
        for candidate in [
            os.path.join(home, "Documents", "gCloud CLI", "google-cloud-sdk"),
            os.path.join(home, "google-cloud-sdk"),
            "/usr/lib/google-cloud-sdk",
        ]:
            if os.path.isdir(candidate):
                return candidate
        return ""  # already on PATH


class Settings(BaseSettings):
    # GCloud SDK — auto-detected per platform, override in .env if needed
    GCLOUD_SDK_PATH: str = _default_gcloud_path()

    # BigQuery
    BQ_PROJECT_PRIMARY: str = "wmt-cp-prod"
    BQ_PROJECT_ANALYTICS: str = "wmt-wfs-analytics"
    BQ_DATE_WINDOW_DAYS: int = 180

    # SharePoint (PO Flip spreadsheet)
    SHAREPOINT_SITE: str = "https://wal-mart.sharepoint.com/sites/FulfillmentServices"
    SHAREPOINT_FILE_ID: str = "3412E7C8-7761-4233-B87D-384885821FEE"
    SHAREPOINT_SHEET: str = "Sheet1"

    # Teams (for flip notifications)
    TEAMS_TEAM_ID: str = ""  # WFS PO Flip Tool team
    TEAMS_CHANNEL_ID: str = ""  # alerts channel

    # SharePoint column indices (1-based)
    SP_COL_AM: str = "B"       # Who submitted
    SP_COL_APPROVED: str = "T" # Y / N
    SP_COL_COMMENT: str = "U"  # Inv. Mgmt Comment

    # Escalation logic
    WOS_THRESHOLD: float = 2.0

    # Scheduler — EST hours to poll SharePoint (24h)
    MONITOR_SCHEDULE_HOURS: str = "9,13,17"

    # App
    APP_PORT: int = 8766

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"

    def configure_gcloud_path(self):
        """Add gcloud SDK bin to PATH so google-auth can find it."""
        if not self.GCLOUD_SDK_PATH:
            return  # already on PATH (Windows installer / system install)
        bin_path = os.path.join(self.GCLOUD_SDK_PATH, "bin")
        separator = ";" if sys.platform == "win32" else ":"
        current = os.environ.get("PATH", "")
        if bin_path not in current:
            os.environ["PATH"] = f"{bin_path}{separator}{current}"

    @property
    def monitor_hours(self) -> list[int]:
        return [int(h.strip()) for h in self.MONITOR_SCHEDULE_HOURS.split(",")]


@lru_cache
def get_settings() -> Settings:
    return Settings()
