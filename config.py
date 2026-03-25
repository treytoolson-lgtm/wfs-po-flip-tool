import os
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # GCloud SDK (non-standard Mac install)
    GCLOUD_SDK_PATH: str = "/Users/t0t0ech/Documents/gCloud CLI/google-cloud-sdk"

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
    APP_PORT: int = 8765

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"

    def configure_gcloud_path(self):
        """Add gcloud SDK bin to PATH so google-auth can find it."""
        bin_path = os.path.join(self.GCLOUD_SDK_PATH, "bin")
        current = os.environ.get("PATH", "")
        if bin_path not in current:
            os.environ["PATH"] = f"{bin_path}:{current}"

    @property
    def monitor_hours(self) -> list[int]:
        return [int(h.strip()) for h in self.MONITOR_SCHEDULE_HOURS.split(",")]


@lru_cache
def get_settings() -> Settings:
    return Settings()
