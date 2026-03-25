import aiosqlite
import logging
from datetime import datetime

DB_PATH = "wfs_tracker.db"
log = logging.getLogger(__name__)

INIT_SQL = """
CREATE TABLE IF NOT EXISTS flip_requests (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    po_number       TEXT NOT NULL,
    seller_name     TEXT,
    am_name         TEXT,
    am_email        TEXT,
    assigned_fc     TEXT,
    request_fc      TEXT,
    sharepoint_row  INTEGER,
    status          TEXT DEFAULT 'PENDING',  -- PENDING, APPROVED, DENIED
    inv_mgmt_comment TEXT,
    submitted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_checked_at TIMESTAMP,
    notified_at     TIMESTAMP
);
"""


async def init_db():
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(INIT_SQL)
        await conn.commit()
    log.info("DB initialized at %s", DB_PATH)


async def add_flip_request(
    po_number: str,
    seller_name: str,
    am_name: str,
    am_email: str,
    assigned_fc: str,
    request_fc: str,
    sharepoint_row: int,
) -> int:
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            """
            INSERT INTO flip_requests
                (po_number, seller_name, am_name, am_email, assigned_fc, request_fc, sharepoint_row)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (po_number, seller_name, am_name, am_email, assigned_fc, request_fc, sharepoint_row),
        )
        await conn.commit()
        return cursor.lastrowid


async def get_pending_requests() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM flip_requests WHERE status = 'PENDING' ORDER BY submitted_at DESC"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def get_all_requests(limit: int = 100) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM flip_requests ORDER BY submitted_at DESC LIMIT ?",
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def update_flip_status(
    request_id: int,
    status: str,
    inv_mgmt_comment: str = "",
):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            """
            UPDATE flip_requests
            SET status = ?,
                inv_mgmt_comment = ?,
                last_checked_at = CURRENT_TIMESTAMP,
                notified_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (status, inv_mgmt_comment, request_id),
        )
        await conn.commit()
