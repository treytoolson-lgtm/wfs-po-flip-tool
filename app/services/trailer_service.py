"""Trailer co-load context service.

Given a TRAILER_ID, fetches all WFS POs riding that physical trailer,
their unit counts and GMV. Used by the Escalation Lookup to surface
trailer-level context alongside the verdict (HTMX lazy-load).

Lives here — not in bigquery.py — to keep bigquery.py under 600 lines.
Fails gracefully: any BQ error returns None without crashing the submission.
"""
from __future__ import annotations

import logging
from typing import Any

from google.cloud import bigquery

log = logging.getLogger(__name__)


_TRAILER_CONTEXT_QUERY = """
WITH trailer_deliveries AS (
    -- Every delivery number on this physical trailer
    SELECT DISTINCT DELIVERY_NUMBER
    FROM `wmt-cp-prod.e2e_fmt_cp.ETUP_DC_TRAILERS`
    WHERE TRAILER_ID = @trailer_id
      AND (GATE_OUT_TS_LCL IS NULL
           OR GATE_OUT_TS_LCL >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 7 DAY))
)
SELECT
    p.PO_NUM                                        AS po_num,
    p.VENDOR_NAME                                   AS seller_name,
    CAST(SUM(p.ORDER_QTY) AS INT64)                 AS total_units,
    ROUND(SUM(p.ORDER_QTY * p.CURR_ITEM_PRICE), 2)  AS gmv
FROM `wmt-cp-prod.e2e_fmt_cp.ETUP_DELIVERY_PO_LINES` p
INNER JOIN trailer_deliveries td ON p.DELIVERY_NUMBER = td.DELIVERY_NUMBER
WHERE p.PO_OWNER = 'WFS'
  AND p.INSERT_DT_UTC >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY p.PO_NUM, p.VENDOR_NAME
ORDER BY gmv DESC
LIMIT 20
"""

_INVALID_IDS = {"", "NONE", "NULL", "N/A", "NA"}


def get_trailer_context(
    trailer_id: str,
    current_po_num: str = "",
) -> dict[str, Any] | None:
    """Return co-load context for a physical trailer.

    Args:
        trailer_id:      The TRAILER_ID from ETUP_DC_TRAILERS.
        current_po_num:  Excluded from the co-load list (already on screen).

    Returns:
        Dict with trailer_id, co_load_po_count, trailer_total_units,
        trailer_total_gmv, co_load_details (list of up to 10 POs).
        Returns None if trailer_id is blank/invalid or BQ fails.
    """
    if not trailer_id or trailer_id.strip().upper() in _INVALID_IDS:
        return None

    try:
        # Lazy import to avoid circular dependency at module load time
        from app.services.bigquery import _get_client

        client = _get_client()
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("trailer_id", "STRING", trailer_id),
            ]
        )
        log.info("Fetching trailer context for TRAILER_ID=%s", trailer_id)
        job  = client.query(_TRAILER_CONTEXT_QUERY, job_config=job_config)
        rows = [dict(r) for r in job.result()]

        if not rows:
            return None

        co_load = [r for r in rows if r["po_num"] != current_po_num]

        return {
            "trailer_id":          trailer_id,
            "co_load_po_count":    len(co_load),
            "trailer_total_units": sum(r["total_units"] or 0 for r in rows),
            "trailer_total_gmv":   round(sum(r["gmv"] or 0 for r in rows), 2),
            "co_load_details":     co_load[:10],
        }
    except Exception as e:
        log.warning("get_trailer_context(%s) failed: %s", trailer_id, e)
        return None
