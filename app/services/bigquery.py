from __future__ import annotations
import logging
import os
import sys
from typing import Any

from google.cloud import bigquery
from google.oauth2 import credentials as google_credentials
import google.auth

from config import get_settings

log = logging.getLogger(__name__)

# Local file to cache FC capacity data across uvicorn workers/reloads


FC_CAPACITY_QUERY = """
WITH trailer_base AS (
    SELECT
        FC_NAME,
        TRAILER_ID,
        ARRIVAL_TS_LCL,
        GATE_OUT_TS_LCL,
        ETUP_COMPLETED_ENTITY_OPERATION_TS_LCL,
        DELIVERY_STATUS,
        -- Use native TRAILER_PO_SLR column — same definition analytics team uses,
        -- no cross-dataset join needed. Replaces the old WFS_IB_FC_DELIVERY_DETAILS join.
        CASE WHEN TRAILER_PO_SLR = 'WFS' THEN 1 ELSE 0 END AS is_wfs
    FROM `wmt-cp-prod.e2e_fmt_cp.ETUP_DC_TRAILERS`
    WHERE FC_NAME IS NOT NULL
      AND (GATE_OUT_TS_LCL IS NULL
           OR GATE_OUT_TS_LCL >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 30 DAY))
),
current_yard AS (
    SELECT
        FC_NAME,
        COUNT(DISTINCT TRAILER_ID)                                              AS trailers_on_yard,
        COUNT(DISTINCT CASE WHEN is_wfs = 1 THEN TRAILER_ID END)               AS wfs_on_yard,
        -- Waiting vs actively unloading (WFS)
        COUNT(DISTINCT CASE WHEN is_wfs = 1
            AND UPPER(DELIVERY_STATUS) IN ('ARV','ARRIVED')
            THEN TRAILER_ID END)                                                AS wfs_waiting_to_unload,
        COUNT(DISTINCT CASE WHEN is_wfs = 1
            AND UPPER(DELIVERY_STATUS) IN ('WRK','WORKING')
            THEN TRAILER_ID END)                                                AS wfs_currently_unloading,
        -- Total network (all sellers)
        COUNT(DISTINCT CASE WHEN UPPER(DELIVERY_STATUS) IN ('ARV','ARRIVED')
            THEN TRAILER_ID END)                                                AS waiting_to_unload,
        COUNT(DISTINCT CASE WHEN UPPER(DELIVERY_STATUS) IN ('WRK','WORKING')
            THEN TRAILER_ID END)                                                AS currently_unloading,
        -- Dwell: cap at 336 hrs (14 days) so stale outliers do not distort the average
        ROUND(AVG(
            CASE WHEN DATETIME_DIFF(CURRENT_DATETIME(), ARRIVAL_TS_LCL, HOUR) <= 336
            THEN DATETIME_DIFF(CURRENT_DATETIME(), ARRIVAL_TS_LCL, HOUR) END
        ), 1)                                                                   AS avg_dwell_hours,
        ROUND(AVG(
            CASE WHEN is_wfs = 1
              AND DATETIME_DIFF(CURRENT_DATETIME(), ARRIVAL_TS_LCL, HOUR) <= 336
            THEN DATETIME_DIFF(CURRENT_DATETIME(), ARRIVAL_TS_LCL, HOUR) END
        ), 1)                                                                   AS wfs_avg_dwell_hours
    FROM trailer_base
    WHERE GATE_OUT_TS_LCL IS NULL
      AND ARRIVAL_TS_LCL IS NOT NULL
      AND ETUP_COMPLETED_ENTITY_OPERATION_TS_LCL IS NULL  -- analytics report excludes trailers already ETUP-complete even if they have not gated out yet
    GROUP BY FC_NAME
),
velocity_data AS (
    SELECT
        FC_NAME,
        ROUND(COUNT(DISTINCT CASE WHEN GATE_OUT_TS_LCL >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 7 DAY)
            THEN TRAILER_ID END) / 7.0, 1)                                     AS velocity_7d,
        ROUND(COUNT(DISTINCT CASE WHEN is_wfs = 1
            AND GATE_OUT_TS_LCL >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 7 DAY)
            THEN TRAILER_ID END) / 7.0, 1)                                     AS wfs_velocity_7d,
    FROM trailer_base
    WHERE GATE_OUT_TS_LCL >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 30 DAY)
    GROUP BY FC_NAME
)
SELECT
    c.FC_NAME                                                                   AS fc_name,
    c.trailers_on_yard,
    c.wfs_on_yard,
    c.wfs_waiting_to_unload,
    c.wfs_currently_unloading,
    c.waiting_to_unload,
    c.currently_unloading,
    -- % of WFS yard sitting in ARV (waiting, not yet docked)
    ROUND(SAFE_DIVIDE(c.wfs_waiting_to_unload, NULLIF(c.wfs_on_yard, 0)) * 100, 1)
                                                                                AS wfs_waiting_ratio_pct,
    c.avg_dwell_hours,
    c.wfs_avg_dwell_hours,
    v.velocity_7d,
    v.wfs_velocity_7d,
    -- PRIMARY congestion metric: days at current velocity to clear what's on yard
    ROUND(SAFE_DIVIDE(c.wfs_on_yard, NULLIF(v.wfs_velocity_7d, 0)), 1)         AS days_to_clear,
    -- Smarter status: velocity-relative + dwell-based (not flat trailer count)
    CASE
        WHEN SAFE_DIVIDE(c.wfs_on_yard, NULLIF(v.wfs_velocity_7d, 0)) > 3
          OR c.wfs_avg_dwell_hours > 24  THEN 'High'
        WHEN SAFE_DIVIDE(c.wfs_on_yard, NULLIF(v.wfs_velocity_7d, 0)) > 1.5
          OR c.wfs_avg_dwell_hours > 12  THEN 'Medium'
        ELSE 'Low'
    END                                                                         AS status
FROM current_yard c
LEFT JOIN velocity_data v ON c.FC_NAME = v.FC_NAME
ORDER BY days_to_clear DESC NULLS LAST
"""

# IB UPH — 5-week trailing average per FC. Weekly data, cached at 6h TTL.
FC_UPH_QUERY = """
SELECT
    level                              AS fc_name_raw,
    ROUND(AVG(metric_value), 0)        AS avg_ib_uph,
    MAX(fyww)                          AS latest_week
FROM `wmt-wfs-analytics.wfs_ops_analytics.IB_OB_Total_UPH_historical`
WHERE metric_name  = 'IB_UPH'
  AND measure_type = 'Actual'
  AND level        != 'Total'
  AND fyww >= (
    SELECT MAX(fyww) - 5
    FROM `wmt-wfs-analytics.wfs_ops_analytics.IB_OB_Total_UPH_historical`
    WHERE metric_name = 'IB_UPH'
  )
GROUP BY 1
ORDER BY avg_ib_uph DESC
"""

def _build_capacity_row(r: Any) -> dict[str, Any]:
    """Map a BQ row to a serialisable dict with all new Phase-1 fields."""
    return {
        "fc_name":                r["fc_name"],
        "trailers_on_yard":       r["trailers_on_yard"],
        "wfs_on_yard":            r["wfs_on_yard"] or 0,
        "wfs_waiting_to_unload":  r["wfs_waiting_to_unload"] or 0,
        "wfs_currently_unloading":r["wfs_currently_unloading"] or 0,
        "waiting_to_unload":      r["waiting_to_unload"] or 0,
        "currently_unloading":    r["currently_unloading"] or 0,
        "wfs_waiting_ratio_pct":  r["wfs_waiting_ratio_pct"] or 0,
        "avg_dwell_hours":        r["avg_dwell_hours"],
        "wfs_avg_dwell_hours":    r["wfs_avg_dwell_hours"] or 0,
        "velocity_7d":            r["velocity_7d"],
        "wfs_velocity_7d":        r["wfs_velocity_7d"] or 0,
        "days_to_clear":          r["days_to_clear"],
        "status":                 r["status"],
    }


def get_fc_capacity_raw() -> list[dict[str, Any]]:
    """Run FC_CAPACITY_QUERY and return rows. No caching — use capacity_service."""
    client = _get_client()
    log.info("Fetching real-time FC capacity data from BigQuery...")
    job = client.query(FC_CAPACITY_QUERY)
    return [_build_capacity_row(r) for r in job.result()]


def get_fc_uph_raw() -> list[dict[str, Any]]:
    """Run FC_UPH_QUERY and return rows. No caching — called by capacity_service."""
    client = _get_client()
    log.info("Fetching IB UPH data from BigQuery...")
    job = client.query(FC_UPH_QUERY)
    return [
        {
            "fc_name_raw": r["fc_name_raw"],
            "avg_ib_uph":  int(r["avg_ib_uph"]) if r["avg_ib_uph"] else None,
            "latest_week": r["latest_week"],
        }
        for r in job.result()
    ]

# Master SQL — Updated 2026-03-25
# - Fixed cartesian product duplicates (validated vs Seller Center)
# - Updated 2026-03-25: L3 now from preproc_offer_detl (covers all WFS items)
#   events_item_list only has event/campaign items, so many POs were blank
MASTER_QUERY = """
WITH po_base AS (
  -- Core PO data with accurate unit counts (no joins = no duplicates)
  SELECT
    p.PO_NUM,
    p.DELIVERY_NUMBER,
    p.DC_NUMBER,
    p.VENDOR_NUM                                    AS PID,
    p.VENDOR_NAME                                   AS SELLER_NAME,
    p.ITEM_ID,
    p.ITEM_NAME,
    ANY_VALUE(p.OFFR_ID)                            AS OFFR_ID,
    SUM(p.ORDER_QTY)                                AS TOTAL_UNITS,
    ROUND(SUM(p.ORDER_QTY * p.CURR_ITEM_PRICE), 2)  AS GMV_IMPACT
  FROM `wmt-cp-prod.e2e_fmt_cp.ETUP_DELIVERY_PO_LINES` p
  WHERE p.PO_NUM IN UNNEST(@po_numbers)
    AND p.INSERT_DT_UTC >= DATE_SUB(CURRENT_DATE(), INTERVAL @date_window_days DAY)
    AND p.PO_OWNER = 'WFS'
  GROUP BY p.PO_NUM, p.DELIVERY_NUMBER, p.DC_NUMBER, p.VENDOR_NUM, p.VENDOR_NAME, p.ITEM_ID, p.ITEM_NAME
),
offer_keys AS (
  SELECT DISTINCT
    OFFR_ID
  FROM po_base
  WHERE OFFR_ID IS NOT NULL
),
po_delivery_dc_keys AS (
  SELECT DISTINCT
    PO_NUM,
    DELIVERY_NUMBER,
    DC_NUMBER
  FROM po_base
),
delivery_dc_keys AS (
  SELECT DISTINCT
    DELIVERY_NUMBER,
    DC_NUMBER
  FROM po_base
),
item_partner_keys AS (
  SELECT DISTINCT
    ITEM_ID,
    CAST(ITEM_ID AS STRING) AS ITEM_ID_STR,
    PID
  FROM po_base
),
seller_keys AS (
  SELECT DISTINCT
    PID
  FROM po_base
),
item_hierarchy AS (
  -- L3 category — keep same latest-row logic, but only for queried offer ids
  SELECT
    offr_id,
    rpt_lvl_3_nm  AS L3_CATEGORY
  FROM (
    SELECT
      p.offr_id,
      p.rpt_lvl_3_nm,
      ROW_NUMBER() OVER (PARTITION BY p.offr_id ORDER BY p.rpt_dt DESC) AS rn
    FROM `wmt-wfs-analytics.WW_MP_DS_MODELS.preproc_offer_detl` p
    JOIN offer_keys k
      ON p.offr_id = k.OFFR_ID
    WHERE p.rpt_dt >= FORMAT_DATE('%F', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
  )
  WHERE rn = 1
),
trailer_info AS (
  -- Filtered to only delivery+DC pairs in this query — same join grain as final join
  SELECT
    t.DELIVERY_NUMBER,
    t.DC_NUMBER,
    ANY_VALUE(t.TRAILER_ID)                   AS TRAILER_ID,
    ANY_VALUE(t.FC_NAME)                      AS FC_NAME,
    ANY_VALUE(t.ARRIVAL_TS_LCL)               AS ARRIVAL_TS_LCL,
    MAX(t.FINAL_RANKING)                      AS FINAL_RANKING,
    ANY_VALUE(UPPER(t.DELIVERY_TYPE_CODE))    AS DELIVERY_TYPE,
    ANY_VALUE(t.DELIVERY_STATUS)              AS DELIVERY_STATUS,
    MAX(t.IS_ESCALATION_INSTOCK)              AS IS_ESCALATION_INSTOCK,
    MAX(t.ESCALATION_EVENT_WINDOW)            AS ESCALATION_EVENT_WINDOW,
    ANY_VALUE(t.ESCALATION_TRAILER_REASON)    AS ESCALATION_TRAILER_REASON,
    ANY_VALUE(t.ESCALATION_PO_REASON)         AS ESCALATION_PO_REASON
  FROM `wmt-cp-prod.e2e_fmt_cp.ETUP_DC_TRAILERS` t
  JOIN delivery_dc_keys k
    ON t.DELIVERY_NUMBER = k.DELIVERY_NUMBER
   AND t.DC_NUMBER = k.DC_NUMBER
  GROUP BY t.DELIVERY_NUMBER, t.DC_NUMBER
),
scheduler_info AS (
  -- Get scheduling metadata (ONE row per PO+Delivery+DC)
  SELECT
    s.PO_NUMBER,
    s.DELIVERY_NUMBER,
    CAST(s.DC_NUMBER AS STRING)               AS DC_NUMBER,
    ANY_VALUE(s.LOAD_TYPE_NAME)               AS LOAD_TYPE_NAME,
    ANY_VALUE(s.APPOINTMENT_DATE)             AS APPOINTMENT_DATE,
    ANY_VALUE(s.WM_YR_WK_NBR)                 AS WM_YR_WK_NBR,
    MAX(s.IS_LTL)                             AS IS_LTL,
    ANY_VALUE(s.INVENTORY_TYPE_NAME)          AS INVENTORY_TYPE_NAME
  FROM `wmt-wfs-analytics.wfs_ops_analytics.mat_inbound_scheduler_base` s
  JOIN po_delivery_dc_keys k
    ON s.PO_NUMBER = k.PO_NUM
   AND s.DELIVERY_NUMBER = k.DELIVERY_NUMBER
   AND CAST(s.DC_NUMBER AS STRING) = k.DC_NUMBER
  WHERE s.wfs_ind = 1
  GROUP BY s.PO_NUMBER, s.DELIVERY_NUMBER, CAST(s.DC_NUMBER AS STRING)
),
events_info AS (
  -- Event/hero flags only for queried item+partner pairs
  SELECT
    e.CATLG_ITEM_ID,
    e.PRTNR_ORG_CD,
    MAX(e.HERO_ITEM_IND)                      AS IS_HERO_ITEM,
    MAX(e.MOSC_ITEM_IND)                      AS IS_MOSAIC_ITEM,
    MAX(e.MINI_EVENT_NM)                      AS EVENT_NAME,
    MAX(e.EVENT_ITEM_STATUS_NM)               AS EVENT_STATUS
  FROM `wmt-wfs-analytics.inv_ana.events_item_list` e
  JOIN item_partner_keys k
    ON e.CATLG_ITEM_ID = k.ITEM_ID
   AND e.PRTNR_ORG_CD = k.PID
  GROUP BY e.CATLG_ITEM_ID, e.PRTNR_ORG_CD
),
isr_info AS (
  -- Filtered to only item+partner pairs in this query — avoids broad ISR scans
  SELECT
    i.catlg_item_id,
    i.prtnr_id,
    MAX(i.WOS_fcst)                           AS WEEKS_OF_SUPPLY,
    MAX(i.hero_flag)                          AS HERO_FLAG_ISR,
    MAX(i.ats_qty_current_wk)                 AS ATS_QTY
  FROM `wmt-wfs-analytics.inv_ana.Inv_ISR_Forward_Looking_copy` i
  JOIN item_partner_keys k
    ON i.catlg_item_id = k.ITEM_ID_STR
   AND i.prtnr_id = k.PID
  GROUP BY i.catlg_item_id, i.prtnr_id
),
am_info AS (
  -- Get AM info only for sellers in this query
  SELECT
    am.partner_id,
    ANY_VALUE(am.am_name)                     AS AM_NAME,
    ANY_VALUE(am.wfs_am_email)                AS AM_EMAIL
  FROM `wmt-wfs-analytics.wfs_ops_analytics.mat_mpoa_seller_mart` am
  JOIN seller_keys k
    ON am.partner_id = k.PID
  GROUP BY am.partner_id
)
-- Final join: Each CTE is already deduped, so no cartesian product!
SELECT
  pb.PO_NUM,
  pb.DELIVERY_NUMBER,
  pb.DC_NUMBER,
  pb.PID,
  pb.SELLER_NAME,
  am.AM_NAME,
  am.AM_EMAIL,
  pb.ITEM_ID,
  pb.ITEM_NAME,
  ih.L3_CATEGORY,
  pb.TOTAL_UNITS,
  pb.GMV_IMPACT,
  t.TRAILER_ID,
  t.FC_NAME,
  t.ARRIVAL_TS_LCL,
  t.FINAL_RANKING,
  t.DELIVERY_TYPE,
  t.DELIVERY_STATUS,
  t.IS_ESCALATION_INSTOCK,
  t.ESCALATION_EVENT_WINDOW,
  t.ESCALATION_TRAILER_REASON,
  t.ESCALATION_PO_REASON,
  s.LOAD_TYPE_NAME,
  s.APPOINTMENT_DATE,
  s.WM_YR_WK_NBR,
  s.IS_LTL,
  s.INVENTORY_TYPE_NAME,
  e.IS_HERO_ITEM,
  e.IS_MOSAIC_ITEM,
  e.EVENT_NAME,
  e.EVENT_STATUS,
  i.WEEKS_OF_SUPPLY,
  i.HERO_FLAG_ISR,
  i.ATS_QTY
FROM po_base pb
LEFT JOIN trailer_info t
  ON  pb.DELIVERY_NUMBER = t.DELIVERY_NUMBER
  AND pb.DC_NUMBER       = t.DC_NUMBER
LEFT JOIN scheduler_info s
  ON  pb.PO_NUM                 = s.PO_NUMBER
  AND pb.DELIVERY_NUMBER        = s.DELIVERY_NUMBER
  AND pb.DC_NUMBER = s.DC_NUMBER
LEFT JOIN item_hierarchy ih
  ON  pb.OFFR_ID = ih.offr_id
LEFT JOIN events_info e
  ON  pb.ITEM_ID   = e.CATLG_ITEM_ID
  AND pb.PID       = e.PRTNR_ORG_CD
LEFT JOIN isr_info i
  ON  CAST(pb.ITEM_ID AS STRING) = i.catlg_item_id
  AND pb.PID                     = i.prtnr_id
LEFT JOIN am_info am
  ON pb.PID = am.partner_id
"""

PLACED_ORDERS_QUERY = """
SELECT
  COUNT(DISTINCT PO_NUMBER) AS placed_po_count,
  SUM(PO_CASE_QTY)          AS total_cases
FROM `wmt-wfs-analytics.wfs_ops_analytics.mat_inbound_scheduler_base`
WHERE wfs_ind       = 1
  AND FACILITY_NM   = @facility_nm
  AND WM_YR_WK_NBR  = @wm_yr_wk_nbr
"""


# Module-level singleton — created once, reused for every query
_bq_client: bigquery.Client | None = None


def _get_client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        settings = get_settings()
        settings.configure_gcloud_path()
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
        )
        _bq_client = bigquery.Client(
            project=settings.BQ_PROJECT_ANALYTICS, credentials=creds
        )
    return _bq_client


def query_po_numbers(po_numbers: list[str]) -> list[dict[str, Any]]:
    """Run master query for 1+ WFA PO numbers. Returns list of item-level rows."""
    settings = get_settings()
    client = _get_client()

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("po_numbers", "STRING", po_numbers),
            bigquery.ScalarQueryParameter("date_window_days", "INT64", settings.BQ_DATE_WINDOW_DAYS),
        ]
    )

    log.info("Running BQ master query for POs: %s", po_numbers)
    job = client.query(MASTER_QUERY, job_config=job_config)
    rows = list(job.result())
    log.info("BQ returned %d rows for %d PO(s)", len(rows), len(po_numbers))
    return [dict(r) for r in rows]


def query_placed_orders(facility_nm: str, wm_yr_wk_nbr: int) -> dict[str, Any]:
    """Get placed PO count + cases at a given FC for a given WM week."""
    client = _get_client()

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("facility_nm", "STRING", facility_nm),
            bigquery.ScalarQueryParameter("wm_yr_wk_nbr", "INT64", wm_yr_wk_nbr),
        ]
    )

    log.info("Querying placed orders: FC=%s WK=%s", facility_nm, wm_yr_wk_nbr)
    job = client.query(PLACED_ORDERS_QUERY, job_config=job_config)
    rows = list(job.result())
    return dict(rows[0]) if rows else {"placed_po_count": 0, "total_cases": 0}


# ============================================================================
# PO FLIP QUERY (Pre-Transit POs from Inbound_Sandbox)
# ============================================================================

FLIP_QUERY = """
WITH filtered_i AS (
  SELECT *
  FROM `wmt-wfs-analytics.WW_WFS_PROD_TABLES.Inbound_Sandbox`
  WHERE po_num IN UNNEST(@po_numbers)
    AND po_status NOT IN ('DELIVERED', 'RECEIVED', 'CANCELLED')
),
offer_keys AS (
  SELECT DISTINCT offr_id
  FROM filtered_i
),
seller_keys AS (
  SELECT DISTINCT prtnr_id AS seller_id
  FROM filtered_i
),
offer_history AS (
  SELECT *
  FROM `wmt-wfs-analytics.WW_MP_DS_MODELS.preproc_offer_detl`
  WHERE offr_id IN (SELECT offr_id FROM offer_keys)
    AND rpt_dt >= FORMAT_DATE('%F', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
),
am_info AS (
  SELECT
    partner_id,
    ANY_VALUE(diplay_name) AS diplay_name,
    ANY_VALUE(am_name) AS am_name,
    ANY_VALUE(wfs_am_email) AS wfs_am_email
  FROM `wmt-wfs-analytics.wfs_ops_analytics.mat_mpoa_seller_mart`
  WHERE partner_id IN (SELECT seller_id FROM seller_keys)
  GROUP BY partner_id
)
SELECT
  i.po_num,
  i.prtnr_id AS seller_id,
  i.prtnr_nm AS seller_name,
  COALESCE(am.diplay_name, i.prtnr_nm) AS seller_display_name,
  COALESCE(am.am_name, 'N/A') AS am_name,
  COALESCE(am.wfs_am_email, 'N/A') AS am_email,
  i.offr_id AS item_id,
  CASE WHEN i.tot_units > 0 THEN i.tot_units ELSE i.total_qty END AS units,
  i.fc AS current_fc,
  i.expctd_dlvry_dt AS expected_delivery_date,
  i.po_status,
  i.wm_week_nbr AS wm_week,
  i.carrier_nm,
  i.freight_class,
  p.rpt_lvl_3_nm AS l3_category,
  p.price_amt AS price_per_unit
FROM filtered_i i
LEFT JOIN offer_history p
  ON i.offr_id = p.offr_id
LEFT JOIN am_info am
  ON i.prtnr_id = am.partner_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY i.offr_id ORDER BY p.rpt_dt DESC) = 1
ORDER BY i.po_num, i.offr_id
"""


def query_flip_pos(po_numbers: list[str]) -> dict[str, Any]:
    """
    Query pre-transit POs from Inbound_Sandbox for flip requests.
    Returns dict with:
      - flippable: List of PO data (status = CREATED/CONFIRMED/SUBMITTED)
      - non_flippable: List of PO numbers that can't be flipped (wrong status)
      - items: Item-level rows
    """
    client = _get_client()

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("po_numbers", "STRING", po_numbers),
        ]
    )

    log.info("Running flip query for POs: %s", po_numbers)
    job = client.query(FLIP_QUERY, job_config=job_config)
    rows = list(job.result())
    log.info("Flip query returned %d rows for %d PO(s)", len(rows), len(po_numbers))

    items = [dict(r) for r in rows]
    
    # Group items by PO
    po_data = {}
    for item in items:
        po_num = item["po_num"]
        units = item.get("units") or 0
        price_str = item.get("price_per_unit") or "0"
        
        # Try to convert price to float
        try:
            price = float(price_str) if price_str else 0.0
        except (ValueError, TypeError):
            price = 0.0
        
        item_gmv = round(price * units, 2)
        
        if po_num not in po_data:
            po_data[po_num] = {
                "po_num": po_num,
                "seller_id": item["seller_id"],
                "seller_name": item["seller_name"],
                "seller_display_name": item.get("seller_display_name") or item["seller_name"],
                "am_name": item["am_name"],
                "am_email": item["am_email"],
                "current_fc": item["current_fc"],
                "expected_delivery_date": str(item["expected_delivery_date"])[:10] if item.get("expected_delivery_date") else "",
                "po_status": item["po_status"],
                "carrier_nm": item.get("carrier_nm") or "",
                "wm_week": item.get("wm_week") or 0,
                "l3_category": item.get("l3_category") or "N/A",
                "total_units": 0,
                "total_gmv": 0.0,
                "is_hero": False,
                "items": []
            }
        
        # Aggregate
        po_data[po_num]["total_units"] += units
        po_data[po_num]["total_gmv"] += item_gmv
        
        po_data[po_num]["items"].append({
            "item_id": item["item_id"],
            "units": units,
            "gmv": item_gmv
        })
    
    # Separate flippable vs non-flippable
    flippable = list(po_data.values())
    found_pos = set(po_data.keys())
    non_flippable = [po for po in po_numbers if po not in found_pos]
    
    return {
        "flippable": flippable,
        "non_flippable": non_flippable,
        "items": items
    }
