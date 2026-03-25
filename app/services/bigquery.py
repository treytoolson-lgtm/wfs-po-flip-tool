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

# Master SQL — Fixed 2026-03-25 to prevent cartesian product duplicates
# Validated with PO 6577303WFA: 1,045 units (matches Seller Center)
# Uses CTEs to aggregate independently, then joins to prevent row multiplication
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
    SUM(p.ORDER_QTY)                                AS TOTAL_UNITS,
    ROUND(SUM(p.ORDER_QTY * p.CURR_ITEM_PRICE), 2)  AS GMV_IMPACT
  FROM `wmt-cp-prod.e2e_fmt_cp.ETUP_DELIVERY_PO_LINES` p
  WHERE p.PO_NUM IN UNNEST(@po_numbers)
    AND p.INSERT_DT_UTC >= DATE_SUB(CURRENT_DATE(), INTERVAL @date_window_days DAY)
    AND p.PO_OWNER = 'WFS'
  GROUP BY p.PO_NUM, p.DELIVERY_NUMBER, p.DC_NUMBER, p.VENDOR_NUM, p.VENDOR_NAME, p.ITEM_ID, p.ITEM_NAME
),
trailer_info AS (
  -- Get ONE representative trailer per delivery (avoid duplicates)
  SELECT DISTINCT
    DELIVERY_NUMBER,
    DC_NUMBER,
    ANY_VALUE(TRAILER_ID)                     AS TRAILER_ID,
    ANY_VALUE(FC_NAME)                        AS FC_NAME,
    ANY_VALUE(ARRIVAL_TS_LCL)                 AS ARRIVAL_TS_LCL,
    MAX(FINAL_RANKING)                        AS FINAL_RANKING,
    ANY_VALUE(UPPER(DELIVERY_TYPE_CODE))      AS DELIVERY_TYPE,
    ANY_VALUE(DELIVERY_STATUS)                AS DELIVERY_STATUS,
    MAX(IS_ESCALATION_INSTOCK)                AS IS_ESCALATION_INSTOCK,
    MAX(ESCALATION_EVENT_WINDOW)              AS ESCALATION_EVENT_WINDOW,
    ANY_VALUE(ESCALATION_TRAILER_REASON)      AS ESCALATION_TRAILER_REASON,
    ANY_VALUE(ESCALATION_PO_REASON)           AS ESCALATION_PO_REASON
  FROM `wmt-cp-prod.e2e_fmt_cp.ETUP_DC_TRAILERS`
  GROUP BY DELIVERY_NUMBER, DC_NUMBER
),
scheduler_info AS (
  -- Get scheduling metadata (ONE row per PO+Delivery+DC)
  SELECT DISTINCT
    PO_NUMBER,
    DELIVERY_NUMBER,
    CAST(DC_NUMBER AS STRING)                 AS DC_NUMBER,
    ANY_VALUE(LOAD_TYPE_NAME)                 AS LOAD_TYPE_NAME,
    ANY_VALUE(APPOINTMENT_DATE)               AS APPOINTMENT_DATE,
    ANY_VALUE(WM_YR_WK_NBR)                   AS WM_YR_WK_NBR,
    MAX(IS_LTL)                               AS IS_LTL,
    ANY_VALUE(INVENTORY_TYPE_NAME)            AS INVENTORY_TYPE_NAME
  FROM `wmt-wfs-analytics.wfs_ops_analytics.mat_inbound_scheduler_base`
  WHERE wfs_ind = 1
  GROUP BY PO_NUMBER, DELIVERY_NUMBER, DC_NUMBER
),
events_info AS (
  -- Get event/hero flags per item (deduped)
  SELECT DISTINCT
    CATLG_ITEM_ID,
    PRTNR_ORG_CD,
    MAX(RPT_HRCHY_LVL_3_DESC)                 AS L3_CATEGORY,
    MAX(HERO_ITEM_IND)                        AS IS_HERO_ITEM,
    MAX(MOSC_ITEM_IND)                        AS IS_MOSAIC_ITEM,
    MAX(MINI_EVENT_NM)                        AS EVENT_NAME,
    MAX(EVENT_ITEM_STATUS_NM)                 AS EVENT_STATUS
  FROM `wmt-wfs-analytics.inv_ana.events_item_list`
  GROUP BY CATLG_ITEM_ID, PRTNR_ORG_CD
),
isr_info AS (
  -- Get ISR inventory metrics per item (deduped)
  SELECT DISTINCT
    catlg_item_id,
    prtnr_id,
    MAX(WOS_fcst)                             AS WEEKS_OF_SUPPLY,
    MAX(hero_flag)                            AS HERO_FLAG_ISR,
    MAX(ats_qty_current_wk)                   AS ATS_QTY
  FROM `wmt-wfs-analytics.inv_ana.Inv_ISR_Forward_Looking_copy`
  GROUP BY catlg_item_id, prtnr_id
),
am_info AS (
  -- Get AM info per seller (deduped)
  SELECT DISTINCT
    partner_id,
    ANY_VALUE(am_name)                        AS AM_NAME,
    ANY_VALUE(wfs_am_email)                   AS AM_EMAIL
  FROM `wmt-wfs-analytics.wfs_ops_analytics.mat_mpoa_seller_mart`
  GROUP BY partner_id
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
  e.L3_CATEGORY,
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
  AND CAST(pb.DC_NUMBER AS INT64) = CAST(s.DC_NUMBER AS INT64)
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


def _get_client() -> bigquery.Client:
    settings = get_settings()
    settings.configure_gcloud_path()
    creds, project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
    )
    return bigquery.Client(project=settings.BQ_PROJECT_ANALYTICS, credentials=creds)


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
