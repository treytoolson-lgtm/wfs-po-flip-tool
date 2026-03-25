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

# Master SQL — validated 2026-03-25 with PO 6577303WFA
# Partition pruned on INSERT_DT_UTC (180 day window)
# Join 3 tightened: added VENDOR_NUM = PRTNR_ORG_CD
MASTER_QUERY = """
SELECT
  p.PO_NUM,
  p.DELIVERY_NUMBER,
  p.DC_NUMBER,
  p.VENDOR_NUM                                    AS PID,
  p.VENDOR_NAME                                   AS SELLER_NAME,
  m.am_name                                       AS AM_NAME,
  m.wfs_am_email                                  AS AM_EMAIL,
  p.ITEM_ID,
  p.ITEM_NAME,
  MAX(e.RPT_HRCHY_LVL_3_DESC)                     AS L3_CATEGORY,
  SUM(p.ORDER_QTY)                                AS TOTAL_UNITS,
  ROUND(SUM(p.ORDER_QTY * p.CURR_ITEM_PRICE), 2)  AS GMV_IMPACT,
  t.TRAILER_ID,
  t.FC_NAME,
  t.ARRIVAL_TS_LCL,
  t.FINAL_RANKING,
  UPPER(t.DELIVERY_TYPE_CODE)                     AS DELIVERY_TYPE,
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
  MAX(e.HERO_ITEM_IND)                            AS IS_HERO_ITEM,
  MAX(e.MOSC_ITEM_IND)                            AS IS_MOSAIC_ITEM,
  MAX(e.MINI_EVENT_NM)                            AS EVENT_NAME,
  MAX(e.EVENT_ITEM_STATUS_NM)                     AS EVENT_STATUS,
  MAX(i.WOS_fcst)                                 AS WEEKS_OF_SUPPLY,
  MAX(i.hero_flag)                                AS HERO_FLAG_ISR,
  MAX(i.ats_qty_current_wk)                       AS ATS_QTY
FROM `wmt-cp-prod.e2e_fmt_cp.ETUP_DELIVERY_PO_LINES` p
LEFT JOIN `wmt-cp-prod.e2e_fmt_cp.ETUP_DC_TRAILERS` t
  ON  p.DELIVERY_NUMBER = t.DELIVERY_NUMBER
  AND p.DC_NUMBER       = t.DC_NUMBER
LEFT JOIN `wmt-wfs-analytics.wfs_ops_analytics.mat_inbound_scheduler_base` s
  ON  p.DELIVERY_NUMBER          = s.DELIVERY_NUMBER
  AND CAST(p.DC_NUMBER AS INT64) = CAST(s.DC_NUMBER AS INT64)
  AND p.PO_NUM                   = s.PO_NUMBER
  AND s.wfs_ind                  = 1
LEFT JOIN `wmt-wfs-analytics.inv_ana.events_item_list` e
  ON  p.ITEM_ID    = e.CATLG_ITEM_ID
  AND p.VENDOR_NUM = e.PRTNR_ORG_CD
LEFT JOIN `wmt-wfs-analytics.inv_ana.Inv_ISR_Forward_Looking_copy` i
  ON  CAST(p.ITEM_ID AS STRING) = i.catlg_item_id
  AND p.VENDOR_NUM              = i.prtnr_id
LEFT JOIN `wmt-wfs-analytics.wfs_ops_analytics.mat_mpoa_seller_mart` m
  ON p.VENDOR_NUM = m.partner_id
WHERE p.PO_NUM IN UNNEST(@po_numbers)
  AND p.INSERT_DT_UTC >= DATE_SUB(CURRENT_DATE(), INTERVAL @date_window_days DAY)
  AND p.PO_OWNER = 'WFS'
GROUP BY
  p.PO_NUM, p.DELIVERY_NUMBER, p.DC_NUMBER,
  p.VENDOR_NUM, p.VENDOR_NAME,
  m.am_name, m.wfs_am_email,
  p.ITEM_ID, p.ITEM_NAME,
  t.TRAILER_ID, t.FC_NAME, t.ARRIVAL_TS_LCL,
  t.FINAL_RANKING, t.DELIVERY_TYPE_CODE, t.DELIVERY_STATUS,
  t.IS_ESCALATION_INSTOCK, t.ESCALATION_EVENT_WINDOW,
  t.ESCALATION_TRAILER_REASON, t.ESCALATION_PO_REASON,
  s.LOAD_TYPE_NAME, s.APPOINTMENT_DATE, s.WM_YR_WK_NBR,
  s.IS_LTL, s.INVENTORY_TYPE_NAME
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
