from __future__ import annotations
from dataclasses import dataclass
from typing import Any

from config import get_settings


@dataclass
class EscalationResult:
    verdict: str          # ESCALATE | BORDERLINE | DO_NOT_ESCALATE
    reasons: list[str]
    at_risk_items: list[dict]
    borderline_items: list[dict]


def _safe_float(val: Any, sentinel: float = 0.0) -> float:
    """Coerce BQ value to float, treating None or 100000 sentinel as sentinel."""
    try:
        f = float(val)
        return sentinel if f >= 99999 else f
    except (TypeError, ValueError):
        return sentinel


def _is_hero(row: dict) -> bool:
    """Hero if flagged in events_item_list OR ISR — spec uses both signals."""
    events_flag = int(row.get("IS_HERO_ITEM") or 0) == 1
    isr_flag = str(row.get("HERO_FLAG_ISR") or "").strip().lower() == "yes"
    return events_flag or isr_flag


def _is_mosaic(row: dict) -> bool:
    return int(row.get("IS_MOSAIC_ITEM") or 0) == 1


def _has_arrived(row: dict) -> bool:
    arrived_statuses = {"ARV", "ARRIVED", "WRK", "WORKING", "OPN"}
    status = str(row.get("DELIVERY_STATUS") or "").strip().upper()
    return status in arrived_statuses


def _is_its(row: dict) -> bool:
    """ITS (In-Transit Shipment) — detected via INVENTORY_TYPE_NAME or LOAD_TYPE_NAME."""
    inv_type = str(row.get("INVENTORY_TYPE_NAME") or "").strip().upper()
    load_type = str(row.get("LOAD_TYPE_NAME") or "").strip().upper()
    return "ITS" in inv_type or "ITS" in load_type


def _is_checked_in(row: dict) -> bool:
    """ITS is 'checked in' once it reaches an arrived/working status at the FC."""
    return _has_arrived(row)


def _load_type_label(row: dict) -> str:
    """Human-readable shipment type for display in reasons."""
    load = str(row.get("LOAD_TYPE_NAME") or "").strip()
    inv  = str(row.get("INVENTORY_TYPE_NAME") or "").strip()
    return load or inv or "Unknown"


def analyze_escalation(
    rows: list[dict],
    fc_status: dict | None = None,
) -> EscalationResult:
    """
    Escalation criteria (per SOP):
      Escalate Hero/Mosaic OOS-risk items for:
        - FTL / Full Truck / Container — when arrived at FC
        - LTL / Limited Truck / Container — when arrived at FC
        - ITS Shipments — ONLY when NOT yet checked in
      Do NOT escalate:
        - ITS already checked in (already being worked)
        - Non-hero/non-mosaic with healthy stock

    fc_status: optional FC congestion row from capacity_service.get_fc_status().
      When provided and FC is High-congestion with days_to_clear > 3:
        - BORDERLINE → ESCALATE (congestion makes the delay materially worse)
      When fc_status is None (cache cold etc.) verdict is unchanged — never errors.
    """
    settings = get_settings()
    threshold = settings.WOS_THRESHOLD

    at_risk: list[dict] = []
    borderline: list[dict] = []
    reasons: list[str] = []

    if not rows:
        return EscalationResult(
            verdict="DO_NOT_ESCALATE",
            reasons=["No data found for this PO."],
            at_risk_items=[],
            borderline_items=[],
        )

    # Shared delivery signals (same for all rows on the same delivery)
    sample = rows[0]
    its = _is_its(sample)
    arrived = _has_arrived(sample)
    load_label = _load_type_label(sample)

    # Gate check: is this shipment in an escalatable state?
    if its:
        # ITS: escalate only when NOT yet checked in
        if _is_checked_in(sample):
            return EscalationResult(
                verdict="DO_NOT_ESCALATE",
                reasons=[f"ITS shipment already checked in (status: {sample.get('DELIVERY_STATUS')}) — no escalation needed."],
                at_risk_items=[],
                borderline_items=[],
            )
        reasons.append(f"ITS shipment not yet checked in (status: {sample.get('DELIVERY_STATUS')}) — eligible for escalation.")
    else:
        # FTL / LTL / LCL: escalate only when arrived
        if not arrived:
            return EscalationResult(
                verdict="DO_NOT_ESCALATE",
                reasons=[f"{load_label} not yet arrived (status: {sample.get('DELIVERY_STATUS')}). Cannot escalate."],
                at_risk_items=[],
                borderline_items=[],
            )
        reasons.append(f"{load_label} arrived at FC (status: {sample.get('DELIVERY_STATUS')}) — eligible for escalation.")

    for row in rows:
        hero = _is_hero(row)
        mosaic = _is_mosaic(row)
        wos = _safe_float(row.get("WEEKS_OF_SUPPLY"), sentinel=999.0)
        oos_flag = int(row.get("IS_ESCALATION_INSTOCK") or 0) == 1

        item_label = f"{row.get('ITEM_NAME', 'Unknown')[:60]} (WOS: {wos:.2f})"

        if (hero or mosaic) and (oos_flag or wos < threshold):
            at_risk.append({**row, "_wos": wos, "_hero": hero, "_mosaic": mosaic})
        elif (hero or mosaic) and threshold <= wos < threshold + 0.5:
            borderline.append({**row, "_wos": wos, "_hero": hero, "_mosaic": mosaic})
        elif not (hero or mosaic) and wos < threshold:
            # OOS signal but non-hero/mosaic — borderline per spec
            borderline.append({**row, "_wos": wos, "_hero": hero, "_mosaic": mosaic})

    if at_risk:
        reasons.append(f"{len(at_risk)} Hero/Mosaic item(s) at OOS risk (WOS < {threshold}).")
        if sample.get("ESCALATION_EVENT_WINDOW"):
            reasons.append(f"Event window: {sample['ESCALATION_EVENT_WINDOW']}")
        verdict = "ESCALATE"
    elif borderline:
        reasons.append(f"{len(borderline)} item(s) borderline — AM judgment required.")
        verdict = "BORDERLINE"
        # FC congestion upgrade: if the FC is severely backed up, borderline becomes escalate
        if fc_status and fc_status.get("status") == "High":
            dtc = fc_status.get("days_to_clear") or 0
            dwell = fc_status.get("wfs_avg_dwell_hours") or 0
            if dtc > 3 or dwell > 24:
                verdict = "ESCALATE"
                reasons.append(
                    f"⚠️ FC congestion upgrade: {fc_status['fc_name']} is HIGH — "
                    f"{dtc:.1f} days to clear yard at current velocity "
                    f"(avg WFS dwell {dwell:.0f}h). Borderline elevated to ESCALATE."
                )
    else:
        reasons.append("No Hero/Mosaic items at OOS risk. Stock levels appear healthy.")
        verdict = "DO_NOT_ESCALATE"

    return EscalationResult(
        verdict=verdict,
        reasons=reasons,
        at_risk_items=at_risk,
        borderline_items=borderline,
    )
