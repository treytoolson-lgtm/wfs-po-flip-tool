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


def _is_ltl(row: dict) -> bool:
    val = row.get("IS_LTL")
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in {"true", "1", "yes"}


def analyze_escalation(rows: list[dict]) -> EscalationResult:
    """
    Apply spec escalation logic across all item rows for a PO.
    Returns a single verdict with per-item breakdown.
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

    # Shared signals (same for all rows on same delivery)
    sample = rows[0]
    arrived = _has_arrived(sample)
    ltl = _is_ltl(sample)

    # Immediate disqualifiers
    if ltl:
        return EscalationResult(
            verdict="DO_NOT_ESCALATE",
            reasons=["Small parcel / LTL shipment — per SOP do not escalate."],
            at_risk_items=[],
            borderline_items=[],
        )

    if not arrived:
        return EscalationResult(
            verdict="DO_NOT_ESCALATE",
            reasons=[f"Trailer not yet arrived (status: {sample.get('DELIVERY_STATUS')}). Cannot escalate."],
            at_risk_items=[],
            borderline_items=[],
        )

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
    else:
        reasons.append("No Hero/Mosaic items at OOS risk. Stock levels appear healthy.")
        verdict = "DO_NOT_ESCALATE"

    return EscalationResult(
        verdict=verdict,
        reasons=reasons,
        at_risk_items=at_risk,
        borderline_items=borderline,
    )
