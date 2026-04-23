#!/usr/bin/env python3
"""Benchmark live BigQuery query helpers for Escalation and PO Flip.

Examples:
  uv run --python .venv/bin/python python scripts/benchmark_queries.py \
    --escalation-po 6577303WFA \
    --flip-po 6840158WFA

  uv run --python .venv/bin/python python scripts/benchmark_queries.py \
    --escalation-po 6577303WFA \
    --compare-ref c76f323

This script can optionally compare the current working tree implementation of
app/services/bigquery.py against a prior git ref.
"""
from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services import bigquery as current_bigquery  # noqa: E402


def _load_module_from_path(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_module_from_git_ref(git_ref: str):
    result = subprocess.run(
        ["git", "show", f"{git_ref}:app/services/bigquery.py"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    with tempfile.NamedTemporaryFile("w", suffix="_bigquery.py", delete=False) as temp_file:
        temp_file.write(result.stdout)
        temp_path = Path(temp_file.name)
    return _load_module_from_path(f"bigquery_{git_ref.replace('/', '_')}", temp_path)


def _time_call(label: str, func: Callable[..., Any], *args) -> tuple[Any, float]:
    start = time.perf_counter()
    result = func(*args)
    elapsed = time.perf_counter() - start
    print(f"{label}: {elapsed:.2f}s")
    return result, elapsed


def _summarize_escalation(rows: list[dict[str, Any]]) -> dict[str, Any]:
    po_numbers = sorted({row.get("PO_NUM") for row in rows})
    return {
        "rows": len(rows),
        "po_numbers": po_numbers,
    }


def _summarize_flip(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "items": len(result.get("items", [])),
        "flippable": len(result.get("flippable", [])),
        "non_flippable": len(result.get("non_flippable", [])),
    }


def _benchmark_current(escalation_pos: list[str], flip_pos: list[str]) -> None:
    print("\n=== Current implementation ===")
    if escalation_pos:
        rows, _ = _time_call(
            f"Escalation {','.join(escalation_pos)}",
            current_bigquery.query_po_numbers,
            escalation_pos,
        )
        print(_summarize_escalation(rows))
    if flip_pos:
        result, _ = _time_call(
            f"PO Flip {','.join(flip_pos)}",
            current_bigquery.query_flip_pos,
            flip_pos,
        )
        print(_summarize_flip(result))


def _benchmark_compare(git_ref: str, escalation_pos: list[str], flip_pos: list[str]) -> None:
    print(f"\n=== Comparing current implementation vs {git_ref} ===")
    baseline = _load_module_from_git_ref(git_ref)

    if escalation_pos:
        _, baseline_elapsed = _time_call(
            f"Baseline escalation {','.join(escalation_pos)}",
            baseline.query_po_numbers,
            escalation_pos,
        )
        _, current_elapsed = _time_call(
            f"Current escalation {','.join(escalation_pos)}",
            current_bigquery.query_po_numbers,
            escalation_pos,
        )
        print({
            "query": "escalation",
            "baseline_ref": git_ref,
            "delta_seconds": round(baseline_elapsed - current_elapsed, 2),
            "speedup_pct": round(((baseline_elapsed - current_elapsed) / baseline_elapsed) * 100, 1)
            if baseline_elapsed
            else None,
        })

    if flip_pos:
        _, baseline_elapsed = _time_call(
            f"Baseline flip {','.join(flip_pos)}",
            baseline.query_flip_pos,
            flip_pos,
        )
        _, current_elapsed = _time_call(
            f"Current flip {','.join(flip_pos)}",
            current_bigquery.query_flip_pos,
            flip_pos,
        )
        print({
            "query": "flip",
            "baseline_ref": git_ref,
            "delta_seconds": round(baseline_elapsed - current_elapsed, 2),
            "speedup_pct": round(((baseline_elapsed - current_elapsed) / baseline_elapsed) * 100, 1)
            if baseline_elapsed
            else None,
        })


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark WFS query helpers")
    parser.add_argument(
        "--escalation-po",
        action="append",
        default=[],
        help="PO to benchmark with query_po_numbers. Can be passed multiple times.",
    )
    parser.add_argument(
        "--flip-po",
        action="append",
        default=[],
        help="PO to benchmark with query_flip_pos. Can be passed multiple times.",
    )
    parser.add_argument(
        "--compare-ref",
        help="Optional git ref to compare against, like HEAD~1 or a commit SHA.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.escalation_po and not args.flip_po:
        print("Pass at least one --escalation-po or --flip-po.")
        return 2

    _benchmark_current(args.escalation_po, args.flip_po)
    if args.compare_ref:
        _benchmark_compare(args.compare_ref, args.escalation_po, args.flip_po)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
