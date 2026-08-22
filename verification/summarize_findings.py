#!/usr/bin/env python3
"""Compute the review measurement track's baseline from the per-finding records.

Reports what the records actually support and refuses to imply more. In
particular it always prints the *measurement coverage* of the first-attempt
rate alongside the rate itself, because a rate computed from a handful of
measured findings out of dozens is not a baseline and should not read like one.

    python verification/summarize_findings.py            # human readable
    python verification/summarize_findings.py --json     # machine readable
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/evidence"
# Below this, a first-attempt rate describes too few findings to compare a tier
# against and is reported as not yet established.
COMPARABLE_COVERAGE = 0.60


def _records() -> list[dict]:
    found: list[dict] = []
    for path in sorted(EVIDENCE.glob("*-finding-records.jsonl")):
        found.extend(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    return found


def _rate(records: list[dict], field: str) -> tuple[float | None, int, int]:
    measured = [record for record in records if record[field] is not None]
    if not measured:
        return None, 0, len(records)
    passed = sum(1 for record in measured if record[field])
    return passed / len(measured), len(measured), len(records)


def summarize() -> dict:
    records = _records()
    by_tier: dict[str, dict] = {}
    for tier in sorted({record["implemented_by"] for record in records if record["implemented_by"]}):
        selected = [record for record in records if record["implemented_by"] == tier]
        eligible = [record for record in selected if record["routed_to"] == "implement"]
        rate, measured, total = _rate(eligible, "first_attempt_pass")
        collateral, collateral_measured, _ = _rate(eligible, "collateral_breakage")
        coverage = measured / total if total else 0.0
        by_tier[tier] = {
            "implement_eligible_findings": total,
            "first_attempt_pass_rate": rate,
            "first_attempt_measured": measured,
            "measurement_coverage": round(coverage, 3),
            "comparable": coverage >= COMPARABLE_COVERAGE and measured > 0,
            "collateral_breakage_rate": collateral,
            "collateral_measured": collateral_measured,
        }
    return {
        "records": len(records),
        "classification": dict(Counter(record["classification"] for record in records)),
        "channel": dict(Counter(record["channel"] for record in records)),
        "caught_by": dict(Counter(record["caught_by"] for record in records)),
        "routed_to": dict(Counter(record["routed_to"] for record in records)),
        "by_tier": by_tier,
        "comparable_baseline": any(tier["comparable"] for tier in by_tier.values()),
    }


def _render(summary: dict) -> str:
    lines = [f"findings recorded: {summary['records']}"]
    for label in ("classification", "routed_to", "channel", "caught_by"):
        counts = ", ".join(f"{key}={value}" for key, value in sorted(summary[label].items()))
        lines.append(f"  {label:<15} {counts}")
    lines.append("")
    for tier, data in summary["by_tier"].items():
        lines.append(f"tier {tier!r} (implement-eligible findings: {data['implement_eligible_findings']})")
        if data["first_attempt_pass_rate"] is None:
            lines.append("  first-attempt pass rate: not measured")
        else:
            lines.append(
                f"  first-attempt pass rate: {data['first_attempt_pass_rate']:.2f} "
                f"over {data['first_attempt_measured']} measured "
                f"(coverage {data['measurement_coverage']:.0%})"
            )
        lines.append(f"  usable as a baseline: {'yes' if data['comparable'] else 'no'}")
    lines.append("")
    if summary["comparable_baseline"]:
        lines.append("A comparable baseline exists; a second tier can be measured against it.")
    else:
        lines.append(
            "No comparable baseline yet. The recorded rounds never separated diagnosis from\n"
            "implementation and landed fixes in per-round batches, so first-attempt data is\n"
            "unrecoverable for them. Capture it going forward, one finding at a time, before\n"
            "swapping the implement tier."
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--json", action="store_true", help="emit the summary as JSON")
    arguments = parser.parse_args(argv)
    summary = summarize()
    print(json.dumps(summary, indent=2) if arguments.json else _render(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
