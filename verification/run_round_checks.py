#!/usr/bin/env python3
"""Run one review round's verification sequence, in the order that works.

Every one of the four W3 review rounds recorded the same mechanical failure
(``docs/evidence/w3-review-rounds.jsonl``): the suite was run, it failed on a
stale reachability manifest and stale verification packets, those were
regenerated, and the suite was run again. Four for four, same cause, no
judgment involved -- which makes it orchestration work rather than engineering
work, and the first thing an orchestrator should own.

The fix is only the ordering. Derived artifacts are refreshed *before* the
suite, never in response to its failure:

    1. reachability manifest  -- recompute each module's symbol list and local
                                import edges and patch them in place
    2. verification packets   -- regenerate all seven action-resource histories
    3. suite                  -- run twice, because a single green run has
                                never been the bar in this repository
    4. wheel                  -- uv build --wheel

Ordering is the whole point, so the steps are not individually selectable.
``--dry-run`` reports what is stale without changing anything, which is what a
read-only reviewer wants; ``--skip-wheel`` exists only because the wheel build
is the slowest step and adds nothing when iterating on tests.

Exit status is 0 only if every step passed. A step that changed a derived
artifact is reported but is not itself a failure -- that is the sequence doing
its job.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs/contracts/tranche4-reachability-v1.json"
CONTRACT_TEST = ROOT / "tests/test_tranche4_reachability_contract.py"
HISTORIES = (
    "pruning", "snapshot-race", "non-disclosure", "redaction",
    "response-loss", "bounded-wait", "fencing",
)


def _contract_module():
    """Load the reachability contract test as a module for its helpers.

    Importing the test file is deliberate: its ``_python_symbols`` and
    ``_local_import_edges`` are the definitions the gate itself uses, so
    recomputing them any other way would let the two drift -- which is the
    class of bug this whole sequence exists to stop.
    """
    spec = importlib.util.spec_from_file_location("_reachability_contract", CONTRACT_TEST)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _patch_manifest(*, dry_run: bool) -> tuple[bool, list[str]]:
    """Bring the manifest's symbol lists and import edges back in line.

    Patches surgically rather than re-serialising: the file mixes indented and
    single-line JSON objects, and a wholesale ``json.dumps`` reflows all ~1700
    lines into an unreviewable diff.
    """
    contract = _contract_module()
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    raw = MANIFEST.read_text(encoding="utf-8")
    notes: list[str] = []
    changed = False

    observed = contract._python_symbols()
    for entry in manifest["python_surfaces"]:
        current = entry["symbols"]
        wanted = observed.get(entry["path"])
        if wanted is None or wanted == current:
            continue
        old = json.dumps(current, ensure_ascii=False)
        new = json.dumps(wanted, ensure_ascii=False)
        needle = f'      "symbols": {old},\n'
        if raw.count(needle) != 1:
            notes.append(f"! {entry['path']}: symbol list changed but could not be patched in place")
            continue
        raw = raw.replace(needle, f'      "symbols": {new},\n')
        notes.append(f"~ {entry['path']}: symbol list updated")
        changed = True

    missing = sorted(contract._local_import_edges() - contract._flatten(manifest["import_groups"], "edges"))
    extra = sorted(contract._flatten(manifest["import_groups"], "edges") - contract._local_import_edges())
    for edge in missing:
        notes.append(f"! unpinned import edge, add it to an import group by hand: {edge}")
    for edge in extra:
        notes.append(f"! manifest pins an import edge that no longer exists: {edge}")

    unpinned = sorted(set(observed) - {entry["path"] for entry in manifest["python_surfaces"]})
    for path in unpinned:
        notes.append(f"! module has no python_surfaces entry, add one by hand: {path}")

    if changed and not dry_run:
        MANIFEST.write_text(raw, encoding="utf-8")
    # Import edges and new modules need a disposition, an owner and a
    # falsifying test written by a person; refusing to invent those is the
    # point of reporting them rather than patching them.
    blocked = any(note.startswith("!") for note in notes)
    return not blocked, notes


def _run(command: list[str], *, label: str) -> bool:
    print(f"--- {label}", flush=True)
    completed = subprocess.run(command, cwd=ROOT)
    return completed.returncode == 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true",
                        help="report stale derived artifacts without changing anything")
    parser.add_argument("--skip-wheel", action="store_true",
                        help="skip the wheel build when iterating on tests")
    parser.add_argument("--pytest", default="uv run --no-sync pytest",
                        help="pytest invocation (default: %(default)s)")
    arguments = parser.parse_args(argv)

    ok, notes = _patch_manifest(dry_run=arguments.dry_run)
    print("--- reachability manifest")
    for note in notes or ["= already in line"]:
        print(f"  {note}")
    if not ok:
        print("\nFAILED: the manifest needs a hand edit that carries a disposition and an owner.")
        return 1

    if arguments.dry_run:
        stale = [note for note in notes if note.startswith("~")]
        print(f"\ndry run: {len(stale)} derived artifact(s) would be updated; packets not checked")
        return 0

    for history in HISTORIES:
        if not _run([sys.executable, "verification/run_action_resource_history.py", history],
                    label=f"verification packet: {history}"):
            print(f"\nFAILED: verification packet {history} could not be regenerated.")
            return 1

    suite = arguments.pytest.split() + ["-q"]
    for attempt in (1, 2):
        if not _run(suite, label=f"suite (run {attempt} of 2)"):
            print(f"\nFAILED: suite run {attempt}.")
            return 1

    if not arguments.skip_wheel and not _run(["uv", "build", "--wheel"], label="wheel"):
        print("\nFAILED: wheel build.")
        return 1

    print("\nOK: derived artifacts refreshed before the suite, suite green twice"
          + ("" if arguments.skip_wheel else ", wheel clean"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
