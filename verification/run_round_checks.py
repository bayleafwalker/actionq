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
import os
import shutil
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
ROUND_RECORDS = ROOT / "docs/evidence/w3-review-rounds.jsonl"


def _untracked_files() -> list[str]:
    """Files git has never seen, which several gates cannot see either.

    ``_tracked_files`` in the reachability contract reads ``git ls-files``, so
    anything unstaged is invisible to the very checks that exist to pin it --
    the suite passes locally and fails in CI the moment it is committed.

    This was first written to watch ``.py`` files, having been prompted by an
    unstaged test module. It missed the next one by exactly one file extension:
    the repository consumer scan classifies documents too, so an unstaged plan
    under ``docs/`` failed CI the same way. Gitignored paths are already
    excluded by ``--exclude-standard``, so anything this reports is a file the
    repository is genuinely supposed to know about.
    """
    completed = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard"],
        cwd=ROOT, capture_output=True, text=True,
    )
    return completed.stdout.split()


def _require_postgres() -> str | None:
    """The suite and the packets both need initdb/pg_ctl on PATH.

    Checked up front rather than left to fail seven packets in: on this machine
    they come from a nix store path that is not on a fresh shell's PATH, so the
    first thing a new session sees would otherwise be a confusing packet
    failure. Reports the missing binaries and how to get them instead.
    """
    missing = [name for name in ("initdb", "pg_ctl") if shutil.which(name) is None]
    if not missing:
        return None
    return (
        f"PostgreSQL binaries not on PATH: {', '.join(missing)}.\n"
        "  Put them there before running, e.g.\n"
        "    export PATH=\"$(dirname $(nix build --no-link --print-out-paths "
        "nixpkgs#postgresql)/bin/initdb):$PATH\"\n"
        "  or set ACTIONQ_PG_BIN to a directory containing initdb and pg_ctl."
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


def _append_round_record(round_number: int, *, skipped_wheel: bool) -> None:
    """Emit the round's mechanical-failure count from the thing that made it zero.

    Only ever reached after every step above has passed, so the count is an
    observation rather than an assertion: this sequence cannot reach here having
    run the suite against stale derived artifacts. The finding counts and
    classification stay hand-written -- those are judgment, and an orchestrator
    that filled them in would be deciding rather than recording.
    """
    head = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT,
                          capture_output=True, text=True).stdout.strip()
    record = {
        "round": round_number,
        "fix_head": head,
        "mechanical_failure_rounds": 0,
        "mechanical_failure_cause": None,
        "emitted_by": "verification/run_round_checks.py",
        "wheel_built": not skipped_wheel,
        "note": "counts, classification and channel findings are added by hand; "
                "this line records only what the sequence itself observed",
    }
    with ROUND_RECORDS.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")


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
    parser.add_argument("--record-round", type=int, metavar="N",
                        help="append a round record for round N, with mechanical_failures 0")
    arguments = parser.parse_args(argv)

    pg_bin = os.environ.get("ACTIONQ_PG_BIN")
    if pg_bin:
        os.environ["PATH"] = f"{pg_bin}{os.pathsep}{os.environ.get('PATH', '')}"
    if not arguments.dry_run:
        # Only the real run needs a database; --dry-run inspects files alone.
        complaint = _require_postgres()
        if complaint:
            print(f"FAILED: {complaint}")
            return 1

    untracked = _untracked_files()
    if untracked:
        print("--- untracked files")
        for path in untracked:
            print(f"  ! {path}: git ls-files cannot see this, and neither can the "
                  "reachability and retirement contracts")
        print("\nFAILED: stage new files before running the round.")
        return 1

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

    if arguments.record_round is not None:
        _append_round_record(arguments.record_round, skipped_wheel=arguments.skip_wheel)
        print(f"--- round record appended for round {arguments.record_round}")

    print("\nOK: derived artifacts refreshed before the suite, suite green twice"
          + ("" if arguments.skip_wheel else ", wheel clean"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
