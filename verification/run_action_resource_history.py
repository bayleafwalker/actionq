#!/usr/bin/env python3
"""Run one exact owner history and capture its runtime assertion receipt."""
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
NODES = {
    "pruning": "test_action_resource_history_pruning_none_some_all_restart",
    "snapshot-race": "test_action_resource_history_threaded_concurrent_terminal_handoff",
    "non-disclosure": "test_action_resource_history_four_case_application_not_found_bytes",
    "redaction": "test_action_resource_history_recursive_six_class_redaction_canaries",
    "response-loss": "test_action_resource_history_commit_response_loss_original_retry",
    "bounded-wait": "test_action_resource_history_wait_zero_thirty_early_spurious_disconnect_restart",
    "legacy-quarantine": "test_action_resource_history_legacy_raw_64_case_preaccess_quarantine",
    "fencing": "test_action_resource_history_two_incarnation_stale_session_fencing",
}
def observed_postgres_version() -> str:
    """Version of the PostgreSQL this regeneration will actually run against.

    Read, never carried forward. Regeneration used to re-read the existing result
    packet and overwrite only implementation_sha and the receipt, so every other
    field -- including environment.postgres_version -- survived verbatim. A
    refresh on a different server therefore kept the old version string and
    silently misattributed the evidence, which is the one thing a verification
    packet must not do.

    Resolved the same way tests/conftest.py resolves it (`shutil.which`), so this
    reports the binary the run will really use rather than a plausible guess.
    """
    initdb = shutil.which("initdb")
    if initdb is None:
        raise SystemExit(
            "cannot determine the PostgreSQL version: initdb is not on PATH. "
            "Refusing to regenerate rather than carry the previous packet's "
            "version forward as if it were observed."
        )
    completed = subprocess.run([initdb, "--version"], capture_output=True, text=True)
    match = re.search(r"(\d+(?:\.\d+)*)", completed.stdout)
    if completed.returncode != 0 or not match:
        raise SystemExit(f"could not parse a version from: {completed.stdout!r}")
    return match.group(1)


def candidate_paths() -> list[str]:
    paths = set()
    for pattern in ("actionq/**/*.py", "actionq/migrations/*.sql", "tests/**/*.py"):
        paths.update(path.relative_to(ROOT).as_posix() for path in ROOT.glob(pattern) if path.is_file())
    paths.update(("verification/run_action_resource_history.py", "verification/validate_action_resource_owner_v1.py"))
    return sorted(paths)

def candidate_digest() -> str:
    digest = hashlib.sha256()
    for relative in candidate_paths():
        digest.update(relative.encode() + b"\0" + (ROOT / relative).read_bytes())
    return digest.hexdigest()

def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in NODES:
        raise SystemExit("usage: run_action_resource_history.py " + "|".join(NODES))
    history = sys.argv[1]
    node = f"tests/test_integration_postgres.py::{NODES[history]}"
    command = [sys.executable, "-m", "pytest", node, "-q", "-s"]
    completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    sys.stdout.write(completed.stdout)
    sys.stderr.write(completed.stderr)
    assertion_id = f"action-resource-owner.{history}.v1"
    records = sorted(set(
        line for line in completed.stdout.splitlines()
        if line.startswith("OWNER_ASSERTION ")
    ))
    expected_record = f"OWNER_ASSERTION {assertion_id}"
    if completed.returncode != 0 or records != [expected_record]:
        return completed.returncode or 1
    assertion_stream = "".join(record + "\n" for record in records)
    receipt = {
        "schema_version": "action-resource-history-receipt/v1",
        "history": history,
        "node": node,
        "assertion_ids": [assertion_id],
        "candidate_tree_digest": candidate_digest(),
        "command": [".venv/bin/python3", "-m", "pytest", node, "-q", "-s"],
        "returncode": completed.returncode,
        "assertion_stream": assertion_stream,
        "assertion_stream_sha256": hashlib.sha256(assertion_stream.encode()).hexdigest(),
    }
    target = ROOT / "verification/history-receipts" / f"{history}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    result_path = ROOT / "verification/results" / f"action-resource-owner-{history}.json"
    result = json.loads(result_path.read_text(encoding="utf-8"))
    result["implementation_sha"] = f"candidate-tree:{receipt['candidate_tree_digest']}"
    # Observed, not inherited -- see observed_postgres_version().
    result.setdefault("environment", {})["postgres_version"] = observed_postgres_version()
    result["execution"]["receipt"] = {
        "path": target.relative_to(ROOT).as_posix(),
        "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
    }
    result_path.write_text(
        json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
