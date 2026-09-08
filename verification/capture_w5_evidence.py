#!/usr/bin/env python3
"""W5 evidence-capture tooling (backlog 5.2 row 7).

The repository workflow owns this tooling only. It must never author,
fabricate, or commit an actual evidence record under docs/evidence/ -- that
is the operator's job, on a live target, so that a change and its proof do
not share an author.

Record 1 (repository-wide consumer inventory + machine diff against
docs/contracts/tranche4-reachability-v1.json) is the one record makeable
fully offline today, and this tool implements it completely: it reuses the
exact AST/scan logic proven by
tests/test_tranche4_reachability_contract.py::test_repository_wide_consumers_and_removed_plane_anchors_are_classified
so the two never drift apart, and it produces a real
``w5-repository-check/v1`` envelope (verification/w5_evidence/envelope.py).

Records 2-7 require a live endpoint, real credentials, and/or the actually
installed old ActionQ wheel -- none of which this repository, this sandbox,
or any agent session may supply (freeze doc falsifier: "a repository test
is presented as deployment evidence"). Their capture functions therefore
FAIL CLOSED: given any missing required live input, or absent a real
capture backend wired in by an operator integration, they raise
EvidenceCaptureError and write nothing record-shaped -- never a fixture,
never a source-tree substitute, never a partial file.

Usage:
    uv run --extra dev python verification/capture_w5_evidence.py record-1 [--out PATH] [--live-jobs PATH]
    uv run --extra dev python verification/capture_w5_evidence.py record-N   # N in 2..7; fails closed here
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "docs/contracts/tranche4-reachability-v1.json"

sys.path.insert(0, str(ROOT))

from verification.w5_evidence.envelope import (  # noqa: E402
    CapturedEvidenceRecord,
    RepositoryCheckRecord,
    ToolVersions,
    binding_digest,
    git_branch,
    git_commit,
    local_actor,
    now_iso8601,
)


class EvidenceCaptureError(RuntimeError):
    """Raised when a capture cannot proceed. Callers must write nothing on this."""


def _tool_versions() -> ToolVersions:
    import platform

    return ToolVersions(
        versions={
            "python": platform.python_version(),
            "capture_w5_evidence": "1.0.0",
        }
    )


def _repository_environment() -> dict[str, str]:
    return {
        "git_commit": git_commit(),
        "git_branch": git_branch(),
        "repo_root": str(ROOT),
    }


# ---------------------------------------------------------------------------
# Record 1: offline, source-tree derived. Fully executable now.
# ---------------------------------------------------------------------------


def _reachability_scan_module():
    """Import the reachability test module for its proven scan logic.

    This is a deliberate reuse of tests/test_tranche4_reachability_contract.py
    rather than a re-implementation, so record 1's diff can never silently
    drift from the executable completeness gate it reports on. Requires the
    `dev` extra (pytest) to be installed, same as running the test suite.
    """
    try:
        from tests import test_tranche4_reachability_contract as module
    except ModuleNotFoundError as error:  # pragma: no cover - environment issue
        raise EvidenceCaptureError(
            "record 1 requires the repository's dev extras (pytest) to import "
            "tests/test_tranche4_reachability_contract.py for its reachability "
            f"scan logic; run via `uv run --extra dev ...`. Underlying error: {error}"
        ) from error
    return module


def capture_record_1(
    *,
    live_jobs: Mapping[str, str] | None = None,
) -> RepositoryCheckRecord:
    """Build the record-1 consumer inventory + manifest diff, fully offline.

    ``live_jobs`` is an OPTIONAL mapping of job identifier -> status string,
    supplied by an operator who has a live jobs source. When absent, the
    job-status dimension is marked explicitly unavailable rather than
    inferring status from a job's mere existence in the source tree
    (backlog 5.9: suspended jobs are not runs).
    """
    module = _reachability_scan_module()
    manifest = module._manifest()

    api_scan = manifest["repository_scans"]["api_consumers"]
    retired_scan = manifest["repository_scans"]["retired_plane"]

    manifest_consumers = module._flatten(manifest["repository_consumer_groups"], "paths")
    manifest_retired = module._flatten(manifest["retired_plane_anchor_groups"], "paths")

    observed_consumers = module._scan_paths(api_scan["pattern"])
    observed_retired = module._scan_paths(retired_scan["pattern"])

    consumer_diff = {
        "extra_in_repository": sorted(observed_consumers - manifest_consumers),
        "missing_from_repository": sorted(manifest_consumers - observed_consumers),
    }
    retired_plane_diff = {
        "extra_in_repository": sorted(observed_retired - manifest_retired),
        "missing_from_repository": sorted(manifest_retired - observed_retired),
    }

    if live_jobs is None:
        job_status = {
            "available": False,
            "reason": "no live jobs source provided to this capture run",
        }
    else:
        counts: dict[str, int] = {}
        for status in live_jobs.values():
            counts[status] = counts.get(status, 0) + 1
        job_status = {
            "available": True,
            "job_count": len(live_jobs),
            "status_counts": counts,
        }

    manifest_sha256 = hashlib.sha256(MANIFEST_PATH.read_bytes()).hexdigest()

    payload = {
        "manifest_path": MANIFEST_PATH.relative_to(ROOT).as_posix(),
        "manifest_sha256": manifest_sha256,
        "consumer_scan_pattern": api_scan["pattern"],
        "retired_plane_scan_pattern": retired_scan["pattern"],
        "consumer_diff": consumer_diff,
        "retired_plane_diff": retired_plane_diff,
        "consumer_diff_is_empty": not (consumer_diff["extra_in_repository"] or consumer_diff["missing_from_repository"]),
        "retired_plane_diff_is_empty": not (
            retired_plane_diff["extra_in_repository"] or retired_plane_diff["missing_from_repository"]
        ),
        "job_status": job_status,
    }

    schema_version = "w5-repository-check/v1"
    record_id = "w5-record-1-consumer-inventory-and-manifest-diff"
    captured_at = now_iso8601()
    environment = _repository_environment()
    actor = local_actor()
    tool_versions = _tool_versions()
    result_digest = binding_digest(
        schema_version=schema_version,
        record_id=record_id,
        captured_at=captured_at,
        environment=environment,
        actor=actor,
        tool_versions=tool_versions,
        payload=payload,
    )

    return RepositoryCheckRecord(
        schema_version=schema_version,
        record_id=record_id,
        captured_at=captured_at,
        environment=environment,
        actor=actor,
        tool_versions=tool_versions,
        payload=payload,
        result_digest=result_digest,
    )


# ---------------------------------------------------------------------------
# Records 2-7: live-only. Fail closed with nothing written when the required
# live input(s) are absent, and require an operator-wired capture backend
# even when inputs are present -- this repository ships no such backend, so
# these paths cannot produce a record-shaped object in this environment.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LiveInputRequirement:
    name: str
    description: str


RECORD_REQUIREMENTS: dict[int, tuple[LiveInputRequirement, ...]] = {
    2: (
        LiveInputRequirement("endpoint", "live served-catalog HTTP(S) endpoint"),
        LiveInputRequirement("old_wheel_path", "path to the actually-installed old ActionQ wheel/venv"),
    ),
    3: (
        LiveInputRequirement("database_url", "live PostgreSQL connection string with role-inventory privileges"),
    ),
    4: (
        LiveInputRequirement("database_url", "live PostgreSQL connection string with catalog/grants privileges"),
    ),
    5: (
        LiveInputRequirement("database_url", "live PostgreSQL connection string"),
        LiveInputRequirement(
            "former_runtime_credential",
            "credential of a former runtime role, to attempt (and record the denial of) direct table DML/sequence use",
        ),
    ),
    6: (
        LiveInputRequirement("old_wheel_path", "path to the actually-installed old ActionQ wheel (not a source-tree simulation)"),
        LiveInputRequirement("database_url", "live PostgreSQL connection string"),
    ),
    7: (
        LiveInputRequirement("database_url", "live PostgreSQL connection string"),
        LiveInputRequirement("archive_reader_credential", "credential of the archive-reader role"),
    ),
}

RECORD_DESCRIPTIONS: dict[int, str] = {
    2: "served catalog and installed CLI writer-surface scan proving no supported legacy writer",
    3: "database role and credential inventory, including owner/membership inheritance",
    4: "effective table and sequence grants for execution/archive/federation/ledger objects",
    5: "denial receipts using each former runtime credential for direct table DML and sequence use",
    6: "denial receipts from the actually installed old ActionQ wheel",
    7: "archive-reader receipts proving required redaction and denial of base-table/federation writes",
}


def capture_live_record(
    number: int,
    inputs: Mapping[str, str],
    *,
    capture_backend: Callable[[int, Mapping[str, str]], Mapping[str, Any]] | None = None,
) -> CapturedEvidenceRecord:
    """Attempt to build a live-captured record for ``number`` in 2..7.

    Fails closed (raises EvidenceCaptureError, writes nothing) when:
      * ``number`` is not a known live record,
      * any required live input is missing, or
      * no real capture backend is wired in (this repository ships none --
        that integration belongs to the operator's environment, never to a
        source-tree simulation or fixture).
    """
    if number not in RECORD_REQUIREMENTS:
        raise EvidenceCaptureError(f"record {number} is not a recognized live-capture record (2-7)")

    requirements = RECORD_REQUIREMENTS[number]
    missing = [req.name for req in requirements if not inputs.get(req.name)]
    if missing:
        described = ", ".join(f"{name} ({next(r.description for r in requirements if r.name == name)})" for name in missing)
        raise EvidenceCaptureError(
            f"record {number} ({RECORD_DESCRIPTIONS[number]}): missing required live input(s): {described}. "
            "Refusing to write anything record-shaped; this tool never falls back to a "
            "source-tree substitute or a fixture for a live record."
        )

    if capture_backend is None:
        raise EvidenceCaptureError(
            f"record {number} ({RECORD_DESCRIPTIONS[number]}): no live capture backend is wired in this "
            "environment. This is expected here -- record capture is the operator's responsibility under "
            "backlog 5.2, never an agent session's. Refusing to write anything record-shaped."
        )

    raw = capture_backend(number, inputs)
    payload = dict(raw)
    required_binding_fields = (
        "environment",
        "database_endpoint_fingerprint",
        "actionq_release",
        "actionq_deployment_revision",
        "vuoro_release",
        "vuoro_deployment_revision",
    )
    missing_from_backend = [name for name in required_binding_fields if name not in payload]
    if missing_from_backend:
        raise EvidenceCaptureError(
            f"record {number} ({RECORD_DESCRIPTIONS[number]}): capture backend result is missing "
            f"required binding field(s): {', '.join(missing_from_backend)}. Refusing to write "
            "anything record-shaped."
        )
    try:
        schema_version = "w5-captured-evidence/v1"
        record_id = f"w5-record-{number}"
        captured_at = now_iso8601()
        environment = dict(payload.pop("environment"))
        database_endpoint_fingerprint = payload.pop("database_endpoint_fingerprint")
        actionq_release = payload.pop("actionq_release")
        actionq_deployment_revision = payload.pop("actionq_deployment_revision")
        vuoro_release = payload.pop("vuoro_release")
        vuoro_deployment_revision = payload.pop("vuoro_deployment_revision")
        actor = local_actor()
        tool_versions = _tool_versions()
        result_digest = binding_digest(
            schema_version=schema_version,
            record_id=record_id,
            captured_at=captured_at,
            environment=environment,
            actor=actor,
            tool_versions=tool_versions,
            payload=payload,
            extra={
                "database_endpoint_fingerprint": database_endpoint_fingerprint,
                "actionq_release": actionq_release,
                "actionq_deployment_revision": actionq_deployment_revision,
                "vuoro_release": vuoro_release,
                "vuoro_deployment_revision": vuoro_deployment_revision,
            },
        )
        return CapturedEvidenceRecord(
            schema_version=schema_version,
            record_id=record_id,
            captured_at=captured_at,
            environment=environment,
            database_endpoint_fingerprint=database_endpoint_fingerprint,
            actionq_release=actionq_release,
            actionq_deployment_revision=actionq_deployment_revision,
            vuoro_release=vuoro_release,
            vuoro_deployment_revision=vuoro_deployment_revision,
            actor=actor,
            tool_versions=tool_versions,
            payload=payload,
            result_digest=result_digest,
        )
    except ValueError as error:
        raise EvidenceCaptureError(
            f"record {number} ({RECORD_DESCRIPTIONS[number]}): capture backend result failed "
            f"envelope validation: {error}. Refusing to write anything record-shaped."
        ) from error


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


_FORBIDDEN_OUT_DIRS = (ROOT / "docs/evidence",)


def _reject_forbidden_out_path(out: str) -> None:
    resolved = Path(out).resolve()
    for forbidden in _FORBIDDEN_OUT_DIRS:
        forbidden_resolved = forbidden.resolve()
        if resolved == forbidden_resolved or forbidden_resolved in resolved.parents:
            raise EvidenceCaptureError(
                f"refusing to write to {resolved}: this tool must never author a record "
                "under docs/evidence/ -- that directory is the operator's, on a live "
                "target, so a change and its proof never share an author."
            )


def _cli_record_1(args: argparse.Namespace) -> int:
    live_jobs = None
    if args.live_jobs:
        live_jobs = json.loads(Path(args.live_jobs).read_text(encoding="utf-8"))
    if args.out:
        try:
            _reject_forbidden_out_path(args.out)
        except EvidenceCaptureError as error:
            print(f"error: {error}", file=sys.stderr)
            return 1
    record = capture_record_1(live_jobs=live_jobs)
    text = json.dumps(record.to_dict(), indent=2, sort_keys=True)
    if args.out:
        Path(args.out).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)
    if not record.payload["consumer_diff_is_empty"] or not record.payload["retired_plane_diff_is_empty"]:
        print(
            "NOTE: nonempty diff against docs/contracts/tranche4-reachability-v1.json "
            "-- this is a repository-check finding to report, not a failure of this tool.",
            file=sys.stderr,
        )
    return 0


def _cli_live_record(number: int, args: argparse.Namespace) -> int:
    inputs = {
        req.name: getattr(args, req.name, None) or ""
        for req in RECORD_REQUIREMENTS[number]
    }
    try:
        # capture_backend is never wired from the CLI -- this repository
        # ships no operator integration -- so this call fails closed today.
        # It is written to also succeed correctly if an operator's own
        # wrapper ever calls capture_live_record directly with a real
        # backend and then drives this same _cli_live_record for output.
        record = capture_live_record(number, inputs, capture_backend=getattr(args, "_capture_backend", None))
    except EvidenceCaptureError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    text = json.dumps(record.to_dict(), indent=2, sort_keys=True)
    out = getattr(args, "out", None)
    if out:
        try:
            _reject_forbidden_out_path(out)
        except EvidenceCaptureError as error:
            print(f"error: {error}", file=sys.stderr)
            return 1
        Path(out).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    record1 = subparsers.add_parser("record-1", help="offline consumer inventory + manifest diff")
    record1.add_argument("--out", help="write the record to this path instead of stdout")
    record1.add_argument("--live-jobs", help="optional path to a JSON {job_id: status} mapping")
    record1.set_defaults(func=_cli_record_1)

    for number in range(2, 8):
        sub = subparsers.add_parser(f"record-{number}", help=RECORD_DESCRIPTIONS[number])
        sub.add_argument("--out", help="write the record to this path instead of stdout")
        for req in RECORD_REQUIREMENTS[number]:
            sub.add_argument(f"--{req.name.replace('_', '-')}", dest=req.name, help=req.description)
        sub.set_defaults(func=lambda args, n=number: _cli_live_record(n, args))

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
