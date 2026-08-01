"""Fresh-workspace execution for immutable candidate verification and waves.

The functions here have no queue, PR, push, approval, or release authority.
They consume only CAS-addressed inputs and publish only CAS-addressed result
records.  Callers retain lifecycle settlement authority.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Any, Callable, Mapping, Sequence

from actionq_contracts import (
    CANDIDATE_INTEGRATION_RESULT_V1,
    CANDIDATE_INTEGRATION_SPEC_V1,
    CANDIDATE_VERIFICATION_RESULT_V1,
    CANDIDATE_VERIFICATION_SPEC_V1,
    PUBLICATION_V1,
    VERIFICATION_PROFILE_V1,
    artifact_digest,
    canonical_bytes,
    require_compatible,
    sha256_digest,
)

from .candidates import CandidateRecoveryJournal, resolve_exact_contract
from .publisher import ArtifactStore, artifact_ref

INTEGRATION_AUTHOR_NAME = "ActionQ Integration"
INTEGRATION_AUTHOR_EMAIL = "integration@actionq.invalid"
INTEGRATION_COMMIT_TIMESTAMP = "2000-01-01T00:00:00Z"


class CandidateExecutionCancelled(RuntimeError):
    """Raised before any candidate result is published after cancellation."""


def _require_not_cancelled(cancelled: Callable[[], bool] | None) -> None:
    if cancelled is not None and cancelled():
        raise CandidateExecutionCancelled("candidate operation was cancelled")


def _put_contract(
    store: ArtifactStore, value: dict[str, Any], *, cancelled: Callable[[], bool] | None = None,
) -> str:
    require_compatible(value)
    raw = canonical_bytes(value)
    _require_not_cancelled(cancelled)
    reference = store.put(raw)
    if artifact_digest(reference) != sha256_digest(value) or store.get(reference) != raw:
        raise RuntimeError("CAS failed to preserve canonical contract bytes")
    return reference


def _put_exact(
    store: ArtifactStore, raw: bytes, *, label: str,
    cancelled: Callable[[], bool] | None = None,
) -> str:
    _require_not_cancelled(cancelled)
    reference = store.put(raw)
    if artifact_ref(raw) != reference or store.get(reference) != raw:
        raise RuntimeError(f"CAS failed to preserve {label} bytes")
    return reference


def _registry_command(
    store: ArtifactStore, profile: dict[str, Any], *, trusted_registry_ref: str,
) -> tuple[str, ...]:
    """Read one exact, operator-owned command registry entry without a shell."""
    if profile["registry_ref"] != trusted_registry_ref:
        raise RuntimeError("verification profile registry is not the operator-trusted registry")
    raw = store.get(trusted_registry_ref)
    if artifact_ref(raw) != profile["registry_ref"]:
        raise RuntimeError("verification registry locator does not match its bytes")
    try:
        registry = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("verification registry is not canonical JSON") from exc
    if sha256_digest(registry) != profile["registry_digest"]:
        raise RuntimeError("verification registry digest does not match its bytes")
    if not isinstance(registry, dict) or canonical_bytes(registry) != raw or set(registry) != {"commands"}:
        raise RuntimeError("verification registry has an invalid exact shape")
    commands = registry["commands"]
    command = commands.get(profile["command_id"]) if isinstance(commands, dict) else None
    if (not isinstance(command, list) or not command
            or any(not isinstance(part, str) or not part for part in command)):
        raise RuntimeError("verification profile command is absent from the read-only registry")
    return tuple(command)


def _require_eligible_publication(store: ArtifactStore, reference: str) -> dict[str, Any]:
    publication = resolve_exact_contract(store, reference, PUBLICATION_V1)
    if publication["terminal_status"] != "verified":
        raise RuntimeError("candidate publication is not eligible for verification")
    return publication


def _checkout_bundle(raw: bytes, directory: Path) -> Path:
    if directory.exists():
        if not directory.is_dir() or directory.is_symlink():
            raise RuntimeError("candidate staging directory is unsafe")
    else:
        directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    bundle = directory / "candidate.bundle"
    bundle.write_bytes(raw)
    os.chmod(bundle, 0o600)
    checkout = directory / "checkout"
    completed = subprocess.run(
        ["git", "clone", "--no-checkout", os.fspath(bundle), os.fspath(checkout)],
        capture_output=True, text=True, check=False,
    )
    if completed.returncode:
        raise RuntimeError("candidate artifact is not a cloneable Git bundle")
    completed = subprocess.run(
        ["git", "-C", os.fspath(checkout), "checkout", "--detach"],
        capture_output=True, text=True, check=False,
    )
    if completed.returncode:
        raise RuntimeError("candidate bundle has no checkoutable exact head")
    return checkout


def _require_descends_from(checkout: Path, base_commit: str, candidate_commit: str) -> None:
    relation = subprocess.run(
        ["git", "-C", os.fspath(checkout), "merge-base", "--is-ancestor", base_commit, candidate_commit],
        capture_output=True, text=True, check=False,
    )
    if relation.returncode:
        raise RuntimeError("candidate does not descend from the frozen integration base")


def verify_candidate(
    store: ArtifactStore,
    *,
    spec_ref: str,
    recovery_root: Path | str | None = None,
    operation_id: str | None = None,
    timeout_seconds: float = 600,
    cancelled: Callable[[], bool] | None = None,
    trusted_registry_ref: str | None = None,
) -> str:
    """Execute only a frozen registered profile in a new bundle checkout.

    A command failure becomes ``candidate-failed``.  Artifact, runtime, or
    journal errors raise instead, deliberately publishing no result artifact.
    """
    if not 0 < timeout_seconds <= 3600:
        raise ValueError("verification timeout must be between zero and one hour")
    _require_not_cancelled(cancelled)
    if trusted_registry_ref is None:
        raise ValueError("an operator-trusted verification registry reference is required")
    spec = resolve_exact_contract(store, spec_ref, CANDIDATE_VERIFICATION_SPEC_V1)
    _require_eligible_publication(store, spec["publication_ref"])
    profile = resolve_exact_contract(store, spec["profile_ref"], VERIFICATION_PROFILE_V1)
    command = _registry_command(store, profile, trusted_registry_ref=trusted_registry_ref)
    candidate_raw = store.get(spec["candidate_ref"])
    if artifact_ref(candidate_raw) != spec["candidate_ref"]:
        raise RuntimeError("candidate locator does not match its bytes")
    journal = None
    if recovery_root is not None:
        if not operation_id:
            raise ValueError("operation_id is required with a recovery root")
        journal = CandidateRecoveryJournal(recovery_root, namespace="verification", operation_id=operation_id)
        journal.write("intent", {"spec_ref": spec_ref, "candidate_ref": spec["candidate_ref"]})
    with tempfile.TemporaryDirectory(prefix="actionq-verify-") as temporary:
        checkout = _checkout_bundle(candidate_raw, Path(temporary))
        try:
            completed = subprocess.run(command, cwd=checkout, capture_output=True, text=True,
                                       timeout=timeout_seconds, check=False)
            outcome = "passed" if completed.returncode == 0 else "candidate-failed"
            evidence = {"contract_id": "candidate-verification-evidence/v1", "command_id": profile["command_id"],
                        "exit_code": completed.returncode, "timed_out": False}
        except subprocess.TimeoutExpired:
            outcome = "candidate-failed"
            evidence = {"contract_id": "candidate-verification-evidence/v1", "command_id": profile["command_id"],
                        "exit_code": None, "timed_out": True}
    _require_not_cancelled(cancelled)
    evidence_raw = canonical_bytes(evidence)
    evidence_ref = _put_exact(store, evidence_raw, label="verification evidence", cancelled=cancelled)
    result = {
        "contract_id": CANDIDATE_VERIFICATION_RESULT_V1,
        "spec_ref": spec_ref, "spec_digest": artifact_digest(spec_ref),
        "candidate_ref": spec["candidate_ref"], "candidate_digest": spec["candidate_digest"],
        "publication_ref": spec["publication_ref"], "publication_digest": spec["publication_digest"],
        "outcome": outcome, "evidence_ref": evidence_ref, "evidence_digest": artifact_digest(evidence_ref),
    }
    result_ref = _put_contract(store, result, cancelled=cancelled)
    if journal is not None:
        _require_not_cancelled(cancelled)
        journal.write("result", {"spec_ref": spec_ref, "result_ref": result_ref})
    return result_ref


def integrate_wave(
    store: ArtifactStore,
    *,
    spec_ref: str,
    recovery_root: Path | str | None = None,
    operation_id: str | None = None,
    cancelled: Callable[[], bool] | None = None,
    trusted_registry_ref: str | None = None,
) -> str:
    """Build a deterministic, local-only wave integration candidate.

    ``member_result_refs`` is consumed in its supplied compiler ordinal order.
    A conflict returns an immutable result but emits neither a bundle nor any
    settlement receipt.
    """
    _require_not_cancelled(cancelled)
    if trusted_registry_ref is None:
        raise ValueError("an operator-trusted verification registry reference is required")
    spec = resolve_exact_contract(store, spec_ref, CANDIDATE_INTEGRATION_SPEC_V1)
    expected_input_set_digest = sha256_digest({
        "contract_id": "immutable-input-set/v1", "inputs": spec["member_result_refs"],
    })
    if spec["input_set_digest"] != expected_input_set_digest:
        raise RuntimeError("integration input_set_digest does not match ordered member results")
    journal = None
    if recovery_root is not None:
        if not operation_id:
            raise ValueError("operation_id is required with a recovery root")
        journal = CandidateRecoveryJournal(recovery_root, namespace="integration", operation_id=operation_id)
        journal.write("intent", {"spec_ref": spec_ref, "topology": spec["topology"]})
    members: list[bytes] = []
    for result_ref in spec["member_result_refs"]:
        _require_not_cancelled(cancelled)
        result = resolve_exact_contract(store, result_ref, CANDIDATE_VERIFICATION_RESULT_V1)
        if result["outcome"] != "passed":
            raise RuntimeError("wave integration requires every frozen member result to be passed")
        verification_spec = resolve_exact_contract(
            store, result["spec_ref"], CANDIDATE_VERIFICATION_SPEC_V1,
        )
        _require_eligible_publication(store, verification_spec["publication_ref"])
        profile = resolve_exact_contract(
            store, verification_spec["profile_ref"], VERIFICATION_PROFILE_V1,
        )
        _registry_command(store, profile, trusted_registry_ref=trusted_registry_ref)
        evidence = store.get(result["evidence_ref"])
        if artifact_ref(evidence) != result["evidence_ref"]:
            raise RuntimeError("verification evidence locator does not match its bytes")
        if (result["candidate_ref"], result["candidate_digest"]) != (
            verification_spec["candidate_ref"], verification_spec["candidate_digest"],
        ):
            raise RuntimeError("passed verification result does not bind its exact verification spec candidate")
        if (result["publication_ref"], result["publication_digest"]) != (
            verification_spec["publication_ref"], verification_spec["publication_digest"],
        ):
            raise RuntimeError("passed verification result does not bind its eligible publication")
        raw = store.get(result["candidate_ref"])
        if artifact_ref(raw) != result["candidate_ref"]:
            raise RuntimeError("verified candidate locator does not match its bytes")
        members.append(raw)
    if spec["topology"] == "stacked":
        with tempfile.TemporaryDirectory(prefix="actionq-stacked-") as temporary:
            root = Path(temporary)
            previous = _checkout_bundle(members[0], root / "member-0")
            previous_head = subprocess.run(
                ["git", "-C", os.fspath(previous), "rev-parse", "HEAD"], check=True,
                capture_output=True, text=True,
            ).stdout.strip()
            _require_descends_from(previous, spec["base_commit"], previous_head)
            for ordinal, raw in enumerate(members[1:], start=1):
                current = _checkout_bundle(raw, root / f"member-{ordinal}")
                current_head = subprocess.run(
                    ["git", "-C", os.fspath(current), "rev-parse", "HEAD"], check=True,
                    capture_output=True, text=True,
                ).stdout.strip()
                _require_descends_from(current, spec["base_commit"], current_head)
                ordered = subprocess.run(
                    ["git", "-C", os.fspath(current), "merge-base", "--is-ancestor", previous_head, current_head],
                    capture_output=True, text=True, check=False,
                )
                if ordered.returncode:
                    raise RuntimeError("stacked candidate does not descend from the preceding frozen candidate")
                previous_head = current_head
        result = {"contract_id": CANDIDATE_INTEGRATION_RESULT_V1, "spec_ref": spec_ref,
                  "spec_digest": artifact_digest(spec_ref), "outcome": "integrated",
                  "candidate_ref": artifact_ref(members[-1]), "candidate_digest": artifact_digest(artifact_ref(members[-1]))}
        _require_not_cancelled(cancelled)
        result_ref = _put_contract(store, result, cancelled=cancelled)
        if journal is not None:
            _require_not_cancelled(cancelled)
            journal.write("result", {"spec_ref": spec_ref, "result_ref": result_ref})
        return result_ref
    with tempfile.TemporaryDirectory(prefix="actionq-integrate-") as temporary:
        root = Path(temporary)
        checkout = _checkout_bundle(members[0], root / "member-0")
        initial_head = subprocess.run(
            ["git", "-C", os.fspath(checkout), "rev-parse", "HEAD"], check=True,
            capture_output=True, text=True,
        ).stdout.strip()
        _require_descends_from(checkout, spec["base_commit"], initial_head)
        base = subprocess.run(["git", "-C", os.fspath(checkout), "rev-parse", spec["base_commit"]],
                              capture_output=True, text=True, check=False)
        if base.returncode:
            raise RuntimeError("integration base is unavailable from the frozen candidate")
        subprocess.run(["git", "-C", os.fspath(checkout), "checkout", "--detach", spec["base_commit"]], check=True,
                       capture_output=True, text=True)
        candidate_commits: list[str] = []
        for ordinal, raw in enumerate(members):
            _require_not_cancelled(cancelled)
            bundle = root / f"member-{ordinal}.bundle"
            bundle.write_bytes(raw)
            os.chmod(bundle, 0o600)
            source = _checkout_bundle(raw, root / f"source-{ordinal}")
            commit = subprocess.run(["git", "-C", os.fspath(source), "rev-parse", "HEAD"], check=True,
                                    capture_output=True, text=True).stdout.strip()
            _require_descends_from(source, spec["base_commit"], commit)
            fetched = subprocess.run(["git", "-C", os.fspath(checkout), "fetch", os.fspath(bundle), commit],
                                     capture_output=True, text=True, check=False)
            if fetched.returncode:
                raise RuntimeError("candidate bundle cannot be imported for integration")
            candidate_commits.append(commit)
        environment = {**os.environ,
                       "GIT_AUTHOR_NAME": INTEGRATION_AUTHOR_NAME,
                       "GIT_AUTHOR_EMAIL": INTEGRATION_AUTHOR_EMAIL,
                       "GIT_COMMITTER_NAME": INTEGRATION_AUTHOR_NAME,
                       "GIT_COMMITTER_EMAIL": INTEGRATION_AUTHOR_EMAIL,
                       "GIT_AUTHOR_DATE": INTEGRATION_COMMIT_TIMESTAMP,
                       "GIT_COMMITTER_DATE": INTEGRATION_COMMIT_TIMESTAMP}
        message = f"actionq integration {artifact_digest(spec_ref)}"
        for commit in candidate_commits:
            _require_not_cancelled(cancelled)
            merged = subprocess.run(
                ["git", "-C", os.fspath(checkout), "merge", "--no-ff", "--no-edit", "-m", message, commit],
                env=environment, capture_output=True, text=True, check=False,
            )
            if merged.returncode:
                subprocess.run(["git", "-C", os.fspath(checkout), "merge", "--abort"], capture_output=True, text=True)
                result = {"contract_id": CANDIDATE_INTEGRATION_RESULT_V1, "spec_ref": spec_ref,
                          "spec_digest": artifact_digest(spec_ref), "outcome": "conflict",
                          "candidate_ref": None, "candidate_digest": None}
                result_ref = _put_contract(store, result, cancelled=cancelled)
                if journal is not None:
                    _require_not_cancelled(cancelled)
                    journal.write("result", {"spec_ref": spec_ref, "result_ref": result_ref})
                return result_ref
        bundle = root / "integration.bundle"
        subprocess.run(["git", "-C", os.fspath(checkout), "bundle", "create", os.fspath(bundle), "HEAD"], check=True,
                       capture_output=True, text=True)
        _require_not_cancelled(cancelled)
        candidate_ref = _put_exact(
            store, bundle.read_bytes(), label="integration candidate bundle", cancelled=cancelled,
        )
    _require_not_cancelled(cancelled)
    result = {"contract_id": CANDIDATE_INTEGRATION_RESULT_V1, "spec_ref": spec_ref,
              "spec_digest": artifact_digest(spec_ref), "outcome": "integrated",
              "candidate_ref": candidate_ref, "candidate_digest": artifact_digest(candidate_ref)}
    result_ref = _put_contract(store, result, cancelled=cancelled)
    if journal is not None:
        _require_not_cancelled(cancelled)
        journal.write("result", {"spec_ref": spec_ref, "result_ref": result_ref})
    return result_ref
