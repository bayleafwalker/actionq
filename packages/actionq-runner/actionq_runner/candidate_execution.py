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
from typing import Any, Mapping, Sequence

from actionq_contracts import (
    CANDIDATE_INTEGRATION_RESULT_V1,
    CANDIDATE_INTEGRATION_SPEC_V1,
    CANDIDATE_VERIFICATION_RESULT_V1,
    CANDIDATE_VERIFICATION_SPEC_V1,
    VERIFICATION_PROFILE_V1,
    artifact_digest,
    canonical_bytes,
    require_compatible,
    sha256_digest,
)

from .candidates import CandidateRecoveryJournal, resolve_exact_contract
from .publisher import ArtifactStore, artifact_ref


def _put_contract(store: ArtifactStore, value: dict[str, Any]) -> str:
    require_compatible(value)
    raw = canonical_bytes(value)
    reference = store.put(raw)
    if artifact_digest(reference) != sha256_digest(value):  # defensive CAS contract
        raise RuntimeError("CAS failed to preserve canonical contract bytes")
    return reference


def _registry_command(store: ArtifactStore, profile: dict[str, Any]) -> tuple[str, ...]:
    """Read one exact, operator-owned command registry entry without a shell."""
    raw = store.get(profile["registry_ref"])
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
) -> str:
    """Execute only a frozen registered profile in a new bundle checkout.

    A command failure becomes ``candidate-failed``.  Artifact, runtime, or
    journal errors raise instead, deliberately publishing no result artifact.
    """
    if not 0 < timeout_seconds <= 3600:
        raise ValueError("verification timeout must be between zero and one hour")
    spec = resolve_exact_contract(store, spec_ref, CANDIDATE_VERIFICATION_SPEC_V1)
    profile = resolve_exact_contract(store, spec["profile_ref"], VERIFICATION_PROFILE_V1)
    command = _registry_command(store, profile)
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
    evidence_raw = canonical_bytes(evidence)
    evidence_ref = store.put(evidence_raw)
    result = {
        "contract_id": CANDIDATE_VERIFICATION_RESULT_V1,
        "spec_ref": spec_ref, "spec_digest": artifact_digest(spec_ref),
        "candidate_ref": spec["candidate_ref"], "candidate_digest": spec["candidate_digest"],
        "outcome": outcome, "evidence_ref": evidence_ref, "evidence_digest": artifact_digest(evidence_ref),
    }
    result_ref = _put_contract(store, result)
    if journal is not None:
        journal.write("result", {"spec_ref": spec_ref, "result_ref": result_ref})
    return result_ref


def integrate_wave(
    store: ArtifactStore,
    *,
    spec_ref: str,
    author_name: str,
    author_email: str,
    commit_timestamp: str,
    recovery_root: Path | str | None = None,
    operation_id: str | None = None,
) -> str:
    """Build a deterministic, local-only wave integration candidate.

    ``member_result_refs`` is consumed in its supplied compiler ordinal order.
    A conflict returns an immutable result but emits neither a bundle nor any
    settlement receipt.
    """
    if not author_name or not author_email or not commit_timestamp:
        raise ValueError("integration commit identity and timestamp are required policy inputs")
    spec = resolve_exact_contract(store, spec_ref, CANDIDATE_INTEGRATION_SPEC_V1)
    journal = None
    if recovery_root is not None:
        if not operation_id:
            raise ValueError("operation_id is required with a recovery root")
        journal = CandidateRecoveryJournal(recovery_root, namespace="integration", operation_id=operation_id)
        journal.write("intent", {"spec_ref": spec_ref, "topology": spec["topology"]})
    members: list[bytes] = []
    for result_ref in spec["member_result_refs"]:
        result = resolve_exact_contract(store, result_ref, CANDIDATE_VERIFICATION_RESULT_V1)
        if result["outcome"] != "passed":
            raise RuntimeError("wave integration requires every frozen member result to be passed")
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
        result_ref = _put_contract(store, result)
        if journal is not None:
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
        environment = {**os.environ, "GIT_AUTHOR_NAME": author_name, "GIT_AUTHOR_EMAIL": author_email,
                       "GIT_COMMITTER_NAME": author_name, "GIT_COMMITTER_EMAIL": author_email,
                       "GIT_AUTHOR_DATE": commit_timestamp, "GIT_COMMITTER_DATE": commit_timestamp}
        message = f"actionq integration {artifact_digest(spec_ref)}"
        for commit in candidate_commits:
            merged = subprocess.run(
                ["git", "-C", os.fspath(checkout), "merge", "--no-ff", "--no-edit", "-m", message, commit],
                env=environment, capture_output=True, text=True, check=False,
            )
            if merged.returncode:
                subprocess.run(["git", "-C", os.fspath(checkout), "merge", "--abort"], capture_output=True, text=True)
                result = {"contract_id": CANDIDATE_INTEGRATION_RESULT_V1, "spec_ref": spec_ref,
                          "spec_digest": artifact_digest(spec_ref), "outcome": "conflict",
                          "candidate_ref": None, "candidate_digest": None}
                result_ref = _put_contract(store, result)
                if journal is not None:
                    journal.write("result", {"spec_ref": spec_ref, "result_ref": result_ref})
                return result_ref
        bundle = root / "integration.bundle"
        subprocess.run(["git", "-C", os.fspath(checkout), "bundle", "create", os.fspath(bundle), "HEAD"], check=True,
                       capture_output=True, text=True)
        candidate_ref = store.put(bundle.read_bytes())
    result = {"contract_id": CANDIDATE_INTEGRATION_RESULT_V1, "spec_ref": spec_ref,
              "spec_digest": artifact_digest(spec_ref), "outcome": "integrated",
              "candidate_ref": candidate_ref, "candidate_digest": artifact_digest(candidate_ref)}
    result_ref = _put_contract(store, result)
    if journal is not None:
        journal.write("result", {"spec_ref": spec_ref, "result_ref": result_ref})
    return result_ref
