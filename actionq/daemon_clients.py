"""Public subprocess clients used by the actionq daemon."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import pwd
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import tomllib
import uuid
from typing import Any, Callable, Protocol, Sequence

from actionq_contracts import (
    DISPATCH_RESULT_V1, DISPATCH_STOP_REASONS, EXECUTION_ENVELOPE_V1, EXECUTION_V1,
    Execution, ExecutionEnvelope,
    artifact_digest,
    canonical_bytes, require_compatible, sha256_digest,
)
from .git_evidence import collect_git_evidence_bounded, git_state_at_start
from .completion_outbox import CompletionOutbox, DEFAULT_OUTBOX_PATH
from .schema import MAX_SCHEMA_VERSION
from .harnesses import HarnessInvocation, get_adapter
from .harnesses.codex_catalog import LUNA_V2_CATALOG_WORKAROUND
from .harness_profiles import validate_harness_profile
from .routing import (
    HarnessRoute,
    RoutingContext,
    RoutingError,
    RoutingRequest,
    RoutingResult,
    resolve_routing,
    same_provider_fallback,
)
from .scope_iterate import (
    PreparedScopeIterate,
    ScopeIterateKernel,
    ScopeIteratePolicy,
    ScopeIterateRequest,
    VerificationResult,
    load_policy,
)
from .usage_limit import classify_usage_limit, write_handoff
from .cas import _DaemonCAS, artifact_ref as _artifact_ref, fsync_directory as _fsync_directory

from .daemon_config import *

class CoordinatorClient(Protocol):
    def claim(self, worker: str, timeout_minutes: int) -> dict[str, Any] | None: ...
    def renew(self, action_id: int, *, worker: str, timeout_minutes: int, claim_receipt: str) -> None: ...
    def emit(self, event_type: str, *, action_id: int | None, actor: str, payload: dict[str, Any]) -> None: ...
    def settle(self, action_id: int, *, result: dict[str, Any], actor: str, claim_receipt: str) -> None: ...
    def register_publication(self, action_id: int, *, publication: dict[str, Any],
                             actor: str, claim_receipt: str) -> dict[str, Any]: ...
    def show(self, action_id: int) -> dict[str, Any]: ...
    def acknowledge_cancellation(self, action_id: int, *, cancel_request_id: str,
                                 former_claim_receipt: str, runner_auth_token: str) -> None: ...
    def reconcile_runner_spool(self, action_id: int, *, attempt_id: str) -> None: ...


class TakeupClient(Protocol):
    def take(self, project: ProjectConfig, *, session_id: str, actor: str, pid: int) -> dict[str, Any]: ...
    def release(self, project: ProjectConfig, *, session_id: str, actor: str, reason: str) -> dict[str, Any]: ...


class ContextClient(Protocol):
    def fetch(self, project: ProjectConfig, *, item_id: int | None, limit: int) -> dict[str, Any]: ...
    def fetch_item(self, project: ProjectConfig, *, item_id: int) -> dict[str, Any]: ...


class ClaimClient(Protocol):
    def start(
        self, project: ProjectConfig, *, item_id: int, actor: str, ttl_seconds: int, branch: str | None
    ) -> dict[str, Any]: ...

    def renew(
        self, project: ProjectConfig, *, claim_id: int, claim_token: str,
        actor: str, ttl_seconds: int, runtime_session_id: str,
    ) -> dict[str, Any]: ...

    def release(
        self, project: ProjectConfig, *, claim_id: int, claim_token: str, actor: str,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class SprintClaimLease:
    """In-memory-only authority proof for the current supervised session."""

    project: ProjectConfig
    claim_id: int
    claim_token: str
    actor: str
    ttl_seconds: int
    runtime_session_id: str


class AuditClient(Protocol):
    def publish(
        self,
        project: ProjectConfig | None,
        *,
        event_type: str,
        actor: str,
        summary: str,
        refs: Sequence[str],
        metadata: dict[str, Any],
        detail: str | None,
    ) -> dict[str, Any]: ...


class ActionctlClient:
    def __init__(self, executable: str, *, runnerctl: str = "actionq-runner",
                 runner_private_key_path: Path | None = None,
                 artifact_root: Path | None = None):
        self.executable = executable
        self.runnerctl = runnerctl
        self.artifact_root = artifact_root
        configured_key = os.environ.get("ACTIONQ_RUNNER_PRIVATE_KEY")
        self.runner_private_key_path = runner_private_key_path or (Path(configured_key) if configured_key else None)
        self.runner_id: str | None = None
        self._daemon_schema_checked = False

    def _run(self, *args: str, allow_empty: bool = False, input_text: str | None = None) -> dict[str, Any] | None:
        environment = os.environ.copy()
        if self.artifact_root is not None:
            environment["ACTIONQ_ARTIFACT_ROOT"] = str(self.artifact_root)
        completed = subprocess.run(
            [self.executable, *args], text=True, input=input_text, capture_output=True,
            check=False, env=environment,
        )
        if allow_empty and completed.returncode == 2:
            return None
        if completed.returncode:
            detail = completed.stderr.strip() or completed.stdout.strip() or "actionctl failed"
            raise RuntimeError(detail)
        return json.loads(completed.stdout)

    def _proof(self, *, runner_id: str, operation: str, resource: str) -> dict[str, Any]:
        if self.runner_private_key_path is None:
            raise RuntimeError("runner private key is not configured")
        request_id = str(uuid.uuid4())
        completed = subprocess.run(
            [self.runnerctl, "sign", "--private-key", str(self.runner_private_key_path),
             "--runner-id", runner_id, "--operation", operation, "--resource", resource,
             "--request-id", request_id], text=True, capture_output=True, check=False,
        )
        if completed.returncode:
            raise RuntimeError(completed.stderr.strip() or "runner signing failed")
        return json.loads(completed.stdout)

    def claim(self, worker: str, timeout_minutes: int) -> dict[str, Any] | None:
        if not self._daemon_schema_checked:
            compatibility = self._run("check-compatibility")
            assert compatibility is not None
            observed = compatibility.get("observed_schema_version")
            if observed != MAX_SCHEMA_VERSION:
                raise RuntimeError(
                    f"actionq-daemon requires execution schema {MAX_SCHEMA_VERSION}; schema 3 is "
                    "maintenance-adapter-only because runner cancellation fencing "
                    "is unavailable"
                )
            self._daemon_schema_checked = True
        self.runner_id = worker
        proof = self._proof(runner_id=worker, operation="execution.action.claim", resource="queue:next")
        return self._run("claim", "--proof-stdin", "--timeout", str(timeout_minutes),
                         allow_empty=True, input_text=json.dumps(proof))

    def show(self, action_id: int) -> dict[str, Any]:
        result = self._run("show", str(action_id))
        assert result is not None
        return result

    def renew(self, action_id: int, *, worker: str, timeout_minutes: int, claim_receipt: str) -> None:
        proof = self._proof(runner_id=worker, operation="execution.action.renew", resource=f"action:{action_id}")
        self._run("renew", str(action_id), "--timeout", str(timeout_minutes), "--proof-stdin",
                  input_text=json.dumps({"claim_receipt": claim_receipt, "runner_proof": proof}))

    def emit(self, event_type: str, *, action_id: int | None, actor: str, payload: dict[str, Any]) -> None:
        args = ["emit", "--type", event_type, "--actor", actor, "--payload", json.dumps(payload, sort_keys=True)]
        if action_id is not None:
            args.extend(["--action", str(action_id)])
        self._run(*args)

    def settle(self, action_id: int, *, result: dict[str, Any], actor: str, claim_receipt: str) -> None:
        proof = self._proof(runner_id=actor, operation="execution.action.settle", resource=f"action:{action_id}")
        self._run(
            "settle", str(action_id), "--packet-stdin",
            input_text=json.dumps({"claim_receipt": claim_receipt, "runner_proof": proof, "result": result}),
        )

    def register_publication(self, action_id: int, *, publication: dict[str, Any],
                             actor: str, claim_receipt: str) -> dict[str, Any]:
        attempt_id = str(publication["attempt_id"])
        resource = f"action:{action_id}:publication:{attempt_id}"
        proof = self._proof(
            runner_id=actor, operation="execution.action.register-publication", resource=resource,
        )
        result = self._run(
            "register-publication", str(action_id), "--proof-stdin",
            input_text=json.dumps({
                "claim_receipt": claim_receipt,
                "runner_proof": proof,
                "attempt_id": attempt_id,
                "journal_ref": str(publication["journal_ref"]),
                "source_commit": str(publication["source_commit"]),
                "candidate_commit": str(publication["candidate_commit"]),
            }),
        )
        assert result is not None
        return result

    def acknowledge_cancellation(self, action_id: int, *, cancel_request_id: str,
                                 former_claim_receipt: str, runner_auth_token: str) -> None:
        if self.runner_id is None:
            raise RuntimeError("runner identity was not established by a claim")
        proof = self._proof(
            runner_id=self.runner_id, operation="execution.action.cancel-ack",
            resource=f"action:{action_id}:cancel:{cancel_request_id}",
        )
        self._run(
            "cancel-ack", str(action_id), "--cancel-request-id", cancel_request_id, "--proof-stdin",
            input_text=json.dumps({"former_claim_receipt": former_claim_receipt,
                                   "runner_auth_token": runner_auth_token,
                                   "runner_proof": proof}),
        )

    def reconcile_runner_spool(self, action_id: int, *, attempt_id: str) -> None:
        if self.runner_id is None:
            raise RuntimeError("runner identity was not established by a claim")
        proof = self._proof(
            runner_id=self.runner_id, operation="runner.spool.reconcile",
            resource=f"action:{action_id}:attempt:{attempt_id}",
        )
        completed = subprocess.run(
            [self.runnerctl, "reconcile", "--proof-stdin"], input=json.dumps(proof),
            text=True, capture_output=True, check=False,
        )
        if completed.returncode:
            raise RuntimeError(completed.stderr.strip() or "runner spool reconciliation failed")


class SprintctlTakeupClient:
    def __init__(self, executable: str):
        self.executable = executable

    def _run(self, project: ProjectConfig, *args: str) -> dict[str, Any]:
        environment = os.environ.copy()
        environment.update(project.env or {})
        completed = subprocess.run([self.executable, *args], cwd=project.path, env=environment,
                                   text=True, capture_output=True, check=False, timeout=30)
        if completed.returncode:
            detail = completed.stderr.strip() or completed.stdout.strip() or "sprintctl takeup failed"
            raise RuntimeError(detail)
        return json.loads(completed.stdout)

    def take(self, project: ProjectConfig, *, session_id: str, actor: str, pid: int) -> dict[str, Any]:
        assert project.sprint_id is not None
        return self._run(project, "takeup", "take", "--sprint-id", str(project.sprint_id), "--actor", actor,
                         "--runtime-session-id", session_id, "--instance-id", session_id, "--pid", str(pid), "--json")

    def release(self, project: ProjectConfig, *, session_id: str, actor: str, reason: str) -> dict[str, Any]:
        assert project.sprint_id is not None
        return self._run(project, "takeup", "release", "--sprint-id", str(project.sprint_id), "--actor", actor,
                         "--runtime-session-id", session_id, "--instance-id", session_id, "--reason", reason, "--json")


class SprintctlContextClient:
    """Requests the Tier-1 ``context-candidates`` packet (item #1116, depends
    on sprintctl #1160 -- ``docs/reference/context-and-handoff.md``).
    """

    def __init__(self, executable: str):
        self.executable = executable

    def fetch(self, project: ProjectConfig, *, item_id: int | None, limit: int) -> dict[str, Any]:
        assert project.sprint_id is not None
        args = [self.executable, "context-candidates", "--sprint-id", str(project.sprint_id),
                "--limit", str(limit), "--json"]
        if item_id is not None:
            args.extend(["--item-id", str(item_id)])
        environment = os.environ.copy()
        environment.update(project.env or {})
        completed = subprocess.run(args, cwd=project.path, env=environment, text=True, capture_output=True,
                                   check=False, timeout=30)
        if completed.returncode:
            detail = completed.stderr.strip() or completed.stdout.strip() or "sprintctl context-candidates failed"
            raise RuntimeError(detail)
        return json.loads(completed.stdout)

    def fetch_item(self, project: ProjectConfig, *, item_id: int) -> dict[str, Any]:
        environment = os.environ.copy()
        environment.update(project.env or {})
        completed = subprocess.run(
            [self.executable, "item", "show", "--id", str(item_id), "--json"],
            cwd=project.path,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )
        if completed.returncode:
            detail = completed.stderr.strip() or completed.stdout.strip() or "sprintctl item show failed"
            raise RuntimeError(detail)
        packet = json.loads(completed.stdout)
        item = packet.get("item")
        if not isinstance(item, dict) or int(item.get("id", 0)) != item_id:
            raise RuntimeError("sprintctl item show returned a mismatched item")
        return item


class SprintctlClaimClient:
    """Pre-start claim acquisition for an explicit, ``claim_eligible``
    context-candidates target (item #1116). This is the only path in this
    module that mutates sprintctl item/claim state before a child session
    starts; it fails closed -- callers must not start the child when
    ``start`` raises.
    """

    def __init__(self, executable: str):
        self.executable = executable

    def start(
        self, project: ProjectConfig, *, item_id: int, actor: str, ttl_seconds: int, branch: str | None
    ) -> dict[str, Any]:
        args = [self.executable, "claim", "start", "--item-id", str(item_id), "--actor", actor,
                "--ttl", str(ttl_seconds), "--json"]
        if branch:
            args.extend(["--branch", branch])
        environment = os.environ.copy()
        environment.update(project.env or {})
        completed = subprocess.run(args, cwd=project.path, env=environment, text=True, capture_output=True,
                                   check=False, timeout=30)
        if completed.returncode:
            detail = completed.stderr.strip() or completed.stdout.strip() or "sprintctl claim start failed"
            raise RuntimeError(detail)
        return json.loads(completed.stdout)

    def renew(
        self, project: ProjectConfig, *, claim_id: int, claim_token: str,
        actor: str, ttl_seconds: int, runtime_session_id: str,
    ) -> dict[str, Any]:
        args = [
            self.executable, "claim", "heartbeat", "--id", str(claim_id),
            "--claim-token", claim_token, "--actor", actor, "--ttl", str(ttl_seconds),
            "--runtime-session-id", runtime_session_id, "--json",
        ]
        environment = os.environ.copy()
        environment.update(project.env or {})
        completed = subprocess.run(args, cwd=project.path, env=environment, text=True,
                                   capture_output=True, check=False, timeout=30)
        if completed.returncode:
            detail = completed.stderr.strip() or completed.stdout.strip() or "sprintctl claim heartbeat failed"
            raise RuntimeError(detail)
        return json.loads(completed.stdout)

    def release(
        self, project: ProjectConfig, *, claim_id: int, claim_token: str, actor: str,
    ) -> dict[str, Any]:
        args = [
            self.executable, "claim", "release", "--id", str(claim_id),
            "--claim-token", claim_token, "--actor", actor,
        ]
        environment = os.environ.copy()
        environment.update(project.env or {})
        completed = subprocess.run(args, cwd=project.path, env=environment, text=True,
                                   capture_output=True, check=False, timeout=30)
        if completed.returncode:
            detail = completed.stderr.strip() or completed.stdout.strip() or "sprintctl claim release failed"
            raise RuntimeError(detail)
        return {"claim_id": claim_id, "status": "released"}


class AuditctlClient:
    """Publishes events through the documented ``auditctl add`` subprocess
    contract (see ``/projects/dev/auditctl/AGENTS.md``: "Publishers call the
    auditctl binary as a subprocess; do not add a Python client API"). Runs
    with the target project's ``direnv``-equivalent env overlay, same as
    ``SprintctlTakeupClient``, so ``AUDITCTL_ARTIFACTS_ROOT`` and any
    repo-local overrides apply.
    """

    def __init__(self, executable: str):
        self.executable = executable

    def publish(
        self,
        project: ProjectConfig | None,
        *,
        event_type: str,
        actor: str,
        summary: str,
        refs: Sequence[str],
        metadata: dict[str, Any],
        detail: str | None,
    ) -> dict[str, Any]:
        args = [
            self.executable, "add",
            "--type", event_type,
            "--actor", actor,
            "--summary", summary,
            "--source", "actionq-daemon",
            "--json",
        ]
        for ref in refs:
            args.extend(["--ref", ref])
        if metadata:
            args.extend(["--metadata", json.dumps(metadata, sort_keys=True)])
        if detail:
            args.extend(["--detail", detail])
        environment = os.environ.copy()
        cwd = None
        if project is not None:
            environment.update(project.env or {})
            cwd = project.path
        completed = subprocess.run(args, cwd=cwd, env=environment, text=True, capture_output=True,
                                   check=False, timeout=30)
        if completed.returncode:
            error_detail = completed.stderr.strip() or completed.stdout.strip() or "auditctl add failed"
            raise RuntimeError(error_detail)
        return json.loads(completed.stdout)
