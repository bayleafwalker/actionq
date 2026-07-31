"""Long-running actionq coordinator using only the public ``actionctl`` API.

The daemon deliberately has no dependency on the compatibility dispatcher.  It
owns one child session at a time and records its lifecycle as coordinator
events, leaving queue mutation to ``actionctl``.
"""
from __future__ import annotations

import argparse
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
    EXECUTION_ENVELOPE_V1, EXECUTION_V1, Execution, ExecutionEnvelope,
    require_compatible, sha256_digest,
)

from .git_evidence import collect_git_evidence_bounded, git_state_at_start
from .harnesses import HarnessInvocation, get_adapter
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


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _is_shared_sprint_backend(project: ProjectConfig | None) -> bool:
    """True for direct Postgres and served Vuoro Sprintctl authorities."""
    return (project.env or {}).get("SPRINTCTL_BACKEND") in {"remote", "served"} if project else False


@dataclass(frozen=True)
class TakeupConfig:
    enabled: bool = False
    remote_only: bool = True
    sprintctl_bin: str = "sprintctl"


@dataclass(frozen=True)
class AuditConfig:
    enabled: bool = False
    auditctl_bin: str = "auditctl"
    # Best-effort, bounded retry: auditctl mints a new event id on every
    # successful `add` call (no de-dup key in its CLI contract), so a retry
    # here is only "idempotent" in the sense that it is bounded and never
    # doubles back into the actionq/dispatch outcome -- it does not achieve
    # exactly-once delivery into auditctl itself. See daemon._publish_audit.
    max_attempts: int = 2
    retry_backoff_seconds: float = 0.2


@dataclass(frozen=True)
class ContextConfig:
    """Tier-1 deterministic context injection at session start (item #1116).

    See ``sprintctl/docs/ops-upgrade-plan.md`` Tier 1 and
    ``agentops/docs/plans/agentops/session-mechanization-plan.md`` Tier 1:
    a bounded, ranked ``context-candidates`` packet is requested before the
    child session starts. ``auto_claim`` gates whether a *found*,
    ``claim_eligible`` explicit target (sprintctl's rank 1 only -- never an
    inferred/advisory candidate) causes a pre-start ``claim start``; the
    packet fetch itself is always best-effort/fail-open regardless of
    ``auto_claim``.
    """

    enabled: bool = False
    remote_only: bool = True
    sprintctl_bin: str = "sprintctl"
    limit: int = 5
    auto_claim: bool = True


@dataclass(frozen=True)
class DaemonConfig:
    poll_interval_seconds: float = 30.0
    heartbeat_interval_seconds: float = 60.0
    cancellation_poll_interval_seconds: float = 2.0
    graceful_shutdown_seconds: float = 30.0
    default_timeout_minutes: int = 30
    session_state_path: Path = Path("~/.local/state/actionq/sessions.json")
    pause_file: Path = Path("~/.local/state/actionq/PAUSED")
    handoff_dir: Path = Path("~/.local/state/actionq/handoff")
    actionctl_bin: str = "actionctl"
    runnerctl_bin: str = "actionq-runner"
    runner_private_key_path: Path = Path("~/.local/state/actionq/runner-identity.pem")
    runner_id: str = "runner:devbox"
    enforce_worker_isolation: bool = True
    # Explicit durable #2032 CAS root.  None preserves legacy runners that do
    # not produce immutable candidates; scope-iterate publication is enabled
    # only when an operator provisions this owner-controlled path.
    artifact_root: Path | None = None
    takeup: TakeupConfig = TakeupConfig()
    audit: AuditConfig = AuditConfig()
    context: ContextConfig = ContextConfig()
    routing: RoutingContext = RoutingContext()


@dataclass(frozen=True)
class ActionConfig:
    runner: str = "fake"
    timeout_minutes: int | None = None
    fake_duration_seconds: float = 0.0
    # Usage-limit pause/resume (#976): "command" is a deterministic,
    # config-driven runner -- not a real harness invocation -- that lets
    # tests simulate a harness process producing known output and a
    # nonzero exit code so pause detection can be verified without calling
    # a real model. ``harness`` names which confirmed-signal set in
    # ``actionq.usage_limit`` classifies this action's captured output.
    command: tuple[str, ...] | None = None
    harness: str | None = None
    model: str | None = None
    prompt: str | None = None
    # A provider-backed scope-iterate worker can run under a separate local
    # identity.  The daemon remains the coordinator and retains queue/sprint
    # authority; only the harness subprocess crosses this boundary.
    worker_user: str | None = None
    # A provider harness is selected through a named implementation profile;
    # arbitrary version strings are not an execution-policy authority.
    harness_profile: str | None = None
    publish_candidate: bool = False
    scope_iterate: ScopeIteratePolicy | None = None


@dataclass(frozen=True)
class ProjectConfig:
    path: Path
    sprint_id: int | None = None
    env: dict[str, str] | None = None
    default_harness: str | None = None
    default_model: str | None = None


@dataclass
class SessionRecord:
    session_id: str
    runtime_session_id: str
    daemon_id: str
    action_id: int
    action_type: str
    project: str | None
    target_ref: str | None
    runner: str
    pid: int | None
    started_at: str | None
    updated_at: str
    # Crash-recovery evidence (#1115): the project repo path and the commit
    # HEAD was at when this session started, when known. ``None`` for older
    # persisted state or sessions with no configured project -- recovery
    # degrades to no git evidence rather than guessing a worktree.
    worktree: str | None = None
    base_commit: str | None = None
    harness: str | None = None
    provider: str | None = None
    model: str | None = None
    requested_selector: str | None = None
    routing_source: str | None = None
    transport: str | None = None
    surface: str | None = None
    fallback_model: str | None = None
    fallback_reason: str | None = None
    caller_harness: str | None = None


class CoordinatorClient(Protocol):
    def claim(self, worker: str, timeout_minutes: int) -> dict[str, Any] | None: ...
    def renew(self, action_id: int, *, worker: str, timeout_minutes: int, claim_receipt: str) -> None: ...
    def emit(self, event_type: str, *, action_id: int | None, actor: str, payload: dict[str, Any]) -> None: ...
    def complete(self, action_id: int, *, result_ref: str, actor: str, claim_receipt: str) -> None: ...
    def register_publication(self, action_id: int, *, publication: dict[str, Any],
                             actor: str, claim_receipt: str) -> dict[str, Any]: ...
    def fail(self, action_id: int, *, reason: str, actor: str, claim_receipt: str) -> None: ...
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
                 runner_private_key_path: Path | None = None):
        self.executable = executable
        self.runnerctl = runnerctl
        configured_key = os.environ.get("ACTIONQ_RUNNER_PRIVATE_KEY")
        self.runner_private_key_path = runner_private_key_path or (Path(configured_key) if configured_key else None)
        self.runner_id: str | None = None

    def _run(self, *args: str, allow_empty: bool = False, input_text: str | None = None) -> dict[str, Any] | None:
        completed = subprocess.run(
            [self.executable, *args], text=True, input=input_text, capture_output=True, check=False
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

    def complete(self, action_id: int, *, result_ref: str, actor: str, claim_receipt: str) -> None:
        proof = self._proof(runner_id=actor, operation="execution.action.complete", resource=f"action:{action_id}")
        self._run("complete", str(action_id), "--result", result_ref, "--proof-stdin",
                  input_text=json.dumps({"claim_receipt": claim_receipt, "runner_proof": proof}))

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

    def fail(self, action_id: int, *, reason: str, actor: str, claim_receipt: str) -> None:
        proof = self._proof(runner_id=actor, operation="execution.action.fail", resource=f"action:{action_id}")
        self._run("fail", str(action_id), "--reason", reason, "--proof-stdin",
                  input_text=json.dumps({"claim_receipt": claim_receipt, "runner_proof": proof}))

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


def _audit_refs(action: dict[str, Any], project: ProjectConfig | None) -> list[str]:
    """``wi:`` only when the action names a target; ``sprint:`` only when
    the project's sprint id is known -- never guess or fabricate either."""
    refs: list[str] = []
    target_ref = action.get("target_ref")
    if target_ref:
        refs.append(f"wi:{target_ref}")
    if project is not None and project.sprint_id is not None:
        refs.append(f"sprint:{project.sprint_id}")
    return refs


def load_config(path: Path) -> tuple[DaemonConfig, dict[str, ActionConfig], dict[str, ProjectConfig]]:
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    global_raw = raw.get("global", {})
    state_path = Path(global_raw.get("session_state_path", DaemonConfig.session_state_path)).expanduser()
    pause_file = Path(global_raw.get("pause_file", DaemonConfig.pause_file)).expanduser()
    handoff_dir = Path(global_raw.get("handoff_dir", DaemonConfig.handoff_dir)).expanduser()
    takeup_raw = global_raw.get("sprintctl_takeup", {})
    audit_raw = global_raw.get("audit", {})
    context_raw = global_raw.get("context", {})
    routing_raw = global_raw.get("routing") or raw.get("routing") or {}
    harnesses = {
        name: HarnessRoute(
            name=name,
            bin=(str(value["bin"]) if value.get("bin") else None),
            provider=(str(value["provider"]) if value.get("provider") else None),
            transport=(str(value["transport"]) if value.get("transport") else None),
            surface=(str(value["surface"]) if value.get("surface") else None),
        )
        for name, value in raw.get("harnesses", {}).items()
    }
    config = DaemonConfig(
        poll_interval_seconds=float(global_raw.get("poll_interval_seconds", 30)),
        heartbeat_interval_seconds=float(global_raw.get("heartbeat_interval_seconds", 60)),
        cancellation_poll_interval_seconds=float(global_raw.get("cancellation_poll_interval_seconds", 2)),
        graceful_shutdown_seconds=float(global_raw.get("graceful_shutdown_seconds", 30)),
        default_timeout_minutes=int(global_raw.get("default_timeout_minutes", 30)),
        session_state_path=state_path,
        pause_file=pause_file,
        handoff_dir=handoff_dir,
        actionctl_bin=str(global_raw.get("actionctl_bin", "actionctl")),
        runnerctl_bin=str(global_raw.get("runnerctl_bin", "actionq-runner")),
        runner_private_key_path=Path(global_raw.get(
            "runner_private_key_path", DaemonConfig.runner_private_key_path
        )).expanduser(),
        runner_id=str(global_raw.get("runner_id", "runner:devbox")),
        enforce_worker_isolation=bool(global_raw.get("enforce_worker_isolation", True)),
        artifact_root=(
            Path(str(global_raw["artifact_root"])).expanduser()
            if global_raw.get("artifact_root") else None
        ),
        takeup=TakeupConfig(
            enabled=bool(takeup_raw.get("enabled", False)),
            remote_only=bool(takeup_raw.get("remote_only", True)),
            sprintctl_bin=str(global_raw.get("sprintctl_bin", "sprintctl")),
        ),
        audit=AuditConfig(
            enabled=bool(audit_raw.get("enabled", False)),
            auditctl_bin=str(global_raw.get("auditctl_bin", "auditctl")),
            max_attempts=int(audit_raw.get("max_attempts", 2)),
            retry_backoff_seconds=float(audit_raw.get("retry_backoff_seconds", 0.2)),
        ),
        context=ContextConfig(
            enabled=bool(context_raw.get("enabled", False)),
            remote_only=bool(context_raw.get("remote_only", True)),
            sprintctl_bin=str(global_raw.get("sprintctl_bin", "sprintctl")),
            limit=int(context_raw.get("limit", 5)),
            auto_claim=bool(context_raw.get("auto_claim", True)),
        ),
        routing=RoutingContext(
            policy_path=(
                Path(str(routing_raw["policy_path"])).expanduser()
                if routing_raw.get("policy_path") else None
            ),
            default_harness=(
                str(routing_raw["default_harness"]) if routing_raw.get("default_harness") else None
            ),
            trusted_caller_harness=(
                str(routing_raw["trusted_caller_harness"])
                if routing_raw.get("trusted_caller_harness") else None
            ),
            caller_provider=(
                str(routing_raw["caller_provider"]) if routing_raw.get("caller_provider") else None
            ),
            caller_transport=(
                str(routing_raw["caller_transport"]) if routing_raw.get("caller_transport") else None
            ),
            caller_surface=(
                str(routing_raw["caller_surface"]) if routing_raw.get("caller_surface") else None
            ),
            harnesses=harnesses,
        ),
    )
    actions = {
        name: ActionConfig(
            runner=str(value.get("runner", "fake")),
            timeout_minutes=(int(value["timeout_minutes"]) if "timeout_minutes" in value else None),
            fake_duration_seconds=float(value.get("fake_duration_seconds", 0)),
            command=(tuple(str(part) for part in value["command"]) if "command" in value else None),
            harness=(str(value["harness"]) if "harness" in value else None),
            model=(str(value["model"]) if "model" in value else None),
            prompt=(str(value["prompt"]) if "prompt" in value else None),
            worker_user=(str(value["worker_user"]) if "worker_user" in value else None),
            harness_profile=(str(value["harness_profile"]) if "harness_profile" in value else None),
            publish_candidate=bool(value.get("publish_candidate", False)),
            scope_iterate=(
                load_policy(value["scope_iterate"], config_dir=path.parent.resolve())
                if "scope_iterate" in value else None
            ),
        )
        for name, value in raw.get("actions", {}).items()
    }
    projects = {
        name: ProjectConfig(
            path=Path(value["path"]).expanduser(),
            sprint_id=(int(value["sprint_id"]) if "sprint_id" in value else None),
            env={str(key): str(item) for key, item in value.get("env", {}).items()} or None,
            default_harness=(
                str(value["default_harness"]) if value.get("default_harness") else None
            ),
            default_model=(str(value["default_model"]) if value.get("default_model") else None),
        )
        for name, value in raw.get("projects", {}).items()
    }
    return config, actions, projects


class Daemon:
    def __init__(
        self,
        config: DaemonConfig,
        actions: dict[str, ActionConfig],
        client: CoordinatorClient,
        projects: dict[str, ProjectConfig] | None = None,
        takeup_client: TakeupClient | None = None,
        audit_client: AuditClient | None = None,
        reload_config: Callable[[], tuple[DaemonConfig, dict[str, ActionConfig], dict[str, ProjectConfig]]] | None = None,
        context_client: ContextClient | None = None,
        claim_client: ClaimClient | None = None,
    ):
        self.config, self.actions, self.client = config, actions, client
        self.projects = projects or {}
        self.takeup_client = takeup_client or SprintctlTakeupClient(config.takeup.sprintctl_bin)
        self.audit_client = audit_client or AuditctlClient(config.audit.auditctl_bin)
        self.context_client = context_client or SprintctlContextClient(config.context.sprintctl_bin)
        self.claim_client = claim_client or SprintctlClaimClient(config.context.sprintctl_bin)
        self._sprint_claim_leases: dict[str, SprintClaimLease] = {}
        self.daemon_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}"
        self.actor = f"actionq-daemon:{self.daemon_id}"
        self._shutdown = False
        self._reload_requested = False
        self._reload_config = reload_config
        self._child: subprocess.Popen[str] | None = None

    def request_shutdown(self, *_: object) -> None:
        self._shutdown = True

    def request_reload(self, *_: object) -> None:
        """Defer SIGHUP reload until no child process is active."""
        self._reload_requested = True

    def _write_state(self, record: SessionRecord | None) -> None:
        path = self.config.session_state_path
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(asdict(record) if record else {}, sort_keys=True), encoding="utf-8")
        temporary.replace(path)

    def _read_state(self) -> SessionRecord | None:
        path = self.config.session_state_path
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            return SessionRecord(**payload) if payload else None
        except (json.JSONDecodeError, TypeError):
            # Preserve malformed state for operator inspection; it must not
            # cause a daemon restart loop or authorize another claim.
            return None

    @staticmethod
    def _pid_alive(pid: int | None) -> bool:
        if pid is None or pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def recover_stale_state(self) -> bool:
        """Emit one inferred terminal event for a dead child from prior state.

        The queue claim intentionally remains untouched; ``actionctl sweep``
        owns requeueing after its lease deadline. Clearing state only after the
        event succeeds makes ordinary restart recovery idempotent.

        Best-effort sprintctl takeup cleanup happens here too: a session
        that never reached its normal exit path never released its takeup,
        so recovery releases it the same way a clean exit would, with the
        same skip-safely and failure-evidence-retention rules as
        ``_takeup_release`` uses on the normal path.
        """
        record = self._read_state()
        if record is None:
            return False
        if self._pid_alive(record.pid):
            return True
        project = self.projects.get(record.project or "")
        released = self._takeup_release(project, record.session_id, "daemon-recovered")
        # Collect surviving commits/worktree evidence (#1115) when the
        # session recorded a project repo and its starting commit. Bounded:
        # a missing/deleted worktree degrades to an honest empty-evidence
        # record (see collect_git_evidence_bounded) rather than blocking
        # recovery -- a session must never be silently lost just because
        # its worktree also disappeared.
        git_evidence = None
        if record.worktree and record.base_commit:
            git_evidence = collect_git_evidence_bounded(Path(record.worktree), record.base_commit)
        publication_resumed = self._resume_interrupted_publication(
            record.action_id, record.action_type, record.session_id,
        )
        publication_reconciled = self._reconcile_terminal_publication(
            record.action_id, record.action_type,
        )
        self.client.emit(
            "session.end-inferred",
            action_id=record.action_id,
            actor=self.actor,
            payload={
                "session_id": record.session_id,
                "runtime_session_id": record.runtime_session_id,
                "daemon_id": self.daemon_id,
                "action_id": record.action_id,
                "action_type": record.action_type,
                "project": record.project,
                "pid": record.pid,
                "started_at": record.started_at,
                "exited_at": _now(),
                "outcome": "end-inferred",
                "exit_code": None,
                "reason": "daemon-startup-stale-state",
                "sprint_takeup_release": released,
                "git": git_evidence,
                "publication_reconciled": publication_reconciled,
                "publication_resumed": publication_resumed,
            },
        )
        self._write_state(None)
        return False

    def run_once(self) -> bool:
        if self.recover_stale_state():
            return False
        if self.config.pause_file.exists():
            self.client.emit(
                "coordinator_paused",
                action_id=None,
                actor=self.actor,
                payload={"daemon_id": self.daemon_id, "pause_file": str(self.config.pause_file)},
            )
            return False
        action = self.client.claim(self.config.runner_id, self.config.default_timeout_minutes)
        if action is None:
            return False
        if self._settle_recovered_publication(action):
            return True
        self._run_action(action)
        return True

    def _runnerctl_json(self, *args: str, input_value: dict[str, Any] | None = None) -> Any:
        completed = subprocess.run(
            [self.config.runnerctl_bin, *args],
            input=(json.dumps(input_value, sort_keys=True) if input_value is not None else None),
            text=True, capture_output=True, check=False,
        )
        if completed.returncode:
            raise RuntimeError(completed.stderr.strip() or "actionq-runner publication command failed")
        return json.loads(completed.stdout)

    def _publication_policy(self, action: dict[str, Any]) -> ActionConfig | None:
        policy = self.actions.get(str(action.get("action_type") or ""))
        return policy if policy is not None and policy.publish_candidate else None

    def _resume_interrupted_publication(
        self, action_id: int, action_type: str, attempt_id: str,
    ) -> bool:
        policy = self.actions.get(action_type)
        if policy is None or not policy.publish_candidate or self.config.artifact_root is None:
            return False
        attempts = self._runnerctl_json(
            "journal-list", "--artifact-root", str(self.config.artifact_root),
            "--action-id", str(action_id),
        )
        interrupted = next(
            (value for value in attempts
             if value.get("attempt_id") == attempt_id and value.get("status") == "incomplete"),
            None,
        )
        if interrupted is None:
            return False
        self._runnerctl_json(
            "journal-resume", "--artifact-root", str(self.config.artifact_root),
            "--action-id", str(action_id), "--attempt-id", attempt_id,
        )
        return True

    def _ack_publication_settlement(
        self, publication: dict[str, Any], *, authoritative_decision: dict[str, Any],
    ) -> None:
        assert self.config.artifact_root is not None
        self._runnerctl_json("settlement-ack", "--packet-stdin", input_value={
            "artifact_root": str(self.config.artifact_root),
            "action_id": int(publication["action_id"]),
            "attempt_id": str(publication["attempt_id"]),
            "journal_ref": str(publication["journal_ref"]),
            "terminal_status": "completed",
            "result_ref": str(publication["journal_ref"]),
            "authoritative_decision": authoritative_decision,
        })

    def _complete_published(
        self, action_id: int, *, claim_receipt: str, publication: dict[str, Any],
    ) -> None:
        result_ref = str(publication["journal_ref"])
        try:
            self.client.complete(
                action_id, result_ref=result_ref, actor=self.config.runner_id,
                claim_receipt=claim_receipt,
            )
        except Exception:
            history = self.client.show(action_id)
            current = history.get("action", {})
            completed = any(
                event.get("event_type") == "action_completed"
                and event.get("payload", {}).get("result_ref") == result_ref
                for event in history.get("events", [])
            )
            if (current.get("status") != "completed"
                    or current.get("result_ref") != result_ref or not completed):
                raise
        else:
            history = self.client.show(action_id)
            current = history.get("action", {})
            completed = any(
                event.get("event_type") == "action_completed"
                and event.get("payload", {}).get("result_ref") == result_ref
                for event in history.get("events", [])
            )
            if (current.get("status") != "completed"
                    or current.get("result_ref") != result_ref or not completed):
                raise RuntimeError("ActionQ completion lacks matching terminal history")
        self._ack_publication_settlement(
            publication, authoritative_decision={
                "action_id": action_id, "status": "completed", "result_ref": result_ref,
                "completed_at": str(current.get("completed_at") or ""),
            },
        )
        self.client.emit(
            "publication.settled", action_id=action_id, actor=self.actor,
            payload={"attempt_id": publication["attempt_id"], "journal_ref": result_ref},
        )

    def _settle_recovered_publication(self, action: dict[str, Any]) -> bool:
        if self._publication_policy(action) is None:
            return False
        if self.config.artifact_root is None:
            raise RuntimeError("publish_candidate requires a provisioned artifact_root")
        publication = self._runnerctl_json(
            "journal-recover", "--artifact-root", str(self.config.artifact_root),
            "--action-id", str(action["id"]),
        )
        if publication is None:
            return False
        history = self.client.show(int(action["id"]))
        registered = any(
            event.get("event_type") == "publication.registered"
            and event.get("payload", {}).get("attempt_id") == publication.get("attempt_id")
            and event.get("payload", {}).get("journal_ref") == publication.get("journal_ref")
            and event.get("payload", {}).get("source_commit") == publication.get("source_commit")
            and event.get("payload", {}).get("candidate_commit") == publication.get("candidate_commit")
            for event in history.get("events", [])
        )
        if not registered:
            return False
        receipt = str(action.get("claim_receipt") or "")
        if not receipt:
            raise RuntimeError("recovered publication settlement requires a live claim receipt")
        self._complete_published(int(action["id"]), claim_receipt=receipt, publication=publication)
        if hasattr(self.client, "reconcile_runner_spool"):
            self.client.reconcile_runner_spool(
                int(action["id"]), attempt_id=str(publication["attempt_id"]),
            )
        return True

    def _reconcile_terminal_publication(self, action_id: int, action_type: str) -> bool:
        policy = self.actions.get(action_type)
        if policy is None or not policy.publish_candidate or self.config.artifact_root is None:
            return False
        publication = self._runnerctl_json(
            "journal-recover", "--artifact-root", str(self.config.artifact_root),
            "--action-id", str(action_id),
        )
        if publication is None:
            return False
        settled = self._runnerctl_json(
            "settlement-query", "--artifact-root", str(self.config.artifact_root),
            "--action-id", str(action_id), "--attempt-id", str(publication["attempt_id"]),
        )
        if settled is not None:
            return True
        history = self.client.show(action_id)
        current = history.get("action", {})
        result_ref = str(publication["journal_ref"])
        completed = any(
            event.get("event_type") == "action_completed"
            and event.get("payload", {}).get("result_ref") == result_ref
            for event in history.get("events", [])
        )
        if (current.get("status") != "completed"
                or current.get("result_ref") != result_ref or not completed):
            return False
        self._ack_publication_settlement(
            publication, authoritative_decision={
                "action_id": action_id, "status": "completed", "result_ref": result_ref,
                "completed_at": str(current.get("completed_at") or ""),
            },
        )
        self.client.emit(
            "publication.settled", action_id=action_id, actor=self.actor,
            payload={"attempt_id": publication["attempt_id"], "journal_ref": result_ref,
                     "recovered_after_response_loss": True},
        )
        return True

    def _publish_scope_candidate(
        self, *, action_id: int, envelope: ExecutionEnvelope,
        scope_result: VerificationResult, exit_code: int, output_path: Path | None,
        test_command: tuple[str, ...],
    ) -> dict[str, Any]:
        if self.config.artifact_root is None:
            raise RuntimeError("publish_candidate requires a provisioned artifact_root")
        redacted_log = ""
        if output_path is not None and output_path.exists():
            redacted_log = output_path.read_text(encoding="utf-8", errors="replace")
        execution = Execution(
            action_id=action_id, attempt_id=envelope.attempt_id,
            command_id=envelope.command_id, exit_code=exit_code, timed_out=False,
        )
        publication = self._runnerctl_json("publish", "--packet-stdin", input_value={
            "artifact_root": str(self.config.artifact_root),
            "action_id": action_id,
            "attempt_id": envelope.attempt_id,
            "worktree": str(scope_result.worktree),
            "source_commit": scope_result.base_sha,
            "candidate_commit": scope_result.head_sha,
            "execution_envelope": envelope.as_dict(),
            "execution": asdict(execution),
            "verification_outcome": "passed",
            "verification_evidence": {
                "command": list(test_command), "outcome": "passed",
                "source_commit": scope_result.base_sha,
                "candidate_commit": scope_result.head_sha,
                "changed_paths": list(scope_result.changed_paths),
            },
            "redacted_log": redacted_log,
        })
        self.client.emit(
            "publication.available", action_id=action_id, actor=self.actor,
            payload={"attempt_id": envelope.attempt_id,
                     "journal_ref": publication["journal_ref"]},
        )
        return publication

    def _after_publication(self, _publication: dict[str, Any]) -> None:
        """Fault-injection seam for the artifact/PostgreSQL crash boundary."""

    def _run_action(self, action: dict[str, Any]) -> None:
        action_id = int(action["id"])
        claim_receipt = str(action.get("claim_receipt") or "")
        runner_auth_token = str(action.get("runner_auth_token") or "")
        if not claim_receipt:
            raise RuntimeError(f"action #{action_id} claim did not include a claim receipt")
        if not runner_auth_token:
            raise RuntimeError(f"action #{action_id} claim did not include runner authentication")
        action_type = str(action["action_type"])
        action_config = self.actions.get(action_type)
        if action_config is None:
            self.client.fail(action_id, reason=f"no daemon config for action type {action_type}", actor=self.config.runner_id, claim_receipt=claim_receipt)
            return
        if action_config.publish_candidate and action_config.runner != "scope-iterate":
            self.client.fail(
                action_id, reason="publication-policy: immutable candidates require scope-iterate",
                actor=self.config.runner_id, claim_receipt=claim_receipt,
            )
            return
        if action_config.publish_candidate and self.config.artifact_root is None:
            self.client.fail(
                action_id, reason="publication-policy: artifact_root is not provisioned",
                actor=self.config.runner_id, claim_receipt=claim_receipt,
            )
            return
        project = self.projects.get(str(action.get("project") or ""))
        routing: RoutingResult | None = None
        if action_config.runner in {"harness", "scope-iterate"}:
            try:
                routing = resolve_routing(
                    RoutingRequest(
                        model_selector="",
                        action_harness=(str(action["harness"]) if action.get("harness") else None),
                        action_class_harness=action_config.harness,
                        project_harness=project.default_harness if project else None,
                        action_model=(str(action["model"]) if action.get("model") else None),
                        action_class_model=action_config.model,
                        project_model=project.default_model if project else None,
                    ),
                    self.config.routing,
                )
                if project is None:
                    raise RoutingError(f"runner {action_config.runner!r} requires a configured project worktree")
                if self.config.enforce_worker_isolation:
                    if action_config.worker_user is None:
                        raise RoutingError(f"runner {action_config.runner!r} requires a distinct worker_user")
                    try:
                        worker_uid = pwd.getpwnam(action_config.worker_user).pw_uid
                    except KeyError as exc:
                        raise RoutingError("configured worker_user does not exist") from exc
                    if worker_uid == os.geteuid():
                        raise RoutingError("worker_user must differ from the trusted supervisor identity")
                if action_config.runner == "harness" and not (action.get("prompt") or action_config.prompt):
                    raise RoutingError("runner 'harness' requires an explicit or action-class prompt")
                if action_config.runner == "scope-iterate":
                    if action_config.scope_iterate is None:
                        raise RoutingError("runner 'scope-iterate' requires an explicit scope_iterate policy")
                    if not self.config.context.enabled or not self.config.context.auto_claim:
                        raise RoutingError(
                            "runner 'scope-iterate' requires context.enabled and context.auto_claim"
                        )
                    if action.get("target_ref") is None:
                        raise RoutingError("runner 'scope-iterate' requires an exact target_ref")
                    if action_config.worker_user is not None and not re.fullmatch(
                        r"[a-z_][a-z0-9_-]*[$]?", action_config.worker_user
                    ):
                        raise RoutingError("scope-iterate worker_user must be a safe local username")
                    if routing.harness == "opencode" or action_config.harness_profile is not None:
                        validate_harness_profile(
                            action_config.harness_profile,
                            routing,
                            worker_user=action_config.worker_user,
                        )
            except RoutingError as exc:
                self.client.fail(action_id, reason=f"harness-routing: {exc}", actor=self.config.runner_id, claim_receipt=claim_receipt)
                return
        session_id = f"aqs:{uuid.uuid4()}"
        ttl_seconds = (action_config.timeout_minutes or self.config.default_timeout_minutes) * 60
        payload = {
            "session_id": session_id, "runtime_session_id": session_id,
            "daemon_id": self.daemon_id, "action_id": action_id,
            "action_type": action_type, "project": action.get("project"),
            "target_ref": action.get("target_ref"), "runner": action_config.runner,
            "ttl_seconds": ttl_seconds,
        }
        if routing is not None:
            payload["routing"] = routing.provenance()
        audit_actor = f"actionq:{session_id}"
        audit_refs = _audit_refs(action, project)
        audit_dispatch = self._publish_audit(
            project, event_type="dispatch.queued", actor=audit_actor,
            summary=f"actionq action queued: {action_type} #{action_id}", refs=audit_refs,
            metadata={"action_id": action_id, "session_id": session_id, "action_type": action_type,
                     "runner": action_config.runner},
        )
        # Tier-1 deterministic context injection (item #1116): a bounded,
        # ranked context-candidates packet is requested before the child
        # starts, and is always fetched best-effort/fail-open. A pre-start
        # claim is only ever attempted for an explicit target sprintctl
        # itself marked claim_eligible -- never for advisory/inferred
        # candidates -- and that attempt fails closed (see
        # ``_context_claim_acquire``): a failure here must stop the action
        # before any child process starts.
        context_result = self._context_candidates_request(project, action)
        prepared_scope: PreparedScopeIterate | None = None
        scope_result: VerificationResult | None = None
        if action_config.runner == "scope-iterate":
            assert project is not None and action_config.scope_iterate is not None
            try:
                self._exact_target_item(action, context_result)
                target_item_id = int(action["target_ref"])
                target_item = self.context_client.fetch_item(
                    project, item_id=target_item_id
                )
                target_item = {**target_item, "claim_eligible": True}
                prepared_scope = ScopeIterateKernel().prepare(
                    ScopeIterateRequest(
                        action_id=action_id,
                        project=str(action["project"]),
                        repository=project.path,
                        action=action,
                        sprint_item=target_item,
                    ),
                    action_config.scope_iterate,
                )
            except Exception as exc:
                self.client.fail(
                    action_id, reason=f"scope-iterate preparation failed: {exc}",
                    actor=self.config.runner_id, claim_receipt=claim_receipt,
                )
                return
        claim_result = self._context_claim_acquire(
            project, context_result, session_id, ttl_seconds,
            branch=prepared_scope.branch if prepared_scope else None,
            exact_target=(action_config.runner == "scope-iterate"),
        )
        self.client.emit("session.dispatch", action_id=action_id, actor=self.actor,
                         payload={**payload, "audit_dispatch": audit_dispatch,
                                 "context": context_result, "context_claim": claim_result})
        if claim_result is not None and claim_result.get("status") == "failed":
            self.client.fail(
                action_id,
                reason=f"context claim acquisition failed before session start: {claim_result['error']}",
                actor=self.config.runner_id, claim_receipt=claim_receipt,
            )
            return
        # Best-effort starting git state for this project (#1115 crash-
        # recovery evidence). Never blocks dispatch: a project with no git
        # repo at its configured path (or none configured at all) simply
        # means recovery will later have no git evidence to collect.
        worktree, base_commit = None, None
        evidence_project = (
            ProjectConfig(
                prepared_scope.worktree,
                sprint_id=project.sprint_id,
                env=project.env,
                default_harness=project.default_harness,
                default_model=project.default_model,
            )
            if prepared_scope is not None and project is not None else project
        )
        if evidence_project is not None:
            try:
                base_commit, _branch = git_state_at_start(evidence_project.path)
                worktree = str(evidence_project.path)
            except Exception:
                worktree, base_commit = None, None
        record = SessionRecord(
            session_id=session_id,
            runtime_session_id=session_id,
            daemon_id=self.daemon_id,
            action_id=action_id,
            action_type=action_type,
            project=action.get("project"),
            target_ref=action.get("target_ref"),
            runner=action_config.runner,
            pid=None,
            started_at=None,
            updated_at=_now(),
            worktree=worktree,
            base_commit=base_commit,
            harness=routing.harness if routing else action_config.harness,
            provider=routing.provider if routing else None,
            model=routing.model if routing else action_config.model,
            requested_selector=routing.requested_selector if routing else action_config.model,
            routing_source=routing.routing_source if routing else None,
            transport=routing.transport if routing else None,
            surface=routing.surface if routing else None,
            fallback_model=routing.fallback_model if routing else None,
            fallback_reason=routing.fallback_reason if routing else None,
            caller_harness=routing.caller_harness if routing else None,
        )
        output_path = (
            self._output_path(session_id)
            if action_config.runner in {"command", "harness", "scope-iterate"}
            else None
        )
        envelope = ExecutionEnvelope(
            contract_id=EXECUTION_ENVELOPE_V1,
            action_id=action_id,
            attempt_id=session_id,
            source_commit=base_commit or "unavailable",
            command_id=f"{action_type}:{action_config.runner}",
            allowed_paths=(
                tuple(action_config.scope_iterate.path_acl.allow)
                if action_config.runner == "scope-iterate" and action_config.scope_iterate is not None
                else ()
            ),
        )
        require_compatible(envelope.as_dict())
        self.client.emit(
            "runner.contract.frozen", action_id=action_id, actor=self.actor,
            payload={"session_id": session_id, "contract": envelope.as_dict(), "digest": sha256_digest(envelope)},
        )
        try:
            self._child = self._start_child(
                action_config,
                project=evidence_project,
                routing=routing,
                prompt=(
                    prepared_scope.prompt if prepared_scope is not None
                    else (str(action["prompt"]) if action.get("prompt") else action_config.prompt)
                ),
                output_path=output_path,
                envelope=envelope,
            )
            record.pid, record.started_at, record.updated_at = self._child.pid, _now(), _now()
            try:
                takeup = self._takeup_take(project, session_id, record.pid)
            except Exception as exc:
                # A takeup failure before the harness starts is an expected,
                # externally-triggerable failure mode (sprintctl down or
                # unreachable), not evidence of a daemon bug -- fail this
                # action and keep polling instead of crashing the whole
                # daemon process. This prevents a session that would be
                # invisible to cockpit takeup from doing model work at all.
                os.killpg(self._child.pid, signal.SIGTERM)
                self._child.wait()
                self.client.fail(action_id, reason=f"sprintctl takeup failed before session start: {exc}",
                                 actor=self.config.runner_id, claim_receipt=claim_receipt)
                return
            self._write_state(record)
            audit_start = self._publish_audit(
                project, event_type="session.start", actor=audit_actor,
                summary=f"actionq session started: {action_type} #{action_id}", refs=audit_refs,
                metadata={"action_id": action_id, "session_id": session_id, "pid": record.pid},
            )
            self.client.emit("session.started", action_id=action_id, actor=self.actor,
                             payload={**payload, "pid": record.pid, "started_at": record.started_at,
                                     "sprint_takeup": takeup, "audit_start": audit_start})
            sprint_claim_lease = self._sprint_claim_leases.get(session_id)
            outcome, exit_code = self._wait_for_child(
                action_id, payload, record, claim_receipt, runner_auth_token, sprint_claim_lease,
                project, audit_actor, audit_refs,
            )
            usage_limit_reason: str | None = None
            if outcome == "failed":
                usage_limit_reason = self._detect_and_handle_usage_limit(
                    action_id=action_id, action_type=action_type, action_config=action_config,
                    payload=payload, record=record, exit_code=exit_code, output_path=output_path,
                    routing=routing,
                )
            if outcome == "completed" and prepared_scope is not None:
                try:
                    scope_result = ScopeIterateKernel().verify(prepared_scope)
                except Exception as exc:
                    outcome = "failed"
                    usage_limit_reason = f"scope-iterate verification failed: {exc}"
            publication: dict[str, Any] | None = None
            if (outcome == "completed" and scope_result is not None
                    and action_config.publish_candidate):
                assert action_config.scope_iterate is not None
                publication = self._publish_scope_candidate(
                    action_id=action_id, envelope=envelope, scope_result=scope_result,
                    exit_code=exit_code, output_path=output_path,
                    test_command=action_config.scope_iterate.test_command,
                )
                self.client.register_publication(
                    action_id, publication=publication, actor=self.config.runner_id,
                    claim_receipt=claim_receipt,
                )
                self._after_publication(publication)
            released = self._takeup_release(project, session_id, f"session-{outcome}")
            audit_exit = self._publish_audit(
                project, event_type="session.exit", actor=audit_actor,
                summary=f"actionq session exited: {action_type} #{action_id} ({outcome})", refs=audit_refs,
                metadata={"action_id": action_id, "session_id": session_id, "outcome": outcome, "exit_code": exit_code},
            )
            settlement_error: str | None = None
            self.client.emit(
                "settlement.pending", action_id=action_id, actor=self.actor,
                payload={**payload, "outcome": outcome, "sprint_claim": self._claim_ref(sprint_claim_lease)},
            )
            if sprint_claim_lease is not None:
                try:
                    self.claim_client.release(
                        sprint_claim_lease.project, claim_id=sprint_claim_lease.claim_id,
                        claim_token=sprint_claim_lease.claim_token, actor=sprint_claim_lease.actor,
                    )
                except Exception as exc:
                    settlement_error = f"sprint claim release failed: {exc}"
                    self.client.emit(
                        "settlement.sprint_claim_release_failed", action_id=action_id, actor=self.actor,
                        payload={**payload, "sprint_claim": self._claim_ref(sprint_claim_lease), "detail": str(exc)},
                    )
                else:
                    self.client.emit(
                        "settlement.sprint_claim_released", action_id=action_id, actor=self.actor,
                        payload={**payload, "sprint_claim": self._claim_ref(sprint_claim_lease)},
                    )
                    self._after_sprint_claim_release(sprint_claim_lease)
            exited = {**payload, "pid": record.pid, "outcome": outcome, "exit_code": exit_code, "exited_at": _now(),
                     "sprint_takeup_release": released, "audit_exit": audit_exit,
                     "usage_limit_paused": usage_limit_reason is not None,
                     "settlement_error": settlement_error}
            self.client.emit("session.exited", action_id=action_id, actor=self.actor, payload=exited)
            if outcome == "claim-lost":
                # The ActionQ receipt is no longer authoritative.  Do not
                # attempt a terminal transition, even a failure: a newer
                # claimant owns that decision.
                self.client.emit(
                    "settlement.actionq_skipped_claim_lost", action_id=action_id, actor=self.actor,
                    payload={**payload, "sprint_claim": self._claim_ref(sprint_claim_lease)},
                )
            elif outcome == "cancelled":
                # The acknowledgement already performed the terminal mutation.
                pass
            elif settlement_error is not None:
                self.client.fail(action_id, reason=settlement_error, actor=self.config.runner_id, claim_receipt=claim_receipt)
            elif outcome == "completed":
                if publication is not None:
                    self._complete_published(
                        action_id, claim_receipt=claim_receipt, publication=publication,
                    )
                else:
                    self.client.complete(
                        action_id,
                        result_ref=(
                            scope_result.result_ref if scope_result is not None
                            else f"session={session_id}"
                        ),
                        actor=self.config.runner_id,
                        claim_receipt=claim_receipt,
                    )
            else:
                self.client.fail(action_id, reason=usage_limit_reason or f"daemon session {outcome}", actor=self.config.runner_id, claim_receipt=claim_receipt)
            if outcome not in {"claim-lost"} and settlement_error is None and hasattr(self.client, "reconcile_runner_spool"):
                self.client.reconcile_runner_spool(action_id, attempt_id=session_id)
        except Exception as exc:
            self.client.fail(action_id, reason=f"daemon failure: {exc}", actor=self.config.runner_id, claim_receipt=claim_receipt)
            raise
        finally:
            self._sprint_claim_leases.pop(session_id, None)
            self._child = None
            self._write_state(None)

    @staticmethod
    def _claim_ref(lease: SprintClaimLease | None) -> dict[str, Any] | None:
        """Return audit-safe claim identity; opaque proof never leaves memory."""
        if lease is None:
            return None
        return {"claim_id": lease.claim_id, "runtime_session_id": lease.runtime_session_id}

    def _after_sprint_claim_release(self, lease: SprintClaimLease) -> None:
        """Lifecycle seam used by the fault harness to model a process crash.

        Production deliberately has no side effect here.  The boundary is
        explicit because Sprintctl release and ActionQ terminal settlement
        are separate authorities and cannot be one transaction.
        """

    def _context_candidates_request(
        self, project: ProjectConfig | None, action: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Best-effort Tier-1 ``context-candidates`` fetch (item #1116).

        Always fails open: an unreachable or erroring sprintctl only yields a
        "failed" advisory result here and never blocks or fails the action by
        itself -- only a claim decision derived from a *successfully
        fetched*, explicit, ``claim_eligible`` target can gate session start
        (see ``_context_claim_acquire``). Returns ``None`` only when the
        feature is fully disabled by config, so callers/tests can
        distinguish "not configured" from "attempted and skipped/failed".
        """
        if not self.config.context.enabled:
            return None
        if project is None or project.sprint_id is None:
            return {"attempted": False, "status": "skipped"}
        if self.config.context.remote_only and not _is_shared_sprint_backend(project):
            return {"attempted": False, "status": "skipped", "reason": "local-mode"}
        target_ref = action.get("target_ref")
        item_id: int | None = None
        if target_ref is not None:
            try:
                item_id = int(target_ref)
            except (TypeError, ValueError):
                item_id = None
        try:
            packet = self.context_client.fetch(project, item_id=item_id, limit=self.config.context.limit)
            return {"attempted": True, "status": "ok", "packet": packet}
        except Exception as exc:
            return {"attempted": True, "status": "failed", "error": str(exc)}

    def _context_claim_acquire(
        self,
        project: ProjectConfig | None,
        context_result: dict[str, Any] | None,
        session_id: str,
        ttl_seconds: int,
        *,
        branch: str | None = None,
        exact_target: bool = False,
    ) -> dict[str, Any] | None:
        """Pre-start claim acquisition for an explicit, eligible target only.

        Only ever attempts a claim for the context packet's
        ``explicit_target`` -- and only when it was both found and marked
        ``claim_eligible`` by sprintctl itself (rank 1; sprintctl never marks
        an inferred/advisory candidate eligible). This never inspects or acts
        on ranks 2-5. Returns ``None`` when no claim was attempted (feature
        disabled, no context, no explicit eligible target); returns a
        ``status: "failed"`` result when an attempted claim fails -- callers
        must treat that as fail-closed and not start the child session.
        """
        if not self.config.context.auto_claim or context_result is None:
            return None
        if context_result.get("status") != "ok":
            return None
        packet = context_result.get("packet") or {}
        explicit_target = packet.get("explicit_target")
        if not explicit_target or not explicit_target.get("found"):
            return None
        eligible = any(
            candidate.get("rank") == 1 and candidate.get("claim_eligible")
            for candidate in packet.get("candidates") or []
        )
        if not eligible:
            return None
        item_id = explicit_target["item_id"]
        if exact_target and not branch:
            return {
                "attempted": False, "status": "failed", "item_id": item_id,
                "error": "scope-iterate exact target claim requires its prepared branch",
            }
        actor = f"actionq:{session_id}"
        try:
            assert project is not None
            claim = self.claim_client.start(project, item_id=item_id, actor=actor,
                                             ttl_seconds=ttl_seconds, branch=branch)
            claim_id = claim.get("claim_id")
            if claim_id is None and isinstance(claim.get("claim"), dict):
                claim_id = claim["claim"].get("claim_id")
            claim_token = claim.get("claim_token")
            if claim_token is None and isinstance(claim.get("claim"), dict):
                claim_token = claim["claim"].get("claim_token")
            if claim_id is None or not claim_token:
                raise RuntimeError("sprintctl claim start did not return claim id and opaque token")
            self._sprint_claim_leases[session_id] = SprintClaimLease(
                project=project, claim_id=int(claim_id), claim_token=str(claim_token),
                actor=actor, ttl_seconds=ttl_seconds, runtime_session_id=session_id,
            )
            return {"attempted": True, "status": "ok", "item_id": item_id, "claim_id": claim_id}
        except Exception as exc:
            return {"attempted": True, "status": "failed", "item_id": item_id, "error": str(exc)}

    @staticmethod
    def _exact_target_item(
        action: dict[str, Any], context_result: dict[str, Any] | None
    ) -> dict[str, Any]:
        try:
            requested = int(action["target_ref"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("target_ref must be one numeric Sprintctl item id") from exc
        if context_result is None or context_result.get("status") != "ok":
            raise RuntimeError("exact target context lookup did not succeed")
        packet = context_result.get("packet") or {}
        explicit = packet.get("explicit_target") or {}
        if not explicit.get("found") or int(explicit.get("item_id", -1)) != requested:
            raise RuntimeError(f"exact target item {requested} was not found in the configured sprint")
        eligible = next(
            (
                candidate for candidate in packet.get("candidates") or []
                if int(candidate.get("item_id", -1)) == requested
                and candidate.get("rank") == 1
                and candidate.get("claim_eligible") is True
            ),
            None,
        )
        if eligible is None:
            raise RuntimeError(f"exact target item {requested} is not claim eligible")
        return explicit.get("item") or eligible.get("item") or explicit

    def _takeup_take(self, project: ProjectConfig | None, session_id: str, pid: int) -> dict[str, Any]:
        if not self.config.takeup.enabled or project is None or project.sprint_id is None:
            return {"attempted": False, "status": "skipped"}
        if self.config.takeup.remote_only and not _is_shared_sprint_backend(project):
            return {"attempted": False, "status": "skipped", "reason": "local-mode"}
        actor = f"actionq:{session_id}"
        result = self.takeup_client.take(project, session_id=session_id, actor=actor, pid=pid)
        return {"attempted": True, "status": "ok", "event_id": result.get("event_id")}

    def _takeup_release(self, project: ProjectConfig | None, session_id: str, reason: str) -> dict[str, Any]:
        if not self.config.takeup.enabled or project is None or project.sprint_id is None:
            return {"attempted": False, "status": "skipped"}
        if self.config.takeup.remote_only and not _is_shared_sprint_backend(project):
            return {"attempted": False, "status": "skipped", "reason": "local-mode"}
        actor = f"actionq:{session_id}"
        try:
            result = self.takeup_client.release(project, session_id=session_id, actor=actor, reason=reason)
            return {"attempted": True, "status": "ok", "event_id": result.get("event_id")}
        except Exception as exc:
            return {"attempted": True, "status": "failed", "error": str(exc)}

    def _publish_audit(
        self,
        project: ProjectConfig | None,
        *,
        event_type: str,
        actor: str,
        summary: str,
        refs: Sequence[str],
        metadata: dict[str, Any],
        detail: str | None = None,
    ) -> dict[str, Any]:
        """Best-effort auditctl publish. Never raises: a failed or skipped
        audit emission must never fail the underlying dispatch/session
        action (item #973 scope). Retries up to ``config.audit.max_attempts``
        times with a short backoff; auditctl itself has no de-dup key in its
        CLI contract, so this bounds the daemon's own retry behavior rather
        than guaranteeing exactly-once delivery into auditctl.
        """
        if not self.config.audit.enabled:
            return {"attempted": False, "status": "skipped"}
        max_attempts = max(1, self.config.audit.max_attempts)
        attempts = 0
        last_error: str | None = None
        while attempts < max_attempts:
            attempts += 1
            try:
                result = self.audit_client.publish(
                    project, event_type=event_type, actor=actor, summary=summary,
                    refs=refs, metadata=metadata, detail=detail,
                )
                return {"attempted": True, "status": "ok", "event_id": result.get("id"), "attempts": attempts}
            except Exception as exc:
                last_error = str(exc)
                if attempts < max_attempts:
                    time.sleep(self.config.audit.retry_backoff_seconds)
        return {"attempted": True, "status": "failed", "error": last_error, "attempts": attempts}

    def _start_child(
        self,
        action: ActionConfig,
        *,
        project: ProjectConfig | None = None,
        routing: RoutingResult | None = None,
        prompt: str | None = None,
        output_path: Path | None = None,
        envelope: ExecutionEnvelope,
    ) -> subprocess.Popen[str]:
        def portable(command: list[str], *, cwd: Path | None = None,
                     environment: dict[str, str] | None = None,
                     stdin_text: str | None = None) -> subprocess.Popen[str]:
            blocked = ("ACTIONQ", "SPRINT", "KUBECONFIG", "SSH_", "GIT_", "AWS_", "TOKEN", "SECRET", "PASSWORD")
            source = environment if environment is not None else os.environ
            clean_env = {key: value for key, value in source.items()
                         if not any(marker in key.upper() for marker in blocked)}
            packet = {
                "envelope": envelope.as_dict(), "command": command,
                "cwd": str(cwd) if cwd else None, "environment": clean_env,
                "registered_command_id": envelope.command_id,
                "contained_worker": self.config.enforce_worker_isolation,
                "stdin": stdin_text, "output_path": str(output_path) if output_path else None,
                # Leave the outer coordinator enough time to observe the
                # runner's waitpid after the runner escalates its child.
                "grace_seconds": max(0.0, self.config.graceful_shutdown_seconds - 0.5),
            }
            child = subprocess.Popen(
                [self.config.runnerctl_bin, "execute"], stdin=subprocess.PIPE,
                text=True, start_new_session=True,
            )
            assert child.stdin is not None
            child.stdin.write(json.dumps(packet, sort_keys=True))
            child.stdin.close()
            return child
        if action.runner in {"fake", "fake-commit"}:
            code = f"import time; time.sleep({action.fake_duration_seconds!r})"
            return portable([sys.executable, "-c", code])
        if action.runner == "command":
            # Deterministic, config-driven runner for usage-limit
            # command-wrapper simulations (#976) -- not a real harness
            # invocation. Output is captured to ``output_path`` so the
            # daemon can classify it for a confirmed usage-limit signal
            # after the child exits.
            if not action.command:
                raise RuntimeError("runner 'command' requires ActionConfig.command")
            return portable(list(action.command))
        if action.runner in {"harness", "scope-iterate"}:
            if project is None or routing is None or prompt is None:
                raise RuntimeError(f"runner {action.runner!r} requires project, routing, and prompt")
            harness_route = (self.config.routing.harnesses or {}).get(routing.harness)
            adapter = get_adapter(
                routing.harness,
                bin_path=harness_route.bin if harness_route else None,
            )
            invocation = HarnessInvocation(
                prompt=prompt,
                worktree=project.path,
                model=routing.model,
                timeout_seconds=(action.timeout_minutes or self.config.default_timeout_minutes) * 60,
                extra_env=project.env or {},
            )
            command = adapter.build_command(invocation)
            env = adapter.build_env(invocation)
            try:
                if action.worker_user is not None:
                    # `sudo -H` supplies the worker's HOME, so provider
                    # credentials remain with that identity.  Preserve only
                    # the reviewed OpenCode policy path; do not pass the
                    # coordinator environment through the privilege boundary.
                    sudo_bin = shutil.which("sudo")
                    if sudo_bin is None:
                        raise RuntimeError("contained worker requires an approved sudo executable")
                    command = [
                        sudo_bin, "-n", "-H",
                        f"--preserve-env=OPENCODE_CONFIG",
                        "-u", action.worker_user, "--",
                        *command,
                    ]
                    env = {"OPENCODE_CONFIG": env.get("OPENCODE_CONFIG", "")}
                stdin_text = adapter.stdin_text(invocation)
                return portable(command, cwd=invocation.worktree, environment=env, stdin_text=stdin_text)
            finally:
                pass
        raise RuntimeError(f"runner {action.runner!r} is not supported by daemon minimum")

    def _output_path(self, session_id: str) -> Path:
        safe = session_id.replace(":", "_").replace("/", "_")
        return self.config.session_state_path.parent / "harness-output" / f"{safe}.log"

    def _detect_and_handle_usage_limit(
        self,
        *,
        action_id: int,
        action_type: str,
        action_config: ActionConfig,
        payload: dict[str, Any],
        record: SessionRecord,
        exit_code: int,
        output_path: Path | None,
        routing: RoutingResult | None = None,
    ) -> str | None:
        """Best-effort usage-limit classification for a failed session.

        Returns an operator-visible, distinct fail reason (and has already
        emitted ``session.paused`` plus written a handoff file) when a
        confirmed usage-limit signal was found in captured output; returns
        ``None`` for an ordinary failure so the caller keeps its normal
        failure reason. Never raises: a classification or handoff-write
        problem must not mask the underlying action outcome (#976
        non-scope: no generic automatic retry, no masking of real
        failures as pauses).
        """
        harness = routing.harness if routing else action_config.harness
        model = routing.model if routing else action_config.model
        if output_path is None or not harness:
            return None
        try:
            output_text = output_path.read_text(encoding="utf-8", errors="replace") if output_path.exists() else ""
        except OSError:
            return None
        signal = classify_usage_limit(harness, exit_code=exit_code, output=output_text)
        if not signal.detected:
            return None

        handoff_path: Path | None
        handoff_error: str | None = None
        fallback: RoutingResult | None = None
        if routing is not None and routing.fallback_model:
            try:
                fallback = same_provider_fallback(
                    routing,
                    reason=signal.reason or "confirmed usage-limit signal",
                )
            except RoutingError:
                fallback = None
        try:
            handoff_path = write_handoff(
                self.config.handoff_dir,
                session_id=record.session_id,
                action_id=action_id,
                action_type=action_type,
                harness=harness,
                model=model,
                reason=signal.reason or "confirmed usage-limit signal",
                evidence=signal.evidence,
                fallback_harness=fallback.harness if fallback else None,
                fallback_provider=fallback.provider if fallback else None,
                fallback_model=fallback.model if fallback else None,
                fallback_reason=fallback.fallback_reason if fallback else None,
            )
        except Exception as exc:  # noqa: BLE001 -- handoff failure must not mask the pause signal itself
            handoff_path = None
            handoff_error = str(exc)

        self.client.emit(
            "session.paused",
            action_id=action_id,
            actor=self.actor,
            payload={
                **payload,
                "pid": record.pid,
                "reason": "usage-limit",
                "mechanism": "checkpoint-and-fail",
                "handoff_ref": str(handoff_path) if handoff_path else None,
                "handoff_error": handoff_error,
                "resumable": handoff_path is not None,
                "evidence": signal.evidence,
                "redispatch_routing": fallback.provenance() if fallback else None,
            },
        )
        return f"usage-limit-paused: {signal.reason}"

    def emit_resume_event(
        self,
        *,
        action_id: int | None,
        session_id: str,
        resumed_from_session_id: str,
        handoff_ref: str | None = None,
        mechanism: str = "redispatch",
    ) -> None:
        """Record that a new session resumes from a prior handoff.

        Resume is always re-dispatch, never process continuation (see
        module docstring in ``actionq.usage_limit``): this only appends the
        correlating ``session.resumed`` event. It is the operator/manual
        re-dispatch drill entry point for #976 -- an operator (or a future
        automated re-dispatch policy) calls this once a new action/session
        has actually started against the handoff's context.
        """
        self.client.emit(
            "session.resumed",
            action_id=action_id,
            actor=self.actor,
            payload={
                "session_id": session_id,
                "resumed_from_session_id": resumed_from_session_id,
                "handoff_ref": handoff_ref,
                "mechanism": mechanism,
            },
        )

    def _wait_for_child(
        self,
        action_id: int,
        payload: dict[str, Any],
        record: SessionRecord,
        claim_receipt: str,
        runner_auth_token: str,
        sprint_claim_lease: SprintClaimLease | None,
        project: ProjectConfig | None = None,
        audit_actor: str | None = None,
        audit_refs: Sequence[str] = (),
    ) -> tuple[str, int]:
        assert self._child is not None
        next_heartbeat = time.monotonic() + self.config.heartbeat_interval_seconds
        next_cancel_poll = time.monotonic()
        while self._child.poll() is None:
            if self._shutdown:
                audit_pause = self._publish_audit(
                    project, event_type="session.pause", actor=audit_actor or self.actor,
                    summary=f"actionq session paused: {payload.get('action_type')} #{action_id} (shutdown)",
                    refs=audit_refs,
                    metadata={"action_id": action_id, "session_id": payload.get("session_id"), "reason": "shutdown"},
                )
                self.client.emit("session.paused", action_id=action_id, actor=self.actor,
                                 payload={**payload, "pid": record.pid, "reason": "shutdown", "audit_pause": audit_pause})
                try:
                    self._child.wait(timeout=self.config.graceful_shutdown_seconds)
                except subprocess.TimeoutExpired:
                    os.killpg(self._child.pid, signal.SIGTERM)
                return "shutdown", self._child.wait()
            if time.monotonic() >= next_cancel_poll:
                try:
                    current = self.client.show(action_id) if hasattr(self.client, "show") else None
                    action = current.get("action", current) if isinstance(current, dict) else None
                    if isinstance(action, dict) and action.get("status") == "cancelling":
                        os.killpg(self._child.pid, signal.SIGTERM)
                        try:
                            self._child.wait(timeout=self.config.graceful_shutdown_seconds)
                        except subprocess.TimeoutExpired:
                            os.killpg(self._child.pid, signal.SIGKILL)
                            self._child.wait()
                        self.client.acknowledge_cancellation(
                            action_id, cancel_request_id=str(action["cancel_request_id"]),
                            former_claim_receipt=claim_receipt,
                            runner_auth_token=runner_auth_token,
                        )
                        return "cancelled", int(self._child.returncode or 0)
                except Exception as exc:
                    os.killpg(self._child.pid, signal.SIGTERM)
                    self._child.wait()
                    self.client.emit("session.paused", action_id=action_id, actor=self.actor,
                        payload={**payload, "pid": record.pid, "reason": "cancellation-control-lost", "detail": str(exc)})
                    return "claim-lost", int(self._child.returncode or 1)
                next_cancel_poll = time.monotonic() + self.config.cancellation_poll_interval_seconds
            if time.monotonic() >= next_heartbeat:
                try:
                    self.client.renew(action_id, worker=self.config.runner_id,
                                      timeout_minutes=self.config.default_timeout_minutes,
                                      claim_receipt=claim_receipt)
                    if sprint_claim_lease is not None:
                        self.claim_client.renew(
                            sprint_claim_lease.project,
                            claim_id=sprint_claim_lease.claim_id,
                            claim_token=sprint_claim_lease.claim_token,
                            actor=sprint_claim_lease.actor,
                            ttl_seconds=sprint_claim_lease.ttl_seconds,
                            runtime_session_id=sprint_claim_lease.runtime_session_id,
                        )
                except Exception as exc:
                    # Renewal is authority, unlike a session heartbeat.  Once
                    # it fails, this worker must not keep executing or settle.
                    os.killpg(self._child.pid, signal.SIGTERM)
                    self._child.wait()
                    self.client.emit("session.paused", action_id=action_id, actor=self.actor,
                        payload={**payload, "pid": record.pid, "reason": "claim-authority-lost", "detail": str(exc)})
                    return "claim-lost", int(self._child.returncode or 1)
                record.updated_at = _now()
                self._write_state(record)
                self.client.emit("session.heartbeat", action_id=action_id, actor=self.actor,
                                 payload={**payload, "pid": record.pid, "status": "running"})
                next_heartbeat = time.monotonic() + self.config.heartbeat_interval_seconds
            time.sleep(0.05)
        exit_code = self._child.returncode
        return ("completed" if exit_code == 0 else "failed"), int(exit_code)

    def run_forever(self) -> None:
        while not self._shutdown:
            if self._reload_requested and self._child is None and self._reload_config:
                self.config, self.actions, self.projects = self._reload_config()
                self._reload_requested = False
            claimed = self.run_once()
            if not claimed:
                time.sleep(self.config.poll_interval_seconds)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the actionq daemon")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    config_path = args.config or Path("~/.config/actionq/config.toml").expanduser()
    if not config_path.exists() and args.config is None:
        config_path = Path("~/.config/actionq-dispatcher/config.toml").expanduser()
    config, actions, projects = load_config(config_path)
    daemon = Daemon(config, actions, ActionctlClient(
        config.actionctl_bin, runnerctl=config.runnerctl_bin,
        runner_private_key_path=config.runner_private_key_path,
    ), projects,
                    reload_config=lambda: load_config(config_path))
    signal.signal(signal.SIGTERM, daemon.request_shutdown)
    signal.signal(signal.SIGINT, daemon.request_shutdown)
    signal.signal(signal.SIGHUP, daemon.request_reload)
    if args.once:
        daemon.run_once()
    else:
        daemon.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
