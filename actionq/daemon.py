"""Long-running actionq coordinator using only the public ``actionctl`` API.

The daemon deliberately has no dependency on the compatibility dispatcher.  It
owns one child session at a time and records its lifecycle as coordinator
events, leaving queue mutation to ``actionctl``.
"""
from __future__ import annotations

import argparse
import json
import os
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

from .git_evidence import collect_git_evidence_bounded, git_state_at_start
from .harnesses import HarnessInvocation, get_adapter
from .routing import (
    HarnessRoute,
    RoutingContext,
    RoutingError,
    RoutingRequest,
    RoutingResult,
    resolve_routing,
    same_provider_fallback,
)
from .usage_limit import classify_usage_limit, write_handoff


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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
    graceful_shutdown_seconds: float = 30.0
    default_timeout_minutes: int = 30
    session_state_path: Path = Path("~/.local/state/actionq/sessions.json")
    pause_file: Path = Path("~/.local/state/actionq/PAUSED")
    handoff_dir: Path = Path("~/.local/state/actionq/handoff")
    actionctl_bin: str = "actionctl"
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
    def emit(self, event_type: str, *, action_id: int | None, actor: str, payload: dict[str, Any]) -> None: ...
    def complete(self, action_id: int, *, result_ref: str, actor: str) -> None: ...
    def fail(self, action_id: int, *, reason: str, actor: str) -> None: ...


class TakeupClient(Protocol):
    def take(self, project: ProjectConfig, *, session_id: str, actor: str, pid: int) -> dict[str, Any]: ...
    def release(self, project: ProjectConfig, *, session_id: str, actor: str, reason: str) -> dict[str, Any]: ...


class ContextClient(Protocol):
    def fetch(self, project: ProjectConfig, *, item_id: int | None, limit: int) -> dict[str, Any]: ...


class ClaimClient(Protocol):
    def start(
        self, project: ProjectConfig, *, item_id: int, actor: str, ttl_seconds: int, branch: str | None
    ) -> dict[str, Any]: ...


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
    def __init__(self, executable: str):
        self.executable = executable

    def _run(self, *args: str, allow_empty: bool = False) -> dict[str, Any] | None:
        completed = subprocess.run(
            [self.executable, *args], text=True, capture_output=True, check=False
        )
        if allow_empty and completed.returncode == 2:
            return None
        if completed.returncode:
            detail = completed.stderr.strip() or completed.stdout.strip() or "actionctl failed"
            raise RuntimeError(detail)
        return json.loads(completed.stdout)

    def claim(self, worker: str, timeout_minutes: int) -> dict[str, Any] | None:
        return self._run("claim", "--worker", worker, "--timeout", str(timeout_minutes), allow_empty=True)

    def emit(self, event_type: str, *, action_id: int | None, actor: str, payload: dict[str, Any]) -> None:
        args = ["emit", "--type", event_type, "--actor", actor, "--payload", json.dumps(payload, sort_keys=True)]
        if action_id is not None:
            args.extend(["--action", str(action_id)])
        self._run(*args)

    def complete(self, action_id: int, *, result_ref: str, actor: str) -> None:
        self._run("complete", str(action_id), "--result", result_ref, "--actor", actor)

    def fail(self, action_id: int, *, reason: str, actor: str) -> None:
        self._run("fail", str(action_id), "--reason", reason, "--actor", actor)


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
        graceful_shutdown_seconds=float(global_raw.get("graceful_shutdown_seconds", 30)),
        default_timeout_minutes=int(global_raw.get("default_timeout_minutes", 30)),
        session_state_path=state_path,
        pause_file=pause_file,
        handoff_dir=handoff_dir,
        actionctl_bin=str(global_raw.get("actionctl_bin", "actionctl")),
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
        action = self.client.claim(self.actor, self.config.default_timeout_minutes)
        if action is None:
            return False
        self._run_action(action)
        return True

    def _run_action(self, action: dict[str, Any]) -> None:
        action_id = int(action["id"])
        action_type = str(action["action_type"])
        action_config = self.actions.get(action_type)
        if action_config is None:
            self.client.fail(action_id, reason=f"no daemon config for action type {action_type}", actor=self.actor)
            return
        project = self.projects.get(str(action.get("project") or ""))
        routing: RoutingResult | None = None
        if action_config.runner == "harness":
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
                    raise RoutingError("runner 'harness' requires a configured project worktree")
                if not (action.get("prompt") or action_config.prompt):
                    raise RoutingError("runner 'harness' requires an explicit or action-class prompt")
            except RoutingError as exc:
                self.client.fail(action_id, reason=f"harness-routing: {exc}", actor=self.actor)
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
        claim_result = self._context_claim_acquire(project, context_result, session_id, ttl_seconds)
        self.client.emit("session.dispatch", action_id=action_id, actor=self.actor,
                         payload={**payload, "audit_dispatch": audit_dispatch,
                                 "context": context_result, "context_claim": claim_result})
        if claim_result is not None and claim_result.get("status") == "failed":
            self.client.fail(
                action_id,
                reason=f"context claim acquisition failed before session start: {claim_result['error']}",
                actor=self.actor,
            )
            return
        # Best-effort starting git state for this project (#1115 crash-
        # recovery evidence). Never blocks dispatch: a project with no git
        # repo at its configured path (or none configured at all) simply
        # means recovery will later have no git evidence to collect.
        worktree, base_commit = None, None
        if project is not None:
            try:
                base_commit, _branch = git_state_at_start(project.path)
                worktree = str(project.path)
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
            if action_config.runner in {"command", "harness"}
            else None
        )
        try:
            self._child = self._start_child(
                action_config,
                project=project,
                routing=routing,
                prompt=(str(action["prompt"]) if action.get("prompt") else action_config.prompt),
                output_path=output_path,
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
                                 actor=self.actor)
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
            outcome, exit_code = self._wait_for_child(action_id, payload, record, project, audit_actor, audit_refs)
            usage_limit_reason: str | None = None
            if outcome == "failed":
                usage_limit_reason = self._detect_and_handle_usage_limit(
                    action_id=action_id, action_type=action_type, action_config=action_config,
                    payload=payload, record=record, exit_code=exit_code, output_path=output_path,
                    routing=routing,
                )
            released = self._takeup_release(project, session_id, f"session-{outcome}")
            audit_exit = self._publish_audit(
                project, event_type="session.exit", actor=audit_actor,
                summary=f"actionq session exited: {action_type} #{action_id} ({outcome})", refs=audit_refs,
                metadata={"action_id": action_id, "session_id": session_id, "outcome": outcome, "exit_code": exit_code},
            )
            exited = {**payload, "pid": record.pid, "outcome": outcome, "exit_code": exit_code, "exited_at": _now(),
                     "sprint_takeup_release": released, "audit_exit": audit_exit,
                     "usage_limit_paused": usage_limit_reason is not None}
            self.client.emit("session.exited", action_id=action_id, actor=self.actor, payload=exited)
            if outcome == "completed":
                self.client.complete(action_id, result_ref=f"session={session_id}", actor=self.actor)
            else:
                self.client.fail(action_id, reason=usage_limit_reason or f"daemon session {outcome}", actor=self.actor)
        except Exception as exc:
            self.client.fail(action_id, reason=f"daemon failure: {exc}", actor=self.actor)
            raise
        finally:
            self._child = None
            self._write_state(None)

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
        if self.config.context.remote_only and (project.env or {}).get("SPRINTCTL_BACKEND") != "remote":
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
        actor = f"actionq:{session_id}"
        try:
            assert project is not None
            claim = self.claim_client.start(project, item_id=item_id, actor=actor,
                                             ttl_seconds=ttl_seconds, branch=None)
            claim_id = claim.get("claim_id")
            if claim_id is None and isinstance(claim.get("claim"), dict):
                claim_id = claim["claim"].get("claim_id")
            return {"attempted": True, "status": "ok", "item_id": item_id, "claim_id": claim_id}
        except Exception as exc:
            return {"attempted": True, "status": "failed", "item_id": item_id, "error": str(exc)}

    def _takeup_take(self, project: ProjectConfig | None, session_id: str, pid: int) -> dict[str, Any]:
        if not self.config.takeup.enabled or project is None or project.sprint_id is None:
            return {"attempted": False, "status": "skipped"}
        if self.config.takeup.remote_only and (project.env or {}).get("SPRINTCTL_BACKEND") != "remote":
            return {"attempted": False, "status": "skipped", "reason": "local-mode"}
        actor = f"actionq:{session_id}"
        result = self.takeup_client.take(project, session_id=session_id, actor=actor, pid=pid)
        return {"attempted": True, "status": "ok", "event_id": result.get("event_id")}

    def _takeup_release(self, project: ProjectConfig | None, session_id: str, reason: str) -> dict[str, Any]:
        if not self.config.takeup.enabled or project is None or project.sprint_id is None:
            return {"attempted": False, "status": "skipped"}
        if self.config.takeup.remote_only and (project.env or {}).get("SPRINTCTL_BACKEND") != "remote":
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
    ) -> subprocess.Popen[str]:
        if action.runner in {"fake", "fake-commit"}:
            code = f"import time; time.sleep({action.fake_duration_seconds!r})"
            return subprocess.Popen([sys.executable, "-c", code], text=True, start_new_session=True)
        if action.runner == "command":
            # Deterministic, config-driven runner for usage-limit
            # command-wrapper simulations (#976) -- not a real harness
            # invocation. Output is captured to ``output_path`` so the
            # daemon can classify it for a confirmed usage-limit signal
            # after the child exits.
            if not action.command:
                raise RuntimeError("runner 'command' requires ActionConfig.command")
            handle = None
            stdout_target: Any = subprocess.DEVNULL
            if output_path is not None:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                handle = open(output_path, "w", encoding="utf-8")
                stdout_target = handle
            try:
                return subprocess.Popen(
                    list(action.command),
                    text=True,
                    start_new_session=True,
                    stdout=stdout_target,
                    stderr=subprocess.STDOUT,
                )
            finally:
                if handle is not None:
                    handle.close()
        if action.runner == "harness":
            if project is None or routing is None or prompt is None:
                raise RuntimeError("runner 'harness' requires project, routing, and prompt")
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
            handle = None
            stdout_target: Any = subprocess.DEVNULL
            if output_path is not None:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                handle = open(output_path, "w", encoding="utf-8")
                stdout_target = handle
            try:
                child = subprocess.Popen(
                    adapter.build_command(invocation),
                    cwd=invocation.worktree,
                    env=adapter.build_env(invocation),
                    stdin=subprocess.PIPE if adapter.stdin_text(invocation) is not None else subprocess.DEVNULL,
                    stdout=stdout_target,
                    stderr=subprocess.STDOUT,
                    text=True,
                    start_new_session=True,
                )
                stdin_text = adapter.stdin_text(invocation)
                if stdin_text is not None and child.stdin is not None:
                    try:
                        child.stdin.write(stdin_text)
                    except BrokenPipeError:
                        pass
                    finally:
                        child.stdin.close()
                return child
            finally:
                if handle is not None:
                    handle.close()
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
        project: ProjectConfig | None = None,
        audit_actor: str | None = None,
        audit_refs: Sequence[str] = (),
    ) -> tuple[str, int]:
        assert self._child is not None
        next_heartbeat = time.monotonic() + self.config.heartbeat_interval_seconds
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
            if time.monotonic() >= next_heartbeat:
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
    daemon = Daemon(config, actions, ActionctlClient(config.actionctl_bin), projects,
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
