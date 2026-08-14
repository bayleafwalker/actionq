"""Configuration and persisted session records for the actionq daemon."""

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
    completion_outbox_path: Path = DEFAULT_OUTBOX_PATH
    actionctl_bin: str = "actionctl"
    runnerctl_bin: str = "actionq-runner"
    runner_private_key_path: Path = Path("~/.local/state/actionq/runner-identity.pem")
    runner_id: str = "runner:devbox"
    enforce_worker_isolation: bool = True
    # Explicit durable CAS root for both runner publications and the
    # privacy-minimal dispatch-result artifacts used by terminal settlement.
    # A missing or invalid root is fail-closed: the daemon must not settle a
    # result whose immutable referent cannot be proven to exist.
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
    # #2033 offline proof: a pinned image executed by a proven rootless OCI
    # engine. The deterministic command is the only worker payload; model
    # credentials and network egress are deliberately out of scope.
    oci_engine: str | None = None
    oci_image: str | None = None
    oci_uid: int | None = None
    oci_cpus: float = 2.0
    oci_memory_bytes: int = 2 * 1024**3
    oci_pids: int = 128
    oci_disk_bytes: int = 10 * 1024**3
    oci_seccomp_sha256: str | None = None


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
    catalog_workaround: str | None = None

    def routing_provenance(self) -> dict[str, str | None] | None:
        if self.harness is None:
            return None
        return {
            "requested_selector": self.requested_selector,
            "trusted_caller_harness": self.caller_harness,
            "resolved_harness": self.harness,
            "resolved_provider": self.provider,
            "resolved_model": self.model,
            "transport": self.transport,
            "surface": self.surface,
            "routing_source": self.routing_source,
            "fallback_model": self.fallback_model,
            "fallback_reason": self.fallback_reason,
            "catalog_workaround": self.catalog_workaround,
        }

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
    harnesses: dict[str, HarnessRoute] = {}
    for name, value in raw.get("harnesses", {}).items():
        catalog_workaround = (
            str(value["catalog_workaround"])
            if "catalog_workaround" in value else None
        )
        if catalog_workaround is not None and (
            name != "codex" or catalog_workaround != LUNA_V2_CATALOG_WORKAROUND
        ):
            raise ValueError(
                f"unsupported catalog_workaround for harness {name!r}: {catalog_workaround!r}"
            )
        harnesses[name] = HarnessRoute(
            name=name,
            bin=(str(value["bin"]) if value.get("bin") else None),
            provider=(str(value["provider"]) if value.get("provider") else None),
            transport=(str(value["transport"]) if value.get("transport") else None),
            surface=(str(value["surface"]) if value.get("surface") else None),
            catalog_workaround=catalog_workaround,
        )
    config = DaemonConfig(
        poll_interval_seconds=float(global_raw.get("poll_interval_seconds", 30)),
        heartbeat_interval_seconds=float(global_raw.get("heartbeat_interval_seconds", 60)),
        cancellation_poll_interval_seconds=float(global_raw.get("cancellation_poll_interval_seconds", 2)),
        graceful_shutdown_seconds=float(global_raw.get("graceful_shutdown_seconds", 30)),
        default_timeout_minutes=int(global_raw.get("default_timeout_minutes", 30)),
        session_state_path=state_path,
        pause_file=pause_file,
        handoff_dir=handoff_dir,
        completion_outbox_path=Path(
            global_raw.get("completion_outbox_path", DEFAULT_OUTBOX_PATH)
        ).expanduser(),
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
            oci_engine=(str(value["oci_engine"]) if "oci_engine" in value else None),
            oci_image=(str(value["oci_image"]) if "oci_image" in value else None),
            oci_uid=(int(value["oci_uid"]) if "oci_uid" in value else None),
            oci_cpus=float(value.get("oci_cpus", 2)),
            oci_memory_bytes=int(value.get("oci_memory_bytes", 2 * 1024**3)),
            oci_pids=int(value.get("oci_pids", 128)),
            oci_disk_bytes=int(value.get("oci_disk_bytes", 10 * 1024**3)),
            oci_seccomp_sha256=(str(value["oci_seccomp_sha256"]) if "oci_seccomp_sha256" in value else None),
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
