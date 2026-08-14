"""Daemon lifecycle, state recovery, and polling loop services."""

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
from .daemon_clients import *
from .daemon_config import _audit_refs, _is_shared_sprint_backend, _now


from .daemon_config import *
from .daemon_clients import *
from .daemon_config import _is_shared_sprint_backend, _now


class DaemonLifecycleMixin:
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
        self._completion_outbox_path = (
            config.session_state_path.parent / "completion-outbox.sqlite3"
            if config.completion_outbox_path == DEFAULT_OUTBOX_PATH
            else config.completion_outbox_path
        )

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

    def _record_session_completion(
        self,
        record: SessionRecord,
        *,
        outcome: str,
        exit_code: int | None,
        usage_limited: bool = False,
    ) -> bool:
        """Record one owner completion fact; never changes coordinator outcome."""

        logical_id = f"daemon:{record.session_id}"
        try:
            outbox = CompletionOutbox(self._completion_outbox_path)
        except Exception as exc:
            print(
                f"actionq.daemon: failed to open session completion outbox: {type(exc).__name__}",
                file=sys.stderr,
            )
            return False
        completed_at = record.updated_at
        started_at = record.started_at or record.updated_at
        if outcome == "end-inferred":
            terminal = ("end-inferred", None, "crash-inferred", False)
        elif outcome == "start-failed":
            terminal = ("failed", None, "start-failed", False)
        elif usage_limited:
            terminal = ("usage-limited", None, "usage-limit", False)
        elif outcome == "cancelled":
            terminal = ("cancelled", None, "cancelled", False)
        elif outcome == "timed-out":
            terminal = ("timed-out", None, "timeout", False)
        elif outcome == "completed":
            terminal = ("succeeded", 0, "completed", False)
        else:
            terminal = ("failed", exit_code or 1, "process-exit", False)
        try:
            start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            end = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
            duration_ms = max(0, int((end - start).total_seconds() * 1000))
        except ValueError:
            duration_ms = None
        fields = {
            "runtime_session_id": record.runtime_session_id,
            "attempt_id": record.session_id,
            "action_id": str(record.action_id),
            "repo": {"project": record.project or "unknown"},
            "harness": record.harness or record.runner,
            "model": {"name": record.model} if record.model else None,
            "terminal": {
                "kind": terminal[0],
                "exit_code": terminal[1],
                "reason_code": terminal[2],
                "retryable": terminal[3],
            },
            "started_at": started_at,
            "completed_at": completed_at,
            "observed_at": completed_at,
            "duration_ms": duration_ms,
            "refs": [],
            "evidence": {
                "dirty": False,
                "commit_count": 0,
                "verification": {"pass": 0, "fail": 0, "error": 0},
            },
            "privacy": {
                "prompt_absent": True,
                "transcript_absent": True,
                "raw_output_absent": True,
                "environment_absent": True,
                "credentials_absent": True,
                "absolute_paths_absent": True,
                "claim_proofs_absent": True,
            },
        }
        try:
            outbox.record(logical_id, fields)
        except Exception as exc:
            print(
                f"actionq.daemon: failed to record session completion: {type(exc).__name__}",
                file=sys.stderr,
            )
            return False
        return True

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
        if record.runner == "oci-scope-iterate":
            action_config = self.actions.get(record.action_type)
            if action_config is None or not action_config.oci_engine:
                return True
            try:
                self._cleanup_oci_container(
                    action_config, action_id=record.action_id, attempt_id=record.session_id,
                )
            except RuntimeError:
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
        record.updated_at = _now()
        self._write_state(record)
        self._record_session_completion(
            record, outcome="end-inferred", exit_code=None
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
                "routing": record.routing_provenance(),
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
        self._require_verified_settlement(action)
        if self._resume_and_settle_interrupted_publication(action):
            return True
        if self._settle_recovered_publication(action):
            return True
        self._run_action(action)
        return True


    def run_forever(self) -> None:
        while not self._shutdown:
            if self._reload_requested and self._child is None and self._reload_config:
                self.config, self.actions, self.projects = self._reload_config()
                self._reload_requested = False
            claimed = self.run_once()
            if not claimed:
                time.sleep(self.config.poll_interval_seconds)


