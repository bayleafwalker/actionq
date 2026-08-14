"""Sprint claim and context-client boundary used by the daemon."""

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
from .daemon_config import _now


class DaemonClaimMixin:
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



