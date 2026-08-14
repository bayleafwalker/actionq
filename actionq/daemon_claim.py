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
