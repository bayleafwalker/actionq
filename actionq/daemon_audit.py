"""Best-effort audit publication boundary for daemon events."""

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


class DaemonAuditMixin:
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



