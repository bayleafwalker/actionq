"""Routing/context resolution boundary for daemon dispatches."""

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

from actionq_contracts import *
from .routing import *
from .daemon_config import *
from .daemon_clients import *
from .daemon_config import _is_shared_sprint_backend


class DaemonRoutingMixin:
    def _context_candidates_request(
        self, project: ProjectConfig | None, action: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Best-effort Tier-1 ``context-candidates`` fetch (item #1116).

        Always fails open: an unreachable or erroring sprintctl only yields a
        "failed" advisory result here and never blocks or fails the action by
        itself -- only a reservation decision derived from a *successfully
        fetched*, explicit, ``reservation_admissible`` target can gate session
        start (see ``_context_reservation_acquire``). Returns ``None`` only when the
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

    def _context_reservation_acquire(
        self,
        project: ProjectConfig | None,
        context_result: dict[str, Any] | None,
        session_id: str,
        ttl_seconds: int,
        *,
        branch: str | None = None,
        exact_target: bool = False,
    ) -> dict[str, Any] | None:
        """Pre-start reservation for an explicit, admissible target only.

        Only ever attempts a reservation for the context packet's
        ``explicit_target`` -- and only when it was both found and marked
        ``reservation_admissible`` by sprintctl itself (rank 1; sprintctl
        never marks an inferred/advisory candidate admissible). This never
        inspects or acts on ranks 2-5. Returns ``None`` when no reservation
        was attempted (feature disabled, no context, no explicit admissible
        target); returns a ``status: "failed"`` result when an attempted
        reservation fails -- callers must treat that as fail-closed and not
        start the child session.
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
            candidate.get("rank") == 1 and candidate.get("reservation_admissible")
            for candidate in packet.get("candidates") or []
        )
        if not eligible:
            return None
        item_id = explicit_target["item_id"]
        actor = f"actionq:{session_id}"
        try:
            assert project is not None
            reservation = self.reservation_client.reserve(
                project, item_id=item_id, actor=actor, role="execution",
                session_id=session_id, correlation_ref=f"actionq:{session_id}",
            )
            reservation_id = reservation.get("id")
            if reservation_id is None:
                raise RuntimeError("sprintctl reservation reserve did not return reservation id")
            self._sprint_reservations[session_id] = SprintReservation(
                project=project, reservation_id=int(reservation_id), actor=actor,
                runtime_session_id=session_id,
            )
            return {
                "attempted": True, "status": "ok", "item_id": item_id,
                "reservation_id": int(reservation_id),
                "conflict": bool(reservation.get("conflict", False)),
                "conflict_severity": reservation.get("conflict_severity", "none"),
                "conflicting_reservations": reservation.get("conflicting_reservations", []),
            }
        except Exception as exc:
            return {"attempted": True, "status": "failed", "item_id": item_id, "error": str(exc)}

    # Compatibility seam for older tests/integrations; it now performs only a
    # credential-free reservation.
    _context_claim_acquire = _context_reservation_acquire

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
                and candidate.get("reservation_admissible") is True
            ),
            None,
        )
        if eligible is None:
            raise RuntimeError(f"exact target item {requested} is not reservation admissible")
        return explicit.get("item") or eligible.get("item") or explicit
