"""Runner, publication, and child-process services for the daemon."""

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
from .daemon_config import _audit_refs, _is_shared_sprint_backend, _now


class DaemonRunnerMixin:
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

    @staticmethod
    def _claim_attempt(action: dict[str, Any]) -> str:
        """Return only the attempt identity ActionQ minted with this claim."""
        attempt_id = action.get("attempt_id")
        claim_attempt_id = action.get("claim_attempt_id")
        if not isinstance(claim_attempt_id, str) or not claim_attempt_id:
            raise RuntimeError("ActionQ claim did not include its claim_attempt_id")
        if not isinstance(attempt_id, str) or not attempt_id:
            raise RuntimeError("ActionQ claim did not include its attempt_id")
        if claim_attempt_id != attempt_id:
            raise RuntimeError("ActionQ claim attempt identities disagree")
        return claim_attempt_id

    def _require_verified_settlement(self, action: dict[str, Any]) -> str:
        if not callable(getattr(self.client, "settle", None)):
            raise RuntimeError("verified dispatch settlement is unavailable")
        return self._claim_attempt(action)

    @staticmethod
    def _result_artifact_bytes(
        *, action_id: int, attempt_id: str, terminal_status: str, stop_reason: str | None,
    ) -> bytes:
        """Serialize only the privacy-safe metadata for one dispatch result.

        Action-specific output and prose remain in their existing private
        evidence channels; neither is copied into the durable result object.
        """
        return canonical_bytes(
            {
                "contract_id": DISPATCH_RESULT_V1,
                "action_id": action_id,
                "attempt_id": attempt_id,
                "terminal_status": terminal_status,
                "stop_reason": stop_reason,
            }
        )

    def _cas(self) -> _DaemonCAS:
        root = self.config.artifact_root
        if root is None:
            raise RuntimeError(
                "verified dispatch settlement requires a provisioned durable artifact_root"
            )
        try:
            return _DaemonCAS(root)
        except Exception as exc:  # noqa: BLE001 - invalid durability must fail closed
            raise RuntimeError(
                "verified dispatch settlement requires a valid durable artifact_root"
            ) from exc

    @staticmethod
    def _verify_cas_bytes(store: _DaemonCAS, result_ref: str, expected: bytes | None = None) -> None:
        try:
            actual = store.get(result_ref)
        except Exception as exc:  # noqa: BLE001 - an unresolvable referent cannot settle
            raise RuntimeError("verified dispatch result referent is not durable") from exc
        if expected is not None and actual != expected:
            raise RuntimeError("verified dispatch result referent bytes changed")
        if _artifact_ref(actual) != result_ref:
            raise RuntimeError("verified dispatch result referent digest does not match")

    def _put_result_artifact(self, material: bytes) -> str:
        store = self._cas()
        try:
            result_ref = store.put(material)
        except Exception as exc:  # noqa: BLE001 - do not settle after a failed CAS write
            raise RuntimeError("verified dispatch result artifact could not be durably stored") from exc
        self._verify_cas_bytes(store, result_ref, material)
        return result_ref

    def _verify_result_ref(self, result_ref: str) -> str:
        store = self._cas()
        self._verify_cas_bytes(store, result_ref)
        return result_ref

    def _settle_result(
        self, action: dict[str, Any], *, claim_receipt: str, terminal_status: str,
        result_ref: str | None = None, stop_reason: str | None = None,
    ) -> None:
        attempt_id = self._require_verified_settlement(action)
        if terminal_status not in {"completed", "no_change", "blocked", "failed", "budget_exhausted"}:
            raise RuntimeError(f"unsupported dispatch terminal status: {terminal_status}")
        if stop_reason is not None and stop_reason not in DISPATCH_STOP_REASONS:
            raise RuntimeError(f"unsupported dispatch stop reason: {stop_reason}")
        successful = terminal_status in {"completed", "no_change"}
        if successful and stop_reason is not None:
            raise RuntimeError("successful dispatch settlement cannot contain a stop reason")
        if not successful and stop_reason is None:
            raise RuntimeError("non-successful dispatch settlement requires a stop reason")
        if result_ref is None:
            result_ref = self._put_result_artifact(self._result_artifact_bytes(
                action_id=int(action["id"]), attempt_id=attempt_id,
                terminal_status=terminal_status, stop_reason=stop_reason,
            ))
        else:
            self._verify_result_ref(result_ref)
        self.client.settle(
            int(action["id"]),
            result={
                "contract_id": DISPATCH_RESULT_V1,
                "action_id": int(action["id"]),
                "attempt_id": attempt_id,
                "terminal_status": terminal_status,
                "result_ref": result_ref,
                "result_digest": artifact_digest(result_ref),
                "stop_reason": stop_reason,
            },
            actor=self.config.runner_id,
            claim_receipt=claim_receipt,
        )

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

    def _resume_and_settle_interrupted_publication(self, action: dict[str, Any]) -> bool:
        """Adopt only a previously frozen interrupted attempt under this live claim."""
        if self._publication_policy(action) is None:
            return False
        claim_attempt_id = self._require_verified_settlement(action)
        if self.config.artifact_root is None:
            raise RuntimeError("publish_candidate requires a provisioned artifact_root")
        history = self.client.show(int(action["id"]))
        frozen = {
            (event.get("payload", {}).get("contract", {}).get("attempt_id"),
             event.get("payload", {}).get("contract", {}).get("source_commit"))
            for event in history.get("events", [])
            if event.get("event_type") == "runner.contract.frozen"
        }
        registered_attempts = {
            (event.get("payload", {}).get("attempt_id"),
             event.get("payload", {}).get("journal_ref"))
            for event in history.get("events", [])
            if event.get("event_type") == "publication.registered"
        }
        attempts = self._runnerctl_json(
            "journal-list", "--artifact-root", str(self.config.artifact_root),
            "--action-id", str(action["id"]),
        )
        interrupted = next(
            (value for value in attempts
             if (value.get("attempt_id"), value.get("source_commit")) in frozen
             and value.get("attempt_id") == claim_attempt_id
             and (value.get("attempt_id"), value.get("journal_ref")) not in registered_attempts),
            None,
        )
        if interrupted is None:
            return False
        command = "journal-resume" if interrupted.get("status") == "incomplete" else "journal-recover"
        publication = self._runnerctl_json(
            command, "--artifact-root", str(self.config.artifact_root),
            "--action-id", str(action["id"]), "--attempt-id", str(interrupted["attempt_id"]),
        )
        if publication is None or publication.get("attempt_id") != claim_attempt_id:
            raise RuntimeError("recovered publication attempt is not the live ActionQ claim attempt")
        receipt = str(action.get("claim_receipt") or "")
        if not receipt:
            raise RuntimeError("publication recovery requires a live claim receipt")
        self.client.register_publication(
            int(action["id"]), publication=publication, actor=self.config.runner_id,
            claim_receipt=receipt,
        )
        self._complete_published(
            int(action["id"]), claim_attempt_id=claim_attempt_id,
            claim_receipt=receipt, publication=publication,
        )
        if hasattr(self.client, "reconcile_runner_spool"):
            self.client.reconcile_runner_spool(
                int(action["id"]), attempt_id=str(publication["attempt_id"]),
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
        self, action_id: int, *, claim_attempt_id: str,
        claim_receipt: str, publication: dict[str, Any],
    ) -> None:
        result_ref = str(publication["journal_ref"])
        if not callable(getattr(self.client, "settle", None)):
            raise RuntimeError("verified dispatch settlement is unavailable")
        if publication.get("attempt_id") != claim_attempt_id:
            raise RuntimeError("publication attempt does not match the live ActionQ claim attempt")
        # ``journal-recover`` verifies the publication receipt, but keep this
        # boundary independently fail-closed: a terminal ActionQ event must
        # never point at bytes that this daemon cannot read and re-hash from
        # the provisioned CAS.
        self._verify_result_ref(result_ref)
        dispatch_result = {
            "contract_id": DISPATCH_RESULT_V1,
            "action_id": action_id,
            "attempt_id": str(publication["attempt_id"]),
            "terminal_status": "completed",
            "result_ref": result_ref,
            "result_digest": artifact_digest(result_ref),
            "stop_reason": None,
        }
        try:
            self.client.settle(
                action_id, result=dispatch_result, actor=self.config.runner_id,
                claim_receipt=claim_receipt,
            )
        except Exception:
            history = self.client.show(action_id)
            current = history.get("action", {})
            completed = any(
                event.get("event_type") == "action_completed"
                and event.get("payload", {}).get("result_ref") == result_ref
                and event.get("payload", {}).get("contract_id") == DISPATCH_RESULT_V1
                and event.get("payload", {}).get("attempt_id") == dispatch_result["attempt_id"]
                and event.get("payload", {}).get("result_digest") == dispatch_result["result_digest"]
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
                and event.get("payload", {}).get("contract_id") == DISPATCH_RESULT_V1
                and event.get("payload", {}).get("attempt_id") == dispatch_result["attempt_id"]
                and event.get("payload", {}).get("result_digest") == dispatch_result["result_digest"]
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
        claim_attempt_id = self._require_verified_settlement(action)
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
        if publication.get("attempt_id") != claim_attempt_id:
            return False
        receipt = str(action.get("claim_receipt") or "")
        if not receipt:
            raise RuntimeError("recovered publication settlement requires a live claim receipt")
        self._complete_published(
            int(action["id"]), claim_attempt_id=claim_attempt_id,
            claim_receipt=receipt, publication=publication,
        )
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
        self._verify_result_ref(result_ref)
        completed = any(
            event.get("event_type") == "action_completed"
            and event.get("payload", {}).get("result_ref") == result_ref
            and event.get("payload", {}).get("contract_id") == DISPATCH_RESULT_V1
            and event.get("payload", {}).get("attempt_id") == publication.get("attempt_id")
            and event.get("payload", {}).get("result_digest") == artifact_digest(result_ref)
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
        claim_attempt_id = self._require_verified_settlement(action)
        claim_receipt = str(action.get("claim_receipt") or "")
        runner_auth_token = str(action.get("runner_auth_token") or "")
        if not claim_receipt:
            raise RuntimeError(f"action #{action_id} claim did not include a claim receipt")
        if not runner_auth_token:
            raise RuntimeError(f"action #{action_id} claim did not include runner authentication")
        action_type = str(action["action_type"])
        action_config = self.actions.get(action_type)
        if action_config is None:
            self._settle_result(action, claim_receipt=claim_receipt, terminal_status="blocked", stop_reason="start-failed")
            return
        scope_runners = {"scope-iterate", "oci-scope-iterate"}
        if action_config.publish_candidate and action_config.runner not in scope_runners:
            self._settle_result(action, claim_receipt=claim_receipt, terminal_status="blocked", stop_reason="start-failed")
            return
        if action_config.publish_candidate and self.config.artifact_root is None:
            self._settle_result(action, claim_receipt=claim_receipt, terminal_status="blocked", stop_reason="start-failed")
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
                self._settle_result(action, claim_receipt=claim_receipt, terminal_status="blocked", stop_reason="start-failed")
                return
        if action_config.runner == "oci-scope-iterate":
            try:
                if project is None or action_config.scope_iterate is None:
                    raise RoutingError("runner 'oci-scope-iterate' requires project and scope_iterate policy")
                if not self.config.context.enabled or not self.config.context.auto_claim:
                    raise RoutingError("runner 'oci-scope-iterate' requires exact-target context claims")
                if action.get("target_ref") is None:
                    raise RoutingError("runner 'oci-scope-iterate' requires an exact target_ref")
                if not action_config.command:
                    raise RoutingError("runner 'oci-scope-iterate' requires a registered deterministic command")
                if (not action_config.oci_engine or not action_config.oci_image
                        or not action_config.oci_seccomp_sha256):
                    raise RoutingError("runner 'oci-scope-iterate' requires an engine, pinned image, and seccomp digest")
                if action_config.oci_uid != os.geteuid() or os.geteuid() == 0:
                    raise RoutingError("rootless OCI keep-id requires the nonroot supervisor UID")
                if (action_config.timeout_minutes or self.config.default_timeout_minutes) > 30:
                    raise RoutingError("rootless OCI pilot timeout may not exceed 30 minutes")
            except RoutingError as exc:
                self._settle_result(action, claim_receipt=claim_receipt, terminal_status="blocked", stop_reason="start-failed")
                return
        # The session identity is the ActionQ claim incarnation.  There is no
        # local fallback: a daemon that cannot prove this binding must stop
        # before emitting session state or starting a child.
        session_id = claim_attempt_id
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
        if action_config.runner in scope_runners:
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
                self._settle_result(action, claim_receipt=claim_receipt, terminal_status="blocked", stop_reason="start-failed")
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
            self._settle_result(action, claim_receipt=claim_receipt, terminal_status="blocked", stop_reason="start-failed")
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
            catalog_workaround=routing.catalog_workaround if routing else None,
        )
        output_path = (
            self._output_path(session_id)
            if action_config.runner in {"command", "harness", "scope-iterate", "oci-scope-iterate"}
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
                if action_config.runner in scope_runners and action_config.scope_iterate is not None
                else ()
            ),
        )
        require_compatible(envelope.as_dict())
        self.client.emit(
            "runner.contract.frozen", action_id=action_id, actor=self.actor,
            payload={"session_id": session_id, "contract": envelope.as_dict(), "digest": sha256_digest(envelope)},
        )
        try:
            try:
                managed_prompt = None
                managed_envelope = action.get("managed_dispatch_envelope")
                if managed_envelope is not None:
                    managed_prompt = managed_envelope["managed_request"]["rendered_prompt"]
                    if not isinstance(managed_prompt, str):
                        raise RoutingError("managed dispatch envelope prompt is invalid")
                self._child = self._start_child(
                    action_config,
                    project=evidence_project,
                    routing=routing,
                    prompt=(
                        prepared_scope.prompt if prepared_scope is not None
                        else (managed_prompt if managed_prompt is not None else (str(action["prompt"]) if action.get("prompt") else action_config.prompt))
                    ),
                    output_path=output_path,
                    envelope=envelope,
                )
            except Exception:
                self._record_session_completion(
                    record, outcome="start-failed", exit_code=None
                )
                raise
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
                self._settle_result(
                    action, claim_receipt=claim_receipt, terminal_status="blocked",
                    stop_reason="start-failed",
                )
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
            record.updated_at = _now()
            usage_limit_reason: str | None = None
            session_failure_reason: str | None = None
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
                    session_failure_reason = f"scope-iterate verification failed: {exc}"
            self._record_session_completion(
                record,
                outcome=outcome,
                exit_code=exit_code,
                usage_limited=usage_limit_reason is not None,
            )
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
                self._settle_result(
                    action, claim_receipt=claim_receipt, terminal_status="failed",
                    stop_reason="settlement-failed",
                )
            elif outcome == "completed":
                if publication is not None:
                    self._complete_published(
                        action_id, claim_attempt_id=claim_attempt_id,
                        claim_receipt=claim_receipt, publication=publication,
                    )
                else:
                    self._settle_result(
                        action, claim_receipt=claim_receipt,
                        terminal_status=("no_change" if scope_result is not None and not scope_result.changed_paths else "completed"),
                    )
            else:
                self._settle_result(
                    action, claim_receipt=claim_receipt,
                    terminal_status=(
                        "budget_exhausted" if usage_limit_reason is not None else "failed"
                    ),
                    stop_reason=(
                        "usage-limit" if usage_limit_reason is not None
                        else "verification-failed" if session_failure_reason is not None
                        else "crash-inferred" if outcome == "end-inferred"
                        else "timeout" if outcome == "timed-out"
                        else "process-exit"
                    ),
                )
            if outcome not in {"claim-lost"} and settlement_error is None and hasattr(self.client, "reconcile_runner_spool"):
                self.client.reconcile_runner_spool(action_id, attempt_id=session_id)
            if (outcome == "completed" and settlement_error is None and publication is not None
                    and action_config.runner == "oci-scope-iterate" and prepared_scope is not None):
                self._destroy_oci_workspace(action_id, session_id, prepared_scope)
        except Exception:
            # A daemon exception is not permission to use a legacy terminal
            # command.  Reconcile an already committed verified decision; if
            # none exists, make one bounded coded settlement attempt. A
            # missing capability or stale claim remains fail-closed.
            try:
                current = self.client.show(action_id).get("action", {})
            except Exception:
                current = {}
            if current.get("status") not in {"completed", "failed", "rejected", "cancelled"}:
                try:
                    self._settle_result(
                        action, claim_receipt=claim_receipt, terminal_status="failed",
                        stop_reason="settlement-failed",
                    )
                except Exception:
                    pass
            raise
        finally:
            self._sprint_claim_leases.pop(session_id, None)
            self._child = None
            cleanup_ok = True
            if action_config.runner == "oci-scope-iterate":
                try:
                    self._cleanup_oci_container(
                        action_config, action_id=action_id, attempt_id=session_id,
                    )
                except RuntimeError as exc:
                    cleanup_ok = False
                    self.client.emit(
                        "workspace.destruction_failed", action_id=action_id, actor=self.actor,
                        payload={"attempt_id": session_id, "detail": str(exc)},
                    )
            if cleanup_ok:
                self._write_state(None)

    @staticmethod
    def _oci_container_name(action_id: int, attempt_id: str) -> str:
        digest = hashlib.sha256(f"{action_id}\0{attempt_id}".encode()).hexdigest()[:20]
        return f"actionq-{digest}"

    def _cleanup_oci_container(
        self, action: ActionConfig, *, action_id: int, attempt_id: str,
    ) -> None:
        """Idempotently remove a deterministic attempt container after runner death."""
        if not action.oci_engine:
            raise RuntimeError("OCI cleanup requires the configured engine")
        name = self._oci_container_name(action_id, attempt_id)
        completed = subprocess.run(
            [action.oci_engine, "rm", "--force", "--ignore", name],
            text=True, capture_output=True, check=False,
        )
        if completed.returncode:
            raise RuntimeError(completed.stderr.strip() or "OCI container cleanup was not observed")

    def _destroy_oci_workspace(
        self, action_id: int, attempt_id: str, prepared: PreparedScopeIterate,
    ) -> None:
        """Remove only the exact supervisor-created worktree after settlement."""
        workspace = prepared.worktree
        root = prepared.policy.worktree_root.resolve()
        resolved = workspace.resolve(strict=True)
        if workspace.is_symlink() or root not in resolved.parents:
            raise RuntimeError("refusing to destroy an untrusted OCI workspace path")
        completed = subprocess.run(
            ["git", "-C", str(prepared.request.repository), "worktree", "remove", "--force", str(resolved)],
            text=True, capture_output=True, check=False,
        )
        if completed.returncode or resolved.exists():
            raise RuntimeError(completed.stderr.strip() or "OCI workspace destruction was not observed")
        self.client.emit(
            "workspace.destroyed", action_id=action_id, actor=self.actor,
            payload={"attempt_id": attempt_id, "workspace_destroyed": True},
        )


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
            allowed_environment = {
                "HOME", "LANG", "LC_ALL", "LC_CTYPE", "OPENCODE_CONFIG",
                "PATH", "TERM", "TMPDIR", "XDG_CACHE_HOME", "XDG_CONFIG_HOME",
                "XDG_STATE_HOME",
            }
            source = environment if environment is not None else os.environ
            clean_env = {
                key: value for key, value in source.items()
                if key in allowed_environment
            }
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
        if action.runner == "oci-scope-iterate":
            if (project is None or not action.command or not action.oci_engine
                    or not action.oci_image or not action.oci_seccomp_sha256):
                raise RuntimeError(
                    "runner 'oci-scope-iterate' requires workspace, command, engine, image, and seccomp digest"
                )
            packet = {
                "engine": action.oci_engine,
                "image": action.oci_image,
                "seccomp_sha256": action.oci_seccomp_sha256,
                "envelope": envelope.as_dict(),
                "registered_command_id": envelope.command_id,
                "deterministic": True,
                "command": list(action.command),
                "workspace": str(project.path),
                "network": "none",
                "uid": action.oci_uid,
                "environment": {"HOME": "/nonexistent", "LANG": "C.UTF-8", "PATH": "/usr/local/bin:/usr/bin:/bin"},
                "limits": {
                    "cpus": action.oci_cpus,
                    "memory_bytes": action.oci_memory_bytes,
                    "pids": action.oci_pids,
                    "disk_bytes": action.oci_disk_bytes,
                    "timeout_seconds": (action.timeout_minutes or self.config.default_timeout_minutes) * 60,
                },
            }
            output_target = output_path or self._output_path(str(envelope.attempt_id))
            output_target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            output_stream = output_target.open("w", encoding="utf-8")
            try:
                child = subprocess.Popen(
                    [self.config.runnerctl_bin, "oci-execute", "--packet-stdin"],
                    stdin=subprocess.PIPE, stdout=output_stream, stderr=subprocess.STDOUT,
                    text=True, start_new_session=True,
                )
            finally:
                output_stream.close()
            assert child.stdin is not None
            child.stdin.write(json.dumps(packet, sort_keys=True))
            child.stdin.close()
            return child
        if action.runner in {"harness", "scope-iterate"}:
            if project is None or routing is None or prompt is None:
                raise RuntimeError(f"runner {action.runner!r} requires project, routing, and prompt")
            harness_route = (self.config.routing.harnesses or {}).get(routing.harness)
            adapter_options: dict[str, Any] = {
                "bin_path": harness_route.bin if harness_route else None,
            }
            if routing.harness == "codex":
                adapter_options["catalog_workaround"] = (
                    harness_route.catalog_workaround if harness_route else None
                )
            adapter = get_adapter(routing.harness, **adapter_options)
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
        deadline = time.monotonic() + float(payload.get("ttl_seconds", 0))
        while self._child.poll() is None:
            if time.monotonic() >= deadline:
                os.killpg(self._child.pid, signal.SIGTERM)
                try:
                    self._child.wait(timeout=self.config.graceful_shutdown_seconds)
                except subprocess.TimeoutExpired:
                    os.killpg(self._child.pid, signal.SIGKILL)
                    self._child.wait()
                return "timed-out", int(self._child.returncode or 1)
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


