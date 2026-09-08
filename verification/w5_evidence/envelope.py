"""Shared envelope types for W5 evidence-capture tooling.

Two record types exist, deliberately NOT interchangeable and with no code
path that converts one into the other:

* ``RepositoryCheckRecord`` (schema_version ``"w5-repository-check/v1"``) --
  produced entirely offline from the source tree.  It proves code intent,
  never deployment truth.
* ``CapturedEvidenceRecord`` (schema_version ``"w5-captured-evidence/v1"``)
  -- produced only by an operator with a live endpoint, real credentials,
  and the actually-installed artifact under test.  It proves deployment
  truth.

This is the direct answer to the freeze falsifier at
docs/plans/2026-08-20-tranche4-federation-storage-contract-freeze.md:553-556:
"a repository test is presented as deployment evidence" must be structurally
impossible, not merely discouraged by convention.

Every binding field on both dataclasses is required (no defaults) and is
validated in ``__post_init__`` so that an unpopulated, empty, or obviously
literal-filled ("TODO", "unknown", "TBD", ...) value raises ``ValueError``
at construction time. Nothing here reads a live endpoint, a credential, or
an installed wheel -- callers in capture_w5_evidence.py do that and hand the
already-populated, real values in.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[2]

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ISO8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)
_LITERAL_FILL_VALUES = {
    "",
    "unknown",
    "unset",
    "todo",
    "tbd",
    "n/a",
    "na",
    "none",
    "null",
    "fixme",
    "xxx",
    "placeholder",
    "changeme",
    "redacted",
    "fake",
    "example",
    "dummy",
    "test",
    "simulated",
}


def _reject_literal_fill(field_name: str, value: str) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string, got {type(value).__name__}")
    if value.strip().lower() in _LITERAL_FILL_VALUES:
        raise ValueError(f"{field_name} is unset or literal-filled: {value!r}")


def _require_nonempty_str(field_name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string, got {value!r}")
    _reject_literal_fill(field_name, value)
    return value


def _require_sha256(field_name: str, value: Any) -> str:
    text = _require_nonempty_str(field_name, value)
    if not _SHA256_RE.fullmatch(text):
        raise ValueError(f"{field_name} must be a lowercase sha256 hex digest, got {value!r}")
    return text


def _require_iso8601(field_name: str, value: Any) -> str:
    text = _require_nonempty_str(field_name, value)
    if not _ISO8601_RE.fullmatch(text):
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp with timezone, got {value!r}")
    return text


def _require_mapping(field_name: str, value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or not value:
        raise ValueError(f"{field_name} must be a non-empty mapping, got {value!r}")
    return value


def binding_digest(*, schema_version: str, record_id: str, captured_at: str,
                    environment: Mapping[str, Any], actor: "Actor", tool_versions: "ToolVersions",
                    payload: Mapping[str, Any], extra: Mapping[str, Any] | None = None) -> str:
    """The sha256 a record's ``result_digest`` must equal.

    Computed over every binding field -- schema_version, record_id,
    captured_at, environment, actor, tool_versions, payload, and (for
    CapturedEvidenceRecord) the deployment-binding fields passed as
    ``extra`` -- so that changing any of them without recomputing the
    digest is detectable at reconstruction time, not just changing the
    payload.
    """
    material: dict[str, Any] = {
        "schema_version": schema_version,
        "record_id": record_id,
        "captured_at": captured_at,
        "environment": dict(environment),
        "actor": {"identity": actor.identity, "method": actor.method},
        "tool_versions": dict(tool_versions.versions),
        "payload": dict(payload),
    }
    if extra:
        material.update(extra)
    canonical = json.dumps(material, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


@dataclass(frozen=True)
class Actor:
    """The identity performing the capture -- never a secret, always a real read."""

    identity: str
    method: str  # e.g. "os.environ[USER]", "git config user.email", "operator-supplied"

    def __post_init__(self) -> None:
        _require_nonempty_str("actor.identity", self.identity)
        _require_nonempty_str("actor.method", self.method)


@dataclass(frozen=True)
class ToolVersions:
    """Versions of the tools that produced a record, as an immutable mapping."""

    versions: Mapping[str, str]

    def __post_init__(self) -> None:
        mapping = _require_mapping("tool_versions", self.versions)
        for key, value in mapping.items():
            _require_nonempty_str(f"tool_versions[{key}]", value)
        object.__setattr__(self, "versions", dict(mapping))

    def as_dict(self) -> dict[str, str]:
        return dict(self.versions)


@dataclass(frozen=True)
class RepositoryCheckRecord:
    """Offline, source-tree-derived record. Proves code intent, not deployment truth.

    schema_version is pinned to a literal so no other schema string can be
    constructed through this type.
    """

    schema_version: str
    record_id: str
    captured_at: str
    environment: Mapping[str, Any]
    actor: Actor
    tool_versions: ToolVersions
    payload: Mapping[str, Any]
    result_digest: str

    def __post_init__(self) -> None:
        if self.schema_version != "w5-repository-check/v1":
            raise ValueError(
                "RepositoryCheckRecord.schema_version must be exactly "
                f"'w5-repository-check/v1', got {self.schema_version!r}"
            )
        _require_nonempty_str("record_id", self.record_id)
        _require_iso8601("captured_at", self.captured_at)
        environment = _require_mapping("environment", self.environment)
        for required in ("git_commit", "git_branch", "repo_root"):
            if required not in environment:
                raise ValueError(f"environment is missing required key {required!r}")
            _require_nonempty_str(f"environment[{required}]", environment[required])
        if not isinstance(self.actor, Actor):
            raise ValueError("actor must be an Actor instance")
        if not isinstance(self.tool_versions, ToolVersions):
            raise ValueError("tool_versions must be a ToolVersions instance")
        payload = _require_mapping("payload", self.payload)
        object.__setattr__(self, "environment", dict(environment))
        object.__setattr__(self, "payload", dict(payload))
        _require_sha256("result_digest", self.result_digest)
        expected_digest = binding_digest(
            schema_version=self.schema_version,
            record_id=self.record_id,
            captured_at=self.captured_at,
            environment=self.environment,
            actor=self.actor,
            tool_versions=self.tool_versions,
            payload=self.payload,
        )
        if self.result_digest != expected_digest:
            raise ValueError(
                "result_digest does not match the record's own binding fields "
                f"(schema_version, record_id, captured_at, environment, actor, "
                f"tool_versions, payload); recomputed {expected_digest!r}, got "
                f"{self.result_digest!r}. A record's digest must be computed over its "
                "own binding fields, not supplied independently."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_id": self.record_id,
            "captured_at": self.captured_at,
            "environment": dict(self.environment),
            "actor": {"identity": self.actor.identity, "method": self.actor.method},
            "tool_versions": self.tool_versions.as_dict(),
            "payload": dict(self.payload),
            "result_digest": self.result_digest,
        }


@dataclass(frozen=True)
class CapturedEvidenceRecord:
    """Live-capture record. Proves deployment truth, never source-tree intent.

    Every field here must come from a real subprocess/environment/network
    read performed by an operator against a live target. No default exists
    for any field -- construction fails if a caller has nothing real to
    supply.
    """

    schema_version: str
    record_id: str
    captured_at: str
    environment: Mapping[str, Any]
    database_endpoint_fingerprint: str
    actionq_release: str
    actionq_deployment_revision: str
    vuoro_release: str
    vuoro_deployment_revision: str
    actor: Actor
    tool_versions: ToolVersions
    payload: Mapping[str, Any]
    result_digest: str

    def __post_init__(self) -> None:
        if self.schema_version != "w5-captured-evidence/v1":
            raise ValueError(
                "CapturedEvidenceRecord.schema_version must be exactly "
                f"'w5-captured-evidence/v1', got {self.schema_version!r}"
            )
        _require_nonempty_str("record_id", self.record_id)
        _require_iso8601("captured_at", self.captured_at)
        environment = _require_mapping("environment", self.environment)
        for required in ("cluster", "namespace_or_scope"):
            if required not in environment:
                raise ValueError(f"environment is missing required key {required!r}")
            _require_nonempty_str(f"environment[{required}]", environment[required])
        _require_sha256("database_endpoint_fingerprint", self.database_endpoint_fingerprint)
        _require_nonempty_str("actionq_release", self.actionq_release)
        _require_nonempty_str("actionq_deployment_revision", self.actionq_deployment_revision)
        _require_nonempty_str("vuoro_release", self.vuoro_release)
        _require_nonempty_str("vuoro_deployment_revision", self.vuoro_deployment_revision)
        if not isinstance(self.actor, Actor):
            raise ValueError("actor must be an Actor instance")
        if not isinstance(self.tool_versions, ToolVersions):
            raise ValueError("tool_versions must be a ToolVersions instance")
        payload = _require_mapping("payload", self.payload)
        object.__setattr__(self, "environment", dict(environment))
        object.__setattr__(self, "payload", dict(payload))
        _require_sha256("result_digest", self.result_digest)
        expected_digest = binding_digest(
            schema_version=self.schema_version,
            record_id=self.record_id,
            captured_at=self.captured_at,
            environment=self.environment,
            actor=self.actor,
            tool_versions=self.tool_versions,
            payload=self.payload,
            extra={
                "database_endpoint_fingerprint": self.database_endpoint_fingerprint,
                "actionq_release": self.actionq_release,
                "actionq_deployment_revision": self.actionq_deployment_revision,
                "vuoro_release": self.vuoro_release,
                "vuoro_deployment_revision": self.vuoro_deployment_revision,
            },
        )
        if self.result_digest != expected_digest:
            raise ValueError(
                "result_digest does not match the record's own binding fields "
                "(schema_version, record_id, captured_at, environment, actor, "
                "tool_versions, payload, database_endpoint_fingerprint, "
                "actionq_release, actionq_deployment_revision, vuoro_release, "
                f"vuoro_deployment_revision); recomputed {expected_digest!r}, got "
                f"{self.result_digest!r}. A record's digest must be computed over its "
                "own binding fields, not supplied independently."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_id": self.record_id,
            "captured_at": self.captured_at,
            "environment": dict(self.environment),
            "database_endpoint_fingerprint": self.database_endpoint_fingerprint,
            "actionq_release": self.actionq_release,
            "actionq_deployment_revision": self.actionq_deployment_revision,
            "vuoro_release": self.vuoro_release,
            "vuoro_deployment_revision": self.vuoro_deployment_revision,
            "actor": {"identity": self.actor.identity, "method": self.actor.method},
            "tool_versions": self.tool_versions.as_dict(),
            "payload": dict(self.payload),
            "result_digest": self.result_digest,
        }


def git_commit(root: Path = ROOT) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()


def git_branch(root: Path = ROOT) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=root, text=True
    ).strip()


def now_iso8601() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def local_actor() -> Actor:
    import getpass

    try:
        identity = subprocess.check_output(
            ["git", "config", "user.email"], cwd=ROOT, text=True
        ).strip()
        method = "git config user.email"
    except (subprocess.CalledProcessError, OSError):
        identity = getpass.getuser()
        method = "getpass.getuser"
    if not identity:
        identity = getpass.getuser()
        method = "getpass.getuser"
    return Actor(identity=identity, method=method)
