"""Protocol-neutral execution contract.

Nothing here imports ACP. This is the vocabulary ActionQ speaks to any harness; an
adapter translates it. Keeping ACP types out of this module is what makes a future
protocol version a new adapter rather than a migration of ActionQ.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class EventKind(str, Enum):
    """Normalized execution telemetry. A useful subset, not a mirror of the wire."""

    RUNTIME_STARTED = "runtime.started"
    RUNTIME_STOPPED = "runtime.stopped"
    OUTPUT_DELTA = "output.delta"
    REASONING_DELTA = "reasoning.delta"
    TOOL_STARTED = "tool.started"
    TOOL_UPDATED = "tool.updated"
    EXECUTION_OUTPUT = "execution.output"
    POLICY_REQUESTED = "policy.requested"
    POLICY_RESOLVED = "policy.resolved"
    CANCELLATION_REQUESTED = "cancellation.requested"
    USAGE_REPORTED = "usage.reported"
    MODEL_BINDING = "model.binding"
    PROTOCOL_UNMAPPED = "protocol.unmapped"


@dataclass(frozen=True)
class ExecutionEvent:
    """One normalized telemetry event.

    ``raw`` retains the originating wire message for optional debug evidence. It is never
    canonical application state -- Vuoro is not a transcript database.
    """

    kind: EventKind
    payload: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] | None = None


class ContextTier(str, Enum):
    HOT = "hot"
    WARM = "warm"
    COLD = "cold"


@dataclass(frozen=True)
class ContextAddress:
    """A reference, never a payload.

    Embedding WARM/COLD content recreates the eager-injection arm that measured 4.7x
    slower at identical acceptance (local-inference F15/F15b).
    """

    tier: ContextTier
    provider: str
    address: str

    def __post_init__(self) -> None:
        if not self.provider or not self.address:
            raise ValueError("a context address requires both a provider and an address")


@dataclass(frozen=True)
class ContextPolicy:
    """What an execution is entitled and expected to receive."""

    hot_material: tuple[str, ...] = ()
    addresses: tuple[ContextAddress, ...] = ()
    hot_token_ceiling: int = 12288
    promotion_allowed: bool = True

    def addresses_for(self, tier: ContextTier) -> tuple[ContextAddress, ...]:
        return tuple(a for a in self.addresses if a.tier is tier)


@dataclass(frozen=True)
class ExecutionInvariants:
    """Machine-checked, never prompt text.

    Every field here is verified around the harness -- before dispatch and after
    completion -- rather than stated inside a prompt and hoped for. A harness that can be
    pointed at the wrong tree by an inherited environment variable will not be saved by an
    instruction telling it not to be.
    """

    root: str
    revision: str
    permitted_paths: tuple[str, ...] = ()
    acceptance_target: str | None = None

    def __post_init__(self) -> None:
        if not self.root:
            raise ValueError("execution invariants require a root")
        if not self.revision:
            raise ValueError("execution invariants require a pinned revision")


@dataclass(frozen=True)
class ExecutionEnvelope:
    """The sealed unit an adapter is asked to run.

    ``model`` is mandatory and has no default on purpose. The harness will happily supply
    one of its own -- see docs/evidence/2026-08-19-acp-v1-conformance.md section A4, where
    an unspecified session silently bound a hosted model. An execution that does not say
    which model it wants is an invalid state, so it is unrepresentable here.
    """

    execution_id: str
    model: str
    instruction: str
    invariants: ExecutionInvariants
    context_policy: ContextPolicy = field(default_factory=ContextPolicy)
    mode: str | None = None
    mcp_servers: tuple[dict[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if not self.execution_id:
            raise ValueError("an execution envelope requires an ActionQ execution id")
        if not self.model:
            raise ValueError(
                "an execution envelope must name a model; harness defaults are not a choice"
            )
        if not self.instruction:
            raise ValueError("an execution envelope requires an instruction")


class BindingStatus(str, Enum):
    """How strongly the adapter knows the execution ran on the model it asked for."""

    VERIFIED = "verified"
    """The agent reported the bound model back and it matched."""

    ASSERTED = "asserted"
    """The agent accepted the binding but exposes no way to read it back.

    Not the same as verified, and never reported as if it were. See
    docs/evidence/2026-08-19-acp-v1-conformance.md section A2b: OpenCode 1.18.18 has no
    ACP method that returns a session's current model, so this is the best available
    state for the primary conformance target.
    """

    UNSUPPORTED = "unsupported"
    """The agent cannot bind a model at all. Always fatal."""


@dataclass(frozen=True)
class ModelBinding:
    """What model the execution asked for, and how well that is known."""

    requested: str
    status: BindingStatus
    observed: str | None = None
    detail: str | None = None

    @property
    def verified(self) -> bool:
        return self.status is BindingStatus.VERIFIED


@dataclass(frozen=True)
class RuntimeHandle:
    """An external runtime handle. Never a work identity.

    ActionQ owns the execution id; this records only how to reach the running session so
    policy can decide to resume, recreate, or fail. Vuoro correctness must not depend on
    session recovery working.
    """

    protocol: str
    agent: str
    external_session_id: str


class PermissionDecision(str, Enum):
    ALLOW = "allow"
    DENY = "deny"
    ESCALATE = "escalate"


@dataclass(frozen=True)
class ExecutionOutcome:
    """What the adapter reports back. Not an ActionQ outcome -- ActionQ decides that."""

    stop_reason: str
    usage: dict[str, Any] = field(default_factory=dict)
    handle: RuntimeHandle | None = None
    model_binding: ModelBinding | None = None
