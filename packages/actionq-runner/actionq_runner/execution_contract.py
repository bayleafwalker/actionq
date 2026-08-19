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
    BOUNDARY_CHECKED = "boundary.checked"
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
    """What an execution is entitled and expected to receive.

    The invariant this exists to hold is *not* a token maximum:

        Bulky context outside HOT must remain addressable rather than silently
        becoming model-visible content.

    ``hot_budget`` is a measured parameter, currently 12 288 (local-inference F15/F15b),
    and is expected to be re-derived. The tier separation is the architecture; the number
    is a setting. Eager injection of WARM/COLD measured 4.7x slower at identical
    acceptance, and nothing errored -- which is why this is enforced rather than advised.
    """

    hot_material: tuple[str, ...] = ()
    addresses: tuple[ContextAddress, ...] = ()
    hot_budget: int = 12288
    promotion_allowed: bool = True
    promotion_budget: int = 0
    """Additional tokens WARM/COLD retrieval may promote into view. 0 means unbounded."""

    def addresses_for(self, tier: ContextTier) -> tuple[ContextAddress, ...]:
        return tuple(a for a in self.addresses if a.tier is tier)


@dataclass(frozen=True)
class ContextUsage:
    """Accounting for one execution's context, with the harness's share broken out.

    Total request length and task-controlled allocation are different quantities, and
    conflating them makes a budget ambiguous. Separating them is also what lets a future
    harness release whose preamble grows be *detected* rather than silently absorbed.

    ``harness_tokens`` is whatever the backend adds on its own account -- system prompt,
    tool definitions, conversation scaffolding. It carries an assurance level because on
    the observed harness it is **not reliably reportable**: OpenCode's ACP
    ``usage.inputTokens`` tracks cache state, not request size, and returned 516 then 4,
    4, 4 for an identical request while the inference server reported a steady 22. See
    docs/evidence/2026-08-19-acp-v1-conformance.md section A8.
    """

    hot_tokens: int = 0
    promoted_tokens: int = 0
    harness_tokens: int | None = None
    harness_assurance: "Assurance" = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.harness_assurance is None:
            object.__setattr__(self, "harness_assurance", Assurance.UNVERIFIABLE)

    @property
    def task_tokens(self) -> int:
        """What the envelope actually allocated. The part policy controls."""
        return self.hot_tokens + self.promoted_tokens

    @property
    def total_tokens(self) -> int | None:
        if self.harness_tokens is None:
            return None
        return self.task_tokens + self.harness_tokens


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


class Assurance(str, Enum):
    """How strongly one execution property is known to hold.

    Assurance is per-property, not per-session. A backend attests to different properties
    with different strength -- OpenCode can prove the working root but not the bound model
    -- and a single "trusted session" flag would average those into a lie. As ACP
    implementations diverge, the granularity is what ages well.

    The rule this encodes: declare what must be true, ask the backend what it can attest,
    verify everything it exposes, and explicitly label the rest. Absence of evidence must
    never quietly become runtime trust.
    """

    VERIFIED = "verified"
    """Observed to hold, independently of what the agent claims about itself."""

    ASSERTED = "asserted"
    """The agent accepted the instruction but exposes no way to confirm it took."""

    UNVERIFIABLE = "unverifiable"
    """The property matters and the backend offers no means to check it at all."""

    UNSUPPORTED = "unsupported"
    """The backend cannot provide the property. Always fatal."""


# Retained name: model binding was the first property to need this distinction.
BindingStatus = Assurance


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
