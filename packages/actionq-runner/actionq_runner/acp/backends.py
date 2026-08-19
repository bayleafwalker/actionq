"""Declared properties of the ACP backends this repo knows how to launch.

Phase 3 proved one codec drives four agents with no vendor branch. What it did not
provide is anywhere to say *which* agents exist and what is true about each, so the
only such list lived inline in a test. This module is that list.

Two properties here exist to stop specific, observed failures rather than to be
tidy:

``model_namespaces``
    Evidence A4: an ACP ``session/new`` with no model silently binds the harness's
    own default -- a hosted model, real cost, plausible output, no error anywhere.
    ``ExecutionEnvelope`` already makes the *empty* model unrepresentable. This
    closes the neighbouring hole: a model that is spelled fine but belongs to a
    different backend, e.g. dispatching ``local3090/worker-fast`` at Claude Code.
    Checked before launch, so it costs nothing and cannot half-run.

``billing``
    Whether executing here spends money. The cost gate that consumes this is
    protocol-neutral policy and does NOT belong in this package; what belongs here
    is the fact itself, declared once, rather than re-derived from an agent's name
    at each call site.

Note what is deliberately absent: no capability lists, no assurance expectations,
no per-agent behaviour. Those are asked of the running agent and verified, never
declared here -- declaring them would be a vendor branch wearing a data structure.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping

from .v1 import ModelBindingError


class Billing(str, Enum):
    """Whether an execution on this backend costs money."""

    LOCAL = "local"
    HOSTED = "hosted"


class BindingChannel(str, Enum):
    """How the model is bound, which bounds how well the binding can be known."""

    #: `session/set_model`, in-protocol, with a chance of read-back.
    PROTOCOL = "protocol"
    #: Out of band (e.g. spawn environment). The protocol neither accepts nor can
    #: report it, so such a binding can never be better than UNVERIFIABLE.
    ENVIRONMENT = "environment"


@dataclass(frozen=True)
class AcpBackend:
    """One launchable ACP agent, and what is declared true about it."""

    name: str
    command: tuple[str, ...]
    billing: Billing
    #: Allowed model-id prefixes. Empty means unconstrained -- which is a real
    #: choice to make deliberately, not a default to fall into.
    model_namespaces: tuple[str, ...] = ()
    default_mode: str | None = None
    spawn_env: Mapping[str, str] = field(default_factory=dict)
    binding_channel: BindingChannel = BindingChannel.PROTOCOL

    def serves_model(self, model: str) -> bool:
        if not self.model_namespaces:
            return True
        return any(model.startswith(prefix) for prefix in self.model_namespaces)


def check_model_allowed(backend: AcpBackend, model: str) -> None:
    """Refuse a model this backend does not serve, before the agent is launched.

    Fails closed and early on purpose. The alternatives were all considered and
    rejected: aliasing the model onto one the backend does serve is evidence A4
    with extra steps; falling back to another backend is silent substitution; and
    warning-then-continuing is precisely the "rely on noticing" pattern the
    governing principle forbids. Raising here means no session, no prompt, and on
    a hosted backend no billed token.
    """
    if backend.serves_model(model):
        return
    served = ", ".join(backend.model_namespaces)
    raise ModelBindingError(
        f"backend {backend.name!r} does not serve model {model!r} "
        f"(it serves: {served}). Refused before launch: dispatching a model to the "
        f"wrong backend produces plausible output from the wrong engine, which is "
        f"the failure ExecutionEnvelope's required model exists to prevent."
    )


#: Backends this repo knows how to launch.
#:
#: Versions are pinned for the same reason engine images are: an unpinned agent
#: silently changes what an execution measured. `opencode` is unpinned only
#: because it is installed on the host rather than fetched per run.
REGISTRY: dict[str, AcpBackend] = {
    "opencode": AcpBackend(
        name="opencode",
        command=("opencode", "acp"),
        billing=Billing.LOCAL,
        # The local llama-swap lanes, served at 127.0.0.1:8020.
        model_namespaces=("local3090/",),
        default_mode="build",
    ),
    "codex": AcpBackend(
        name="codex",
        command=("npx", "-y", "@agentclientprotocol/codex-acp@1.4.0"),
        billing=Billing.HOSTED,
        model_namespaces=("gpt-",),
        default_mode="agent",
    ),
    "claude-code": AcpBackend(
        name="claude-code",
        command=("npx", "-y", "@zed-industries/claude-code-acp@0.16.2"),
        billing=Billing.HOSTED,
        model_namespaces=("claude-",),
        # Left None until a capture proves the mode vocabulary. `_set_mode` raises
        # on a rejected mode, so guessing one here would turn an unknown into a
        # failed run.
        default_mode=None,
    ),
}
