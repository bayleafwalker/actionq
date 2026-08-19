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

Scope, because the name understates it and overstates it at once: this registry
covers backends reached **over ACP**. ACP is one integration path, not the only
one -- `actionq/harnesses/` holds thin native adapters that invoke vendor CLIs
directly, which for a purchased subscription is usually the less invasive and more
capable route (finding A19). A lane being absent here does not mean it cannot be
routed to; it means it is not reached this way.

Conversely, `BindingAssurance` and the execution/supervision/interactive flags are
NOT ACP concepts. They belong to any lane. They live here only because ACP is
currently the sole consumer; when a second integration registers, they should move
up beside the harness layer.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping

from .v1 import ModelBindingError


class BindingAssurance(str, Enum):
    """How well it is known that a lane ran the model that was asked for.

    A ladder, not a gate. A lane low on it is not disqualified -- capability
    absence changes assurance, it does not fail the run (A11) -- but a policy may
    require a rung, and the recorded value must never be rounded up.
    """

    #: Nothing was requested or checked.
    NONE = "none"
    #: A model was requested and the backend said ok. Claude's `session/set_model`
    #: lives here and no higher: it says ok to any string (A13).
    REQUESTED = "requested"
    #: The harness reported back what it resolved, and that was checked against
    #: the request before any prompt was sent. Claude via cwd-scoped settings
    #: (A17) and Codex via `session/set_model` reach this.
    HARNESS_VERIFIED = "harness-verified"
    #: The serving side was observed independently of the harness's own claim.
    #: Only reachable where the endpoint is ours to inspect -- i.e. the local
    #: llama-swap lanes (A5). Hosted models cannot reach this, and pretending
    #: otherwise would be inventing attestation.
    END_TO_END_VERIFIED = "end-to-end-verified"


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
    #: Bound by writing `<cwd>/.claude/settings.json` before `session/new`, and
    #: verified by comparing the session's reported `models.currentModelId`
    #: against what was requested. Measured on Claude Code ACP 0.16.2 (A17): the
    #: read-back is honest and catches mis-resolution, but it is only sound when
    #: paired with an advertised-id check -- see `verify_bound_model`.
    SETTINGS = "settings"
    #: `session/set_model` exists and returns success, but validates nothing and
    #: cannot be read back -- so the success response is not evidence of anything.
    #: Measured on Claude Code ACP 0.16.2 (finding A13): it accepted every id
    #: offered including another vendor's, a local profile name, obvious garbage
    #: and the empty string, while session/status and session/info are both
    #: method_not_found. This is worse than an outright missing set_model, which
    #: at least fails closed and loudly.
    UNVALIDATED = "unvalidated"


@dataclass(frozen=True)
class AcpBackend:
    """One launchable ACP agent, and what is declared true about it."""

    name: str
    command: tuple[str, ...]
    billing: Billing
    #: What this lane may be used FOR. A lane is not only a queue consumer: a
    #: frontier subscription is most valuable supervising work it did not
    #: personally perform. These are orthogonal -- a lane can supervise without
    #: being a good bounded-execution target, and vice versa.
    execution: bool = True
    supervision: bool = False
    interactive: bool = False
    binding_assurance: BindingAssurance = BindingAssurance.REQUESTED
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


def binding_is_trustworthy(backend: AcpBackend) -> bool:
    """Whether this backend's model binding can be believed at all.

    False for UNVALIDATED: the agent says "ok" to any id and offers no read-back,
    so nothing about the response distinguishes a correct binding from a silently
    wrong one. A policy requiring a verified model must refuse such a backend
    rather than record its cheerful success as an assertion.
    """
    return backend.binding_channel is not BindingChannel.UNVALIDATED


class ModelResolutionError(ModelBindingError):
    """The session did not end up bound to the model that was requested."""


def verify_bound_model(
    requested: str, current_model_id: str, advertised: "list[str] | tuple[str, ...]"
) -> None:
    """Check that a session actually bound the model that was asked for.

    Both halves are load-bearing, and neither is sufficient alone (finding A17):

    1. **The requested id must be one the agent advertised.** An unrecognised id is
       not rejected -- Claude Code appends it to its own roster and adopts it
       verbatim, so it round-trips cleanly through the read-back while being
       something the backend never validated.
    2. **The reported id must equal the requested one.** The agent's matcher does
       substring matching in both directions and returns the FIRST hit, so
       `sonnet-5` resolves to `sonnet` even though an exact `sonnet-5` entry
       exists further down the list. Only an equality check catches that.

    Raises before any prompt is sent, so on a hosted backend nothing is billed.
    """
    if requested not in tuple(advertised):
        raise ModelResolutionError(
            f"model {requested!r} is not advertised by this agent "
            f"(advertised: {', '.join(advertised)}). An unadvertised id is adopted "
            f"verbatim rather than rejected, so it would appear to bind correctly "
            f"while naming something the backend never validated."
        )
    if current_model_id != requested:
        raise ModelResolutionError(
            f"requested model {requested!r} but the session reports "
            f"{current_model_id!r}. The agent matches model ids by substring in "
            f"both directions and takes the first hit, so a longer name silently "
            f"resolves to a shorter one."
        )


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
        # The only lane whose serving side we can observe directly (A5), because
        # llama-swap is ours. That is what END_TO_END means here.
        binding_assurance=BindingAssurance.END_TO_END_VERIFIED,
    ),
    "codex": AcpBackend(
        name="codex",
        command=("npx", "-y", "@agentclientprotocol/codex-acp@1.4.0"),
        billing=Billing.HOSTED,
        model_namespaces=("gpt-",),
        default_mode="agent",
        supervision=True,
        interactive=True,
        binding_assurance=BindingAssurance.HARNESS_VERIFIED,
    ),
    "claude-code": AcpBackend(
        name="claude-code",
        command=("npx", "-y", "@zed-industries/claude-code-acp@0.16.2"),
        billing=Billing.HOSTED,
        # Observed, not guessed. The 2026-08-19 capture shows this agent advertises
        # exactly three model ids -- `default`, `sonnet`, `haiku` -- which are
        # SELECTORS, not model names: `default` resolved to Opus 4.6 and `sonnet` to
        # Sonnet 4.5 on the day. An earlier guess of a "claude-" prefix here was
        # wrong, and the capture is the only reason that is known.
        #
        # These are exact ids, so the prefix match is an exact match.
        #
        # This check is not defence in depth here -- it is the ONLY validation in
        # the system. The agent itself validates nothing (see binding_channel).
        model_namespaces=("default", "sonnet", "haiku"),
        # Observed: default | acceptEdits | plan | dontAsk | bypassPermissions.
        # Left None deliberately -- the envelope should name one, and note that
        # three of those five are permission-weakening.
        default_mode=None,
        # NOT via session/set_model, which is UNVALIDATED (A13) and must not be
        # used to bind this agent. The settings path is verifiable (A17).
        binding_channel=BindingChannel.SETTINGS,
        # Frontier reasoning capacity. Its highest-value use is planning,
        # review and synthesis over work it did not execute itself -- not
        # competing with worker-fast for bounded implementation jobs.
        supervision=True,
        interactive=True,
        binding_assurance=BindingAssurance.HARNESS_VERIFIED,
    ),
}
