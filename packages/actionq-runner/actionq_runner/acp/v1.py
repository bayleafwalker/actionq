"""ACP v1 execution adapter.

All ACP wire vocabulary is confined to this module and its sibling ``telemetry``. ActionQ
types never import it. Verified against OpenCode 1.18.18 on 2026-08-19; see
``docs/evidence/2026-08-19-acp-v1-conformance.md``.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from queue import Empty, Queue
from typing import Any, Callable, Iterator

from ..execution_contract import (
    BindingStatus,
    ExecutionEnvelope,
    ExecutionEvent,
    ExecutionOutcome,
    EventKind,
    ModelBinding,
    PermissionDecision,
    RuntimeHandle,
)
from .jsonrpc import JsonRpcError, StdioJsonRpc
from .telemetry import normalize_inbound

PROTOCOL_VERSION = 1


class AcpError(RuntimeError):
    """The agent violated an expectation the adapter cannot proceed under."""


class ModelBindingError(AcpError):
    """The session is not bound to the model the envelope named.

    Raised rather than warned. An execution running on an unrequested backend produces
    plausible results and bills silently; that is the failure class this adapter exists to
    make impossible.
    """


@dataclass(frozen=True)
class AgentCapabilities:
    """What the agent said it can do, as observed at ``initialize``."""

    protocol_version: int
    agent_name: str
    agent_version: str
    load_session: bool = False
    session_capabilities: frozenset[str] = frozenset()
    prompt_capabilities: frozenset[str] = frozenset()
    auth_methods: tuple[str, ...] = ()
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_initialize(cls, result: dict[str, Any]) -> "AgentCapabilities":
        agent_caps = result.get("agentCapabilities") or {}
        info = result.get("agentInfo") or {}
        return cls(
            protocol_version=result.get("protocolVersion", 0),
            agent_name=info.get("name", "unknown"),
            agent_version=info.get("version", "unknown"),
            load_session=bool(agent_caps.get("loadSession")),
            session_capabilities=frozenset((agent_caps.get("sessionCapabilities") or {}).keys()),
            prompt_capabilities=frozenset(
                k for k, v in (agent_caps.get("promptCapabilities") or {}).items() if v
            ),
            auth_methods=tuple(m.get("id", "") for m in result.get("authMethods") or []),
            raw=result,
        )


PermissionResolver = Callable[[dict[str, Any]], PermissionDecision]


def _deny_all(_request: dict[str, Any]) -> PermissionDecision:
    """Default authority: deny.

    ACP supplies the mechanism; Vuoro supplies the authority. An adapter with no policy
    wired in has no authority to grant, so it grants nothing.
    """
    return PermissionDecision.DENY


class AcpExecutionAdapter:
    """Drives one ACP session for one ActionQ execution.

    Lifecycle: ``open`` -> ``prompt`` -> (``events``) -> ``close``. One adapter instance
    per execution; sessions are not pooled, because a session id is a runtime handle whose
    reuse is a policy decision made above this layer.
    """

    protocol = "acp"

    def __init__(
        self,
        command: list[str],
        *,
        agent: str,
        permission_resolver: PermissionResolver | None = None,
        on_stderr: Callable[[str], None] | None = None,
        request_timeout: float = 60.0,
        require_verified_model: bool = False,
    ) -> None:
        if not command:
            raise ValueError("an ACP adapter requires a command to launch the agent")
        self._command = list(command)
        self._agent = agent
        self._resolve_permission = permission_resolver or _deny_all
        self._on_stderr = on_stderr
        self._timeout = request_timeout
        self._require_verified_model = require_verified_model
        self._binding: ModelBinding | None = None
        self._rpc: StdioJsonRpc | None = None
        self._capabilities: AgentCapabilities | None = None
        self._session_id: str | None = None
        self._envelope: ExecutionEnvelope | None = None
        self._events: Queue[ExecutionEvent] = Queue()

    # -- introspection -----------------------------------------------------

    @property
    def capabilities(self) -> AgentCapabilities:
        if self._capabilities is None:
            raise AcpError("capabilities are unknown until open() has run initialize")
        return self._capabilities

    @property
    def model_binding(self) -> ModelBinding | None:
        """How well the bound model is known. ``None`` until open() has run."""
        return self._binding

    @property
    def handle(self) -> RuntimeHandle | None:
        if self._session_id is None:
            return None
        return RuntimeHandle(
            protocol=self.protocol, agent=self._agent, external_session_id=self._session_id
        )

    # -- lifecycle ---------------------------------------------------------

    def open(self, envelope: ExecutionEnvelope) -> RuntimeHandle:
        if self._rpc is not None:
            raise AcpError("adapter is already open")
        self._envelope = envelope
        root = envelope.invariants.root

        env = dict(os.environ)
        # OpenCode resolves its project from PWD, not the process cwd. Leaving an
        # inherited PWD in place points the harness at whatever tree the caller happened
        # to be standing in, and it reports plausible results for that tree instead.
        # See local-inference/benchmarks/evidence/2026-08-19-project-root-bug.md.
        env["PWD"] = root

        self._rpc = StdioJsonRpc(
            self._command,
            cwd=root,
            env=env,
            on_inbound=self._handle_inbound,
            on_stderr=self._on_stderr,
        )

        result = self._rpc.request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "clientCapabilities": {"fs": {"readTextFile": True, "writeTextFile": True}},
            },
            timeout=self._timeout,
        )
        capabilities = AgentCapabilities.from_initialize(result)
        if capabilities.protocol_version != PROTOCOL_VERSION:
            raise AcpError(
                f"agent negotiated ACP v{capabilities.protocol_version}, "
                f"adapter speaks v{PROTOCOL_VERSION}"
            )
        self._capabilities = capabilities

        session = self._rpc.request(
            "session/new",
            {
                "cwd": root,
                "mcpServers": [dict(s) for s in envelope.mcp_servers],
            },
            timeout=self._timeout,
        )
        session_id = session.get("sessionId")
        if not session_id:
            raise AcpError("session/new returned no sessionId")
        self._session_id = session_id
        self._emit(EventKind.RUNTIME_STARTED, {"session_id": session_id, "agent": self._agent})

        if envelope.mode is not None:
            self._set_mode(envelope.mode)
        self._bind_model(envelope.model, session)

        handle = self.handle
        assert handle is not None
        return handle

    def _set_mode(self, mode: str) -> None:
        assert self._rpc is not None
        try:
            self._rpc.request(
                "session/set_mode",
                {"sessionId": self._session_id, "modeId": mode},
                timeout=self._timeout,
            )
        except JsonRpcError as error:
            if error.method_not_found:
                raise AcpError(
                    f"envelope requests mode {mode!r} but the agent has no session/set_mode"
                ) from error
            raise

    def _bind_model(self, model: str, session_result: dict[str, Any]) -> None:
        """Set the session model, then establish how well the binding is known.

        The session was created bound to whatever the harness defaults to -- a hosted model
        in the observed case -- and the ``ok`` from set_model is not evidence that the
        binding changed. Where the agent can report its bound model, verify it. Where it
        cannot, say so in the binding record rather than reporting an assertion as a fact.
        """
        assert self._rpc is not None
        try:
            self._rpc.request(
                "session/set_model",
                {"sessionId": self._session_id, "modelId": model},
                timeout=self._timeout,
            )
        except JsonRpcError as error:
            if error.method_not_found:
                self._binding = ModelBinding(
                    requested=model,
                    status=BindingStatus.UNSUPPORTED,
                    detail="agent exposes no session/set_model",
                )
                raise ModelBindingError(
                    f"agent {self._agent!r} cannot bind a model (no session/set_model); "
                    f"it would silently run the envelope on its own default"
                ) from error
            raise ModelBindingError(f"session/set_model rejected {model!r}: {error}") from error

        observed = self._observed_model()
        if observed is not None:
            if observed != model:
                raise ModelBindingError(
                    f"session reports model {observed!r} after binding {model!r}"
                )
            self._binding = ModelBinding(
                requested=model, status=BindingStatus.VERIFIED, observed=observed
            )
        else:
            # No read-back method exists on this agent. Fall back to the weaker check the
            # protocol does allow: the model must at least be one the agent offered.
            available = _model_options(session_result)
            if available and model not in available:
                raise ModelBindingError(
                    f"agent does not offer model {model!r}; offered: {sorted(available)[:5]}..."
                )
            self._binding = ModelBinding(
                requested=model,
                status=BindingStatus.ASSERTED,
                detail=(
                    "agent accepted the binding but exposes no method to read it back; "
                    "the model was confirmed present in the session's offered options only"
                ),
            )

        self._emit(
            EventKind.MODEL_BINDING,
            {
                "requested": self._binding.requested,
                "status": self._binding.status.value,
                "observed": self._binding.observed,
                "detail": self._binding.detail,
            },
        )
        if self._require_verified_model and not self._binding.verified:
            raise ModelBindingError(
                f"model binding for {model!r} is {self._binding.status.value}, not verified, "
                f"and require_verified_model is set"
            )

    def _observed_model(self) -> str | None:
        """Read the session's current model back, if the agent exposes a way to."""
        assert self._rpc is not None
        for method in ("session/status", "session/info"):
            try:
                state = self._rpc.request(
                    method, {"sessionId": self._session_id}, timeout=self._timeout
                )
            except JsonRpcError as error:
                if error.method_not_found:
                    continue
                raise
            except Exception:
                continue
            model = _model_option_value(state)
            if model is not None:
                return model
        return None

    def prompt(self, text: str | None = None, *, timeout: float = 1800.0) -> ExecutionOutcome:
        """Submit the envelope's instruction and block until the turn ends."""
        if self._rpc is None or self._session_id is None or self._envelope is None:
            raise AcpError("prompt() requires an open session")
        content = text if text is not None else self._envelope.instruction
        result = self._rpc.request(
            "session/prompt",
            {"sessionId": self._session_id, "prompt": [{"type": "text", "text": content}]},
            timeout=timeout,
        )
        stop_reason = result.get("stopReason", "unknown")
        usage = result.get("usage") or {}
        self._emit(EventKind.RUNTIME_STOPPED, {"stop_reason": stop_reason, "usage": usage})
        return ExecutionOutcome(
            stop_reason=stop_reason,
            usage=usage,
            handle=self.handle,
            model_binding=self._binding,
        )

    def cancel(self) -> None:
        if self._rpc is None or self._session_id is None:
            return
        self._emit(EventKind.CANCELLATION_REQUESTED, {"session_id": self._session_id})
        self._rpc.notify("session/cancel", {"sessionId": self._session_id})

    def close(self) -> None:
        if self._rpc is None:
            return
        if self._session_id and "close" in self.capabilities.session_capabilities:
            try:
                self._rpc.request(
                    "session/close", {"sessionId": self._session_id}, timeout=5.0
                )
            except Exception:
                pass
        self._rpc.close()
        self._rpc = None

    def __enter__(self) -> "AcpExecutionAdapter":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # -- events ------------------------------------------------------------

    def events(self, *, timeout: float = 0.0) -> Iterator[ExecutionEvent]:
        """Drain normalized telemetry produced so far."""
        while True:
            try:
                yield self._events.get(block=timeout > 0, timeout=timeout or None)
            except Empty:
                return

    def _emit(self, kind: EventKind, payload: dict[str, Any], raw: dict[str, Any] | None = None) -> None:
        self._events.put(ExecutionEvent(kind=kind, payload=payload, raw=raw))

    def _handle_inbound(self, message: dict[str, Any]) -> None:
        """Notifications and agent-initiated requests arrive here, off the reader thread."""
        method = message.get("method")
        if method == "session/request_permission":
            self._answer_permission(message)
            return
        for event in normalize_inbound(message):
            self._events.put(event)

    def _answer_permission(self, message: dict[str, Any]) -> None:
        assert self._rpc is not None
        params = message.get("params") or {}
        self._emit(EventKind.POLICY_REQUESTED, {"request": params}, raw=message)
        try:
            decision = self._resolve_permission(params)
        except Exception as error:  # a resolver that fails denies; it does not allow
            decision = PermissionDecision.DENY
            self._emit(EventKind.POLICY_RESOLVED, {"decision": decision.value, "error": str(error)})
        else:
            self._emit(EventKind.POLICY_RESOLVED, {"decision": decision.value})
        if "id" not in message:
            return
        option = _permission_option(params, decision)
        if option is None:
            self._rpc.respond(message["id"], {"outcome": {"outcome": "cancelled"}})
            return
        self._rpc.respond(message["id"], {"outcome": {"outcome": "selected", "optionId": option}})


def _model_options(session_result: dict[str, Any]) -> set[str]:
    for option in session_result.get("configOptions") or []:
        if option.get("id") == "model":
            return {o.get("value") for o in option.get("options") or [] if o.get("value")}
    return set()


def _model_option_value(state: dict[str, Any]) -> str | None:
    for option in state.get("configOptions") or []:
        if option.get("id") == "model":
            value = option.get("currentValue")
            return value if isinstance(value, str) else None
    model = state.get("model")
    if isinstance(model, str):
        return model
    if isinstance(model, dict):
        value = model.get("id") or model.get("modelId")
        return value if isinstance(value, str) else None
    return None


def _permission_option(params: dict[str, Any], decision: PermissionDecision) -> str | None:
    """Map a policy decision onto one of the options the agent offered.

    ESCALATE has no ACP representation -- escalation is a Vuoro outcome, not a harness
    feature -- so it is reported as a cancelled request and resolved above this layer.
    """
    if decision is PermissionDecision.ESCALATE:
        return None
    wanted = ("allow_always", "allow_once") if decision is PermissionDecision.ALLOW else (
        "reject_always",
        "reject_once",
    )
    options = params.get("options") or []
    by_kind = {o.get("kind"): o.get("optionId") for o in options if isinstance(o, dict)}
    for kind in wanted:
        if by_kind.get(kind):
            return by_kind[kind]
    return None
