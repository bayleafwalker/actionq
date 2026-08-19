"""ACP adapter. Version-specific codecs live in submodules; ActionQ imports neither."""
from __future__ import annotations

from .jsonrpc import JsonRpcError, JsonRpcTimeout, StdioJsonRpc
from ..execution_boundary import BoundaryCheck, BoundaryReport, BoundaryViolation
from ..context_policy import ContextPlan, verify_context_policy
from ..execution_contract import Assurance, BindingStatus, ContextUsage, ModelBinding
from .backends import (
    REGISTRY,
    AcpBackend,
    Billing,
    BindingChannel,
    binding_is_trustworthy,
    check_model_allowed,
)
from .v1 import (
    PROTOCOL_VERSION,
    AcpError,
    AcpExecutionAdapter,
    AgentCapabilities,
    ModelBindingError,
)

__all__ = [
    "PROTOCOL_VERSION",
    "REGISTRY",
    "AcpBackend",
    "Billing",
    "BindingChannel",
    "binding_is_trustworthy",
    "check_model_allowed",
    "Assurance",
    "BindingStatus",
    "ContextPlan",
    "ContextUsage",
    "verify_context_policy",
    "BoundaryCheck",
    "BoundaryReport",
    "BoundaryViolation",
    "ModelBinding",
    "AcpError",
    "AcpExecutionAdapter",
    "AgentCapabilities",
    "JsonRpcError",
    "JsonRpcTimeout",
    "ModelBindingError",
    "StdioJsonRpc",
]
