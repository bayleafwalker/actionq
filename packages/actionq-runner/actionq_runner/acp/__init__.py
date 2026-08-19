"""ACP adapter. Version-specific codecs live in submodules; ActionQ imports neither."""
from __future__ import annotations

from .jsonrpc import JsonRpcError, JsonRpcTimeout, StdioJsonRpc
from ..execution_contract import BindingStatus, ModelBinding
from .v1 import (
    PROTOCOL_VERSION,
    AcpError,
    AcpExecutionAdapter,
    AgentCapabilities,
    ModelBindingError,
)

__all__ = [
    "PROTOCOL_VERSION",
    "BindingStatus",
    "ModelBinding",
    "AcpError",
    "AcpExecutionAdapter",
    "AgentCapabilities",
    "JsonRpcError",
    "JsonRpcTimeout",
    "ModelBindingError",
    "StdioJsonRpc",
]
