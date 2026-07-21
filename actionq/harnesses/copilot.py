"""Copilot CLI harness adapter placeholder (work item #970).

Non-scope per the work item: "add Copilot only after its noninteractive
auth path is proven." ``gh copilot`` in this environment is documented as
interactive-preview only (``gh copilot --help`` describes downloading and
running an interactive CLI; there is no confirmed noninteractive,
already-authenticated invocation path here). Rather than guess at flags or
silently fall back to another harness, this adapter always raises
``HarnessUnsupportedError`` -- ``get_adapter("copilot-cli")`` in
``actionq.harnesses`` surfaces the same error so callers get an explicit,
typed rejection instead of a working-looking adapter that has never been
verified.
"""
from __future__ import annotations

from .base import HarnessAdapter, HarnessInvocation, HarnessUnsupportedError


class CopilotAdapter(HarnessAdapter):
    name = "copilot-cli"

    def __init__(self, bin_path: str | None = None):
        super().__init__(bin_path or "gh")

    def build_command(self, invocation: HarnessInvocation) -> list[str]:
        raise HarnessUnsupportedError(
            "copilot-cli has no proven noninteractive auth path in this environment; "
            "see actionq/harnesses/copilot.py for the non-scope rationale."
        )

    def invoke(self, invocation: HarnessInvocation):
        raise HarnessUnsupportedError(
            "copilot-cli has no proven noninteractive auth path in this environment; "
            "see actionq/harnesses/copilot.py for the non-scope rationale."
        )
