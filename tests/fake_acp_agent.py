"""A scriptable fake ACP agent, driven over stdio like the real one.

Behaviour is set by ACP_FAKE_BEHAVIOUR in the environment so a test can produce a
misbehaving agent without mocking the adapter's own transport.
"""
from __future__ import annotations

import json
import os
import sys


def main() -> None:
    behaviour = os.environ.get("ACP_FAKE_BEHAVIOUR", "good")
    bound_model = "vendor/harness-default"
    session_id = "ses_fake_0001"

    def out(obj: dict) -> None:
        sys.stdout.write(json.dumps(obj) + "\n")
        sys.stdout.flush()

    def reply(rid, result) -> None:
        out({"jsonrpc": "2.0", "id": rid, "result": result})

    def fail(rid, code, message) -> None:
        out({"jsonrpc": "2.0", "id": rid, "error": {"code": code, "message": message}})

    model_option = {
        "id": "model",
        "type": "select",
        "currentValue": bound_model,
        "options": [
            {"value": "vendor/harness-default", "name": "Default"},
            {"value": "local3090/worker-fast", "name": "worker-fast"},
        ],
    }

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        message = json.loads(line)
        method = message.get("method")
        rid = message.get("id")

        if method == "initialize":
            version = 2 if behaviour == "wrong_version" else 1
            reply(rid, {
                "protocolVersion": version,
                "agentInfo": {"name": "fake", "version": "0.0.1"},
                "agentCapabilities": {
                    "loadSession": True,
                    "promptCapabilities": {"image": True, "embeddedContext": True},
                    "sessionCapabilities": {"close": {}, "list": {}},
                },
                "authMethods": [{"id": "none", "name": "none"}],
            })
        elif method == "session/new":
            reply(rid, {"sessionId": session_id, "configOptions": [dict(model_option)]})
        elif method == "session/list":
            if behaviour == "no_session_list":
                fail(rid, -32601, "Method not found: session/list")
            else:
                cwd = "/somewhere/else/entirely" if behaviour == "wrong_root" else os.getcwd()
                reply(rid, {"sessions": [{"sessionId": session_id, "cwd": cwd,
                                          "title": "t", "updatedAt": "now"}]})
        elif method == "session/set_mode":
            reply(rid, {})
        elif method == "session/set_model":
            if behaviour == "no_set_model":
                fail(rid, -32601, "Method not found: session/set_model")
            elif behaviour == "lying_set_model":
                reply(rid, {})  # says ok, never rebinds
            else:
                bound_model = message["params"]["modelId"]
                model_option["currentValue"] = bound_model
                reply(rid, {})
        elif method == "session/status":
            if behaviour == "no_status":
                fail(rid, -32601, "Method not found: session/status")
            else:
                reply(rid, {"configOptions": [dict(model_option)]})
        elif method == "session/prompt":
            out({"jsonrpc": "2.0", "method": "session/update", "params": {
                "sessionId": session_id,
                "update": {"sessionUpdate": "agent_message_chunk",
                           "content": {"type": "text", "text": "hello"}}}})
            out({"jsonrpc": "2.0", "method": "session/update", "params": {
                "sessionId": session_id,
                "update": {"sessionUpdate": "tool_call", "toolCallId": "t1",
                           "title": "read README.md", "status": "pending", "kind": "read"}}})
            if behaviour == "orphan_permission":
                # Permission for a tool call that was never announced.
                out({"jsonrpc": "2.0", "id": 9002, "method": "session/request_permission",
                     "params": {"sessionId": session_id,
                                "toolCall": {"toolCallId": "never-announced"},
                                "options": [
                                    {"optionId": "a1", "kind": "allow_once", "name": "Allow"},
                                    {"optionId": "r1", "kind": "reject_once", "name": "Reject"}]}})
            if behaviour == "update_after_terminal":
                out({"jsonrpc": "2.0", "method": "session/update", "params": {
                    "sessionId": session_id,
                    "update": {"sessionUpdate": "tool_call_update", "toolCallId": "t1",
                               "status": "completed"}}})
                out({"jsonrpc": "2.0", "method": "session/update", "params": {
                    "sessionId": session_id,
                    "update": {"sessionUpdate": "tool_call_update", "toolCallId": "t1",
                               "status": "in_progress"}}})
            if behaviour == "update_without_id":
                out({"jsonrpc": "2.0", "method": "session/update", "params": {
                    "sessionId": session_id,
                    "update": {"sessionUpdate": "tool_call_update", "status": "completed"}}})
            if behaviour == "orphan_update":
                out({"jsonrpc": "2.0", "method": "session/update", "params": {
                    "sessionId": session_id,
                    "update": {"sessionUpdate": "tool_call_update",
                               "toolCallId": "never-announced", "status": "completed"}}})
            if behaviour == "asks_permission":
                out({"jsonrpc": "2.0", "id": 9001, "method": "session/request_permission",
                     "params": {"sessionId": session_id,
                                "toolCall": {"toolCallId": "t1"},
                                "options": [
                                    {"optionId": "a1", "kind": "allow_once", "name": "Allow"},
                                    {"optionId": "r1", "kind": "reject_once", "name": "Reject"}]}})
            out({"jsonrpc": "2.0", "method": "session/update", "params": {
                "sessionId": session_id,
                "update": {"sessionUpdate": "something_new_we_do_not_map"}}})
            reply(rid, {"stopReason": "end_turn",
                        "usage": {"inputTokens": 10, "outputTokens": 2, "totalTokens": 12}})
        elif method == "session/close":
            reply(rid, {})
            return
        elif rid is not None:
            fail(rid, -32601, f'"Method not found": {method}')


if __name__ == "__main__":
    main()
