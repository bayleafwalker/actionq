from __future__ import annotations

import argparse
import json
from pathlib import Path

from .identity import sign_runner_request, verify_runner_request
from .executor import execute
from .publisher import acknowledge_settlement, list_publications, publish, query_settlement, recover_publication, resume_publication
from .staging import mark_reconciled, open_staging


def main() -> None:
    parser = argparse.ArgumentParser(prog="actionq-runner")
    commands = parser.add_subparsers(dest="command", required=True)
    sign = commands.add_parser("sign")
    sign.add_argument("--private-key", type=Path, required=True)
    sign.add_argument("--runner-id", required=True)
    sign.add_argument("--operation", required=True)
    sign.add_argument("--resource", required=True)
    sign.add_argument("--request-id", required=True)
    commands.add_parser("execute")
    publish_command = commands.add_parser("publish")
    publish_command.add_argument("--packet-stdin", action="store_true", required=True)
    journal_list = commands.add_parser("journal-list")
    journal_list.add_argument("--artifact-root", type=Path, required=True)
    journal_list.add_argument("--action-id", type=int, required=True)
    journal_recover = commands.add_parser("journal-recover")
    journal_recover.add_argument("--artifact-root", type=Path, required=True)
    journal_recover.add_argument("--action-id", type=int, required=True)
    journal_recover.add_argument("--attempt-id")
    journal_resume = commands.add_parser("journal-resume")
    journal_resume.add_argument("--artifact-root", type=Path, required=True)
    journal_resume.add_argument("--action-id", type=int, required=True)
    journal_resume.add_argument("--attempt-id", required=True)
    settlement_ack = commands.add_parser("settlement-ack")
    settlement_ack.add_argument("--packet-stdin", action="store_true", required=True)
    settlement_query = commands.add_parser("settlement-query")
    settlement_query.add_argument("--artifact-root", type=Path, required=True)
    settlement_query.add_argument("--action-id", type=int, required=True)
    settlement_query.add_argument("--attempt-id", required=True)
    reconcile = commands.add_parser("reconcile")
    reconcile.add_argument("--proof-stdin", action="store_true", required=True)
    args = parser.parse_args()
    if args.command == "execute":
        raise SystemExit(execute(json.load(__import__("sys").stdin)))
    if args.command == "publish":
        print(json.dumps(publish(json.load(__import__("sys").stdin)), sort_keys=True, separators=(",", ":")))
        return
    if args.command == "journal-list":
        print(json.dumps(list_publications(args.artifact_root, args.action_id), sort_keys=True, separators=(",", ":")))
        return
    if args.command == "journal-recover":
        print(json.dumps(recover_publication(args.artifact_root, args.action_id, args.attempt_id), sort_keys=True, separators=(",", ":")))
        return
    if args.command == "journal-resume":
        print(json.dumps(resume_publication(args.artifact_root, args.action_id, args.attempt_id), sort_keys=True, separators=(",", ":")))
        return
    if args.command == "settlement-ack":
        print(json.dumps(acknowledge_settlement(json.load(__import__("sys").stdin)), sort_keys=True, separators=(",", ":")))
        return
    if args.command == "settlement-query":
        print(json.dumps(query_settlement(args.artifact_root, args.action_id, args.attempt_id), sort_keys=True, separators=(",", ":")))
        return
    if args.command == "reconcile":
        proof = json.load(__import__("sys").stdin)
        resource = str(proof.get("resource") or "")
        parts = resource.split(":", 3)
        if len(parts) != 4 or parts[0] != "action" or parts[2] != "attempt":
            raise PermissionError("invalid spool reconciliation resource")
        action_id, attempt_id = int(parts[1]), parts[3]
        verify_runner_request(proof, operation="runner.spool.reconcile", resource=resource)
        mark_reconciled(open_staging(action_id, attempt_id), terminal=True)
        return
    proof = sign_runner_request(
        args.private_key, runner_id=args.runner_id, operation=args.operation, resource=args.resource,
        request_id=args.request_id,
    )
    print(json.dumps(proof, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
