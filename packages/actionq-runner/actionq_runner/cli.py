from __future__ import annotations

import argparse
import json
from pathlib import Path

from .identity import sign_runner_request
from .executor import execute
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
    reconcile = commands.add_parser("reconcile")
    reconcile.add_argument("--action-id", type=int, required=True)
    reconcile.add_argument("--attempt-id", required=True)
    args = parser.parse_args()
    if args.command == "execute":
        raise SystemExit(execute(json.load(__import__("sys").stdin)))
    if args.command == "reconcile":
        mark_reconciled(open_staging(args.action_id, args.attempt_id), terminal=True)
        return
    proof = sign_runner_request(
        args.private_key, runner_id=args.runner_id, operation=args.operation, resource=args.resource,
        request_id=args.request_id,
    )
    print(json.dumps(proof, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
