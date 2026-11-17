from __future__ import annotations

import json
import os
import sys
from difflib import get_close_matches
from urllib.parse import urlsplit

import click

from . import __version__
from . import db
from . import federation_schema
from . import schema as schema_contract
from .application import ActionQApplication


def _json_option(ctx: click.Context, param: click.Parameter, value: bool) -> None:
    if value:
        ctx.find_root().meta["json"] = True


class ActionQCommand(click.Command):
    def __init__(self, *args, **kwargs):
        params = list(kwargs.pop("params", []))
        if not any("--json" in getattr(param, "opts", ()) for param in params):
            params.append(click.Option(
                ["--json"], is_flag=True, expose_value=False, callback=_json_option,
                help="Emit JSON output (default for non-terminal output).",
            ))
        super().__init__(*args, params=params, **kwargs)

    def get_short_help_str(self, limit: int = 1000) -> str:
        text = self.short_help or (self.help.splitlines()[0] if self.help else "")
        return text if limit is None else text[:limit]


class OperatorClickException(click.ClickException):
    def __init__(self, message: str, *, why: str | None = None, fix: str | None = None,
                 exit_code: int = 1, json_output: bool = False):
        super().__init__(message)
        self.why = why
        self.fix = fix
        self.exit_code = exit_code
        self.json_output = json_output

    def show(self, file=None):
        file = file or click.get_text_stream("stderr")
        click.echo(f"error: {self.format_message()}", file=file)
        if self.why:
            click.echo(f"  why: {self.why}", file=file)
        if self.fix:
            click.echo(f"  fix: {self.fix}", file=file)
        if self.json_output:
            click.echo(json.dumps({
                "schema": "actionctl.error/v1",
                "error": {"code": self.exit_code, "message": self.format_message(), "fix": self.fix},
            }))


class ActionQGroup(click.Group):
    command_class = ActionQCommand

    def get_short_help_str(self, limit: int = 1000) -> str:
        text = self.short_help or (self.help.splitlines()[0] if self.help else "")
        return text if limit is None else text[:limit]

    def invoke(self, ctx: click.Context):
        try:
            return super().invoke(ctx)
        except db.ActionQError as exc:
            if str(exc) == "ACTIONQ_URL is required":
                raise OperatorClickException(
                    "ACTIONQ_URL is required",
                    why="the PostgreSQL connection string is not configured",
                    fix="export ACTIONQ_URL=postgresql://USER:PASSWORD@HOST:5432/DATABASE",
                    json_output=ctx.find_root().meta.get("json", False),
                ) from exc
            raise OperatorClickException(
                str(exc), json_output=ctx.find_root().meta.get("json", False)
            ) from exc
        except json.JSONDecodeError as exc:
            raise OperatorClickException(
                f"invalid JSON input: {exc.msg} at line {exc.lineno} column {exc.colno}",
                why="a command expected a JSON object on stdin",
                fix="correct the JSON input and run the command again",
                json_output=ctx.find_root().meta.get("json", False),
            ) from exc
        except Exception as exc:
            try:
                psycopg, _ = db._import_psycopg()
            except RuntimeError:
                raise
            if not isinstance(exc, (psycopg.OperationalError, psycopg.InterfaceError)):
                raise
            dsn = os.environ.get("ACTIONQ_URL", "")
            host = _dsn_host(dsn)
            state = getattr(exc, "sqlstate", None) or ""
            detail = str(exc).lower()
            authenticated = state.startswith("28") or any(
                marker in detail for marker in ("authentication failed", "password authentication failed")
            )
            reachable_failure = isinstance(
                exc, (psycopg.OperationalError, psycopg.InterfaceError)
            ) and not state.startswith("3")
            code = 4 if authenticated else 75 if reachable_failure else 1
            reason = "database authentication was rejected" if authenticated else "the database could not be reached"
            fix = (f"check the credentials in ACTIONQ_URL for {host}"
                   if authenticated else f"check PostgreSQL is reachable at {host} and verify ACTIONQ_URL")
            raise OperatorClickException(
                f"PostgreSQL connection failed for host {host}", why=reason, fix=fix,
                exit_code=code, json_output=ctx.find_root().meta.get("json", False),
            ) from exc

    def format_commands(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        categories = (
            ("Start here", ("ls", "show", "events", "sessions")),
            ("Change", ("add", "claim", "renew", "complete", "fail", "cancel", "cancel-ack", "reject", "settle", "emit", "group")),
            ("Maintain", ("sweep",)),
            ("Plumbing", ("migrate", "federation", "check-compatibility", "register-publication")),
        )
        remaining = set(self.list_commands(ctx))
        for heading, names in categories:
            rows = [(name, self.get_command(ctx, name)) for name in names if name in remaining]
            if rows:
                formatter.write_heading(heading)
                formatter.write_dl([(name, command.get_short_help_str(limit=1000)) for name, command in rows])
                remaining.difference_update(name for name, _ in rows)
        if remaining:
            formatter.write_heading("More")
            formatter.write_dl([(name, self.get_command(ctx, name).get_short_help_str(limit=1000))
                                for name in sorted(remaining)])

    def format_epilog(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        if self.epilog:
            formatter.write("\n" + self.epilog + "\n")

    def resolve_command(self, ctx: click.Context, args: list[str]):
        try:
            return super().resolve_command(ctx, args)
        except click.UsageError as exc:
            if args:
                suggestions = get_close_matches(args[0], self.list_commands(ctx), n=1, cutoff=0.55)
                if suggestions:
                    raise click.UsageError(
                        f"No such command '{args[0]}'. Did you mean '{suggestions[0]}'?", ctx,
                    ) from exc
            raise


def _dsn_host(dsn: str) -> str:
    if not dsn:
        return "localhost"
    try:
        parsed = urlsplit(dsn)
        if parsed.hostname:
            return parsed.hostname
    except ValueError:
        pass
    for part in dsn.split():
        key, sep, value = part.partition("=")
        if sep and key.lower() == "host":
            return value.strip("'\"")
    return "localhost"


def _schema(ctx: click.Context) -> str:
    return ctx.obj["schema"]


def _app(ctx: click.Context) -> ActionQApplication:
    return ActionQApplication(schema=_schema(ctx))


def _connect(schema: str | None = None, *, require_compatibility: bool = True):
    try:
        conn = db.connect()
        if require_compatibility:
            try:
                db.require_compatible(conn, db.schema_name(schema))
                conn.rollback()
            except Exception:
                conn.close()
                raise
        return conn
    except (db.ActionQError, RuntimeError) as exc:
        ctx = click.get_current_context(silent=True)
        json_output = bool(ctx and ctx.find_root().meta.get("json"))
        if str(exc) == "ACTIONQ_URL is required":
            raise OperatorClickException(
                "ACTIONQ_URL is required",
                why="the PostgreSQL connection string is not configured",
                fix="export ACTIONQ_URL=postgresql://USER:PASSWORD@HOST:5432/DATABASE",
                json_output=json_output,
            ) from exc
        raise OperatorClickException(str(exc), json_output=json_output) from exc


def _echo_json(value, *, pretty: bool = True) -> None:
    click.echo(db.to_json(value, pretty=pretty and sys.stdout.isatty()))


def _table(rows, columns: tuple[tuple[str, str], ...], noun: str) -> None:
    if not rows:
        click.echo(f"0 {noun}")
        return
    states = {
        "pending": "○ queued", "dispatched": "○ queued", "paused": "○ idle",
        "claimed": "◐ running", "running": "◐ running", "completed": "● done",
        "exited": "● done", "failed": "✗ failed", "rejected": "✗ failed",
    }
    values = []
    for row in rows:
        line = []
        for key, _ in columns:
            value = row.get(key)
            if key == "status" and value is not None:
                value = states.get(str(value), "◌ unknown")
            line.append(str(value if value is not None else "-"))
        values.append(line)
    widths = [max(len(label), *(len(line[index]) for line in values))
              for index, (_, label) in enumerate(columns)]
    click.echo("  ".join(label.ljust(widths[i]) for i, (_, label) in enumerate(columns)))
    for line in values:
        click.echo("  ".join(value.ljust(widths[i]) for i, value in enumerate(line)).rstrip())
    click.echo(f"{len(rows)} {noun}")


@click.group(cls=ActionQGroup, invoke_without_command=True, no_args_is_help=False,
             epilog="Examples:\n  actionctl ls\n  actionctl show 42\n  actionctl events --action 42\n\nRun 'actionctl <command> --help' for command details.")
@click.option("--json", is_flag=True, expose_value=False, callback=_json_option,
              help="Emit JSON output (default for non-terminal output).")
@click.option("--schema", default=None, help="Postgres schema (default: ACTIONQ_SCHEMA or actionq)")
@click.version_option(__version__, prog_name="actionctl")
@click.pass_context
def cli(ctx: click.Context, schema: str | None) -> None:
    """Manage the actionq dispatch queue."""
    ctx.obj = {"schema": db.schema_name(schema)}
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@cli.command(short_help="Apply serialized deployment migrations.")
@click.option("--json-output", is_flag=True, help="Emit the deployment-job result as JSON.")
@click.pass_context
def migrate(ctx: click.Context, json_output: bool) -> None:
    """Apply serialized deployment migrations using a DDL-authorized role."""
    with _connect(_schema(ctx), require_compatibility=False) as conn:
        result = db.migrate(conn, _schema(ctx))
    if json_output or ctx.find_root().meta.get("json"):
        _echo_json(result)
    else:
        versions = result["applied_versions"]
        detail = f"applied {versions}" if versions else "already current"
        click.echo(f"Migrated schema {_schema(ctx)} ({detail})")


@cli.group("federation")
def federation() -> None:
    """Manage federation schema operations.

    A separate command group rather than a ``--domain`` flag on ``migrate``,
    because the freeze requires the execution and federation migration
    selections to be *explicit* inputs: a flag with a default is a selection
    that can be made by omission, and the failure it invites -- an operator
    intending one domain and running the other against a live database -- is
    the one that cannot be undone by rerunning the command.

    The two loaders share no state. An execution v12 compatibility result never
    claims federation readiness, and this group never touches execution
    migrations.
    """


@federation.command("migrate")
@click.option("--schema", "federation_schema_name", default=None,
              help="Federation schema (default: ACTIONQ_FEDERATION_SCHEMA or federation)")
@click.option("--json-output", is_flag=True, help="Emit the migration result as JSON.")
def federation_migrate(federation_schema_name: str | None, json_output: bool) -> None:
    """Apply federation schema migrations.

    Takes its own ``--schema`` rather than inheriting the group's execution
    schema: they are different schemas in the same database, and inheriting
    would make ``actionctl --schema execution federation migrate`` silently
    create federation tables in the execution schema.
    """
    selected = federation_schema_name or federation_schema.configured_schema()
    with _connect(require_compatibility=False) as conn:
        result = federation_schema.migrate(conn, selected)
    if json_output or click.get_current_context().find_root().meta.get("json"):
        _echo_json(result)
    else:
        versions = result.get("applied_versions") or []
        detail = f"applied {versions}" if versions else "already current"
        click.echo(f"Migrated federation schema {selected} ({detail})")


@federation.command("check-compatibility")
@click.option("--schema", "federation_schema_name", default=None,
              help="Federation schema (default: ACTIONQ_FEDERATION_SCHEMA or federation)")
def federation_check_compatibility(federation_schema_name: str | None) -> None:
    """Check federation schema compatibility.

    Reports ``uninitialized`` until the migration has run, which is what makes
    it usable as the precondition check before a cutover rather than only as a
    postcondition.
    """
    selected = federation_schema_name or federation_schema.configured_schema()
    with _connect(require_compatibility=False) as conn:
        compatibility = federation_schema.check_compatibility(conn, selected)
    _echo_json(compatibility.as_dict())
    if not compatibility.compatible:
        raise click.exceptions.Exit(3)


@cli.command("check-compatibility", short_help="Check execution schema compatibility.")
@click.pass_context
def check_compatibility(ctx: click.Context) -> None:
    """Report execution API/schema compatibility without running DDL."""
    with _connect(_schema(ctx), require_compatibility=False) as conn:
        compatibility = schema_contract.check_compatibility(conn, _schema(ctx))
    _echo_json(compatibility.as_dict())
    if not compatibility.compatible:
        raise click.exceptions.Exit(3)


@cli.command(short_help="Enqueue an action.")
@click.option("--type", "action_type", required=True, help="Action type")
@click.option("--project", default=None, help="Project key")
@click.option("--target", "target_ref", default=None, help="Target reference")
@click.option("--source", "source_refs", multiple=True, help="Source/context reference")
@click.option("--priority", default=100, type=int, show_default=True)
@click.option("--parent", "parent_id", default=None, type=int, help="Parent action id")
@click.option("--created-by", default="human:cli", show_default=True)
@click.pass_context
def add(ctx, action_type, project, target_ref, source_refs, priority, parent_id, created_by) -> None:
    """Enqueue an action."""
    action = _app(ctx).enqueue(
        action_type=action_type,
        project=project,
        target_ref=target_ref,
        source_refs=list(source_refs),
        priority=priority,
        parent_id=parent_id,
        created_by=created_by,
    )
    _echo_json(action)


@cli.command("ls")
@click.option("--status", default=None)
@click.option("--type", "action_type", default=None)
@click.option("--project", default=None)
@click.option("--limit", default=50, type=int, show_default=True)
@click.pass_context
def list_cmd(ctx, status, action_type, project, limit) -> None:
    """List actions."""
    rows = _app(ctx).list_actions(
        status=status,
        action_type=action_type,
        project=project,
        limit=limit,
    )
    if sys.stdout.isatty() and not ctx.find_root().meta.get("json"):
        _table(rows, (("id", "ID"), ("action_type", "TYPE"), ("status", "STATUS"), ("project", "PROJECT")), "actions")
    else:
        _echo_json(rows)


@cli.command(short_help="Show an action and its event history.")
@click.argument("action_id", type=int)
@click.pass_context
def show(ctx, action_id: int) -> None:
    """Show one action and its event history."""
    result = _app(ctx).show_action(action_id)
    if result is None:
        raise click.ClickException(f"Action #{action_id} not found")
    _echo_json(result)


@cli.group("group")
def execution_group() -> None:
    """Manage immutable bounded projections over ordinary actions."""


@execution_group.command("realize")
@click.option("--spec-stdin", is_flag=True, required=True, help="Read execution-group/v1 JSON from stdin")
@click.option("--created-by", default="human:cli", show_default=True)
@click.pass_context
def group_realize(ctx, spec_stdin: bool, created_by: str) -> None:
    """Create an immutable execution group."""
    spec = json.load(sys.stdin)
    if spec.get("contract_id") != "execution-group/v1" or set(spec) != {
        "contract_id", "plan_ref", "max_parallel", "failure_policy", "members",
    }:
        raise click.ClickException("group spec must be exactly execution-group/v1")
    result = _app(ctx).realize_execution_group(
        plan_ref=spec["plan_ref"], max_parallel=spec["max_parallel"],
        failure_policy=spec["failure_policy"], members=spec["members"], created_by=created_by,
    )
    _echo_json(result)


@execution_group.command("show")
@click.argument("group_id")
@click.pass_context
def group_show(ctx, group_id: str) -> None:
    """Show an execution group."""
    result = _app(ctx).show_execution_group(group_id)
    if result is None:
        raise click.ClickException("Execution group not found")
    _echo_json(result)


@execution_group.command("ls")
@click.option("--limit", default=50, type=int, show_default=True)
@click.pass_context
def group_list(ctx, limit: int) -> None:
    """List execution groups."""
    _echo_json(_app(ctx).list_execution_groups(limit=limit))


@execution_group.command("stop-new-claims")
@click.argument("group_id")
@click.option("--reason", required=True)
@click.option("--actor", default="human:cli", show_default=True)
@click.pass_context
def group_stop(ctx, group_id: str, reason: str, actor: str) -> None:
    """Stop new claims for an execution group."""
    _echo_json(_app(ctx).stop_execution_group(group_id=group_id, actor=actor, reason=reason))


@cli.command(short_help="Claim one pending action.")
@click.option("--proof-stdin", is_flag=True, required=True, help="Read signed runner proof as JSON from stdin")
@click.option("--timeout", "timeout_minutes", default=30, type=int, show_default=True,
              help="Lease duration in minutes.")
@click.pass_context
def claim(ctx, proof_stdin: bool, timeout_minutes: int) -> None:
    """Claim one pending action as JSON; exits non-zero if none are available."""
    try:
        action = _app(ctx).claim(runner_proof=json.load(sys.stdin), timeout_minutes=timeout_minutes)
    except db.NoActionAvailable as exc:
        click.echo(str(exc), err=True)
        raise click.exceptions.Exit(2) from exc
    _echo_json(action)


@cli.command(short_help="Renew a claim deadline.")
@click.argument("action_id", type=int)
@click.option("--timeout", "timeout_minutes", default=30, type=int, show_default=True,
              help="Lease duration in minutes.")
@click.option("--proof-stdin", is_flag=True, required=True, help="Read signed runner proof and claim receipt from stdin")
@click.pass_context
def renew(ctx, action_id: int, timeout_minutes: int, proof_stdin: bool) -> None:
    """Renew (extend) a claim's deadline as an authority command.

    Exits non-zero with the durable rejection reason for a stale or
    invalid renewal (wrong worker, expired lease, wrong status, or an
    unknown action) -- the action row is left unchanged and a
    claim_renewal_rejected event is still recorded for the caller to read
    back via `show`/`events`.
    """
    proof = json.load(sys.stdin)
    try:
        action = _app(ctx).renew(
            action_id=action_id,
            worker=None,
            timeout_minutes=timeout_minutes,
            claim_receipt=str(proof["claim_receipt"]), runner_proof=dict(proof["runner_proof"]),
        )
    except db.ClaimRejected as exc:
        click.echo(str(exc), err=True)
        raise click.exceptions.Exit(2) from exc
    _echo_json(action)


@cli.command(short_help="Complete a claimed action.")
@click.argument("action_id", type=int)
@click.option("--result", "result_ref", required=True)
@click.option("--proof-stdin", is_flag=True, required=True)
@click.pass_context
def complete(ctx, action_id: int, result_ref: str, proof_stdin: bool) -> None:
    """Complete a claimed action."""
    proof = json.load(sys.stdin)
    action = _app(ctx).complete(
        action_id=action_id,
        result_ref=result_ref,
        actor=None, claim_receipt=str(proof["claim_receipt"]), runner_proof=dict(proof["runner_proof"]),
    )
    _echo_json(action)


@cli.command(short_help="Fail a claimed action.")
@click.argument("action_id", type=int)
@click.option("--reason", required=True)
@click.option("--proof-stdin", is_flag=True, required=True)
@click.pass_context
def fail(ctx, action_id: int, reason: str, proof_stdin: bool) -> None:
    """Fail a claimed action."""
    proof = json.load(sys.stdin)
    action = _app(ctx).fail(
        action_id=action_id,
        reason=reason,
        actor=None, claim_receipt=str(proof["claim_receipt"]), runner_proof=dict(proof["runner_proof"]),
    )
    _echo_json(action)


@cli.command(short_help="Reject a claimed action.")
@click.argument("action_id", type=int)
@click.option("--reason", required=True)
@click.option("--validator", required=True)
@click.option("--proof-stdin", is_flag=True, required=True)
@click.pass_context
def reject(ctx, action_id: int, reason: str, validator: str, proof_stdin: bool) -> None:
    """Reject a claimed action."""
    proof = json.load(sys.stdin)
    action = _app(ctx).reject(
        action_id=action_id,
        reason=reason,
        validator=validator,
        actor=None, claim_receipt=str(proof["claim_receipt"]), runner_proof=dict(proof["runner_proof"]),
    )
    _echo_json(action)


@cli.command(short_help="Settle a claimed action.")
@click.argument("action_id", type=int)
@click.option("--packet-stdin", is_flag=True, required=True,
              help="Read the result packet, claim receipt, and runner proof as JSON from stdin")
@click.pass_context
def settle(ctx, action_id: int, packet_stdin: bool) -> None:
    """Settle a claimed action from a verified dispatch-result/v1 packet."""
    packet = json.load(sys.stdin)
    if not isinstance(packet, dict) or set(packet) != {"claim_receipt", "runner_proof", "result"}:
        raise click.ClickException("settlement packet contains unsupported fields")
    result = packet["result"]
    if (
        not isinstance(packet["claim_receipt"], str)
        or not isinstance(packet["runner_proof"], dict)
        or not isinstance(result, dict)
        or result.get("action_id") != action_id
    ):
        raise click.ClickException("settlement result action_id does not match the command")
    _echo_json(_app(ctx).settle(
        action_id=action_id,
        result=result,
        actor=None,
        claim_receipt=str(packet["claim_receipt"]),
        runner_proof=dict(packet["runner_proof"]),
    ))


@cli.command(short_help="Request cancellation and record an event.")
@click.argument("action_id", type=int)
@click.option("--reason", required=True)
@click.option("--actor", default="human")
@click.pass_context
def cancel(ctx, action_id: int, reason: str, actor: str) -> None:
    """Request cancellation of an action."""
    action = _app(ctx).cancel(
        action_id=action_id,
        reason=reason,
        actor=actor,
    )
    _echo_json(action)


@cli.command("cancel-ack", short_help="Acknowledge a cancellation request.")
@click.argument("action_id", type=int)
@click.option("--cancel-request-id", required=True)
@click.option("--proof-stdin", is_flag=True, required=True, help="Read private acknowledgement proofs as JSON from stdin")
@click.pass_context
def cancel_ack(ctx, action_id: int, cancel_request_id: str, proof_stdin: bool) -> None:
    """Acknowledge a cancellation request."""
    proof = json.load(sys.stdin)
    if set(proof) != {"former_claim_receipt", "runner_auth_token", "runner_proof"}:
        raise click.ClickException("cancellation proof must contain exactly the required private fields")
    _echo_json(_app(ctx).acknowledge_cancellation(
        action_id=action_id, cancel_request_id=cancel_request_id,
        former_claim_receipt=str(proof["former_claim_receipt"]),
        runner_auth_token=str(proof["runner_auth_token"]),
        runner_proof=dict(proof["runner_proof"]),
    ))


@cli.command("register-publication", short_help="Record a publication event.")
@click.argument("action_id", type=int)
@click.option("--attempt-id")
@click.option("--journal-ref")
@click.option("--source-commit")
@click.option("--candidate-commit")
@click.option("--proof-stdin", is_flag=True, required=True, help="Read signed runner proof and live claim receipt from stdin")
@click.pass_context
def register_publication(
    ctx,
    action_id: int,
    attempt_id: str,
    journal_ref: str,
    source_commit: str,
    candidate_commit: str,
    proof_stdin: bool,
) -> None:
    """Register a publication for a claimed action."""
    proof = json.load(sys.stdin)
    private_fields = {"claim_receipt", "runner_proof"}
    public_fields = {"attempt_id", "journal_ref", "source_commit", "candidate_commit"}
    if not private_fields <= set(proof) or set(proof) - private_fields - public_fields:
        raise click.ClickException("publication proof contains unsupported fields")
    stdin_public = {field: proof.get(field) for field in public_fields if field in proof}
    option_public = {
        "attempt_id": attempt_id,
        "journal_ref": journal_ref,
        "source_commit": source_commit,
        "candidate_commit": candidate_commit,
    }
    if stdin_public and any(value is not None for value in option_public.values()):
        raise click.ClickException("publication fields must be supplied either by options or stdin, not both")
    values = stdin_public or option_public
    if set(values) != public_fields or not all(isinstance(value, str) for value in values.values()):
        raise click.ClickException("publication fields are required and must be strings")
    _echo_json(_app(ctx).register_publication(
        action_id=action_id,
        attempt_id=values["attempt_id"],
        journal_ref=values["journal_ref"],
        source_commit=values["source_commit"],
        candidate_commit=values["candidate_commit"],
        claim_receipt=str(proof["claim_receipt"]),
        runner_proof=dict(proof["runner_proof"]),
    ))


@cli.command(short_help="Sweep and expire timed-out actions.")
@click.pass_context
def sweep(ctx) -> None:
    """Expire timed-out actions."""
    rows = _app(ctx).sweep()
    _echo_json(rows)


@cli.command(short_help="Read the event log.")
@click.option("--since", default=None, help="Timestamp lower bound")
@click.option("--type", "event_type", default=None, help="Event type")
@click.option("--action", "action_id", default=None, type=int, help="Action id")
@click.option("--limit", default=100, type=int, show_default=True)
@click.option("--follow", is_flag=True, default=False)
@click.pass_context
def events(ctx, since, event_type, action_id, limit, follow) -> None:
    """Read the event log."""
    if follow:
        with _connect(_schema(ctx)) as conn:
            for event in db.follow_events(
                conn,
                _schema(ctx),
                event_type=event_type,
                action_id=action_id,
            ):
                click.echo(db.to_json(event))
                sys.stdout.flush()
            return
    rows = _app(ctx).list_events(
        since=since,
        event_type=event_type,
        action_id=action_id,
        limit=limit,
    )
    _echo_json(rows)


@cli.command(short_help="Summarize coordinator sessions.")
@click.option("--project", default=None, help="Project key")
@click.option("--active", "active_only", is_flag=True, default=False, help="Show only non-exited sessions")
@click.option("--limit", default=100, type=int, show_default=True)
@click.pass_context
def sessions(ctx, project: str | None, active_only: bool, limit: int) -> None:
    """Summarize daemon sessions from session.* coordinator events."""
    rows = _app(ctx).list_sessions(
        project=project,
        active_only=active_only,
        limit=limit,
    )
    if sys.stdout.isatty() and not ctx.find_root().meta.get("json"):
        _table(rows, (("session_id", "SESSION"), ("action_id", "ACTION"), ("status", "STATUS"), ("project", "PROJECT")), "sessions")
    else:
        _echo_json(rows)


_EMIT_EVENT_TYPES = ["coordinator_cycle", "coordinator_paused", *db.SESSION_EVENT_TYPES]


@cli.command(short_help="Append a coordinator event.")
@click.option("--type", "event_type", required=True, type=click.Choice(_EMIT_EVENT_TYPES))
@click.option("--action", "action_id", default=None, type=int)
@click.option("--actor", default=None)
@click.option("--payload", default="{}", help="JSON payload")
@click.pass_context
def emit(ctx, event_type: str, action_id: int | None, actor: str | None, payload: str) -> None:
    """Append a coordinator event without direct SQL access by the dispatcher."""
    parsed = db.parse_json(payload, default={})
    if not isinstance(parsed, dict):
        raise click.ClickException("--payload must be a JSON object")
    event = _app(ctx).emit_event(
        event_type=event_type,
        action_id=action_id,
        actor=actor,
        payload=parsed,
    )
    _echo_json(event)
