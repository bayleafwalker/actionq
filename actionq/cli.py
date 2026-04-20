from __future__ import annotations

import json
import sys

import click

from . import __version__
from . import db


class ActionQGroup(click.Group):
    def invoke(self, ctx: click.Context):
        try:
            return super().invoke(ctx)
        except db.ActionQError as exc:
            raise click.ClickException(str(exc)) from exc


def _schema(ctx: click.Context) -> str:
    return ctx.obj["schema"]


def _connect():
    try:
        return db.connect()
    except (db.ActionQError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc


def _echo_json(value, *, pretty: bool = True) -> None:
    click.echo(db.to_json(value, pretty=pretty))


@click.group(cls=ActionQGroup)
@click.option("--schema", default=None, help="Postgres schema (default: ACTIONQ_SCHEMA or actionq)")
@click.version_option(__version__, prog_name="actionctl")
@click.pass_context
def cli(ctx: click.Context, schema: str | None) -> None:
    """Manage the actionq dispatch queue."""
    ctx.obj = {"schema": db.schema_name(schema)}


@cli.command()
@click.pass_context
def migrate(ctx: click.Context) -> None:
    """Create or upgrade the actionq Postgres schema."""
    with _connect() as conn:
        db.migrate(conn, _schema(ctx))
    click.echo(f"Migrated schema {_schema(ctx)}")


@cli.command()
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
    with _connect() as conn:
        action = db.enqueue(
            conn,
            _schema(ctx),
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
    with _connect() as conn:
        rows = db.list_actions(
            conn,
            _schema(ctx),
            status=status,
            action_type=action_type,
            project=project,
            limit=limit,
        )
    _echo_json(rows)


@cli.command()
@click.argument("action_id", type=int)
@click.pass_context
def show(ctx, action_id: int) -> None:
    """Show one action and its event history."""
    with _connect() as conn:
        action = db.get_action(conn, _schema(ctx), action_id)
        if action is None:
            raise click.ClickException(f"Action #{action_id} not found")
        events = db.action_events(conn, _schema(ctx), action_id)
    _echo_json({"action": action, "events": events})


@cli.command()
@click.option("--worker", required=True, help="Worker identity")
@click.option("--timeout", "timeout_minutes", default=30, type=int, show_default=True)
@click.pass_context
def claim(ctx, worker: str, timeout_minutes: int) -> None:
    """Claim one pending action as JSON; exits non-zero if none are available."""
    try:
        with _connect() as conn:
            action = db.claim(
                conn,
                _schema(ctx),
                worker=worker,
                timeout_minutes=timeout_minutes,
            )
    except db.NoActionAvailable as exc:
        click.echo(str(exc), err=True)
        raise click.exceptions.Exit(2) from exc
    _echo_json(action)


@cli.command()
@click.argument("action_id", type=int)
@click.option("--result", "result_ref", required=True)
@click.option("--actor", default=None)
@click.pass_context
def complete(ctx, action_id: int, result_ref: str, actor: str | None) -> None:
    with _connect() as conn:
        action = db.complete(conn, _schema(ctx), action_id, result_ref, actor=actor)
    _echo_json(action)


@cli.command()
@click.argument("action_id", type=int)
@click.option("--reason", required=True)
@click.option("--actor", default=None)
@click.pass_context
def fail(ctx, action_id: int, reason: str, actor: str | None) -> None:
    with _connect() as conn:
        action = db.fail(conn, _schema(ctx), action_id, reason, actor=actor)
    _echo_json(action)


@cli.command()
@click.argument("action_id", type=int)
@click.option("--reason", required=True)
@click.option("--validator", required=True)
@click.option("--actor", default=None)
@click.pass_context
def reject(ctx, action_id: int, reason: str, validator: str, actor: str | None) -> None:
    with _connect() as conn:
        action = db.reject(
            conn,
            _schema(ctx),
            action_id,
            reason=reason,
            validator=validator,
            actor=actor,
        )
    _echo_json(action)


@cli.command()
@click.argument("action_id", type=int)
@click.option("--reason", required=True)
@click.option("--actor", default="human")
@click.pass_context
def cancel(ctx, action_id: int, reason: str, actor: str) -> None:
    with _connect() as conn:
        action = db.cancel(conn, _schema(ctx), action_id, reason, actor=actor)
    _echo_json(action)


@cli.command()
@click.pass_context
def sweep(ctx) -> None:
    with _connect() as conn:
        rows = db.sweep(conn, _schema(ctx))
    _echo_json(rows)


@cli.command()
@click.option("--since", default=None, help="Timestamp lower bound")
@click.option("--type", "event_type", default=None, help="Event type")
@click.option("--action", "action_id", default=None, type=int, help="Action id")
@click.option("--limit", default=100, type=int, show_default=True)
@click.option("--follow", is_flag=True, default=False)
@click.pass_context
def events(ctx, since, event_type, action_id, limit, follow) -> None:
    """Read the event log."""
    with _connect() as conn:
        if follow:
            for event in db.follow_events(
                conn,
                _schema(ctx),
                event_type=event_type,
                action_id=action_id,
            ):
                click.echo(db.to_json(event))
                sys.stdout.flush()
            return
        rows = db.list_events(
            conn,
            _schema(ctx),
            since=since,
            event_type=event_type,
            action_id=action_id,
            limit=limit,
        )
    _echo_json(rows)


@cli.command()
@click.option("--type", "event_type", required=True, type=click.Choice(["coordinator_cycle", "coordinator_paused"]))
@click.option("--action", "action_id", default=None, type=int)
@click.option("--actor", default=None)
@click.option("--payload", default="{}", help="JSON payload")
@click.pass_context
def emit(ctx, event_type: str, action_id: int | None, actor: str | None, payload: str) -> None:
    """Append a coordinator event without direct SQL access by the dispatcher."""
    parsed = db.parse_json(payload, default={})
    if not isinstance(parsed, dict):
        raise click.ClickException("--payload must be a JSON object")
    with _connect() as conn:
        event = db.insert_event(
            conn,
            _schema(ctx),
            event_type=event_type,
            action_id=action_id,
            actor=actor,
            payload=parsed,
        )
        conn.commit()
    _echo_json(event)
