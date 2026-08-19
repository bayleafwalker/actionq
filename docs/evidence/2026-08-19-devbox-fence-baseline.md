# Evidence — the devbox fence experiment has no baseline (F1–F6)

**Recorded:** 2026-08-19 · **Host:** devbox (`actionq-dispatch.service`, PID 3124844)
**Question:** the handoff flagged an unresolved contradiction — the unit reports `active` since
2026-08-16, its dispatcher log ends 2026-08-15 with `actionq schema 'execution' is too-new:
schema version 12 exceeds supported maximum 11`, and the running process appeared to be a
different binary. Untangle which component that error belongs to *before* attributing anything
to the fence.

All observations are read-only. Nothing on devbox was changed; the fence is still up.

---

## F1 — The schema error belongs to `actionctl`, the CLI subprocess, not the daemon core

The traceback in `~/.local/state/actionq-dispatcher/daemon.log` terminates in
`actionq/daemon.py:claim` → `_run("claim", ...)` → `raise RuntimeError(detail)`. `_run` shells
out to `actionctl`; the `too-new` message is that subprocess's stderr, surfaced verbatim. The
daemon's own preflight is the opposite verdict — from the unit journal at start:

```
"minimum_schema_version": 1, "maximum_schema_version": 12,
"observed_schema_version": 12, "state": "compatible"
```

So the daemon adapter supported schema 12 while the `actionctl` it invoked capped at 11. It
crashed the pane (`Pane is dead (status 1, Sat Aug 15 14:27:28 2026)`) and the 2026-08-16
00:49:59 restart cleared it. **The error is resolved and belongs to the CLI, not the daemon.**

## F2 — The "different binary" is the same path, replaced underneath a running process

Running: `/home/agent/.local/share/uv/tools/actionq/bin/python /home/agent/.local/bin/actionq-daemon
--config /etc/actionq/config.toml`, started `Sun Aug 16 00:50:06 2026`.

Package files on that path were rewritten `2026-08-19 00:50:10` (uv tool upgrade to 0.1.28,
matching `~/.local/share/uv/tools/` mtimes). The process holds the 2026-08-16 code in memory;
the on-disk source is three days newer. That, and nothing else, is why the stack-frame line
numbers in the log's head (2157/2132/897/340) differ from its tail (2435/1001/351) — one log
file, several daemon versions. **No second binary exists.**

## F3 — The fence is live and correctly wired

`config.toml` sets `pause_file = "/home/agent/.local/state/actionq-dispatcher/PAUSED"`, overriding
the package default (`~/.local/state/actionq/PAUSED`, `daemon_config.py:116`). The running daemon
emits, every 30 s since the fence went up:

```
22788  2026-08-19T19:06:20.025339Z  coordinator_paused
       actor:   actionq-daemon:devbox:3124844:9ccc27f1-...
       payload: {"pause_file": "/home/agent/.local/state/actionq-dispatcher/PAUSED"}
```

The PID matches the running process, and the path matches the file that was written. **The fence
does what the handoff says it does.**

## F4 — Nothing was being claimed for four days *before* the fence

Every event in the log from the 2026-08-16 unit start to the 2026-08-19 19:06 fence:

| Since | Event types |
|---|---|
| 2026-08-16T00:00Z | `coordinator_paused` × 41 — **and nothing else** |
| 2026-08-19T19:06Z | `coordinator_paused` × 41 (the same ones; all post-fence) |

Last non-`coordinator_paused` event of any kind, from any host: **2026-08-15T16:18:08Z**
(`action_completed`, action 14) — and that actor was `WorkstationLinux`, not devbox.

Absence of `coordinator_cycle` is *not* evidence here: it appears only in `cli.py:426`'s
`_EMIT_EVENT_TYPES` allowlist and is never emitted by the daemon. The load-bearing absence is
the claim events.

## F5 — The queue is empty, and has been since 2026-08-15

`actionctl ls`: 11 `failed`, 3 `completed`, **zero** `ready`/`claimed`/`blocked`. Newest action
`created_at` 2026-08-15T16:08:08Z. A daemon claiming nothing from an empty queue is correct
behaviour, not a symptom.

## F6 — devbox's last real work was 2026-08-09, and it failed

47 non-paused devbox-actor events since 2026-07-01; the last eight are one action (id 7), which
ran 2026-08-09T13:32:25Z → 13:32:33Z and ended `action_failed` / `"failure_reason": "daemon
session failed"` (a `canary:served-execution-success` probe). Ten days before the fence.

---

## What this means for step 1 of the goal

**The fence experiment currently cannot produce the evidence it was designed to produce.** It
was set up to answer "what breaks when devbox stops claiming?" — but devbox had already claimed
nothing for ten days, and the queue has been empty for four. A quiet week from here is
indistinguishable from the quiet fortnight that preceded it, so it argues for nothing. Reading
it as "nothing broke when we fenced it" would be reading a pre-existing idle state as an
experimental result — exactly the misattribution the handoff warned about.

The fence is not wrong and should not be removed on this basis; it is simply unfalsifiable as
it stands.

To make it informative, work has to exist for the fence to withhold: enqueue actions devbox
would otherwise claim, leave the fence up, and record what fails to happen and who notices.
Until then the honest record of step 1 is *"no signal — the daemon was already idle"*, not
*"nothing broke"*.

Two things worth separating from the deletion argument, because they are true regardless of it:

- The 2026-08-09 canary **failed**, and nothing has exercised the devbox execution path since.
  The execution plane's last observed behaviour on this host is a failure, not a success.
- `actionctl` and the daemon adapter drifted apart across a schema bump (F1) and only a restart
  reconciled them. That is a live coupling between two independently-installed uv tools.
