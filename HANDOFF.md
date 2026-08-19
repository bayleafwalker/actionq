# Handoff — actionq

**As of:** 2026-08-19 · **Branch:** `feat/acp-backend-registry` ([PR #26](https://github.com/bayleafwalker/actionq/pull/26)) · **Suite:** 718 passed, 19 skipped, 0 failed

Read this, then `docs/plans/2026-08-19-execution-plane-deletion-constraint.md`, then
`docs/evidence/2026-08-19-acp-v1-conformance.md` (findings A1–A19). This file is the map.

---

## 0. LIVE STATE — a production daemon is paused right now

The devbox ActionQ daemon is **fenced and not claiming work**, deliberately, since
2026-08-19:

```bash
ssh devbox-agent 'cat /home/agent/.local/state/actionq-dispatcher/PAUSED'   # why + when
ssh devbox-agent 'rm  /home/agent/.local/state/actionq-dispatcher/PAUSED'   # resume
```

This is step 1 of the deletion work below: stop consuming new work, change nothing else,
and see what actually breaks. The unit stays `active` and the DB is untouched. The fence is
read at `daemon_lifecycle.run_once()` **after** `recover_stale_state()` and **before**
`client.claim()`, so recovery still runs and in-flight work is not killed.

**If you are wondering why no work is being dispatched, this is why** — but note that it was
*already* not dispatching before the fence went up, and that the queue has no automated producer
at all (`docs/evidence/2026-08-19-devbox-fence-baseline.md`, F9). Decide deliberately whether to
resume; do not resume by reflex.

The daemon PID changed on 2026-08-19T20:13:56Z (cluster maintenance evicted `actionq-pg`; the
daemon crash-looped and systemd restarted it, 10 restarts). The fence survived it. The running
process is now the same code as the installed 0.1.28 package.

Side effect worth knowing: `coordinator_paused` is emitted every poll (30 s), so a long
pause is ~2,880 events/day into the event log.

---

## 1. The governing constraint

> **Do not own an agent execution plane.** Execution is mature *in the products around us*.
> Cross-product, subscription-preserving orchestration is not a clean commodity — and that
> gap is not an invitation to build one. Keep the coordination layer thin enough to throw
> away.

Measured in this repo (2026-08-19):

| Surface | LOC | |
|---|---:|---|
| `daemon_runner`, `session_wrapper`, `daemon_*`, `server`, `application_dispatch`, `routing`, `usage_limit`, `scope_iterate` | ~5,030 | execution plane — delete |
| lease/claim/heartbeat, entangled through `db.py` (2,051) + `schema.py` (1,588) | large | same, harder to extract |
| `execution_contract`, `execution_boundary`, `context_policy`, `acp/backends` | ~1,005 | **own** |
| `acp/v1.py` | 760 | integrate — thin adapter, replaceable by construction |

**The homemade execution plane is ~5x the size of the part worth owning.** That ratio is the
argument.

---

## 2. The goal

> **Reduce actionq from an execution plane to a federation layer — evidence first, deletion
> last, without breaking the deployed devbox host.**

Not a rewrite, and not a plan to execute top-down. Each step must produce evidence that
justifies the next one. Ordered:

1. **Read the fence experiment.** The daemon has been paused since 2026-08-19. What actually
   stopped working? *Done when:* there is a written list of what broke and what did not.
   A quiet week is itself the strongest possible argument for deletion — record that outcome
   as carefully as a noisy one.
2. **Qualify the native harnesses, the way ACP was qualified.** `actionq/harnesses/`
   (`claude.py`, `codex.py`, `opencode.py`, `copilot.py`) invoke vendor CLIs directly. Under
   this architecture *those are the integration points*, and their binding assurance is
   **completely unmeasured** — everything in A12–A19 characterises the ACP bridge, which
   should not be in Claude's path at all. *Done when:* each native harness has a
   qualification record in the shape of the A19 table, produced by a re-runnable probe under
   `verification/`.
3. **Delete, in an order that keeps devbox working.** Start with what the fence proved is
   unused. The lease/claim surface inside `db.py`/`schema.py` comes last because it is
   entangled with schema versions that the deployed daemon still reads.

*Done when the whole thing is done:* what remains is work identity, relations and revisions;
authority; evidence requirements and acceptance; references to external executions;
reconciliation; backend qualification; and a narrator-facing read/write surface. **No worker
daemon, no queue, no leases, no fan-out engine.**

---

## 3. Settled — do not relitigate without new evidence

| Decision | Why | Evidence |
|---|---|---|
| `session/set_model` is not a binding channel for Claude Code | returns success for *any* string — another vendor's id, a local profile, garbage, empty string — and there is no read-back | A13 |
| Claude Code **is** bindable, via `<cwd>/.claude/settings.json` + `currentModelId` read-back | verifiable before a single token is billed | A17 |
| Binding needs **two** checks, not one | substring matching downgrades `sonnet-5`→`sonnet`; an unadvertised id is *adopted verbatim* and round-trips cleanly | A17, `verify_bound_model` |
| ACP is **one integration, not the funnel** | native harnesses preserve subscription, session, context and first-party improvements | A19 |
| The durable output of the ACP work is the **qualification record**, not an implementation | workflow-specific, outlives the adapter, regenerates in minutes | A19 |
| `session/list` stays below the runner boundary | machine-global: 50 sessions across 12 unrelated projects, incl. the observing session | A15 |
| Providers are **not fungible** for subscription work | OpenCode cannot use Claude Pro/Max; Agentic Workflows use provider credentials | plan doc |
| Work state stays ours | adopting GitHub Issues means adopting its meaning of open/closed in place of `ready\|claimed\|blocked\|accepted\|rejected\|superseded\|integrated` | plan doc |

A16 is **superseded** and kept deliberately: it recommended refusing Claude on premises that
were wrong (API pricing under a subscription; misapplying `local-inference`'s single-GPU
admission rule across tiers). The reusable lesson is that **an execution lane was judged only
as a queue consumer**, when a frontier subscription's value is supervising work it did not
perform.

---

## 4. Traps in this repo

- **The verification bundle digests `tests/**/*.py`.** Regeneration must be the **last**
  step; any later test edit invalidates it. This caught me three times in one session:

  ```bash
  nix shell nixpkgs#postgresql -c bash -c 'for h in pruning snapshot-race non-disclosure \
    redaction response-loss bounded-wait legacy-quarantine fencing; do \
    .venv/bin/python verification/run_action_resource_history.py "$h" || echo "FAILED $h"; done'
  ```
- **The suite needs PostgreSQL binaries on PATH.** `nix shell nixpkgs#postgresql -c ...`.
  The recorded `environment.postgres_version` is now *observed* at regeneration rather than
  carried forward from the previous packet — it claimed 18.4 while this host runs 18.6.
- **`tests/test_managed_dispatch.py` needs the agentops sibling checkout** to be on a branch
  carrying `templates/dispatch/scripts/render_managed_capsule.py`. It now skips with a clear
  message instead of aborting collection of all 700+ tests.
- **Never `pgrep -f` a pattern your own command line contains.** A cleanup loop matched its
  own ssh invocation and killed its own session mid-run. Use `[b]in/postgres` style.
- **Disposable test clusters leak on abnormal exit.** `pytest_unconfigure` does not run under
  SIGKILL; devbox had clusters alive 8 days. Now handled by `atexit` + SIGTERM/SIGINT/SIGHUP
  handlers *and* a startup reaper (`_reap_stale_clusters`) that skips live clusters by
  probing `postmaster.pid`. The reaper is the half that survives SIGKILL.

---

## 5. Open work

- **Step 1 is closed.** `docs/evidence/2026-08-19-devbox-fence-baseline.md` (F1–F10). It did
  *not* conclude the way it was designed to: the fence was fed, shown to hold against real
  claimable work (F8), then the soak was **skipped by decision** after 21 minutes and the probe
  actions cancelled. **Do not cite the soak.** The conclusion rests on **F9** — seventeen actions
  across the queue's entire history, all hand-created, and no dispatch planner exists on any
  host. There is no standing demand for a claim loop. That is the deletion argument for the
  queue; it is *not* an argument that dispatched execution was worthless (action 14 succeeded),
  only that demand is episodic and human-initiated — a shape a federation layer serves without
  a daemon.
- **Native harness qualification is unstarted** and is now the highest-value next measurement
  outright, since step 1 is closed and step 3 is deletion work.
- `actionq-dispatcher`'s remote branches were never audited: its `gh pr list` failed during
  the branch sweep and it was skipped rather than assumed empty. Redo that one repo.
- ~~Untangle the devbox schema-error / different-binary contradiction~~ **resolved** — F1, F2.
  The `too-new` error was `actionctl`'s, surfaced through the daemon's `claim` subprocess while
  the daemon adapter itself reported schema 12 compatible; the 2026-08-16 restart cleared it.
  There is no second binary — uv rewrote the package on disk 2026-08-19 under a process still
  running the 2026-08-16 code. Residue worth keeping: `actionctl` and the daemon adapter are
  independently-installed uv tools that drifted across a schema bump and only a restart
  reconciled them.
- 20 previously machine-only branches were pushed to origin across the estate; they still
  need merge-or-drop decisions.
