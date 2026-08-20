# Handoff — actionq

**As of:** 2026-08-20 · **`main`:** `0fb33f6` (PR #28 merged) · **Suite:** 718 passed, 19 skipped, 0 failed

**Open PR you own:** [#29](https://github.com/bayleafwalker/actionq/pull/29) on
`evidence/native-harness-qualification` — both harness qualification records (N1–N13) and the
re-runnable probe. **Merge it first;** step 3 starts from it.

Read this, then `docs/plans/2026-08-19-execution-plane-deletion-constraint.md`, then the two
evidence records: `docs/evidence/2026-08-19-acp-v1-conformance.md` (A1–A19, the ACP bridge) and
`docs/evidence/2026-08-19-native-harness-qualification.md` (N1–N5, the Claude native harness)
and `docs/evidence/2026-08-20-native-harness-qualification-codex-opencode-copilot.md` (N6–N13,
the other three).
`docs/evidence/2026-08-19-devbox-fence-baseline.md` (F1–F10) is the closed fence experiment —
read it only if you are tempted to cite the fence for anything. This file is the map.

---

## 0. LIVE STATE — the daemon is running; the fence experiment is over

The devbox ActionQ daemon is **unfenced and claiming normally** as of 2026-08-19T20:18Z. The
queue holds no claimable work (11 `failed`, 3 `completed`, 3 `cancelled`), so a quiet daemon is
expected and is not a fault.

```bash
ssh devbox-agent 'systemctl is-active actionq-dispatch.service'
ssh devbox-agent 'ls /home/agent/.local/state/actionq-dispatcher/PAUSED'   # absent = running
```

The fence (step 1) ran 2026-08-19 19:06Z → 20:18Z and is **closed**. It was shown to hold
against real claimable work, then ended by decision after a 21-minute soak; the probe actions
were cancelled. **Do not cite the soak as evidence** — see §5 and
`docs/evidence/2026-08-19-devbox-fence-baseline.md` (F1–F10).

To re-fence, write the pause file back (`pause_file` in `/etc/actionq/config.toml`, which is
nix-managed and points at `~/.local/state/actionq-dispatcher/PAUSED`, *not* the package default).
It is read at `daemon_lifecycle.run_once()` **after** `recover_stale_state()` and **before**
`client.claim()`, so recovery still runs and in-flight work is not killed. While fenced,
`coordinator_paused` is emitted every poll (30 s) — ~2,880 events/day into the event log.

Two things a fresh session should not misread:

- **Nothing autonomously feeds this queue** (F9). Seventeen actions across its entire history,
  all hand-created. An idle daemon is the normal condition, not a symptom.
- The daemon restarted at 2026-08-19T20:13:56Z (10 restarts) when planned cluster maintenance
  evicted `actionq-pg`. It recovered unaided. That outage was **maintenance, not a fault**, and
  is recorded as an explicit non-finding.

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

1. ~~**Read the fence experiment.**~~ **CLOSED** — F1–F10. It did not conclude as designed: the
   fence was fed, proven to hold against real claimable work, then ended by decision after a
   21-minute soak. **Never cite the soak.** The conclusion is **F9**: seventeen actions across
   the queue's entire history, all hand-created, and no dispatch planner exists on any host.
   *There is no standing demand for a claim loop.*
2. ~~**Qualify the native harnesses, the way ACP was qualified.**~~ **CLOSED** — N1–N5 (Claude)
   and N6–N13 (Codex, OpenCode, Copilot), re-runnable via
   `verification/probe_native_harness_binding.py`. Binding assurance is **per-vendor and
   unequal**: Copilot verifies pre-flight, Claude attributes post-hoc, Codex cannot do either,
   OpenCode has never completed a turn here. `actionq/harnesses/`
   (`claude.py`, `codex.py`, `opencode.py`, `copilot.py`) invoke vendor CLIs directly. Under
   this architecture *those are the integration points*, and their binding assurance is
   **completely unmeasured** — everything in A12–A19 characterises the ACP bridge, which
   should not be in Claude's path at all. *Done.*
3. **Delete, in an order that keeps devbox working.** ← **YOU ARE HERE.** Start with what the fence proved is
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
| Claude's native harness emits **no drifted flags** — every flag `build_command` produces exists on the installed CLI | probed against `claude --help` on this host | N1 |
| The native harness has a **real model read-back**: `modelUsage` keys name the resolved dated id | `--output-format json` returns it; `provider: "firstParty"` confirms the subscription path | N2 |
| That read-back is **post-hoc, not pre-flight** — unlike A17's `currentModelId` | it arrives only after the turn has already consumed usage | N2, A17 |
| An unknown model **fails closed**, consuming nothing | probe: rejected before any turn ran | N3 |
| `is_error` and `modelUsage` are **independent signals** — a rejected turn can still consume usage on a model the caller never asked for | `gpt-4o` was rejected with the same error class, yet 897 input tokens were consumed against `claude-haiku-4-5` | N4 |
| `session/list` stays below the runner boundary | machine-global: 50 sessions across 12 unrelated projects, incl. the observing session | A15 |
| Binding assurance is **per-vendor and unequal**; there is no uniform "which model ran" guarantee | Copilot pre-flight, Claude post-hoc, Codex none, OpenCode unverified | N6–N13 |
| The federation schema must record **assurance level alongside the execution reference** | follows directly from the row above; it is a schema requirement, not an implementation detail | N6–N13 |
| Codex has **no model read-back at all** | the JSONL reports usage without model attribution; the rollout file echoes the *resolved request*, and names a model even when none ran | N8 |
| Ambient user config is **part of the execution identity** for Codex | `~/.codex/config.toml` supplied `gpt-5.6-luna` on a dispatch that requested nothing | N7 |
| Copilot **has** a proven noninteractive path and the best binding of the four | `copilot -p … --allow-all-tools --output-format json`; `model.call_start` precedes the call | N12, N13 |
| `base.py`'s "never inspect output for meaning" contract is the obstacle to binding assurance | every read-back above requires inspecting output; reconciliation belongs *just above* the adapter | N5, N6–N13 |
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
- **A harness rejection is not proof that nothing ran.** N4: `--model gpt-4o` returned an
  error envelope *and* billed 897 input tokens against `claude-haiku-4-5`. Any qualification
  probe must read `is_error` and `modelUsage` separately; treating the error as "no execution"
  under-reports usage against a model the caller never requested.
- **Dollar figures in Claude CLI envelopes are notional.** `total_cost_usd` / `costUSD` are
  equivalent-usage accounting, not spend: this estate holds subscriptions only, with API
  billing enabled nowhere (`provider: "firstParty"` is the envelope's own confirmation).
  Never write them up as cost.
- **The `actionq-dispatcher` checkout's GitHub remote is `bayleafwalker/actionq-dispatch`** —
  no `-er`. A `gh` command that infers the repo from the directory name fails; that is what
  made the branch sweep skip it, not an access problem.
- **Two harness adapters encode stale environment facts as permanent decisions.**
  `copilot.py` refuses to run on a premise that expired (it is no longer a `gh` extension);
  `opencode.py`'s docstring says the binary is a broken Bun shim, which it no longer is. Nothing
  detected either drift. Re-run `verification/probe_native_harness_binding.py` before trusting
  any adapter docstring.
- **`opencode run` hangs at `init` in this environment** — zero stdout, ~7 min observed, exit
  124. Cause not isolated; a sandbox artifact is *not* excluded, so do not write it up as an
  OpenCode defect. Budget for it: `HarnessInvocation.timeout_seconds` defaults to **1800 s**, so
  one attempt burns 30 minutes to learn nothing.
- **Redirecting `2>&1` while probing a `--output-format json` CLI manufactures false findings.**
  Copilot writes its `--model` rejection to stderr and keeps stdout valid JSONL; merging the
  streams made it look like a broken JSON contract. Keep the streams separate.
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
- **Step 2 is closed.** All four harnesses have qualification records and a re-runnable probe.
  The follow-on work it exposed, none of it blocking step 3:
  - `codex.py` should pass `--ignore-user-config` (or capture the resolved config), and decide
    `--ephemeral` deliberately — today it silently persists sessions while `claude.py` does not.
  - `copilot.py` can be implemented; N12 gives the proven invocation. Pin `--model`, since the
    default resolves `auto` and routes per turn.
  - `opencode.py` stays unverified until one turn completes somewhere; fix the missing `--`
    prompt separator and pass `--format json` when it does.
  - Model reconciliation belongs **above** the adapter, per `base.py`'s contract (N5, point 3).
- **PR #29 is open** on `evidence/native-harness-qualification`, now carrying both
  qualification records and the probe. Merge it before starting step 3.
- ~~Audit `actionq-dispatcher`'s remote branches~~ **done, clean** — one remote branch, PR #1
  MERGED, 0 commits not in `main`. The earlier `gh pr list` failure was the repo-name mismatch
  in §4, not an unaudited backlog.
- ~~Untangle the devbox schema-error / different-binary contradiction~~ **resolved** — F1, F2.
  The `too-new` error was `actionctl`'s, surfaced through the daemon's `claim` subprocess while
  the daemon adapter itself reported schema 12 compatible; the 2026-08-16 restart cleared it.
  There is no second binary — uv rewrote the package on disk 2026-08-19 under a process still
  running the 2026-08-16 code. Residue worth keeping: `actionctl` and the daemon adapter are
  independently-installed uv tools that drifted across a schema bump and only a restart
  reconciled them.
- 20 previously machine-only branches were pushed to origin across the estate; they still
  need merge-or-drop decisions.
- Minor, noted only: two detached-HEAD worktrees under `/projects/dev/_projects/` reference
  actionq-dispatcher (`2f299ce`, `9acf071`) and were never reconciled; the merged remote branch
  `chore/remove-stale-environment-pointer-20260811` can be deleted.
