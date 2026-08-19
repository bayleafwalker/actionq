# Evidence — native harness qualification, Claude (N1–N5)

**Recorded:** 2026-08-19 · **Harness:** `actionq/harnesses/claude.py` · **CLI:** installed `claude`

Step 2 of the goal in `HANDOFF.md`: qualify the native harnesses the way ACP was qualified
(A12–A19). Those findings characterise the *ACP bridge*, which under this architecture should not
be in Claude's path at all. `claude.py` is the real integration point and was **completely
unmeasured**. This record covers Claude; `codex.py`, `opencode.py` and `copilot.py` remain open.

The question is A19's, not "does it run": **can the caller establish which model actually ran,
and at what point relative to being billed?**

---

## N1 — The adapter's emitted flags all exist on the installed CLI

`ClaudeAdapter.build_command` emits `-p`, `--output-format json`, `--no-session-persistence`,
`--add-dir <worktree>`, and conditionally `--model`, `--allowedTools`, `--disallowedTools`. Every
one is present in the installed CLI's `--help`, including `--no-session-persistence`, which is the
flag most likely to have bit-rotted since the adapter was ported from `actionq-dispatch`'s
`ClaudeWorker` in July. **No drift.**

## N2 — The native path *does* have a model read-back, and it is richer than anything ACP offered

A successful `--output-format json` envelope carries `modelUsage`, keyed by the **resolved
concrete model id**:

```json
"modelUsage": {
  "claude-haiku-4-5-20251001": {
    "inputTokens": 9, "outputTokens": 37, "costUSD": 0.034924,
    "canonicalModel": "claude-haiku-4-5", "provider": "firstParty",
    "contextWindow": 200000, "maxOutputTokens": 32000
  }
}
```

Invoked with the alias `--model haiku`, the envelope names the dated id that actually served the
turn, its canonical form, and that it was `firstParty` — i.e. subscription, not API. This is a
genuine binding channel and it settles attribution, which A13 established `session/set_model`
could not do at all.

**But it is post-hoc.** The read-back arrives *after the turn has run and consumed subscription
usage.* (`total_cost_usd`/`costUSD` in this envelope is **notional usage accounting, not money** —
this estate holds subscriptions only, with API billing enabled nowhere. `provider: "firstParty"`
is the envelope's own confirmation of that. Read every dollar figure in this document as
"equivalent usage", never as spend.) A17's `.claude/settings.json` + `currentModelId` check remains the only
**pre-flight** verification, and `claude.py` uses neither channel — `build_command` writes
`--model` and never reads anything back.

## N3 — An unrecognised model id fails closed, consuming nothing

`--model definitely-not-a-model-xyz`:

```
[claude-code:unrecognized_model] {"model":"definitely-not-a-model-xyz","query_source":"sdk"}
is_error: true   total_cost_usd: 0   modelUsage: {}   duration_api_ms: 0
```

Nothing ran and nothing was consumed. This is the *opposite* of the ACP behaviour in A13, where any
string — garbage included — returned success. **On this axis the native path is strictly better
than the bridge.**

## N4 — But rejection is not uniformly free, and a rejected turn can consume usage on a model the caller never asked for

`--model gpt-4o` (another vendor's id) is also rejected as `unrecognized_model` — and yet:

```
is_error: true   total_cost_usd: 0.000952
modelUsage: { "claude-haiku-4-5-20251001": { "inputTokens": 897, "outputTokens": 11, ... } }
```

Same error class as N3, materially different outcome: 897 input tokens were consumed against
**`claude-haiku-4-5`, a model the caller never requested**, on a turn that reports `is_error:
true`. No money moved — this is subscription usage — but usage is the scarce resource under a
subscription, and it was spent on a model nobody selected. Whatever handles this path performs its own model call before or during the rejection.

This is the A17 lesson arriving again by a new route: **one check is not enough.** A caller
trusting `is_error` alone concludes "nothing happened" and is wrong about both usage and model
attribution. A caller trusting `modelUsage` alone sees a Haiku turn it cannot explain.

**Qualification rule for this harness:** treat `is_error` and `modelUsage` as *independent*
signals. Non-empty `modelUsage` on an errored turn means something ran that you did not ask for —
reconcile it rather than discarding it. Under a subscription this matters *more* than it would
under API billing: usage is rationed and non-transferable, so an unrequested Haiku turn is drawn
from the same allowance the requested work needs.

## N5 — What `claude.py` would need to be qualified, not merely working

Today the adapter is write-only with respect to model binding. To reach the A19 standard it needs:

1. **Pre-flight** — the A17 `.claude/settings.json` + `currentModelId` read-back, verified before
   the subprocess starts, with the two checks A17 requires (no substring matching; an unadvertised
   id round-trips cleanly and must not be treated as adopted).
2. **Post-hoc reconciliation** — assert the `modelUsage` key set matches what was requested, and
   surface a mismatch rather than letting `HarnessResult` pass it through uninspected. Note this
   cuts against `base.py`'s stated contract that an adapter "never inspects the output for
   meaning"; the reconciliation likely belongs just above the adapter, not inside it.
3. **The N4 case as a regression test** — an errored turn with non-empty `modelUsage` is the
   shape most likely to silently regress.

---

## Method

Three live invocations against the installed CLI, prompt `Reply with exactly: ok`, in a
disposable git repo, with `--disallowedTools Bash,Edit,Write` on the first. Total **notional
usage** for this qualification: ~$0.036-equivalent, drawn from a subscription; no API billing is
enabled anywhere in this estate. The findings are re-derivable at that price, which is the point
A19 made about qualification records outliving the adapters they describe.

Not yet measured: `codex.py`, `opencode.py`, `copilot.py`; whether `--no-session-persistence`
actually prevents session creation (a `session_id` is still returned on every turn); and whether
the N4 billed call is fixed-size or scales with prompt length.
