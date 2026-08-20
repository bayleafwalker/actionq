# Evidence — native harness qualification, Codex / OpenCode / Copilot (N6–N13)

**Recorded:** 2026-08-20 · **Harnesses:** `actionq/harnesses/{codex,opencode,copilot}.py`
**CLIs:** `codex-cli 0.148.0`, `opencode 1.18.18`, `GitHub Copilot CLI 1.0.47`
**Probe:** `verification/probe_native_harness_binding.py` (re-runnable, covers all four)

Completes step 2 of `HANDOFF.md`. Claude was qualified in
`docs/evidence/2026-08-19-native-harness-qualification.md` (N1–N5); this record covers the
remaining three. Same question, from A19:

> **Can the caller establish which model actually ran, and at what point relative to consuming
> subscription usage?**

All three CLIs are installed and two of the three adapters' module docstrings assert facts about
them that are **no longer true**. That is the headline: the adapters were written against an
environment that has since moved, and nothing detected the drift.

Usage below is in each vendor's own unit — Codex tokens, Copilot premium requests. No figure in
this document is money; this estate holds subscriptions only, with API billing enabled nowhere.

---

## Summary

| Harness | Flag drift | Read-back | Phase | Fails closed? | Qualified? |
|---|---|---|---|---|---|
| `claude` (N1–N5) | none | `modelUsage`, resolved dated id | **post-hoc** | yes, but see N4 | partially |
| `codex` | none | **none anywhere** | — | yes (server-side) | **no** |
| `opencode` | n/a — no turn completes | — | — | unknown | **no** |
| `copilot` | adapter refuses to run at all | `chosenModel`/`model.call_start` | **pre-flight** | yes (local) | **best of the four** |

---

## N6 — Codex emits no drifted flags, but omits two that matter

Every flag `CodexAdapter.build_command` emits (`exec`, `--skip-git-repo-check`, `--json`,
`--sandbox`, `-C`, `--model`, trailing `-`) exists on the installed CLI. **No drift.**

Two flags the CLI offers and the adapter does *not* pass are load-bearing for this architecture:

- **`--ignore-user-config`** — without it, `~/.codex/config.toml` is layered under every
  invocation. See N7.
- **`--ephemeral`** — without it, every dispatched turn persists a session rollout to
  `~/.codex/sessions/`. The Claude adapter deliberately passes `--no-session-persistence`; the
  Codex adapter is silent, so the two harnesses have **opposite** persistence behaviour with
  nothing in the code marking the asymmetry as a choice.

## N7 — Codex runs whatever the ambient user config says, and the caller cannot see it

`HarnessInvocation.model` is optional. When it is `None` the adapter emits no `--model`, and the
model is resolved from `~/.codex/config.toml`, which on this host reads:

```toml
model = "gpt-5.6-luna"
model_reasoning_effort = "xhigh"
service_tier = "fast"
```

A default-model dispatch therefore ran **`gpt-5.6-luna`** — a model actionq never requested, never
recorded, and cannot observe. The file is outside the repo, outside NixOS management, and mutable
by any tool the user runs. Reasoning effort and service tier are ambient in exactly the same way.

## N8 — The Codex JSONL stream never names the model. There is no read-back at all.

A complete successful run, in full:

```json
{"type":"thread.started","thread_id":"01a01db6-..."}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"ok"}}
{"type":"turn.completed","usage":{"input_tokens":15520,"cached_input_tokens":9984,
  "cache_write_input_tokens":0,"output_tokens":5,"reasoning_output_tokens":0}}
```

Usage is reported **without model attribution**. This is strictly weaker than Claude's N2: Claude
tells you post-hoc which model burned the tokens, Codex never tells you at all.

The persisted rollout file *does* contain `"model":"gpt-5.6-luna"` — but it is **not a read-back**.
Probed with a bogus id, the rollout records `definitely-not-a-model-xyz`, a model that never ran.
It echoes the *resolved request*, not what the server served. Treating it as confirmation would be
a mistake, and it only exists at all because the adapter omits `--ephemeral` (N6) — i.e. the one
artifact that looks like evidence is a side effect of an unmarked default.

## N9 — Codex fails closed on an unknown model, but the check is the server's, not the CLI's

`--model totally-not-a-model-xyz` and `--model claude-opus-5` both exit 1 with no `turn.completed`
and **no usage consumed**. Better than Claude's N4, where a rejected turn still burned 897 tokens.

But read the sequence:

```
item_0 error: Configured service tier `priority` is not advertised as supported for model `X`
              and will be omitted from requests.
item_1 error: Model metadata for `X` not found. Defaulting to fallback metadata; this can
              degrade performance and cause issues.
turn.started
error: 400 invalid_request_error — 'X' is not supported when using Codex with a ChatGPT account.
```

The CLI **proceeds past an unknown model**, substituting fallback metadata, and starts the turn.
Only the account backend rejects it. So "fails closed" holds *for ids this entitlement refuses* —
it is not a local guarantee. An id the server happens to accept but the caller never meant would
run silently, with N8 ensuring nobody could tell.

## N10 — OpenCode's docstring caveat is stale, and the adapter has never completed a turn

`opencode.py` states the installed binary "resolves to a bundled Bun runtime shim rather than a
working OpenCode CLI (`opencode --help` prints Bun's own help)". **That is no longer true.**
`opencode 1.18.18` is a real CLI with a documented `run` subcommand, `--model`, and a
`--format json` event stream the adapter does not use.

The caveat's *conclusion* nevertheless still stands, for a different reason. Four attempts —
including `--pure` (no plugins) and `--auto`, up to 7 minutes — all hang at the same point:

```
message=init
```

Zero bytes on stdout, ~1.1 KB of INFO logs on stderr, no model request ever issued. Reproducible.

**Cause not isolated.** Two likely sandbox artifacts were ruled out — loopback bind+connect works
in this environment, and `--pure` excludes plugin loading — but I did not isolate it further, and
this was run under a sandboxed shell. **Do not record this as an OpenCode defect.** What is
established is narrower and sufficient: *no noninteractive OpenCode turn has ever been observed to
complete from this environment*, so the adapter remains unverified exactly as its docstring warns.

## N11 — The shape of that failure is invisible to the adapter, and expensive

Whatever the cause, what `base.invoke` returns is:

```python
HarnessResult(exit_code=-1, stdout="", stderr="", timed_out=True)
```

Indistinguishable from a model that thought silently until the clock ran out. At
`HarnessInvocation.timeout_seconds`' default of **1800 s**, each attempt costs 30 minutes of wall
clock to learn nothing. Two further adapter faults compound it: the prompt is passed as a bare
positional with no `--` separator (a prompt beginning with `-` is parsed as a flag), and
`--format json` is never passed, so even a successful turn would return prose with no structured
read-back.

## N12 — Copilot's non-scope rationale is stale; a noninteractive path is proven

`copilot.py` raises `HarnessUnsupportedError` unconditionally, on the premise that
"`gh copilot` … is documented as interactive-preview only" with "no confirmed noninteractive,
already-authenticated invocation path here".

Both halves are now wrong. Copilot is no longer a `gh` extension — it is a standalone `copilot`
binary at `~/.local/bin/copilot`, already authenticated, whose own `--help` says: *"use -p/--prompt
for non-interactive scripting."* This invocation completes and exits 0:

```bash
copilot -p "<prompt>" --allow-all-tools --no-color --output-format json --add-dir <worktree>
```

`--allow-all-tools` is documented as **required for non-interactive mode**. The adapter still
points `bin_path` at `gh`.

## N13 — Copilot is the only harness of the four with a *pre-flight* model read-back

Its JSONL names the model **before the call is issued**, and again per message:

```json
{"type":"session.auto_mode_resolved","data":{"chosenModel":"claude-haiku-4.5",
  "candidateModels":["claude-haiku-4.5","gpt-5-mini"],"routingMethod":"hydra","confidence":0.97}}
{"type":"session.tools_updated","data":{"model":"claude-haiku-4.5"}}
{"type":"model.call_start","data":{"turnId":"0","model":"claude-haiku-4.5"}}
{"type":"assistant.message","data":{"model":"claude-haiku-4.5","content":"ok"}}
{"type":"session.usage_checkpoint","data":{"totalPremiumRequests":0.33,"totalNanoAiu":870400000}}
```

This is what A17 wanted and N2 found Claude's native path lacked. Three properties matter:

1. **Pre-flight** — `model.call_start` precedes the call, so a mismatch is catchable before usage
   is consumed, not merely attributable afterwards.
2. **Per-message attribution** — `assistant.message` carries its own `model`, stronger than
   Claude's aggregate `modelUsage`.
3. **Usage in the subscription's own unit** — `totalPremiumRequests`, not a notional dollar figure.
   Nothing needs reinterpreting the way N2's `costUSD` did.

An unavailable `--model` **fails closed locally**, before any call: exit 1, no usage, error on
*stderr* while stdout stays valid JSONL. (An earlier read of mine claimed the error corrupted the
JSON stream; that was an artifact of my own `2>&1` and is **not** a finding.)

The one caveat for reproducibility: with no `--model`, Copilot resolves `auto`, which routes per
turn — observed choosing `claude-haiku-4.5` on one run and `gpt-5-mini` on the next, from the same
prompt. A dispatcher that needs a fixed model must pin `--model` explicitly; `auto` is a *reported*
choice, not a stable one.

---

## Non-findings (recorded so they are not mistaken for evidence later)

- **Copilot returning `400 model_not_supported` three times.** One run at 05:54Z retried and failed
  on `claude-haiku-4.5`; the identical invocation succeeded at 06:01Z. **Transient**, not an
  entitlement limit. Do not cite it.
- **The OpenCode hang as a product defect.** See N10 — reproducible, but the cause was not isolated
  and a sandbox artifact is not excluded.
- **Codex's `service tier 'priority'` warning** while config says `service_tier = "fast"`. Noticed,
  not chased; no bearing on binding.

---

## What this means for step 3

The deletion argument does not depend on any of this — F9 already carries it. What these findings
change is the *shape of the federation layer that replaces the execution plane*:

1. **Binding assurance is per-vendor and unequal.** Copilot verifies pre-flight, Claude attributes
   post-hoc, Codex cannot do either, OpenCode is unverified. A federation layer cannot offer one
   uniform "which model ran" guarantee across harnesses. It must **record the assurance level
   alongside the execution reference** — that is a schema requirement, not an implementation
   detail.
2. **Ambient config is part of the execution identity.** N7 means a Codex execution reference is
   meaningless without capturing the config that resolved it. Either pass `--ignore-user-config`
   and set everything explicitly, or record the resolved model from the rollout — knowing N8's
   caveat that it is the request, not the service.
3. **`base.py`'s contract is the obstacle.** Every read-back above requires inspecting output for
   meaning, which `base.py` forbids adapters from doing. As N5 already concluded for Claude, the
   reconciliation belongs **just above the adapter**. Three more harnesses now say the same thing.
4. **Two adapters encode stale environment facts as permanent decisions** (N10, N12). Copilot is
   refused for a reason that expired; OpenCode carries a caveat whose stated evidence is gone. The
   qualification record is the durable asset (A19) *because* the adapters rot — but only if it is
   re-run. `verification/probe_native_harness_binding.py` exists so that it can be.
