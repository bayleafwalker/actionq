# Proposal: ACP as the harness-facing execution protocol

**Status:** accepted; v1 adapter, envelope enforcement, context enforcement and observed wire shapes implemented 2026-08-19 (Phase 3 — remaining: second-agent fungibility)
**Date:** 2026-08-19
**Targets:** `docs/contracts/vuoro-execution-adapter.md`
**Origin:** RTX 3090 local inference work; see `/projects/dev/local-inference/docs/10-execution-boundary.md`

## Position

Make **ACP the preferred harness-facing execution protocol where available**, and
explicitly **do not** make ACP the internal ActionQ/Vuoro protocol.

```text
                    Vuoro domain
 ┌──────────────────────────────────────────┐
 │ sprintctl                                │
 │     ↓                                    │
 │ ActionQ  ← lifecycle / claims / policy   │
 │     ↓                                    │
 │ execution contract                       │
 │     ↓                                    │
 │ ACP runner/client adapter                │
 └─────┬────────────────────────────────────┘
       │ ACP
       ├──────── OpenCode  `opencode acp`
       ├──────── Claude Agent ACP
       ├──────── Codex ACP
       └──────── future ACP harness
```

Not `Vuoro → OpenCode-specific integration`, and emphatically not
`Vuoro domain model == ACP domain model`.

## Why now

ACP covers almost exactly the mechanics the current runner adapter wants: capability
negotiation, session create/resume, prompt submission, streaming session updates, tool-call
reporting, permission requests, cancellation, working-directory semantics and MCP-server
attachment. Those are **execution mechanics**, not work-management semantics.

**Verified locally (2026-08-19):** `opencode acp` exists in the installed OpenCode 1.18.18
— `start ACP (Agent Client Protocol) server`, JSON-RPC over stdio, with `--port`,
`--hostname` and mDNS options. So this is a real integration path today, not speculative
protocol architecture.

**Verified against the spec:** ACP v1 defines `initialize`, `authenticate`, `session/new`,
`session/prompt`, `session/cancel` (agent side), `session/request_permission` and the
`session/update` notification (client side), with `session/load` behind a capability.

Driving `opencode acp` is materially cleaner than driving `opencode run ...` and inferring
lifecycle from process output.

## Revised adapter shape

Current contract is approximately `prepare / run / cancel / collect`. Proposed:

```text
ExecutionAdapter
    capabilities()
    open(execution_envelope)
    prompt(...)
    events()
    authorize(...)
    cancel()
    close()
```

with implementations:

```text
AcpExecutionAdapter      # the default path
ClaudeNativeAdapter      # fallback / capabilities ACP cannot express
CodexNativeAdapter       # fallback
```

Policy: **ACP-first, native-second.**

```yaml
runtimes:
  opencode:
    adapter: acp
    command: ["opencode", "acp"]
  claude:
    adapter: acp
    command: ["claude-agent-acp"]
  codex:
    adapter: acp
    command: ["codex-acp"]
  weird-future-agent:
    adapter: native
```

Native adapters exist for capabilities ACP cannot expose, incomplete implementations, and
harness-specific optimisations worth preserving. They are not the default path.

## What ACP must not own

ActionQ's existing separation survives unchanged. ACP is **not** authority for:

```text
work identity          claim/lease ownership    readiness
dependency state       execution eligibility    retry policy
acceptance             integration              canonical artifact identity
audit history          operator decisions
```

These mappings would all be wrong:

```text
ACP session         == ActionQ execution
ACP stopReason      == ActionQ outcome
ACP permission      == Vuoro authorization
ACP session history == audit record
```

Correct shape — the ACP session id is an external runtime handle, nothing more:

```text
ActionQ execution ID
    └── runtime:
        protocol: acp
        agent: opencode
        external_session_id: abc123
```

## Permissions: ACP supplies mechanism, Vuoro supplies authority

ACP has a client-side `session/request_permission`. In an editor that means "ask the human".
Here, back it with policy instead:

```text
OpenCode
  ↓ session/request_permission
Vuoro ACP adapter
  ↓
execution policy / envelope
  ├─ already permitted → allow
  ├─ forbidden         → deny
  └─ semantic decision → escalate to human
```

Far better than teaching every harness independently what Vuoro's permission model means.
This is also the natural seam for the "human contribution is perpendicular" work: escalation
is a policy outcome, not a harness feature.

## ACP and MCP are orthogonal

```text
ACP:   how you interact with an agent
MCP:   what capabilities/resources an agent has
Vuoro: what work exists, who may execute it, what completion means, what is retained
```

ACP sessions can be handed MCP servers by the client at session setup, which keeps this
clean: `Vuoro → ACP → agent`, and separately `agent → MCP → sprintctl facade / knowledge /
external systems`.

## Telemetry normalization

Do not mirror ACP updates as Vuoro domain events. Normalize a useful subset:

```text
ACP                        Vuoro execution telemetry
──────────────────────────────────────────────────────
session/new            →   runtime.started
session/update text    →   output.delta
tool_call              →   tool.started
tool_call_update       →   tool.updated
terminal output        →   execution.output
request_permission     →   policy.requested
permission response    →   policy.resolved
session/cancel         →   cancellation.requested
idle + stop reason     →   runtime.stopped
```

Fan out to `auditctl` (durable evidence), Prometheus (metrics), Loki (operational streams).
Keep raw ACP traffic as optional debug evidence, never canonical application state — Vuoro
should not become a transcript database.

## Recovery

Given `ActionQ execution E-123` bound to `ACP session S-987`, if the harness dies ActionQ
still knows E-123's state. Policy then decides: resume S-987, create S-988 from the sealed
execution envelope, or fail E-123.

**Vuoro correctness must not depend on session recovery working.** That is the right failure
boundary, and it is why the session id is a handle rather than an identity.

## Version isolation

Isolate ACP types behind one adapter:

```text
BAD                          GOOD
ActionQ types                ActionQ
  import acp.Session           ↓
  import acp.StopReason      Vuoro Execution Contract
  import acp.ToolCall          ↓
                             adapter/acp/{v1,v2}
```

Implement **v1 first** — that is what deployed agents speak, and it is the version whose
method set is confirmed above. Structure the adapter so a later version is another
codec/lifecycle implementation rather than a migration of Vuoro itself.

> **Unverified:** the existence and status of an ACP v2 draft (reported as published
> 2026-07-20 with breaking redesigns) comes from the requester, not from a source this
> proposal checked. It does not change the recommendation — if anything, an in-flux next
> version is a stronger argument for isolation — but it should be confirmed before any v2
> work is scheduled.

## Acceptance test for this proposal

Not another OpenCode feature test:

```text
same sealed ActionQ execution
        ↓
   ACP runner
    ↙      ↘
OpenCode   Codex/Claude
    ↓        ↓
normalized Vuoro event stream
        ↓
same acceptance/reconciliation path
```

Passing that demonstrates the execution backend has become **fungible**, which is worth
considerably more than "Vuoro supports OpenCode".

## Boundary this preserves

```text
STRATEGIC / YOURS            STANDARDIZE / BORROW
─────────────────────        ──────────────────────────────
sprint/work semantics        agent session protocol → ACP
ActionQ lifecycle            agent tools/resources  → MCP
policy, envelopes            harness impl → OpenCode/Claude/Codex
evidence, audit              model API → OpenAI-compatible
acceptance, reconciliation   inference scheduling → llama-swap
identity/provenance          observability → OTel/Prom/Loki
```

Healthier than Vuoro becoming a handcrafted replacement for things the ecosystem has since
standardized.


---

# Addendum: the execution envelope has two halves

**Added 2026-08-19, from experiment 2.3** (`local-inference/docs/08-measurements.md`,
Findings 15 and 15b).

The envelope was previously discussed as roughly `task / repo / revision / permissions /
acceptance`. Measurement now says that is only half of it. Context allocation is not an
implementation detail of the harness — it changed accepted-task wall time by **4.7x** on a
task with plentiful but sparse evidence, at identical acceptance. It therefore belongs in
the execution contract.

```text
execution envelope
├── invariants                 machine-checked, never prompt text
│   ├── repo / root
│   ├── revision
│   ├── permitted paths
│   └── acceptance target
│
└── context policy             what the execution is entitled and expected to receive
    ├── HOT material           inlined
    ├── WARM addresses         addressable, retrieved on evidence of need
    ├── COLD addresses         bulky/reference, targeted retrieval only
    └── promotion policy       what may be promoted, and the ceiling on it
```

References, not payloads: WARM and COLD belong in the envelope as **addresses plus a
provider**, not as embedded content. Embedding them recreates arm A, which was the slow arm.

## Why invariants must be machine-checked, not prompted

Three failures in this repo were all the same shape — a silent gap between what config or a
prompt claimed and what actually happened:

1. Benchmark scenarios declared concurrency the server did not provide (`--parallel 1`).
2. A context-limit check never fired, so oversized prompts were sent and rejected for weeks.
3. **OpenCode resolved its project from the `PWD` environment variable**, so runs operated on
   the wrong repository entirely and reported plausible results for it. Preserved as
   `local-inference/benchmarks/evidence/2026-08-19-project-root-bug.md`.

None raised an error. The governing principle:

> **Make invalid states unrepresentable or loudly observable. Never rely on noticing that
> something silently did not happen.**

So `root`, `revision`, `permitted paths` and `acceptance target` are **verified around the
harness** — before dispatch and after completion — rather than stated inside its prompt and
hoped for. A harness that can be pointed at the wrong tree by an inherited environment
variable will not be saved by an instruction telling it not to be.

## How this divides with ACP

```text
ACP     transports the agent session
Vuoro   determines what context the execution is entitled and expected to receive,
        and verifies the invariants independently of what the agent reports
```

The context policy is Vuoro's to set and audit. ACP's `session/new` cwd/roots and MCP
attachments are the mechanism by which HOT/WARM/COLD addresses reach the agent — the
mechanism, never the authority.

## Testable consequence

`local-inference/benchmarks/regression/selective-retrieval.yaml` is the permanent fixture:
large available evidence, sparse relevant evidence, identical acceptance, and the assertion
that retrieval stays **selective**. If a future context provider starts eagerly injecting
everything, nothing errors — work simply gets several times slower. That is the class of
regression a context policy in the execution contract exists to prevent.


---

# Implementation status (2026-08-19)

Conformance was **observed before the adapter was written**, not read from the spec.
Evidence: `docs/evidence/2026-08-19-acp-v1-conformance.md`, raw trace alongside it.

## Landed

| Module | Role |
|---|---|
| `actionq_runner/execution_contract.py` | protocol-neutral vocabulary — envelope, invariants, context policy, normalized events, binding strength. Imports no ACP. |
| `actionq_runner/acp/jsonrpc.py` | line-delimited JSON-RPC over child stdio. Transport only. |
| `actionq_runner/acp/v1.py` | `AcpExecutionAdapter` — the v1 codec and lifecycle. |
| `actionq_runner/acp/telemetry.py` | `session/update` → normalized events. |
| `tests/fake_acp_agent.py` | scriptable agent, including dishonest behaviours. |
| `tests/test_acp_adapter.py` | 19 tests, no harness required. |
| `tests/test_acp_conformance_live.py` | 5 tests against the real harness, `ACP_LIVE=1`. |

The version boundary the proposal asked for holds: ACP vocabulary appears only under
`acp/`, and a v2 codec would be a sibling of `v1.py`.

## What the evidence changed about the design

**Model identity is now part of the envelope, and it is mandatory.** A `session/new` with
no model specified binds a *hosted* model (`opencode-go/kimi-k2.7-code`) and reports
nothing. An execution would have run on the wrong backend, billed for it, and returned
plausible output. `ExecutionEnvelope` rejects an empty model at construction, so that state
is unrepresentable rather than merely discouraged.

**Binding strength is explicit.** OpenCode 1.18.18 has no method that reports a session's
current model — `session/status`, `session/info` and `session/active` appear in the binary
but return `-32601`. So the confirmation step cannot complete in-protocol against the
primary target. Rather than assert the binding as verified, the adapter records
`VERIFIED` / `ASSERTED` / `UNSUPPORTED`, emits it as telemetry, and attaches it to the
outcome. `require_verified_model=True` makes the weak case fatal.

**Permissions fail closed.** With no resolver wired in, and when a resolver raises, the
answer is deny. ACP supplies mechanism; an adapter with no authority grants nothing.
`ESCALATE` has no ACP representation and is reported as a cancelled request, resolved above
this layer — escalation is a policy outcome, not a harness feature.

**Unmapped traffic is visible.** Anything the codec does not recognise becomes
`protocol.unmapped` rather than being dropped, so an agent that starts sending something
new is observable. The live test asserts the real harness produces none.

**`PWD` is set explicitly at spawn.** OpenCode resolves its project from `PWD`, not the
process cwd.

## Not done

- **Tool-call and permission flows are unobserved against a real harness.** The live probe
  used `plan` mode and a trivial prompt to isolate protocol from agent behaviour. The fake
  agent covers the adapter's handling; the wire shapes for `tool_call`, `tool_call_update`
  and `session/request_permission` are assumed from the spec, not confirmed.
- **The acceptance test is not built.** Only the OpenCode leg exists. Fungibility remains
  a claim, not a measurement, until a second agent runs the same sealed envelope.
- **The context policy is modelled but not enforced.** (Envelope *invariants* are now
  enforced — see below. Context policy is the remaining half.) `ContextPolicy` is carried on the
  envelope; nothing yet resolves WARM/COLD addresses or holds the HOT ceiling. That is the
  seam where `local-inference/benchmarks/regression/selective-retrieval.yaml` should be
  pointed once a provider exists.
- **`session/load` / resume is unused.** Advertised as available; recovery policy is not
  implemented.
- **ACP v2 remains unverified.** Unchanged by this work.

## Measured, not tuned

An early reading of a **7 771-token harness preamble** appeared here. It was wrong and has
been retracted — see evidence A8. The figure tracks prefix-cache state, not request size.


---

# Envelope enforcement (2026-08-19, second slice)

`actionq_runner/execution_boundary.py` — protocol-neutral, imports no ACP. The checks run
*outside* whatever agent is executing, because a harness cannot be trusted to report that
it was pointed at the wrong tree.

## Before dispatch — while nothing is running

| Check | Catches |
|---|---|
| `root.exists` | an envelope naming a tree that is not there |
| `root.is_worktree` | a root that is not version-controlled, so nothing is attributable |
| `root.is_worktree_top` | a *subdirectory* root, which would silently make the path allowlist relative to the wrong base |
| `revision.pinned` / `revision.recorded` | a tree that is not on the revision the envelope froze |

A pinned revision must match. The literal `HEAD` means "unpinned — resolve it once and hold
the execution to that", so the post-check compares against what the tree actually was
rather than against the word `HEAD`.

## After completion — what the execution actually did

| Check | Catches |
|---|---|
| `revision.unchanged` | a harness that committed or moved `HEAD` |
| `paths.permitted` | changes outside the allowlist, diffed against a pre-dispatch baseline so pre-existing dirt is not blamed on the execution |
| `acceptance.target_present` | an acceptance target that was never produced |

An **empty `permitted_paths` means no modification was authorised** and any change is a
violation. That is the fail-closed reading and it matches the frozen allowlist the portable
runner already enforces — whose `_changed_paths` this module reuses rather than
reimplementing.

## In-protocol — the one piece of execution identity ACP can read back

`session/list` reports each session's `cwd`. The adapter compares it to the envelope root
(`session.root_readback`). It is the agent's self-report, labelled as such, and an agent
without `session/list` is recorded as **unverifiable rather than passing** — the distinction
that keeps a missing check from looking like a satisfied one.

Measured: an explicit `session/new` cwd **beats a hostile inherited `PWD`**, so the ACP path
does not reproduce the `opencode run` project-root bug (evidence A7). The adapter sets `PWD`
anyway, and the read-back is the regression guard.

## Failure mode

Violations raise `BoundaryViolation` by default. `enforce_invariants=False` downgrades to
observe-and-record — the report still lands in `boundary_reports` and in the
`boundary.checked` event stream, so a violation is never invisible, only non-fatal.

## Tests

11 boundary tests, 11 adapter-level enforcement tests, 3 live. Each targets a way an
execution can succeed while having operated on something other than what the envelope
named: wrong revision, missing root, subdirectory root, harness that commits, changes
outside the lane, pre-existing dirt misattributed, and an agent reporting the wrong tree.

## Still not done

- Context policy enforcement (WARM/COLD resolution, HOT ceiling). The regression fixture
  `local-inference/benchmarks/regression/selective-retrieval.yaml` should be pointed here
  once a provider exists.
- Real permission and tool-call wire shapes remain unverified against a live harness.
- The two-agent fungibility acceptance test.


---

# Context enforcement (2026-08-19, third slice)

`actionq_runner/context_policy.py` — protocol-neutral.

## The invariant is not a token maximum

`assert prompt_tokens <= 12288` would have turned a measured parameter into the
architecture. What is enforced instead:

> **Bulky context outside HOT must remain addressable rather than silently becoming
> model-visible content.**

That survives HOT being re-derived at 10 240 or 16 384. `hot_budget` is a field with the
measured default (12 288), and a test asserts enforcement responds to changing it rather
than to a constant in the code.

## Two jobs, separately

**Before dispatch** — deterministic, against content we hold:

| Check | Catches |
|---|---|
| `context.tiers_remain_addressable` | a WARM/COLD entry whose "address" is a wall of content — a payload wearing a reference's clothes, which recreates the 4.7x-slower arm |
| `context.hot_within_budget` | HOT material over its allocation |
| `context.promotion_bounded` | a policy that forbids promotion while carrying a promotion budget |

**During execution** — `account_promotion` records what retrieval brought into view and
checks it against `promotion_allowed` / `promotion_budget`, so promotion is *recorded*
rather than merely permitted.

## Token counting has provenance

The count must come from the execution's own tokenizer, so `TokenCounter` is injected;
`llama_cpp_counter` supplies a real one. When none is given, a crude ~4-chars-per-token
fallback is used and the resulting assurance is **ASSERTED, not VERIFIED**, with the reason
stated on the check. An approximate count is never reported as a measured one.

## Harness overhead is accounted separately — and is currently unmeasurable

`ContextUsage` splits `hot_tokens` / `promoted_tokens` (what policy controls) from
`harness_tokens` (what the backend adds on its own account), because conflating them makes
the budget ambiguous: total request length and task-controlled allocation are different
quantities.

`harness_tokens` carries its own assurance, and on the observed harness it is
**UNVERIFIABLE**. ACP's `usage.inputTokens` reports prefix-cache state, not request size —
516, then 4, 4, 4 for an identical request against a steady server-side 22 (evidence A8).
So the intended preamble-growth regression guard **cannot be built on ACP usage**. What
exists instead is a live test asserting the figure is *unstable*; if a release makes it
stable, that test fails and the accounting can be revisited.

## Per-property assurance

The ASSERTED/VERIFIED distinction generalised. `adapter.assurance()` returns:

```text
model           ASSERTED       no read-back exists (A2b)
root            VERIFIED       session/list reports cwd (A7)
revision        VERIFIED       checked outside the harness
changed_paths   VERIFIED       diffed against a pre-dispatch baseline
acceptance      VERIFIED       target presence checked
context_hot     VERIFIED       with a real tokenizer; ASSERTED with the fallback
harness_usage   UNVERIFIABLE   the backend's figure does not measure what it appears to
```

Not one "trusted ACP session" flag. A backend attests to different properties with
different strength, and collapsing them reports the weakest as strong. The rule:

> Declare what must be true → ask the backend what it can attest → verify everything it
> exposes → explicitly label the rest unverifiable.

Stronger than fail-closed alone, because it stops absence of evidence from quietly becoming
runtime trust.

## Tests

14 context-policy, 6 adapter-level, 3 live (61 offline + 11 live on this branch).

## Still not done

- **Real permission and tool-call wire shapes.** Still the only part of the adapter based
  primarily on the fake agent rather than observed traffic, and therefore the next slice —
  deliberately inducing Read, Edit, a shell command, and both permission outcomes, then
  turning each observed shape into a fixture.
- **The two-agent fungibility test**, which is worth doing only after the above; otherwise
  two agents pass through an adapter whose less-trivial handling is still hypothetical.
- **A retrieval provider.** `ContextPolicy` carries addresses; nothing resolves them yet.
  `local-inference/benchmarks/regression/selective-retrieval.yaml` points here once one
  exists.


---

# Observed wire shapes (2026-08-19, fourth slice)

Closes the last part of the adapter that rested on the fake agent rather than observed
traffic. Six scenarios induced against OpenCode 1.18.18 with permissions forced to `ask`;
raw stdio traces preserved in `docs/evidence/opencode-1.18.18/`. Full detail in evidence A9.

## Two fixture layers, kept apart

```text
docs/evidence/opencode-1.18.18/*.raw.jsonl   what did OpenCode actually do?
tests/test_acp_wire_shapes.py                what contract does ActionQ depend on?
```

The raw traces are never asserted field-by-field. The tests replay them through the real
normalizer and assert *semantics*: identity retained across updates, lifecycle internally
coherent, completion observable, nothing unmapped. ACP leaves most update fields optional,
so a second agent may legitimately send far less — asserting OpenCode's exact transcript
would turn a protocol adapter into an OpenCode-1.18.18 transcript parser.

## What observation changed

- **Identity is `toolCallId` alone.** `title` was observed to mutate mid-lifecycle
  (`"read"` → `"data.txt"` for one call), so correlating on it would mis-attribute work.
- **`in_progress` repeats**; a one-transition-per-status assumption would be wrong.
- **A fourth kind, `search`, appeared unbidden** — the kind set is recorded, not enumerated.
- **`optionId` is harness-private** (`once`/`always`/`reject`); `kind` is the portable
  field. The adapter already selected by `kind`, now confirmed rather than assumed.
  `reject_always` was not offered, so decisions degrade across the kinds present.
- **A rejected tool ends `failed` while the turn ends `end_turn`.** A policy denial is a
  tool-level outcome, not a turn-level abort.
- **Cancellation** yields `stopReason: "cancelled"`; the adapter now answers every
  outstanding permission request with `cancelled` when it cancels, per the v1 contract.

## No fuzzy correlation around authorization

A `session/request_permission` naming a `toolCallId` the adapter never saw announced is a
**protocol fault, answered with denial** — never attached to "probably the most recent
tool". The resolver is not even consulted. Three further malformed cases are refused the
same way rather than absorbed: an update with no `toolCallId`, an update for an unannounced
call, and an update after a terminal status. Each is recorded in `protocol_faults` and
surfaced as telemetry, because accepting incoherent lifecycle state is how false authority
gets manufactured.

## Tests

25 replay tests over the captured evidence, 6 malformed-lifecycle tests (92 offline, 11
live on this branch).

## Remaining

**Second-agent fungibility** — partially evidenced, see below. Per-agent differences should
be made *visible* rather than normalized away — fungibility means Vuoro executes correctly
across both backends, not that both pretend to identical capability. The assurance map
already expresses exactly that.


---

# Second-agent status (2026-08-19, fifth slice — partial)

Three shipping ACP v1 implementations probed; `initialize` responses preserved in
`docs/evidence/acp-agents/`. Full detail in evidence A10.

## Evidenced: the codec is protocol-shaped

All three negotiate `protocolVersion: 1` and **one codec parses all three with no
per-vendor branch**. Their capabilities genuinely diverge — no two of the three agree on
session capabilities; only OpenCode advertises `close`; Gemini advertises none at all and
adds `audio` prompt support.

This makes per-property assurance concrete rather than theoretical. `session/list` carries
root verification (A7), so the *same adapter* yields `root: VERIFIED` on OpenCode and
`root: UNVERIFIABLE` on Gemini. A single "trusted session" flag would have to pick one and
be wrong about the other. Differences stay visible, which is what fungibility should mean:
Vuoro executes correctly across backends, not that backends pretend to be alike.

13 divergence tests run against the captured responses.

## Not evidenced: execution parity

Neither second agent could be driven past `session/new`, for environmental reasons rather
than protocol ones:

- **Claude Code ACP 0.16.2** refuses to run nested inside a live Claude Code session —
  *"Nested sessions share runtime resources and will crash all active sessions."*
  Bypassable by unsetting `CLAUDECODE`; deliberately not done underneath a running session.
- **Gemini CLI 0.55.1** has no credential configured.

**The two-harness acceptance test is therefore unrun.** Capability parity is not execution
parity, and the fungibility claim in this proposal is not yet demonstrated.

To finish it, one of:

1. Run the Claude Code leg from a shell with no active Claude Code session (`CLAUDECODE`
   unset), where the nesting guard is satisfied honestly rather than bypassed.
2. Configure a Gemini credential.
3. Install a third-party ACP agent with no such constraint.

The adapter needs no change for any of these; the work is one capture run plus the
comparison assertions.
