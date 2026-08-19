# Evidence: ACP v1 conformance of `opencode acp`

**Date:** 2026-08-19 · **Agent:** OpenCode 1.18.18 · **Transport:** JSON-RPC over stdio
**Raw trace:** `2026-08-19-acp-conformance-trace.jsonl`
**Probes:** `docs/plans/acp-execution-adapter.md` (Phase 3)

Observed by driving the agent directly, not read from the spec. Every claim below has a
line in the trace or a server-side log entry behind it.

## A1 — protocol version and capabilities are as the proposal assumed

`initialize` with `protocolVersion: 1` returns:

```json
{"protocolVersion":1,
 "agentInfo":{"name":"OpenCode","version":"1.18.18"},
 "agentCapabilities":{
   "loadSession":true,
   "mcpCapabilities":{"http":true,"sse":true},
   "promptCapabilities":{"embeddedContext":true,"image":true},
   "sessionCapabilities":{"close":{},"fork":{},"list":{},"resume":{}}},
 "authMethods":[{"id":"opencode-login","name":"Login with opencode",
                 "description":"Run `opencode auth login` in the terminal"}]}
```

v1 is confirmed as the deployed version. `session/load` is advertised as available
(`loadSession: true`), which the proposal had marked as capability-gated.

## A2 — the deployed method set is larger than the proposal recorded

The proposal listed `initialize`, `authenticate`, `session/{new,prompt,cancel,load}`,
`session/request_permission`, `session/update`. Extracted from the shipped binary:

```
session/active   session/cancel   session/close    session/fork
session/info     session/list     session/load     session/message
session/new      session/part     session/prompt   session/request_permission
session/resume   session/set_config_option         session/set_mode
session/set_model                 session/status   session/update
```

Treat this as OpenCode's surface, not as ACP v1's. Which of these are core v1 versus
OpenCode extensions is **not** established by this probe, and the adapter must not assume
any of the extras exist on another agent. `session/{new,prompt,set_mode,set_model,list}` were
each driven successfully; the rest are named-in-binary only.

## A2b — three of those names are not methods (correction to A2)

Driving them refutes the binary-strings inventory:

```
session/status  -> -32601 Method not found
session/info    -> -32601 Method not found
session/active  -> -32601 Method not found
session/list    -> ok  {"sessions":[{sessionId, cwd, title, updatedAt}, ...]}
```

So a string in the binary is not evidence of a method. The inventory in A2 is a list of
candidates to probe, nothing more, and this document's own A2 overstated it until probed.

The consequence is substantive: **`session/list` returns no model, and no other method
reports a session's current model. There is no way to read the bound model back over ACP
from OpenCode 1.18.18.**

That means the verification A4 demands cannot be completed in-protocol against the primary
conformance target. The binding is therefore recorded with an explicit strength rather than
asserted as fact:

| status | meaning |
|---|---|
| `VERIFIED` | the agent reported its bound model and it matched |
| `ASSERTED` | the agent accepted the binding and offers the model, but exposes no read-back |
| `UNSUPPORTED` | the agent cannot bind a model at all — always fatal |

OpenCode yields `ASSERTED`. The adapter emits a `model.binding` telemetry event carrying the
status and attaches it to the execution outcome, so a weak binding is visible in audit
rather than indistinguishable from a strong one. `require_verified_model=True` makes the
weak case fatal for callers that need proof of the backend; against this harness that
setting refuses every execution, which is the honest answer.

Out-of-band confirmation remains available and is what A5 used: observe the inference
endpoint. That works for local models and is outside ACP's reach.

## A3 — unknown methods fail loudly

```json
{"id":3,"error":{"code":-32601,
  "message":"\"Method not found\": session/definitely_not_a_method",
  "data":{"method":"session/definitely_not_a_method"}}}
```

Standard JSON-RPC `-32601`. Capability gaps are therefore observable rather than silent,
which is the precondition for probing a second agent's surface at `open()` time instead of
configuring it by hand.

## A4 — **the session model defaults to a hosted model, and nothing says so**

This is the finding that changes the adapter's design.

`session/new` returns a `configOptions` array alongside the session id:

```
id=model  type=select  currentValue=opencode-go/kimi-k2.7-code  (84 options)
id=mode   type=select  currentValue=build                       (2 options: build, plan)
```

Three providers are visible — `opencode`, `opencode-go`, `local3090` — with
`local3090/worker-fast` and `local3090/devstral` among them. But a session created with no
model specified runs on **`opencode-go/kimi-k2.7-code`**, a hosted model.

Nothing in the ACP handshake surfaces this as a decision. An ActionQ execution dispatched
over ACP without an explicit model would run on a hosted backend, bill accordingly, and
return plausible results — with no error at any layer. That is precisely the failure class
the governing principle names:

> Make invalid states unrepresentable or loudly observable. Never rely on noticing that
> something silently did not happen.

It is the same shape as the `PWD` project-root bug
(`local-inference/benchmarks/evidence/2026-08-19-project-root-bug.md`): the harness resolves
something important from its own ambient defaults, and reports success either way.

**Consequence for the adapter:** model identity is part of the execution envelope and must
be *set and then verified*, never defaulted. `open()` fails closed when the envelope names
no model — `ExecutionEnvelope` rejects an empty model at construction, so the invalid state
is unrepresentable — and confirms the bound model after setting it rather than trusting the
`ok` reply. See A2b for how far that confirmation can actually get against this harness.

## A5 — the model binding was verified at the server, not from the reply

`session/set_model` → `local3090/worker-fast` returned `ok`. That reply is not evidence.
Independent confirmation from llama-swap:

```
POST /v1/chat/completions 200 "opencode/1.18.18 ai-sdk/... bun/1.3.14"   638.7ms
POST /v1/chat/completions 200 "opencode/1.18.18 ai-sdk/... bun/1.3.14"  3074.3ms
```

Two observations:

1. The request genuinely reached the local endpoint — the binding took effect.
2. **One prompt produced two completions.** OpenCode issues at least one side-effect model
   call per turn beyond the prompt itself. Any per-execution cost or token accounting that
   assumes one call per prompt will undercount. Not investigated further here.

## A6 — a complete prompt turn, end to end

`session/prompt` with a trivial text prompt, mode `plan` (edit tools disallowed), in a
throwaway git repo:

```json
{"id":5,"result":{"stopReason":"end_turn",
 "usage":{"inputTokens":7771,"outputTokens":2,"totalTokens":7773}}}
```

Notifications received during the turn:

```
session/update : agent_message_chunk   x1
session/update : usage_update          x1
```

`stopReason` and a usage block come back on the response; streaming arrives as
`session/update` notifications discriminated by a `sessionUpdate` field. This matches the
shape the proposal's telemetry-normalization table assumed.

`stopReason` and a usage block come back on the response. **The 7 771 figure in that usage
block is not what it looks like — see A8, which retracts the reading originally given
here.**

## A7 — the effective root *is* readable back, and a hostile PWD does not move it

Added while implementing envelope enforcement.

`session/list` returns each session's `cwd`:

```json
{"sessions":[{"sessionId":"ses_...","cwd":"/…/sandbox","title":"…","updatedAt":"…"}]}
```

So unlike the model (A2b), the effective root **can** be read back over ACP. It is the
agent's self-report rather than an independent observation, and is labelled as such — but a
self-report that disagrees with the envelope is still decisive, and the check costs one
round trip.

Tested directly against the original bug: with `--cwd` set to the sandbox and a **hostile
`PWD`** pointing at `/projects/dev/actionq`, the session still reported the sandbox.

```
requested cwd : /…/scratchpad/acp/sandbox
hostile PWD   : /projects/dev/actionq
reported cwd  : /…/scratchpad/acp/sandbox      MATCH
```

**The ACP path does not reproduce the `opencode run` project-root bug.** An explicit
`session/new` cwd wins. The adapter still sets `PWD` explicitly at spawn — belt and braces,
since that costs nothing — and the root read-back is what would notice if this regressed. A
live test pins the behaviour.

## A8 — ACP `usage.inputTokens` is not a measure of context size (retracts A6)

A6 originally read `inputTokens: 7771` on a two-token answer as OpenCode's fixed system
prompt and tool definitions — "fixed overhead on every ACP execution", consuming most of
the HOT tier. That reading was wrong, and it was wrong in the way this repository keeps
finding things wrong: a single plausible observation, generalised without a second look.

Measuring it properly, by varying task length and fitting the intercept:

```
task_tokens=    10   input_tokens=7771   implied overhead=7761
task_tokens=   211   input_tokens= 530   implied overhead= 319
task_tokens=   811   input_tokens=1116   implied overhead= 305
```

The implied overhead is not fixed. Repeating the *identical* short prompt four times:

```
run 1  inputTokens = 516
run 2  inputTokens =   4
run 3  inputTokens =   4
run 4  inputTokens =   4
```

Ground truth from the inference server for the same content, taken directly:

```
prompt_tokens=22   cached_tokens= 0
prompt_tokens=22   cached_tokens=18
prompt_tokens=22   cached_tokens=18
```

The server's accounting is stable at 22. OpenCode's ACP figure moves 516 → 4 → 4 → 4 for
an unchanged request, tracking **prefix-cache state**, not request size.

**Consequences.**

1. There is no evidence of a ~7 K harness preamble. The A6 claim is retracted, and nothing
   should be tuned against it.
2. `usage.inputTokens` cannot be used for budget accounting, cost attribution, or a
   preamble-growth regression guard. The adapter emits it as `usage_reported` alongside an
   explicit `usage_assurance: unverifiable`, and budget decisions use the pre-dispatch
   count, which we compute ourselves from content we hold.
3. A live test asserts the figure is *unstable* across identical requests. If a future
   release makes it stable, that test fails and the accounting can be revisited.

The general lesson is the same one A2b taught about `session/status`: a number a harness
reports is a claim, and a claim that has not been checked against an independent
observation is not a measurement.

## A9 — real tool-call and permission wire shapes (closes the A6 gap)

Induced against OpenCode 1.18.18 with a project-level `opencode.json` setting
`{"edit":"ask","bash":"ask"}`, so permission requests were forced rather than hoped for.
Raw stdio traces preserved verbatim in `opencode-1.18.18/`.

### Tool-call lifecycle

| capture | kinds | statuses | permission | stopReason |
|---|---|---|---|---|
| `read` | read | pending → in_progress → completed | none | `end_turn` |
| `execute` | execute | pending → in_progress ×3 → completed | allow | `end_turn` |
| `permission-allow` | read, edit | pending → in_progress → completed | allow | `end_turn` |
| `permission-reject` | search, read, edit | edit: pending → in_progress → **failed** | reject | `end_turn` |
| `permission-cancel` | execute | pending → in_progress → **failed** | cancelled | **`cancelled`** |

Observations that changed the adapter:

1. **`title` mutates mid-lifecycle.** The same call is announced as `"read"` and later
   reported as `"data.txt"`. Correlating on it would mis-attribute work, so identity is
   `toolCallId` and nothing else.
2. **`in_progress` repeats.** Any state machine assuming one transition per status is
   wrong.
3. **A fourth kind, `search`, appeared unbidden.** The kind set is not fixed and is
   recorded rather than enumerated.
4. **A rejected tool ends `failed` while the turn still ends `end_turn`.** A policy denial
   is a tool-level outcome, not a turn-level abort — which is how ActionQ must interpret
   it.

### Permission request

```json
{"sessionId":"ses_…",
 "toolCall":{"toolCallId":"55Da1uyr…","title":"…/data.txt","kind":"edit",
             "status":"pending","locations":[…],"rawInput":{…}},
 "options":[{"optionId":"once","kind":"allow_once","name":"Allow once"},
            {"optionId":"always","kind":"allow_always","name":"Always allow"},
            {"optionId":"reject","kind":"reject_once","name":"Reject"}]}
```

**`optionId` values are harness-private strings** (`once`, `always`, `reject`) and carry no
portable meaning. The portable field is `kind`. The adapter already selected by `kind` and
returned the paired `optionId`; this confirms that was right rather than lucky.
`reject_always` was **not offered** — the option set is partial, so a policy decision must
degrade across the kinds actually present.

### Cancellation

Cancelling mid-turn with a permission request outstanding yields `stopReason: "cancelled"`,
and the affected tool resolves to `failed` rather than dangling. The adapter now answers
every outstanding permission request with `cancelled` when it cancels, per the v1 contract;
leaving them unanswered strands the agent.

## A10 — three ACP v1 implementations, and they diverge

Captured `initialize` responses in `acp-agents/`:

| agent | version | sessionCapabilities | promptCapabilities | loadSession |
|---|---|---|---|---|
| OpenCode | 1.18.18 | `close`, `fork`, `list`, `resume` | image, embeddedContext | yes |
| Claude Code ACP (`@zed-industries/claude-code-acp`) | 0.16.2 | `fork`, `list`, `resume` — **no `close`** | image, embeddedContext | yes |
| Gemini CLI (`--experimental-acp`) | 0.55.1 | **none advertised** | image, **audio**, embeddedContext | yes |

All three negotiate `protocolVersion: 1`, and **one codec parses all three with no
per-vendor branch** — the adapter is protocol-shaped rather than OpenCode-shaped. No two of
the three agree on session capabilities.

This vindicates per-property assurance concretely rather than theoretically. `session/list`
is what carries the root verification (A7); Gemini does not advertise session capabilities
at all, so the *same adapter* would yield `root: VERIFIED` on OpenCode and
`root: UNVERIFIABLE` on Gemini. A single "trusted ACP session" flag would have had to pick
one of those and be wrong about the other.

### Execution legs are blocked, and not by the protocol

Neither second agent could be driven past `session/new`:

- **Claude Code ACP** refuses to launch inside an existing Claude Code session:
  *"Nested sessions share runtime resources and will crash all active sessions."* The guard
  is bypassable by unsetting `CLAUDECODE`, which is **not** something to do underneath a
  live session on the strength of a benchmark.
- **Gemini CLI** returns `-32000 "Gemini API key is missing or not configured."` No
  credential is present, and this repository does not acquire one on its own initiative.

So the capability half of fungibility is evidenced; the **execution half is not**, and the
two-harness acceptance test remains unrun. The obstacles are environmental — a process
guard and a missing credential — not protocol incompatibility, but an unrun test is unrun
and is recorded as such.

## A11 — two backends, one sealed envelope, one acceptance path

The acceptance test the proposal asked for. Codex ACP 1.4.0 supplied the second leg; its
existing `~/.codex/auth.json` worked without modification, so authentication was an
observed property rather than a prerequisite that had to be manufactured.

Same `ExecutionEnvelope` (task, invariants, permitted paths, acceptance target, context
policy), same adapter, invariant enforcement **on**, differing only in the runtime binding
each backend requires:

| | OpenCode 1.18.18 | Codex ACP 1.4.0 |
|---|---|---|
| model | `local3090/worker-fast` | `gpt-5.6-sol[low]` |
| mode | `build` | `agent` |
| stopReason | `end_turn` | `end_turn` |
| tools | edit | edit, execute |
| protocol faults | none | none |
| **acceptance** | **passed** | **passed** |
| artifact | `OK` | `OK` |

Assurance, side by side:

```text
                opencode        codex
model           asserted        asserted
revision        verified        verified
root            VERIFIED        UNVERIFIABLE
context_tiers   verified        verified
context_hot     asserted        asserted
harness_usage   unverifiable    unverifiable
```

**Fungibility is not parity.** The backends reached the same acceptance outcome while
attesting differently, and the difference survives into the outcome instead of being
averaged away. Codex cannot attest its working root — its `session/list` covers persisted
threads, not the live session — and that lowers assurance without failing the execution.

### Two corrections this run forced

1. **Model advertisement has no single shape.** Codex exposes *both* a `configOptions`
   select carrying base ids and a `models.availableModels` block carrying the
   `id[effort]` forms that `set_model` actually validates. Picking one as authoritative
   rejected a valid model; the codec now takes the union of what the agent advertises.
2. **Absence of evidence is not evidence of disagreement.** A session missing from the
   agent's listing first surfaced as a *failed* root check. It is not: nothing was
   contradicted. It now yields `UNVERIFIABLE` and passes, while a listing that reports a
   *different* root still fails. Punishing a backend for a capability gap would have made
   the assurance map a compatibility gate.

The rule this settles:

> **Capability absence changes assurance. It does not fail the run, and it does not select
> a vendor-specific implementation path.**

One codec drives OpenCode, Codex, Claude Code ACP and Gemini CLI with no vendor branch.

## What this does not establish

- ~~Tool-call and permission flows were not exercised.~~ **Closed by A9.**
- ~~The two-harness acceptance test has not been run.~~ **Closed by A11**, on OpenCode and
  Codex. The Claude Code and Gemini legs remain unexecuted (nesting guard, missing
  credential) and are not needed for the claim.
- The sealed task was **small** — create a one-word file. It exercises the full envelope,
  acceptance and assurance path, not a demanding workload.
- The **harness's true context overhead is unmeasured.** A8 establishes only that ACP's
  reported figure cannot measure it. Determining it would need server-side observation of
  the actual request, which is available for local models and not for hosted ones.
- ACP v2's status remains **unverified**, exactly as the proposal flagged. Nothing here
  bears on it.

---

# Phase 4 addendum — Claude Code ACP, measured

Captured 2026-08-19 from a plain shell (the nesting guard in A10 makes this impossible from
inside a Claude Code session). Harness: `verification/capture_claude_acp.py` and
`verification/probe_claude_acp_set_model.py`. Raw:
`acp-agents/claude-code-acp-0.16.2.session-new.json` and `…set-model-probe.json`.

## A12 — `session/new` exposes both a model roster and a mode vocabulary

`@zed-industries/claude-code-acp@0.16.2` returns `models.availableModels` **and**
`models.currentModelId` from `session/new`, so the existing `_model_options` reader finds
them with no new code. Three ids, and they are **selectors, not model names**:

| modelId | described as |
|---|---|
| `default` | Opus 4.6 · Most capable for complex work |
| `sonnet` | Sonnet 4.5 · Best for everyday tasks |
| `haiku` | Haiku 4.5 · Fastest for quick answers |

`currentModelId` was `sonnet` at session creation — i.e. **a session begins bound to a model
nobody chose**, which is A4 restated for this agent.

Modes: `default`, `acceptEdits`, `plan`, `dontAsk`, `bypassPermissions`. Three of the five
weaken permissions. A bogus modeId returns `-32603 Internal error` rather than a clean
rejection, so mode validation exists but is not well-formed.

## A13 — `session/set_model` returns success for *any* string, and cannot be read back

This is the finding that governs whether Claude Code ACP can be wired at all.

`session/set_model` exists — so the adapter's `method_not_found` fail-closed path never
fires. It answered `{"result":{}}` to every one of these:

| sent | in advertised list? | result |
|---|---|---|
| `sonnet` | yes | accepted |
| `haiku` | yes | accepted |
| `claude-sonnet-4-5` | no | **accepted** |
| `gpt-5.6-sol[low]` | no (another vendor) | **accepted** |
| `local3090/worker-fast` | no (a local profile) | **accepted** |
| `definitely-not-a-model-9999` | no | **accepted** |
| `""` (empty string) | no | **accepted** |

And there is **no read-back**: `session/status` and `session/info` are both `-32601`.

> **A successful `set_model` response from this agent is not evidence that anything was
> bound.** It is not evidence that the id was even understood.

This is worse than the missing `set_model` the plan anticipated. A missing method fails
closed and loudly; this fails open and cheerfully, on a hosted model that bills. It is the
same shape as the six in `local-inference` HANDOFF §3: the operation reports success, the
result is plausible, and nothing at any layer says otherwise.

## A14 — why: the ACP layer is a bridge that neither owns nor checks model selection

Read from the published package rather than inferred.

`claude-code-acp` is a thin translation shim over `@anthropic-ai/claude-agent-sdk`, which it
**pins at 0.2.44**. Two consequences follow, and they answer two separate questions:

1. **No validation.** `unstable_setSessionModel` (`dist/acp-agent.js:483`) checks only that
   the session exists, then forwards the raw string to `query.setModel(params.modelId)`.
   The bridge does not validate because selecting models is not its job — but the ACP client
   is told `ok` regardless. Note the method's own name: `unstable_`.
2. **The roster is as old as the pinned SDK.** `availableModels` is built
   (`dist/acp-agent.js:920-938`) from the model list the bundled Claude Code CLI reports at
   initialization, and those strings live in that SDK version's `cli.js`. 0.16.2 is the
   **latest** published bridge, so this is not a stale pin on our side — the current bridge
   ships a roster that predates the Claude 5 family. "The harness handles the model side" is
   exactly right, and is precisely why the ACP surface cannot answer for it.

A third detail, worth naming because it is a trap for anyone tempted to pass a newer id: the
settings-to-model matcher (`dist/acp-agent.js:924-928`) matches **substrings in both
directions** — `m.value.includes(settings.model) || settings.model.includes(m.value)`. A
configured `sonnet-5` therefore matches the `sonnet` entry and silently resolves to
**Sonnet 4.5**. Asking for a newer model by a longer name is a way to get an older one with
no error.

## A15 — `session/list` is machine-global, not connection-scoped

The listing returned **50 sessions with a `nextCursor`, spanning 12 unrelated working
directories**, including sessions belonging to other projects entirely and the very Claude
Code session that was running at capture time. Entries carry `sessionId`, `cwd`, `updatedAt`
and a `title` taken from the session's opening text.

Two consequences:

- It is **not** a usable root-verification channel (A11 correction 2 assumed a narrower
  scope). A session appearing in the list says nothing about the connection asking.
- It is an **information-exposure surface**: any ACP client that can reach this agent can
  enumerate the titles and working directories of unrelated local Claude Code sessions.

## A16 — what this means for wiring (SUPERSEDED by A17, kept for the record)

The original A16 concluded: *"do not wire this as a routine execution backend — it solves no
class of work `worker-fast` does not, it costs money, and it cannot confirm what it ran."*

**Two of those three premises were wrong, and the third was too narrow.** Recorded rather than
quietly edited, because the mistake is instructive.

- *"Costs money"* assumed API pricing. Under a subscription the marginal cost of an execution
  is ~zero. The scarce resource is not dollars, it is a bounded quantity of high-quality
  reasoning.
- *"Solves no class of work `worker-fast` does not"* misapplied the admission rule from
  `local-inference`. That rule governs **which serving lane owns one 24 GB GPU**; it is sound
  inside the serving tier and meaningless across tiers. `worker-fast` exists precisely so
  frontier capacity is not spent on "inspect these seven files, make this bounded change,
  return evidence". It was never a candidate to replace a frontier planner.
- *"Cannot confirm what it ran"* was true of `session/set_model` (A13) and **false of the
  agent as a whole** — see A17. Disqualifying one channel is not disqualifying the backend.

The general error is worth naming: **an execution lane was being judged only as a queue
consumer.** A frontier subscription's highest-value use is supervising, planning and
synthesising over work it did not personally perform. A model that scores a lane solely on
bounded-execution merit cannot see that, and will keep producing conclusions like the one
above.

## A17 — Claude Code *can* be bound verifiably, just not through `set_model`

`settings.model` is read from `<cwd>/.claude/settings.json`, and `cwd` is already something
`ExecutionEnvelope` controls — so per-execution binding needs no new transport.
`getAvailableModels` resolves it against the roster, calls `query.setModel` with the
**resolved** value, and `session/new` returns `models.currentModelId` (`dist/acp-agent.js:940`
returns `currentModel.value`, the resolved entry, not an echo of the request).

Measured (`acp-agents/claude-code-acp-0.16.2.settings-binding-probe.json`):

| `settings.model` | `currentModelId` | advertised list became | reading |
|---|---|---|---|
| *(absent)* | `sonnet` | 3 entries | inherits the user's global settings — **not** a deterministic default |
| `sonnet` / `haiku` / `default` | tracks exactly | 3 entries | binding works, verifiable before any prompt |
| `sonnet-5` | **`sonnet`** | 4 entries (incl. `sonnet-5`) | substring match wins on the *earlier* entry |
| `totally-unknown-model` | *echoes back* | 4 entries (incl. it) | **adopted, not rejected** |

Two traps, and they need different defences — which is why `verify_bound_model` checks two
things and not one:

1. **The substring downgrade.** The matcher tests `m.value.includes(settings.model) ||
   settings.model.includes(m.value)` and takes the first hit, so `sonnet-5` binds `sonnet`
   (Sonnet 4.5) *even though an exact `sonnet-5` entry exists further down the list*. Asking
   for a newer model by a longer name is a way to get an older one. Caught only by
   **equality** between requested and reported.
2. **The adopted unknown.** An unrecognised id is appended to the agent's own roster and
   bound verbatim, so it round-trips cleanly and an equality-only check is satisfied by
   something the backend never validated. Caught only by **advertised-membership**.

Neither check subsumes the other. Together they give a binding that is verified at the
harness boundary before a single token is billed.

**Ceiling, stated so it is not rounded up:** this is `HARNESS_VERIFIED`, not
`END_TO_END_VERIFIED`. `currentModelId` is the bridge's report of its own resolution. It is
*responsive* — it changes with input and exposes mis-resolution, which is what makes it worth
having — but it is not independent observation of the serving side. Only the local llama-swap
lanes can reach that (A5), because only they are ours to inspect. Hosted models cannot, and
claiming otherwise would be inventing attestation.

## A18 — lanes are not only queue consumers

Recorded in `acp/backends.py` rather than left as prose:

- `BindingAssurance`: `NONE` → `REQUESTED` → `HARNESS_VERIFIED` → `END_TO_END_VERIFIED`.
  A ladder, not a gate: a lower rung does not disqualify a lane, but a policy may require a
  rung and the recorded value must never be rounded up. `opencode`/local is
  `END_TO_END_VERIFIED`; `codex` and `claude-code` are `HARNESS_VERIFIED`; Claude's
  `set_model` path alone would be `REQUESTED` and is not used.
- `execution` / `supervision` / `interactive` as independent capabilities. Frontier
  subscriptions are marked for supervision and interactive use, which is where their value
  actually is.

Consequence for A15, worth stating as a boundary rather than a caveat: **`session/list` must
stay below the runner boundary.** It is machine-global, so it can never be equated with "my
sessions". A coordinator owns the set of session handles it created or explicitly attached —
never a listing it merely observed.

## A19 — the qualification is the durable asset; ACP is one integration, not the funnel

A16–A18 were written as if ACP were *the* way a backend participates. It is not, and the
distinction decides what should be built.

`actionq/harnesses/` already contains thin native adapters — `claude.py`, `codex.py`,
`opencode.py`, `copilot.py` — that invoke each vendor CLI directly. For a purchased Claude
subscription that is almost certainly the **least invasive trustworthy integration**: it keeps
the provider's own session, context, tooling and first-party improvements, and it inherits
model selection from the harness's native configuration rather than from a bridge that has
already been measured as unreliable (A13).

Routing Claude through ACP instead would trade those away for protocol uniformity. Uniformity
is worth paying for where it buys something — one codec across four agents, no vendor branch
in the contract (A10, A11) — and worth refusing where it costs native capability and buys
only symmetry. **Claude is the second case.** The bridge is a compatibility layer over a
harness that was bought precisely for its native behaviour.

What this week's probing produced that *is* durable is not an ACP implementation. It is the
qualification record:

```text
claude-code-acp 0.16.2
  session/set_model     : accepts any string, no read-back        -> unusable for binding
  cwd settings + new    : resolved value reported, mis-resolution
                          detectable                              -> HARNESS_VERIFIED
  advertised roster     : default | sonnet | haiku (SDK-pinned,
                          predates the Claude 5 family)
  unknown model ids     : adopted into the roster, not rejected
  session/list          : machine-global; never "my sessions"
  suitable for          : supervision, interactive, planning
  unsuitable for        : any policy requiring END_TO_END_VERIFIED
```

That table is specific to this workflow, survives the adapter it was measured through, and is
the thing to keep. `verification/capture_claude_acp.py`,
`probe_claude_acp_set_model.py` and `probe_claude_acp_settings_binding.py` regenerate it in
minutes against any future version — which is the point of owning qualification rather than
owning execution.

**Scope note on `acp/backends.py`:** the registry is ACP-scoped by construction and lives
inside `acp/` for the same boundary reason as everything else there. The lane taxonomy it
carries — `BindingAssurance`, and `execution`/`supervision`/`interactive` — is *not*
ACP-specific and applies equally to a native-harness or attached lane. If a second integration
path is registered, that taxonomy should move up beside the harness layer and `acp/` should
keep only the ACP facts. It is not moved now because there is exactly one consumer, and
guessing the shared shape from one example is how the wrong abstraction gets frozen in.
