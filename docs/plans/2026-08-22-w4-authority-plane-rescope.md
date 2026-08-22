# W4 rescoped: federation as authority-plane contracts

Status: **authorized and built** (2026-08-22). This document was ratified as a scope proposal
and the build it scopes is `actionq/vuoro_federation.py` plus its contract test, reachability
entries and the 0.1.27 version bump. The "not authorization to build" line it originally carried
is retired here rather than deleted quietly, because a reader arriving at this document later
needs to know it stopped being a proposal and when.

What remains unauthorized is unchanged and is not weakened by that: no migration is run, nothing
is deployed, no credential or role is fenced, and no grant is issued. Those are W5 and remain
operator-owned.

Supersedes `docs/plans/2026-08-22-w4-scope.md` (PR #40, paused as architecturally superseded).
That document's corrections are retained and carried forward here; what it could not settle —
section 2, "the manifest cannot express five domains today" — is settled elsewhere and no longer
blocks anything.

Basis: the Vuoro composition v4 design freeze
(`vuoro docs/plans/2026-08-22-composition-v4-design-freeze.md`, merged as vuoro #51) and the v4
candidate that implements it; W3 as merged (`227f55f`); `actionq/federation.py`,
`actionq/federation_schema.py` and `actionq/federation_backfill.py` at `86a374c`.

## 1. What the freeze changed, and what it did not

The blocking decision in #40 §2 was a choice between three bad options: revise the manifest
schema, publish a second ActionQ distribution, or let one `register` entrypoint quietly serve two
`owning_domain` values. v4 makes the question disappear rather than answering it. A role is no
longer a wheel: a **provider** is a release unit, one repository ships as many as it needs, and an
**authority binding** — not a packaging accident — is what carries the guarantee that repinning
one capability cannot repin another.

So the shape is now: ActionQ ships **at least two provider records** — one for frozen
`execution/v1`, one for the iterative federation contracts — from one repository and, if it
suits, one wheel. The v4 validator rejects a single release unit backing both a frozen and an
iterative exclusive capability, which is the mechanised form of what #40 was trying to protect
with packaging.

What did not change: `execution/v1` stays byte for byte identical, W4 is still source-only, and
every migration, deployment, credential and role-fencing action is still W5 and operator-owned.
<!-- claim: w4r-execution-v1-frozen -->

## 2. The three proposed federation properties, confirmed or corrected

The freeze marked three assignments **proposed pending owner confirmation** and named them a
W4-rescoping input. They are ActionQ-side facts, and here they are, against the source.

### 2.1 Owner of `federation.principal/v1` — **corrected: not ActionQ**

ActionQ implements no principal registry. `FederationPrincipal` (`actionq/federation.py:31-46`)
is a frozen value object over an *already authenticated* caller: an environment, a principal id
and a set of authority strings, constructed by `authenticated()` from whatever the caller passes.
Nothing mints it, nothing stores it, nothing can enumerate it. Ownership is a plain text
comparison — `row["owner_principal_id"] != principal.principal_id`
(`actionq/federation.py:183`).

A contract whose subject is identity cannot be owned by a component that only consumes identity.
The issuer owns `federation.principal/v1`, and today the issuer is Vuoro's identity plane: the
static identity registry and the gateway assertion resolver that produce `Identity.actor`. That
is a correction to the freeze's "issuer, not settled", made specific.

ActionQ's obligation under the contract is narrower and real: it must never accept a principal id
whose stability it has not been given evidence for. Section 3 is that rule.

### 2.2 `global` scope for `federation.principal/v1` — **confirmed as a change, not a record**

ActionQ partitions federation state by environment at the primary key:
`PRIMARY KEY (environment, principal_id, operation, idempotency_key)` and the decision table's
equivalent (`actionq/federation_schema.py:200-201`), with `environment` a required non-empty
field of the principal itself.

So `global` is not what ActionQ implements, and the freeze was right to flag it. It is
nonetheless the right contract scope, and the reason is section 3: once a principal id is minted
once and never reissued, it identifies the same subject in every environment, and ActionQ's
`environment` column becomes a partition key rather than part of identity.

`global` is **adopted**, because the stable-identifier rule was adopted with it and is
implemented on the issuer side (vuoro #53). The conditional this document originally carried is
discharged: the alternative was scoping the contract to `environment`, which would have forced an
edit to a contract shipped `frozen: true` and would have left `non-transferable` an obligation
that stops meaning anything at exactly the boundary ownership crosses. A global frozen contract
whose central guarantee is environment-local is not worth shipping.

### 2.3 `project` scope for `federation.grant/v1` — **corrected to `environment`**

There is no project dimension anywhere in the federation schema. Grants today are the flat
`authorities: frozenset[str]` on the principal, checked by string membership
(`actionq/federation.py:166`), over a fixed vocabulary: `federation.create`, `federation.relate`,
`federation.evidence.ingest`, `federation.acceptance.decide`, `federation.settlement.record`,
`federation.supersede`, plus `federation.backfill`. A `project`-scoped grant contract would
describe a dimension no implementation has, and would be unfalsifiable in exactly the way the
freeze's own gate exists to prevent.

Correct `federation.grant/v1` to `scope_kind: environment` for v1. Project-scoped grants are a
v2 change and should be proposed as one, with the schema change that makes them real.
<!-- claim: w4r-grant-scope-matches-implementation -->

## 3. The Identity → `FederationPrincipal` mapping, settled

This was the largest open item in the tranche, in #40 §9 and again in the freeze §7. It is
settled here.

**The failure it must prevent.** Ownership equality turns on `principal_id` forever, and v1 has
no ownership-transfer operation. If the string that becomes `principal_id` can be reissued —
an actor renamed, a decommissioned actor's name given to someone else — then every resource the
first holder owned silently becomes the second holder's, and there is no operation that can
correct it. The data is unfixable, not merely wrong.

**The rule**, adopted and implemented on the issuer side in vuoro #53. `principal_id` is a
**mint-once identifier, never reissued, distinct from the display actor**, carried by the
identity assertion rather than derived from a name:

    principal_id := "<issuer>:<subject>:<epoch>"

* `issuer` names the identity plane that minted it, so two issuers cannot collide;
* `subject` is the stable subject identifier the issuer already has;
* `epoch` is a per-subject issuance counter that **increments on any reissue** — a rename, a
  credential re-mint, a decommission-and-reuse. A reissued actor is therefore a *different*
  principal by construction, inherits nothing, and needs no transfer operation to be correct.

Three consequences, stated because each is a change somewhere:

1. **Vuoro's `Identity` gains a stable `principal_id` beside `actor`.** `actor` stays the display
   and provenance spelling; `principal_id` is what federation ownership binds. They are not the
   same field and must not be conflated: `actor` is chosen for humans and is expected to change.
2. **The gateway assertion carries an epoch claim.** It previously carried `actor`, `sub`
   (asserted equal to `actor`), `workspace_id`, `authorities`, `repo_ids` and per-request `iat`
   / `nbf` / `exp` on a 30-second lifetime — and `iat` is per *request*, not per issuance, so
   nothing in that set could distinguish a reissued actor from the original. `principal_epoch`
   is now required and the resolver composes `issuer:sub:epoch`; the static registry requires a
   minted `principal_id` per entry. **Minting and persisting the per-subject epoch is Vuoro
   Cloud's half and is not yet built** — until it is, the gateway path refuses every assertion,
   which is the correct failure direction.
3. **The reserved system namespace stays reserved.** `BACKFILL_PRINCIPAL_ID =
   "federation-backfill/v1"` (`actionq/federation_backfill.py:41`) is a pinned literal, and
   deliberately so: it is half of what makes backfilled changes distinguishable from native ones
   in a change ledger with no provenance column. The rule above must not be read as requiring
   every principal id to be minted per subject — system principals occupy a reserved prefix and
   are exempt by name.

**What ActionQ enforces.** Not the minting — it cannot. ActionQ rejects a `principal_id` that
does not parse as a mint-once identifier or a reserved system principal, at the serving edge,
before any command executes. That is a rule about the shape of the id, which is the only part of
the guarantee ActionQ can verify locally; the rest is the issuer's conformance evidence, which
the v4 validator requires as `ownership_evidence` on any provider bound to
`federation.principal/v1`.
<!-- claim: w4r-reissued-identity-cannot-inherit -->

## 4. The rescoped slice

**ActionQ side** — no modification of `execution/v1`, and #40's four additions carried forward
with their reasoning intact:

1. a federation serving surface in its own module (`actionq/vuoro_federation.py`), with its own
   `register` callable and its own compatibility record from
   `federation_schema.check_compatibility`. Not inside `actionq/vuoro.py`: an adapter names one
   module, and putting federation registration in the execution adapter's module is the opposite
   of separate. Under v4 this module also grows `build(runtime)` so the adapter satisfies the
   uniform construction protocol without a Vuoro-side shim;
2. `tests/test_federation_catalog_contract.py`, which does not exist;
3. a version bump and a tagged release — `pyproject.toml` is still the pinned `0.1.26`, and
   `tests/test_release_contract.py` / `test_release_version.py` are in the blast radius;
4. reachability-manifest entries for the new module, its import edges, its
   `catalog_operation_groups` and a `catalog_handler_bindings` row per federation operation;
5. **new**: `principal_id` shape enforcement at the serving edge (section 3), and the
   `Identity`-to-`FederationPrincipal` construction that reads it.

**Vuoro side** — its own repository and pull request:

6. two provider records for ActionQ in the profile, one per release unit, and adapters and
   bindings for the federation contracts. No four-domain constant to edit and no per-domain
   construction block to add: that is what the v4 candidate already removed;
7. `Identity.principal_id` and the issuer-side epoch (section 3), in the static registry and the
   gateway assertion;
8. a re-run of `validate_released_execution_adapter.py` against the **new** wheel. Publishing for
   federation moves the execution provider's artifact, so "execution/v1 is unchanged" must be
   re-proved against the artifact that will actually be deployed — #40's finding, and it still
   holds under v4 because the provider record pins a digest.

Nothing else. The dormant module gains no new commands and no migration is run anywhere.
<!-- claim: w4r-slice-contents -->

## 5. Ordering, unchanged and still not negotiable

The design order is the reverse of the release order. A published wheel cannot be amended, and
the Vuoro profile must name a digest that already exists, so ActionQ merges and publishes before
the Vuoro pull request can be validated — while the *shape* must be settled before ActionQ builds
anything. v4 settles the shape, which is what makes this rescope possible now.

One ordering constraint is sharper than #40 stated it: **backfill completes before any native
principal holds `federation.create` in an environment.** The backfill principal writes with
`federation.create` and `federation.relate` and is distinguishable only by its pinned id; a
native principal creating resources in the same environment mid-backfill produces a ledger whose
provenance cannot be reconstructed, and the change ledger is frozen with no provenance column to
add one to.
<!-- claim: w4r-backfill-precedes-native-create -->

## 6. Falsifiers

```falsifiers
{
  "minimum_coverage": 0.2,
  "falsifiers": [
    {
      "id": "w4r-execution-v1-frozen",
      "claim": "execution/v1 stays byte-for-byte identical: same operations, same wire bytes, same hash.",
      "scope": "the exact sha256 of the 26-operation catalog payload, which is what byte-for-byte means here",
      "test": "tests/test_vuoro_adapter_integration.py::test_catalog_wire_hash_and_registration_definitions_are_exact"
    },
    {
      "id": "w4r-grant-scope-matches-implementation",
      "claim": "Grants are authority strings on the principal with no project dimension, so federation.grant/v1 is environment-scoped in v1.",
      "scope": "the federation authority vocabulary is a flat set checked by string membership, and no federation table carries a project column",
      "test": null,
      "gap": "Falsifiable against ActionQ source today, but the assertion belongs with the contract record it constrains, which lives in the Vuoro support manifest. It lands with the Vuoro-side profile change (item 6), not here."
    },
    {
      "id": "w4r-reissued-identity-cannot-inherit",
      "claim": "A reissued actor identity cannot acquire a historical principal's ownership.",
      "scope": "a principal_id whose epoch differs from a resource owner's principal_id fails the ownership comparison, and an id that is neither a mint-once identifier nor a reserved system principal is rejected before any command executes",
      "test": null,
      "gap": "The enforcement does not exist yet: it is item 5 of the slice. This is the rule the build implements, and its test is written with it."
    },
    {
      "id": "w4r-slice-contents",
      "claim": "The slice is five ActionQ additions and three Vuoro ones, with no other surface changed.",
      "scope": "scope statement only; enforced by review of the change set and by the reachability manifest rejecting any unpinned new module or import edge",
      "test": null,
      "gap": "A scope boundary is a property of a diff, not of code at rest. The mechanised half is the reachability gate; the rest is what this document is reviewed for."
    },
    {
      "id": "w4r-backfill-precedes-native-create",
      "claim": "Backfill completes before any native principal holds federation.create in an environment.",
      "scope": "backfilled changes are distinguishable from native ones only by the pinned backfill principal id, so a native create during backfill leaves unreconstructable provenance",
      "test": null,
      "gap": "An operational ordering constraint over a deployment, not over code at rest. W5 owns the grant that would violate it; recorded here because the constraint originates in W4's data model."
    }
  ]
}
```

Coverage is **1 of 5**. A scope document is mostly unfalsifiable until the thing it scopes
exists — the same honest number #40 landed on, for the same reason. What changed is that four of
its six claims were about a blocked decision, and none of these are.

## 7. What this leaves open

Two of #40's smaller open items are still open, and neither blocks the build:

* whether federation operations are repository-scoped or plain authority-gated in Vuoro's
  authorizer, which is execution-specific today;
* who owns release-order steps 3 and 5-7 — Vuoro merge, release, deploy, client rediscovery,
  evidence capture — which no document assigns.

One new one, created by section 3 and now the critical-path item: **Vuoro Cloud must mint and
persist a per-subject epoch** before any gateway-issued identity can be resolved at all. The
service side refuses an assertion without the claim, which is the right direction to fail but
makes the cloud change a prerequisite rather than a follow-up. The static registry path is
unaffected — its ids are minted by the operator in the registry file.

## 8. Handed to W5

Recorded here rather than in a W5 plan because no W5 plan exists yet; whoever writes one should
start from this list. Neither item is urgent and neither justifies a release of its own.

1. **Bind `federation.resource/v1` in the Vuoro profile.** The composition now pins a wheel that
   *can* serve it — actionq 0.1.27 carries `actionq/vuoro_federation.py` with `build`/`register`
   — but the profile declares no adapter record, no closure and no attestation for it, so
   nothing is bound and nothing is served. This is the first real W5 unit, and it is the point
   at which the "no migration, no deployment, no grant" fence starts to matter: the federation
   schema reports `uninitialized` until its migration runs, so binding without migrating
   produces an incompatible domain at startup rather than a working surface. Backfill still
   precedes any native principal holding `federation.create` in an environment (§5).

2. **Move ActionQ's own adapter-kit pin.** `pyproject.toml:34` still requires
   `vuoro-adapter-kit` 0.1.0 by URL and digest, while the composition installs 0.1.1 — the kit
   that carries the uniform construction shims. Nothing fails today: a direct-URL requirement
   carries no version specifier, so `uv pip check` is satisfied and the release gates pass. But
   the owner's declared dependency is behind what its consumer runs, which is the kind of
   divergence that is cheap to fix inside the next ActionQ change and expensive to discover
   during a rollback. **Fold it into the next release; do not cut one for it.**
