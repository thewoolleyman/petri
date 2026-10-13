# Handoff: Frontend Package (03)

**Follows:** ce8de21 (core + executors complete)
**Spec:** `engine-spec.md` — the single source of truth. This document is a work
order: it cites the spec, it does not restate it. Where they appear to
conflict, the spec wins and the conflict is a finding to report.

**Purpose:** Turn workflow files into HIR graphs. GHA YAML → HIR, the complete
`${{ }}` expression grammar mapped onto the builtins table, and the native
format parser. Pure code, no IO beyond reading files — the most
property-testable package so far, and scoped deliberately to keep it that way.

**Gate:** package 02 closes when the Linux CI job is green. Do not start this
package before that.

---

## 0. Scope

**In:** native format parser; GHA workflow parser and lowering (spec §12); the
GHA expression grammar with contexts and functions; composite action inlining;
the `Unsupported { feature, hint }` diagnostic and the rejection set; a
`petri check <file>` CLI subcommand; the compatibility corpus harness (§6).

**Out (do not build):** action shims and the JS host, artifact/cache stores
(package 04); BuildKite frontend, reusable workflows (`workflow_call`), Docker
actions, `workflow_dispatch` inputs, service containers (v2). No new engine
semantics — if a construct seems to need one, stop and report rather than
extending the core.

---

## 1. Architecture constraint

`frontend-gha` and `frontend-native` are **pure**: file text in, `Graph` +
diagnostics out. No filesystem access beyond reading the workflow and its
local composite actions; no network (remote action resolution is package 04).
This is what makes the package property-testable and what keeps GHA's
peculiarities from leaking into the engine.

A sandbox provider selection is a validated provider kind, not a closed set:
`local` means the host, `docker` and `daytona` their plugins, and any other
valid kind a provider the host registers. A frontend carries the name through
as written and never maps an unknown one to the host; the runtime decides
whether it is registered. A registered provider runs ordinary process scopes
from its registration's template, so a frontend refuses the image and resource
settings that would size a built-in provider's sandbox under it.

Every frontend emits diagnostics through one shared type:

```rust
pub struct Diagnostic {
    pub severity: Severity,          // Error | Warning
    pub code: SmolStr,               // stable, greppable: "unsupported.concurrency"
    pub message: String,
    pub hint: Option<String>,        // what to do instead
    pub span: Span,                  // file, line, column — from the YAML node
}
```

`Unsupported` is a `Diagnostic` with an `unsupported.*` code. **Rejection is
loud and specific** (spec §12): never parse-and-ignore, never silently
approximate. A workflow with any Error diagnostic does not produce a graph.

---

## 2. Expression grammar (largest sub-task; do this first)

The GHA `${{ }}` grammar is the compatibility surface that decides whether real
workflows run correctly. A 90% implementation produces silently-wrong `if:`
evaluation, which is worse than a rejection.

**Required:** literals (`null`, bool, number incl. exponent form, single-quoted
strings with `''` escape); index/property access, including `.` on arrays
(object-filter semantics) and `['key']`; `*` wildcard filters; operators `!`,
`<`, `<=`, `>`, `>=`, `==`, `!=`, `&&`, `||` with GHA's precedence; and — the
part most often got wrong — GHA's **loose equality and truthiness** coercion
rules, plus `&&`/`||` returning operand *values*, not booleans.

**Functions:** `contains`, `startsWith`, `endsWith`, `format`, `join`,
`toJSON`, `fromJSON`, `hashFiles`, and the status functions `success()`,
`failure()`, `cancelled()`, `always()`.

**Contexts:** `github`, `env`, `vars`, `job`, `jobs`, `steps`, `runner`,
`secrets`, `strategy`, `matrix`, `needs`, `inputs`.

**Mapping rule (this is the point):** GHA functions lower onto
`ir::expr::BUILTINS` (spec §7) — they do **not** become a second evaluator.
Where a GHA function has no builtin, add one *through the table's growth bar*
(pure, total, tested, justified) rather than special-casing in the frontend.
Report any GHA function whose semantics can't be expressed as a total builtin;
that's a genuine finding about the expression language, not a licence to
diverge.

Status functions and contexts resolve against `EvalEnv`'s `RunContext` (spec
§4) — `steps.x.outcome`/`conclusion` derive from
`PartialSuccess.underlying` per spec §12. `secrets` is **not** readable in
expressions (spec §11): `secrets.FOO` in an env-shaped position lowers to
`{"$secret": "FOO"}`; anywhere else it is an Error diagnostic, not a runtime
`secret_misplaced`.

**Testing:** this sub-task is property-tested, not example-tested. Parse →
print → reparse round-trips; differential tests against the documented
coercion table; a truthiness/equality matrix as a table test. Fuzz the parser
for panics — malformed workflow text must always yield diagnostics, never a
panic.

---

## 3. GHA lowering

Follow spec §12's table exactly. Beyond it, three things that need care:

**Composite actions** inline as subgraphs (spec §12). `uses:` with a local
path (`./.github/actions/foo`) is in scope; anything requiring network
resolution (`owner/repo@ref`, Docker actions) is `unsupported.action.remote` —
package 04. Inlined nodes get namespaced instance names; nested composites
inline recursively with a documented depth cap producing a diagnostic, not a
stack overflow.

**Matrix** → `ForEach { target: Subgraph }` with `fail-fast`/`max-parallel`
mapped. `include`/`exclude` expansion follows GHA's ordering rules — get this
from the documented semantics, not intuition; it's a common source of
mismatch. Matrix values that are expressions (`fromJSON(needs.x.outputs.y)`)
stay unevaluated in HIR — that is the whole point of lazy lowering (spec §5).

**Job/step statuses.** `if:` conditions default to `success()` semantics when
absent; the `always()`/`failure()`/`cancelled()` family must behave correctly
for skipped and partial-success upstreams. Build the truth table explicitly and
test it — this is where compatibility bugs are most likely and least visible.

**Rejection set** (Error, `unsupported.*`): `concurrency` (D2, spec §12),
`workflow_call`, `workflow_dispatch` inputs, service `containers`,
`defaults.run` if not implemented, `runs-on` labels the executor doesn't know
(per-label, spec §12), remote `uses:`, `container:` if not mapped to a Docker
scope. Each needs a hint naming the alternative or the package that will add
it.

*Amended 2026-09-04.* Much of that set has since shrunk; `crates/github/SUPPORT.md`
is the live account. One contract to hold on to from that work: a frontend
lowers its format's container and service flags into the graph's typed
options (`ir::ContainerOptions`, `ir::ServiceOptions`) and rejects a flag with
no typed mapping at lowering as `unsupported.container.option`, naming the
flag. The graph never carries raw engine flags, and an executor never
receives an option it cannot honor. Service port publications are warned
about (`ignored.services.ports`) and dropped: a service is reached by its
name on the scope's network.

---

## 4. Native format

The native format is where the engine's full expressiveness is reachable
(cycles, `Any`/`Quorum` joins, multi-arm selection, `for_each` both ways). Its
job in this package is to be a faithful surface over spec §2–§5, with two
rules:

- **Fan-out is explicit** (spec §2): `next:` produces one select group;
  `parallel:` produces multiple. This must be impossible to get accidentally.
- **Normalize, don't relax**: `Quorum{1}` on a loop head normalizes to `Any` in
  lowering (spec §8, invariant 8); the frontend surfaces invariant-8 violations
  as diagnostics with the corollary as the hint ("put a join node in front of
  the loop head"), because that error will otherwise read as arbitrary.

Ship a format reference doc with a worked example per construct, including the
sequential `for_each` desugar (spec §5) so users can see what their loop
becomes.

---

## 5. CLI: `petri check`

`petri check <workflow>` parses, lowers, validates (spec §8), and prints
diagnostics with spans — plus the scope re-entry lint, labelled as the
over-approximation it is. Exit non-zero on any Error. This is the fastest
feedback loop for the corpus work in §6 and the first thing a user will run.

Also useful and cheap: `petri check --print-graph` emitting the lowered graph
in a stable text form. It makes lowering bugs reviewable and gives the corpus
harness something to diff.

---

## 6. Compatibility corpus (the acceptance bar)

Assemble `.github/workflows` from **~20 popular OSS repos** (vendored at a
pinned commit; record provenance and licence). The harness runs `petri check`
over all of them and reports, per workflow: lowered clean / lowered with
warnings / rejected with a specific `unsupported.*` code / **failed for any
other reason**.

**The bar for this package is not 100% lowering.** It is: every workflow
either lowers correctly or is rejected with a specific, actionable code — zero
crashes, zero silent approximations, zero generic errors. Track the pass-rate
breakdown over time; it is the empirical answer to which action shims package
04 should build first, so report the histogram of `unsupported.action.remote`
targets by action name.

---

## 7. Acceptance

1. Expression suite: coercion/truthiness matrix, precedence, object filters,
   `&&`/`||` value-returning semantics — table-tested; parser fuzzed for
   panics (none).
2. Every GHA function lowers to a BUILTINS entry; a test asserts no frontend
   evaluator path exists (spec §7's table gates dispatch).
3. Status-function truth table across success / failure / partial-success /
   skipped / cancelled upstreams.
4. Matrix: static, `include`/`exclude`, and expression-valued (stays
   unevaluated in HIR, expands at runtime); `fail-fast` maps to the splice
   cancel scope.
5. Composite action inlining incl. nested, with the depth cap producing a
   diagnostic.
6. `secrets.FOO` lowers to `$secret` in env position; Error diagnostic
   elsewhere; no secret value ever appears in a lowered graph.
7. Native format: cycle + XOR routing + `Any` join example lowers and runs
   (reuse the engine E2E harness); invariant-8 violation produces the hinted
   diagnostic.
8. Corpus harness green against the bar in §6, with the report committed.
9. End-to-end: at least two real corpus workflows *run* on the executor from
   package 02 (pick ones whose steps are pure `run:` — no action shims yet).

### Node metadata for hosts

A frontend attaches `Node::meta` (opaque to the engine) and the public event
contract carries it verbatim on every event about the node. Two keys have a
shared meaning across frontends: `kind` names the logical role of the node in
the source format (the Fabro frontend writes `start`, `exit`, `command`,
`agent`, `human`, `parallel`, `parallel.branch`, `parallel.fan_in`, and so
on), and `synthetic: true` marks a node the frontend invented during lowering
(the Fabro `goal_check`, a `parallel.branch` delegate that runs its branch in
a child invocation, a synthetic `<fork>.fan_in`). A third key,
`branch_role = { fork, index }` (`ir::placeholder::BRANCH_ROLE_META`), lets a
frontend declare a node's branch membership when the branch runs in another
invocation, and the engine's `BranchMap` honours it where the node's own
graph gives it no role (a node that forks in its own graph keeps its fork
role, so a nested fork projects as its own occurrence). A `for_each` expansion
item of `{"$placeholder": true}` (`ir::placeholder::PLACEHOLDER_ITEM_KEY`) is
the one item an empty list expands to when the lowering needs the template
to fire once; its clone is no branch to `BranchMap` or the event stream. A
host uses these, together with the engine's branch role, to tell logical
stages from lowering artifacts; it never reads node names for that.

## 8. Reporting

Same format as packages 01–02. The departures section with reasoning is the
part I read first. Two specific things I want called out if they occur: any
GHA semantic that cannot be expressed within the current engine or expression
table (that's a spec finding, not a frontend workaround), and any place the
corpus revealed a construct common enough that the rejection set should shrink
before package 04.
