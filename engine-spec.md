# Engine Specification (v2, consolidated)

**Status:** Single source of truth as of ce8de21. Supersedes `ir-design.md`,
`core-gaps-handoff.md`, and `executor-handoff.md` (archive them; do not update
them further). Incorporates every accepted departure from builder READMEs 1–29
and the ce8e follow-ups. Where this document and the code disagree, that is a
bug in one of them — flag it, don't silently pick one.

---

## 1. Model

The engine executes a directed graph (cycles permitted) with token-flow
semantics. A node **fires** when its join policy is satisfied by incoming tokens
of one generation; on completion, routing emits tokens on outgoing edges.
Multiple tokens in flight = concurrent nodes. The run is **complete at
quiescence**: no live firings and no pending token can satisfy any join. Run
status folds from node outcomes under the graph's `Completion` policy:
`AnyFailure` (default — any failed record fails the run, the CI rule) or
`TerminalNode(id)` (success iff that node has a success-like final record at
quiescence; failures elsewhere are control flow, and a missing record is a
failure — the successful nonterminal dead end fails, a pinned departure from
fabro_core). Root cancellation and engine `RunError`s outrank both. The
`run.failed` static keeps its own meaning — "any failure so far" (errors or
failed history), under every policy — deliberately not the folded status,
which under `TerminalNode` would read `Failed` until the exit record exists.

This quiescence result describes workflow execution. An embedding host may
declare required run finalization through `ExecutionHooks`. The IO driver
awaits it before environment release; the coordinator then commits the
overall run result. Required finalization can fail successful execution,
never upgrade failed execution, and never replace cancellation. Execution
results remain distinct. The committed terminal result and finalization
failure detail are immutable (`crates/core/execution/HOOKS.md`).

All coordination lives in a pure, sans-IO core: `apply(state, event) ->
(state, commands)` — deterministic, no clocks, no RNG, no filesystem. All side
effects live behind traits (`Executor`, `StepKind`, `LogSink`,
`SecretProvider`, stores).

## 2. Routing: AND-of-XOR normal form

A node's routing is a list of **routing groups**. Each group independently emits
**at most one** token. `FirstMatch` tries arms in order. `Tiered` selects the first
tier with eligible candidates, then applies that tier's persisted pick policy.
N groups may emit N tokens concurrently.

| Pattern | Representation |
|---|---|
| Default: pick one successor | 1 group, N guarded arms (+ fallthrough) |
| Unconditional next | 1 group, 1 arm, `Guard::Always` |
| Fan-out (parallel) | N groups of 1 arm each |
| Conditional fan-out (OR-split) | N groups; a group may emit nothing (`Fallthrough::NoEmit`) |
| Loop back-edge | arm whose edge has `back: true` (increments generation) |

Fan-out requires writing multiple groups; it can never occur implicitly.

## 3. Core types (normative shapes; code is authoritative for detail)

```rust
// Identifiers: NodeId, EdgeId, ScopeId, ExprId (u32 newtypes over an id-space
// marker parameter, default `Live`; a GraphFragment reuses the same node/edge/
// scope/expression types over `Local` ids, and the type system refuses a mixed-
// space id); CancelScopeId (u32); FiringId (u64, unique per execution);
// Generation(u32); Attempt(u32, 1-based). Firing key: (NodeId, Generation, Attempt).

pub enum Guard { Always, Expr(ExprId) }

pub struct Edge {
    pub id: EdgeId, pub to: NodeId, pub guard: Guard,
    pub map: Option<ExprId>,      // token payload; default = source outcome.output
    pub back: bool,               // traversal increments Generation
    pub weight: u32,              // default 1
    pub label: Option<SmolStr>,
    pub transition: EdgeTransition, // Continue | Restart
}

pub struct RoutingGroup {
    pub policy: SelectionPolicy,
    pub arms: Vec<Edge>,
    pub fallthrough: Fallthrough,
}
pub enum Fallthrough { NoEmit, Error }
pub struct Routing { pub groups: Vec<RoutingGroup> }

pub enum JoinPolicy { All, Any, Quorum { n: u32 } }   // matched per (node, generation)

pub struct Node {
    pub id: NodeId, pub name: SmolStr, pub scope: ScopeId,
    pub step: StepRef,                     // registry key + config (HIR: may hold ExprIds)
    pub join: JoinPolicy,
    pub precondition: Option<ExprId>,      // false => Skipped without executing; routing still runs
    pub routing: Routing,
    pub budget: Budget,                    // max_firings (counts generations), timeout (per attempt)
    pub retry: RetryPolicy,
    pub run_on_cancel: bool,               // §5: may fire inside a cancelled scope
    pub cancel_group: Option<NodeId>,      // §5: independent group, identified by its anchor
    pub splice_policy: SplicePolicy,       // §5.1: Deny (default) | Append | Replace{scope}
    pub meta: Value,                       // opaque, host-facing; the core never reads it
    pub expand: Option<Expansion>,         // HIR only
}

pub struct RetryPolicy {
    pub max_attempts: NonZeroU32,          // 1 = no retries (default)
    pub backoff: Backoff,                  // initial, factor, max; jitter applied by driver
    pub retry_on: RetryOn,                 // Vec<StatusKind> + failure classes (§3.1)
    pub on_exhaustion: Exhaustion,         // Fail | AcceptPartial
}

pub enum Expansion {
    ForEach { items: ExprId, target: ExpandTarget,
              max_parallel: Option<u32>, fail_fast: bool },
}
pub enum ExpandTarget { Node, Subgraph { entry: NodeId, exit: NodeId } }

pub struct Scope {
    pub id: ScopeId,
    pub env: BTreeMap<SmolStr, ExprOrValue>,
    pub runtime: RuntimeSpec,      // HostProcess | Docker { image, .. };
                                   // + requirements: Vec<SmolStr> (opaque labels, D3)
    pub workspace: WorkspacePolicy,
}

pub struct Token {
    pub edge: EdgeId,
    pub generation: Generation,    // renamed from `gen` (edition-2024 keyword)
    pub payload: Value, pub from: FiringId,
}

pub enum Completion {               // Graph.completion: how run status folds (§1)
    AnyFailure,                     // default
    TerminalNode(NodeId),
}
```

### 3.1 Status — closed and permanent

```rust
pub enum Status {
    Success,
    PartialSuccess { underlying: Option<UnderlyingFailure> },  // success-like; carries the real failure
    Failure(FailureInfo),                                // FailureInfo.class: SmolStr (§13)
    Skipped, Cancelled, TimedOut,
}

pub enum UnderlyingFailure {       // exactly the statuses `is_failure` names
    Failure(FailureInfo),
    TimedOut,
}
```

Rules (violations are review-blockers):
1. **Closed enum** — six variants, permanent. Frontend concepts map onto them.
2. **One classification point** — `Status::is_success_like()` (`Success |
   PartialSuccess`) is the only definition of success-likeness; joins, cancel
   scopes, default guards, retry defaults all call it. Never open-code the match.
   (`is_success()` and `FailureInfo.retryable` were deleted for violating this;
   do not reintroduce.)
3. **Log truth** — converting a failure to `PartialSuccess` must preserve the
   real failure in `underlying`: the failure it was converted from, whole — a
   `Failure` with its info, or a `TimedOut` (which has no info to keep). It is
   not a cause; a policy made the conversion. All conversion paths: process `soft_fail`
   config; `Exhaustion::AcceptPartial` (fires whenever a retryable status hits
   exhaustion, including `max_attempts: 1`; `RetryPolicy::finalize`, applied
   by the driver to every returned attempt before a host prepares the result,
   and by the engine only to the `invalid_splice` failure it makes itself);
   direct StepKind return.
4. `StatusKind` is the payload-free discriminant for `retry_on` matching only —
   derived via one `From<&Status>` impl; not a second classification point
   (an explicit `PartialSuccess` entry in `retry_on` cannot defeat rule 2).
5. `Status::Cancelled` carries no payload. Cancellation escalation detail lives
   in `output.cancel_escalation: "sigterm" | "sigkill" | "cancel_forced" |
   "cancelled_before_resume" | "killed_before_resume"` (the last two are
   resume's, §10).

## 4. Firing, retries, run context

**Firing rule.** Per (node, generation): satisfy join → check budget → evaluate
`precondition` (false ⇒ synthesize `Skipped`; routing still runs) → emit
`StartStep`. Token generation on emit = source generation, +1 per back edge.
`Cancelled` joins the statuses that flow through routing (§5);
`is_success_like` is untouched — it remains `Success | PartialSuccess`.

**Budget refusal.** A key the budget refuses (`firing_count >= max_firings`)
records `RunError::BudgetExceeded`, which fails the run; it takes the key's
tokens and marks the key fired, and it records and routes nothing, so work
downstream of it waits. Routing nothing is what makes the budget a
termination guarantee: a routed outcome could take a back edge, be refused
again in the next generation, and route again, forever. (A mutation that
routes a failure instead overflows the stack in the flow property test.)
Only a key that would run is an error. A key that would only complete
`Cancelled` without running (§5) stops quietly: it takes its tokens and is
marked fired, but records no error. Nothing would have run, so a cancelled
loop whose back arm is `always` ends at its budget without failing the run;
after a group cancel, the rest of the run keeps its own status.

**Retries.** Each firing starts at `Attempt(1)`; counters reset per firing (a
later generation retries fresh). On a matching non-final failure the core emits
`ScheduleRetry` (deterministic base delay; driver adds jitter and sleeps; feeds
back `RetryElapsed`). **Retries are invisible everywhere except the event log:**
routing, run-context recording, cancel-scope propagation, and `kv` merges key
off the final attempt only. Non-final attempts' full outcomes (including their
`context_updates`) live in their finish records for tooling. `AcceptPartial`'s
converted outcome *is* final and merges normally; the engine records the
finish it is given and does not convert again, so a host's prepared result
stands. Success-like statuses are never retried. `Budget.max_firings` counts firings, not attempts;
`Budget.timeout` is per attempt.

**Run context.** Core-maintained, derived state (never checkpointed
separately):

- `nodes.<instance>.{status, output, generation, attempts}` — written only by
  the core on final-attempt `StepFinished`; clones record under instance names
  (`build#2`). The separator is `#`, not `[n]`: instance names become expression
  keys, and `nodes.build[2].status` would collide visually with the `[0]` /
  `['key']` index syntax a frontend grammar has to parse, while `build#2` is
  unambiguously one key.
- `kv.*` — written only via `Outcome.context_updates`, merged in `apply()` in
  event order, last-write-wins.

Expressions evaluate against `EvalEnv { token, run: &RunContext, statics }` —
guards, `map`, preconditions, `items`. This **replaced** the interim
firing-context mechanism; edges no longer thread status. `success()` on an
entry node evaluates true off its seed; on clones it resolves via instance
names.

## 5. Splices, loops, cancel scopes

**Splice** (`Expansion::ForEach`, evaluated inline in `apply`): clone target per
item with `item`/`index` bound; each splice creates a fresh `CancelScope`
(`fail_fast` cancels siblings via it); `max_parallel` is scheduler admission
control. **Supersession rule:** the splice replaces the template region's edges
entirely — a collector's `All` join counts spliced edges only, never a template
edge no token can cross (that was a real deadlock; the regression test is named
for it). Collector ordering uses `index` in token payloads. Spliced tokens
inherit the splicing firing's generation. `Command::ExpandNode` exists as a seam
for external item resolution but is never emitted today.

**Entry nodes** are seeded via synthetic seed edges allocated at runtime from
`max declared id + 1` (collision impossible by construction; validation never
sees them; `EdgeId::SEED` is rejected in routing groups). Join counting is
uniform: `All` over one seed edge = one seed token. The one exception is a
`for_each` clone's entry: the template's join already admitted the expansion,
so the clone's seed forces entry for its generation, as a restart's entry
does, and the join is not applied twice. Without that, a `Quorum { n >= 2 }`
on the `for_each` node would admit the template and then start no clone.

**Sequential for_each** is a frontend desugar, not IR: entry emits
`{items, idx: 0, acc: []}`; final select group has a back arm guarded
`idx + 1 < len(items)` with an accumulating `map`, and an exit arm carrying the
accumulator. Generations distinguish iterations; budgets cap runaway loops.

**Cancel scopes** — dynamic sets of firings cancellable as a unit: the run
root, each splice, job-level cancel-on-failure. Stopping has two tiers, Cancel
and Kill — the workflow-level analogue of `SIGTERM` and `SIGKILL`.

**Declared cancellation groups.** `Node.cancel_group` names an anchor node. The
anchor names itself, and every member uses the same resource scope. A group
cannot cross an expansion boundary. Each group gets a child cancellation scope;
expansion remaps the anchor so each matrix leg gets a separate group. Dynamic
splices remain children of their owner's cancellation scope.

`Event::CancelRequested { target: group }` politely cancels the named node's group
and its descendants. Unknown nodes and nodes without a group are logged no-ops.
This event does not mark the root cancelled. `RunHandle::cancel_group` exposes
the request by node name and is available as a driver-provided step capability.

**Cancel** (`Event::CancelRequested { scope }`) asks the scope to stop:

1. Live firings get `DeliverControl { Cancel }`. A cancelled firing's final
   outcome **routes like any other outcome**. Retry is still refused: the point
   of cancelling is to stop the work.
2. Pending tokens are **not** dropped. A node in a cancelled scope whose join
   is satisfied completes `Cancelled` without executing — no `StartStep` —
   unless it opted in via `Node.run_on_cancel`:
   - `run_on_cancel` set → evaluate the precondition. Absent or true → the node
     **fires for real**. False → complete `Cancelled`. An evaluation error
     keeps the ordinary behavior — `RunError::Eval` plus a routed `Failure`
     outcome — because cancellation must not convert a broken expression into a
     clean cancellation.
   - `run_on_cancel` unset → complete `Cancelled` without evaluating anything.
     Un-marked work can never restart, whatever its gates say.
   - An expansion node in a cancelled scope never expands; it completes
     `Cancelled`. `run_on_cancel` on an expansion node is a validation warning
     (ignored in v1).
   - The same admission applies to work **fed by** cancelled work: a node any
     of whose join tokens was emitted by a firing that recorded `Cancelled`
     completes `Cancelled` unless it is marked. This is what keeps a
     `fail_fast` splice's un-marked collector — outside the cancelled scope —
     from starting, while a marked one fires and gathers partial results.
   - A completion without running counts against the budget, and the budget
     refuses it quietly (§4, "Budget refusal").
3. A firing **awaiting a retry backoff** has no work in flight and no driver
   task to deliver to, so the core settles it at once instead of waiting out
   the backoff: it records a `Cancelled` outcome and routes it (under Kill:
   records without routing). Settling leaves a tombstone; the one matching late
   `RetryElapsed` consumes it silently — the driver's sleeper cannot be
   recalled, and replay must stay clean. Every other invalid `RetryElapsed` —
   unknown firing, not awaiting, duplicate after the tombstone is consumed —
   still raises `UnknownFiring` / `UnexpectedRetry`; the no-op is
   cancellation-specific, never a blanket swallow of malformed input. A
   firing **awaiting its admission** has no step either, and settles the same
   way: its pending decision is withdrawn, and the driver drops the late
   answer. A kill of `ROOT` settles these firings before it drops the run's
   other pending decisions.

**Kill** (`Event::KillRequested { scope }`) stops the scope: the forced tier,
scope-addressed like `CancelRequested` (a kill of `ROOT` kills the run).
Killing marks the scope closure killed (killed implies cancelled), drops its
pending and deferred tokens, swallows tokens aimed inside it, records live
firings' outcomes **without routing**, and admits nothing — `run_on_cancel`
included. It also withdraws the routing decisions still open for outcomes
recorded inside it before the kill, so nothing routes out of a killed closure,
however long the host took to decide; the driver drops the late answers. Delivery is `Control::Kill`, sent to **every** live firing in the
closure, already-cancelling ones included; a step kind receiving it goes
straight to `SIGKILL`, no ladder. No new `Status` or `RunStatus` variant: how a
firing was stopped is a mode, not an outcome — a killed firing records
`Cancelled`, and the logged `KillRequested` event carries the mode.

Expressions see cancellation through two statics: `run.cancelled` (root-only,
built from the folded run status) and `scope_cancelled` (true when the firing's
node lies in a cancelled cancel-scope; a root cancel marks every scope, so it
subsumes `run.cancelled` for gating). The upstream status fold has a
`cancelled` arm — failure > cancelled > skipped > success — so the core
`success()` guard is false and `cancelled()` true over a cancelled upstream.

**Terminal scope release:** when the run finishes, the core emits
`ReleaseScope` for every still-held scope and removes them from `held_scopes`
in the same transition, before `FinishExecution` — a finished serialized state
claims no resources. Nothing can need an environment after `FinishExecution`. (This also
covers the parked-token leak that exists independently of cancellation: a token
parked at an unsatisfiable join no longer holds its environment past the end of
the run.)

After a cancel the core routes and fires whatever `run_on_cancel` admits;
**bounding** cleanup is the driver's job (§10): a cleanup that outlives the
grace is ended by feeding back `KillRequested`.

`Control::Deliver` is not a stop signal: it never starts the cancellation
ladder or the kill tier, and delivering one touches no cancel-scope state.

The engine does not gain a hierarchical step. A step can receive an
`InvocationClient` capability from the coordinator and call a pre-registered graph.

### 5.1 Outcome-driven splice: the boundary

The engine's one mutation applicator takes a producer-neutral prepared input;
`ForEach` and step-initiated uploads are its two producers. Three
representations, one direction of travel:

- `SpliceRequest { mode, fragment, context, attachments }` is fragment-local, serialized
  in `StepFinished` via `Outcome.splices: Vec<SpliceRequest>` (ordered, empty by
  default), and untrusted. `GraphFragment` is an executable-plan type — local
  nodes, edges, resource scopes, an `ExprTable`, explicit entries and exits, in
  the `Local` id space — with no run `params`, root entries, or `completion`.
  Fragment scopes are isolated by default. `context: InheritUploader(scope)`
  maps one declared local scope to the uploader's live resource scope instead
  and copies the uploader clone's `matrix`, `item`, and `index` bindings to all
  fragment nodes. This gives a run-time planner the uploader's workspace,
  runtime, services, and expansion context without copying frontend-specific
  data into the core. The named local scope must exist; its own scope settings
  do not replace the live scope. Fragment validation is the one §8 invariant
  engine over a fragment-local view (`validate_fragment`,
  with a registry-aware variant mirroring `validate_with` for the host half of
  the two-stage contract) plus fragment-only rules: exits resolve, no HIR
  `expand`, and an empty fragment is valid only under `Replace`.
- `PreparedSplice` has every identifier remapped and every structural,
  composition, capability, and secret-reference check complete. Engine-private,
  non-serializable, constructible only by preparation: `apply_prepared_splice`
  is infallible for domain errors. `ForEach` supersession and outcome
  retraction ride it as data, so the applicator stays producer-neutral.
- `AppliedSplice` is runtime bookkeeping, serialized with `EngineState`: a
  `SpliceBatchId`, the core-stamped owner `NodeId`, added nodes and cancel
  scope on one common record, and producer-only data on a `SpliceProducer` arm
  (`ForEach` scheduling and supersession; outcome retractions as
  `AdmissionKey { node, generation }` structs).

**Policy.** `Node.splice_policy` is a closed capability, totally ordered:
`Deny < Append < Replace(OwnBatches) < Replace(AllPending)`. `Deny` is the
default. `authorize(policy, mode)` rejects an operation above the node's
policy; the delegation check rejects a fragment node whose declared policy
exceeds its uploader's — a legality check, never a mutation: excess authority
in either direction rejects loudly as `invalid_splice`, and nothing is
clamped. `AllPending` is run-scoped destructive authority; the cap permits
delegating any policy at equal or lower strength, which is what allows normal
chained uploads.

**Attachment** is concrete stage-1 IR on the request. Entries implicitly
attach through new select groups on the uploader, added pre-routing, guarded
success-like **and pinned to the uploading firing's generation** (the groups
persist on the node, and a loop-head uploader's next generation must not
re-seed an earlier batch). Batch exits gain edges to each existing forward
dependent of the uploader — `All` joins only; anything else rejects (a
generated batch barrier for `Any`/`Quorum` is a v2 seam) — and a dependent the
same transaction retracted is skipped. `Attachment::DependsOn` pairs a
fragment-local node with a typed `ExistingNodeRef` — an instance name, never a
live `NodeId` — resolved through the preparation name index, which covers the
live graph and nodes added by earlier requests in the same outcome. A
reference must resolve to exactly one admission: a target with admissions in
more than one generation — any loop node — rejects in v1. By state at splice
time: a not-final reference extends the referenced node's routing with a real
unconditional edge (ordering; it has not routed yet, so the edge can still
emit); a final reference becomes a guard over `nodes.<name>.status` — the
record exists, so the guard does not need to wait. Both are completion
ordering; status gating stays the frontend's business. A reference to a key
retracted by an earlier request in the same transaction rejects.

Identifier remapping allocates from live-graph high-water marks; instance
names are used verbatim under the existing `#` scheme, and a collision with
any live or planned name is `invalid_splice`. Each applied batch gets a fresh
`CancelScope` parented under the uploader firing's current cancel scope.

### 5.2 Finalization: prepare-then-commit

Only a firing's **final attempt** applies its requests; a non-final attempt's
requests are recorded in its `StepFinished` and change nothing. For a
candidate-final result, in order:

1. **Cancelled-scope check first.** If the firing's scope is cancelled (killed
   included), the splice list is normally dropped wholesale — no policy check,
   no validation, no `invalid_splice` — and the rest of the outcome records,
   merges, and routes under the normal cancel semantics. One narrow cleanup
   case remains eligible for normal preparation: the uploader is marked
   `run_on_cancel`, every request is `Append`, and every fragment node is also
   marked `run_on_cancel`. A mixed or destructive list is dropped atomically.
   Cancellation can therefore admit explicit cleanup, but not ordinary work.
2. **Prepare** every request in order inside one transaction plan owning the
   allocation cursors, the name index, planned edges, and planned retractions.
   Preparation never touches canonical `EngineState`, so a rejected
   transaction leaks nothing — not even allocator movement.
3. **On rejection**, convert the whole outcome to the canonical
   `Failure{class: invalid_splice}` — the step's `output` and metrics kept, its
   `context_updates` dropped, the message naming the request index and
   location, raw requests remaining only in the External event — and only then
   run `retry_on` and exhaustion. `invalid_splice` is an ordinary retry class:
   a matching retry re-runs the step, and a later attempt can succeed.
4. **If all requests prepare, commit** consumes the plan: record the outcome,
   apply in order, route the uploader (with its refreshed routing), run
   quiescence. Nothing partial ever commits, splices apply before the uploader
   routes (no lost-upload race), and replaying the External `StepFinished`
   regenerates the same mutations byte-identically.

V1 commits at the uploader's final attempt, not per upload call — a deliberate
departure from Buildkite's apply-per-call, preserving "retries are invisible
outside the log". If commit-at-call becomes necessary, the v2 seam is a
distinct External `SpliceRequested` event, not an overload of `StepProgressRecorded`.

### 5.3 Replace: generation-scoped retraction

`Replace` computes a set of `AdmissionKey`s, not a permanent set of nodes. A
key is retractable when it has no live firing and no final record — exactly
the keys with parked tokens or `max_parallel` deferrals. Under `OwnBatches`,
candidates are limited to batches whose recorded owner is the retracting
uploader's `NodeId` (stable across generations, so a loop-head uploader can
replace its own earlier batches); under `AllPending`, every retractable key
qualifies. Retraction drops the parked tokens and deferrals, swallows later
tokens for those exact keys, and records the keys on the batch. Running
firings, history, and future generations are untouched; retracted admissions
never fire, leave no record, and do not block quiescence or terminal release.
The retractable set is a pure function of `EngineState` at finalization time —
never stored in the External event; replay re-derives it.

The driver adds one splice-scoped secret rule (§11): a registered secret value
inside a `SpliceRequest` fails the firing with `invalid_splice` and no
fragment applied — never masked, because masking a fragment would change the
executable plan. The driver clears `Outcome.splices` before the append, so the
offending request never reaches the log.

## 6. Engine interface, event log, ResolvedFiring

Events: `ExecutionStarted{start}`, `AdmissionDecided`, `RoutingResolved`,
`RouteApplied`, `TokenEmitted`, `StepStarted{firing, attempt}`,
`StepProgressRecorded`, `StepFinished{firing, attempt, outcome}`,
`RetryElapsed`, `NodeExpanded`, `CancelRequested{target: scope | group}`,
`KillRequested{scope}`, `ControlRequested{firing, ctl}`. On the wire each is
an object tagged by `event` with a `<subject>.<verb>` name (`step.finished`)
and its fields beside the tag; the public event stream carries the same
object unchanged. Commands: `StartStep(ResolvedFiring)`,
`DeliverControl`, `ScheduleRetry`, `ExpandNode` (reserved), `AcquireScope`,
`ReleaseScope`, `Admit`, `ResolveRouting`, `FinishExecution`.

Event log version 12 is the one-vocabulary form above, with snake-case tags on
every enum inside a record, the scope records (v11), and a partial success
that keeps its whole underlying failure (v12). Earlier log versions are
rejected; there is no migration.

Every execution start and attempt start uses `Admit` → `AdmissionDecided`. Every final
firing outcome, including a terminal node with no groups, uses one
`ResolveRouting` → `RoutingResolved` round trip. The core computes candidates and
validates the recorded decision before it applies `RouteApplied`. Replay uses a
recorded decision and reissues only an unresolved command.

`EngineExit::Terminal` ends an invocation unless it is already cancelled by its
coordinator. `EngineExit::Restart` ends one reset-free engine and names the edge,
target, and source firing for a successor execution in the same invocation.

**`ControlRequested`** is the host delivering a value into a live firing — a
human gate's answer, a supervisor's steering — through the engine, so question
and answer are both in the log and replay/resume reproduce a pending
interaction. Only `Control::Deliver` to a live, not-cancelling,
not-awaiting-retry firing emits `DeliverControl`; no state change, no routing
effect. Everything else is a **logged no-op**, never a `RunError`: a dead or
unknown firing (a late answer must not fail the run), and `Cancel`/`Kill`,
whose own scope-routed events carry closure bookkeeping (`cancelling`, kill
tiers, `run_on_cancel` admission) a raw per-firing path would bypass. The
command-or-no-command result of `apply` is the disposition the driver reports.

**Log v6 + EventSource.** Append-then-apply; every record carries
`EventSource::{External, Core}` (closed enum). Replay feeds back **External
only**; the core must regenerate its own events byte-identically — that
regeneration is the determinism assertion, not redundancy. Arrival order at the
driver's single mpsc is canonical; all wall-clock nondeterminism (completion
order, retry timing, races) is captured in External events.

**Observers.** The driver's `EventObserver` is the host-facing record stream:
every appended record, External and Core, in seq order, exactly once per
driver lifetime — at-least-once across a resume, deduped by `(log identity,
seq)` (`seq` is per-log; the identity is host-named, stable across resume,
fresh per fork) — with the post-apply `EngineState` alongside, so a consumer
resolves a firing to its node, name and `meta` in place
(`EngineState::firing_node`, which searches live firings and history both). A
callback, deliberately not a broadcast channel: broadcast drops on lag, and a
store ingest must never lose a record. `EventObserver::durable(seq)` is the
acknowledgement seam beside it: a step's acknowledged progress send
(`StepCtx::logs.send_acked`) resolves after the driver's append once every
observer confirms its durable storage holds the record, and a store's write
failure is the sender's error; an observer that stores nothing answers at
once. A plain send is queued, ordered by the completion fence ahead of the
attempt's outcome, and not yet durable.

**Public event contract.** An observer sees records; a host projects events.
`execution::events` (`crates/core/execution/EVENTS.md`, versioned) derives one
`RunEvent` stream from coordinator and engine records with the post-apply
state, live and from a finished run dir alike (`replay_run`), with stable
identities `(source log, seq, index)`, parent links across nested executions,
and subjects that carry the node's `meta` and its branch role. Every event is
derived from a durable record; the projector's `observed_at` is the one
live-only field. One vocabulary for records and events: a public event is
named after its record and carries the stored line unchanged under
`record`, with what Petri derived beside it under `derived`, so a host that
stores `record` values stores the logs; `verify_export` (run beside
`verify_replay` at the end of a run) checks that the exported records equal
the stored logs and replay to them. The core's own records are in the
stream with `origin: core`; replay regenerates them. The pure state machine reads no clock: the recording time
comes from the driver, which reads the wall clock once per apply after the
append and hands it to observers (`recorded_at`; the coordinator store stamps
its own records the same way), and the stock run-dir writer persists it in
the `events.jsonl` framing beside the record — never in the core's
`EventRecord`, so replay stays byte-identical — so every `RunEvent` carries
the time its record was appended, live and on replay alike. The other
observed times are the attempt duration the driver fills into
`metrics.duration_ms` when a step kind reported none, and the projector's
`observed_at`. A record read back from the log is the record that was written, floats included (`serde_json` with `float_roundtrip`), so a replayed stream equals the live one event for event.

**Awaited extension points.** A host that must finish work before execution
continues installs `driver::lifecycle::ExecutionHooks`
(`Runtime::hooks`). The order for a completed node is: node admission
(`before_attempt`, once per attempt, so a retry-sensitive hook runs per
attempt; a paused firing keeps its identity, starts no attempt, and a cancel
settles it) → the attempt → the step's own result policy (a Fabro stage
finalizes an exhausted retry inside the step, explicit routes first) → the
node's exhaustion policy (`RetryPolicy::finalize`, applied once by the driver
before the hand-off; the engine records the finish it is given) → result
preparation (`prepare_result`, once per attempt, before the `StepFinished`
record; on the final attempt the host is handed the effective outcome the
engine will record and may change it, and the routes resolved below see the
change; an adjustment keeps the original status and output in a recorded
`result_prepared` note) → the
canonical record → `after_record` → route selection by the decision resolver
→ `transition` (once per completed firing, no-route completions included; an
override is traced as an intervention, a fatal error blocks every group, a
best-effort problem is recorded) → advancement. Each callback runs on its own
task and re-enters the loop through the signal channel; its notes are appended
as `StepProgressRecorded` records ahead of the record they annotate, so they
are durable and replayed. Decisions the callbacks influence are the ordinary
recorded events (`AdmissionDecided`, `StepFinished`, `RoutingResolved`); replay
consumes them and calls nothing; resume reissues only a pending decision under
its `DecisionId` and re-dispatches an attempt whose finish never landed, so a
callback may run twice for one operation across a crash and a host with
external effects deduplicates on that identity. With no hooks installed the
driver's path is unchanged.

**Persistence surface.** The core's whole persistence surface is `EventLog`'s
serde plus `EventLog::try_from_records(version, records)` (version checked
under the standing no-migrator policy; seqs contiguous from 0). How records
are framed and stored is the host's business. Above the core, a run's
durable record goes through the `petri-store` seam (`store::RunStore` opens
a run by key, `store::RunLogs` appends and reads records per log and puts
and gets blobs by digest): the coordinator log, one engine log per
execution and the sandbox resource log are append-only logs of records, and
registered graphs are content-addressed blobs. The stored unit is the
record, `{"seq", "origin", "recorded_at", "body"}`, the same value a public
event carries under `record`; a backend stores it opaquely, and every
decode and version check stays on Petri's side. Two backends ship in tree:
`RunDirStore`, the run directory (`run.json`, `coordinator.jsonl`,
`resources.jsonl`, `graphs/<digest>.json`, one
`executions/<execution>/events.jsonl` per execution, one record per line
and no header), and `MemoryRunStore` for tests. The engine log version is
pinned by the run format version, checked on the run declaration. The
layout is the standalone petri host's own and is documented with it, not
here.

**A failed write ends the lifetime.** When a write to the run's store fails,
in any of its logs, the coordinator's lifetime ends at once, and the next
lifetime resumes from what the store holds. A write can land and still
report a failure, so memory may no longer match the log: every writer of the
run refuses from then on, the lease layer answers no lookup and so calls no
provider, and the driver stops without recording anything more
(`ExecutionReport::store_failure`). No firing fails for the store, and no
hook runs after the failure; the coordinator returns
`CoordinatorError::StoreFailed`. A run-directory handle refuses every write
after its first failed append, and the next writer truncates a partial line
and counts one that landed. Before any attempt starts in a scope, and before
a scope is released, the driver waits for the run log to hold the scope's
`scope.acquired`, so the `scope_released` point of a scope an earlier
lifetime acquired is always found again on resume
(`Driver::observe_run_log`).

**Resume.** `engine::resume(graph, &log)` rebuilds a crashed run by replay and
reconciles what is still owed. The loaded log must be a **byte-prefix** of the
regenerated one — not equal: a crash can land between an External append and
the flush of the Core records it derived, so the regenerated log may be
longer; anything else is real divergence and refuses with `ReplayMismatch`.
Because `apply` is deterministic, every `StartStep` and `ScheduleRetry` is
regenerated byte-identically — nothing about in-flight work needs separate
persistence. `ResumePoint` carries the rebuilt state, the pending commands in
dispatch order (`AcquireScope` per held scope; per live firing the last
`StartStep` — unless awaiting a retry, whose `ScheduleRetry` is re-armed
instead, the sets disjoint because `awaiting_retry()` ⊂ `live_firings()`), and
the re-dispatched firings, which are the host's view: **resume is invisible in
the log** — no marker event, no `LOG_VERSION` bump — so a resumed execution
reaches the same state from the same records and `verify_replay` keeps meaning
something; a host mints new execution identities from `ResumePoint`/
`ResumeInfo`, never from a log event. `EventLog::prefix(len)` is the
rewind/fork helper: resuming a truncated log is rewind, doing it under a fresh
log identity is fork. Observer delivery across a resume is at-least-once,
deduped by `(log identity, seq)` as above.

**ResolvedFiring** is the fully-bound payload of `StartStep` — the "second IR",
scoped to a firing (the graph itself is the **live graph**, mutated by
splices; "HIR-ness" is a per-node property). Constructor invariant, enforced in
`ResolvedFiring::new` and on deserialization (`serde(try_from)`): **no
unresolved expression placeholders; `{"$secret": "NAME"}` is the one permitted
non-literal form** (§11). A placeholder surviving resolution fails the node
with `UnresolvedConfig` naming the path; `placeholder_path` is shared between
plan validation and the boundary check. Commands are not persisted today, so
the invariant is defense-in-depth locally — but it becomes load-bearing when v2
distribution serializes commands to remote agents. The secret test greps the
whole serialized `EngineState`, which is strictly stronger than grepping the
log; keep that form.

## 7. Expression language

Small, **total**: missing fields evaluate to `null`; a guard always yields a
boolean; no user lambdas. Function calls dispatch **through**
`ir::expr::BUILTINS` — a closed, enumerable table that gates dispatch (arity
checked from the entry; a function absent from the table cannot be called even
if a match arm exists; every entry has a conformance test). 19 entries as of
ce8de21; the code is the authoritative list. **Growth bar, documented on the
table:** pure, total, tested, and justified by an acceptance test that cannot be
written without it (how `split`, `sort_by_key`, `pluck` earned entry — and
`matches`, full-`regex` unanchored search, which a frontend condition grammar
with a regex operator cannot lower without). This
table is the target surface for the GHA `${{ }}` grammar (package 03). A
strict/unknown-field-lint mode is a v2 seam.

## 8. Validation

**Load-time invariants:**
1. Every cycle contains ≥ 1 back edge.
2. `Guard::Always` only as a group's final arm.
3. Groups non-empty; empty `Routing.groups` = terminal node.
4. `max_firings ≥ 1`; any node reachable via a back edge has a finite budget.
5. `EdgeId`s unique; `EdgeId::SEED` rejected in routing groups.
6. Expression references resolve; HIR-only fields absent from executable plans.
7. `ExpandTarget::Subgraph{entry, exit}`: exit postdominates entry; no edges
   cross the boundary except into entry / out of exit.
8. **Any node with an incoming back edge has `JoinPolicy::Any`** — forward
   edges carry only generation 0, back edges only ≥ 1; `All`/`Quorum` over both
   is unsatisfiable for every generation. Deliberately strict: `Quorum{1}` is
   rejected too (one canonical spelling; frontends normalize `Quorum{1}` →
   `Any` in lowering rather than relaxing this). Corollary for users: a node
   cannot be both a multi-branch `All` join and a loop head — put a join node
   in front of the loop head. Entry-node seeding checks consider forward edges
   only (an entry node may also be a loop head).
9. `Completion::TerminalNode(id)`: the node must exist. Nothing more — the node
   is *expected* to be terminal, but the semantics only need a final record, so
   terminal shape and reachability rules belong to frontends.
10. **Every join can be satisfied.** A routing group emits at most one token
    each time its node fires, and a node fires at most once per generation, so
    two arms of one group never both deliver to one `(node, generation)`. An
    `All` join may not count two arms of one group: it would wait forever, and
    under `AnyFailure` the run would still report success. Expansion does not
    change this, since every clone copies its groups whole. A `Quorum { n }`
    needs `n` routing groups that can feed it, an entry's seed counting as one;
    the node a `for_each` body exits to is exempt, since each clone adds a
    group at run time. A `for_each` node's own quorum is counted like any
    other; its clones start without it (§5, entry nodes). Loop heads
    (invariant 8 already requires `Any`) and restart targets (a successor
    execution enters them directly, without the join) are exempt. A splice
    fragment is checked for `All` only: attachment adds groups, so its nodes'
    fan-in is known when the fragment applies.

**Lint (warning, not error):** possible scope re-entry after release — a node
outside a scope both reachable from it and reaching back into it. Suppressed
when every re-entry node's join is `All` with ≥ 1 incoming forward edge from
inside the scope (sound: that arm either pins the scope via a pending token or
renders the join unsatisfiable). The suppression's `!back` filter is
unreachable given invariant 8 and is kept as documented defense. Remaining
positives are a labelled over-approximation; the warning names the re-entry
node and the fix.

**Lint (warning, not error):** a join across generations
(`lint.join_across_generations`) — an `All` join, or a `Quorum` that needs
both sides, over an edge from a node a loop reaches and an edge from a node
no loop reaches. A token keeps the generation its loop gave it (§4), so the
join fires only when the loop exits in generation 0; after one iteration it
waits forever, and under `AnyFailure` the run still succeeds. A warning, not
an error, because the graph works for a loop that does not iterate and
nothing else can express the wait yet (§14). Not exact: it also fires for a
loop that never iterates, and it misses a join between two loops that exit in
different generations.

**Firing-time errors are node failures, routable, never run aborts:**
`UnresolvedConfig`, `secret_misplaced`, `bad_output_file`, `env_acquire`.

## 9. Scopes and environments

A scope is a resource scope (workspace, container, env, secrets), not a
sequence. **Held, not refcounted:** acquired before the first firing needs it,
released once no firing, pending token, or deferred join needs it (per-firing
refcounting tears a job down between consecutive steps — rejected). **Release
is irreversible;** re-entry acquires a fresh environment (the §8 lint is the
static counterpart). Acquire failure fails every pending firing in the scope
instance with class `env_acquire` via ordinary `StepFinished` events.

Scope environment expressions read graph parameters and the execution's initial
context (`EngineStart.context` as `kv`). Later context updates and firing-local
values do not change the scope environment.

**`acquire` fences prior work.** A driver crash kills no running step (release
owns cleanup; remote sandboxes outlive workers by design), so the `Executor`
contract carries one more rule: when `acquire` returns, no process from a
previous acquisition of that scope can still mutate the workspace or be
observed as this environment's status. The Host plugin implements it with
`host-registry/<resource>/groups/<generation>/` records under the run directory
and a publication handshake
(the sentinel durably records its pgid, then checks a `fenced` marker, only
then spawns the workload; the fencer writes markers before reading records) —
and while anything of a discovered group lives, the group's own in-group
watcher is the killer; the fencer only waits for it to drain. **Nothing ever
signals a bare recorded pgid** — it can be recycled to an innocent — so a group
that never drains fails the acquire with the typed `FenceLeaked` error, and the
scope's firings fail routably; cleanup belongs to the operator or host policy
(an identity-bound kill via Linux `pidfd` is an optional platform upgrade,
never a requirement). **Sandbox scopes are keyed by lease, not by
environment.** A sandbox lives as long as the durable sandbox lease
that names its workspace (`SandboxLeaseId`, allocated by the coordinator per
`{invocation, scope}`); `EnvironmentId` is one execution's in-process handle on
that sandbox and names no container. The lease manager keeps one live handle
and a holder count per lease: a second acquire on a live lease — a restarted
execution, a nested invocation with an `Inherited { lease }` binding, two
concurrent scopes over one inherited workspace — reuses the handle and never
stops the sandbox. Only crash recovery, with no live holder, fences: it lists
the provider by `petri.workspace=<run id>/<workspace id>`, attaches the one
recorded match, stops it once (ending whatever a dead execution or a dead
plugin generation left running, one-shot containers included) and starts it
once before any holder resumes; more than one match is an `env_acquire`
error, never an arbitrary choice. A fresh create happens only when
reconciliation finds no match. The run id is the run's store key, carried
by the run declaration and handed to every executor of the run, so any
executor over the same run computes the same labels; a driver with no
coordinator names its run after its run directory. A provider is reached one
of two ways. By default every provider, Host, Docker, and Daytona included, is
a sandbox-driver JSON-RPC plugin: a plugin process that dies fails every
in-flight call routably, is never asked to replay an ambiguous call, and is
relaunched single-flight by the next call, and a generation change forces the
recovery fence before any holder resumes. An embedder that links the built-in
providers instead hands the runtime a factory per kind
(`Runtime::in_process_providers`); the router then reaches those providers in
its own process, launches no plugin (a kind without a factory fails at
acquire), connects each one once per run and checks its health before any lease
uses it. Both ways record the same fingerprint for the same backend, so a lease
recorded one way is recovered or pruned the other. A plugin of a kind Petri does
not know can be registered beside them (`ProviderRegistrations`), with the
`SandboxSpec` template its ordinary process scopes are built from; its plugin
must report a resource identity, and its fingerprint is `<kind>:<identity>`.
The router keeps one executor per provider kind, and resume, release, and
prune reach each lease through the kind its record names. Finishing a run closes its
admission to every environment it handed out, so a retained environment
refuses work afterwards whichever way its provider is reached. The
durable resource record (one line per transition in the run's resource log,
the latest per lease current) is the crash-safe authority:
it carries the allocation state (`allocating`, `live`, `stopped`, `deleted`),
a pending intent (`stop`, `delete`) written before the provider call and
cleared only after it succeeds, the real provider kind and resource id, and a
non-secret provider fingerprint that recovery, release and prune validate
before acting. A confirmed record whose resource is missing is `env_acquire`,
not a silent replacement — its workspace was lost. The fence is idempotent and
covers the workspace only: side effects outside it may have happened in the
crashed attempt and happen again — resume is **at-least-once for external side
effects**, exactly-once only for the log and the workspace fence.

Host provider: one sandbox per lease and a workspace under the run directory;
retention defaults to keep-on-failure (`always|on_failure|never`). Petri passes
its private registry directory to the plugin, or to the in-process factory,
and includes the canonical path in the provider fingerprint. The provider owns the registry, process groups,
and workspace lifecycle. Petri explicitly marks its workspace as managed;
other callers' designated directories remain untouched by delete. Stopped
sandboxes can be attached and pruned after a plugin restart. Successful stop
removes drained generation records. Container executor
(`executor-sandbox` over the Docker or Daytona provider, a plugin or in process): one sandbox
per lease — pull if-not-present, an init process, **the workspace inside the
sandbox** (Docker: a volume the sandbox owns at `/workspace`; nothing is bound
from Petri's machine, so a remote daemon works and file I/O goes through the
provider's filesystem facet), a long-lived POSIX init; steps go to the
provider's exec as a program plus arguments, which the provider runs directly
under a `/bin/sh` wrapper, so no shell interprets them and an image without
bash (alpine) works; one-shot action containers are the provider's own
operation over the same workspace and network namespace. Per-execution
release ends that execution's execs and drops its holder; the sandbox is
stopped when the last holder is gone and its invocation releases the lease,
and `Retention` then keeps the stopped sandbox (its workspace with it) or
deletes it. `petri sandbox prune` deletes kept sandboxes later, through the
same pending-delete record state, and leaves a `deleted` tombstone while the
run directory exists. Image contract: must provide `/bin/sh`, `env`, and
`setsid` (busybox/util-linux both do).
Backend selection belongs to `RunOptions.sandbox`, separate from a graph's
runtime target. Docker maps process targets to a pinned runner container.
Daytona maps a process target to a runner VM and a container target to a job
container inside that VM. The VM owns the job, sidecars, and one-shot action
containers; stopping it ends all of them. The VM workspace is shared with
nested containers inside the VM, with no bind from Petri's machine. Runner
snapshot names include the selected image and resource allocation. Concurrent
scopes share preparation; later runs reuse the named snapshot. Petri disables
automatic VM lifecycle timers. Lease release keeps or deletes the VM, while
shared runner snapshots remain available. A stopped VM can be attached without
contacting its Docker daemon; start refreshes the private preview connection.
A provider without a route to Petri reports `host_unreachable`; actions omit
ObjectService variables until an advertised address is configured.

Services require a containerized job: declared `services` become sidecar
containers on a per-scope network reached by alias, and a bare host process
that declares services fails at acquire with `env_acquire`, the message naming
the fix. Container and service options are typed in the graph
(`ir::ContainerOptions`, `ir::ServiceOptions`: env, user, DNS, added
capabilities, privilege; the job container's platform; a service's entrypoint
and health check). A frontend lowers its format's flags into them and rejects
a flag with no typed mapping at lowering, naming it, so an executor never
sees an option it cannot honor. A service has no port publications: it is
reached by its name on the scope's network. Parallel steps sharing a workspace: declared file
conflicts or isolated overlays remain future work; v1 native format should not
encourage intra-scope parallel writes to the same paths.

## 10. Driver, process StepKind, cancellation

**Layering:** Driver owns IO scheduling and translates (no policy); Executor
owns environments (`acquire`/`release` + the `ExecEnv` spawn capability handed
to steps); StepKind owns step semantics and never mentions host-vs-Docker.

**Capabilities.** `StepCtx` carries a typed, host-registered capability map
(`Capabilities`, in the `steps` crate): components define concrete handle
types and register values — on the `Runtime` builder or per run via
`Driver::with_capabilities` — and a step asks by type; **the core never names
a capability**. The key is the concrete type (`Arc<dyn Any>` downcasts only to
sized types), so a `dyn`-trait service rides behind a concrete newtype.
Duplicate registration panics, like step registration. `require_capability`
fails routably with class `capability_unavailable` (§13), mirroring
`secret_unavailable`. Capabilities live entirely on the effects side: nothing
touches the engine, the log, or determinism.

**Driver rules:** single command consumer, single External-event producer;
per-attempt timeout timers (expiry → cancellation ladder → `TimedOut`); retry
jitter + sleep → `RetryElapsed`; hard deadline after `Control::Cancel` of
`grace + 5s`, then task abort and synthesized `Cancelled` with
`cancel_escalation: "cancel_forced"`. `release` never fails the run. Observers
are notified after every apply, and the driver awaits every observer's
`finish` before building the report; observer failures surface in
`RunReport.observer_errors` and never change the run status (a host with
fatal-sink semantics watches its own observer and cancels via `RunHandle`).
The stock petri host persists a post-mask run dir through an observer battery;
details live with that host, not here.

**Determinism.** Under a simulated clock the driver is a function of its
inputs, so a seeded simulation replays a run exactly. Its `select!`s are
biased: the main loop takes abandoned scope handles, then background task
exits, both bounded, then signals; a firing's log forwarder closes before it
receives, and closing still hands back every queued event. Its state maps are
ordered. Every duration it records, such as `scope.acquired`'s
`duration_ms`, reads the runtime's clock, and the time it stamps on observed
records comes from `RunConfig.recording_clock`, the wall clock by default. Its
one draw, for a weighted routing tier, comes from the operating system in
`DefaultDecisionResolver` and from a seed in `SeededDecisionResolver`; the
draw is in the log either way, so replay never needs it. Step output goes
to `RunConfig.step_logs`, a `StepLogStore`: the run directory's `logs/` by
default, memory in a simulation.

The coordinator above it is a function of its inputs the same way. Its loop
is biased too: the host's commands (cancel, pause, control), then finished
drivers and lease releases, then fork admissions, then start requests, so new
work comes last. The runtime passes one recording clock to every driver of a
run and to the coordinator and resource logs (`Runtime::recording_clock`),
one step-log store per execution directory (`Runtime::step_logs`), and a seed
for every execution's weighted draws (`Runtime::decision_seed`, mixed with
the execution's id). With a fixed `RunOptions::run_key`, a seeded run stores
the same logs byte for byte.

**Timeout accounting.** `Budget.timeout_policy` says who enforces the
per-attempt timeout. `ExecutorEnforced` (the default): the driver arms the
timer at dispatch and it counts **active work only**. A `Question` on the
firing's progress channel starts an interaction wait for that firing and
attempt and pauses the timer; the delivered `Answer` naming that question, or
the step's own `QuestionExpired` report of it, ends the wait, and the timer
resumes with the remaining time once no question of the attempt is pending.
Overlapping questions from one attempt are one pause; an answer to a question
the attempt never asked, or asked and already had answered, is stale and
changes nothing. Each arming has its own id, so an
expiry queued by a timer that was paused or re-armed since is ignored. A
sibling firing's wait never touches another firing's timer. `HandlerManaged`:
the driver arms no timer; the step consumes `timeout` itself (a process
deadline the executor enforces through `ProcessSpec.timeout`, reported as
`ExitStatus.timed_out`; an agent's own turn deadline; a human gate's answer
deadline, which the gate reports as a `QuestionExpired` progress event before
it acts on it). Both kinds keep cancellation, kill and the hard deadline. Resume
re-dispatches an attempt with a fresh budget, and the redispatched attempt's
own question pauses that budget before any of it is charged, exactly as a
live attempt's does. Wait starts and ends are reported through `tracing`
(`interaction wait started` / `ended`, by firing, attempt and question id);
the durable record is the `StepProgressRecorded` question and, ending it,
the `ControlRequested` answer or the `StepProgressRecorded` expiry in the
log. The
run-wide stall watchdog and the failure circuit breaker (`Graph.policy`) are
host policies over the observer stream and the routing middleware; neither is
a timer of the driver's.

**Delivery.** A `DeliverControl{Deliver}` only forwards to the firing's control
channel — no deadline, no reason: a delivered value never starts the
cancellation ladder or the kill tier. Delivery is **reliable, not `try_send`**:
every control send rides a per-firing serialized forwarder that awaits channel
capacity, so a full channel never blocks the driver loop and sends land in
order (channel capacity is `CONTROL_CHANNEL_CAPACITY = 32`, a named
implementation constant, not a compatibility rule). `RunHandle::deliver`
returns a disposition the forwarder completes: `Delivered` only after the send
lands; `NotLive` when `apply` emitted no command or the firing ended first.
Best-effort steering drop semantics live in the host hub, on top of this
reliable primitive.

**Resume rules.** `Driver::resume(graph, log, …)` is the primary API — the
host hands in the graph and log however it stored them — and returns
`ResumeInfo` beside the driver so effect identities are installed before
`run()`. On the resume path `run()` skips `ExecutionStarted`, notifies every
observer of the regenerated suffix **before** dispatching any pending command,
then enters the normal loop. Dispatch differences: a firing whose `started`
flag is set gets no second `StepStarted` ack (it is already in the log); a
firing marked `cancelling` — either tier — is **never re-spawned**: the driver
finishes it directly with a `Cancelled` outcome carrying the per-tier
`cancel_escalation` value (`cancelled_before_resume` / `killed_before_resume`;
data values, not new vocabulary), the tier read from the replayed state, the
polite outcome routing exactly as a live cancel's would while `run_on_cancel`
cleanup firings re-dispatch normally. Timers restart in full — the log has no
clock, so a pending retry waits its whole base delay again and per-attempt
timeouts start fresh. **No automatic re-delivery:** a logged
`ControlRequested{Deliver}` is not re-forwarded — the resumed step waits again
and the host re-sends what its own store says is outstanding, re-registering
dynamic secrets (`answer:<id>`) first or the delivery fails the step with
`secret_unavailable`. External side effects are at-least-once across a resume
(§9).

**Two-tier stop wiring.** The driver never decides to stop the run by itself;
it feeds events and the core decides. The first root `cancel` feeds
`CancelRequested { ROOT }` and arms the cleanup-grace timer
(`RunConfig.cleanup_grace`, default 2 minutes, per-run override). Timer expiry,
or a second root `cancel` (a CLI maps a second Ctrl-C to it), feeds
`KillRequested { ROOT }`. Both are ordinary External events, so the hard stop
is in the log and replay reproduces it. After a Kill the core emits no further
`StartStep`s; the driver's `DeliverControl { Kill }` reaches every live task —
including ones already politely cancelling — and arms a zero-slack hard
deadline as the backstop for a step kind that ignores it.

For every plugin exec and one-shot, the plugin registers stop tokens before
opening the data channel. Petri forwards cancellation only after that channel
is accepted, so an immediate TERM or KILL cannot overtake exec registration.

**Process StepKind:** `bash -eo pipefail -c <run>` (or `sh`); env may contain
secret refs; no step-level timeout (node budget governs). Outcome mapping: 0 →
`Success`; N≠0 with `soft_fail` match → `PartialSuccess{underlying:
exit_status:N}`; N≠0 → `Failure{exit_status:N}`; foreign signal →
`Failure{signal:S}`; our ladder → `Cancelled`/`TimedOut`. Outputs-file
protocol: `CI_OUTPUT` env points to a per-firing file; `key=value` + GHA
heredoc form parsed into `Outcome.output` (plus `output.exit_status`); parse
failure = `bad_output_file`. Log capture: **one merged, arrival-ordered,
stream-tagged `lines()` stream** (two receivers cannot recover an order never
recorded); 64 KiB line cap with truncation marker; capture drains through
cancellation until both streams close.

**Cancellation (one ladder for cancel and timeout):** TERM to the **process
group** → grace (default 10s, per-scope) → KILL to the group. `Control::Kill`
skips the ladder: straight to KILL, no grace. Host: each spawn starts the group
with a **sentinel** supervisor — the group leader, which runs the workload as a
member of the same group, reports its exit status out of band, closes its
inherited copies of the stdout/stderr pipes after the spawn (so the pipes reach
EOF when the workload exits), ignores `SIGTERM` so the polite ladder passes
through it, and stays alive until the sandbox lease stops. While the sentinel lives the
group is never empty, so the kernel cannot recycle the pgid; the Host provider owns
the sentinel's unreaped handle, so even a killed sentinel pins the id as a
zombie. Sandbox stop sends one `killpg(SIGKILL)` **while the id is still pinned**,
reaps the sentinel, then performs only **non-signalling** bounded observation
of group death (procfs on Linux, libproc on macOS) — never a signal after the
reap frees the id, and never an `ESRCH` probe, which a zombie leader defeats. A
group that outlives the deadline is a report entry, and no zombie outlives
a successful stop. All other signalling stays `killpg` only. Docker: `docker kill` reaches PID 1 only —
step-level signalling is `docker exec <c> kill -TERM -<PGID>` (**no `--`
separator**: busybox `kill` rejects it and a rejected signal is a silent one;
this corrects the original handoff text). The in-container wrapper records the
step's exit status beside its pgid via **atomic write (temp + rename)**;
`wait` follows **a recorded status or group death, whichever first** — never
the exec client's return (setsid may fork; a trapping step outlives the
client). Liveness polling interval is the documented `LIVENESS_POLL` constant.
Races: exactly one terminal `StepFinished` per firing; first terminal wins
(natural exit vs ladder; timeout vs cancel by driver arrival order — the log
then makes it canonical, so replay agrees whichever way it landed).
Post-conditions: streams drained, outputs file still parsed if present,
release still runs, nothing leaked (no workspace, container, or process
group). Known limitation: host-executor double-fork daemons escape the group;
Docker's PID namespace is the mitigation.

## 11. Secrets

Secret **values** never enter the event log or serialized state. Config carries
`{"$secret": "NAME"}` through `ResolvedFiring`; resolution happens at
spawn-time via `SecretProvider` directly into the child env. `$secret` is valid
only in env-shaped positions (`secret_misplaced` otherwise); secrets are absent
from `EvalEnv` — guards cannot read them by construction. Masking (exact-match
→ `***`, min length 6, multiline masked per line) applies before append to:
`StepEvent::Log` lines, `StepEvent::Artifact` names and uris, and all string
values in `StepEvent::Custom`, `Outcome.output` and `context_updates`. Encoded
variants (base64/urlencoded) are a documented v2 gap.

`Graph.params` are recorded data: a raw secret value never belongs in them —
the graph is persisted as it ran (replay needs it byte-exact, so a masked copy
would be a different graph), and this contract is what makes that safe. The
standalone petri host adds a checkable backstop: if the masker already
recognizes a value in the serialized graph, it refuses to start the run rather
than persist. Exact-value masking of registered values is the driver's job;
pattern-based redaction beyond registered values is the **emitting step's**
job before it sends — a product's redaction policy does not belong in the
core.

A splice payload is never masked: masking a fragment would change the
executable plan, so the driver's backstop (§5.3) fails the firing and clears
`Outcome.splices` before the append instead.

A sensitive `Deliver` payload crosses the same way: `{"$secret": "answer:<id>"}`
in the event (the log keeps the reference), resolved by the driver at
command-dispatch time into the step. `SecretProvider::register(name, value)`
lets the host add a dynamic value after run start (default: a typed unsupported
error); duplicate names are rejected so an answer id can never shadow a
configured secret, and registration feeds the masker, so anything resolvable is
maskable by construction — the lifetime is the provider instance, i.e. the run.
Dynamic values are not in the log by design, so after a crash the host must
re-provide them on resume; an unresolvable reference at dispatch fails the step
with `secret_unavailable`.

## 12. Frontend lowering contracts

**GHA** (exercises the degenerate subset — no back edges, no `Any`/`Quorum`,
no multi-arm groups): job → Scope + chained step nodes; `needs` → `All` join;
k dependents → k single-arm groups; job `if:` → precondition on the job's
`start` node (the engine plays GitHub's server: no secrets, no workspace, no
env files); step-level conditions (`if:`, `pre-if`, `post-if`) → a **gate** in
the step's config — an expression tree whose engine-evaluable subtrees ride as
`$expr` placeholders and whose `env.*` / `hashFiles` leaves the step kind
resolves at spawn, returning `Skipped` (or `Cancelled` under a cancelled
scope) when it is false. Step nodes carry no precondition and set
`run_on_cancel` across the board (matrix expansion heads excepted), so
post-cancel behavior is decided by evaluating conditions, not by inspecting
their text; `strategy.matrix`
(+`fail-fast`/`max-parallel`) → `ForEach{Subgraph}`; `timeout-minutes` →
budget; `continue-on-error` → `soft_fail` (context provider derives
`outcome`/`conclusion` from `PartialSuccess.underlying`); `runs-on` →
`RuntimeSpec.requirements` (executor maps known labels, rejects unknown
per-label); composite actions inline. **Rejected loudly, or ignored loudly —
never parse-and-drop** (the shared `Unsupported{feature, hint}` diagnostic, or
an `ignored.*` warning naming what was dropped): GHA `concurrency:` and
`permissions:` are ignored with a warning — one local run has nothing to race,
and cross-run semantics stay with the driver layer (D2); BuildKite
`concurrency_group` (D2) and anything else unimplemented are rejected.
Native format: `next:` → one group; `parallel:` → multiple
groups; `for_each` + `parallel: true|false` → `ForEach` vs cycle desugar.

**Attractor** (`crates/attractor/FORMAT.md`; Fabro's settings layer is
`crates/fabro/FORMAT.md`; exercises the whole engine): every node's
edges → one `Tiered` routing group with Fabro's four tiers (conditions;
preferred label; suggested targets ranked by index; the fallback guarded by the
`on_failure` / `on_retries_exhausted` policy), `Fallthrough::NoEmit`; start →
entry noop; exit → `Completion::TerminalNode`; `goal_gate` → a `goal_check`
noop before exit with back arms to the retry-target chain; DFS-classified
back edges, `Any` joins, `Budget.max_firings` capped at 500;
`loop_restart` → `EdgeTransition::Restart`; `component` → the `attractor/fork`
step (it takes the fork snapshot of `kv` once and offloads what a fan-out
would copy per child to the output store) whose branches are synthetic
`attractor/branch` delegates (`kind = "parallel.branch"`), each running a copy of
its target as a child invocation from that snapshot with no merge back,
`Expansion::ForEach` over the delegate for `for_each`;
`tripleoctagon` → the `attractor/fan_in` step behind an `All` join, publishing
`parallel.results` and `parallel.branch_count` with the ordered branch
envelopes as output; `max_parallel` → `AttemptAdmission` on the child
invocations, one gate per parent execution and fork visit, one slot per live
child, taken before the child's driver starts (in declaration order), held
until the child's end is recorded and handed back during a retry backoff;
`RunPolicy.max_invocations = 10,000` → the coordinator's run-wide
invocation ceiling, checked at create, resume and every declaration; `house`
→ a nested invocation by graph digest; `tab` and a prompted `tripleoctagon` → the `attractor/prompt` step;
`import` → expanded at load, so the persisted graph carries the imported
nodes; `[run.prepare]` → command nodes between start and its successors.
Conditions lower onto the expression language with Fabro's text comparison,
truthiness and numeric rules spelled out; `outcome=X` names only the four
Fabro outcomes. The legacy dialect's attributes and unknown outcome values are
specific `unsupported.*` rejections. Failure promotion is Fabro's: the step checks the
node's explicit routes (carried in its config) against the failed outcome and
the prospective context before it classifies, so an `outcome=failed` edge on a
`succeed` node is taken and only an unmatched failure becomes a
`PartialSuccess` that reports `succeeded`. The classification still happens
once, at the step boundary, and the core's merge rules are unchanged.

## 13. Failure-class registry (grep anchor; extend here first)

**Step outcome classes** (`FailureInfo.class`, matchable by `retry_on`):

| Class | Raised when |
|---|---|
| `exit_status:N` | the process exited non-zero |
| `signal:S` | the process was killed by a signal that was not ours |
| `retry_requested` | a step asked to be run again (Attractor's `RETRY`) |
| `bad_output_file` | the outputs file did not parse |
| `bad_config` | the step's config did not deserialize |
| `secret_misplaced` | a `$secret` ref outside an env-shaped position |
| `secret_unavailable` | a named secret is not configured for this run |
| `workspace_setup` | the workspace could not be prepared for the step |
| `spawn_failed` | the process could not be started at all |
| `env_acquire` | the scope's environment could not be materialized |
| `no_runner` | no step kind is registered for a node's `StepRef.kind` |
| `capability_unavailable` | a step required a host capability no one registered (§10) |
| `invalid_splice` | a splice transaction was rejected: policy, validation, or composition (§5.2) — also the driver's splice-payload secret backstop (§5.3) |

Not classes, listed here so they are findable: `cancel_forced` is a value of
`output.cancel_escalation` (§3.1 rule 5); `UnresolvedConfig` is an error type that
surfaces as a node failure.

Names say what went wrong, so a `retry_on` entry reads as a policy rather than a
riddle — this is why `spawn` and `workspace` became `spawn_failed` and
`workspace_setup`. `no_runner` is caught at load: the one step registry
implements the lookup `validate_with` takes, so a node naming a kind with no
runner is a validation error, and the driver's firing-time guard is the backstop
for a caller that skipped validation.

## 14. Deferred and v2 seams (build nothing here)

Outcome-driven splice shipped (§5.1–5.3); still deferred from it: the
BuildKite component itself, commit-at-call (`SpliceRequested`), referencing an
existing resource scope by stable identity, and a generated batch barrier for
`Any`/`Quorum` dependents. Cross-run concurrency: driver-layer service
(D2); eventual `concurrency_key` is additive. Placement semantics beyond
opaque labels (D3). `Control::Pause` (enum is `#[non_exhaustive]`); `Steer` and
`Approve` shipped as `Control::Deliver` (§6, §10). Strict expression mode. Encoded-secret masking. Content
caching (`StepKind::fingerprint` defaults `None`). JS action host; action
shims are package 04. Windows; service containers; resource limits.

**Joining after a loop (open design question).** Nothing resets a generation:
a token leaving a loop keeps the loop's generation, and a join matches one
generation, so no graph can wait for a loop and a path that skips it once the
loop has iterated (`lint.join_across_generations`, §8). Closing the gap needs
a way to leave a loop at a known generation — for example an exit edge that
resets the generation, or a join that matches its inputs across generations —
and either changes the core's token rules. Undecided. As of the lint's
introduction, no Fabro or GitHub Actions corpus workflow has the shape.

## 15. Testing notes (institutional memory)

- **Trace green tests to their assertion.** Both major Docker bugs hid behind
  tests that could not fail (group-kill "passing" because release removed the
  container; graceful-exit reported because the exec client returned early).
  When a test guards a signal path, assert the *mechanism* (escalation level,
  who ended the process), not just the end state.
- **Assert the duration, not just the result.** Elapsed time was the tell that
  `wait` was following the client instead of the step.
- **Determinism canaries:** replay byte-identity on every E2E; the raced tests
  run repeatedly and assert replay agrees whichever way the race lands.
- Acceptance batteries live with their packages (core §7 tests 1–5; executor
  §7 tests 1–10) and are named for the rules they pin.
