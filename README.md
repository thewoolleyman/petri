# Engine IR

A Rust implementation of [`engine-spec.md`](engine-spec.md): a token-flow graph with
explicit routing, plus the pure state machine that executes it.

```
crates/core/ir         the vocabulary: graph, ids, expressions, values, validation
crates/core/store      the run store seam: logs and blobs by key; the run directory and in-memory backends
crates/core/engine     the sans-IO state machine: apply(state, event) -> (state, commands)
crates/core/executor   the environment interface; executor-sandbox implements it
crates/core/executor-sandbox  scopes and leases over Host, Docker, and Daytona plugins
crates/core/steps      step kinds and the one registry; frontends depend on names, not on this
crates/core/driver     the IO loop between the pure core and real processes
crates/core/execution  run, invocation, and execution coordination over the store seam
crates/core/frontend   what every format shares; frontend-native is core's own format
crates/core/runtime    core assembled: the `Runtime` builder components register onto
crates/core/cli        the command line, format-agnostic; the shipped binary hands it a runtime
crates/core/testkit    the scaffolding the end-to-end batteries share
crates/github/frontend   the GitHub Actions frontend: lowers workflow files to the IR
crates/github/actions    the GitHub Actions step kinds: run, action, checkout
crates/github/objects    the ObjectService: cache, artifacts, and tool-cache storage
crates/github/acceptance the acceptance battery: corpus harness and end-to-end runs
crates/attractor/frontend    the Attractor frontend: lowers Graphviz DOT workflows to the IR; reads no settings file
crates/attractor/steps       the Attractor step kinds: command, wait, human, agent over ACP or native Pebble, prompt, nested workflow, the stage step, the local hook service, MCP servers as Pebble tool sources (`pebble::mcp`), fidelity preambles, retained threads, project memory, Fabro's skill directories, context compaction, the output store, and the stub registry
crates/fabro/frontend    the Fabro frontend: Fabro's settings layers (`workflow.toml`, `.fabro/project.toml`, the user settings) and launch precedence around the Attractor frontend
crates/fabro/acceptance  the Fabro battery: corpus harness, runs under stubs, the Fabro oracle, real steps, the compatibility contract and bundle manifest
crates/petri/lib       the distribution: core plus every component
crates/petri/cli       the shipped `petri` binary: the distribution handed to core's CLI
```

Packages are named `petri-*` and arrows point down. Core never depends on a component; components
depend on core and never on each other, with one declared layer: `fabro` sits on `attractor`, since
the Fabro frontend wraps the Attractor frontend; only the distribution names them all.
`crates/petri/lib/tests/layering.rs` enforces this from `cargo metadata`. The next
format (CircleCI, RWX) is a `crates/<component>/` directory and one
registration in the distribution, as `crates/fabro/` is.

A node fires when its **join policy** is satisfied by incoming tokens. On completion
its **routing policy** emits tokens on outgoing edges. Routing is an AND of XORs:
each select group emits at most one token, and groups emit concurrently. The default
is selection, so fan-out is never implicit — it takes writing more than one group.

```rust
use engine::{EngineStart, EngineState, Event, apply};

let mut state = EngineState::new(graph);
let (state, commands) = apply(state, Event::ExecutionStarted(EngineStart::default()));
// run the commands, feed the results back as events
```

## Where the design lands in the code

| Design section | Code |
|---|---|
| §2 routing, AND-of-XOR | `ir::graph::{Routing, RoutingGroup, SelectionPolicy, Guard, Fallthrough}` |
| §3 core types | `ir::graph`, `ir::ids` |
| §3 expressions | `ir::expr` — `Expr`, `ExprTable`, `Context`, `eval` |
| §4 runtime types | `ir::flow` — `Token`, `Outcome`, `Status`, `Metrics` |
| §4 firing rule | `engine::apply::try_fire` |
| §4 quiescence | `EngineState::is_quiescent`, `apply::finish_if_quiescent` |
| §5 engine interface | `engine::event` — `Event`, `Command`; `engine::apply::apply` |
| §5 event log | `engine::log` — v9, with durable admission and routing decisions and a recording time beside every persisted record |
| §5 replay | `engine::replay` — `verify_replay` is the determinism canary |
| §4 retries | `ir::RetryPolicy`, `engine::apply::on_retry_elapsed` |
| §4 run context | `ir::RunContext`, `engine::state::EngineState::record_outcome` |
| §4 expression environment | `ir::EvalEnv` — one way to see upstream state |
| exec §1 layering | `executor::Executor` / `ExecEnv`, `steps::StepRunner`, `driver::Driver` |
| exec §2 driver loop | `driver::run` — one channel, so arrival order is the total order |
| exec §3 process step | `steps::process`, `steps::outputs` |
| exec §4 cancellation | `steps::process::ladder`, `driver::Driver::on_hard_deadline` |
| exec §5 environments | `executor::scope` (the interface), `executor_sandbox::{HostExecutor, SandboxExecutor, RoutingExecutor}` |
| exec §6 secrets | `executor::secrets`, `driver::LogSink` |
| execution hierarchy | `execution::{Coordinator, InvocationId, ExecutionId, InvocationClient}` |
| standalone persistence | `store::{RunStore, RunLogs}` behind `execution::{CoordinatorStore, ResourceStore, StoreWriter}`; `store::RunDirStore` is the run directory, one engine log per execution |
| §5a cancel scopes | `engine::state::CancelScope`, `apply::on_cancel`, `apply::on_kill` |
| §6 HIR → plan lowering | `engine::context::resolve_config`, `apply::expand` |
| §6 splice semantics | `engine::event::SubgraphSplice`, `apply::on_node_expanded` |
| §6a sequential `for_each` | `ir::desugar::sequential_for_each` — no new IR, just a cycle |
| GHA frontend mapping | `crates/core/engine/tests/gha.rs` |
| §7 validation invariants | `ir::validate` — `check` for errors and warnings, `validate` for errors alone |
| §8 reserved seams | `StepKind::fingerprint`, `Control`, `Command::{Acquire,Release}Scope`, `EventLog::version` |

## Reading the tests

The test suites are the executable form of the design document.

```
crates/core/engine/tests/routing.rs          §2 selection, fan-out, OR-split, guards, preconditions
crates/core/engine/tests/joins.rs            §3 All / Any / Quorum, generations, entry seeding
crates/core/engine/tests/loops.rs            §6a sequential for_each, back edges, firing budgets
crates/core/engine/tests/expansion.rs        §6 parallel for_each, collectors, max_parallel, fail_fast
crates/core/engine/tests/cancellation.rs     §5a cancel scopes, both tiers: routing, run_on_cancel, kill
crates/core/engine/tests/scopes.rs           §3 resource scopes, env, acquire/release
crates/core/engine/tests/gha.rs              the GHA mapping table, end to end
crates/core/engine/tests/retries.rs          handoff §1 attempts, backoff, exhaustion
crates/core/engine/tests/run_context.rs      handoff §2 nodes.* / kv.*, goal gates, merge order
crates/core/engine/tests/partial_success.rs  handoff §4 soft failure, is_success_like
crates/core/engine/tests/seeding.rs          seed edges for entry nodes and clone entries
crates/core/engine/tests/resolved_firing.rs  the executor boundary: no unresolved ExprId crosses it
crates/core/engine/tests/event_log.rs        §5 logging, determinism, serde round-trip, §8 seams
crates/core/engine/tests/flow_properties.rs  §3–§5 over random flows: joins, generations, budgets, retries, cancel and kill, replay
crates/core/engine/tests/resume_properties.rs  §6 resume over random flows: a log cut anywhere resumes to the live state, owes what was outstanding, and goes on to the same log
crates/core/engine/tests/lean_model.rs       the core against the Lean model in `lean/`: flows and `deterministic_pick` (`mise run test:lean`)
crates/core/ir/tests/validation.rs           §7, invariant by invariant
crates/core/ir/tests/expressions.rs          the expression language

crates/core/driver/tests/e2e.rs           exec §7 1-2: native loop and GHA-shaped, real processes
crates/core/driver/tests/cancellation.rs  exec §7 3,4,5,9: the ladder and the hard deadline
crates/core/driver/tests/kill.rs          §10 two-tier stop: cleanup after cancel, cleanup grace, KillRequested
crates/core/driver/tests/release.rs       §10 sentinel-pinned groups: release kills, reaps, then only observes
crates/core/driver/tests/timeout.rs       exec §7 6: timeouts, and the race under replay
crates/core/driver/tests/environments.rs  exec §7 7,10: acquire failure and retention
crates/core/driver/tests/secrets.rs       exec §7 8: masking, and what reaches the log
crates/core/driver/tests/docker.rs        exec §7 3,10 Docker halves; skipped without a daemon
crates/core/driver/tests/determinism.rs   §10 determinism: a seeded run on a simulated clock replays byte for byte
crates/core/driver/tests/simulation.rs    §10 simulation: seeded crashes, stops and faults against a simulated sandbox world
crates/core/executor-sandbox/tests/docker_backend.rs   the sandbox executor over the Docker plugin: the workspace in the sandbox, exit codes and signals, files over the wire, the crash fence, retention, services, one-shot actions
crates/core/executor-sandbox/tests/daytona_backend.rs  the same executor over the Daytona plugin, live (`mise run test:daytona`, `PETRI_REQUIRE_DAYTONA`): the runner VM, a nested container job, files, the fence, retention, output after idle and under a burst, a preview URL, the failure modes; `DAYTONA.md` maps Fabro's former live suite onto it
crates/petri/lib/tests/daytona.rs             the standalone host on `--backend daytona`, live: retention and `prune` with the tombstone, `resume` fencing the crashed VM

crates/core/frontend/tests/expr_grammar.rs   frontend §7 1-2: grammar, precedence, coercion, fuzz, table gate
crates/github/frontend/tests/lowering.rs     frontend §7 3-6, the pure half: what lowering produces
crates/core/frontend-native/tests/native.rs  frontend §7 7, the pure half; invariant 8 hint
crates/github/acceptance/tests/gha_e2e.rs    frontend §7 3-6, run for real: truth table, matrix, composites
crates/core/runtime/tests/native_e2e.rs      frontend §7 7: the cycle, run end to end
crates/core/runtime/tests/admission.rs       the admission seam: a pass rewrites the root and the children with the static capabilities, a problem is a diagnostic on the node's span, a changed child's digest is followed through the parent's reference
crates/github/acceptance/tests/harness.rs    frontend §7 8: every corpus workflow lowers or is rejected specifically
crates/github/acceptance/tests/e2e.rs        frontend §7 9: two real corpus workflows run on the executor

crates/attractor/frontend/tests/lowering.rs      Fabro plan §7 3 (the language): tiers, failure policies, goal gates, parallel, budgets, rejections
crates/fabro/frontend/tests/lowering.rs        the settings layers: `workflow.toml` sections, `.fabro/project.toml`, the user settings, `[run.inputs]` defaults, hooks and MCP layering, the launch record, the bundle root
crates/attractor/frontend/tests/conditions.rs    Fabro plan §7 4: the condition grammar against Fabro's semantics
crates/attractor/frontend/tests/fuzz.rs          Fabro plan §7 1: arbitrary text never panics the parser
crates/fabro/acceptance/tests/harness.rs     Fabro plan §7 2: every corpus file lowers or is rejected specifically; REPORT.md
crates/fabro/acceptance/tests/runs.rs        Fabro plan §7 5: every lowered corpus file runs under stubs; RUNS.md
crates/fabro/acceptance/tests/routing.rs     Fabro plan §7 3, 5, 7: the scripted battery, checked against the Fabro oracle
crates/fabro/acceptance/tests/exhaustion.rs  readiness item 4: the failure policy after retries in Fabro's order (explicit routes first under `succeed`, `allow_partial` before any route, an expired human gate), expectations from the pinned source
crates/fabro/acceptance/tests/workflow.rs    Fabro plan §5.2: nested workflows through the coordinator; one child per manager attempt
crates/attractor/steps/tests/manager.rs          readiness item 4: the manager loop under a controlled clock (polls, stop condition, exhaustion, defaults, reattach, cancel)
crates/attractor/steps/tests/parallel.rs         readiness item 3: branches as child invocations; static, mixed and all-failed forks, promotion, duplicate targets, empty `for_each`, item labels, repeated forks, nested forks, cancellation, resume; the fork snapshot offloaded (a 50-item fork's children, resume through the store, two forks in sequence)
crates/core/execution/tests/admission.rs      readiness item 3: `max_parallel` as a bound on a fork's live children (dispatch in declaration order, a branch in backoff holds none, cancel while queued, redispatch on resume) and the run-wide 10,000 invocation ceiling (boundary, nested, finished children, lower limits, refusals, resume)
crates/attractor/steps/tests/prompt.rs           readiness item 4: `attractor/prompt` against a scripted model client, contracts and repair turns, the prompted fan-in, prompt events
crates/attractor/steps/tests/admission.rs        model resolution at admission: concrete routes and frozen plans on every LLM node, `attractor.model.unknown` and `attractor.model.fallbacks` at the node's span, the graph unchanged without a client, the `start` stage's table check without a catalog
crates/fabro/acceptance/tests/e2e.rs         Fabro plan §7 6: gh-list, hello, a for_each fan-out, random selection, two 1,000-item forks in one run, end to end
crates/petri/cli/tests/fabro_cli.rs          Fabro plan §6: `petri run --auto-approve` answers a human gate
crates/petri/cli/tests/inspect_cli.rs        black box phase 2: `petri inspect` over finished, restarted, failed, cancelled and damaged run dirs, and over a fork of a run the binary wrote, which `petri resume` then continues
crates/petri/lib/tests/fork.rs               `host::fork_from` through the embedding boundary: a three-stage run forked after its first stage, a fork at the last position with and without `rerun_last`, a fork after a parallel fan-in and before it, a position inside a branch refused, a fork at a failed firing on its failure route, and a fork in a host's own store
crates/petri/cli/tests/fabro_resume_blackbox.rs  `petri resume` through the binary: a run killed with SIGKILL continues without repeating finished work, a paused run stays paused across the resume until an unpause, a waiting gate asks again, `inspect` reports `paused`, and the refusals (finished, leased, missing, corrupt)
crates/core/execution/tests/inspect.rs      black box phase 2: `inspect_run` reconstruction, retries, children, torn and corrupt logs
crates/core/execution/tests/determinism.rs  §10 determinism: a seeded coordinator run stores the same logs byte for byte, every record stamped from the simulated clock
crates/core/execution/tests/simulation.rs   §10 simulation: seeded invocation trees, forks, gates, restarts, cancels, the breaker and crashes, resumed over one store
crates/petri/cli/tests/fabro_blackbox.rs     the Fabro black box battery: the shipped binary against provider twins on loopback, scripted interviews, retention, the readiness milestone A smoke run with no `fabro` on PATH (`milestone_a_smoke_run_without_fabro_on_path`); every read of a finished run goes through `petri inspect --json`
crates/petri/cli/tests/fabro_scenarios_blackbox.rs black box phase 4: every required (scenario, backend, agent) cell of `crates/fabro/acceptance/scenarios/matrix.json`, each a scenario file in the versioned format `scenarios/SCHEMA.md` documents, run through the shipped binary with provider twins, a fixture repository with real local Git remotes, and a scripted interviewer; `scripts/fabro-coverage-report.py` merges the per-cell records into `coverage.json`
crates/petri/cli/tests/fabro_differential.rs black box phase 5: every scenario through the shipped binary and the pinned Fabro binary, independent expectations per engine, the committed reference, and the comparison under `tests/support/fabro/compare.rs` with decision records (`crates/fabro/acceptance/decisions/`)
crates/fabro/acceptance/tests/reference_version.rs  every oracle fixture, scenario reference, decision record, staged bundle and evidence record names the pinned Fabro revision
crates/petri/lib/tests/interview.rs          the interview dispatcher on the standalone host: parallel gates, sensitive masking, a failing interviewer, cancellation, occurrence across a loop, nested invocation paths, the re-ask, expiry
crates/petri/lib/tests/controls.rs           readiness item 6: the circuit breaker across restarts and resume, node visit totals across `loop_restart`, the stall watchdog, pause, cancel while paused, steering; the durable pause (a pause survives a dropped coordinator and holds admission on resume until an unpause; a paused resumed run can be cancelled)
crates/petri/lib/tests/interrupt.rs          the interrupt control: an interrupt with text during a native tool call ends the turn, keeps the session, and the text opens the next turn; a plain interrupt parks the turn until the next steer; a gate with no turn refuses it by name; the ACP backend's `session/cancel` without ending the agent
crates/core/driver/tests/interview_budget.rs readiness item 6 on a controlled clock: own-stage and sibling waits, overlapping questions, active work after a wait, handler-managed nodes, cancellation during a wait, a fresh budget on redispatch
crates/petri/lib/tests/embedding.rs          readiness item 7: a Fabro workflow without adapters, then with fake adapters (pause, skip, block, prepared results, route override, fatal and best-effort transitions, a hook service); the timeline reconstructed from public events; slow, failing and recovering consumers
crates/attractor/steps/tests/steps.rs            Fabro plan §5.2, §6: command, wait, human answered through deliver; Fabro's failure promotion; output references above 100 KiB; a long line stored whole, or its loss counted on the outcome
crates/attractor/steps/tests/agent.rs            Fabro plan §7 6: the agent step against Fabro's fake ACP agent
crates/attractor/steps/tests/acp.rs              follow-up C3: the ACP client against Petri's scripted agent (`tests/testdata/scripted_acp_agent.py`): every `session/update` variant as the `acp` envelope, the permission policy without a hook, the session usage extension in the stage's metrics, `authenticate` before `session/new`, only the named `$secret` references in the agent's environment, masked
crates/petri/lib/tests/acp_products.rs       follow-up C3, live (`--ignored`): Claude Code (`claude-code-acp`) and Gemini CLI (`gemini --acp`) through the ACP step, on the host and in a container the test builds, creating a file, the session's tool calls and usage on the stream, a `[[run.hooks]]` block honoured; each cell skips itself without the product's binary and credential
crates/fabro/frontend/tests/hooks.rs         readiness item 5: `[[run.hooks]]` at every phase with Fabro's payload and order, decisions, placement, timeouts, HTTP, prompt and agent hooks, native tool hooks, threads and fidelity, project memory, `speed` and `max_tokens`, the ACP hook mapping (a block rejects the permission request, the post-tool hooks run on reported tool calls, allow once under a hook)
crates/fabro/frontend/tests/mcp.rs           readiness item 9b: `[run.agent.mcps]` against the scripted `mcp_server.py`: a stdio server's tool writes into the workspace, hooks block MCP tools, error results, timeouts, a crashed server, cancellation, a retained thread, start failures, the http and sandbox transports over streamable HTTP and SSE, every fact read from Pebble's own events
crates/petri/cli/tests/fabro_mcp_blackbox.rs  readiness item 9b (milestone C2) through the binary: a configured MCP server's tool called by the twin's model under its qualified name, the effect in the workspace and the result in the next request, a hook block, failures, Ctrl-C, a retained thread, a start failure on the terminal, a masked secret, refused settings
crates/petri/cli/tests/fabro_fallback_blackbox.rs  readiness item 9a (milestone C1) through the binary: `[run.model.fallbacks]` with the twins injecting failures: a successful primary, qualifying and non-qualifying failures, a third provider after two failures, chain exhaustion, a tool effect not repeated across a failover, cancellation during fallback, refusal versus other content filters, a request timeout, client retries spent before the chain advances, a workflow retry that is not a failover, reasoning effort mapping with `NoNearbyReasoningLevel` and `ChainEmpty`, repair turns on the plan with a target's chain inert, a retained thread on the fallback route, a prompt node
crates/petri/cli/tests/llm_client.rs         the client `petri::llm_client` builds, against a twin: its same-route retries and its call budget
crates/petri/lib/tests/fallback_events.rs    readiness item 9a: a stage's fallback plan, routes, failover decision, per-route usage and outcome rebuilt from public `RunEvent`s alone
crates/petri/lib/tests/model_admission.rs    admitted routes survive catalog changes before dispatch and before resume (alias, default provider, eligible providers), read from the plan event and Pebble's session route; a pinned route the client cannot address fails with `llm:pinned_route_unavailable`
crates/petri/cli/tests/fabro_hooks_blackbox.rs  readiness item 5 through the binary: a configured hook blocks a real tool effect of a native agent (`a_configured_hook_blocks_a_real_tool_effect_in_the_native_backend`); two `full` nodes share one conversation
crates/attractor/steps/src/fallback.rs (unit)     readiness item 9a: chain resolution keyed by the canonical model, refused keys, skipped candidates with Fabro's notices, per-target reasoning effort with `NoNearbyReasoningLevel` and `ChainEmpty`, the plan's positions, the typed failure's eligibility (`lithos-llm`'s `failover_eligible`) for every `lithos-llm` kind, and every Pebble error's class
crates/fabro/frontend/tests/skills.rs        readiness item 9c: Fabro's skill directories in order on real scopes, precedence across the three, the reference prompt section and tool definition, `/name` expansion, `[run.agent] skills`, malformed and missing skills diagnosed (fixtures `crates/fabro/acceptance/testdata/skills`)
crates/petri/cli/tests/fabro_skills_blackbox.rs  readiness item 9c through the binary: precedence and the reference prompt and tool on both vocabularies, a hook blocking a skill-driven tool call, a `for_each` branch keeping its loaded skill, the warnings on the terminal and in the event log
crates/attractor/steps/tests/compaction.rs         readiness item 9e: Fabro's compaction on the native backend against a scripted model: the trigger below, at and above the 80 percent threshold, continuation and thread reuse across the boundary, tool pairing, a host `CompactionPolicy`, a failed summary, cancellation, the lost-thread fallback, and the public events and usage
crates/petri/cli/tests/fabro_compaction_blackbox.rs  readiness item 9e through the binary: a native agent compacts and finishes correct work, a later `full` node reuses the compacted thread, a failed summary call is non-fatal, a lost thread starts at `summary:high`, and a run cancelled during the summary call stops
crates/attractor/steps/tests/subagents.rs        readiness item 9d: a native agent delegates to Pebble-built children on real scopes; hooks and the question rule inside a child; the invocation ceiling never counts a child; the open-session bound; child failure, nesting, cancellation, thread reuse, resume; a child re-reads the parent's project memory and re-discovers its skills
crates/petri/cli/tests/fabro_subagents_blackbox.rs  readiness item 9d through the binary: a parent delegates a workspace change; a hook blocks a child's effect; a child's failure is the parent's tool result; concurrent children and one invocation; a grandchild; an interrupt closes the child; a retained thread carries a child's result; accounting reconstructed from `execution::replay_run`
crates/petri/cli/tests/fabro_terminal_blackbox.rs  readiness item 2 through the binary: retry notices, branch attribution with masked secrets, the bounded echo, `--interactive` for every question type with invalid and missing input, Docker retention after success, failure and cancellation with `petri sandbox prune`
crates/petri/cli/tests/fabro_milestone_blackbox.rs  readiness item 8 through the binary: one workflow with `run.prepare`, commands, a native agent editing a file under a tool hook, a retained thread, project memory, a scripted decision, a bounded fan-out consumed downstream, run-end hooks and file checks; success, failure and cancellation
crates/petri/cli/tests/fabro_readiness_blackbox.rs  readiness item 10 (milestone D) through the binary: the item 8 workflow with every item 9 facility in one run (a skill-guided plan, a hooked MCP write, a hooked sub-agent, a fan-out, a compaction and a later node on the compacted thread with an MCP call, a second thread failing over to the Anthropic twin), no platform Git operation in the repository the run prepared, every family on the public stream; an exhausted chain and an interrupt inside a child's tool as separate cases
crates/petri/lib/tests/embedding_readiness.rs  readiness item 10 through the embedding boundary: the same combined execution run in-process by a host with its own hooks, interviewer and sink against the twins; the same files and scripts, the run rebuilt from public events, replay equal to the live stream
crates/petri/cli/tests/fabro_evidence_blackbox.rs  the readiness gate's evidence: a scenario through the binary leaves a complete record with every pin, a failed scenario keeps its case directory, the coverage report counts only passed cells, a required asset fails instead of skipping
crates/petri/lib/tests/fabro_dependencies.rs  readiness item 1: no Fabro crate anywhere in Petri's dependency graph
crates/petri/cli/tests/standalone.rs          readiness item 1: the binary runs a Fabro workflow with no `fabro` on PATH
```

### The Attractor language and the Fabro layer

`crates/attractor/FORMAT.md` is the language as lowered: every construct in a
DOT file, what it becomes in the IR, what is refused, and the step kinds a
graph runs on. `crates/fabro/FORMAT.md` is the Fabro layer around it: how
`workflow.toml`, `.fabro/project.toml` and the user settings resolve into the
run settings the lowering applies, and the launch record Fabro adds to the
graph. Fabro is one implementation of the Attractor specification, and the
next revision of that specification follows Fabro's dialect, so a Fabro
workflow is an Attractor workflow with Fabro's files beside it. The split, and
the `attractor` prefix on the step kinds, diagnostics and event kinds the
language emits, are recorded in `.ai/plans/attractor-split.md`.

### The Fabro compatibility contract

`crates/fabro/acceptance/CONTRACT.md` freezes the Fabro reference revision
(`crates/fabro/corpus-pin.txt`), the required workflow bundles
(`crates/fabro/acceptance/bundles.lock.json`; their files are vendored under
`crates/fabro/acceptance/bundles/` with their provenance and verified against
the lock by `scripts/corpus-fetch-fabro-bundles.sh`), the feature matrix for every
construct and `workflow.toml` option those bundles use, and the accepted
differences. Petri production code never links or launches Fabro. The parity
harness in `crates/fabro/oracle/harness/` builds the pinned `fabro` binary from
the fetched corpus and drives it as a subprocess through its CLI and server
API; `scripts/oracle-regenerate.sh` uses it to refresh the oracle fixtures, and
the routing test rejects a fixture from any other Fabro revision.
`scripts/fabro-provision.sh` is the one way both harnesses obtain that binary.
The differential matrix (`crates/fabro/acceptance/DIFFERENTIAL.md`,
`mise run test:fabro:differential`) runs every scenario through the shipped
binary and the pinned Fabro, compares the runs under fixed rules, and accepts
a difference only through a committed decision record.

The complete-workflow scenarios are data, not code: one versioned JSON file
per scenario under `crates/fabro/acceptance/scenarios/`
(`SCHEMA.md` there is the format, `README.md` the layout), naming the pinned
bundle and its digest, the fixture repository and its local remotes, the twin
scripts, the interview entries, the controls and faults, and every expected
observation, written before the run. `crates/fabro/acceptance/scenarios/matrix.json`
lists every required (scenario, backend, agent) cell with its state, so a
filtered or skipped case stays visible; `mise run test:fabro:blackbox` runs
them and reports the coverage, and `mise run test:fabro:blackbox:strict`
requires Docker, the vendored bundles, and no skipped cell.
`mise run check:fabro:readiness` is the final readiness gate: the same
strict run, then a coverage report that fails unless every required cell
passed, blocked cells included; the nightly workflow runs it on the runners
that have Docker.

### The integration handoff

`crates/fabro/HANDOFF.md` is what a host that embeds Petri to run Fabro
workflows relies on: the optional interfaces (`ExecutionHooks`,
`HookService`, `Interviewer`, `RunEventSink`, `SecretProvider`,
`OutputStore`, the `PebbleClient`, `HostTools` and `FabroHome`
capabilities), the
identities and event positions, the lifecycle acknowledgements, the
compatibility versions and the strict rejection of incompatible logs, what
stays Fabro's, and the integration checklist. The event contract's coverage
matrix is `crates/core/execution/EVENTS.md`; the milestone D audit of every
required row is in `crates/fabro/acceptance/CONTRACT.md`.

### CI, evidence, and the readiness gate

Required CI (`.github/workflows/ci.yml`, job `check` on Linux and macOS)
fetches the GitHub Actions corpus and the Fabro corpus, verifies the vendored
Fabro bundle set (`scripts/corpus-fetch-fabro-bundles.sh`: every tracked file
against its locked digest, never skipped, no fetch), builds the sandbox
plugins, and runs
`mise run check` with `PETRI_REQUIRE_CORPUS`, `PETRI_REQUIRE_FABRO_CORPUS`,
`PETRI_REQUIRE_FABRO_BUNDLES` and, on Linux, `PETRI_REQUIRE_DOCKER` set, so a
missing asset, binary, scenario, or backend fails instead of skipping. Every
required host scenario runs on both runners; the Docker subset runs on Linux,
which also checks that no `petri-` container is left behind. The provider
twins are `petri-cli` dev-dependencies from the public
`lithoscomputer/twins` repository, so a build is their fetch. A second required
job, `fabro compatibility`, builds the pinned `fabro` binary from the fetched
corpus (`scripts/fabro-provision.sh`; only the binary is cached, keyed by the pin
and the toolchain; about three minutes on a miss) and runs the comparison
matrix through `mise run test:fabro:differential` with
`PETRI_REQUIRE_FABRO_BINARY` set. A third required job, `lean model`,
installs elan (pinned by version and digest), builds the Lean model in
`lean/`, which checks every proof, and runs `mise run test:lean`: the engine's
`lean_model` tests compare the real core with the model's executable, with
`PETRI_REQUIRE_LEAN_MODEL` set. The Nightly workflow repeats the black box
set three times under different test schedules
(`mise run test:fabro:blackbox:repeat`), runs the long tests, and reruns the
matrix; it adds repetitions and is never the only parity evidence.

Every black box scenario writes a machine-readable evidence record
(`crates/petri/cli/tests/support/fabro/record.rs`, schema version 1) into
the run-scoped directory `PETRI_EVIDENCE_DIR` names (`mise run
test:fabro:blackbox` creates one under `target/fabro-evidence/` and links
`latest` to it): the Petri, Pebble, lithos-llm, sandbox-driver, twins, and
Fabro pins read from the manifests; the launch inputs and configuration; the
twin scenarios matched; raw and normalized observations; the final context
through `petri inspect --json`; artifacts; process output; assertions;
compatibility decisions; the cleanup result; and links to the owning
library's contract tests. A failed record keeps the whole case directory; a
test that panics before finishing still leaves a failed record.
`scripts/fabro-coverage-report.py` folds the records, the scenario matrix (`matrix.json`), the per-cell results,
the differential matrix's engine records, and Nextest's JUnit output into
`coverage.json` and `coverage.md` with required, passed, failed, skipped,
missing, blocked, and excluded cells. Only `passed` counts; a skip, an
exclusion, or an empty run never does, and a runner failure overrides a passed
record. A pinned-Fabro assertion that a decision record lists under
`known_defects` is expected and counts as passed with a note naming the
record; any other failed assertion fails the cell. CI writes the report into the job summary
and keeps the full bundles of a failed run and the compact records of a passed
run as job artifacts. The readiness checklist with each item's evidence source is the
"Readiness gate checklist" section of that contract.

#### Library and repository gates

Pebble owns the agent loop, coding-agent behavior, and the MCP client;
`lithos-llm` owns provider transport and request retries. A change to either runs the owning repository's required checks
before Petri moves its lock; then the relevant Petri scenarios run again
through the shipped binary, and their evidence records cite the new commits.
A library test pass never replaces a required Petri scenario. The locked
commits live only in `Cargo.lock`; the contract's "Pinned revisions" table
names where each revision lives, the Fabro reference and the runner image
included.
See [git dependencies](DEVELOPING.md#git-dependencies) for how they are
named and updated.
The library batch the readiness work asked for landed on
`petri/readiness-batch` in each repository and is pinned: Pebble's
summary-call usage accounting, `continue_prompt` for a failover with no
repeated tool effect, child agents inheriting project memory and skill
directories, skipped-skill reporting, sequential sub-agent tools, a routed
scripted provider; sandbox-driver's silence-based output drain and the
preview-URL operation for servers inside containers. The later
`petri-readiness-gaps` revision exports Pebble's project-memory loader, which
prompt nodes now load through, and the tool-round budget agent hooks enforce.
Pebble `c91810f` and lithos-llm `55add45` carry token usage and cost as one
lithos-llm `Usage`; every `usage` Petri records takes that shape
(`crates/attractor/FORMAT.md`, "Usage").

Host and container scopes use the `sandbox-driver-host` and
`sandbox-driver-docker` plugins by default. Petri launches them and
communicates over JSON-RPC; no provider crate is linked into Petri itself. An
embedder that links the built-in providers (Fabro does) passes a factory per
kind to `Runtime::in_process_providers`; the runtime then uses those providers
directly and launches no plugin, and leases keep the same fingerprints either
way (`crates/core/executor-sandbox/src/in_process.rs`). `mise run plugins:build`
installs both plugins from the locked sandbox-driver commit under `target/plugins/bin`. The development
and test tasks select those binaries. Docker tests skip when no daemon is
reachable; Host execution requires its plugin and no daemon. A provider that
lost command output on its own transport (Daytona's encoded exec can tear a
frame) counts the loss in its result; Petri keeps the command's exit status
and appends one line, `[sandbox] N output frame(s), M bytes dropped by the
provider`, to the command's stderr, so the agent and the run log see it.

The Host plugin keeps private records under `<run_dir>/host-registry`.
It owns each lease's workspace and process groups. After a plugin crash,
recovery fences old work before reuse and never signals saved process ids.
Retention keeps or deletes the workspace with its sandbox; `petri sandbox
prune` deletes retained Host resources through a fresh plugin.

Host jobs inherit `PATH`, `HOME`, `USER`, `SHELL`, `LANG`, `TERM`,
`TMPDIR`, `GOPATH`, `CARGO_HOME`, and `NVM_DIR`. Pass other values and secrets
explicitly through workflow environment settings.

`petri run --backend host|docker|daytona|<kind>` selects the execution backend.
Host is the default, unless the workflow's own configuration asks for another
(a Fabro `workflow.toml` `[environments.<id>] provider`, read back from the
lowered graph through `Frontend::launch_settings`; `petri resume` finds the
format from the stored graph through `Frontend::claims_graph`); an explicit
`--backend` always wins. Docker runs process jobs in a pinned slim runner image.
Daytona runs them in a container with nested Docker; container jobs, services,
and Docker actions stay inside that sandbox. `--daytona-kind vm` explicitly
selects a VM when the account supports it. `--runner-image LABEL=IMAGE` overrides a
runner label. Docker defaults cover Ubuntu 22.04, 24.04, and 26.04. Daytona's
published Docker-in-Docker default covers Ubuntu 24.04 and `ubuntu-latest`;
other labels need an override. Unknown or conflicting labels fail at acquire.

Any other provider kind names a sandbox-driver plugin registered through
`--sandbox-providers <file>`, which run, resume, and prune all take; an
embedder registers the same through `Runtime::sandbox_providers`. A registered
provider runs ordinary process scopes from the template its registration
supplies, and a lease goes back to the provider its record names whatever the
current backend. See
[registered sandbox providers](crates/core/executor-sandbox/PROVIDERS.md).

Daytona reads its credentials from `DAYTONA_API_KEY` or `DAYTONA_JWT_TOKEN`
and the SDK's endpoint and organization variables. The CLI and embedded library
share a baseline of 2 CPUs and 4096 MiB of memory, the runner's minimums.
Disk is unspecified by default, so Daytona chooses the allocation. Existing
`--daytona-cpus`, `--daytona-memory-mb`, and `--daytona-disk-mb` flags override
each value independently; clients set `SandboxOptions` and `DaytonaResources`.
Explicit disk allocations must be positive; Daytona enforces image-fit and
account limits. No VM access or enlarged disk quota is needed for the defaults.
The executor uses the driver's whole-GiB allocation rule before checking the
memory minimum, naming a snapshot, and comparing its reported resources.
Equivalent requests therefore reuse a snapshot, including requests supplied
in MiB by an embedded client. Omitted disk remains unspecified.
Runner snapshots are named by image and effective resources, prepared once per run,
and reused across runs. Snapshot preparation has a 15-minute deadline.
Petri disables automatic sandbox stop, pause, deletion, and TTL timers; lease
release and prune control cleanup. Shared runner snapshots remain available.

Daytona has no inferred route to Petri's ObjectService. Ordinary JavaScript
and Docker actions still run, with the service variables omitted. Artifact
and cache actions need `PETRI_SANDBOX_DAYTONA_HOST_ADDRESS` set to a name or
address reachable from the sandbox. Local transport and adapter tests pass. The
live Daytona tier (`mise run test:daytona`, see
[the live Daytona tier](crates/core/executor-sandbox/DAYTONA.md)) needs
credentials. Its default process and nested-container smoke tests have passed
against the hosted service; the full tier remains separate validation.

Release archives bundle the Docker, Host, and Daytona plugin executables from
the locked sandbox-driver commit. Petri embeds their SHA-256 digests at release build time.
`scripts/release-verify.sh` checks the archive, runs Host and Docker workflows
without development mode, and verifies rejection of modified plugins.
sandbox-driver, Pebble, and lithos-llm are public repositories fetched over
HTTPS, so no job needs a deploy key. See
[git dependencies](DEVELOPING.md#git-dependencies).

Fabro agent nodes can use Pebble directly as a Rust library. Set
`backend="api"` and `model="provider/model"` on the node, or set graph
`backend` and `default_model`. The `petri` distribution reads provider
credentials from its environment. ACP remains the default. See
[native Pebble configuration](crates/attractor/FORMAT.md#native-pebble) for scope
requirements, client injection, project memory, skills, context compaction,
tool hooks, retained threads, sub-agents, events, and accounting. Fabro's `fidelity` modes, threads and
`[[run.hooks]]` are described under "Steps at run time" on the same page.

`petri inspect --run-dir <dir> [--json]` reconstructs a run from its run
directory alone: `run.json`, `coordinator.jsonl`, the registered graphs, and
each execution's `events.jsonl`, replayed through the engine. It never starts a
step, contacts a provider, takes the run lease, or writes under the run
directory, so it works after the process exits, after the source workflow is
gone, and while another process holds the run. `--json` prints a versioned
document (`inspect_format_version`) with the run status, the root invocation
and its final execution, every invocation with its parent call and children,
every execution in order with its own derived run context (`kv` plus the
node-instance records: status, output, generation, attempts), the final firing
history, every attempt including retries, every applied route, and the
interview receipt (`interviews`, from `<run-dir>/interviews.json`) when the run
had an interviewer. Secret values stay masked or as `$secret` references. An interrupted run is reported
as incomplete with the reasons (exit 1); a corrupt, truncated-then-terminated,
diverging, or unsupported-version log is an error (exit 2), never a final
snapshot. The field contract is
[`crates/core/execution/INSPECT.md`](crates/core/execution/INSPECT.md).

`mise run test:remote` transfers an artifact through a separate Docker daemon
with no host filesystem mounts. `mise run check` includes this test. Set
`PETRI_REQUIRE_DOCKER=1` to require Docker instead of skipping unavailable tests.

`mise run test:daytona` runs the live Daytona tier against the account
`DAYTONA_API_KEY` names: the sandbox executor's Daytona battery
(`crates/core/executor-sandbox/tests/daytona_backend.rs`), the host's
retention, prune and resume cells (`crates/petri/lib/tests/daytona.rs`), the
CLI retention cell, and the GitHub Actions cell with a VM job and a nested
container job. It builds the Daytona plugin (`mise run plugins:build:daytona`)
and sets `PETRI_REQUIRE_DAYTONA=1`, so a missing credential or plugin fails
with its reason instead of skipping. Without the variable the same tests skip
with a `skipping:` notice, which is what the routine suite does. No CI job has
the credential, so the tier is run by hand before a change to the Daytona
path or the sandbox-driver pin lands. It creates billable sandboxes.
[`crates/core/executor-sandbox/DAYTONA.md`](crates/core/executor-sandbox/DAYTONA.md)
maps Fabro's former 22 live Daytona tests onto the tier and records the gaps.

`mise run test` runs all of them with Nextest, then runs the maintained doctests
with Cargo.

## Decisions the design document left open

**The expression language.** §3 names `ExprId` and says guards are "boolean
expressions over (outcome, contexts)" without fixing the language. `ir::expr`
defines a small total one: literals, variable lookup, field and index access,
arithmetic and comparison, short-circuiting boolean operators, conditionals, array
and object construction, and a fixed function set. Missing fields evaluate to `null`
rather than erroring, so a guard is always total. Two list functions, `sort_by_key`
and `pluck`, exist so a collector can put clone results back in `items` order
without needing lambdas.

Evaluation is total by design: a guard must always produce a boolean, so a run never
fails at the wrong moment over a typo. The cost is that a typo is silently falsy
instead. A strict mode, or an unknown-field lint at load time, is a v2 seam (§8).

**Expression environment.** One `EvalEnv` for guards, `map`, preconditions and
`Expansion.items` alike. `token` and `input` resolve to the payload on the first input
edge and shadow any static of the same name; `nodes.*` and `kv.*` read the run
context; everything else — scope `env`, node identity, generation, attempt, the
firing's own outcome, `item` / `index` in a clone — comes from `StaticCtx`.

The earlier ad-hoc firing-context mechanism is gone rather than coexisting: `outputs`
and `upstream` bindings no longer exist, and a precondition's `success()` folds the
upstream statuses it reads out of `RunContext` by looking up each input edge's source
node. Nothing is threaded through token payloads to carry status.

## Folded back into the design document

These began as implementation notes and are now rules in `engine-spec.md`, because
each follows from something the design already said rather than from a choice the code
made:

- **A splice supersedes the whole template region** (§6, splice semantics). `All` is
  defined over incoming edges *at firing time*, so a template edge that no token can
  cross is a deadlock generator. Superseding is not deletion: the edges stay in the
  graph and only leave the join count. Regression test:
  `superseded_template_edges_do_not_deadlock_the_collector`.
- **Scope release is irreversible** (§3, and a §7 warning). A scope is held until no
  firing, token or deferred join needs it; a path that leaves and returns gets a fresh
  runtime and workspace. Validation warns rather than errors, because whether a given
  path really releases the scope depends on runtime state. The check is a static
  over-approximation.
- **A loop head must join with `Any`** (§7, invariant 8), with the corollary that a
  node cannot be both a multi-branch `All` join and a loop head.
- **`Token.generation`** (§4) and **`Command::StartStep(ResolvedFiring)`** (§5).

Core Semantics Patch 01 landed on top of those: attempts and `RetryPolicy` (§3, §4),
`PartialSuccess` with `is_success_like` as the single classification point (§4),
`RunContext` and `EvalEnv` (§4), event log v2 with replay (§5), and
`RuntimeSpec.requirements` (§3).

## Where the code departs from the documents

Each of these is a place the literal text did not survive contact with the compiler or
with a working execution path.

### From the IR design

1. **Entry nodes and clone entries get synthetic seed edges.** §3 seeds entry nodes
   with a token, but a token names an edge and an entry node has none. The engine
   allocates one seed edge per entry, above every declared edge id, so joins count it
   like any other incoming edge and the firing rule needs no special case: `All` over
   a single seed edge is satisfied by the seed token. Seed edges never appear in a
   `Routing` group and never collide with a declared id
   (`crates/core/engine/tests/seeding.rs`).

2. **`Command::ExpandNode` is defined but not emitted.** `items` is a pure
   expression, so the core evaluates it and builds the splice itself, in the same
   `apply` call. The variant is marked `// reserved: external expansion`.

3. **`ExpandTarget::Subgraph { entry, .. }` requires `entry` to be the expanding
   node.** Otherwise it is ambiguous whether the expanding node runs before the region
   is cloned. Violations are a run error, not a panic.

4. **An entry node may have a back edge pointing at it.** A loop head that is also the
   graph entry is legitimate: the seed starts generation 0 and the back edge starts
   each later one. Validation rejects only *forward* edges into an entry.

5. **Budget exhaustion drops the tokens.** §7 requires a finite cap on looped nodes
   but does not say what happens at the cap. The firing is refused, the tokens are
   dropped, a `RunError::BudgetExceeded` is recorded, and the run folds to failed.

6. **`apply` drains an internal event queue.** The signature is the document's, but
   routing emits tokens by feeding `Event::TokenEmitted` back to itself, each logged
   before it is applied.

### From Core Semantics Patch 01

7. **`RetryOn.statuses` is `Vec<StatusKind>`, not `SmallVec<Status>`.** `Status`
   carries payloads — `Failure(FailureInfo)`, `PartialSuccess { underlying }` — so a
   list of them cannot express "match this variant". `StatusKind` is the payload-free
   discriminant. It is not a second classification point: the never-retry-a-success
   rule still goes through `Status::is_success_like`. `Vec` rather than `SmallVec`
   avoids adding a dependency for a list that is almost always one or two entries.

8. **`RuntimeSpec` became a struct.** It was an enum, and an enum cannot gain a
   common `requirements` field. It is now `RuntimeSpec { target: RuntimeTarget,
   requirements: Vec<SmolStr> }`, with the old variants moved to `RuntimeTarget`.

9. **Log records carry `EventSource`.** §7 test 1 asks for byte-identical replay, but
   nothing said how replay tells a core-emitted event from a host-fed one — and
   replaying core events instead of regenerating them would make the check a copy
   rather than a determinism test. Each record now says which it is, and
   `verify_replay` feeds back only the `External` ones.

10. **`context_updates` merge on final attempts only.** §1.2 pins "run-context
    recording of the final status" to the final attempt but does not say what happens
    to `kv` on an attempt that is then retried. Merging a discarded attempt's writes
    would leave state from work that was thrown away, so the merge follows the same
    rule as the status record. **Worth a second opinion** — a step that wants to leave
    a breadcrumb on every attempt cannot, today.

11. **A retry re-resolves the step config.** The attempt after a backoff binds its
    config against the run context as it stands then, not as it stood before attempt
    one. Deterministic either way; this way a retry can see what changed.

12. **`Exhaustion::AcceptPartial` fires whenever a retryable status has no attempts
    left**, including `max_attempts: 1` where no retry was ever possible. Reading it
    the other way would make `allow_partial` silently inert unless retries were also
    configured. The driver applies it (`RetryPolicy::finalize`) to every returned
    attempt before a host prepares the result, so the engine records the finish it
    is given and a host's prepared result is never converted again; the engine
    applies it only to the `invalid_splice` failure it makes itself.

13. **`base_delay` uses repeated multiplication, not `powi`.** `powi` is not
    guaranteed bit-identical across platforms, and the delay goes into the log.

14. **`Status::is_success()` and `FailureInfo.retryable` are gone.** The first was a
    near-duplicate of `is_success_like` sitting next to it in the same `impl` — 
    exactly the footgun the one-classification-point rule warns about. The second was
    a second retry signal competing with `RetryPolicy` and `FailureInfo.class`.

15. **A firing awaiting a retry stays live.** That is what keeps its scope held and
    the run non-quiescent across the backoff, rather than a run appearing to finish
    mid-retry.

16. **`NodeRecord.gen` is `generation`**, for the same keyword reason as `Token`.

17. **`StaticCtx` is a defined type.** §2.3 names `statics: &StaticCtx` without
    saying what it holds. It holds the per-firing bindings that are neither the token
    nor run state: scope `env`, node identity, generation, attempt, the firing's own
    outcome, and `item` / `index`.

### From the executor handoff

18. **`Executor::release` takes a `ScopeOutcome`.** The handoff's signature is
    `release(&self, env: EnvHandle)`, but the retention default is *keep on failure*
    and an environment cannot know whether the work inside it failed. The driver
    knows, so it says.

19. **`ProcessHandle` exposes one merged `lines()` stream, not `stdout()` and
    `stderr()`.** §3.4 requires log lines in arrival order across both streams, and
    merging two receivers after the fact cannot recover an order that was never
    recorded. Each line carries its stream tag, so nothing is lost.

20. **The cancellation escalation is in `output`, not a failure class.** §2.4 asks
    for `Status::Cancelled` with class `"cancel_forced"`, and §7 test 5 for a class
    recording the TERM-to-KILL escalation — but `Status::Cancelled` carries no
    `FailureInfo`, and adding one would widen a closed enum that core §4 says is
    permanent. So `cancel_escalation` is a field on the outcome's output object,
    valued `sigterm`, `sigkill` or `cancel_forced`.

21a. **In-container signals are `kill -SIG -PGID`, with no `--` separator.** The
    handoff spells it `docker exec <c> kill -<SIG> -- -<PGID>`, which is the POSIX
    form — and busybox rejects it outright: `sh: invalid number '--'`. A rejected
    signal is a silent one, so cancellation never reached the step; the ladder waited
    out its whole grace period and the step ran on until the container was torn down.
    Since alpine is the obvious base image, the separator cannot be used.
    `kill -TERM -123` is understood by busybox ash, dash and bash alike, and the call
    goes through `sh -c` so it is the shell builtin rather than whichever `kill`
    binary the image carries.

    This one hid behind a green test. `docker_cancel_kills_the_exec_process_group`
    asserted that a backgrounded grandchild stopped ticking after a cancel — which it
    did, but because release removed the container, not because the signal landed.
    The test now also asserts the escalation was `sigterm`: that step does not trap
    TERM, so TERM alone must have ended it, and a signal that never arrives shows up
    as a `sigkill` escalation instead. **A cancellation test that only checks the
    process is gone will pass on teardown alone; it has to pin how it went.**

21b. **Docker `wait` follows the process group, not the `docker exec` client.** `setsid` forks when its caller is already a process-group
    leader, and whether `docker exec` hands it one is not something to rely on. When
    it forks, `setsid` exits as soon as the child is running and `docker exec` returns
    0 while the step is still going — the status is lost, and so is the step's real
    duration. A test caught this: `exit 7` came back `Success`. The wrapper now
    records the status beside the pgid, and `wait` polls the process group's liveness
    rather than trusting an early return, which makes the executor correct whichever
    way `setsid` behaves. `docker_wait_follows_the_step_not_the_client` pins both the
    status and the duration.

    The same reasoning covers a second way the client lies. After `SIGTERM` the
    wrapper dies at once — it traps nothing — so the client returns while a step that
    *does* trap TERM is still running. Believing it there reports a graceful exit,
    skips the escalation to `SIGKILL`, and leaves the step running. So `wait` waits
    for a recorded status or a dead process group, whichever comes first; `SIGKILL`
    guarantees the second, which is what bounds the loop.
    `docker_cancel_without_a_recorded_status_still_reports_cancelled` pins the path
    where the wrapper is killed before it can record anything.

22. **The ladder lives in the step kind; the driver owns the outer deadline and the
    timeout-versus-cancel decision.** A step reports `Cancelled` however it was
    stopped, because it cannot know why. Only the driver sees which terminal arrived
    first, so it rewrites `Cancelled` to `TimedOut` when its timer got there first.
    This keeps `Control` closed at `Cancel` rather than growing a variant per reason.

23. **Retry jitter is a hash of `(firing, attempt)`, not an RNG.** Jitter's job is
    decorrelating *different* retries so they do not stampede, which a per-firing hash
    does. It avoids a dependency and leaves the driver reproducible, so retry timing
    is testable.

24. **`ResolvedFiring` never actually reaches the event log here.** §6.1 motivates
    the secret-reference amendment with "`ResolvedFiring` is serialized into
    `StartStep` records", but in this implementation commands are not events, so no
    command is ever logged. The amendment is implemented anyway — it is the right
    invariant for any host that persists commands — and the secret test greps the
    whole serialized `EngineState`, not just the log, which is the stronger check.

25. **`split(string, separator)` joined the expression language**, and the language
    now has a gate. The outputs-file protocol yields strings, so a step that produces
    a list of regions produces one string, and `for_each` needs an array — acceptance
    test 1 cannot be written without it.

    Three functions had by then accreted under pressure from individual tests, which
    is how ad-hoc scripting languages are born. `ir::expr::BUILTINS` is now a table
    that **gates dispatch** rather than describing it: a call is looked up there
    before any match arm is reached, and arity is checked once from the entry. A
    function missing from the table is unknown however many arms exist, and an entry
    with no arm fails its own conformance test. The bar for adding one — pure, total,
    tested including error cases, and justified by something that cannot be written
    without it — is documented on the table itself.

26. **Docker images must provide `setsid`.** busybox and util-linux both do, so
    alpine, debian and ubuntu are all fine. A spawn into an image without it fails
    with a spawn error rather than silently losing the process group.

27. **Log draining after a process ends is bounded at 5 seconds.** §3.4 says capture
    continues "until both streams close", which a grandchild holding the pipe open can
    delay indefinitely. The driver's hard deadline would eventually fire, but bounding
    the drain keeps the failure local and legible.

28. **`ScopeSpec`, `EnvHandle` and `Teardown` are defined here.** §1 names the first
    two without giving their shape.

29. **`RunHandle` is how a cancel gets in.** The handoff has `CancelRequested`
    arriving as an event without saying who sends it; `Driver::handle()` returns a
    handle that can inject one into a run in flight.

30. **Cancelled outcomes route, and stopping has two tiers.** Frontend finding 3:
    GitHub runs `if: always()` and `if: cancelled()` cleanup after a cancel, and
    the engine could not, ever — the spec had cancellation drop the scope's
    tokens, making `Cancelled` the one terminal status that was recorded but
    never routed. Spec §5 now says: a cancelled firing's outcome routes like any
    other, pending tokens survive, and what stops work from restarting is
    **structural** — a node in a cancelled scope, or fed by a token from a firing
    that recorded `Cancelled`, completes `Cancelled` without evaluating anything
    unless it carries `Node.run_on_cancel`. No gate anywhere has to be right for
    cancellation to be safe (review showed the implicit gates could not be: a
    job's first step, a dependent whose needs finished pre-cancel, and a
    `max_parallel`-deferred clone all pass them). The engine's *previous* cancel
    behavior was renamed rather than deleted: `Event::KillRequested` is the
    forced tier — tokens drop, nothing routes, nothing is admitted,
    `Control::Kill` goes straight to `SIGKILL` — and it is in the log, so replay
    gets the hard stop for free and the mode of stopping is recorded, never
    inferred. The driver wires the tiers as cleanup grace and second-cancel; the
    host executor grew sentinel-pinned process groups so release can end
    stragglers without ever signalling a recycled pgid. Log v2 → v3. (The GHA
    frontend has since stopped choosing where the flag lands from condition
    text: it sets `run_on_cancel` on every node it emits — matrix expansion
    heads excepted — and lets each step's gate evaluate the condition against
    the post-cancel state. The engine's structural rule is unchanged; the flag
    just stopped being an admission *decision* in that frontend.)

## Frontends (package 03)

Both frontends are pure: text in, `Graph` and diagnostics out. One parser reads the
`${{ }}` grammar; two lowerings give it meaning. The GHA lowering maps `==` onto
`loose_eq`, `&&` onto a value-returning conditional, and every GitHub function onto a
table entry — there is no second evaluator, and a property test asserts that whatever
parses lowers only onto `ir::expr::BUILTINS`. The native lowering maps the same syntax
onto the engine's own `Eq` / `And` / `Not`.

### The corpus

316 workflows from 21 repositories with `.github/workflows` (22 fetched). Two
instruments measure them, both regenerated by the harness and committed beside
the corpus pins:

- **`crates/github/corpus/REPORT.md`** — the *lowering* bar: every workflow
  either lowers or is rejected with a specific `unsupported.*` code — zero
  panics, zero generic errors. Windows/macOS workflows, callee-only reusable
  files, and broken-upstream references leave the denominator by policy.
- **`crates/github/corpus/RUNS.md`** — the *run-time* bar: every in-scope
  workflow that lowers, run end to end (`--ignored`, network + Docker) with
  `run:` scripts stubbed to `true` and `uses:` steps real, every host scope
  rewritten to a pinned [sandbox-images](https://github.com/lithoscomputer/sandbox-images)
  runner container so corpus code never executes on the host. First-failure
  classes are ranked, with server-coupled failures (OIDC, repository secrets,
  SaaS backends, credentials) kept apart from real runtime-tier gaps — the
  same denominator discipline, applied at run time.

The corpus is fetched, not committed: the workflows are their authors' property, so
`scripts/corpus-fetch.sh` downloads them at the commits pinned in
`crates/github/corpus-pins.txt`, and `crates/github/corpus/` is gitignored apart from
the two reports. Each fetched repo gets a `PROVENANCE.md` with its commit and licence.
The corpus tests skip when the corpus is absent, and `PETRI_REQUIRE_CORPUS=1` — which
CI sets — turns that skip into a failure.

Corpus workflows also run end to end in the acceptance battery
(`crates/github/acceptance/tests/e2e.rs`), with a stub `gh` on `PATH` so nothing
reaches GitHub's API; the deterministic runtime-tier batteries (artifacts across
jobs, the cache round-trip, local checkout, the tool cache) live beside it.

### The runtime tier

What a lowered workflow finds at run time, standing in for the world GitHub
provides around a job:

- **The ObjectService** (`crates/github/objects`): one process per run speaking
  the results backend the 2026 toolkit converged on — twirp `ArtifactService`
  and `CacheService` under one listener, one minted token, one signed-URL blob
  implementation. Artifacts are run-scoped under the run dir; cache entries
  live in the host's persistent store (`$PETRI_STORE`, default
  `~/.cache/petri/store`, 10 GiB LRU) and outlive the run. The token is also
  the auth: the exact bearer string, salted per run, so binding beyond
  loopback for container reachability (`host.docker.internal`, guaranteed via
  `--add-host`) never leaves an open write endpoint.
- **Local checkout**: a supportable `actions/checkout` call substitutes at
  lowering for the `github/checkout` step, which materializes the workspace
  from the run's own repository — committed HEAD plus the uncommitted tracked
  diff and untracked-but-not-ignored files, `.git` and mode bits included —
  offline and token-less. Run identity is honest too: `default_params` fills
  `github.sha`/`ref` from the checkout's HEAD (run parameters, so lowering and
  replay are untouched).
- **The tool cache**: host jobs point `RUNNER_TOOL_CACHE` at
  `<store>/toolcache/<os>`; a runner image that ships a populated
  `/opt/hostedtoolcache` keeps its own.

**The GitHub API stance (v1, decided 2026-08-28): petri neither proxies nor
blocks it.** `github-script` and `gh` steps hit the real API with the ambient
token when one exists (`$GITHUB_TOKEN`, else a logged-in `gh`); without one,
public reads work and authenticated calls fail routably. **A local run with a
real token can mutate — comment, tag, release — exactly as the workflow says.**
Prefer a fine-grained read-only token when running workflows you did not
write. The corpus sweep runs token-less by policy, so corpus code can never
act with your identity. A default-deny proxy stays future work if real demand
appears.

### Findings for the spec

These are places a real workflow needs something the engine or the expression table
cannot express. They are reported, not worked around; each is rejected with the code
in brackets.

1. **There was no per-run input channel.** The `github`, `vars` and `runner` contexts
   are values a host knows and the graph does not. I added `Graph.params`: a read-only
   map merged into every firing's statics at the lowest precedence, filled in by the
   host before the run. It lives on the graph rather than in a `RunStarted` payload so
   the graph a run used is self-describing and replay needs nothing more. This is an
   IR extension, made because nothing in the package works without it; if it should
   live elsewhere, it is a small swap.

2. **`hashFiles()` cannot be a total builtin** [`unsupported.expression.hashFiles`].
   It reads the workspace at run time. Resolved for step config: a literal-pattern
   call in `run:`, `env:` or `with:` lowers to a sentinel — the second permitted
   non-literal across the executor boundary, like `$secret` — which the GitHub step
   kinds replace at spawn with the hash, computed in the job environment
   (`crates/github/actions/src/hashfiles.rs`). Resolved for step-level conditions
   too: an `if:` lowers to a config gate the step evaluates at spawn, and the
   sentinel rides in the gate's literals (the React sizebot shape,
   `if: hashFiles(...) != ''`, runs). In a position the engine evaluates — a job
   `if:`, an output, a matrix, or under another function in a condition — it
   stays rejected, and so do computed patterns.

3. **Nothing ran after a cancel** — resolved; see departure 30 and spec §5.
   `if: cancelled()` and `if: always()` cleanup can now run: cancelled outcomes
   route, admission is the structural `Node.run_on_cancel` flag, and the forced
   tier (`KillRequested`) keeps the old stop-everything semantics under its own
   logged event. The pinning test flipped into
   `cancelled_steps_run_after_a_cancel`.

4. **One scope per job, so no per-leg runtime** [`unsupported.runs_on.expression`].
   `runs-on: ${{ matrix.os }}` is the single most common reason a matrix job is
   rejected: 47 workflows. Each leg would need its own environment. Either scopes
   become per-clone, or the frontend splits a matrix over `runs-on` into one job per
   OS at lowering time.

5. **Secrets in expressions** [`unsupported.secrets.expression`]. `if: ${{ secrets.X
   != '' }}` and `${{ secrets.A || secrets.B }}` are real idioms — 12 workflows. The
   engine keeps secrets out of `EvalEnv` by construction, and the docs' context
   matrix gives conditions no `secrets` context either, so the rejection stands
   (we adhere to the docs even where GitHub accidentally accepts more). The
   sanctioned form works now: pass the secret through an environment variable and
   test `env.NAME` in the step's `if:` — the gate resolves it at spawn,
   step-side, so no value ever enters an expression the engine evaluates. A job
   output that is a whole-value secret is dropped with a warning
   (`ignored.secret_output`), as GitHub drops it.

6. **Background steps are a job-local DAG** — resolved. `background: true`
   fans a `run:` or `uses:` step out from the foreground chain. `wait`,
   `wait-all`, and `parallel` add explicit joins, and an implicit wait joins
   remaining work before post-action cleanup. A private environment channel
   defers outputs, `GITHUB_ENV`, and `GITHUB_PATH` until the join publishes the
   step. `cancel: id` stops that branch through an engine cancellation group,
   joins it, and publishes its effects. The target records `cancelled`; the
   cancel control succeeds and the foreground job continues. Matrix legs and
   deferred composite actions preserve their cancellation boundaries.

7. **Action manifests resolve when their steps run** — resolved. Remote
   references still pin at lowering, but local and pinned manifests use one
   run-time resolver path. `uses: ./localClone` and
   `uses: ./node/.github/actions/x` can name directories created by an earlier
   step. A remote composite or called workflow can also use a `./` action from
   its own pinned repository.

8. **`GITHUB_ENV` and `GITHUB_PATH`** are job-scoped mutable env across steps.
   Resolved: the `github/run` and `github/action` step kinds (`crates/github/actions`)
   accumulate them in files in the job's workspace and apply them to every later
   step, with no core support — see `.ai/plans/github-actions-runner.md`.

9. **The positional YAML reader has two gaps** [`unsupported.yaml.anchors`,
   `unsupported.yaml.multiline_flow`]: no anchors or aliases, and a multi-line
   `[ … ]` value followed by a dedent is misread. Three cpython workflows. Both are
   library limitations, named as such.

Custom shells are resolved: `python`, `pwsh` and any `{0}` template (`bash -el {0}`,
`/usr/bin/env bash {0}`) lower now — the step writes the script to a file and runs
the template over it, as GitHub does; only the Windows-only `cmd` and `powershell`
stay rejected. `concurrency:` is ignored with a warning (`ignored.concurrency`):
cross-run mutual exclusion has nothing to race in a single local run, and its
cross-run semantics stay with the multi-run driver layer (D2). Windows and macOS
runners are out of scope (`runs_on.windows`, `runs_on.macos`): the local executor
emulates Linux runners only. The declared support matrix is
`crates/github/SUPPORT.md`, held in sync with the corpus report by a harness test.
Reusable workflows now run as child invocations, including a call matrix with
a job matrix inside the child. Remaining `runs-on` expression limits are listed
in the support matrix.

### What the core may know

An audit after package 03 asked whether GitHub-specific code had leaked into the
core, since every frontend added later — CircleCI, Buildkite, RWX, Attractor — would
otherwise leave its own residue there. The rule that came out of it:

> A primitive may enter `ir`/`engine` when it is generic — a thing several formats
> would plausibly want, described on its own terms. Nothing uniquely one format's may.
> Format semantics are *compositions* of primitives, and they live in that format's
> crate.

What the audit found and changed:

- **One real leak:** the `matrix_combinations` builtin carried GitHub's
  `include`/`exclude` merging rule. Replaced by generic combinators (above); the
  GitHub rule is spelled out in `frontend_gha::expr_lower::matrix_legs`, and GitHub's
  documented examples are tested there, through the engine's evaluator.
- **Wrong layer:** the GHA expression lowering (`gha`, `gha_function`,
  `GHA_FUNCTIONS`) lived in the *shared* `frontend` crate. Moved to
  `frontend-gha`. The shared crate now owns syntax and the strict lowering only.
- **Framing:** `loose.rs` and the `loose_*` summaries described themselves as
  "GitHub's rules". The semantics are the JavaScript-family ones; they are now
  described that way. `filter_field` became `pluck_present`; `gha_format` became
  `positional_format`.
- **Kept, by judgment:** `loose_string` renders a container as its type name
  (`Array`, `Object`), which is exactly GitHub's output. A loose string coercion needs
  *some* rule for containers, and type-name makes a value that should have been a
  scalar visible instead of a JSON blob silently reaching a command line. Documented
  on those terms.
- **Runtime layer, GitHub-influenced by design:** the process step's outputs file
  accepts the `key<<DELIM` heredoc form, a superset of `$GITHUB_OUTPUT`'s format so
  that variable can be a plain alias. A compatibility choice inside one step kind,
  documented as such; the engine does not know it exists.
- **Clean:** `Status`, `RetryPolicy`, `Expansion`, `RunContext`, `Graph.params`,
  `NoopStep`, `soft_fail`, the driver, the executor. Comments that cited GitHub as
  the *reason* for a rule were reworded to state the rule.

### What the executor interface may know

The same audit, for executors. There are two today — host and Docker — and there
will be more; a cloud executor most likely. The rule mirrors the core's:

> The `executor` crate is the interface. It names no executor, and neither does
> anything above it: the IR describes the *kind* of environment a scope needs, step
> kinds reach the workspace only through the interface, and the driver translates.
> Each executor is its own crate, and adding one is adding a crate, not editing the
> interface.

What the audit found and changed:

- **`RuntimeTarget::Docker { image, args }` named an executor in the IR**, and
  `args` were Docker CLI flags. It is `RuntimeTarget::Container { image }` now: a
  *kind* of environment several executors can provide. Engine flags, mounts and
  limits are executor configuration, not something the graph carries.
- **The step kind assumed the workspace was on this machine.** The process step
  read and wrote its outputs file with `std::fs` on `ExecEnv::workspace()`, a
  host-visible path — true for a host workspace, false for anything else.
  `workspace()` is gone from the interface; `read_file` and `write_file` take its
  place. The host executor answers from the filesystem; the container executor
  answers through the sandbox's filesystem facet over the plugin wire, because a
  sandbox owns its workspace and nothing on this machine mirrors it. The step
  kind is none the wiser.
- **The shared error and report types had Docker fields.** `EnvError::Docker` is
  `EnvError::Backend { backend, operation, message }`; `ReleaseReport` describes
  what was released and kept as text rather than as `container_removed` /
  `workspace_removed` booleans that would grow a field per executor.
- **Already right when the audit ran:** the executors live outside the interface
  crate (`executor-sandbox` over the provider plugins), with a private teardown record the interface carries
  as an opaque `Teardown` trait object and hands back untouched; the output pump
  lives in the interface crate because every executor needs it and the line cap
  must be decided once.
- **Kept:** `Retention` (keep-on-failure) is generic — anything with a workspace has
  the question. `Sig` is POSIX; a remote executor maps it. Placement labels stay
  opaque in `RuntimeSpec.requirements`, and the set the GHA frontend accepts is
  GitHub's hosted-runner vocabulary, which is GitHub knowledge in the GitHub crate;
  which of those any executor honours is that executor's, and not yet plumbed.

### Departures from the frontend handoff

30. **`Graph.params`** — see finding 1.

31. **Twenty-three new builtins, one gate — and none of them GitHub's.** The
    handoff's mapping rule (no second evaluator) means GitHub's operators and
    functions must become table entries, so the question is what *kind* of entry.
    The rule adopted: a primitive may enter the core when it is generic — loose
    JavaScript-family coercion (`loose_eq`/`lt`/`le`/`gt`/`ge`/`truthy`/`number`/
    `string`), string functions (`contains_ci`, `starts_with`, `ends_with`, `format`,
    `join`, `to_json`, `from_json`), record access (`get_ci`, `values`,
    `pluck_present`, `keys`, `omit`) and record combinators (`cartesian`,
    `reject_where`, `extend_where`). Nothing uniquely GitHub's may. GitHub's matrix
    `include`/`exclude` rule, which an earlier draft had as a `matrix_combinations`
    builtin, is now a *composition* the GHA frontend emits:
    `extend_where(reject_where(cartesian(omit(m, [include, exclude])), m.exclude),
    m.include, keys(axes))` — the engine has combinators, GitHub has a use of them,
    and Buildkite's `adjustments` will compose the same pieces differently. The table
    grew from 19 to 42 and still gates dispatch. See "What the core may know" below.

32. **Coercion follows the runner, not the docs' wording.** GitHub's docs say a string
    coerces "from any legal JSON number format"; the runner uses .NET's
    `AllowLeadingSign | AllowDecimalPoint | AllowExponent`, which also takes `+1`, `01`
    and `1.`. Workflows run against the runner.

33. **`.` on a plain array is `null`, not a filter.** The handoff says "`.` on arrays
    (object-filter semantics)". In the runner, property access maps over elements only
    on a *filtered* array — the result of `*`; on an ordinary array it is `null`.
    `a.list.k` is `null` and `a.list.*.k` is the mapped list, and the test pins both.
    Filtered-ness is tracked statically in the lowering, so no runtime flag exists.

34. **Every GHA property access is `get_ci`.** Contexts are case-insensitive in
    GitHub; `Field` is not. `github.Event_Name` works, at the cost of a call per
    property.

35. **Status functions expand per site, not to the core builtins.** A step's
    `success()` means "no earlier step of this job failed, and the job was not
    cancelled out from under it", built as an expression over the earlier steps'
    `nodes.*` records (plus the `scope_cancelled` static, waived when the job's
    own gate admitted it *after* the cancel — cleanup jobs run their plain steps,
    as GitHub's do) and conjoined with "the job started"; a job's means "every
    needed job's `done` reports success and the run is not cancelled". The core's
    `success()` builtin folds immediate upstream edges, which is not GitHub's
    job-status rule. A step or job `if:` naming no status function gets
    `success() &&` in front, as GitHub does; one that names any status function
    stands alone. Job conditions stay engine preconditions; step conditions ride
    as config gates the step evaluates at spawn (which is what lets them read
    `env.*` — `GITHUB_ENV` appends included — and `hashFiles`), with the same
    expressions inside as `$expr` leaves.

36. **Jobs get `start` and `done` noop nodes.** `start` is the gate (`needs` + `if:`);
    `done` folds the job — `{ result, outputs }` — from the payload the last step's
    edge carries, one per matrix leg, and fans out to dependents. Every `needs.J.*`
    reads `J/done`. `NoopStep` is a step kind whose output is its resolved config,
    which makes a structural node also a way to compute a value.

37. **`kv` is unused by the GHA lowering.** Job outputs ride the summary payload
    instead; there was no need for run-scoped state.

38. **`secrets.misplaced` became `unsupported.secrets.expression`.** The handoff
    asked for an Error diagnostic; this is one, coded as a rejection because it is an
    engine property by construction, not a user mistake — and so it counts as
    specific in the corpus.

39. **`permissions:` and `on:` are accepted.** Both describe the GitHub side —
    token scope, triggers — and change nothing about how the graph runs locally, so
    they are not "parse and ignore" in the sense the rule forbids. `permissions` gets
    an `ignored.permissions` warning so nobody wonders; `on:` is the host's business.

40. **Fuzzing is proptest, not cargo-fuzz.** Random text, random bytes lossily
    decoded, and random glued syntax fragments, 600 cases each, assert the parser
    never panics. cargo-fuzz needs nightly; this runs in the ordinary suite.

41. **`serde_json` has `preserve_order` on, workspace-wide.** Matrix axes,
    `include` order and outputs follow document order in GitHub; a sorted map
    silently reordered them (the matrix test caught it).

42. **The driver now records `StepStarted`.** It never had, only the test harness
    did — so a real run's log had no start events. A package 02 gap, found by the first
    test that read a real driver's log for them.

43. **Reusable-workflow callers are not asked for `runs-on`**, `strategy:` without
    `matrix:` is legal, `uses: ./` is the repository root, and current GitHub-hosted
    labels (`ubuntu-slim`, `ubuntu-26.04`, `ubuntu-24.04-arm`, `macos-15`) are known.
    All four came from the corpus; each cost a real workflow a generic error before.
    (macOS labels have since moved out of scope — `runs_on.macos`; the local executor
    emulates Linux runners only.)

44. **`KNOWN_RUNS_ON` lives in the frontend.** The handoff puts label mapping in the
    executor; `petri check` still needs to reject unknown labels without one, so the
    frontend carries the list the local executor honours. Third-party runner labels
    (`depot-*`, `namespace-profile-*`, `*-16-core-*`) are rejected per label: 23
    workflows.

45. **Composite outputs are expression substitution.** `steps.<caller>.outputs.<n>`
    resolves to the action's declared `value:` expression, lowered in the composite's
    own site — no node, no record.

46. **`GITHUB_OUTPUT` is an alias**, via `ProcessConfig.output_env_aliases`. A `run:`
    step that appends to it lands in the outputs file without a shim.

## Testing notes

Three things this package taught, kept because they generalise:

- **Assert the duration, not just the result.** A `docker exec` that returns early
  looks exactly like a step that finished fast. Pinning elapsed time alongside the
  exit status in `docker_wait_follows_the_step_not_the_client` is what makes that
  regression loud instead of silent.
- **A cancellation test that only checks the process is gone will pass on teardown
  alone.** Releasing the scope kills everything either way, so the test has to pin
  *how* the step ended — which signal, which escalation — not merely that it did.
- **Docker tests skip without a plugin and a daemon, and `PETRI_REQUIRE_DOCKER`
  turns that skip into a failure.** CI sets it on the Linux job. A silently skipped
  acceptance battery is indistinguishable from a passing one, and that job exists
  precisely to say the battery ran. The live Daytona tier follows the same
  convention with `PETRI_REQUIRE_DAYTONA`, which `mise run test:daytona` sets,
  and so does the Lean model check with `PETRI_REQUIRE_LEAN_MODEL`, which
  `mise run test:lean` sets.
- **A container test reads the workspace through the sandbox, never through a
  host path.** The workspace lives in the sandbox's own volume. A test checks a
  file the step wrote with `ExecEnv::read_file`, or with `docker exec` through
  `testkit::container_read` when it has no environment handle; a heartbeat that
  must be seen to stop is measured by a step inside the sandbox and reported in
  its output.

## Not built

Out of scope per executor §0, and not built: GHA action shims and the JS action host,
artifact and cache stores, remote or distributed executors, Windows, service
containers, and cpu/memory limits. The `LogSink` writes to the run directory and
optionally stdout; richer sinks come later.

Deliberately out of scope, per the core handoff: cross-run concurrency groups
(D2 — the engine carries no concurrency semantics; the GHA frontend ignores
`concurrency:` with a warning, a single local run having nothing to race), and
placement *semantics* for `RuntimeSpec.requirements` (D3 — the labels are
carried, uninterpreted).

Still v2 in the design document: content caching, remote scope placement, and
`Control::{Pause, Steer, Approve}`. The seams are in place — the log is versioned and
rejects old versions cleanly, `EngineState` serializes whole, `StepKind::fingerprint`
defaults to `None`, and `Control` is `#[non_exhaustive]` (`Kill` was its first
addition). Replay has landed; `engine::verify_replay` is the determinism canary.

## The terminal path

`petri run <workflow>` is the standalone runner: one invocation loads,
validates and executes one root workflow with no server, database or UI. It
prints the run dir first, then every step's output on stderr as
`[<node>#<firing>] <line>`, or `[invocation-<N>/<node>#<firing>] <line>` for a
parallel branch (a child invocation), so interleaved output stays attributable
(each line is masked and cut at 4096 characters; a stage's echo stops at 64 KiB
with one marker naming its log file, which keeps every line; a scheduled retry
is announced as `retry: attempt N of M in Ss`; `--quiet` turns the echo off),
then one `<status> <node>` line per finished node, the retained workspace
paths, and `run: <status>`. The exit code is 0 for success, 1 for a failed or
cancelled run, 2 for a usage error, 3 for a host error, and 4 when the run
finished but its interview did not go as scripted.

**Answering questions.** A `attractor/human` gate, and a native agent's question
tool, ask through the core `Question` event; the host's `Interviewer` answers
(`execution::Interviewer`, an open trait a product host implements too). The
CLI ships three, one per option, and they exclude one another:

- `--interactive` prints each question on stderr and reads one line from
  stdin. Questions from parallel stages are serialized; a `yes_no` or
  `confirmation` gate takes Enter as its default; a `multi_select` gate takes
  comma-separated keys; a `freeform` gate takes text. EOF or three invalid
  answers fail the gate closed.
- `--auto-approve` takes the default choice, or empty text.
- `--interview-script <file>` answers from a versioned JSON script and fails
  the run when a question matches no entry, matches several, exhausts an
  entry's `count`, or a required entry goes unused. It never falls back to the
  terminal or to auto-approval.

An interview script:

```json
{
  "version": 1,
  "entries": [
    {
      "id": "ship-it",
      "match": { "node": "gate", "kind": "yes_no", "options": ["Y", "N"] },
      "count": 1,
      "action": { "kind": "choice", "value": "Y" }
    }
  ]
}
```

`match` fields (all optional, all must hold): `node`, `invocation_path` (`/`
for the root, `/<slot>` per nested call), `occurrence` (which distinct question
of that node, 1-based), `ask` (which time the same question was asked, after a
rejected answer), `kind`, `text`, `text_contains`, `options` (the offered keys
in order), `default`, `freeform`, `sensitive`, `reference_url_contains` (a
`review_target` gate's URL). Actions: `choice` (`value`), `choices`
(`values`, for `multi_select`; Fabro's `multi_selected` `option_keys`), `text`
(`value`), `negative` (the `N`/`no` option), `invalid` (`value`, sent as a
choice the step must reject; the re-ask matches `"ask": 2`), `cancel`,
`withhold` (no reply until the question expires or is cancelled, so a gate
with a `timeout` expires into its `human.default_choice` or Fabro's retry
outcome and the receipt records the question as `timed_out`).
`delay_ms` waits before acting; `required: false` lets an entry go unused.

The terminal shows a `review_target` gate's reference as `review: <label>
<url>` and a gate's answer deadline as `(answer within 90s)`. Steering is not
answering: a control line (below) never consumes a pending question's answer.

**Controls.** `--control <file>` tails a file for run controls while the run is
live, one per appended line: `pause` holds every attempt not yet admitted
(running work continues, and Ctrl-C still cancels), `unpause` releases them,
`steer <node> <text>` delivers guidance to the named stage's live firing (an
agent queues it for its session; a human gate ignores it), `interrupt <node>
[text]` stops the named agent stage's current model turn and keeps its
session (the text, else the next `steer`, is the stage's next input; a stage
with no turn in flight refuses it), and `cancel` cancels the run (a second
`cancel` reaches the kill tier). Each line's effect is reported as
`control: ...` on stderr; a line that is not a command, names a stage that is
not running, or interrupts a stage with no model turn is reported and
skipped. An embedded host drives the same
`execution::controls::ControlService`, installing its live-turn set as a
capability so `interrupt` can find a turn.

The pause is durable. Each `pause` and `unpause` is a coordinator record
(`RunPaused`, `RunUnpaused`), so `petri inspect` reports `paused` and a resume
of a run whose last control was a pause starts with admission held.

**Resuming.** `petri resume --run-dir <dir>` continues a run whose process
died. It needs only the run directory: not the workflow file, not its inputs.
The workflow's format is found from the stored root graph
(`Frontend::claims_graph`), and its launch settings and default retention
apply as they did to `petri run`. The session options are given again and
apply to the resumed session: `--interactive`, `--auto-approve`,
`--interview-script`, `--control`, `--retain`, `--quiet`, `--dry-run`, and the
provider and runner options. Finished work is not repeated; the node that was
in flight at the crash starts again. A gate that was waiting asks again, and
the resumed interviewer answers it; the interview receipt written beside the
run is the resumed session's. A paused run stays paused until an `unpause`
arrives through `--control`, which a resume tails from its current end, so
lines the earlier process applied are not applied again. Before anything
starts, the command reads the coordinator log without the lease and refuses
with exit code 2 and no work done: a run that already finished (`already
finished (<status>); nothing to resume`), a run another process holds (the
lease on `run.json`), a paused run given no `--control` file, and a run
directory that is missing or does not decode. What a resume does not restore:
an agent node's retained thread. As in Fabro, whose `AgentApiBackend` keeps
full-fidelity sessions in memory per worker, a node that was mid-turn starts a
fresh session from the `summary:high` preamble.

**Policies.** A Fabro graph's `stall_timeout` (default 30 minutes) cancels a
run that emits no execution event for that long; a pending question parks the
clock, and the terminal prints `stall watchdog: no execution activity for N
s`. Its `loop_restart_signature_limit` (default 3) fails a run whose node
repeats one deterministic failure that many times and blocks a `loop_restart`
edge taken by anything but a transient failure. See `crates/attractor/FORMAT.md`,
"Watchdog and circuit breaker".

**The receipt.** Every run with an interviewer writes
`<run-dir>/interviews.json` (`execution::InterviewReceipt`, version 2) each
time a question's outcome is recorded, so a crash loses only the questions
still waiting, which the resumed run asks again. A resumed run continues the
receipt it finds: the receipt's `lifetime` counts the processes that wrote
it, and each record carries the `lifetime` that asked it. One
record per question with its invocation, execution, firing, attempt, node,
occurrence, ask, question id, kind, text, offered option keys, the review
`reference` and `timeout_ms` when the question had them, the reply
(`answered` with `choice`/`choices`/`text`, `cancelled`, `failed`, or
`timed_out` with the `default` the gate took on its own when it had one: the
gate's own report of its expiry, never inferred from how the firing ended),
and how it left (`delivered`, `not_live`, `late`, `shutdown`, `withheld`,
`expired`); the
`errors` list; and under `script`, a scripted interviewer's per-entry
`consumed`/`remaining` counts. The records are in the receipt order: by
invocation path, then invocation, execution, firing, occurrence, and ask
(the root's questions first, then each nested invocation's in path order;
within an invocation, the order the run asked them), whatever order the
answers arrived in, so a re-asked question follows its original ask, and a
question asked again after a resume follows the earlier process's. A
sensitive answer appears only as its
`{"$secret": "answer:<id>"}` reference. A non-empty `errors` list is exit
code 4, whatever the engine status; the persisted run is not rewritten.

**Model selection.** `petri run --model <name>` and `--provider <id>` choose
the model for every prompt or agent node that names none of its own, over the
graph's `default_model`/`default_provider` and `[run.model]` in `workflow.toml`
(and the project and settings layers), as `fabro run --model` and `--provider`
do. Each applies on its own. A node that names its own model, by attribute or
stylesheet rule, keeps it, and keeps its provider to the node and the defaults.
`--provider` alone, where nothing names a model, runs the provider's default
model from the runner's catalog (`openai` is `gpt-5.6-sol`); the pinned
interview bundle relies on this. A host can also bind its own last-resort
default (`petri.default_model`, `petri.default_provider`), below every file
layer. The launch lands in the persisted root graph's `fabro.launch` parameter
(`model`, `provider`, `default_model`, `default_provider`, as given), so
`petri inspect` and a replay see it; `petri replay` takes the same options so
the graph lowers the same. `petri resume` needs nothing: the stored graph
already carries the choice.

**Environment selection.** `petri run --environment <id>` selects the
`[environments.<id>]` table the run executes in over what any settings layer's
`[run.environment]` names, as `fabro run --environment` does; the table may
come from `workflow.toml`, `.fabro/project.toml` or the user settings layer
(`$FABRO_HOME/settings.toml`), which is how a bundle whose project file selects
a Daytona environment still runs on the host. The resolved environment lands
in the persisted root graph's `fabro.environment` parameter.

**Retention.** `--retain always|on-failure|never` decides what happens to the
run's workspaces at teardown. The default is the workflow format's: Fabro
keeps every workspace (its result is the files), other formats keep a failed
run's and delete a successful run's. The run reports each retained workspace:
a host workspace as its path under the run dir, a container's as its sandbox
and the `petri sandbox prune --run-dir <dir>` command that deletes it.

**Provider configuration.** Native agents (`backend="api"`) use the
distribution's `lithos-llm` client (`petri::llm_client`). Credentials are the
provider library's conventional variables (`OPENAI_API_KEY`,
`ANTHROPIC_API_KEY`, and so on), read per request. `PETRI_LLM_CATALOG` names
one or more catalog TOML files (path-separator delimited) layered over the
built-in catalog, so a test or a gateway deployment redirects a provider:

```toml
schema_version = 1
[providers.openai]
base_url = "http://127.0.0.1:3000"
```

`PETRI_LLM_PROVIDERS` (comma-separated provider ids) narrows which providers
may be routed to at all; a model on any other provider is unavailable. An
unreadable or invalid layer leaves the client unbuilt and every native agent
node failing with `pebble_unconfigured`, rather than reaching a live
endpoint. Fabro prompt nodes (`tab`) use the same client for their one
tool-free call.

Three retry mechanisms exist and each has one owner. `PETRI_LLM_RETRY_ATTEMPTS`
(default 3; `1` disables) is the client's own budget for sending one request
again on the same route after a retryable failure, with exponential backoff and
the provider's `Retry-After` honored; `PETRI_LLM_TIMEOUT_MS` bounds one call,
retries included. Pebble replays a model turn whose response stream broke, on
its default policy. Both report on the agent's event stream as Pebble's
`LlmRetry`. Fabro's `[run.model.fallbacks]` chain is planned by Petri and run
by Pebble, and starts only once both are spent; see "Model fallback" in
`crates/attractor/FORMAT.md`. A workflow retry (`retries` on a node) is a new
attempt with a new plan and is none of these.

**Workflow secrets.** A Fabro `workflow.toml` environment value written as
`{{ secrets.NAME }}` is a secret reference. The standalone runner resolves it
from the environment variable `PETRI_SECRET_NAME` when the command that
carries it starts, masks the value in every log, and fails the command with
`secret_unavailable` when the variable is unset. An embedding host supplies
its own `SecretProvider` instead.

## The embedding path

A host that embeds Petri (a platform running Fabro workflows, say) uses the
same `Runtime` the CLI does. It checks a workflow with `Runtime::check` from
a file on disk, or with `Runtime::check_source` from text it holds in memory
beside a `frontend::FileSource` for the settings files and `@file`
references (`frontend::MapFiles` is a map of paths to text), and adds three
things, all Petri-owned types with no platform vocabulary in them:

- **Events.** `execution::events` is the versioned public event contract
  (`crates/core/execution/EVENTS.md`). An `EventProjector` is an
  `ExecutionObserver` that derives `RunEvent`s from every record and hands
  them to the host's `RunEventSink`, awaited per event through a bounded
  queue (`ProjectorOptions`: 1024 events and a 30 second stall budget by
  default), so a slow store delays delivery but never slows the run or
  grows memory; what finds the queue full, or follows a failed or stalled
  sink, is counted in the `ProjectionReceipt` and stays in the durable log.
  `replay_run` rebuilds the same events, with the same identities, from a
  run's store after the fact (`replay_run_dir` over a run directory),
  `replay_since` hands back the events past held positions, and
  `EventProjector::primed` attaches at resume. Every event names its run, invocation, execution,
  node (with the frontend's `meta`), firing, visit, attempt and branch role.
  One vocabulary for records and events: every event is named after its
  record and carries the stored log line unchanged under `record`, with what
  Petri derived beside it under `derived`; `verify_export` proves the
  exported records are the stored logs at the end of every run beside
  `verify_replay`.
- **Admission passes.** `Runtime::admission` registers an `AdmissionPass`
  that `Runtime::check` runs over every graph the step registry accepted,
  the root and the pre-lowered children, with the runtime's static
  capabilities: how a component resolves what the frontend cannot and
  writes it into the graph a run persists. A problem is an error diagnostic
  on the node's `meta.span`; a changed child's digest is followed through
  the parent's reference. The Attractor component's pass pins every LLM
  node's model against the host's catalog (`crates/attractor/FORMAT.md`,
  "Model resolution at admission").
- **Awaited extension points.** `Runtime::hooks` installs
  `driver::lifecycle::ExecutionHooks`: `before_attempt` (pause, skip or block
  an attempt), `prepare_result` (adjust the effective result; the original is
  recorded beside it), `after_record`, and `transition` (override the selected
  route, report best-effort problems, or fail advancement). Callbacks see an
  immutable `FiringView` and change the run only through their return values.
  Without hooks the driver's fast path is unchanged.
- **Hook service.** `execution::hooks::HookService` is the one interface a
  hook executor implements (`crates/core/execution/HOOKS.md`); the
  `HookAdapter` is its one caller from workflow points, so the local hook
  executor and a platform's hook service both run each configured hook once.
  `attractor_steps::hooks::LocalHooks` is the local one: `attractor_steps::register`
  installs it unless the host installed a service first, and it serves the
  per-firing points, the native agent's tool boundary, the ACP client's
  permission requests and the run-level events from one place
  (`crates/attractor/FORMAT.md`, "Hooks").
- **Host tools.** `attractor_steps::host_tools::HostTools` is a capability
  holding builders of Pebble `RegisteredTool`s. Every native agent session
  calls them once with a `HostToolContext` (run key, invocation, execution,
  node, firing, attempt) and registers the tools beside Pebble's own, so a
  host tool runs under the same tool hooks, is recorded on the same event
  stream under the stage's identity, and reaches a sub-agent through
  Pebble's own rule when marked `allow_in_subagents`. The standalone runner
  registers none.
- **Questions.** `execution::Interviewer` and the `InterviewDispatcher`, as
  on the terminal path.
- **Forks.** `execution::host::fork_from` seeds a new run from a stored
  run's records up to a position (an execution and a firing of the root
  invocation): the same graphs, the position execution's log cut after the
  firing's routing, the finished children the kept firings called, and a
  run declaration whose `forked_from` names the source. No sandbox lease
  is carried over; the host continues the new run with
  `host::resume_configured`, restoring whatever it wants in the fresh
  workspace from `scope_acquired`. `ForkOptions::rerun_last` runs the
  position's firing again instead of keeping its finish. The design is
  `crates/core/execution/FORK.md`.

`crates/petri/lib/tests/embedding.rs` is the worked example: a Fabro workflow
run without adapters, then with fake adapters that exercise every point, with
the timeline, attempts, branches, question, outcomes and accounting
reconstructed from public events alone.

## Local run layout

The standalone host stores one root run as `Run → Invocation → Execution → Firing`
through the store seam (`crates/core/store`): every durable thing is a log of
records or a content-addressed blob, and a backend stores what it is given. The
run directory is the `RunDirStore` layout of that store. `coordinator.jsonl`
records graph registrations, invocation calls, execution successors, and final
results; `resources.jsonl` records every sandbox lease transition; each execution
keeps an independent engine log. `petri inspect` reads this layout back.

```text
<run-dir>/
  run.json                 the run's key, and the file the writer lease locks
  coordinator.jsonl
  resources.jsonl
  graphs/<sha256>.json
  executions/<execution-id>/events.jsonl
```

A line is the record's JSON value, `{seq, origin, recorded_at, body}`, the same
value a public event carries under `record`; there is no header line, and the
engine log version is pinned by the run format version on the run declaration.
A restart creates a successor execution in the same invocation. It starts at the
selected target with empty context and carried firing budgets. Nested workflow calls
create invocations, not separately managed runs. The coordinator holds the run's
exclusive writer lease (a lock on `run.json`) while it creates or resumes the run;
a reader takes no lease. A host with a store of its own installs it with
`Runtime::store` and keeps the run directory for what is a file by nature
(workspaces, step output). `petri resume --run-dir` continues the run from this
layout alone; `RunPaused` and `RunUnpaused` records in `coordinator.jsonl` make a
pause durable across it.
