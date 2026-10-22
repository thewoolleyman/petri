# The Fabro layer, as lowered

Petri runs Fabro workflows: an Attractor graph (`*.fabro`, `*.dot`) with
Fabro's settings files around it. The language, every construct in the DOT
file, and what the graph runs are
[`crates/attractor/FORMAT.md`](../attractor/FORMAT.md). This page says how
each section of Fabro's files resolves into the run settings that lowering
applies, and what the Fabro launch keeps for itself. The Fabro frontend
(`crates/fabro/frontend`) reads the files the way the pinned Fabro reads
them, then lowers through the Attractor frontend. The reference Fabro
revision, the required workflow bundles, the feature matrix, and the
accepted differences are frozen in
[`crates/fabro/acceptance/CONTRACT.md`](acceptance/CONTRACT.md).

What a host that embeds Petri relies on (interfaces, identities, event
positions, acknowledgements, versions) and what stays Fabro's platform's is
[`crates/fabro/HANDOFF.md`](HANDOFF.md).

## The files

Three layers, lowest first: the operator's `~/.fabro/settings.toml`
(`$FABRO_HOME/settings.toml` when set; the host reads it and binds the text
as the `fabro.settings_toml` compile variable, so lowering reads no
environment), `.fabro/project.toml` at the bundle root, and `workflow.toml`
beside the workflow file. The bundle root is the nearest ancestor holding a
`.fabro` directory, the root Fabro resolves `fabro/...` paths against; a file
inside the bundle belongs to the bundle's parent. Every section below is
read once at load and resolved into a value the lowering applies; a file that
names a setting and cannot be read is an error, never a silent skip. A host
that embeds Petri binds the lowest layer's text itself
(`Fabro::with_settings_toml`) and may bind its MCP catalog beside it
(`Fabro::with_mcp_catalog_toml`, the `fabro.mcp_catalog_toml` variable): a
Fabro server hands its environment catalog to Petri as `[environments.<id>]`
tables of the settings layer, and its MCP catalog through the second
variable, so a bundle can name either without declaring it.

## Sections

| Fabro | At load |
|---|---|
| `[workflow]` `name`, `description`, `graph`, `metadata`, `engine` | accepted and read by nothing: `engine = "petri"` or `"legacy"` is the key Fabro reads to choose the engine, and Petri is the engine, so the value is not inspected. Any other `[workflow]` key is `unsupported.workflow_toml.key`, as Fabro's parser refuses it |
| `[run.inputs]` in `workflow.toml` beside the file | input defaults, under the host's `--input` / `--inputs-file` |
| `[run] goal` (text or `{ file }`) | the run goal, over the graph's own `goal` attribute, which is the default when no settings state one. Above `workflow.toml` sits the launch: `petri run --goal <text>` (bound as the `petri.launch_goal` compile variable, which a host binds to the goal its run resolved, an intent's override included) replaces `[run] goal`. One rule, as Fabro's run materialization orders them: launch goal, else `[run] goal`, else the graph's `goal`. Both settings forms render `{{ inputs.* }}` and `{{ vars.* }}` at load, as the graph's goal does, and the goal that wins is the one `{{ goal }}`, every agent and prompt stage, and `Graph.params["goal"]` see |
| `[run.model]` `provider`, `name`, `controls.reasoning_effort`, `controls.speed` | the model, provider, reasoning effort and speed an agent or prompt node gets when neither it nor the graph (`default_model`, `default_provider`) names one. The launch sits on both sides of the file layers, as Fabro's run settings order them. Above them: `petri run --model`, `--provider` (bound as the `petri.launch_model` and `petri.launch_provider` compile variables, which a host binds to its own explicit flags, as Fabro's `fabro run --model`, `--provider`) override `[run.model]` and the graph's defaults, each on its own, on every agent and prompt node that names no model of its own, by attribute or stylesheet rule. A node that names its own model keeps it and gets no launch provider either: its provider comes from the node and the defaults. A provider alone, where nothing names a model, runs its default model from the runner's catalog. Below them: a host's own default (`petri.default_model`, `petri.default_provider`, such as a server's catalog default; `petri run` binds neither) fills the name and provider nothing else set. Blank values count as unset. `Graph.params["fabro.launch"]` records all four as given (`model`, `provider`, `default_model`, `default_provider`) |
| `[run.model.fallbacks]` `"<model>" = ["provider:model", ...]` | Fabro's model-keyed fallback chains ("Model fallback" in `crates/attractor/FORMAT.md`). The frontend checks the shape (a table keyed by a requested model; each entry a bare token, `provider:selector`, or the legacy `provider/selector`; a provider-qualified key is refused as Fabro refuses it, `fabro.model_fallbacks`) and puts the chains on every agent and prompt node config under `fallbacks`; the runner resolves them against its catalog at the first LLM stage |
| `[run.execution]` `mode`, `approval` | launch defaults in `Graph.params["fabro.launch"]`: `mode = "dry_run"` runs the stub registry and simulates every sandbox (no provider plugin is launched, whatever `[run.environment]` selects), `approval = "auto"` answers every question with its first choice. `--dry-run`, `--auto-approve`, `--interactive` and `--interview-script` win |
| `[run.clone]` `enabled`, `depth` | the server clones the repository into the sandbox before the first stage | the root `start` stage checks the repository out into its workspace before anything runs there: a clone of the repository the run was loaded from (`--repo`, else the bundle root above the workflow file; the runtime binds it as the `petri.repository` compile variable, absolute) at `depth` commits (Fabro's default 100; `0` is the full history), packed on the host and delivered through the scope's executor to the environment's own `tar`, so a Docker workspace receives the same files as a host one. `tar` extracts with `--no-same-owner`, so the files belong to the user the stages run as, never to the host user that packed them, and `git` in a root sandbox does not refuse the workspace as a repository of dubious ownership. The clone's `origin` is the repository's own `origin` when it has one; nothing is fetched. `enabled = false` starts from an empty workspace. A file outside a Git work tree, or a host that lowers in memory without binding the variable, also starts empty, with a `checkout:` log line saying so. The launch parameter carries `clone` (`enabled`, `depth`, `repository`); a delivered checkout logs `checkout: <root> at <commit> (depth N)` and emits an `attractor.checkout` `StepEvent::Custom` (`repository`, `commit`, `depth`, `files`) |
| `[run.model]` in `.fabro/project.toml` and the host's user settings layer | the three layers combine, settings under project under workflow | the same order: a `[run.model]` key `workflow.toml` leaves unset is filled from `.fabro/project.toml`, then from the settings layer (`fabro.settings_toml`, which `petri` binds from `$FABRO_HOME/settings.toml`, else `~/.fabro/settings.toml`). This is how a workflow that names no model, such as the pinned interview workflow, gets the operator's default |
| `[run.environment]` `id` over `[environments.<id>]` | `provider` selects the sandbox backend when `--backend` is not given: `local` is the host, `docker` the Docker plugin, `daytona` the Daytona plugin; a dry run selects none, since it acquires every scope on the simulated provider. `image.docker` becomes the scope's container image under `docker` and `daytona`, and warns `ignored.workflow_toml.environments.<id>.image` under `local`, which runs on the host as Fabro does. `env` is the scope environment; a value that is exactly `{{ secrets.NAME }}` is a `$secret` reference every command resolves at spawn (the standalone runner reads `PETRI_SECRET_NAME`; a missing secret fails the command with `secret_unavailable`) and masks in every log. `resources` size a Daytona runner, and warn `ignored.workflow_toml.environments.<id>.resources` under `local` and `docker`, which run unconstrained here. The other keys Fabro's environment table accepts, `cwd`, `network`, `lifecycle`, `labels` and `image.dockerfile`, are the platform's: known keys, read by nothing, accepted silently in every layer (the platform acts on them around the engine; the standalone runner builds no image and runs on `image.docker` or the backend's default runner image). A key outside Fabro's table warns `ignored.workflow_toml.environments.<id>.<key>` rather than refusing the run, since the host's layer is Fabro's own catalog. Both tables are read from every layer, the settings layer under `.fabro/project.toml` under `workflow.toml`, and merge key by key as Fabro's `combine` merges them (a higher layer's `image.docker` wins, a lower layer's `resources` still apply, `env` keys combine), so a bundle can name an environment only the host's layer declares, and a bundle with no `[run.environment]` runs in the one the settings layer selects; every diagnostic names the layer its setting came from. Above every layer sits the launch: `petri run --environment <id>` (bound as the `petri.launch_environment` compile variable, which a host binds to the environment its run selected) selects the id over every layer's `[run.environment]`, as `fabro run --environment` does. An `id` no layer declares is an error, as is a provider that is not `local`, `docker`, `daytona`, or a provider kind (lowercase letters, digits, interior hyphens; `simulated` is reserved). Any other valid kind names a provider the host registers (`--sandbox-providers`, see `crates/core/executor-sandbox/PROVIDERS.md`) and is carried through as written; `image` is refused under it (`unsupported.workflow_toml.environments.custom`), since its sandbox comes from the registration; `resources` lower as they do under `daytona` and size the run's sandbox over the registration's template, field by field; and `env` applies as everywhere |
| `[run.prepare]` `steps`, `timeout` | setup steps lowered as command nodes `run_prepare_1`, `run_prepare_2`, ... between `start` and its successors, so they run in the selected environment before any node, in order, each with the section's `timeout` (default `5m`), its `env`, and `on_failure="exit"`: a failed step ends the run before the first node. `command` argv is joined with shell quoting; `script` runs as written; `{{ inputs.* }}`, `{{ vars.* }}` and `{{ goal }}` render at load. The whole file is validated before any step runs |
| `[[run.hooks]]` in `workflow.toml`, `.fabro/project.toml` at the repository root, and the host's user settings layer | local hooks ("Hooks" in `crates/attractor/FORMAT.md`). Each layer is read on its own and the three merge as Fabro's `combine_hooks` does: settings, then project, then workflow, a higher layer's entry replacing a lower one with the same `id` in place and the rest appending. Every field is validated at load (`fabro.hooks.toml`, `.entry`, `.event`, `.transport`, `.timeout`, `.matcher`); a hooks layer that cannot be read is an error, so a configured hook is never skipped silently. A `checkpoint_saved` hook warns `fabro.hooks.checkpoint_saved` and never runs. The merged list lands in `Graph.params["attractor.hooks"]` and on the `start` and `exit` stages. The user layer (`~/.fabro/settings.toml`) is outside the repository, so the host passes its text as the `fabro.settings_toml` compile variable when it wants one |
| `[run.agent.mcps.<name>]` in `workflow.toml`, `.fabro/project.toml` and the host's user settings layer | MCP servers for native agent nodes ("MCP servers" in `crates/attractor/FORMAT.md`). Each layer is read on its own with Fabro's field rules (`type` is `stdio`, `http` or `sandbox`; exactly one of `script` and `command`; `url`; `port`; `protocol` on `http` and `sandbox`, `streamable_http` by default or `sse`; `env`, `headers`; `startup_timeout` default `10s`, `tool_timeout` default `60s`; `enabled`), the three merge by name with the higher layer replacing the lower one whole (Fabro's sticky map) and `enabled = false` removing the name, and the merged list is carried on every agent node's config (`mcps`) and into nested workflows. `{{ inputs.* }}`, `{{ vars.* }}` and `{{ goal }}` substitute at load; a value under `env` or `headers` that is exactly `{{ secrets.NAME }}` is a `$secret` reference resolved when the server launches. A reference entry (`id = "<catalog id>"`, with `enabled`) resolves against the MCP catalog the host binds as `fabro.mcp_catalog_toml`, a TOML table keyed by catalog id with each entry in the inline shape: the entry is read with the inline rules under the reference's name, its `source` `mcp-catalog:<id>`, as Fabro's `resolve_mcp_entries` resolves it. Errors: `fabro.mcps.entry`, `fabro.mcps.type`, `fabro.mcps.shape`, `fabro.mcps.toml` (a layer that names servers and does not parse), `fabro.mcps.unbound`, `fabro.mcps.env_token` (`{{ env.* }}`, refused as Fabro refuses it), `fabro.mcps.reference` (an id the bound catalog does not have), `fabro.mcps.catalog` (a catalog that does not parse, or an entry that is not an inline server); `unsupported.workflow_toml.run.agent.mcps.reference` (`id = ...` with no catalog bound: the standalone runner has none), `unsupported.workflow_toml.run.agent.mcps.secret` (a secret token anywhere but a whole `env` or `headers` value) |
| other sections in `workflow.toml` | every section is diagnosed, none is dropped silently. Platform-only sections warn `ignored.workflow_toml.<section>` with why (`[run.working_dir]`, `[run.metadata]`, `[run.run_branch]`, `[run.meta_branch]`, `[run.pull_request]`, `[run.git]`, `[run.integrations]`, `[run.checkpoint]`, `[run.artifacts]`, `[run.notifications]`, `[run.interviews]`, `[run.scm]`, `[run.agent] fabro_tools`, and the top-level `[project]`, `[cli]`, `[server]`, `[llm]`). A requirement the standalone runner cannot meet is a specific `unsupported.workflow_toml.*` error (the MCP row above lists its three). A key Fabro's parser refuses (a legacy top-level key, an unknown `[run]` key, `_version` other than 1) is `unsupported.workflow_toml.key` / `unsupported.workflow_toml.version` with Fabro's rename hint. See `crates/fabro/acceptance/CONTRACT.md` for the per-option table |

The launch settings `workflow.toml` declared land in
`Graph.params["fabro.launch"]` (`sandbox_backend`, `dry_run`, `auto_approve`,
the Daytona sizes), beside the launch's model choice and the host's default as
given (`model`, `provider`, `default_model`, `default_provider`; `null` when
the launch or the host named none) and the `clone`
record (`enabled`, `depth`, `repository`), and the resolved environment in
`Graph.params["fabro.environment"]`; the CLI reads them back through
`Frontend::launch_settings` when it starts the run. The Fabro frontend adds
both after the Attractor lowering returns; the language's own parameters
(`inputs`, `vars`, `goal`, `attractor.hooks`, `attractor.workflow`) are the
lowering's.

## Diagnostics

What the Fabro frontend raises keeps the `fabro` prefix: `fabro.workflow_toml`
(a file that is not TOML), `fabro.hooks.*` and `fabro.mcps.*` (a malformed
entry in any layer or in the host's MCP catalog), `fabro.model_fallbacks` (a chain table Fabro's parser
would refuse), `fabro.petri_extension` (`[run.agent] skills`, which Fabro
refuses), and the `unsupported.workflow_toml.*` and
`ignored.workflow_toml.*` families ("other sections" above). Everything the
DOT file itself raises is the language's, `attractor.*`.

Workflow environment secret references also reach agent processes: an ACP
agent however it is named (`acp.command`, `acp.config` or `PETRI_ACP_COMMAND`),
and a native agent's tool shells. They resolve when the agent starts, and a
missing one fails the agent node with `secret_unavailable` (Fabro failed the
whole run at start instead). Explicit `acp.config.env` entries override the
workflow environment for the same name. That is the reverse of Fabro, where
the workflow environment won. As in Fabro, an ACP agent gets no product API
key the workflow does not name. Secret values never enter the admitted graph or
unmasked run records. See "ACP products" in the Attractor format.
