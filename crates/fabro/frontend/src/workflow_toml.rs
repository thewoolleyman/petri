//! `workflow.toml` beside the workflow file: every section Fabro's settings
//! parser accepts, read here into what the standalone runner acts on, warns
//! about, or refuses.
//!
//! The readiness plan's rule applies to every section: a platform-only
//! option warns with why (`ignored.workflow_toml.<section>`), a requirement
//! the runner cannot meet is a specific `unsupported.workflow_toml.*` error
//! before any node runs, and a key Fabro's own parser refuses is an error
//! with Fabro's rename hint. Nothing is dropped silently.
//!
//! What the runner acts on:
//!
//! - `[run.inputs]`: input defaults under the host's `--input`.
//! - `[run] goal`: the run goal, over the graph's own `goal` attribute, as
//!   Fabro's run materialization orders them. The `{ file }` form reads beside
//!   `workflow.toml`. Above it sits the launch: `petri run --goal`, or the goal
//!   a host's run stated, bound as `petri.launch_goal`, replaces it. Both
//!   render `{{ inputs.* }}` and `{{ vars.* }}` as the file's other settings
//!   strings do.
//! - `[run.model]`: the default `provider`, `name`, `reasoning_effort` and
//!   `speed` an agent or prompt node gets when neither it nor the graph sets
//!   one. `[run.model.fallbacks]` is read by [`crate::fallbacks`].
//! - `[run.execution]`: `mode = "dry_run"` and `approval = "auto"` become the
//!   run's launch defaults; `--dry-run` and `--auto-approve` still win.
//! - `[run.environment]` and `[environments.<id>]`: the `provider` selects the
//!   sandbox backend (`local` is the host, `docker` the Docker plugin,
//!   `daytona` the Daytona plugin) when `--backend` is not given;
//!   `image.docker` is the scope's container image under those two; `env` is
//!   the scope environment, with `{{ secrets.NAME }}` as a `$secret` reference
//!   resolved at spawn; `resources` are the Daytona runner size, or a
//!   registered provider's sandbox size over its template. The other keys
//!   Fabro's environment table accepts (`cwd`, `network`, `lifecycle`,
//!   `labels`, `image.dockerfile`) are the Fabro platform's: known, read by
//!   nothing here, and accepted silently in every layer, as `[workflow] engine`
//!   is ([`ENVIRONMENT_KEYS`]). A warning is for a key the lowering reads but
//!   cannot apply as Fabro does: an image on the `local` provider (which Fabro
//!   ignores on the host) and `resources` off Daytona. A key neither Petri nor
//!   Fabro's table knows warns as ignored. Both tables are read from every
//!   settings layer and merged key by key, the host's settings layer under
//!   `.fabro/project.toml` under `workflow.toml` ([`EnvironmentLayers`]), so a
//!   bundle can name an environment the host's catalog declares.
//! - `[run.prepare]`: setup steps that run as command nodes between `start` and
//!   its successors, in the selected environment, before any workflow node.
//!   Each step gets the section's `timeout` (default five minutes) and
//!   `on_failure="exit"`, so a failed step ends the run before the nodes.
//! - `[[run.hooks]]`: the local hook layer beside the workflow, read by
//!   `crate::hooks` together with `.fabro/project.toml` and the host's settings
//!   layer.

use std::collections::BTreeMap;

use frontend::{
    CompileInputs, Diagnostics, FileSource, LAUNCH_ENVIRONMENT_VAR, LAUNCH_GOAL_VAR, Span,
};
use frontend_attractor::model::parse_duration;
use frontend_attractor::template::Context;
use frontend_attractor::{
    DEFAULT_PRESERVE_TURNS, DEFAULT_THRESHOLD_PERCENT, EnvValue, Environment, PrepareStep,
    RunSettings,
};
use serde_json::{Value, json};

use crate::hooks::{PROJECT_FILE, SETTINGS_HOOKS_VAR};
use crate::model_layers::LaunchModel;
use crate::secrets::{InterpolationError, interpolate};
use crate::skills;

/// The settings schema version this build reads, Fabro's `_version`.
const WORKFLOW_TOML_VERSION: i64 = 1;

/// Fabro's default timeout for one `[run.prepare]` step.
const PREPARE_TIMEOUT_MS: u64 = 300_000;

/// The `Graph.params` key the launch settings persist under.
pub const LAUNCH_PARAM: &str = "fabro.launch";

/// The `Graph.params` key the resolved environment persists under.
pub const ENVIRONMENT_PARAM: &str = "fabro.environment";

/// Everything `workflow.toml` asks of a run: what the lowering applies
/// ([`RunSettings`]) and what the Fabro launch keeps for itself.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Settings {
    /// What the Attractor lowering applies.
    pub run:          RunSettings,
    /// The launch's model choice and the host's default, as given; already
    /// folded into `run.model_override` and `run.model`.
    pub launch:       LaunchModel,
    /// `[run.execution]`.
    pub dry_run:      bool,
    pub auto_approve: bool,
    /// The file's path and text, for the hook and MCP loaders'
    /// `workflow.toml` layer.
    pub hooks_text:   Option<(String, String)>,
}

impl Settings {
    /// The launch settings the persisted graph carries. `repository` is the
    /// absolute root the run checks out when `[run.clone]` is enabled, when
    /// the host bound one.
    pub fn launch_param(&self, repository: Option<&str>) -> Value {
        let environment = self.run.environment.as_ref();
        json!({
            "dry_run": self.dry_run,
            "auto_approve": self.auto_approve,
            "model": self.launch.model,
            "provider": self.launch.provider,
            "default_model": self.launch.default_model,
            "default_provider": self.launch.default_provider,
            "clone": {
                "enabled": self.run.clone.enabled,
                "depth": self.run.clone.depth,
                "repository": repository,
            },
            "sandbox_backend": environment.map(Environment::sandbox_backend),
            "cpu_cores": environment.and_then(|e| e.cpu_cores),
            "memory_mb": environment.and_then(|e| e.memory_mb),
            "disk_mb": environment.and_then(|e| e.disk_mb),
        })
    }

    /// The environment record the persisted graph carries, for inspection.
    pub fn environment_param(&self) -> Option<Value> {
        let environment = self.run.environment.as_ref()?;
        Some(json!({
            "id": environment.id,
            "provider": environment.provider,
            "image": environment.image,
            "env": environment.env.iter().map(|(k, v)| (k.clone(), v.to_json())).collect::<serde_json::Map<_, _>>(),
        }))
    }
}

/// Read `workflow.toml` beside `file`, and the environment tables of the
/// layers below it. Input defaults land in `template`; everything else
/// comes back as [`Settings`]. Problems are diagnosed.
pub fn read(
    file: &str,
    files: &dyn FileSource,
    inputs: &CompileInputs,
    template: &mut Context,
    diags: &mut Diagnostics,
) -> Settings {
    let dir = file.rfind('/').map_or("", |i| &file[..i]);
    let path = if dir.is_empty() {
        "workflow.toml".to_string()
    } else {
        format!("{dir}/workflow.toml")
    };
    let text = files.read(&path);
    let table: Option<toml::Table> = match text.as_deref().map(str::parse) {
        None => None,
        Some(Ok(table)) => Some(table),
        Some(Err(error)) => {
            // The hook loader still sees the text: a configured hook in an
            // unparseable file is an error there, never a silent skip.
            diags.warning(
                "fabro.workflow_toml",
                Span::file(&path),
                format!("`{path}` is not valid TOML and is ignored: {error}"),
            );
            None
        }
    };
    let mut reader = Reader {
        path: &path,
        dir,
        span: Span::file(&path),
        files,
        template,
        diags,
        settings: Settings {
            run: RunSettings {
                prepare_timeout_ms: PREPARE_TIMEOUT_MS,
                ..RunSettings::default()
            },
            ..Settings::default()
        },
    };
    if let Some(table) = &table {
        reader.top_level(table);
    }
    reader.launch_goal(inputs);
    // The environment resolves over every layer, so a bundle with no
    // `workflow.toml` still runs in the environment the host's layer names.
    let layers = EnvironmentLayers::read(files, inputs, table.as_ref().map(|t| (path.as_str(), t)));
    reader.environment_keys(&layers);
    reader.environment(&layers);
    let mut settings = reader.settings;
    settings.hooks_text = text.map(|text| (path, text));
    settings
}

/// `[environments.<id>]` and `[run.environment]` from every settings layer,
/// lowest first: the host's settings layer (`fabro.settings_toml`),
/// `.fabro/project.toml`, `workflow.toml`. The tables merge key by key,
/// tables recursively and everything else replaced, as Fabro's `combine`
/// merges them: a higher layer's `image.docker` wins over a lower one's
/// while the lower one's `resources` still apply, and `env` keys combine.
/// Above every layer sits the launch: `petri run --environment`, or the
/// environment a host's run selected, bound as `petri.launch_environment`,
/// selects the id as `fabro run --environment` does.
struct EnvironmentLayers {
    environments:    toml::Table,
    run_environment: Option<toml::Table>,
    /// The id the launch selected, over every layer's `[run.environment]`.
    launch:          Option<String>,
    /// The layer each merged key came from, by dotted path
    /// (`environments.review.image.docker`). A value copied whole records
    /// its own path, so a lookup takes the longest recorded prefix.
    sources:         BTreeMap<String, String>,
}

impl EnvironmentLayers {
    /// `workflow` is the bundle's own file, already parsed, when it exists.
    /// A lower layer that is not TOML is skipped here: the model layer's
    /// reader has warned that the file is ignored.
    fn read(
        files: &dyn FileSource,
        inputs: &CompileInputs,
        workflow: Option<(&str, &toml::Table)>,
    ) -> Self {
        let launch = inputs
            .vars
            .get(LAUNCH_ENVIRONMENT_VAR)
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .map(str::to_owned);
        let mut layers = Self {
            environments: toml::Table::new(),
            run_environment: None,
            launch,
            sources: BTreeMap::new(),
        };
        if let Some(Value::String(text)) = inputs.vars.get(SETTINGS_HOOKS_VAR)
            && let Ok(table) = text.parse::<toml::Table>()
        {
            layers.add("settings.toml", &table);
        }
        if let Some(text) = files.read(PROJECT_FILE)
            && let Ok(table) = text.parse::<toml::Table>()
        {
            layers.add(PROJECT_FILE, &table);
        }
        if let Some((path, table)) = workflow {
            layers.add(path, table);
        }
        layers
    }

    fn add(&mut self, source: &str, table: &toml::Table) {
        if let Some(environments) = table.get("environments").and_then(toml::Value::as_table) {
            merge_tables(
                &mut self.environments,
                environments,
                "environments",
                source,
                &mut self.sources,
            );
        }
        let run_environment = table
            .get("run")
            .and_then(toml::Value::as_table)
            .and_then(|run| run.get("environment"))
            .and_then(toml::Value::as_table);
        if let Some(run_environment) = run_environment {
            let merged = self.run_environment.get_or_insert_with(toml::Table::new);
            merge_tables(
                merged,
                run_environment,
                "run.environment",
                source,
                &mut self.sources,
            );
        }
    }

    /// The layer the value at `path` came from: the longest recorded prefix.
    fn source_of(&self, path: &str) -> Option<&str> {
        let mut candidate = path;
        loop {
            if let Some(source) = self.sources.get(candidate) {
                return Some(source);
            }
            candidate = &candidate[..candidate.rfind('.')?];
        }
    }
}

/// Merge `upper` into `target`: a table into a table recursively, any other
/// value replacing what is there. `sources` records the layer of every key
/// written, and forgets the sub-keys of a value replaced whole.
fn merge_tables(
    target: &mut toml::Table,
    upper: &toml::Table,
    prefix: &str,
    source: &str,
    sources: &mut BTreeMap<String, String>,
) {
    for (key, value) in upper {
        let path = format!("{prefix}.{key}");
        if let (Some(toml::Value::Table(lower)), toml::Value::Table(upper)) =
            (target.get_mut(key), value)
        {
            merge_tables(lower, upper, &path, source, sources);
            continue;
        }
        target.insert(key.clone(), value.clone());
        let replaced = format!("{path}.");
        sources.retain(|recorded, _| recorded != &path && !recorded.starts_with(&replaced));
        sources.insert(path, source.to_owned());
    }
}

struct Reader<'a> {
    path:     &'a str,
    dir:      &'a str,
    span:     Span,
    files:    &'a dyn FileSource,
    template: &'a mut Context,
    diags:    &'a mut Diagnostics,
    settings: Settings,
}

impl Reader<'_> {
    fn warn(&mut self, code: &str, message: String) {
        self.diags.warning(code, self.span.clone(), message);
    }

    fn unsupported(&mut self, feature: &str, message: String, hint: &str) {
        self.diags
            .unsupported(feature, self.span.clone(), message, hint);
    }

    fn ignored(&mut self, section: &str, why: &str) {
        let path = self.path;
        self.ignored_in(path, section, why);
    }

    /// [`Self::ignored`] for a section read from `source`, one of the
    /// settings layers.
    fn ignored_in(&mut self, source: &str, section: &str, why: &str) {
        self.diags.warning(
            &format!("ignored.workflow_toml.{section}"),
            Span::file(source),
            format!("`[{section}]` in `{source}` is ignored: {why}"),
        );
    }

    /// [`Self::unsupported`] for a setting read from `source`, one of the
    /// settings layers.
    fn unsupported_in(&mut self, source: &str, feature: &str, message: String, hint: &str) {
        self.diags
            .unsupported(feature, Span::file(source), message, hint);
    }

    fn top_level(&mut self, value: &toml::Table) {
        for key in value.keys() {
            if !WORKFLOW_TOML_TOP_LEVEL.contains(&key.as_str()) {
                let hint = rename_hint(key)
                    .unwrap_or("remove it; Fabro's settings schema has no such key");
                let path = self.path;
                self.unsupported(
                    "workflow_toml.key",
                    format!("`{key}` in `{path}` is not a key Fabro's `workflow.toml` accepts"),
                    hint,
                );
            }
        }
        if let Some(version) = value.get("_version").and_then(toml::Value::as_integer)
            && version != WORKFLOW_TOML_VERSION
        {
            let path = self.path;
            self.unsupported(
                "workflow_toml.version",
                format!(
                    "`_version = {version}` in `{path}` is not the settings schema version this \
                     build reads ({WORKFLOW_TOML_VERSION})"
                ),
                "set `_version = 1`",
            );
        }
        for (section, why) in WORKFLOW_TOML_INERT {
            if value.contains_key(*section) {
                self.ignored(section, why);
            }
        }
        if let Some(llm) = value.get("llm").and_then(toml::Value::as_table) {
            for key in WORKFLOW_TOML_LEGACY_LLM_KEYS {
                if llm.contains_key(*key) {
                    let path = self.path;
                    self.unsupported(
                        "workflow_toml.key",
                        format!("`llm.{key}` in `{path}` is a legacy key Fabro refuses"),
                        "rename to `[run.model]`",
                    );
                }
            }
        }
        if let Some(workflow) = value.get("workflow").and_then(toml::Value::as_table) {
            self.workflow_table(workflow);
        }
        if let Some(run) = value.get("run").and_then(toml::Value::as_table) {
            self.run_table(run);
        }
    }

    /// `[workflow]`: the keys Fabro's parser accepts are known and read by
    /// nothing here (`engine` picks the engine Fabro runs the workflow on;
    /// Petri is that engine, so the value is not inspected). A key outside
    /// the set is refused as Fabro refuses it.
    fn workflow_table(&mut self, workflow: &toml::Table) {
        for key in workflow.keys() {
            if !WORKFLOW_SECTION_KEYS.contains(&key.as_str()) {
                let path = self.path;
                self.unsupported(
                    "workflow_toml.key",
                    format!(
                        "`workflow.{key}` in `{path}` is not a key Fabro's `[workflow]` table \
                         accepts"
                    ),
                    "remove it; Fabro's settings schema has no such key",
                );
            }
        }
    }

    fn run_table(&mut self, run: &toml::Table) {
        // Input defaults first: every other section may render `{{ inputs.* }}`.
        if let Some(inputs) = run.get("inputs").and_then(toml::Value::as_table) {
            for (name, value) in inputs {
                let json = serde_json::to_value(value).unwrap_or(Value::Null);
                self.template.default_input(name, json);
            }
        }
        for (key, item) in run {
            match key.as_str() {
                // Inputs were read above. `[[run.hooks]]` is read by
                // `lower::hooks`, with the project and settings layers, from
                // the text kept on the settings. `[run.environment]` is read
                // with the other layers' by `Reader::environment`.
                "inputs" | "hooks" | "environment" => {}
                "goal" => self.goal(item),
                "model" => self.model(item),
                "execution" => self.execution(item),
                "prepare" => self.prepare(item),
                "agent" => self.agent(item),
                "clone" => self.clone_section(item),
                other => self.other_run_key(other),
            }
        }
    }

    /// A `[run]` key with no reader of its own: a platform-only section that
    /// warns, or a key Fabro's parser refuses.
    fn other_run_key(&mut self, key: &str) {
        let path = self.path;
        if let Some((section, why)) = RUN_SECTIONS_IGNORED
            .iter()
            .find(|(section, _)| *section == key)
        {
            self.warn(
                &format!("ignored.workflow_toml.run.{section}"),
                format!("`[run.{section}]` in `{path}` is ignored: {why}"),
            );
        } else {
            self.unsupported(
                "workflow_toml.key",
                format!("`run.{key}` in `{path}` is not a key Fabro's `[run]` table accepts"),
                "remove it; Fabro's settings schema has no such key",
            );
        }
    }

    /// `goal = "text"` or `[run.goal] file = "path"`.
    fn goal(&mut self, item: &toml::Value) {
        let text = match item {
            toml::Value::String(text) => text.clone(),
            toml::Value::Table(table) => {
                let Some(file) = table.get("file").and_then(toml::Value::as_str) else {
                    self.unsupported(
                        "workflow_toml.key",
                        format!(
                            "`[run.goal]` in `{}` must be a string or `{{ file = ... }}`",
                            self.path
                        ),
                        "write `goal = \"...\"` or `goal = { file = \"goal.md\" }`",
                    );
                    return;
                };
                let path = if self.dir.is_empty() {
                    file.to_string()
                } else {
                    format!("{}/{file}", self.dir)
                };
                let Some(text) = self.files.read(&path) else {
                    self.diags.error(
                        "fabro.file_not_found",
                        self.span.clone(),
                        format!(
                            "`[run.goal] file = \"{file}\"` names `{path}`, which cannot be read"
                        ),
                    );
                    return;
                };
                text
            }
            _ => {
                self.unsupported(
                    "workflow_toml.key",
                    format!(
                        "`run.goal` in `{}` must be a string or `{{ file = ... }}`",
                        self.path
                    ),
                    "write `goal = \"...\"`",
                );
                return;
            }
        };
        if let Some(rendered) = self.render(&text, "`[run] goal`") {
            self.settings.run.goal = Some(rendered);
        }
    }

    /// The goal the launch stated (`petri run --goal`, or a host run's goal
    /// override, bound as `petri.launch_goal`), over `[run] goal`. Rendered
    /// as `[run] goal` is, so the two forms read the same language.
    fn launch_goal(&mut self, inputs: &CompileInputs) {
        let Some(goal) = inputs
            .vars
            .get(LAUNCH_GOAL_VAR)
            .and_then(Value::as_str)
            .filter(|goal| !goal.trim().is_empty())
        else {
            return;
        };
        if let Some(rendered) = self.render(goal, "the launch `--goal`") {
            self.settings.run.goal = Some(rendered);
        }
    }

    /// Render `{{ inputs.* }}`, `{{ vars.* }}` and `{{ goal }}` in a settings
    /// string. `{{ secrets.* }}` is refused here: only an environment value
    /// may carry one.
    fn render(&mut self, text: &str, what: &str) -> Option<String> {
        match interpolate(text, self.template, false) {
            Ok(rendered) => Some(rendered.text),
            Err(InterpolationError::SecretNotAllowed { name }) => {
                self.unsupported(
                    "workflow_toml.secret_position",
                    format!(
                        "{what} in `{}` reads `{{{{ secrets.{name} }}}}`; a secret may appear only \
                         as an environment value, never in text that reaches a log",
                        self.path
                    ),
                    "move the secret to `env` and read the variable in the script",
                );
                None
            }
            Err(InterpolationError::Unbound { name }) => {
                self.unsupported(
                    "template.unbound_input",
                    format!(
                        "{what} in `{}` reads `{{{{ {name} }}}}`, which no input binds",
                        self.path
                    ),
                    &format!(
                        "pass `--input {}=VALUE`, or add a default under `[run.inputs]`",
                        name.strip_prefix("inputs.").unwrap_or(&name)
                    ),
                );
                None
            }
            Err(InterpolationError::Env { name }) => {
                self.unsupported(
                    "workflow_toml.env_token",
                    format!(
                        "{what} in `{}` reads `{{{{ env.{name} }}}}`, which Fabro parses but never \
                         resolves",
                        self.path
                    ),
                    "use `{{ inputs.NAME }}` or `{{ secrets.NAME }}`",
                );
                None
            }
        }
    }

    /// An environment value: literal text, or exactly `{{ secrets.NAME }}`.
    /// `source` is the file `section` was read from.
    fn env_value(
        &mut self,
        source: &str,
        section: &str,
        key: &str,
        value: &toml::Value,
    ) -> Option<EnvValue> {
        let Some(text) = value.as_str() else {
            self.unsupported_in(
                source,
                "workflow_toml.key",
                format!("`{section}.env.{key}` in `{source}` must be a string"),
                "write the value as a string",
            );
            return None;
        };
        match interpolate(text, self.template, true) {
            Ok(rendered) => match rendered.secret {
                Some(name) => Some(EnvValue::Secret(name)),
                None => Some(EnvValue::Literal(rendered.text)),
            },
            Err(InterpolationError::SecretNotAllowed { name }) => {
                self.unsupported_in(
                    source,
                    "workflow_toml.secret_position",
                    format!(
                        "`{section}.env.{key}` in `{source}` mixes `{{{{ secrets.{name} }}}}` with \
                         other text; a secret must be the whole value"
                    ),
                    "write `KEY = \"{{ secrets.NAME }}\"` on its own",
                );
                None
            }
            Err(InterpolationError::Unbound { name }) => {
                self.unsupported_in(
                    source,
                    "template.unbound_input",
                    format!(
                        "`{section}.env.{key}` in `{source}` reads `{{{{ {name} }}}}`, which no \
                         input binds"
                    ),
                    "pass the input, or add a default under `[run.inputs]`",
                );
                None
            }
            Err(InterpolationError::Env { name }) => {
                self.unsupported_in(
                    source,
                    "workflow_toml.env_token",
                    format!(
                        "`{section}.env.{key}` in `{source}` reads `{{{{ env.{name} }}}}`, which \
                         Fabro parses but never resolves"
                    ),
                    "use `{{ inputs.NAME }}` or `{{ secrets.NAME }}`",
                );
                None
            }
        }
    }

    fn model(&mut self, item: &toml::Value) {
        let Some(model) = item.as_table() else {
            return;
        };
        for (key, value) in model {
            match key.as_str() {
                "fallbacks" => {
                    self.settings.run.model.fallbacks =
                        super::fallbacks::read(self.path, self.diags, value);
                }
                "provider" => self.settings.run.model.provider = value.as_str().map(str::to_owned),
                "name" => self.settings.run.model.name = value.as_str().map(str::to_owned),
                "controls" => {
                    let Some(controls) = value.as_table() else {
                        continue;
                    };
                    if let Some(effort) = controls
                        .get("reasoning_effort")
                        .and_then(toml::Value::as_str)
                    {
                        self.settings.run.model.reasoning_effort = Some(effort.to_owned());
                    }
                    if let Some(speed) = controls.get("speed").and_then(toml::Value::as_str) {
                        self.settings.run.model.speed = Some(speed.to_owned());
                    }
                }
                other => {
                    let path = self.path;
                    self.unsupported(
                        "workflow_toml.key",
                        format!("`run.model.{other}` in `{path}` is not a key Fabro accepts"),
                        "use `provider`, `name`, `controls` or `fallbacks`",
                    );
                }
            }
        }
    }

    /// `[run.clone]`: `enabled` and `depth`, as Fabro's `RunCloneLayer`
    /// reads them. A negative depth is Fabro's full history (0).
    fn clone_section(&mut self, item: &toml::Value) {
        let Some(clone) = item.as_table() else {
            return;
        };
        for (key, value) in clone {
            match (key.as_str(), value) {
                ("enabled", toml::Value::Boolean(enabled)) => {
                    self.settings.run.clone.enabled = *enabled;
                }
                ("depth", toml::Value::Integer(depth)) => {
                    self.settings.run.clone.depth = (*depth).max(0);
                }
                ("enabled" | "depth", _) => {
                    let path = self.path;
                    self.unsupported(
                        "workflow_toml.key",
                        format!("`run.clone.{key}` in `{path}` has the wrong type"),
                        "`enabled` is a boolean, `depth` an integer",
                    );
                }
                (other, _) => {
                    let path = self.path;
                    self.unsupported(
                        "workflow_toml.key",
                        format!("`run.clone.{other}` in `{path}` is not a key Fabro accepts"),
                        "use `enabled` or `depth`",
                    );
                }
            }
        }
    }

    fn execution(&mut self, item: &toml::Value) {
        let Some(execution) = item.as_table() else {
            return;
        };
        for (key, value) in execution {
            match (key.as_str(), value.as_str()) {
                ("mode", Some("normal")) | ("approval", Some("prompt")) => {}
                ("mode", Some("dry_run")) => self.settings.dry_run = true,
                ("approval", Some("auto")) => self.settings.auto_approve = true,
                ("mode", _) => self.unsupported(
                    "workflow_toml.key",
                    format!(
                        "`run.execution.mode` in `{}` must be `normal` or `dry_run`",
                        self.path
                    ),
                    "set `mode = \"normal\"` or `mode = \"dry_run\"`",
                ),
                ("approval", _) => self.unsupported(
                    "workflow_toml.key",
                    format!(
                        "`run.execution.approval` in `{}` must be `prompt` or `auto`",
                        self.path
                    ),
                    "set `approval = \"prompt\"` or `approval = \"auto\"`",
                ),
                (other, _) => self.unsupported(
                    "workflow_toml.key",
                    format!(
                        "`run.execution.{other}` in `{}` is not a key Fabro accepts",
                        self.path
                    ),
                    "use `mode` or `approval`",
                ),
            }
        }
    }

    fn agent(&mut self, item: &toml::Value) {
        let Some(agent) = item.as_table() else {
            return;
        };
        // Fabro's `[run.agent]` accepts `fabro_tools` and `mcps` only. Neither
        // sub-agents (readiness item 9d) nor compaction (item 9e) has a
        // `workflow.toml` surface at the pinned revision: every native agent
        // gets the sub-agent tools (`super::subagents`), and compaction is
        // always on with Fabro's hardcoded values (`lower::compaction`), so a
        // key asking for either is refused as Fabro refuses it, never passed
        // silently. `skills` is the standalone
        // runner's own extension (`skills::read`).
        for key in agent.keys() {
            if !matches!(key.as_str(), "fabro_tools" | "mcps" | "skills") {
                let path = self.path;
                let (message, hint) = if key == "compaction" {
                    (
                        format!(
                            "`run.agent.compaction` in `{path}` is not a key Fabro's `[run.agent]` \
                             table accepts (it takes `fabro_tools` and `mcps`); compaction is \
                             always on with Fabro's values: a trigger at \
                             {DEFAULT_THRESHOLD_PERCENT} percent of the context window and \
                             {DEFAULT_PRESERVE_TURNS} preserved turns"
                        ),
                        "remove it; Fabro's settings schema has no such key",
                    )
                } else if key == "subagents" {
                    (
                        format!(
                            "`run.agent.subagents` in `{path}` is not a key Fabro's `[run.agent]` \
                             table accepts (it takes `fabro_tools` and `mcps`); sub-agents are \
                             always available to native agents"
                        ),
                        "remove it; native agents always have the sub-agent tools, as in Fabro, \
                         and Fabro's settings schema has no such key",
                    )
                } else {
                    (
                        format!(
                            "`run.agent.{key}` in `{path}` is not a key Fabro's `[run.agent]` \
                             table accepts (it takes `fabro_tools` and `mcps`)"
                        ),
                        "remove it; Fabro's settings schema has no such key",
                    )
                };
                self.unsupported("workflow_toml.key", message, hint);
            }
        }
        if let Some(value) = agent.get("skills") {
            self.settings.run.skills = skills::read(value, self.path, &self.span, self.diags);
        }
        if agent.get("fabro_tools").and_then(toml::Value::as_bool) == Some(true) {
            let path = self.path;
            self.warn(
                "ignored.workflow_toml.run.agent.fabro_tools",
                format!(
                    "`fabro_tools = true` in `{path}` is ignored: run-management tools are a \
                     Fabro platform facility; agents get the sandbox tools only"
                ),
            );
        }
        // `[run.agent.mcps]` is read by `lower::mcps` from this file's text,
        // together with the other settings layers.
    }

    /// Every `[environments.<id>]` and `[run.environment]` key of every
    /// layer is one Fabro's table accepts, whether this lowering reads it or
    /// the platform does; a key outside the set is ignored with a warning
    /// that names the layer. A warning, not Fabro's refusal: the host's
    /// settings layer is Fabro's own catalog, which its parser accepted, so
    /// a key this build has not learned must not refuse the run.
    fn environment_keys(&mut self, layers: &EnvironmentLayers) {
        let default_source = self.path;
        let at = |path: &str| layers.source_of(path).unwrap_or(default_source).to_owned();
        for (id, table) in &layers.environments {
            let Some(table) = table.as_table() else {
                continue;
            };
            for key in table.keys() {
                if !ENVIRONMENT_KEYS.contains(&key.as_str()) {
                    self.ignored_in(
                        &at(&format!("environments.{id}.{key}")),
                        &format!("environments.{id}.{key}"),
                        "not a key Fabro's `[environments.<id>]` table accepts",
                    );
                }
            }
        }
        if let Some(run_env) = &layers.run_environment {
            for key in run_env.keys() {
                if !RUN_ENVIRONMENT_KEYS.contains(&key.as_str()) {
                    self.ignored_in(
                        &at(&format!("run.environment.{key}")),
                        &format!("run.environment.{key}"),
                        "not a key Fabro's `[run.environment]` table accepts",
                    );
                }
            }
        }
    }

    /// `[run.environment]` over `[environments.<id>]`, both merged across
    /// the layers, Fabro's `combine`: the run's fields win, the named
    /// environment fills the rest; the launch's selection wins over every
    /// layer's `id`. Every diagnostic names the layer the setting came from.
    /// The platform's keys ([`ENVIRONMENT_KEYS`]) are not inspected.
    fn environment(&mut self, layers: &EnvironmentLayers) {
        let empty = toml::Table::new();
        let run_env = match (&layers.run_environment, &layers.launch) {
            (Some(run_env), _) => run_env,
            (None, Some(_)) => &empty,
            (None, None) => return,
        };
        let default_source = self.path;
        let at = |path: &str| layers.source_of(path).unwrap_or(default_source).to_owned();
        let id = layers
            .launch
            .as_deref()
            .or_else(|| run_env.get("id").and_then(toml::Value::as_str))
            .unwrap_or("default")
            .to_string();
        let base_table = format!("environments.{id}");
        let Some(base) = layers.environments.get(&id).and_then(toml::Value::as_table) else {
            let hint = format!(
                "add `[environments.{id}]` with a `provider` to `workflow.toml`, \
                 `.fabro/project.toml` or the settings layer"
            );
            if layers.launch.is_some() {
                self.unsupported(
                    "workflow_toml.run.environment",
                    format!(
                        "the launch selects the environment `{id}` (`--environment {id}`), which \
                         no `[environments.{id}]` table declares in any settings layer"
                    ),
                    &hint,
                );
            } else if run_env.keys().any(|k| k != "id") || id != "default" {
                let source = at("run.environment.id");
                self.unsupported_in(
                    &source,
                    "workflow_toml.run.environment",
                    format!(
                        "`[run.environment] id = \"{id}\"` in `{source}` names no \
                         `[environments.{id}]` table in any settings layer"
                    ),
                    &hint,
                );
            }
            return;
        };
        let provider = base
            .get("provider")
            .and_then(toml::Value::as_str)
            .unwrap_or("")
            .to_string();
        // `local`, `docker` and `daytona` are the providers Petri places
        // itself; any other valid kind names a provider the host registers,
        // and is carried through as written. `simulated` is a dry run's.
        if provider == "simulated"
            || sandbox_driver::ProviderKind::try_new(provider.as_str()).is_err()
        {
            let source = at(&format!("{base_table}.provider"));
            self.unsupported_in(
                &source,
                "workflow_toml.environments.provider",
                format!(
                    "`[environments.{id}] provider = \"{provider}\"` in `{source}` is not \
                     `local`, `docker`, `daytona`, or a provider kind (lowercase letters, \
                     digits, and interior hyphens)"
                ),
                "set the provider to `local`, `docker`, `daytona`, or a registered provider kind",
            );
            return;
        }
        let built_in = matches!(provider.as_str(), "local" | "docker" | "daytona");
        // A field, with the table it was read from: the run's own table
        // over the named environment's.
        let field = |key: &str| {
            run_env
                .get(key)
                .map(|value| (value, "run.environment"))
                .or_else(|| base.get(key).map(|value| (value, base_table.as_str())))
        };
        // A registered provider is provisioned from its registration's
        // template, so an image would be ignored and is refused instead.
        // `resources` size the run's sandbox over the template's and are
        // lowered below, as Daytona's are.
        if !built_in {
            for key in ["image"] {
                if let Some((_, table)) = field(key) {
                    self.unsupported_in(
                        &at(&format!("{table}.{key}")),
                        "workflow_toml.environments.custom",
                        format!(
                            "`[environments.{id}] provider = \"{provider}\"` is a registered \
                             provider; `{key}` applies to `docker` and `daytona` only"
                        ),
                        "configure the provider's sandbox in its registration instead",
                    );
                    return;
                }
            }
        }
        let mut environment = Environment {
            id:        id.clone(),
            provider:  provider.clone(),
            image:     None,
            env:       BTreeMap::new(),
            cpu_cores: None,
            memory_mb: None,
            disk_mb:   None,
        };
        // `image.docker` only: `image.dockerfile` is the platform's, which
        // builds the image there; the scope here runs on `image.docker` or
        // the backend's default runner image.
        if let Some((image, table)) = field("image")
            && let Some(image) = image.as_table()
            && let Some(docker) = image.get("docker").and_then(toml::Value::as_str)
        {
            if provider == "local" {
                // Fabro ignores the image on the host, and a server's
                // catalog may carry one on a `local` environment (its
                // seeded default keeps the image whatever the provider).
                self.ignored_in(
                    &at(&format!("{table}.image.docker")),
                    &format!("environments.{id}.image"),
                    "the `local` provider runs on the host; an image applies to `docker` and \
                     `daytona`",
                );
            } else {
                environment.image = Some(docker.to_string());
            }
        }
        if let Some((resources, table)) = field("resources")
            && let Some(resources) = resources.as_table()
        {
            if provider == "daytona" || !built_in {
                environment.cpu_cores = resources
                    .get("cpu")
                    .and_then(toml::Value::as_integer)
                    .and_then(|n| u32::try_from(n).ok());
                environment.memory_mb = resources.get("memory").and_then(size_mb);
                environment.disk_mb = resources.get("disk").and_then(size_mb);
            } else {
                self.ignored_in(
                    &at(&format!("{table}.resources")),
                    &format!("environments.{id}.resources"),
                    "resource limits apply to a Daytona runner; the host and Docker providers \
                     run unconstrained",
                );
            }
        }
        // `env`: the named environment's values under the run's, Fabro's
        // sticky map order.
        let mut merged: Vec<(String, toml::Value, &str)> = Vec::new();
        if let Some(env) = base.get("env").and_then(toml::Value::as_table) {
            for (key, value) in env {
                merged.push((key.clone(), value.clone(), base_table.as_str()));
            }
        }
        if let Some(env) = run_env.get("env").and_then(toml::Value::as_table) {
            for (key, value) in env {
                merged.retain(|(k, _, _)| k != key);
                merged.push((key.clone(), value.clone(), "run.environment"));
            }
        }
        for (key, value, table) in merged {
            let source = at(&format!("{table}.env.{key}"));
            if let Some(value) = self.env_value(&source, table, &key, &value) {
                environment.env.insert(key, value);
            }
        }
        self.settings.run.environment = Some(environment);
    }

    fn prepare(&mut self, item: &toml::Value) {
        let Some(prepare) = item.as_table() else {
            return;
        };
        if let Some(timeout) = prepare.get("timeout") {
            match timeout.as_str().and_then(parse_duration) {
                Some(duration) => {
                    self.settings.run.prepare_timeout_ms =
                        u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
                }
                None => self.unsupported(
                    "workflow_toml.key",
                    format!(
                        "`run.prepare.timeout` in `{}` must be a duration such as `5m`",
                        self.path
                    ),
                    "write the timeout with a unit",
                ),
            }
        }
        for key in prepare.keys() {
            if !matches!(key.as_str(), "steps" | "timeout") {
                let path = self.path;
                self.unsupported(
                    "workflow_toml.key",
                    format!("`run.prepare.{key}` in `{path}` is not a key Fabro accepts"),
                    "use `steps` and `timeout`",
                );
            }
        }
        let Some(steps) = prepare.get("steps").and_then(toml::Value::as_array) else {
            return;
        };
        for (index, step) in steps.iter().enumerate() {
            let Some(step) = step.as_table() else {
                self.unsupported(
                    "workflow_toml.run.prepare",
                    format!(
                        "`run.prepare.steps[{index}]` in `{}` must be a table",
                        self.path
                    ),
                    "write `[[run.prepare.steps]]` with `script` or `command`",
                );
                continue;
            };
            let script = step.get("script").and_then(toml::Value::as_str);
            let command = step.get("command").and_then(toml::Value::as_array);
            let text = match (script, command) {
                (Some(script), None) => {
                    self.render(script, &format!("`run.prepare.steps[{index}].script`"))
                }
                (None, Some(argv)) => {
                    let mut words = Vec::with_capacity(argv.len());
                    let mut ok = true;
                    for (position, word) in argv.iter().enumerate() {
                        let Some(word) = word.as_str() else {
                            ok = false;
                            self.unsupported(
                                "workflow_toml.run.prepare",
                                format!(
                                    "`run.prepare.steps[{index}].command[{position}]` in `{}` must \
                                     be a string",
                                    self.path
                                ),
                                "write every argument as a string",
                            );
                            continue;
                        };
                        match self.render(word, &format!("`run.prepare.steps[{index}].command`")) {
                            Some(rendered) => words.push(rendered),
                            None => ok = false,
                        }
                    }
                    ok.then(|| shlex::try_join(words.iter().map(String::as_str)).ok())
                        .flatten()
                }
                _ => {
                    self.unsupported(
                        "workflow_toml.run.prepare",
                        format!(
                            "`run.prepare.steps[{index}]` in `{}`: exactly one of script or command \
                             must be set",
                            self.path
                        ),
                        "give the step a `script` or a `command`, not both",
                    );
                    None
                }
            };
            let Some(text) = text else {
                continue;
            };
            let mut env = BTreeMap::new();
            if let Some(table) = step.get("env").and_then(toml::Value::as_table) {
                let path = self.path;
                for (key, value) in table {
                    if let Some(value) =
                        self.env_value(path, &format!("run.prepare.steps[{index}]"), key, value)
                    {
                        env.insert(key.clone(), value);
                    }
                }
            }
            for key in step.keys() {
                if !matches!(key.as_str(), "script" | "command" | "env") {
                    let path = self.path;
                    self.unsupported(
                        "workflow_toml.key",
                        format!("`run.prepare.steps[{index}].{key}` in `{path}` is not a key Fabro accepts"),
                        "use `script` or `command`, and `env`",
                    );
                }
            }
            self.settings
                .run
                .prepare
                .push(PrepareStep { script: text, env });
        }
    }
}

/// Fabro's size grammar: an integer with `B`, `KB`, `MB`, `GB`, `TB` (or
/// the `iB` forms), a bare integer meaning gigabytes; in MiB.
fn size_mb(value: &toml::Value) -> Option<u64> {
    let bytes = match value {
        toml::Value::Integer(n) => u64::try_from(*n).ok()?.checked_mul(1024 * 1024 * 1024)?,
        toml::Value::String(text) => {
            let trimmed = text.trim();
            let split = trimmed
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(trimmed.len());
            let number: u64 = trimmed[..split].parse().ok()?;
            let multiplier: u64 = match trimmed[split..].trim().to_ascii_uppercase().as_str() {
                "" | "GB" | "GIB" => 1024 * 1024 * 1024,
                "B" => 1,
                "KB" | "KIB" => 1024,
                "MB" | "MIB" => 1024 * 1024,
                "TB" | "TIB" => 1024_u64.pow(4),
                _ => return None,
            };
            number.checked_mul(multiplier)?
        }
        _ => return None,
    };
    Some(bytes / (1024 * 1024))
}

/// The top-level keys Fabro's settings parser accepts; anything else is a
/// hard error there and here.
const WORKFLOW_TOML_TOP_LEVEL: &[&str] = &[
    "_version",
    "project",
    "workflow",
    "environments",
    "run",
    "cli",
    "server",
    "llm",
];

/// The `[workflow]` keys Fabro's settings parser accepts: the bundle's
/// name, description, graph path and metadata, and `engine`
/// (`"petri"` or `"legacy"`), which Fabro reads to choose the engine.
const WORKFLOW_SECTION_KEYS: &[&str] = &["name", "description", "graph", "metadata", "engine"];

/// The `[environments.<id>]` keys Fabro's settings parser accepts
/// (`EnvironmentLayer` in `fabro-config`). The lowering reads `provider`,
/// `image.docker`, `resources` and `env`; `cwd`, `network`, `lifecycle`,
/// `labels` and `image.dockerfile` are the Fabro platform's, which acts on
/// them around the engine (the working directory, network policy, sandbox
/// lifecycle and labels, the image build), so they are known here and read
/// by nothing, as `[workflow] engine` is.
const ENVIRONMENT_KEYS: &[&str] = &[
    "provider",
    "cwd",
    "image",
    "resources",
    "network",
    "lifecycle",
    "labels",
    "env",
];

/// The `[run.environment]` keys Fabro's settings parser accepts
/// (`RunEnvironmentLayer`): the `id` and the named environment's own
/// overrides, less `provider` and `cwd`.
const RUN_ENVIRONMENT_KEYS: &[&str] = &[
    "id",
    "image",
    "resources",
    "network",
    "lifecycle",
    "labels",
    "env",
];

/// Legacy `[llm]` keys Fabro refuses with a rename hint.
const WORKFLOW_TOML_LEGACY_LLM_KEYS: &[&str] = &[
    "provider",
    "model",
    "temperature",
    "max_tokens",
    "fallbacks",
    "fallback",
];

/// Top-level sections that are accepted in a workflow file but carry nothing
/// the standalone runner acts on.
const WORKFLOW_TOML_INERT: &[(&str, &str)] = &[
    (
        "project",
        "project settings belong to `.fabro/project.toml`; the standalone runner reads none",
    ),
    (
        "cli",
        "Fabro CLI settings do not apply to the standalone runner",
    ),
    (
        "server",
        "Fabro server settings do not apply to the standalone runner",
    ),
    (
        "llm",
        "the provider catalog comes from the distribution and `PETRI_LLM_CATALOG`, not from \
         the workflow file",
    ),
];

/// `[run.*]` sections the standalone runner reads but does not act on, each
/// with why.
const RUN_SECTIONS_IGNORED: &[(&str, &str)] = &[
    (
        "working_dir",
        "the working directory is the sandbox workspace the run was given",
    ),
    ("metadata", "run metadata is a Fabro platform record"),
    (
        "run_branch",
        "the standalone runner performs no Git operations of its own",
    ),
    (
        "meta_branch",
        "the standalone runner performs no Git operations of its own",
    ),
    (
        "pull_request",
        "the standalone runner performs no Git operations of its own",
    ),
    (
        "git",
        "the standalone runner performs no Git operations of its own",
    ),
    (
        "integrations",
        "platform integrations are supplied by an embedding host, not the standalone runner; \
         the run inherits the ambient `GITHUB_TOKEN` or none",
    ),
    (
        "checkpoint",
        "the standalone runner does not checkpoint the workspace; it retains it instead",
    ),
    (
        "artifacts",
        "artifact selection is not implemented yet; the whole retained workspace is the result",
    ),
    (
        "notifications",
        "notification routes are a Fabro platform facility",
    ),
    (
        "interviews",
        "interview routing is a Fabro platform facility; the host's interviewer answers",
    ),
    ("scm", "SCM metadata is a Fabro platform record"),
];

/// Fabro's rename hint for a top-level key its parser refuses.
fn rename_hint(key: &str) -> Option<&'static str> {
    Some(match key {
        "version" => "rename to `_version`",
        "goal" | "goal_file" | "work_dir" | "directory" => "move to `[run]`",
        "graph" => "move to `[workflow]`",
        "labels" => "move to `[run.metadata]`",
        "vars" => "rename to `[run.inputs]`",
        "setup" => "rename to `[run.prepare]`",
        "sandbox" => "rename to `[run.environment]` and `[environments.<slug>]`",
        "checkpoint" => "move under `[run.checkpoint]`",
        "pull_request" => "move under `[run.pull_request]`",
        "artifacts" => "move under `[run.artifacts]`",
        "hooks" => "move under `[[run.hooks]]`",
        "mcp_servers" => "move under `[run.agent.mcps.<name>]`",
        "exec" => "rename to `[cli.exec]`",
        "api" => "rename to `[server.api]`",
        "web" => "rename to `[server.web]`",
        "artifact_storage" => "rename to `[server.artifacts]`",
        "storage_dir" | "data_dir" => "rename to `[server.storage] root`",
        "max_concurrent_runs" => "rename to `[server.scheduler]`",
        "fabro" => "rename to `[project]`",
        "git" => "split into `[run.git]` and `[server.integrations.github]`",
        "github" => {
            "split into `[server.integrations.github]` and `[run.integrations.github.permissions]`"
        }
        "slack" => "move under `[server.integrations.slack]`",
        "log" => "rename to `[server.logging]` or `[cli.logging]`",
        "prevent_idle_sleep" => "rename to `[cli.exec] prevent_idle_sleep`",
        "verbose" => "rename to `[cli.output] verbosity`",
        "upgrade_check" => "rename to `[cli.updates] check`",
        "dry_run" => "rename to `[run.execution] mode = \"dry_run\"`",
        "auto_approve" => "rename to `[run.execution] approval = \"auto\"`",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_follow_fabro() {
        assert_eq!(
            size_mb(&toml::Value::String("16GB".into())),
            Some(16 * 1024)
        );
        assert_eq!(size_mb(&toml::Value::String("512MiB".into())), Some(512));
        assert_eq!(size_mb(&toml::Value::Integer(2)), Some(2048));
        assert_eq!(size_mb(&toml::Value::String("1.5GB".into())), None);
    }
}
