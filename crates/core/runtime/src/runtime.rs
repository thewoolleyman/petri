//! The standard configuration, as a builder.
//!
//! Every place that runs a graph used to wire the same pieces by hand: a
//! registry with `noop` and `process`, an executor, a secret provider, a
//! `RunConfig`, a driver, and the replay canary. [`Runtime`] is that wiring,
//! written once. The CLI, the acceptance harness, and an external repository
//! all configure the same builder; extension is registration, not new plumbing.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::{env, fs, io, mem, process};

use driver::{
    DecisionRolls, Driver, EventObserver, ExecutionHooks, ExecutionReport, HookContext,
    RecordingClock, ResumeError, ResumeInfo, RunConfig, RunGuard, SandboxAssignment,
    SeededDecisionResolver, StepLogStore,
};
use engine::{EngineStart, EventLog, ReplayMismatch};
use executor::{
    DEFAULT_GRACE, Executor, MapSecrets, Masker, ProgressSink, Retention, SecretProvider,
};
use executor_sandbox::{
    InProcessProviders, LeaseLedger, ProviderRegistrations, RoutingExecutor, SandboxOptions,
};
use frontend::{CompileInputs, DirFiles, FileSource, Frontend, Lowered, REPOSITORY_VAR, Span};
use ir::{ExecutionId, Graph, InvocationId};
use serde_json::Value;
use smol_str::SmolStr;
use store::{Access, OwnerId, RunDirStore, RunKey, RunLogs, RunStore, StoreError};
use tracing::field::Empty;

/// The knobs a run gets, with the defaults the driver documents.
#[derive(Clone, Debug)]
pub struct RunOptions {
    /// Where workspaces and logs live.
    pub run_dir:             PathBuf,
    /// Between `SIGTERM` and `SIGKILL`, per scope.
    pub grace:               Duration,
    /// How much longer than `grace` a step gets before the driver stops
    /// waiting.
    pub hard_deadline_slack: Duration,
    /// Between the first root cancel and the `KillRequested` that ends whatever
    /// cleanup is still running.
    pub cleanup_grace:       Duration,
    pub retention:           Retention,
    /// Echo step output to this process's stderr, prefixed by node and
    /// firing.
    pub echo:                bool,
    /// Replay the log after the run and fail on any divergence. The determinism
    /// canary; on by default.
    pub verify_replay:       bool,
    pub sandbox:             SandboxOptions,
    /// The run's identity in its store and on its sandbox providers. A host
    /// with a run id of its own passes it; a resume of a stored run finds
    /// the stored one; otherwise Petri mints one when the run is prepared.
    pub run_key:             Option<RunKey>,
}

impl RunOptions {
    pub fn new(run_dir: impl Into<PathBuf>) -> Self {
        Self {
            run_dir:             run_dir.into(),
            grace:               DEFAULT_GRACE,
            hard_deadline_slack: Duration::from_secs(5),
            cleanup_grace:       driver::DEFAULT_CLEANUP_GRACE,
            retention:           Retention::default(),
            echo:                false,
            verify_replay:       true,
            sandbox:             SandboxOptions::default(),
            run_key:             None,
        }
    }
}

/// Why a workflow file could not be loaded at all: an IO-or-usage problem, as
/// opposed to a rejected workflow, which comes back as diagnostics.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// `--format` (or its API equivalent) named a format no registered
    /// frontend answers to.
    #[error("unknown format `{name}`; known formats: {}", known.join(", "))]
    UnknownFormat { name: String, known: Vec<String> },
    /// No `--format` was given and no registered frontend claims the path.
    #[error("no frontend claims `{}`", path.display())]
    NoFrontend { path: PathBuf },
    #[error("could not read {}", path.display())]
    Read {
        path:   PathBuf,
        #[source]
        source: io::Error,
    },
}

/// What a per-run service provisioner returns beside the capabilities: the
/// running service, opaque to the runtime. The driver holds it for the run's
/// lifetime and awaits its [`RunGuard::teardown`] when the run ends; drop is
/// the fallback for a run that never finishes.
pub type RunServiceGuard = Box<dyn RunGuard>;

/// A per-run service provisioner — see [`Runtime::run_services`].
type RunProvisioner = Arc<
    dyn Fn(
            &Path,
            ::steps::CapabilitiesBuilder,
        ) -> (::steps::CapabilitiesBuilder, Option<RunServiceGuard>)
        + Send
        + Sync,
>;

/// A problem an admission pass found: the diagnostic code it is reported
/// under, the node it belongs to (by name) or `None` for a problem of the
/// whole graph, and the message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionProblem {
    pub code:    SmolStr,
    pub node:    Option<SmolStr>,
    pub message: String,
}

impl AdmissionProblem {
    pub fn new(code: &str, node: Option<&str>, message: impl Into<String>) -> Self {
        Self {
            code:    SmolStr::new(code),
            node:    node.map(SmolStr::new),
            message: message.into(),
        }
    }
}

/// A pass [`Runtime::check`] runs over a lowered graph after the step
/// registry accepted it: the seam a component uses to resolve what the
/// frontend could not (a model selector against the host's catalog) and to
/// write the result into the graph, so the graph a run persists is what its
/// steps read.
///
/// A pass sees the runtime's static capabilities (`Runtime::capability`),
/// never a run's services or its directory. It returns every problem it
/// found; `check` turns each into an error diagnostic on the node's span and
/// hands out no graph. A pass must not panic on any graph the registry
/// accepted.
///
/// A pass may change a pre-lowered child graph as freely as the root: the
/// parent names its child by content digest, and `check` re-digests every
/// child a pass changed and rewrites the references to it, so the child is
/// still found by its digest at run time.
pub trait AdmissionPass: Send + Sync {
    fn admit(&self, graph: &mut Graph, caps: &::steps::Capabilities) -> Vec<AdmissionProblem>;
}

type ExecutorLayer = Arc<dyn Fn(Arc<dyn Executor>) -> Arc<dyn Executor> + Send + Sync>;

/// The assembled system: frontends, step kinds, executors, secrets, options.
pub struct Runtime {
    frontends:       Vec<Box<dyn Frontend>>,
    steps:           ::steps::Registry,
    executor:        Option<Arc<dyn Executor>>,
    executor_layers: Vec<ExecutorLayer>,
    /// The store runs live in; `None` is the run directory under
    /// `RunOptions::run_dir`.
    store:           Option<Arc<dyn RunStore>>,
    secrets:         Arc<dyn SecretProvider>,
    observers:       Vec<Arc<dyn EventObserver>>,
    progress:        Option<Arc<dyn ProgressSink>>,
    hooks:           Option<Arc<dyn ExecutionHooks>>,
    caps:            ::steps::CapabilitiesBuilder,
    provisioners:    Vec<RunProvisioner>,
    admissions:      Vec<Arc<dyn AdmissionPass>>,
    options:         RunOptions,
    /// The standard router acquires every scope on the simulated provider:
    /// a dry run.
    simulated:       bool,
    /// The providers the standard router reaches, when the host chose them;
    /// standalone Petri's first-party plugins otherwise.
    providers:       Option<ProviderRegistrations>,
    sources:         Sources,
}

/// Makes a store for the step output of the execution whose directory it is
/// given.
type StepLogs = Arc<dyn Fn(&Path) -> Arc<dyn StepLogStore> + Send + Sync>;

/// Where a run's recording stamps, step output and weighted routing draws
/// come from. The defaults are the wall clock, each execution directory's
/// `logs/` and the operating system. A simulation passes a virtual clock,
/// in-memory logs and a seed, so a run is a function of its inputs.
#[derive(Clone, Default)]
struct Sources {
    clock:     Option<RecordingClock>,
    step_logs: Option<StepLogs>,
    seed:      Option<u64>,
}

impl Sources {
    /// A driver's configuration with this run's clock and step logs.
    fn configure(&self, mut config: RunConfig) -> RunConfig {
        if let Some(clock) = &self.clock {
            config = config.with_recording_clock(clock.clone());
        }
        if let Some(step_logs) = &self.step_logs {
            let logs = step_logs(&config.run_dir);
            config = config.with_step_logs(logs);
        }
        config
    }

    /// A standalone driver with this run's seeded draws, when it has a seed.
    fn equip(&self, driver: Driver) -> Driver {
        match self.seed {
            Some(seed) => {
                driver.with_decision_resolver(Arc::new(SeededDecisionResolver::new(seed)))
            }
            None => driver,
        }
    }
}

impl Runtime {
    /// The standard configuration: the formats and step kinds core itself owns
    /// — the `native` frontend, the `noop` and `process` steps — no
    /// secrets, and, unless [`Runtime::executor`] overrides it, the
    /// [`RoutingExecutor`] dispatching by each scope's
    /// [`ir::RuntimeTarget`].
    ///
    /// A distribution or a consumer registers its own frontends on top with
    /// [`Runtime::frontend`]; each one goes to the front of the list, ahead of
    /// the native catch-all.
    ///
    /// The default run directory is under the system temp dir; set a real one
    /// with [`Runtime::options`].
    #[expect(
        clippy::absolute_paths,
        reason = "this crate's own `steps` module and the external `steps` crate differ only \
                  by the leading `::`; spelling the local one in full keeps the two apart \
                  beside the `::steps::` uses a few lines below"
    )]
    pub fn standard() -> Self {
        Self {
            frontends:       vec![Box::new(frontend_native::Native)],
            steps:           crate::steps::standard(),
            executor:        None,
            executor_layers: Vec::new(),
            store:           None,
            secrets:         Arc::new(MapSecrets::empty()),
            observers:       Vec::new(),
            progress:        None,
            hooks:           None,
            caps:            ::steps::Capabilities::builder(),
            provisioners:    Vec::new(),
            admissions:      Vec::new(),
            options:         RunOptions::new(
                env::temp_dir().join(format!("petri-run-{}", process::id())),
            ),
            simulated:       false,
            providers:       None,
            sources:         Sources::default(),
        }
    }

    /// No frontends, no step kinds: for a consumer that assembles everything
    /// itself.
    pub fn bare() -> Self {
        Self {
            frontends:       Vec::new(),
            steps:           ::steps::Registry::new(),
            executor:        None,
            executor_layers: Vec::new(),
            store:           None,
            secrets:         Arc::new(MapSecrets::empty()),
            observers:       Vec::new(),
            progress:        None,
            hooks:           None,
            caps:            ::steps::Capabilities::builder(),
            provisioners:    Vec::new(),
            admissions:      Vec::new(),
            options:         RunOptions::new(
                env::temp_dir().join(format!("petri-run-{}", process::id())),
            ),
            simulated:       false,
            providers:       None,
            sources:         Sources::default(),
        }
    }

    /// Stamp every record a run appends, its drivers' and its coordinator's,
    /// with `clock` instead of the wall clock.
    #[must_use]
    pub fn recording_clock(mut self, clock: RecordingClock) -> Self {
        self.sources.clock = Some(clock);
        self
    }

    /// Keep each driver's step output in the store `logs` makes for its
    /// execution directory, instead of that directory's `logs/`.
    #[must_use]
    pub fn step_logs(
        mut self,
        logs: impl Fn(&Path) -> Arc<dyn StepLogStore> + Send + Sync + 'static,
    ) -> Self {
        self.sources.step_logs = Some(Arc::new(logs));
        self
    }

    /// Roll every weighted routing draw from `seed` instead of the operating
    /// system. The draws are recorded either way.
    #[must_use]
    pub fn decision_seed(mut self, seed: u64) -> Self {
        self.sources.seed = Some(seed);
        self
    }

    /// Register a frontend. It goes to the front of the list, so a specific
    /// format is asked before the native catch-all.
    #[must_use]
    pub fn frontend(mut self, frontend: impl Frontend + 'static) -> Self {
        self.frontends.insert(0, Box::new(frontend));
        self
    }

    /// Replace the step registry.
    #[must_use]
    pub fn steps(mut self, registry: ::steps::Registry) -> Self {
        self.steps = registry;
        self
    }

    /// Register one step kind on the current registry.
    #[must_use]
    pub fn step<S: ::steps::Step>(mut self, step: S) -> Self {
        self.steps.register(step);
        self
    }

    /// Use one executor for every scope, whatever its target — the
    /// [`RoutingExecutor`] included.
    #[must_use]
    pub fn executor(mut self, executor: impl Executor + 'static) -> Self {
        self.executor = Some(Arc::new(executor));
        self
    }

    /// Wrap the run's executor while retaining the standard router's lease
    /// ledger, reconciliation and pruning. Layers are applied in registration
    /// order; the last registered layer is outermost. Each layer must forward
    /// acquisition and release to the executor it receives. A layer that adds
    /// to every process's environment, such as credentials, applies
    /// [`executor::EnvHandle::with_spawn_env`] to the handle it acquired.
    #[must_use]
    pub fn executor_layer(
        mut self,
        layer: impl Fn(Arc<dyn Executor>) -> Arc<dyn Executor> + Send + Sync + 'static,
    ) -> Self {
        self.executor_layers.push(Arc::new(layer));
        self
    }

    /// Acquire every scope on the simulated provider: no plugin is launched,
    /// no process runs, no workspace exists, and the run's scope records name
    /// the provider `simulated`. `petri run --dry-run` sets this, so a dry
    /// run touches no provider; a host that registers the stubs chooses,
    /// since its own hooks may still work a real workspace. The sandbox
    /// options a run is given still parse and are recorded, but place
    /// nothing. Does not apply under [`Runtime::executor`].
    #[must_use]
    pub fn simulated_sandboxes(mut self) -> Self {
        self.simulated = true;
        self
    }

    /// Reach Host, Docker, and Daytona through the built-in providers the
    /// embedding application links, instead of launching their plugins.
    /// Every standard router this runtime builds uses them: runs, resumes,
    /// [`Runtime::sandbox_router_for`], and prune. A kind with no factory
    /// fails routably at acquire; no plugin is launched in its place. A dry
    /// run ([`Runtime::simulated_sandboxes`]) still touches no provider,
    /// and [`Runtime::executor`] still replaces the router outright.
    #[must_use]
    pub fn in_process_providers(self, providers: InProcessProviders) -> Self {
        self.sandbox_providers(ProviderRegistrations::linked(providers))
    }

    /// Reach exactly the providers `providers` registers: linked built-ins,
    /// first-party plugins, and plugins registered for custom kinds. Every
    /// standard router this runtime builds uses them, as with
    /// [`Runtime::in_process_providers`]; a kind with no entry fails
    /// routably at acquire, and no plugin is launched in its place.
    #[must_use]
    pub fn sandbox_providers(mut self, providers: ProviderRegistrations) -> Self {
        self.providers = Some(providers);
        self
    }

    /// Keep every run's durable record in `store` instead of the run
    /// directory: a host's database, or [`store::MemoryRunStore`] for a run
    /// that leaves no files of record. The run directory still holds what
    /// is a file by nature (workspaces, step output). A resume through a
    /// host store needs `RunOptions::run_key`, since the directory no
    /// longer names the run.
    #[must_use]
    pub fn store(mut self, store: Arc<dyn RunStore>) -> Self {
        self.store = Some(store);
        self
    }

    #[must_use]
    pub fn secrets(mut self, secrets: impl SecretProvider + 'static) -> Self {
        self.secrets = Arc::new(secrets);
        self
    }

    /// Register an event observer on every driver this runtime builds: it sees
    /// every appended record, in seq order, with the post-apply state.
    #[must_use]
    pub fn observe(mut self, observer: Arc<dyn EventObserver>) -> Self {
        self.observers.push(observer);
        self
    }

    /// Register a progress sink on every driver this runtime builds: live
    /// acquisition events — image pulls, service health — which never enter
    /// the replay log.
    #[must_use]
    pub fn progress(mut self, sink: Arc<dyn ProgressSink>) -> Self {
        self.progress = Some(sink);
        self
    }

    /// Install the host's awaited extension points
    /// ([`driver::lifecycle`]) on every driver this runtime builds. Without
    /// them every driver takes the unchanged fast path.
    #[must_use]
    pub fn hooks(mut self, hooks: Arc<dyn ExecutionHooks>) -> Self {
        self.hooks = Some(hooks);
        self
    }

    /// Register a host service for steps, keyed by its concrete type: every
    /// step this runtime's drivers run can ask for it through `StepCtx`.
    ///
    /// # Panics
    ///
    /// On a duplicate type — registration is configuration, the same rule as
    /// step registration. A service with *run* lifetime registers a
    /// [`Runtime::run_services`] provisioner instead; a host assembling its
    /// own driver can also build its own [`::steps::Capabilities`] and call
    /// `Driver::with_capabilities` directly.
    #[must_use]
    pub fn capability<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        self.caps = self.caps.provide(value);
        self
    }

    /// Register a per-run service provisioner: called once per driver this
    /// runtime builds — fresh and resumed runs alike — with the run directory,
    /// to stand a run-scoped host service up and hand its capability to the
    /// run's steps. The guard it returns rides the driver; dropping the driver
    /// is the teardown. A provisioner that cannot start its service returns
    /// the capabilities unchanged and no guard — the steps that need it then
    /// fail routably (`capability_unavailable`), never the run.
    #[must_use]
    pub fn run_services<F>(mut self, provision: F) -> Self
    where
        F: Fn(
                &Path,
                ::steps::CapabilitiesBuilder,
            ) -> (::steps::CapabilitiesBuilder, Option<RunServiceGuard>)
            + Send
            + Sync
            + 'static,
    {
        self.provisioners.push(Arc::new(provision));
        self
    }

    /// Register an admission pass: [`Runtime::check`] runs it over every
    /// graph the step registry accepted, the root and the pre-lowered
    /// children, in registration order, with the runtime's static
    /// capabilities.
    #[must_use]
    pub fn admission(mut self, pass: impl AdmissionPass + 'static) -> Self {
        self.admissions.push(Arc::new(pass));
        self
    }

    #[must_use]
    pub fn options(mut self, options: RunOptions) -> Self {
        self.options = options;
        self
    }

    /// The options runs get, for a host wrapping this runtime — the standalone
    /// petri host reads the run directory here.
    pub fn run_options(&self) -> &RunOptions {
        &self.options
    }

    /// The awaited extension points installed with [`Runtime::hooks`], for a
    /// host that wraps them (a control service holding admission while paused
    /// delegates every other point to these).
    pub fn installed_hooks(&self) -> Option<Arc<dyn ExecutionHooks>> {
        self.hooks.clone()
    }

    /// Whether the installed host declares required completion work.
    pub fn requires_run_finalization(&self) -> bool {
        self.hooks
            .as_ref()
            .is_some_and(|hooks| hooks.requires_run_finalization())
    }

    /// The step registry, for lookups (`type_known`-style lints, validation).
    pub fn registry(&self) -> &::steps::Registry {
        &self.steps
    }

    // ── Frontends ──────────────────────────────────────────────────────────

    /// The frontend for a file: by `name` when given, else the first that
    /// claims the path.
    /// The frontend that recognizes `graph` as its own lowering, if any: how
    /// a host that holds only a stored graph (a resume) finds the format's
    /// launch settings and defaults.
    pub fn frontend_for_graph(&self, graph: &Graph) -> Option<&dyn Frontend> {
        self.frontends
            .iter()
            .map(AsRef::as_ref)
            .find(|frontend| frontend.claims_graph(graph))
    }

    pub fn frontend_for(
        &self,
        path: &Path,
        name: Option<&str>,
    ) -> Result<&dyn Frontend, LoadError> {
        let all: Vec<&dyn Frontend> = self.frontends.iter().map(AsRef::as_ref).collect();
        match name {
            Some(name) => frontend::by_name(&all, name).ok_or_else(|| LoadError::UnknownFormat {
                name:  name.to_string(),
                known: all.iter().map(|f| f.name().to_string()).collect(),
            }),
            None => frontend::detect(&all, path).ok_or_else(|| LoadError::NoFrontend {
                path: path.to_path_buf(),
            }),
        }
    }

    /// Read and lower one file. `Err` is an IO-or-usage problem; a rejected
    /// workflow comes back as `Ok` with diagnostics and no graph.
    ///
    /// The repository is the directory `repo` names, else the one the
    /// frontend finds for the file; the frontend reads `@file` references and
    /// the settings files beside the workflow from it, and the compile
    /// variable `petri.repository` is bound to its absolute path unless
    /// `inputs` already binds it. [`Runtime::lower_source`] is the same
    /// lowering over text the caller holds in memory.
    ///
    /// The span is the only place that knows which frontend claimed the file;
    /// the frontends themselves stay free of tracing, with diagnostics as their
    /// one output channel.
    #[tracing::instrument(
        name = "runtime.lower",
        level = "debug",
        skip_all,
        fields(
            workflow_file = %file.display(),
            frontend = Empty,
            node_count = Empty,
        )
    )]
    pub fn lower(
        &self,
        file: &Path,
        format: Option<&str>,
        repo: Option<&Path>,
        inputs: &CompileInputs,
    ) -> Result<Lowered, LoadError> {
        let frontend = self.frontend_for(file, format)?;
        let repo = repo.map_or_else(|| frontend.repo_root(file), Path::to_path_buf);
        let text = fs::read_to_string(file).map_err(|e| LoadError::Read {
            path:   file.to_path_buf(),
            source: e,
        })?;
        let name = file
            .strip_prefix(&repo)
            .unwrap_or(file)
            .to_string_lossy()
            .into_owned();
        // The repository root, absolute, for a format whose runs check it
        // out. A caller that bound the variable itself keeps its value.
        let mut inputs = inputs.clone();
        if !inputs.vars.contains_key(REPOSITORY_VAR) {
            // A relative file's root can be the empty path, the current
            // directory; canonicalize needs a name for it.
            let base = if repo.as_os_str().is_empty() {
                Path::new(".")
            } else {
                repo.as_path()
            };
            let absolute = fs::canonicalize(base).unwrap_or_else(|_| base.to_path_buf());
            inputs.vars.insert(
                SmolStr::new(REPOSITORY_VAR),
                Value::String(absolute.to_string_lossy().into_owned()),
            );
        }
        let files = DirFiles { root: repo };
        Ok(Self::lower_with(frontend, &name, &text, &files, &inputs))
    }

    /// Lower one workflow the caller holds in memory: [`Runtime::lower`]
    /// without the disk. `file` is the repository-relative path of the
    /// workflow, with `/` separators: the frontend is chosen by its
    /// extension (or by `format`), and every span the lowering reports
    /// names it. `files` is the repository the frontend reads `@file`
    /// references and the settings files beside the workflow from
    /// (`frontend::MapFiles` holds one as a map of paths to text). `inputs`
    /// is used as given: a host whose runs check a repository out binds
    /// `petri.repository` itself, the way [`Runtime::lower`] does from the
    /// repository directory.
    ///
    /// # Errors
    ///
    /// When `format` names no registered frontend, or no frontend claims
    /// `file`. A rejected workflow is `Ok` with diagnostics and no graph.
    #[tracing::instrument(
        name = "runtime.lower",
        level = "debug",
        skip_all,
        fields(workflow_file = %file, frontend = Empty, node_count = Empty)
    )]
    pub fn lower_source(
        &self,
        file: &str,
        text: &str,
        files: &dyn FileSource,
        format: Option<&str>,
        inputs: &CompileInputs,
    ) -> Result<Lowered, LoadError> {
        let frontend = self.frontend_for(Path::new(file), format)?;
        Ok(Self::lower_with(frontend, file, text, files, inputs))
    }

    /// The lowering both entry points share, recorded on the current
    /// `runtime.lower` span.
    fn lower_with(
        frontend: &dyn Frontend,
        file: &str,
        text: &str,
        files: &dyn FileSource,
        inputs: &CompileInputs,
    ) -> Lowered {
        let span = tracing::Span::current();
        span.record("frontend", frontend.name());
        let lowered = frontend.load(file, text, files, inputs);
        if let Some(graph) = &lowered.graph {
            span.record("node_count", graph.nodes.len());
        }
        lowered
    }

    /// [`Runtime::lower`], then validate the graph — and every pre-lowered
    /// child graph — against the step registry, so an unregistered kind or a
    /// bad literal config is a diagnostic here rather than a step failure at
    /// firing time. Only the registry pass runs: the frontend already ran the
    /// structural passes when it lowered. Then every registered
    /// [`AdmissionPass`] runs over the accepted graphs; a problem a pass
    /// reports is an error diagnostic on its node's span, and the graph is
    /// withheld as it is for a registry error.
    pub fn check(
        &self,
        file: &Path,
        format: Option<&str>,
        repo: Option<&Path>,
        inputs: &CompileInputs,
    ) -> Result<Lowered, LoadError> {
        let mut lowered = self.lower(file, format, repo, inputs)?;
        self.validate_and_admit(&mut lowered, &file.to_string_lossy());
        Ok(lowered)
    }

    /// [`Runtime::check`] over a workflow the caller holds in memory:
    /// [`Runtime::lower_source`], then the same registry validation and
    /// admission passes. The parameters are those of `lower_source`; a
    /// registry or admission diagnostic names `file`.
    ///
    /// # Errors
    ///
    /// As [`Runtime::lower_source`].
    pub fn check_source(
        &self,
        file: &str,
        text: &str,
        files: &dyn FileSource,
        format: Option<&str>,
        inputs: &CompileInputs,
    ) -> Result<Lowered, LoadError> {
        let mut lowered = self.lower_source(file, text, files, format, inputs)?;
        self.validate_and_admit(&mut lowered, file);
        Ok(lowered)
    }

    /// What [`Runtime::check`] adds to lowering: the registry pass over the
    /// root and the children, then the admission passes. A problem withholds
    /// the graphs and is reported as an error diagnostic in `file`.
    fn validate_and_admit(&self, lowered: &mut Lowered, file: &str) {
        let span = Span::file(file);
        let mut errors = Vec::new();
        if let Some(graph) = &lowered.graph {
            errors.extend(
                ir::validate_step_kinds(graph, &self.steps)
                    .err()
                    .into_iter()
                    .flatten(),
            );
        }
        for child in &lowered.children {
            errors.extend(
                ir::validate_step_kinds(child, &self.steps)
                    .err()
                    .into_iter()
                    .flatten(),
            );
        }
        if !errors.is_empty() {
            lowered.graph = None;
            lowered.children.clear();
            for error in errors {
                lowered
                    .diagnostics
                    .error(error.code(), span.clone(), error.to_string());
            }
            return;
        }
        let problems = self.admit(lowered);
        if !problems.is_empty() {
            lowered.graph = None;
            lowered.children.clear();
            for (problem, span) in problems {
                lowered
                    .diagnostics
                    .error(&problem.code, span_of(file, span), problem.message);
            }
        }
    }

    /// Run every admission pass over the root graph and the children, in
    /// registration order, then follow every changed child's new digest
    /// through the references to it. Each problem comes back with its
    /// node's source position, read from the frontend's `meta.span`, when
    /// it names a node the graph has.
    fn admit(&self, lowered: &mut Lowered) -> Vec<(AdmissionProblem, Option<(u32, u32)>)> {
        if self.admissions.is_empty() {
            return Vec::new();
        }
        let caps = self.caps.clone().build();
        let before: Vec<String> = lowered
            .children
            .iter()
            .map(frontend::graph_digest)
            .collect();
        let mut problems = Vec::new();
        let graphs = lowered.graph.iter_mut().chain(lowered.children.iter_mut());
        for graph in graphs {
            for pass in &self.admissions {
                for problem in pass.admit(graph, &caps) {
                    let position = problem
                        .node
                        .as_deref()
                        .and_then(|name| node_position(graph, name));
                    problems.push((problem, position));
                }
            }
        }
        if problems.is_empty() {
            redigest_children(lowered, before);
        }
        problems
    }

    // ── Running ────────────────────────────────────────────────────────────

    /// A driver over this configuration, for callers that need the handle (to
    /// cancel a run in flight). [`Runtime::run`] is the plain path.
    pub fn driver(&self, graph: Graph) -> Driver {
        let run = self.prepare_run(&self.options.run_dir);
        let driver = Driver::new(
            graph,
            run.executor.clone(),
            self.steps.clone(),
            self.secrets.clone(),
            self.run_config(),
        );
        run.equip_standalone(self.sources.equip(driver))
    }

    /// Prepare resources that are shared by every execution in one root run.
    pub fn prepare_run(&self, run_dir: impl Into<PathBuf>) -> RunRuntime {
        let run_dir = run_dir.into();
        let key = self.run_key_for(&run_dir);
        let (executor, router) = self.executor_for_run(&run_dir, &key);
        self.provision_run(run_dir, key, executor, router)
    }

    /// The run's key: the one the options name, else the one the run
    /// directory already stores (a resume over the run-directory store),
    /// else a fresh one.
    pub fn run_key_for(&self, run_dir: &Path) -> RunKey {
        self.options
            .run_key
            .clone()
            .or_else(|| {
                self.store
                    .is_none()
                    .then(|| RunDirStore::new(run_dir).stored_key().ok().flatten())
                    .flatten()
            })
            .unwrap_or_else(RunKey::mint)
    }

    /// The store runs under `run_dir` live in: the installed one, else the
    /// run directory itself.
    pub fn store_for(&self, run_dir: &Path) -> Arc<dyn RunStore> {
        self.store
            .clone()
            .unwrap_or_else(|| Arc::new(RunDirStore::new(run_dir)))
    }

    /// Open the run under `run_dir` in the runtime's store: what a host does
    /// to inspect, replay or prune a run without preparing it.
    pub async fn open_run(
        &self,
        run_dir: &Path,
        access: Access,
    ) -> Result<Arc<dyn RunLogs>, StoreError> {
        let key = self.run_key_for(run_dir);
        self.store_for(run_dir).open(&key, access).await
    }

    fn executor_for_run(
        &self,
        run_dir: &Path,
        key: &RunKey,
    ) -> (Arc<dyn Executor>, Option<Arc<RoutingExecutor>>) {
        // The standard router is kept by its own type too: the coordinator
        // hands it the lease ledger and releases leases through it. A
        // caller-supplied executor manages its own sandboxes.
        let (mut executor, router): (Arc<dyn Executor>, _) =
            if let Some(executor) = self.executor.clone() {
                (executor, None)
            } else {
                let router = self.default_router_for(run_dir, key);
                (router.clone(), Some(router))
            };
        for layer in &self.executor_layers {
            executor = layer(executor);
        }
        (executor, router)
    }

    fn provision_run(
        &self,
        run_dir: PathBuf,
        key: RunKey,
        executor: Arc<dyn Executor>,
        router: Option<Arc<RoutingExecutor>>,
    ) -> RunRuntime {
        let (caps, guards) = self.provision(&run_dir);
        RunRuntime {
            store: self.store_for(&run_dir),
            run_dir,
            key,
            owner: OwnerId::mint(),
            options: self.options.clone(),
            executor,
            router,
            steps: self.steps.clone(),
            secrets: self.secrets.clone(),
            observers: self.observers.clone(),
            progress: self.progress.clone(),
            hooks: self.hooks.clone(),
            caps,
            guards,
            sources: self.sources.clone(),
        }
    }

    /// Run every registered per-run service provisioner once, and collect the
    /// capabilities and guards it leaves behind.
    fn provision(&self, run_dir: &Path) -> (::steps::Capabilities, Vec<RunServiceGuard>) {
        let mut caps = self.caps.clone();
        let mut guards = Vec::new();
        for provision in &self.provisioners {
            let (next, guard) = provision(run_dir, caps);
            caps = next;
            guards.extend(guard);
        }
        (caps.build(), guards)
    }

    /// A driver continuing a crashed run's log, however the host stored it —
    /// the mirror of [`Runtime::driver`] over `Driver::resume`. `ResumeInfo`
    /// comes back beside the driver, so a host installs its own execution
    /// identities for the re-dispatched firings before calling `run()`.
    pub fn resume_driver(
        &self,
        graph: Graph,
        log: EventLog,
    ) -> Result<(Driver, ResumeInfo), ResumeError> {
        let key = self.run_key_for(&self.options.run_dir);
        let (executor, router) = self.executor_for_run(&self.options.run_dir, &key);
        let (driver, info) = Driver::resume(
            graph,
            log,
            executor.clone(),
            self.steps.clone(),
            self.secrets.clone(),
            self.run_config(),
        )?;
        let run = self.provision_run(self.options.run_dir.clone(), key, executor, router);
        Ok((
            run.equip_standalone(self.sources.equip(driver))
                .with_capability(info.clone()),
            info,
        ))
    }

    fn run_config(&self) -> RunConfig {
        self.sources.configure(
            base_run_config(&self.options, self.options.run_dir.clone())
                .with_retention(self.options.retention),
        )
    }

    /// Run a graph to completion. With `verify_replay` on (the default), the
    /// log is replayed afterwards and any divergence is the error.
    pub async fn run(&self, graph: Graph) -> Result<ExecutionReport, ReplayMismatch> {
        self.run_verified(graph, |graph| Ok(self.driver(graph)))
            .await
    }

    /// Run the driver `build` makes over `graph` to completion, with the same
    /// verification as [`Runtime::run`]: `verify_replay` on (the default)
    /// replays the log against the graph as it was before the run, and any
    /// divergence is the error. For hosts that build their own driver — one
    /// with observers attached, or a resumed one.
    pub async fn run_verified<E: From<ReplayMismatch>>(
        &self,
        graph: Graph,
        build: impl FnOnce(Graph) -> Result<Driver, E>,
    ) -> Result<ExecutionReport, E> {
        let original = self.options.verify_replay.then(|| graph.clone());
        let report = build(graph)?.run().await;
        if let Some(graph) = original {
            // The engine itself emits nothing, precisely because replay would
            // say it all a second time; the check gets a span here instead.
            let span = tracing::debug_span!(
                "runtime.verify_replay",
                record_count = report.state.log.len()
            );
            let _entered = span.enter();
            engine::verify_replay(graph, &report.state.log)?;
        }
        Ok(report)
    }

    /// The mask set of the configured `SecretProvider`, for a host that
    /// persists anything beside the run.
    pub fn masker(&self) -> Masker {
        self.secrets.masker()
    }

    /// Build the standard sandbox router for maintenance without starting
    /// run services. A caller-supplied executor owns its own resources.
    pub fn sandbox_router_for(&self, run_dir: &Path) -> Option<Arc<RoutingExecutor>> {
        self.executor
            .is_none()
            .then(|| self.default_router_for(run_dir, &self.run_key_for(run_dir)))
    }

    fn default_router_for(&self, run_dir: &Path, key: &RunKey) -> Arc<RoutingExecutor> {
        let router = if self.simulated {
            RoutingExecutor::simulated(run_dir)
        } else {
            let router = RoutingExecutor::with_options(
                run_dir,
                self.options.retention,
                self.options.sandbox.clone(),
            );
            match &self.providers {
                Some(providers) => router.with_providers(providers.clone()),
                None => router,
            }
        };
        Arc::new(router.with_run_id(key.as_str()))
    }
}

/// How a [`RunRuntime`] opens its run: the access modes of
/// [`store::Access`] under the run's own key and owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunAccess {
    Create,
    Write,
    Read,
}

/// Shared runtime services and configuration for all executions in one run.
pub struct RunRuntime {
    /// The store the run's durable record lives in.
    store:     Arc<dyn RunStore>,
    run_dir:   PathBuf,
    /// The run's identity in its store and on its providers.
    key:       RunKey,
    /// This coordinator instance, for the store's writer lease.
    owner:     OwnerId,
    options:   RunOptions,
    executor:  Arc<dyn Executor>,
    /// The standard router, when the executor is one.
    router:    Option<Arc<RoutingExecutor>>,
    steps:     ::steps::Registry,
    secrets:   Arc<dyn SecretProvider>,
    observers: Vec<Arc<dyn EventObserver>>,
    progress:  Option<Arc<dyn ProgressSink>>,
    hooks:     Option<Arc<dyn ExecutionHooks>>,
    caps:      ::steps::Capabilities,
    guards:    Vec<RunServiceGuard>,
    sources:   Sources,
}

impl RunRuntime {
    /// Whether the installed host declares required completion work.
    pub fn requires_run_finalization(&self) -> bool {
        self.hooks
            .as_ref()
            .is_some_and(|hooks| hooks.requires_run_finalization())
    }

    /// A standalone driver's completion owns this run's service teardown.
    /// It is the run's one execution: the root invocation's first.
    fn equip_standalone(self, driver: Driver) -> Driver {
        let context = HookContext::new(self.key.clone(), InvocationId::ROOT, ExecutionId::new(0));
        let driver = attach(
            driver.with_capabilities(self.caps.clone()),
            &self.observers,
            self.progress.as_ref(),
            self.hooks.as_ref(),
            context,
        );
        driver.with_run_guard(Box::new(self))
    }

    pub fn run_dir(&self) -> &Path {
        &self.run_dir
    }

    /// The run's key: its identity in its store, and the run id every
    /// sandbox of the run is labelled with.
    pub fn run_key(&self) -> &RunKey {
        &self.key
    }

    /// This coordinator instance's owner id, minted when the run was
    /// prepared: what the store's writer lease is taken for.
    pub fn owner(&self) -> &OwnerId {
        &self.owner
    }

    /// The store the run's durable record lives in.
    pub fn store(&self) -> &Arc<dyn RunStore> {
        &self.store
    }

    /// Open this run in its store: `Create` for a fresh run, `Write` to
    /// continue one, both under this instance's owner; `Read` takes no
    /// lease.
    pub async fn open(&self, access: RunAccess) -> Result<Arc<dyn RunLogs>, StoreError> {
        let access = match access {
            RunAccess::Create => Access::Create {
                owner: self.owner.clone(),
            },
            RunAccess::Write => Access::Write {
                owner: self.owner.clone(),
            },
            RunAccess::Read => Access::Read,
        };
        self.store.open(&self.key, access).await
    }

    /// The standard routing executor, when this run uses it: the host that
    /// prunes sandboxes reaches the run's lease manager through it.
    pub fn sandbox_router(&self) -> Option<&Arc<RoutingExecutor>> {
        self.router.as_ref()
    }

    /// Hand the router the durable record of sandbox leases. Every
    /// container scope acquired after this is recorded there; before it, or
    /// under a caller-supplied executor, leases live in memory.
    pub fn attach_lease_ledger(&self, ledger: Arc<dyn LeaseLedger>) {
        if let Some(router) = &self.router {
            router.set_ledger(ledger);
        }
    }

    /// Reconcile the run's recorded leases with their providers before any
    /// create: the takeover step of a resume. Each lease goes to the
    /// provider its record names. A no-op under a caller-supplied executor.
    pub async fn reconcile_leases(
        &self,
        leases: &[executor_sandbox::RecordedLease],
    ) -> executor_sandbox::ReconcileReport {
        match &self.router {
            Some(router) => router.reconcile(leases).await,
            None => executor_sandbox::ReconcileReport::default(),
        }
    }

    /// End a lease's sandbox: stop it, then keep or delete it by this run's
    /// retention for `outcome`. A no-op under a caller-supplied executor.
    pub async fn release_lease(
        &self,
        lease: executor::SandboxLeaseId,
        outcome: executor::ScopeOutcome,
    ) -> executor::ReleaseReport {
        match &self.router {
            Some(router) => router.release_lease(lease, outcome).await,
            None => executor::ReleaseReport::default(),
        }
    }

    /// Build one execution driver without provisioning run services again.
    /// `context` names the execution: its ids supply the scope identities,
    /// and the installed hooks receive it with every callback.
    pub fn driver(
        &self,
        graph: Graph,
        start: EngineStart,
        execution_dir: impl Into<PathBuf>,
        context: HookContext,
        sandbox: SandboxAssignment,
        secrets: Arc<dyn SecretProvider>,
    ) -> Driver {
        let driver = Driver::new(
            graph,
            self.executor.clone(),
            self.steps.clone(),
            secrets,
            self.execution_config(execution_dir.into(), &context, sandbox),
        )
        .with_engine_start(start)
        .with_capabilities(self.caps.clone());
        attach(
            driver,
            &self.observers,
            self.progress.as_ref(),
            self.hooks.as_ref(),
            context,
        )
    }

    /// The resume counterpart of [`RunRuntime::driver`].
    pub fn resume_driver(
        &self,
        graph: Graph,
        log: EventLog,
        execution_dir: impl Into<PathBuf>,
        context: HookContext,
        sandbox: SandboxAssignment,
        secrets: Arc<dyn SecretProvider>,
    ) -> Result<(Driver, ResumeInfo), ResumeError> {
        let (driver, info) = Driver::resume(
            graph,
            log,
            self.executor.clone(),
            self.steps.clone(),
            secrets,
            self.execution_config(execution_dir.into(), &context, sandbox),
        )?;
        let driver = driver
            .with_capabilities(self.caps.clone())
            .with_capability(info.clone());
        Ok((
            attach(
                driver,
                &self.observers,
                self.progress.as_ref(),
                self.hooks.as_ref(),
                context,
            ),
            info,
        ))
    }

    pub fn secret_provider(&self) -> Arc<dyn SecretProvider> {
        self.secrets.clone()
    }

    /// The clock the run's records are stamped with.
    pub fn recording_clock(&self) -> RecordingClock {
        self.sources.clock.clone().unwrap_or_default()
    }

    /// Where an execution's weighted routing draws come from: the operating
    /// system, or the run's seed mixed with the execution's id, so no two
    /// executions draw the same sequence.
    pub fn decision_rolls(&self, execution: ExecutionId) -> DecisionRolls {
        self.sources
            .seed
            .map_or_else(DecisionRolls::default, |seed| {
                DecisionRolls::seeded(seed ^ execution.raw().wrapping_mul(0xD1B5_4A32_D192_ED03))
            })
    }

    pub fn masker(&self) -> Masker {
        self.secrets.masker()
    }

    /// Tear down run services and plugins after each lease applies retention.
    pub async fn finish(mut self) {
        for guard in mem::take(&mut self.guards) {
            guard.teardown().await;
        }
        if let Some(router) = &self.router {
            router.shutdown().await;
        }
    }

    fn execution_config(
        &self,
        execution_dir: PathBuf,
        context: &HookContext,
        sandbox: SandboxAssignment,
    ) -> RunConfig {
        // An execution ends before its invocation can restart. Workspace
        // retention therefore belongs to the lease's release, not to an
        // individual driver release.
        self.sources.configure(
            base_run_config(&self.options, execution_dir)
                .with_retention(Retention::Always)
                .with_scope_identities(
                    context.execution.environment_prefix(),
                    context.invocation.workspace_prefix(),
                )
                .with_sandbox_assignment(sandbox),
        )
    }
}

#[async_trait::async_trait]
impl RunGuard for RunRuntime {
    async fn teardown(self: Box<Self>) {
        self.finish().await;
    }
}

/// The `{line, column}` the frontend recorded under a node's `meta.span`,
/// when the node exists and the position is known.
fn node_position(graph: &Graph, name: &str) -> Option<(u32, u32)> {
    let node = graph.body.nodes.iter().find(|node| node.name == name)?;
    let span = node.meta.get("span")?;
    let read = |key: &str| span.get(key)?.as_u64().and_then(|n| u32::try_from(n).ok());
    let line = read("line")?;
    (line > 0).then(|| (line, read("column").unwrap_or(0)))
}

/// Rewrite every reference to a child whose content changed. A parent names
/// a child by the digest the frontend computed; a step config string equal
/// to a changed child's old digest becomes the new one, in the root and in
/// every child. A child that references a changed child changes too, so the
/// rewrite repeats until every digest is stable; references form a tree, so
/// that takes at most one round per level.
fn redigest_children(lowered: &mut Lowered, mut before: Vec<String>) {
    for _ in 0..=lowered.children.len() {
        let after: Vec<String> = lowered
            .children
            .iter()
            .map(frontend::graph_digest)
            .collect();
        let changed: Vec<(&String, &String)> = before
            .iter()
            .zip(&after)
            .filter(|(old, new)| old != new)
            .collect();
        if changed.is_empty() {
            return;
        }
        let graphs = lowered.graph.iter_mut().chain(lowered.children.iter_mut());
        for graph in graphs {
            for node in &mut graph.body.nodes {
                for (old, new) in &changed {
                    replace_string(&mut node.step.config, old, new);
                }
            }
        }
        before = after;
    }
}

/// Replace every string in `value` equal to `from` with `to`.
fn replace_string(value: &mut Value, from: &str, to: &str) {
    match value {
        Value::String(text) if text == from => to.clone_into(text),
        Value::Array(items) => {
            for item in items {
                replace_string(item, from, to);
            }
        }
        Value::Object(map) => {
            for item in map.values_mut() {
                replace_string(item, from, to);
            }
        }
        _ => {}
    }
}

/// A span in `file`: at `position` when a node supplied one, else the file
/// alone.
fn span_of(file: &str, position: Option<(u32, u32)>) -> Span {
    match position {
        Some((line, column)) => Span::new(file, line, column),
        None => Span::file(file),
    }
}

/// The `RunConfig` fields every driver takes straight from [`RunOptions`];
/// retention and scope identities stay with each caller.
fn base_run_config(options: &RunOptions, run_dir: PathBuf) -> RunConfig {
    let mut config = RunConfig::new(run_dir)
        .with_grace(options.grace)
        .with_cleanup_grace(options.cleanup_grace)
        .with_echo(options.echo);
    config.hard_deadline_slack = options.hard_deadline_slack;
    config
}

/// Attach the runtime-registered observers, progress sink and hooks to a
/// driver. The hooks are one object per runtime; `context` is what tells
/// them which execution each callback belongs to.
fn attach(
    mut driver: Driver,
    observers: &[Arc<dyn EventObserver>],
    progress: Option<&Arc<dyn ProgressSink>>,
    hooks: Option<&Arc<dyn ExecutionHooks>>,
    context: HookContext,
) -> Driver {
    for observer in observers {
        driver = driver.observe(observer.clone());
    }
    if let Some(progress) = progress {
        driver = driver.with_progress(progress.clone());
    }
    if let Some(hooks) = hooks {
        driver = driver.with_hooks(hooks.clone(), context);
    }
    driver
}
