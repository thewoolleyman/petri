//! The command line.
//!
//! `check <workflow>` parses, lowers and validates a workflow file — including
//! the step registry, so an unknown step kind or a bad literal config is caught
//! here — and prints every diagnostic with its span. `run` runs one to
//! completion and writes the event log beside the workspaces. `replay` feeds a
//! saved log back through the engine and checks byte-identity — the determinism
//! canary, runnable from the shell.
//!
//! This crate names no format. It is the command line over whatever
//! [`Runtime`] it is handed: [`main`] takes a factory and calls it once per
//! command, so the binary that ships — and any other binary — decides which
//! frontends and step kinds are registered. Anything a format needs from its
//! host comes from the frontend itself — where its repository root is, and the
//! run parameters a run would otherwise have to hard-code — so no command here
//! has a special case for one format.

pub mod answer;
pub mod control;
mod inspect;
mod providers;
mod resume;
mod session;
#[cfg(test)]
mod tests;

use std::error::Error;
use std::path::{Path, PathBuf};
use std::process::{self, ExitCode};
use std::{env, fs};

use clap::{Args, Parser, Subcommand, ValueEnum};
use execution::controls::ControlService;
use execution::prune as sandbox_prune;
use runtime::engine::{self, EventLog};
use runtime::executor::Retention;
use runtime::frontend::{
    self, CompileInputs, Frontend, LAUNCH_ENVIRONMENT_VAR, LAUNCH_GOAL_VAR, LAUNCH_MODEL_VAR,
    LAUNCH_PROVIDER_VAR, LaunchSettings, Lowered, WorkspaceRetention,
};
use runtime::ir::Graph;
use runtime::{DaytonaSandboxKind, LoadError, RunOptions, Runtime, SandboxBackend, SandboxOptions};
use session::{Session, SessionArgs, Start};
use tracing::field::Empty;

use crate::control::TailFrom;

#[derive(Parser)]
#[command(name = "petri", version, about = "A token-flow workflow engine")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
struct FileArgs {
    /// The workflow file.
    file:        PathBuf,
    /// Which format the file is in. Guessed from its path when omitted; the
    /// last frontend asked claims everything, so an unrecognized path is
    /// native.
    #[arg(long)]
    format:      Option<String>,
    /// Repository root, for resolving a format's local includes. Defaults to
    /// wherever the file's own format says its repository root is.
    #[arg(long)]
    repo:        Option<PathBuf>,
    /// A run input the format renders before lowering, as `KEY=VALUE`. The
    /// value is read as JSON when it parses as JSON, else as a string.
    /// Repeatable; later values win.
    #[arg(long = "input", value_name = "KEY=VALUE")]
    inputs:      Vec<String>,
    /// A JSON file of run inputs: either `{"inputs": {...}, "vars": {...}}`
    /// or a flat object of inputs. `--input` values land on top of it.
    #[arg(long)]
    inputs_file: Option<PathBuf>,
}

impl FileArgs {
    /// The compile inputs these arguments describe. `Err` is a usage error.
    fn compile_inputs(&self) -> Result<CompileInputs, String> {
        let mut inputs = CompileInputs::new();
        if let Some(path) = &self.inputs_file {
            let text = fs::read_to_string(path)
                .map_err(|e| format!("could not read {}: {e}", path.display()))?;
            let value: serde_json::Value = serde_json::from_str(&text)
                .map_err(|e| format!("{} is not JSON: {e}", path.display()))?;
            let serde_json::Value::Object(mut map) = value else {
                return Err(format!("{} must hold a JSON object", path.display()));
            };
            let has_sections = map.contains_key("inputs") || map.contains_key("vars");
            if has_sections {
                if let Some(unexpected) = map
                    .keys()
                    .find(|key| !matches!(key.as_str(), "inputs" | "vars"))
                {
                    return Err(format!(
                        "{} has unexpected top-level key `{unexpected}`; a sectioned input file permits only `inputs` and `vars`",
                        path.display()
                    ));
                }
                for (section, target) in
                    [("inputs", &mut inputs.inputs), ("vars", &mut inputs.vars)]
                {
                    let Some(value) = map.remove(section) else {
                        continue;
                    };
                    let serde_json::Value::Object(values) = value else {
                        return Err(format!(
                            "{} field `{section}` must hold a JSON object",
                            path.display()
                        ));
                    };
                    if values.contains_key("") {
                        return Err(format!(
                            "{} field `{section}` contains an empty key",
                            path.display()
                        ));
                    }
                    target.extend(values.into_iter().map(|(k, v)| (k.into(), v)));
                }
            } else {
                if map.contains_key("") {
                    return Err(format!("{} contains an empty input key", path.display()));
                }
                inputs
                    .inputs
                    .extend(map.into_iter().map(|(k, v)| (k.into(), v)));
            }
        }
        for pair in &self.inputs {
            let Some((key, value)) = pair.split_once('=') else {
                return Err(format!("`--input {pair}` is not `KEY=VALUE`"));
            };
            if key.is_empty() {
                return Err("`--input` keys cannot be empty".into());
            }
            let value = serde_json::from_str(value)
                .unwrap_or_else(|_| serde_json::Value::String(value.to_string()));
            inputs.inputs.insert(key.into(), value);
        }
        Ok(inputs)
    }
}

/// `--model`, `--provider` and `--environment`: the launch-level settings a
/// format's run configuration reads. A format whose LLM nodes name models
/// reads the model choice over its defaults (the graph's, the run
/// configuration's) but below a model a node names itself; a format with
/// named environments reads the environment selection over its files. Both
/// follow Fabro's own options. A format without either ignores them.
#[derive(Args, Default)]
struct ModelArgs {
    /// The model every prompt or agent node runs on, over the graph's and the
    /// workflow's run configuration's defaults. A node that names its own
    /// model keeps it.
    #[arg(long)]
    model:       Option<String>,
    /// The provider for a node that names no model of its own, over the same
    /// defaults. Alone, where nothing names a model, the provider's default
    /// model in the runner's catalog.
    #[arg(long)]
    provider:    Option<String>,
    /// The execution environment to run in, by the id the workflow's run
    /// configuration declares it under, over what the configuration
    /// selects itself.
    #[arg(long)]
    environment: Option<String>,
    /// The run goal, over the one the workflow's run configuration or the
    /// file itself states.
    #[arg(long)]
    goal:        Option<String>,
}

impl ModelArgs {
    /// Bind the launch settings as compile variables, for the format to read.
    fn bind(&self, mut inputs: CompileInputs) -> CompileInputs {
        if let Some(model) = &self.model {
            inputs = inputs.with_var(LAUNCH_MODEL_VAR, model.as_str());
        }
        if let Some(provider) = &self.provider {
            inputs = inputs.with_var(LAUNCH_PROVIDER_VAR, provider.as_str());
        }
        if let Some(environment) = &self.environment {
            inputs = inputs.with_var(LAUNCH_ENVIRONMENT_VAR, environment.as_str());
        }
        if let Some(goal) = &self.goal {
            inputs = inputs.with_var(LAUNCH_GOAL_VAR, goal.as_str());
        }
        inputs
    }
}

#[derive(Args)]
struct ProviderArgs {
    /// Where workflow processes run: host, docker, daytona, or a provider
    /// kind registered in --sandbox-providers. Defaults to what the
    /// workflow's own configuration asks for, else host.
    #[arg(long)]
    backend:            Option<SandboxBackend>,
    /// A TOML file registering sandbox-driver plugins under custom provider
    /// kinds, beside the first-party ones. The same file serves run,
    /// resume, and prune.
    #[arg(long, value_name = "PATH")]
    sandbox_providers:  Option<PathBuf>,
    /// Allow plugins without a pinned checksum. Debug builds allow them by
    /// default.
    #[arg(long)]
    sandbox_plugin_dev: bool,
}

/// A sandbox configuration the command line cannot use: the run never
/// starts.
#[expect(clippy::print_stderr, reason = "a command line diagnostic")]
fn provider_error(message: &str) -> ExitCode {
    eprintln!("sandbox providers: {message}");
    ExitCode::FAILURE
}

impl ProviderArgs {
    /// Whether `--backend` was given, so a workflow's own environment does
    /// not override it.
    fn backend_given(&self) -> bool {
        self.backend.is_some()
    }

    /// The runtime with `--sandbox-providers` registered, when given.
    async fn configure(&self, runtime: Runtime) -> Result<Runtime, String> {
        match &self.sandbox_providers {
            Some(path) => providers::configure(runtime, path).await,
            None => Ok(runtime),
        }
    }

    fn options(&self) -> SandboxOptions {
        SandboxOptions {
            backend: self.backend.clone().unwrap_or_default(),
            plugin_dev: self.sandbox_plugin_dev.then_some(true),
            ..Default::default()
        }
    }
}

#[derive(Args)]
struct RunnerArgs {
    /// Daytona offering for the runner: container (default) or vm.
    #[arg(long)]
    daytona_kind:      Option<DaytonaSandboxKind>,
    /// Override a placement label's runner image. Repeatable; later values win.
    #[arg(long = "runner-image", value_name = "LABEL=IMAGE", value_parser = runner_image)]
    images:            Vec<(String, String)>,
    /// CPUs in a Daytona runner snapshot (default and minimum: 2).
    #[arg(long, value_parser = clap::value_parser!(u32).range(2..))]
    daytona_cpus:      Option<u32>,
    /// Memory in a Daytona runner snapshot, in MiB (default and minimum: 4096).
    #[arg(long, value_parser = clap::value_parser!(u64).range(4096..))]
    daytona_memory_mb: Option<u64>,
    /// Disk in a Daytona runner snapshot, in MiB. Omit to let Daytona choose.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    daytona_disk_mb:   Option<u64>,
}

fn runner_image(value: &str) -> Result<(String, String), String> {
    match value.split_once('=') {
        Some((label, image)) if !label.trim().is_empty() && !image.trim().is_empty() => {
            Ok((label.to_owned(), image.to_owned()))
        }
        _ => Err("expected LABEL=IMAGE with a nonempty label and image".to_owned()),
    }
}

#[derive(Subcommand)]
enum Command {
    /// Parse, lower and validate a workflow file; print diagnostics.
    Check {
        #[command(flatten)]
        target:      FileArgs,
        /// Print the lowered graph in a stable text form.
        #[arg(long)]
        print_graph: bool,
        /// Print diagnostics as JSON lines instead of text.
        #[arg(long)]
        json:        bool,
    },
    /// Lower a workflow file and print the graph in a stable text form.
    PrintGraph {
        #[command(flatten)]
        target: FileArgs,
    },
    /// Run a workflow file to completion.
    Run {
        #[command(flatten)]
        target:   FileArgs,
        /// Where workspaces, logs and `events.json` go. Defaults to a fresh
        /// directory under the system temp dir, printed at start.
        #[arg(long)]
        run_dir:  Option<PathBuf>,
        #[command(flatten)]
        session:  SessionArgs,
        /// Simulate the step kinds that offer it (Fabro's stages) instead of
        /// running them: every stage succeeds, a human gate takes its first
        /// choice. No sandbox is acquired on any provider, so no plugin is
        /// needed, whatever backend the workflow selects.
        #[arg(long)]
        dry_run:  bool,
        #[command(flatten)]
        model:    ModelArgs,
        #[command(flatten)]
        provider: ProviderArgs,
        #[command(flatten)]
        runner:   RunnerArgs,
    },
    /// Continue an interrupted run from its run directory. Needs no workflow
    /// file: the run directory is the record. Finished work is not repeated;
    /// a gate that was waiting asks again; a paused run stays paused until an
    /// unpause arrives through `--control`. Refuses a finished run, a run
    /// another process holds, and a run directory that does not decode.
    Resume {
        /// The run's directory.
        #[arg(long)]
        run_dir:  PathBuf,
        #[command(flatten)]
        session:  SessionArgs,
        /// Continue with the simulated step kinds, as `run --dry-run` would.
        #[arg(long)]
        dry_run:  bool,
        #[command(flatten)]
        provider: ProviderArgs,
        #[command(flatten)]
        runner:   RunnerArgs,
    },
    /// Reconstruct a run from its run directory's durable files and print
    /// the result. Read-only: nothing starts, nothing is written.
    Inspect {
        /// The run's directory.
        #[arg(long)]
        run_dir: PathBuf,
        /// Print the versioned JSON document instead of a summary.
        #[arg(long)]
        json:    bool,
    },
    /// Sandboxes a run holds on its provider.
    #[command(subcommand)]
    Sandbox(SandboxCommand),
    /// Replay a saved event log against the workflow and verify byte-identity.
    ///
    /// Lower the same file on the same machine — and the same checkout state,
    /// since a format's default run parameters may read it (GHA reads HEAD) —
    /// as the original run: the graph, including the default run parameters,
    /// is the replay's input. A host that persists the graph itself (the run
    /// dir's `graphs/<digest>.json`) has no such constraint.
    Replay {
        #[command(flatten)]
        target: FileArgs,
        /// The `events.json` a run wrote.
        log:    PathBuf,
        /// The launch settings the original run was given, so the graph
        /// lowers the same.
        #[command(flatten)]
        model:  ModelArgs,
    },
}

#[derive(Subcommand)]
enum SandboxCommand {
    /// Delete every sandbox a finished or abandoned run still holds on its
    /// provider, and record each as gone. Refuses a run a live process
    /// holds. Deleting a sandbox also deletes its managed workspace.
    Prune {
        /// The run's directory.
        #[arg(long)]
        run_dir:  PathBuf,
        #[command(flatten)]
        provider: ProviderArgs,
    },
}

/// `--retain`: when a run's workspaces survive it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Retain {
    /// Keep every workspace: after success, failure and cancellation.
    Always,
    /// Keep a failed run's workspaces; delete a successful run's.
    OnFailure,
    /// Delete every workspace.
    Never,
}

impl From<Retain> for Retention {
    fn from(retain: Retain) -> Self {
        match retain {
            Retain::Always => Self::Always,
            Retain::OnFailure => Self::OnFailure,
            Retain::Never => Self::Never,
        }
    }
}

impl From<WorkspaceRetention> for Retain {
    fn from(retention: WorkspaceRetention) -> Self {
        match retention {
            WorkspaceRetention::Always => Self::Always,
            WorkspaceRetention::OnFailure => Self::OnFailure,
            WorkspaceRetention::Never => Self::Never,
        }
    }
}

/// Which runtime a command wants from the factory it is handed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeMode {
    /// The real step kinds.
    Real,
    /// `run --dry-run`: the distribution's simulated step kinds, for formats
    /// that have them — a workflow runs end to end with no model, shell or
    /// person behind its stages.
    DryRun,
}

/// Parse the arguments and run the command, on a runtime from `make`. One
/// command, one runtime.
pub async fn main(make: impl Fn(RuntimeMode) -> Runtime) -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Check {
            target,
            print_graph,
            json,
        } => check(&make(RuntimeMode::Real), &target, print_graph, json),
        Command::PrintGraph { target } => check(&make(RuntimeMode::Real), &target, true, false),
        Command::Run {
            target,
            run_dir,
            session,
            dry_run,
            model,
            provider,
            runner,
        } => {
            let run_dir = run_dir
                .unwrap_or_else(|| env::temp_dir().join(format!("petri-run-{}", process::id())));
            // The workflow is lowered before the runtime mode is settled:
            // its own configuration (Fabro's `[run.execution]` and
            // `[run.environment]`) supplies the defaults an explicit option
            // does not override. Both registries validate the same kinds;
            // only the real runtime's admission passes change the graph (a
            // model pinned against the catalog), and a dry run keeps its
            // selectors, so `--dry-run` lowers on the simulated runtime.
            let first_mode = if dry_run {
                RuntimeMode::DryRun
            } else {
                RuntimeMode::Real
            };
            let mut lowered = match lowered_graph(&make(first_mode), &target, &model, false, false)
            {
                Ok(lowered) => lowered,
                Err(code) => return code,
            };
            let launch = launch_settings(&make(RuntimeMode::Real), &target, &lowered);
            let runtime_mode = if dry_run || launch.dry_run {
                RuntimeMode::DryRun
            } else {
                RuntimeMode::Real
            };
            let rt = make(runtime_mode);
            if runtime_mode != first_mode {
                // The launch asked for a dry run after a real lowering:
                // lower again on the simulated runtime, so the graph carries
                // no admission-time resolution. The diagnostics were printed
                // once already; a graph the simulated runtime withholds keeps
                // the real one, which validated the same kinds.
                let inputs = target
                    .compile_inputs()
                    .map_or_else(|_| CompileInputs::new(), |inputs| model.bind(inputs));
                if let Ok(again) = rt.check(
                    &target.file,
                    target.format.as_deref(),
                    target.repo.as_deref(),
                    &inputs,
                ) && again.graph.is_some()
                {
                    lowered = again;
                }
            }
            // One control service for the terminal's control file and an
            // embedded host alike; its pause hook wraps the hooks the
            // distribution installed, so both run at admission.
            let controls = ControlService::new();
            let hooks = controls.hooks(rt.installed_hooks());
            let rt = rt.hooks(hooks).capability(controls.turns());
            let default_retention = rt
                .frontend_for(&target.file, target.format.as_deref())
                .ok()
                .map(Frontend::default_retention);
            let options = match session.run_options(
                &run_dir,
                default_retention,
                &launch,
                &provider,
                runner,
            ) {
                Ok(options) => options,
                Err(error) => return provider_error(&error),
            };
            let rt = match provider.configure(rt.options(options)).await {
                Ok(rt) => rt,
                Err(error) => return provider_error(&error),
            };
            let answers = session.answers(&launch);
            let session = Session {
                answers,
                controls,
                control_file: session.control.map(|path| (path, TailFrom::Start)),
            };
            Box::pin(run(&rt, &target, &run_dir, lowered, session)).await
        }
        Command::Resume {
            run_dir,
            session,
            dry_run,
            provider,
            runner,
        } => {
            Box::pin(resume::resume(
                &make, run_dir, dry_run, session, provider, runner,
            ))
            .await
        }
        Command::Replay { target, log, model } => {
            replay(&make(RuntimeMode::Real), &target, &model, &log)
        }
        Command::Inspect { run_dir, json } => inspect::inspect(&run_dir, json).await,
        Command::Sandbox(SandboxCommand::Prune { run_dir, provider }) => {
            let mut options = RunOptions::new(&run_dir);
            options.sandbox = provider.options();
            let rt = match provider
                .configure(make(RuntimeMode::Real).options(options))
                .await
            {
                Ok(rt) => rt,
                Err(error) => return provider_error(&error),
            };
            prune(&rt).await
        }
    }
}

/// `sandbox prune`: one line per lease, and a failure exit when any lease
/// could not be pruned, so a script can retry.
#[expect(
    clippy::print_stderr,
    reason = "the command's report is its output; there is no subscriber to route it to"
)]
async fn prune(rt: &Runtime) -> ExitCode {
    let report = match sandbox_prune::prune(rt).await {
        Ok(report) => report,
        Err(error) => {
            eprintln!("error: {}", error_chain(&error));
            return ExitCode::from(3);
        }
    };
    for (lease, ids) in &report.deleted {
        eprintln!("lease {lease}: deleted {}", ids.join(", "));
    }
    for lease in &report.clean {
        eprintln!("lease {lease}: nothing to prune");
    }
    for (lease, problem) in &report.problems {
        eprintln!("lease {lease}: {problem}");
    }
    if report.is_clean() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// An error and its whole `source()` chain on one line, so a typed cause
/// (the io error under a failed read, say) actually reaches the user.
fn error_chain(error: &dyn Error) -> String {
    let mut out = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

/// Lower and validate, or explain why not. `Err` carries the exit code.
#[expect(
    clippy::print_stdout,
    reason = "`--json` makes the diagnostics the command's output; a caller reads them on stdout"
)]
#[expect(
    clippy::print_stderr,
    reason = "the CLI reports diagnostics to the user on stderr, clear of the command's own output"
)]
fn lowered_graph(
    rt: &Runtime,
    target: &FileArgs,
    model: &ModelArgs,
    json: bool,
    validate_only: bool,
) -> Result<Lowered, ExitCode> {
    let mut inputs = match target.compile_inputs() {
        Ok(inputs) => model.bind(inputs),
        Err(message) => {
            eprintln!("error: {message}");
            return Err(ExitCode::from(2));
        }
    };
    // A check given no inputs validates the file as written: an input it
    // reads is a warning, not a reason to stop. A run always needs them.
    if validate_only && target.inputs.is_empty() && target.inputs_file.is_none() {
        inputs = inputs.with_unbound_as_warning();
    }
    match rt.check(
        &target.file,
        target.format.as_deref(),
        target.repo.as_deref(),
        &inputs,
    ) {
        Ok(lowered) => {
            for d in lowered.diagnostics.iter() {
                if json {
                    match serde_json::to_string(d) {
                        Ok(line) => println!("{line}"),
                        Err(e) => eprintln!("error: could not serialize a diagnostic: {e}"),
                    }
                } else {
                    eprintln!("{d}");
                }
            }
            Ok(lowered)
        }
        Err(error) => {
            eprintln!("error: {}", error_chain(&error));
            Err(ExitCode::from(2))
        }
    }
}

#[expect(
    clippy::print_stdout,
    reason = "the printed graph is this command's output; stdout is the CLI's channel for it"
)]
#[expect(
    clippy::print_stderr,
    reason = "the summary is for the user, on stderr so stdout carries only the graph"
)]
#[tracing::instrument(
    name = "cli.check",
    skip_all,
    fields(
        workflow_file = %target.file.display(),
        format = target.format.as_deref(),
        error_count = Empty,
        warning_count = Empty,
    )
)]
fn check(rt: &Runtime, target: &FileArgs, print_graph: bool, json: bool) -> ExitCode {
    let lowered = match lowered_graph(rt, target, &ModelArgs::default(), json, true) {
        Ok(lowered) => lowered,
        Err(code) => return code,
    };
    let errors = lowered.diagnostics.errors().count();
    let warnings = lowered.diagnostics.warnings().count();
    let span = tracing::Span::current();
    span.record("error_count", errors);
    span.record("warning_count", warnings);
    if let Some(graph) = &lowered.graph {
        if print_graph {
            print!("{}", frontend::print_graph(graph));
        }
        if !json {
            eprintln!(
                "ok: {} node(s), {} scope(s){}",
                graph.nodes.len(),
                graph.scopes.len(),
                if warnings > 0 {
                    format!(", {warnings} warning(s)")
                } else {
                    String::new()
                }
            );
        }
        ExitCode::SUCCESS
    } else {
        if !json {
            eprintln!("rejected: {errors} error(s), {warnings} warning(s)");
        }
        ExitCode::FAILURE
    }
}

#[expect(
    clippy::print_stderr,
    reason = "the CLI reports a rejected workflow to the user on stderr"
)]
// The run dir is the user's first line of output; it is not repeated in the
// span.
#[tracing::instrument(
    name = "cli.run",
    skip_all,
    fields(workflow_file = %target.file.display(), status = Empty)
)]
async fn run(
    rt: &Runtime,
    target: &FileArgs,
    run_dir: &Path,
    lowered: Lowered,
    session: Session,
) -> ExitCode {
    let Some(mut graph) = lowered.graph else {
        eprintln!(
            "rejected: {} error(s); nothing to run",
            lowered.diagnostics.errors().count()
        );
        return ExitCode::FAILURE;
    };
    default_params(rt, target, &mut graph);
    let start = Start::Fresh {
        graph,
        children: lowered.children,
    };
    Box::pin(session::drive(rt, run_dir, start, session)).await
}

#[expect(
    clippy::print_stderr,
    reason = "the replay verdict is this command's output, and the user reads it on stderr"
)]
#[tracing::instrument(
    name = "cli.replay",
    skip_all,
    fields(workflow_file = %target.file.display(), event_log = %log_path.display())
)]
fn replay(rt: &Runtime, target: &FileArgs, model: &ModelArgs, log_path: &Path) -> ExitCode {
    let lowered = match lowered_graph(rt, target, model, false, false) {
        Ok(lowered) => lowered,
        Err(code) => return code,
    };
    let Some(mut graph) = lowered.graph else {
        eprintln!("rejected; nothing to replay");
        return ExitCode::FAILURE;
    };
    default_params(rt, target, &mut graph);

    let text = match fs::read_to_string(log_path) {
        Ok(text) => text,
        Err(e) => {
            eprintln!("error: could not read {}: {e}", log_path.display());
            return ExitCode::from(2);
        }
    };
    let log: EventLog = match serde_json::from_str(&text) {
        Ok(log) => log,
        Err(e) => {
            eprintln!(
                "error: {} is not an event log: {}",
                log_path.display(),
                error_chain(&e)
            );
            return ExitCode::from(2);
        }
    };

    match engine::verify_replay(graph, &log) {
        Ok(state) => {
            eprintln!(
                "replay is byte-identical: {} record(s), status {:?}",
                state.log.len(),
                state.folded_status()
            );
            ExitCode::SUCCESS
        }
        Err(mismatch) => {
            eprintln!("error: {mismatch}");
            ExitCode::FAILURE
        }
    }
}

/// What the workflow's own configuration says about launching it, read from
/// the lowered graph by its format. Nothing when the graph did not lower.
fn launch_settings(rt: &Runtime, target: &FileArgs, lowered: &Lowered) -> LaunchSettings {
    let Some(graph) = &lowered.graph else {
        return LaunchSettings::default();
    };
    rt.frontend_for(&target.file, target.format.as_deref())
        .map(|frontend| frontend.launch_settings(graph))
        .unwrap_or_default()
}

/// Fill in what the file's own format says a host owes it, without overwriting
/// a parameter the graph already carries.
#[expect(
    clippy::print_stderr,
    reason = "a mistyped `--format` must reach the user instead of being swallowed"
)]
fn default_params(rt: &Runtime, target: &FileArgs, graph: &mut Graph) {
    let frontend = match rt.frontend_for(&target.file, target.format.as_deref()) {
        Ok(frontend) => frontend,
        // A named format that does not exist is a usage error worth saying,
        // even from this backstop; a path no frontend claims stays quiet —
        // there are simply no defaults to fill in.
        Err(error @ LoadError::UnknownFormat { .. }) => {
            eprintln!("warning: {error}");
            return;
        }
        Err(_) => return,
    };
    let repo = target
        .repo
        .clone()
        .unwrap_or_else(|| frontend.repo_root(&target.file));
    for (key, value) in frontend.default_params(&repo) {
        graph.params.entry(key).or_insert(value);
    }
}
