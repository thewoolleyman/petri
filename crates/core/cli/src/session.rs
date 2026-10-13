//! One terminal session over a run, shared by `petri run` and `petri
//! resume`: the interviewer, the control service and its file, the stall
//! watchdog, Ctrl-C, the event log and receipt written beside the run, the
//! retained-workspace report, and the exit code.
//!
//! The two commands differ only in how the run starts ([`Start`]): a fresh
//! run hands the host its lowered graph, a resume hands it the run dir. Every
//! option that shapes the session ([`SessionArgs`]) applies to both.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Args;
use execution::controls::ControlService;
use execution::host::{HostError, HostRun};
use execution::watchdog::StallWatchdog;
use execution::{
    Access, CoordinatorHandle, InterviewDispatcher, InterviewReceipt, Interviewer, LeaseState,
    RECEIPT_FILE, RECEIPT_VERSION, ResourceStore, host, open_run_dir,
};
use runtime::driver::ExecutionReport;
use runtime::executor::Retention;
use runtime::frontend::{LaunchSettings, WorkspaceRetention};
use runtime::ir::{Graph, RunStatus};
use runtime::{DaytonaResources, RunOptions, Runtime, SandboxBackend};
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::field::display;

use crate::answer::{AutoApproveInterviewer, ScriptedInterviewer, TerminalInterviewer};
use crate::control::{self, TailFrom};
use crate::{ProviderArgs, Retain, RunnerArgs, error_chain};

/// The options a run and a resume share: how questions are answered, where
/// controls come from, what happens to workspaces, and how much is echoed.
#[derive(Args)]
pub(crate) struct SessionArgs {
    /// Do not echo step output.
    #[arg(long)]
    pub(crate) quiet:            bool,
    /// Answer a step's question — a human gate — from the terminal:
    /// the question is printed and one line is read from stdin.
    #[arg(long, conflicts_with_all = ["auto_approve", "interview_script"])]
    pub(crate) interactive:      bool,
    /// Answer every step's question with its default choice.
    #[arg(long, conflicts_with = "interview_script")]
    pub(crate) auto_approve:     bool,
    /// Answer every step's question from a JSON interview script, and
    /// fail the run when a question matches no entry or a required entry
    /// goes unused. See `cli::answer` for the format.
    #[arg(long, value_name = "FILE")]
    pub(crate) interview_script: Option<PathBuf>,
    /// When to keep the run's workspaces: always, on-failure, or never.
    /// Defaults to what the workflow's format declares (Fabro: always;
    /// other formats: on-failure).
    #[arg(long, value_name = "POLICY")]
    pub(crate) retain:           Option<Retain>,
    /// Read run controls from this file while the run is live: one
    /// `pause`, `unpause`, `steer <node> <text>` or `cancel` per appended
    /// line. See `cli::control` for the format.
    #[arg(long, value_name = "FILE")]
    pub(crate) control:          Option<PathBuf>,
}

impl SessionArgs {
    /// The run options these arguments and the workflow's own launch
    /// settings describe. `default_retention` is the format's, when the
    /// format is known.
    pub(crate) fn run_options(
        &self,
        run_dir: &Path,
        default_retention: Option<WorkspaceRetention>,
        launch: &LaunchSettings,
        provider: &ProviderArgs,
        runner: RunnerArgs,
    ) -> Result<RunOptions, String> {
        let mut options = RunOptions::new(run_dir);
        options.echo = !self.quiet;
        options.retention = self
            .retain
            .or_else(|| default_retention.map(Retain::from))
            .map_or(options.retention, Retention::from);
        let backend_given = provider.backend_given();
        options.sandbox = provider.options();
        if !backend_given && let Some(backend) = launch.sandbox_backend.as_deref() {
            // The frontend validated the name; a spelling the host cannot
            // place is an error here, never a silent fall back to the host.
            options.sandbox.backend = backend
                .parse::<SandboxBackend>()
                .map_err(|error| format!("the workflow selects a sandbox backend: {error}"))?;
        }
        options.sandbox.runner_images = runner.images.into_iter().collect();
        options.sandbox.daytona_kind = runner.daytona_kind.unwrap_or(options.sandbox.daytona_kind);
        let defaults = options.sandbox.daytona_resources;
        options.sandbox.daytona_resources = DaytonaResources {
            cpu_cores: runner.daytona_cpus.unwrap_or(defaults.cpu_cores),
            memory_mb: runner.daytona_memory_mb.unwrap_or(defaults.memory_mb),
            disk_mb:   runner.daytona_disk_mb.or(defaults.disk_mb),
        };
        Ok(options)
    }

    /// How the session answers questions: the explicit option, else the
    /// workflow's own `auto_approve` launch setting, else no interviewer.
    pub(crate) fn answers(&self, launch: &LaunchSettings) -> Option<Answers> {
        match (
            self.interactive,
            self.auto_approve || launch.auto_approve,
            &self.interview_script,
        ) {
            (_, _, Some(script)) => Some(Answers::Scripted(script.clone())),
            (true, _, None) => Some(Answers::Interactive),
            (_, true, None) => Some(Answers::AutoApprove),
            _ => None,
        }
    }
}

/// How the session answers questions.
pub(crate) enum Answers {
    Interactive,
    AutoApprove,
    Scripted(PathBuf),
}

/// How the run starts.
pub(crate) enum Start {
    /// A fresh run of a lowered graph and the child graphs it may invoke.
    Fresh {
        graph:    Graph,
        children: Vec<Graph>,
    },
    /// The run in the run dir, continued from its durable record. The stall
    /// budget is read from the stored root graph by the caller.
    Resume { stall_timeout: Option<Duration> },
}

/// What one session is made of, beyond how it starts.
pub(crate) struct Session {
    pub(crate) answers:      Option<Answers>,
    /// The one control service; its pause hook is already installed on the
    /// runtime.
    pub(crate) controls:     ControlService,
    /// The control file to tail, when there is one, and from where.
    pub(crate) control_file: Option<(PathBuf, TailFrom)>,
}

/// Run the session to its exit code: 0 for a successful run, 1 for a failed
/// or cancelled one, 2 when the interviewer could not be set up, 3 when the
/// host failed, 4 when the interview receipt has errors.
#[expect(
    clippy::print_stderr,
    reason = "the CLI reports each step and the final status to the user on stderr"
)]
pub(crate) async fn drive(
    rt: &Runtime,
    run_dir: &Path,
    start: Start,
    session: Session,
) -> ExitCode {
    let interviewer: Option<Arc<dyn Interviewer>> = match session.answers {
        None => None,
        Some(Answers::AutoApprove) => Some(Arc::new(AutoApproveInterviewer)),
        Some(Answers::Interactive) => match TerminalInterviewer::start() {
            Ok(terminal) => Some(Arc::new(terminal)),
            Err(error) => {
                eprintln!("error: could not read the terminal: {error}");
                return ExitCode::from(2);
            }
        },
        Some(Answers::Scripted(path)) => match ScriptedInterviewer::load(&path) {
            Ok(script) => Some(Arc::new(script)),
            Err(error) => {
                eprintln!("error: {}", error_chain(&error));
                return ExitCode::from(2);
            }
        },
    };
    let controls = session.controls;
    eprintln!("run dir: {}", run_dir.display());
    let mut ctrl_c = None;
    let mut control_task = None;
    let stop_controls = CancellationToken::new();
    let mut observers: Vec<Arc<dyn execution::ExecutionObserver>> =
        vec![Arc::new(controls.clone())];
    // The stall watchdog, when the graph declares a budget.
    let stall_timeout = match &start {
        Start::Fresh { graph, .. } => graph.policy.stall_timeout,
        Start::Resume { stall_timeout } => *stall_timeout,
    };
    let watchdog = stall_timeout.map(StallWatchdog::new);
    let mut watchdog_task = None;
    if let Some(watchdog) = &watchdog {
        observers.push(Arc::new(watchdog.clone()));
    }
    // A resumed run continues the receipt an earlier process wrote, and the
    // receipt is written as each question's outcome is recorded, so a crash
    // loses only the questions still waiting, which the resumed run asks
    // again.
    let earlier = match &start {
        Start::Resume { .. } => read_receipt(run_dir),
        Start::Fresh { .. } => None,
    };
    let dispatcher = interviewer.map(|interviewer| {
        let dispatcher = InterviewDispatcher::continuing(interviewer, earlier);
        let dir = run_dir.to_path_buf();
        dispatcher.publish_to(Arc::new(move |receipt| {
            save_receipt(&dir, receipt);
        }));
        dispatcher
    });
    if let Some(dispatcher) = &dispatcher {
        observers.push(Arc::new(dispatcher.clone()));
    }
    let with_handle = |handle: CoordinatorHandle, secrets| {
        if let Some(dispatcher) = &dispatcher {
            dispatcher.wire(handle.clone(), secrets);
        }
        controls.wire(handle.clone());
        if let Some(watchdog) = &watchdog {
            watchdog_task = Some(watchdog.start(handle.clone()));
        }
        if let Some((path, from)) = session.control_file {
            control_task = Some(tokio::spawn(control::drive(
                path,
                controls.clone(),
                stop_controls.clone(),
                from,
            )));
        }
        ctrl_c = Some(tokio::spawn(cancel_on_ctrl_c(handle)));
    };
    let outcome: Result<ExecutionReport, HostError> = match start {
        Start::Fresh { graph, children } => {
            let mut host_run = HostRun::new(graph).with_children(children);
            for observer in observers {
                host_run = host_run.observe(observer);
            }
            host::run_configured(rt, host_run, with_handle).await
        }
        Start::Resume { .. } => {
            host::resume_configured(rt, Vec::new(), observers, with_handle).await
        }
    };
    if let Some(task) = ctrl_c {
        task.abort();
    }
    stop_controls.cancel();
    if let Some(task) = control_task {
        let _ = task.await;
    }
    if let Some(task) = watchdog_task {
        task.stop().await;
    }
    if let Some(stall) = watchdog.as_ref().and_then(StallWatchdog::tripped) {
        eprintln!(
            "stall watchdog: no execution activity for {} s (stall_timeout {} s); the run was \
             cancelled",
            stall.idle_ms / 1000,
            stall.stall_timeout_ms / 1000
        );
    }
    // The receipt is written whatever the run did: a failed run's interviews
    // are evidence too.
    let receipt = match &dispatcher {
        Some(dispatcher) => Some(dispatcher.shutdown().await),
        None => None,
    };
    if let Some(receipt) = &receipt {
        write_receipt(run_dir, receipt);
    }
    let report = match outcome {
        Ok(report) => report,
        Err(error) => {
            eprintln!("error: {}", error_chain(&error));
            return ExitCode::from(3);
        }
    };

    let log_path = run_dir.join("events.json");
    match serde_json::to_vec_pretty(&report.state.log) {
        Ok(bytes) => {
            if let Err(e) = fs::write(&log_path, bytes) {
                eprintln!("warning: could not write {}: {e}", log_path.display());
            } else {
                eprintln!("event log: {}", log_path.display());
            }
        }
        Err(e) => eprintln!("warning: could not encode the event log: {e}"),
    }

    for record in report.state.history() {
        eprintln!("  {} {}", record.outcome.status.tag(), record.name);
    }
    report_workspaces(run_dir, rt.run_options().retention).await;
    let status = report.status;
    tracing::Span::current().record("status", display(status));
    eprintln!("run: {status}");
    if let Some(receipt) = &receipt
        && !receipt.is_clean()
    {
        // The engine's status stands as persisted; the interview is what
        // failed, and the exit code says so.
        eprintln!(
            "interview verification failed: {} problem(s); see {}",
            receipt.errors.len(),
            run_dir.join(RECEIPT_FILE).display()
        );
        for error in &receipt.errors {
            eprintln!("  {error}");
        }
        return ExitCode::from(4);
    }
    if status == RunStatus::Success {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Persist the interview receipt beside the run, and say where it is.
#[expect(
    clippy::print_stderr,
    reason = "the CLI reports where the receipt went on stderr"
)]
fn write_receipt(run_dir: &Path, receipt: &InterviewReceipt) {
    if save_receipt(run_dir, receipt)
        && (!receipt.questions.is_empty() || !receipt.errors.is_empty())
    {
        eprintln!("interviews: {}", run_dir.join(RECEIPT_FILE).display());
    }
}

/// Write the receipt whole, through a temporary file renamed over the old
/// one, so a crash mid-write leaves the last receipt intact. Whether it was
/// written.
#[expect(
    clippy::print_stderr,
    reason = "the CLI reports a failure to write the receipt on stderr"
)]
fn save_receipt(run_dir: &Path, receipt: &InterviewReceipt) -> bool {
    let path = run_dir.join(RECEIPT_FILE);
    let staged = path.with_extension("json.tmp");
    let written = serde_json::to_vec_pretty(receipt)
        .map_err(|error| format!("could not encode the interview receipt: {error}"))
        .and_then(|bytes| {
            fs::write(&staged, bytes)
                .and_then(|()| fs::rename(&staged, &path))
                .map_err(|error| format!("could not write {}: {error}", path.display()))
        });
    if let Err(error) = &written {
        eprintln!("warning: {error}");
    }
    written.is_ok()
}

/// The receipt an earlier process of this run wrote, when it is one this
/// build continues.
fn read_receipt(run_dir: &Path) -> Option<InterviewReceipt> {
    let bytes = fs::read(run_dir.join(RECEIPT_FILE)).ok()?;
    let receipt: InterviewReceipt = serde_json::from_slice(&bytes).ok()?;
    (receipt.version == RECEIPT_VERSION).then_some(receipt)
}

/// Say where the retained workspaces are, or how to reach them. A host
/// workspace is a directory under the run dir; a container's lives on its
/// provider and is reached through the sandbox, or deleted with
/// `petri sandbox prune`.
#[expect(
    clippy::print_stderr,
    reason = "the retained workspace path is the run's result for the user, on stderr"
)]
async fn report_workspaces(run_dir: &Path, retention: Retention) {
    let logs = match open_run_dir(run_dir, Access::Read).await {
        Ok(logs) => logs,
        Err(error) => {
            tracing::debug!(error = %error, "no run to report workspaces for");
            return;
        }
    };
    let store = match ResourceStore::load(&logs).await {
        Ok(store) => store,
        Err(error) => {
            tracing::debug!(error = %error, "no sandbox resource records to report");
            return;
        }
    };
    for record in store.records() {
        match record.state {
            LeaseState::Deleted => {}
            LeaseState::Allocating | LeaseState::Live | LeaseState::Stopped => {
                if record.provider == execution::HOST_PROVIDER {
                    let path = run_dir
                        .join("scopes")
                        .join(record.workspace.as_str())
                        .join("work");
                    eprintln!("workspace: {}", path.display());
                } else {
                    eprintln!(
                        "workspace: {} on {} sandbox {} (delete with `petri sandbox prune \
                         --run-dir {}`)",
                        record.workspace,
                        record.provider,
                        record.resource_id.as_deref().unwrap_or("?"),
                        run_dir.display()
                    );
                }
            }
        }
    }
    if retention == Retention::Never {
        tracing::debug!("workspaces deleted by retention policy");
    }
}

/// Map Ctrl-C onto the run's two-tier stop: the first cancels the run —
/// cleanup steps and release still happen — and any further Ctrl-C reaches
/// the drivers' kill tier. The task holds no cleanup-sensitive state; the run
/// aborts it once the report is in.
#[expect(
    clippy::print_stderr,
    reason = "the CLI tells the user what each Ctrl-C did on stderr"
)]
async fn cancel_on_ctrl_c(handle: CoordinatorHandle) {
    let mut cancelled = false;
    loop {
        if signal::ctrl_c().await.is_err() {
            return;
        }
        if cancelled {
            eprintln!("killing the run");
        } else {
            eprintln!("cancelling the run; Ctrl-C again to kill");
            cancelled = true;
        }
        handle.cancel_root_for(execution::CancelReason::Interrupt);
    }
}
