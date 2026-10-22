//! `petri resume --run-dir <dir>`: continue a run from its run directory.
//!
//! The run directory is the record (`run.json`, `coordinator.jsonl`, the
//! registered graphs, one engine log per execution). Nothing else is needed:
//! not the workflow file, not its inputs. The format is found from the
//! stored root graph (`Frontend::claims_graph`), and its launch settings and
//! default retention apply as they did to `petri run`. The interviewer,
//! control file, retention, echo, provider and backend options are the
//! session's own and are given again; a gate that was waiting when the
//! process died asks again, and the new interviewer answers it.
//!
//! What a resume does not restore: an agent node's retained thread. As in
//! Fabro, a node that was mid-turn starts a fresh session from the
//! `summary:high` preamble.
//!
//! Before anything starts, the command reads the coordinator log through a
//! read handle (no lease) and refuses, with exit code 2 and no work done: a
//! run that already recorded its finish, a run another process holds (the
//! lease), a paused run given no `--control` file (nothing could unpause
//! it), and a run directory that is missing or does not decode.

use std::path::PathBuf;
use std::process::ExitCode;

use execution::controls::ControlService;
use execution::{Access, OwnerId, StoreError, host, open_run_dir};
use runtime::Runtime;
use runtime::frontend::{Frontend, LaunchSettings};
use runtime::store::StoreError as BackendError;

use crate::control::TailFrom;
use crate::session::{self, Session, SessionArgs, Start};
use crate::{ProviderArgs, RunnerArgs, RuntimeMode, error_chain};

/// `petri resume`. `make` builds the runtime the way `run` would, once the
/// stored graph has said whether the run was a dry run.
#[expect(
    clippy::print_stderr,
    reason = "the CLI reports why a run cannot be resumed to the user on stderr"
)]
#[tracing::instrument(name = "cli.resume", skip_all, fields(run_dir = %run_dir.display()))]
pub(crate) async fn resume(
    make: &impl Fn(RuntimeMode) -> Runtime,
    run_dir: PathBuf,
    dry_run: bool,
    args: SessionArgs,
    provider: ProviderArgs,
    runner: RunnerArgs,
) -> ExitCode {
    let logs = match open_run_dir(&run_dir, Access::Read).await {
        Ok(logs) => logs,
        Err(error) => {
            eprintln!("error: {}", error_chain(&error));
            return ExitCode::from(2);
        }
    };
    let state = match host::stored_state(&*logs).await {
        Ok(state) => state,
        Err(error) => {
            eprintln!("error: {}", error_chain(&error));
            return ExitCode::from(2);
        }
    };
    if let Some(status) = state.run_status {
        eprintln!(
            "error: the run in {} already finished ({status}); nothing to resume",
            run_dir.display()
        );
        return ExitCode::from(2);
    }
    if state.paused && args.control.is_none() {
        eprintln!(
            "error: the run is paused; pass --control <FILE> so an unpause can reach it, or \
             cancel it there"
        );
        return ExitCode::from(2);
    }
    // The lease, taken and released: a run a live process holds is refused
    // here, with the reason, before any runtime is built. The window between
    // this and the coordinator's own lease is the same refusal at exit 3.
    match open_run_dir(&run_dir, Access::Write {
        owner: OwnerId::mint(),
    })
    .await
    {
        Ok(lease) => drop(lease),
        Err(StoreError::Store(error @ BackendError::Leased { .. })) => {
            eprintln!("error: {error}");
            return ExitCode::from(2);
        }
        Err(error) => {
            eprintln!("error: {}", error_chain(&error));
            return ExitCode::from(2);
        }
    }
    let graph = match host::stored_root_graph(&*logs).await {
        Ok(Some(graph)) => graph,
        Ok(None) => {
            eprintln!("error: the run declared no root invocation; nothing to resume");
            return ExitCode::from(2);
        }
        Err(error) => {
            eprintln!("error: {}", error_chain(&error));
            return ExitCode::from(2);
        }
    };
    // The format's launch settings and defaults, from the stored graph. A
    // graph no frontend claims gets the defaults.
    let probe = make(RuntimeMode::Real);
    let frontend = probe.frontend_for_graph(&graph);
    let launch = frontend.map_or_else(LaunchSettings::default, |frontend| {
        frontend.launch_settings(&graph)
    });
    let default_retention = frontend.map(Frontend::default_retention);
    let runtime_mode = if dry_run || launch.dry_run {
        RuntimeMode::DryRun
    } else {
        RuntimeMode::Real
    };
    let rt = make(runtime_mode);
    // One control service, as in `run`; it starts paused when the log says
    // so, before the first attempt is admitted.
    let controls = ControlService::new();
    let hooks = controls.hooks(rt.installed_hooks());
    let rt = rt.hooks(hooks).capability(controls.turns());
    let options = match args.run_options(&run_dir, default_retention, &launch, &provider, runner) {
        Ok(options) => options,
        Err(error) => return crate::provider_error(&error),
    };
    let rt = match provider.configure(rt.options(options)).await {
        Ok(rt) => rt,
        Err(error) => return crate::provider_error(&error),
    };
    let answers = args.answers(&launch);
    if state.paused {
        eprintln!(
            "resumed paused: attempts are held until an unpause arrives through the control file"
        );
    }
    let session = Session {
        answers,
        controls,
        control_file: args.control.map(|path| (path, TailFrom::End)),
    };
    let start = Start::Resume {
        stall_timeout: graph.policy.stall_timeout,
    };
    Box::pin(session::drive(&rt, &run_dir, start, session)).await
}
