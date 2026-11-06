//! The process step kind: run a script, capture its output, honour
//! cancellation.
//!
//! Written once against [`ExecEnv`](executor::ExecEnv). It never names an
//! executor, and never assumes the workspace is on this machine.

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::time::Duration;

use executor::{ExitStatus, LogLine, ProcessSpec, Sig, StdinMode};
use ir::placeholder::SECRET_REF_KEY;
use ir::{
    Control, FailureClass, FailureInfo, LogStream, Outcome, Status, StepEvent, StepKindId, Value,
};
use serde::Deserialize;
use serde_json::Map;
use smol_str::SmolStr;
use tokio::sync::mpsc;
use tokio::time;

use crate::ctx::{Step, StepCtx, StepFailure};
use crate::drain::{DRAIN_IDLE_LIMIT, Drain, Forwarder};
use crate::outputs::{BAD_OUTPUT_CLASS, parse};

/// The step kind id the process step registers under.
pub const PROCESS_KIND: StepKindId = StepKindId::new_static("process");

/// The environment variable naming the outputs file.
pub(crate) const OUTPUT_ENV: &str = "CI_OUTPUT";

/// A `$secret` reference turned up somewhere it is not allowed.
pub const SECRET_MISPLACED_CLASS: FailureClass = FailureClass::new_static("secret_misplaced");

/// A named secret is not configured for this run.
pub const SECRET_UNAVAILABLE_CLASS: FailureClass = FailureClass::new_static("secret_unavailable");

/// The workspace could not be prepared for the step.
pub const WORKSPACE_CLASS: FailureClass = FailureClass::new_static("workspace_setup");

/// The process could not be started at all.
pub(crate) const SPAWN_CLASS: FailureClass = FailureClass::new_static("spawn_failed");

/// The line the step logs when its post-exit drain ended on silence, so the
/// persisted log says its tail may be missing.
pub const OUTPUT_INCOMPLETE_NOTE: &str =
    "… [output incomplete: nothing more arrived within 5s of the process ending]";

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Shell {
    #[default]
    Bash,
    Sh,
}

impl Shell {
    /// Fail on error, and fail a pipeline on any stage's error.
    fn invocation(self) -> (&'static str, Vec<&'static str>) {
        match self {
            Self::Bash => ("bash", vec!["-eo", "pipefail", "-c"]),
            Self::Sh => ("sh", vec!["-e", "-c"]),
        }
    }

    /// [`Self::invocation`] as one command line running a script file instead
    /// of `-c` text: the same shell, the same failure semantics. For callers
    /// that stage a script and spawn a wrapper over it.
    pub fn file_invocation(self) -> &'static str {
        match self {
            Self::Bash => "bash -eo pipefail",
            Self::Sh => "sh -e",
        }
    }
}

/// Which non-zero exits are soft failures.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum SoftFail {
    All(bool),
    ExitStatuses(Vec<i32>),
    #[default]
    #[serde(skip)]
    Off,
}

impl SoftFail {
    fn matches(&self, code: i32) -> bool {
        match self {
            Self::Off => false,
            Self::All(all) => *all,
            Self::ExitStatuses(codes) => codes.contains(&code),
        }
    }
}

/// Either a literal env value or a reference to a secret.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ValueOrSecretRef {
    /// `{"$secret": "NAME"}` — resolved by [`resolve_env_refs`] at spawn,
    /// straight into the child's environment, never written to the log. The
    /// reference stays a reference until then: nothing rewrites it to its
    /// plaintext in place.
    Secret {
        #[serde(rename = "$secret")]
        name: SmolStr,
    },
    Literal(Value),
}

/// The process step's config, as authored.
///
/// It stays as authored: nothing rewrites its texts with resolved secret
/// plaintext, which is why its `Debug` may stay derived. `env` may hold
/// `{"$secret": …}` references, resolved at spawn by [`resolve_env_refs`]. A
/// frontend whose configs carry placeholder sentinels of their own (the GitHub
/// session) resolves them on its own redacting carrier and hands the resolved
/// parts to [`run_resolved`] — a `ProcessConfig` never holds that plaintext.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessConfig {
    /// The script text.
    pub run:                String,
    #[serde(default)]
    pub shell:              Shell,
    #[serde(default)]
    pub env:                BTreeMap<SmolStr, ValueOrSecretRef>,
    /// Relative to the workspace root.
    #[serde(default)]
    pub working_dir:        Option<PathBuf>,
    #[serde(default)]
    pub soft_fail:          SoftFail,
    /// Extra environment variables that name the same outputs file as
    /// `CI_OUTPUT`, so a frontend whose scripts already write to a
    /// differently named file needs no shim. (The GitHub Actions frontend
    /// sets `["GITHUB_OUTPUT"]`.)
    #[serde(default)]
    pub output_env_aliases: Vec<SmolStr>,
}

/// Runs a script as a process, as its own process group.
pub struct ProcessStep;

#[async_trait::async_trait]
impl Step for ProcessStep {
    const NAME: &'static str = "process";
    type Config = ProcessConfig;

    /// A `$secret` outside an env-shaped position outranks deserialization: it
    /// is a misplaced secret, not a malformed config, and would otherwise
    /// reach the process as literal JSON.
    fn check_raw(&self, config: &Value) -> Result<(), StepFailure> {
        check_misplaced_secret(config, &["env"])
    }

    async fn run(&self, config: ProcessConfig, ctx: StepCtx) -> Outcome {
        match execute(config, ctx).await {
            Ok(outcome) => outcome,
            Err(failure) => failure.into(),
        }
    }
}

async fn execute(config: ProcessConfig, ctx: StepCtx) -> Result<Outcome, StepFailure> {
    let env = resolve_env_refs(&config.env, ctx.secrets.as_ref())?;
    run_resolved(
        &config.run,
        config.shell,
        env,
        config.working_dir,
        &config.soft_fail,
        &config.output_env_aliases,
        ctx,
    )
    .await
}

/// Resolve an env map's `{"$secret": …}` references into the values a child
/// process receives. Secrets are resolved here, at spawn time, and nowhere
/// else; the resolved map goes straight into a [`ProcessSpec`] and is never
/// written down.
pub fn resolve_env_refs(
    env: &BTreeMap<SmolStr, ValueOrSecretRef>,
    secrets: &dyn executor::SecretProvider,
) -> Result<BTreeMap<SmolStr, SmolStr>, StepFailure> {
    let mut resolved = BTreeMap::new();
    for (key, value) in env {
        let value = match value {
            ValueOrSecretRef::Secret { name } => secrets
                .resolve(name)
                .map_err(|e| fail(SECRET_UNAVAILABLE_CLASS, e.to_string()))?
                .expose(),
            ValueOrSecretRef::Literal(literal) => SmolStr::new(stringify(literal)),
        };
        resolved.insert(key.clone(), value);
    }
    Ok(resolved)
}

/// The process step's machinery once every reference is resolved: the outputs
/// file, the spawn, output capture, the cancel ladder, and the fold into an
/// outcome. Shared with step kinds that resolve their configs themselves —
/// the GitHub session hands its resolved carrier here — so the contract lives
/// once.
pub async fn run_resolved(
    run: &str,
    shell: Shell,
    mut env: BTreeMap<SmolStr, SmolStr>,
    working_dir: Option<PathBuf>,
    soft_fail: &SoftFail,
    output_env_aliases: &[SmolStr],
    mut ctx: StepCtx,
) -> Result<Outcome, StepFailure> {
    // The outputs file lives in the workspace, reached through the environment: the
    // step kind never assumes the workspace is on this machine.
    let output_rel = format!(".ci/out/{}.env", ctx.firing.raw());
    let output_rel_path = PathBuf::from(&output_rel);
    ctx.env
        .write_file(&output_rel_path, b"")
        .await
        .map_err(|e| {
            fail(
                WORKSPACE_CLASS,
                format!("could not create the outputs file: {e}"),
            )
        })?;
    let output_path = SmolStr::new(format!("{}/{output_rel}", ctx.env.workspace_path()));
    env.insert(SmolStr::new(OUTPUT_ENV), output_path.clone());
    for alias in output_env_aliases {
        env.insert(alias.clone(), output_path.clone());
    }

    let (program, mut args) = shell.invocation();
    let mut argv: Vec<SmolStr> = args.drain(..).map(SmolStr::new).collect();
    argv.push(SmolStr::new(run));
    let spec = ProcessSpec {
        program: SmolStr::new(program),
        args: argv,
        env,
        cwd: working_dir,
        stdin: StdinMode::Null,
        output: executor::OutputMode::Lines,
        timeout: None,
    };

    let mut handle = ctx
        .env
        .spawn(spec)
        .await
        .map_err(|e| fail(SPAWN_CLASS, e.to_string()))?;

    // Log capture runs on its own task and keeps going through cancellation, so a
    // cancelled step's final output is not lost.
    let forwarder = handle.lines().map(|lines| {
        let logs = ctx.logs.clone();
        Forwarder::spawn(lines, move |LogLine { stream, line, .. }| {
            let logs = logs.clone();
            async move {
                if logs.send(StepEvent::Log { stream, line }).await.is_err() {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            }
        })
    });

    let grace = ctx.env.grace();
    let ending = ladder(&mut *handle, &mut ctx.control, grace).await;

    // The process is gone; drain what it left, for as long as lines keep
    // arriving. Silence for the idle limit ends the drain and is recorded in
    // the log, since the tail may then be missing.
    if let Some(forwarder) = forwarder
        && forwarder.finish(DRAIN_IDLE_LIMIT).await == Drain::Silent
    {
        tracing::warn!(
            firing = %ctx.firing.raw(),
            "the step's output drain ended on silence; its log may be incomplete"
        );
        let _ = ctx.logs.try_send(StepEvent::Log {
            stream: LogStream::Stderr,
            line:   OUTPUT_INCOMPLETE_NOTE.to_owned(),
        });
    }

    let output = match read_outputs(&*ctx.env, &output_rel_path).await {
        Ok(output) => output,
        Err(message) => return Err(fail(BAD_OUTPUT_CLASS, message)),
    };

    Ok(ending_outcome(&ending, soft_fail, output))
}

/// Fold an [`Ending`] into the step's outcome: the exit status into the output
/// as `exit_status`, natural endings through `soft_fail`, a ladder-stopped
/// process to `Cancelled` with its `cancel_escalation`.
///
/// Public for the same reason [`ladder`] is: every step kind that waits on a
/// process reports what happened through this one contract.
pub fn ending_outcome(
    ending: &Ending,
    soft_fail: &SoftFail,
    mut output: Map<String, Value>,
) -> Outcome {
    match ending {
        Ending::Natural(status) => {
            output.insert("exit_status".into(), exit_value(status));
            natural_outcome(status, soft_fail, Value::Object(output))
        }
        Ending::Signalled {
            escalation,
            status: exit,
        } => {
            if let Some(exit) = exit {
                output.insert("exit_status".into(), exit_value(exit));
            }
            // The escalation goes in the output: `Status::Cancelled` carries no
            // FailureInfo, and inventing one would mean widening a closed enum.
            output.insert(
                "cancel_escalation".into(),
                Value::String(escalation.to_string()),
            );
            Outcome::new(Status::Cancelled, Value::Object(output))
        }
    }
}

/// How a waited-on process ended: naturally, or because the ladder stopped it.
pub enum Ending {
    Natural(ExitStatus),
    Signalled {
        escalation: &'static str,
        status:     Option<ExitStatus>,
    },
}

/// SIGTERM to the group, grace, then SIGKILL to the group. `Control::Kill`
/// skips the ladder: straight to SIGKILL, no grace — whether it arrives first
/// or while the polite ladder is already waiting.
///
/// Idempotent by construction: once the ladder has started, further `Cancel`s
/// are drained and ignored rather than restarting it. A `Deliver` is not a
/// stop: a process has nothing to hand a value to, so it is dropped and the
/// wait goes on.
///
/// Public: every step kind that waits on a
/// [`ProcessHandle`](executor::ProcessHandle) honours cancellation through this
/// one ladder.
pub async fn ladder(
    handle: &mut dyn executor::ProcessHandle,
    control: &mut mpsc::Receiver<Control>,
    grace: Duration,
) -> Ending {
    // First terminal wins. If the process exits before any signal lands, the
    // outcome is the natural one and the cancel is a no-op.
    let stop = loop {
        tokio::select! {
            result = handle.wait() => {
                return match result {
                    Ok(status) => Ending::Natural(status),
                    Err(err) => {
                        // The synthesized -1 is indistinguishable in the log
                        // from a step that really exited -1, so the reason the
                        // wait failed has nowhere else to go.
                        tracing::warn!(error = ?err, "waiting on the step process failed");
                        Ending::Natural(ExitStatus::code(-1))
                    }
                };
            }
            ctl = control.recv() => match ctl {
                Some(Control::Deliver(_)) => {}
                stop => break stop,
            },
        }
    };

    if !matches!(stop, Some(Control::Kill)) {
        let _ = handle.signal(Sig::Term).await;
        let deadline = time::sleep(grace);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                result = handle.wait() => {
                    return Ending::Signalled {
                        escalation: "sigterm",
                        status: result.ok(),
                    };
                }
                ctl = control.recv() => {
                    // A Kill joins mid-grace and escalates now; anything else
                    // joins the ladder already in flight.
                    if matches!(ctl, Some(Control::Kill)) {
                        break;
                    }
                }
                () = &mut deadline => break,
            }
        }
    }

    let _ = handle.signal(Sig::Kill).await;
    let status = handle.wait().await.ok();
    Ending::Signalled {
        escalation: "sigkill",
        status,
    }
}

/// The outcome of a process that ended on its own: exit 0 is success, a foreign
/// signal is a `signal:N` failure, and a non-zero exit is a failure unless
/// `soft_fail` claims the code.
pub(crate) fn natural_outcome(status: &ExitStatus, soft_fail: &SoftFail, output: Value) -> Outcome {
    if status.is_success() {
        return Outcome::new(Status::Success, output);
    }
    if let Some(signal) = status.signal {
        // Killed by something that was not us.
        return Outcome::new(
            Status::Failure(
                FailureInfo::new(format!("step was killed by signal {signal}"))
                    .with_class(FailureClass::signal(signal)),
            ),
            output,
        );
    }
    let code = status.code.unwrap_or(-1);
    let failure = FailureInfo::exit_status(code);
    if soft_fail.matches(code) {
        // The real failure rides along in `underlying`: the log never records a clean
        // success for something that failed.
        Outcome::new(Status::partial(failure), output)
    } else {
        Outcome::new(Status::Failure(failure), output)
    }
}

async fn read_outputs(
    env: &dyn executor::ExecEnv,
    path: &Path,
) -> Result<Map<String, Value>, String> {
    match env.read_file(path).await {
        Ok(Some(bytes)) => parse(&String::from_utf8_lossy(&bytes)).map_err(|e| e.to_string()),
        // No file is not an error: a step need not write outputs.
        Ok(None) => Ok(Map::new()),
        Err(e) => Err(format!("could not read the outputs file: {e}")),
    }
}

fn exit_value(status: &ExitStatus) -> Value {
    match (status.code, status.signal) {
        (Some(code), _) => Value::from(code),
        (None, Some(signal)) => Value::String(format!("signal:{signal}")),
        _ => Value::Null,
    }
}

/// A config value as the string a child process sees. Null renders empty —
/// an environment variable has no null, and GitHub's expression coercion
/// (which the GHA frontend leans on) spells the same rule — never the JSON
/// spelling `null`, which actions then read as a real path or version.
pub fn stringify(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn fail(class: FailureClass, message: impl Into<String>) -> StepFailure {
    StepFailure {
        class,
        message: message.into(),
    }
}

/// `check_raw` for a config whose secrets belong only in the given top-level
/// maps: find a `{"$secret": …}` reference anywhere else and fail with where it
/// is. For any step kind whose config is secret-shaped — the process step
/// allows `env`; the GitHub step kinds add their own maps.
pub fn check_misplaced_secret(config: &Value, allowed_maps: &[&str]) -> Result<(), StepFailure> {
    let Some(path) = misplaced_secret(config, allowed_maps) else {
        return Ok(());
    };
    let allowed = allowed_maps
        .iter()
        .map(|m| format!("`{m}`"))
        .collect::<Vec<_>>()
        .join(" or ");
    Err(fail(
        SECRET_MISPLACED_CLASS,
        format!("`{SECRET_REF_KEY}` is only valid in {allowed}; found one at `{path}`"),
    ))
}

/// Find a `$secret` reference outside the allowed maps, and say where it is. A
/// direct entry of an allowed map may be a reference; anything deeper may not.
fn misplaced_secret(config: &Value, allowed_maps: &[&str]) -> Option<String> {
    fn walk(value: &Value, path: &str) -> Option<String> {
        match value {
            Value::Object(map) => {
                if map.contains_key(SECRET_REF_KEY) {
                    return Some(path.to_string());
                }
                map.iter()
                    .find_map(|(key, child)| walk(child, &format!("{path}.{key}")))
            }
            Value::Array(items) => items
                .iter()
                .enumerate()
                .find_map(|(i, child)| walk(child, &format!("{path}[{i}]"))),
            _ => None,
        }
    }
    let top = config.as_object()?;
    if top.contains_key(SECRET_REF_KEY) {
        return Some("<root>".into());
    }
    top.iter().find_map(|(key, value)| {
        if allowed_maps.contains(&key.as_str()) {
            let Some(map) = value.as_object() else {
                return walk(value, key);
            };
            map.iter().find_map(|(name, child)| {
                let is_ref = child
                    .as_object()
                    .is_some_and(|m| m.contains_key(SECRET_REF_KEY));
                if is_ref {
                    None
                } else {
                    walk(child, &format!("{key}.{name}"))
                }
            })
        } else {
            walk(value, key)
        }
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn secrets_are_fine_in_allowed_maps_and_nowhere_else() {
        let ok = json!({ "env": { "TOKEN": { "$secret": "T" } }, "inputs": { "token": { "$secret": "T" } } });
        assert_eq!(misplaced_secret(&ok, &["env", "inputs"]), None);
        assert_eq!(misplaced_secret(&ok, &["env"]), Some("inputs.token".into()));
        let bad = json!({ "run": { "$secret": "T" } });
        assert_eq!(misplaced_secret(&bad, &["env"]), Some("run".into()));
        let nested = json!({ "env": { "X": { "nested": { "$secret": "T" } } } });
        assert_eq!(
            misplaced_secret(&nested, &["env"]),
            Some("env.X.nested".into())
        );
        let root = json!({ "$secret": "T" });
        assert_eq!(misplaced_secret(&root, &["env"]), Some("<root>".into()));
    }
}

#[cfg(test)]
mod stringify_tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn null_renders_empty_and_scalars_render_plain() {
        assert_eq!(stringify(&Value::Null), "");
        assert_eq!(stringify(&json!("text")), "text");
        assert_eq!(stringify(&json!(true)), "true");
        assert_eq!(stringify(&json!(3)), "3");
    }
}
