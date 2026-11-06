//! `attractor/command`: Fabro's command node. A bash (or python) script in the
//! scope environment, its combined output captured, an optional stdin from
//! the run context, and — with `output_schema="routing"` — the last JSON
//! object of the output read as a routing directive.

use std::collections::{BTreeMap, VecDeque};
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use executor::{ProcessSpec, StdinMode};
use frontend_attractor::Policy;
use frontend_attractor::kinds::{COMMAND_KIND, StageOutcome};
use ir::{FailureClass, LogStream, Outcome, StepEvent, StepKindId, Value};
use serde::Deserialize;
use serde_json::json;
use smol_str::SmolStr;
use steps::{
    DRAIN_IDLE_LIMIT, Drain, Ending, Forwarder, Step, StepCtx, StepFailure, ValueOrSecretRef,
    ladder, resolve_env_refs,
};
use tokio::io::AsyncWriteExt as _;

use crate::blobs::{self, OutputStore};
use crate::directive;
use crate::outcome::{ExplicitRoutes, Stage};

/// The step kind id.
pub const KIND: StepKindId = COMMAND_KIND;

/// The process could not be started.
pub const SPAWN_CLASS: FailureClass = FailureClass::new_static("spawn_failed");

/// How much captured output a stage keeps in memory. Output above
/// [`blobs::OFFLOAD_THRESHOLD`] leaves the context for the run's output store
/// as a durable reference, as Fabro offloads it; a run with no store keeps
/// the tail with a truncation marker.
pub const OUTPUT_CAP: usize = 8 * 1024 * 1024;

const TRUNCATED: &str = "\n… [output truncated]\n";

/// The metric under `metrics.custom` counting the output bytes the stage
/// did not keep: what the executor's line cap cut off long lines
/// (`executor::lines::LINE_CAP`) plus what [`OUTPUT_CAP`] discarded from
/// the front. Present only when it is not zero, so no byte is lost
/// silently: the output is whole, or this says how much is missing.
pub const DROPPED_BYTES_METRIC: &str = "output.dropped_bytes";

/// The metric under `metrics.custom` counting the lines the line cap cut.
/// Present only when it is not zero.
pub const TRUNCATED_LINES_METRIC: &str = "output.truncated_lines";

/// The metric under `metrics.custom` set to `true` when the capture ended
/// on silence after the script was gone, so the output's tail may be
/// missing by an amount nobody counted. Absent otherwise.
pub const INCOMPLETE_METRIC: &str = "output.incomplete";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandConfig {
    pub label:                String,
    pub node:                 String,
    #[serde(default)]
    pub goal:                 String,
    pub script:               String,
    #[serde(default = "default_language")]
    pub language:             String,
    #[serde(default)]
    pub stdin:                Value,
    #[serde(default)]
    pub output_schema:        Option<Value>,
    #[serde(default)]
    pub on_failure:           Option<Policy>,
    #[serde(default)]
    pub on_retries_exhausted: Option<Policy>,
    /// The node's explicit routes, for failure promotion.
    #[serde(default, rename = "routes")]
    pub explicit_routes:      Option<ExplicitRoutes>,
    #[serde(default)]
    pub timeout_ms:           Option<u64>,
    /// Environment overlays for this process, including secret references
    /// resolved at spawn. Saved graphs retain their encoded overlays.
    #[serde(default)]
    pub env:                  BTreeMap<SmolStr, ValueOrSecretRef>,
    /// The run context at spawn, resolved by the engine.
    #[serde(default)]
    pub kv:                   Value,
}

fn default_language() -> String {
    "shell".into()
}

pub struct CommandStep;

#[async_trait::async_trait]
impl Step for CommandStep {
    const NAME: &'static str = "attractor/command";
    type Config = CommandConfig;

    async fn run(&self, config: CommandConfig, ctx: StepCtx) -> Outcome {
        match execute(config, ctx).await {
            Ok(outcome) => outcome,
            Err(failure) => failure.into(),
        }
    }
}

/// Fabro's default command deadline when the node sets no `timeout`.
pub const DEFAULT_TIMEOUT_MS: u64 = 600_000;

/// The failure class of a command the sandbox ended at its deadline.
pub const TIMEOUT_CLASS: &str = "timeout";

/// The last 2000 characters of the output, appended to a failure reason.
fn append_tail(reason: &mut String, output: &str) {
    if output.trim().is_empty() {
        return;
    }
    let tail: String = output
        .chars()
        .rev()
        .take(2000)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    reason.push('\n');
    reason.push_str(tail.trim_end());
}

/// The text a context value feeds to stdin: strings as they are, everything
/// else as JSON.
fn stdin_text(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone().into_bytes()),
        other => Some(other.to_string().into_bytes()),
    }
}

/// What the capture did not keep, for the outcome's metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct OutputLoss {
    /// Bytes the line cap cut off long lines plus bytes the in-memory cap
    /// discarded from the front.
    dropped_bytes:   usize,
    /// Lines the line cap cut.
    truncated_lines: usize,
    /// The capture ended on silence, so the tail may be missing by an
    /// uncounted amount.
    incomplete:      bool,
}

impl OutputLoss {
    /// Record the loss on `metrics`, each fact only when there is one.
    fn record(self, metrics: &mut ir::Metrics) {
        if self.dropped_bytes > 0 {
            metrics
                .custom
                .insert(DROPPED_BYTES_METRIC.into(), json!(self.dropped_bytes));
        }
        if self.truncated_lines > 0 {
            metrics
                .custom
                .insert(TRUNCATED_LINES_METRIC.into(), json!(self.truncated_lines));
        }
        if self.incomplete {
            metrics.custom.insert(INCOMPLETE_METRIC.into(), json!(true));
        }
    }
}

#[derive(Default)]
struct OutputTail {
    bytes:     VecDeque<u8>,
    /// The output carries the truncation marker: the in-memory cap
    /// discarded its front, or the capture ended on silence.
    truncated: bool,
    loss:      OutputLoss,
}

impl OutputTail {
    fn push(&mut self, bytes: &[u8]) {
        if bytes.len() >= OUTPUT_CAP {
            self.loss.dropped_bytes += self.bytes.len() + bytes.len() - OUTPUT_CAP;
            self.bytes.clear();
            self.bytes.extend(&bytes[bytes.len() - OUTPUT_CAP..]);
            self.truncated = true;
            return;
        }
        self.bytes.extend(bytes);
        if self.bytes.len() > OUTPUT_CAP {
            let excess = self.bytes.len() - OUTPUT_CAP;
            self.loss.dropped_bytes += excess;
            self.truncated = true;
            self.bytes.drain(..excess);
        }
    }

    /// One captured line, with its newline only when the script wrote one:
    /// `command.output` is the script's output byte for byte, as Fabro keeps
    /// it, so a final line the script left unterminated stays that way.
    /// `dropped` is what the executor's line cap cut off the line, counted
    /// here so the outcome says how much of the output is missing.
    fn push_line(&mut self, line: &str, terminated: bool, dropped: usize) {
        if dropped > 0 {
            self.loss.truncated_lines += 1;
            self.loss.dropped_bytes += dropped;
        }
        self.push(line.as_bytes());
        if terminated {
            self.push(b"\n");
        }
    }

    /// The capture ended on silence: the marker applies and the outcome
    /// says the tail may be missing.
    fn ended_on_silence(&mut self) {
        self.truncated = true;
        self.loss.incomplete = true;
    }

    fn render(&self) -> String {
        let bytes: Vec<u8> = self.bytes.iter().copied().collect();
        let start = bytes
            .iter()
            .position(|byte| byte & 0b1100_0000 != 0b1000_0000)
            .unwrap_or(bytes.len());
        let tail = String::from_utf8(bytes[start..].to_vec())
            .expect("the captured output was valid UTF-8 before its prefix was truncated");
        if self.truncated || start > 0 {
            format!("{TRUNCATED}{tail}")
        } else {
            tail
        }
    }
}

async fn execute(config: CommandConfig, mut ctx: StepCtx) -> Result<Outcome, StepFailure> {
    let final_attempt = ctx.is_final_attempt();
    let (program, args) = match config.language.as_str() {
        "python" => ("python3", vec!["-c".to_string(), config.script.clone()]),
        _ => ("bash", vec![
            "-c".to_string(),
            format!("exec 2>&1\n{}", config.script),
        ]),
    };
    let store = ctx.capability::<OutputStore>();
    let stdin_value = match &store {
        Some(store) => blobs::hydrate(config.stdin.clone(), store.0.as_ref()).await,
        None => config.stdin.clone(),
    };
    let stdin = stdin_text(&stdin_value);
    let env = resolve_env_refs(&config.env, ctx.secrets.as_ref())?;
    let spec = ProcessSpec::new(
        program,
        &args.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .with_env(env)
    .with_stdin(if stdin.is_some() {
        StdinMode::Piped
    } else {
        StdinMode::Null
    })
    // The command owns its deadline (`TimeoutPolicy::HandlerManaged`): the
    // sandbox ends the process at `timeout_ms`, and the driver arms no timer
    // of its own around this step.
    .with_timeout(Some(Duration::from_millis(
        config.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS),
    )));
    let mut handle = ctx.env.spawn(spec).await.map_err(|e| StepFailure {
        class:   SPAWN_CLASS,
        message: e.to_string(),
    })?;
    if let (Some(bytes), Some(mut writer)) = (stdin, handle.stdin()) {
        tokio::spawn(async move {
            let _ = writer.write_all(&bytes).await;
            let _ = writer.shutdown().await;
        });
    }
    let captured = Arc::new(Mutex::new(OutputTail::default()));
    let forwarder = handle.lines().map(|lines| {
        let logs = ctx.logs.clone();
        let captured = captured.clone();
        Forwarder::spawn(lines, move |line| {
            captured
                .lock()
                .expect("the output tail lock is not poisoned")
                .push_line(&line.line, line.terminated, line.dropped);
            let logs = logs.clone();
            async move {
                let _ = logs
                    .send(StepEvent::Log {
                        stream: LogStream::Stdout,
                        line:   line.line,
                    })
                    .await;
                ControlFlow::Continue(())
            }
        })
    });
    let grace = ctx.env.grace();
    let ending = ladder(&mut *handle, &mut ctx.control, grace).await;
    // The script is gone; keep capturing for as long as its output keeps
    // arriving. Silence for the idle limit ends the capture, and the output
    // then carries the truncation marker, since its tail may be missing.
    if let Some(forwarder) = forwarder
        && forwarder.finish(DRAIN_IDLE_LIMIT).await == Drain::Silent
    {
        tracing::warn!(
            node = %config.node,
            "the command's output drain ended on silence; its output may be incomplete"
        );
        captured
            .lock()
            .expect("the output tail lock is not poisoned")
            .ended_on_silence();
    }
    let (output, loss) = {
        let captured = captured
            .lock()
            .expect("the output tail lock is not poisoned");
        (captured.render(), captured.loss)
    };

    let mut stage = match ending {
        Ending::Natural(status) if status.is_success() => {
            Stage::new(StageOutcome::Succeeded, config.on_failure)
        }
        Ending::Natural(status) if status.timed_out => {
            // Fabro's `Script timed out after {ms}ms`: a handler failure,
            // classed as a timeout so the reference's transient rule applies.
            let mut reason = format!(
                "Script timed out after {}ms: {}",
                config.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS),
                config.script
            );
            append_tail(&mut reason, &output);
            Stage::failed(reason, TIMEOUT_CLASS, config.on_failure)
        }
        Ending::Natural(status) => {
            let (reason, class) = match (status.code, status.signal) {
                (_, Some(signal)) => (
                    format!("script was killed by signal {signal}"),
                    FailureClass::signal(signal),
                ),
                (code, None) => {
                    let code = code.unwrap_or(-1);
                    (
                        format!("script exited with status {code}"),
                        FailureClass::exit_status(code),
                    )
                }
            };
            let mut reason = reason;
            append_tail(&mut reason, &output);
            Stage::failed(reason, class.as_str(), config.on_failure)
        }
        Ending::Signalled { .. } => {
            let mut out = serde_json::Map::new();
            out.insert("stdout".into(), json!(output));
            let mut outcome = Outcome::new(ir::Status::Cancelled, Value::Object(out));
            loss.record(&mut outcome.metrics);
            return Ok(outcome);
        }
    };
    if let Ending::Natural(status) = &ending {
        stage
            .output
            .insert("exit_status".into(), json!(status.code));
    }
    stage
        .context_updates
        .insert(SmolStr::new("command.output"), json!(output));

    if stage.outcome == StageOutcome::Succeeded
        && config
            .output_schema
            .as_ref()
            .is_some_and(|s| s == "routing")
    {
        match directive::parse(&output) {
            Ok(directive) => {
                directive.apply_to(&mut stage);
            }
            Err(error) => {
                stage = Stage::failed(
                    format!("the script's output is not a routing directive: {error}"),
                    "bad_output",
                    config.on_failure,
                );
                stage
                    .context_updates
                    .insert(SmolStr::new("command.output"), json!(output));
            }
        }
    }
    // The stage output carries the same text; both leave for the store above
    // the threshold, and the logical value stays readable through it.
    let mut stdout = json!(output);
    if let Some(store) = &store {
        blobs::offload_value(&mut stdout, store.0.as_ref()).await;
        blobs::offload_updates(&mut stage.context_updates, store.0.as_ref()).await;
    }
    stage.output.insert("stdout".into(), stdout);
    let mut outcome = stage
        .with_routing(config.explicit_routes.clone(), config.kv.clone())
        .with_retries(config.on_retries_exhausted, final_attempt)
        .into_outcome(&config.node);
    loss.record(&mut outcome.metrics);
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `command.output` is the script's output byte for byte: a final line
    /// the script did not terminate gains no newline, as Fabro keeps it.
    #[test]
    fn captured_output_keeps_the_final_lines_newline_state() {
        let mut tail = OutputTail::default();
        tail.push_line("a", true, 0);
        tail.push_line("b", false, 0);
        assert_eq!(tail.render(), "a\nb");

        let mut tail = OutputTail::default();
        tail.push_line("a", true, 0);
        tail.push_line("b", true, 0);
        assert_eq!(tail.render(), "a\nb\n");

        assert_eq!(OutputTail::default().render(), "");
        assert_eq!(tail.loss, OutputLoss::default());
    }

    /// A capture that ended on silence carries the truncation marker and
    /// reports the output incomplete.
    #[test]
    fn a_silent_drain_marks_the_output_truncated() {
        let mut tail = OutputTail::default();
        tail.push_line("kept", true, 0);
        tail.ended_on_silence();
        assert_eq!(tail.render(), format!("{TRUNCATED}kept\n"));
        assert!(tail.loss.incomplete);
        let mut metrics = ir::Metrics::default();
        tail.loss.record(&mut metrics);
        assert_eq!(metrics.custom.get(INCOMPLETE_METRIC), Some(&json!(true)));
        assert!(!metrics.custom.contains_key(DROPPED_BYTES_METRIC));
    }

    /// Every byte the capture did not keep is counted: what the line cap
    /// cut off a line, and what the in-memory cap discarded from the front.
    #[test]
    fn dropped_bytes_are_counted_from_both_caps() {
        let mut tail = OutputTail::default();
        tail.push_line("cut …[line truncated: 7 bytes dropped]", true, 7);
        tail.push_line("whole", true, 0);
        assert_eq!(tail.loss, OutputLoss {
            dropped_bytes:   7,
            truncated_lines: 1,
            incomplete:      false,
        });
        assert!(!tail.truncated, "the line cap alone adds no output marker");

        let mut tail = OutputTail::default();
        tail.push(&vec![b'a'; OUTPUT_CAP]);
        tail.push(b"bcd");
        assert_eq!(tail.loss.dropped_bytes, 3);
        assert!(tail.truncated);
        let mut tail = OutputTail::default();
        tail.push(b"xy");
        tail.push(&vec![b'a'; OUTPUT_CAP + 5]);
        assert_eq!(
            tail.loss.dropped_bytes, 7,
            "the earlier bytes and the excess"
        );
        let mut metrics = ir::Metrics::default();
        tail.loss.record(&mut metrics);
        assert_eq!(metrics.custom.get(DROPPED_BYTES_METRIC), Some(&json!(7)));
        assert!(!metrics.custom.contains_key(TRUNCATED_LINES_METRIC));
    }
}
