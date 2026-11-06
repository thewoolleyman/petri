//! Step kinds: the semantics of running one step, and the one registry.
//!
//! A step kind knows nothing about where a process runs. It receives an
//! [`ExecEnv`](executor::ExecEnv) capability and uses it, which is why the
//! process step is written once and runs either way.

mod caps;
mod ctx;
mod drain;
mod noop;
mod outputs;
mod process;
mod progress;
pub mod question;

pub use caps::{CAPABILITY_UNAVAILABLE_CLASS, Capabilities, CapabilitiesBuilder};
pub use ctx::{Registry, Step, StepCtx, StepFailure, StepRunner};
pub use drain::{DRAIN_IDLE_LIMIT, Drain, Forwarder};
pub use noop::{NOOP_KIND, NoopStep};
pub use outputs::{BAD_OUTPUT_CLASS, OutputError, parse as parse_outputs};
pub use process::{
    Ending, OUTPUT_INCOMPLETE_NOTE, PROCESS_KIND, ProcessConfig, ProcessStep,
    SECRET_MISPLACED_CLASS, SECRET_UNAVAILABLE_CLASS, Shell, SoftFail, ValueOrSecretRef,
    WORKSPACE_CLASS, check_misplaced_secret, ending_outcome, ladder, resolve_env_refs,
    run_resolved, stringify,
};
pub use progress::{Progress, ProgressAck, ProgressError, ProgressSender};
pub use question::{
    ANSWER_KEY, ANSWER_SECRET_PREFIX, Answer, EXPIRED_KEY, INTERRUPT_KEY, Interrupt, QUESTION_KEY,
    Question, QuestionExpired, QuestionOption, QuestionReference, STEER_KEY, Steer,
};
