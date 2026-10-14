//! The acknowledged progress path: `send_acked` resolves only once the record
//! is in every observer's durable storage, a store's write failure reaches the
//! step as an error, and a crash after an acknowledgement keeps the
//! acknowledged record while the unacknowledged ones ride the re-dispatched
//! attempt.

mod support;

use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use driver::lifecycle::{ExecutionHooks, HookContext, Note, ScopeReleased};
use driver::{Driver, EventObserver, ObserveError, RunConfig};
use engine::{EngineState, Event, EventLog, EventRecord, LOG_VERSION};
use executor::MapSecrets;
use ir::{
    CancelScopeId, ExecutionId, GraphBuilder, InvocationId, Outcome, RunStatus, ScopeId, StepEvent,
    Value,
};
use serde_json::json;
use steps::{ProgressError, Registry};
use store::RunKey;
use support::*;
use tokio::sync::watch;
use tokio::time::{sleep, timeout};

// ── A durable store whose writer can lag the driver ────────────────────────

/// Stands in for a log file whose writer has not caught up with the driver:
/// `on_record` takes every record at once, but the store persists them only
/// while its gate is open, and `durable` answers only once the record is
/// persisted — or, when told to fail, with the write failure.
struct GatedStore {
    open:        AtomicBool,
    pending:     Mutex<Vec<EventRecord>>,
    persisted:   Mutex<Vec<EventRecord>>,
    /// The seq of the last persisted record: a store attached at resume is
    /// handed the suffix only, so it counts seqs, not records.
    last_seq:    watch::Sender<Option<u64>>,
    pending_len: watch::Sender<usize>,
    /// Every `durable` answer from this seq on is a write failure.
    fail_from:   Option<u64>,
}

impl GatedStore {
    fn new(open: bool) -> Arc<Self> {
        Arc::new(Self::with_failure(open, None))
    }

    /// An open store whose every write fails.
    fn failing() -> Arc<Self> {
        Arc::new(Self::with_failure(true, Some(0)))
    }

    fn with_failure(open: bool, fail_from: Option<u64>) -> Self {
        Self {
            open: AtomicBool::new(open),
            pending: Mutex::new(Vec::new()),
            persisted: Mutex::new(Vec::new()),
            last_seq: watch::channel(None).0,
            pending_len: watch::channel(0).0,
            fail_from,
        }
    }

    /// Stop persisting: what the driver appends from here on stays pending.
    fn close(&self) {
        self.open.store(false, Ordering::SeqCst);
    }

    /// Persist everything pending and keep persisting.
    fn open(&self) {
        self.open.store(true, Ordering::SeqCst);
        let mut persisted = self.persisted.lock().expect("not poisoned");
        persisted.append(&mut self.pending.lock().expect("not poisoned"));
        self.pending_len.send_replace(0);
        self.last_seq
            .send_replace(persisted.last().map(|record| record.seq));
    }

    /// Whether a persisted `StepProgress` carries the custom marker `n`.
    fn has_persisted(&self, n: u64) -> bool {
        self.persisted
            .lock()
            .expect("not poisoned")
            .iter()
            .any(|record| marker_of(&record.event) == Some(n))
    }

    /// The persisted records as the log a resume loads.
    fn crash_log(&self) -> EventLog {
        let records = self.persisted.lock().expect("not poisoned").clone();
        EventLog::try_from_records(LOG_VERSION, records).expect("a contiguous prefix")
    }

    /// The custom markers among the persisted records, in order.
    fn persisted_markers(&self) -> Vec<u64> {
        self.persisted
            .lock()
            .expect("not poisoned")
            .iter()
            .filter_map(|record| marker_of(&record.event))
            .collect()
    }

    /// Whether a pending (appended, not persisted) record carries marker `n`.
    fn has_pending(&self, n: u64) -> bool {
        self.pending
            .lock()
            .expect("not poisoned")
            .iter()
            .any(|record| marker_of(&record.event) == Some(n))
    }

    /// Whether a pending (appended, not persisted) record matches.
    fn has_pending_event(&self, matches: impl Fn(&Event) -> bool) -> bool {
        self.pending
            .lock()
            .expect("not poisoned")
            .iter()
            .any(|record| matches(&record.event))
    }

    /// Wait until the driver has handed the store a record that matches,
    /// which the store holds unwritten.
    async fn wait_pending_event(&self, matches: impl Fn(&Event) -> bool) {
        let mut pending = self.pending_len.subscribe();
        timeout(
            Duration::from_secs(10),
            pending.wait_for(|_| self.has_pending_event(&matches)),
        )
        .await
        .expect("the driver appends within the deadline")
        .expect("the store outlives the wait");
    }

    /// Wait until the driver has appended the record with marker `n` and
    /// handed it to the store, which holds it unwritten.
    async fn wait_pending(&self, n: u64) {
        let mut pending = self.pending_len.subscribe();
        timeout(
            Duration::from_secs(10),
            pending.wait_for(|_| self.has_pending(n)),
        )
        .await
        .expect("the driver appends within the deadline")
        .expect("the store outlives the wait");
    }
}

#[async_trait::async_trait]
impl EventObserver for GatedStore {
    fn on_record(&self, record: &EventRecord, _recorded_at: u64, _state: &EngineState) {
        if self.open.load(Ordering::SeqCst) {
            let mut persisted = self.persisted.lock().expect("not poisoned");
            persisted.push(record.clone());
            self.last_seq.send_replace(Some(record.seq));
        } else {
            let mut pending = self.pending.lock().expect("not poisoned");
            pending.push(record.clone());
            self.pending_len.send_replace(pending.len());
        }
    }

    async fn durable(&self, seq: u64) -> Result<(), ObserveError> {
        if let Some(from) = self.fail_from
            && seq >= from
        {
            return Err(ObserveError::new("gated-store", "the write failed"));
        }
        let mut last = self.last_seq.subscribe();
        last.wait_for(|last| last.is_some_and(|last| last >= seq))
            .await
            .map_err(|_| ObserveError::new("gated-store", "the store went away"))?;
        Ok(())
    }
}

fn custom(n: u64) -> StepEvent {
    StepEvent::Custom(json!({ "kind": "ack-test", "n": n }))
}

/// The custom marker a `StepProgress` record carries, if it is one of ours.
fn marker_of(event: &Event) -> Option<u64> {
    match event {
        Event::StepProgressRecorded {
            ev: StepEvent::Custom(value),
            ..
        } if value["kind"] == "ack-test" => value["n"].as_u64(),
        _ => None,
    }
}

fn markers(log: &EventLog) -> Vec<u64> {
    log.events().filter_map(marker_of).collect()
}

/// An error and its source chain on one line.
fn chain(error: &dyn Error) -> String {
    let mut out = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

// ── Step kinds ─────────────────────────────────────────────────────────────

const ACKING: ir::StepKindId = ir::StepKindId::new_static("acking");

/// Sends one acknowledged event and reports what the store held when the
/// acknowledgement came back; a failed acknowledgement is the step's failure.
struct AckingStep {
    store:            Arc<GatedStore>,
    persisted_at_ack: Arc<Mutex<Vec<bool>>>,
}

impl ir::StepKind for AckingStep {
    fn id(&self) -> ir::StepKindId {
        ACKING
    }

    #[expect(
        clippy::unnecessary_literal_bound,
        reason = "the `StepKind` trait fixes this signature; an impl cannot widen the lifetime"
    )]
    fn name(&self) -> &str {
        "acking"
    }
}

#[async_trait::async_trait]
impl steps::StepRunner for AckingStep {
    async fn run(&self, ctx: steps::StepCtx) -> Outcome {
        match ctx.logs.send_acked(custom(1)).await {
            Ok(()) => {
                self.persisted_at_ack
                    .lock()
                    .expect("not poisoned")
                    .push(self.store.has_persisted(1));
                Outcome::success(json!("acked"))
            }
            Err(error @ ProgressError::NotDurable { .. }) => {
                Outcome::failure(format!("not durable: {}", chain(&error)))
            }
            Err(error) => Outcome::failure(format!("other: {}", chain(&error))),
        }
    }
}

const CRASHING: ir::StepKindId = ir::StepKindId::new_static("crashing");

/// The attempt a crash interrupts: an acknowledged event the store persisted,
/// then — with the store's gate closed — a plain event and an acknowledged one
/// that never gets its answer. Re-dispatched after the crash, with `crash`
/// off, it emits all three and finishes.
struct CrashingStep {
    store: Arc<GatedStore>,
    crash: bool,
}

impl ir::StepKind for CrashingStep {
    fn id(&self) -> ir::StepKindId {
        CRASHING
    }

    #[expect(
        clippy::unnecessary_literal_bound,
        reason = "the `StepKind` trait fixes this signature; an impl cannot widen the lifetime"
    )]
    fn name(&self) -> &str {
        "crashing"
    }
}

#[async_trait::async_trait]
impl steps::StepRunner for CrashingStep {
    async fn run(&self, ctx: steps::StepCtx) -> Outcome {
        ctx.logs.send_acked(custom(1)).await.expect("acknowledged");
        assert!(
            self.store.has_persisted(1),
            "the acknowledgement means the store has the record"
        );
        if self.crash {
            self.store.close();
        }
        ctx.logs.send(custom(2)).await.expect("queued");
        // With the gate closed this never resolves: the run dies first.
        ctx.logs.send_acked(custom(3)).await.expect("acknowledged");
        Outcome::success(json!("all three"))
    }
}

fn one_node(kind: ir::StepKindId) -> ir::Graph {
    let mut b = GraphBuilder::new();
    b.add_step("work", ScopeId::new(0), kind);
    b.build()
}

fn registry_with(runner: Arc<dyn steps::StepRunner>) -> Registry {
    let mut registry = runners();
    registry.register_runner(runner);
    registry
}

fn driver_with(
    graph: ir::Graph,
    dir: &RunDir,
    runner: Arc<dyn steps::StepRunner>,
    store: &Arc<GatedStore>,
) -> Driver {
    host_driver_full(
        graph,
        dir,
        MapSecrets::empty(),
        RunConfig::new(dir.path()),
        registry_with(runner),
    )
    .observe(store.clone() as Arc<dyn EventObserver>)
}

// ── The battery ────────────────────────────────────────────────────────────

/// The acknowledgement waits for the store: with the gate closed the driver
/// appends the record and the step keeps waiting; once the store persists it,
/// the acknowledgement arrives and the step finds the record persisted.
#[tokio::test]
async fn an_acknowledgement_arrives_only_once_the_store_has_the_record() {
    let dir = RunDir::new("ack-waits");
    let store = GatedStore::new(false);
    let persisted_at_ack = Arc::new(Mutex::new(Vec::new()));
    let step = Arc::new(AckingStep {
        store:            store.clone(),
        persisted_at_ack: persisted_at_ack.clone(),
    });
    let driver = driver_with(one_node(ACKING), &dir, step, &store);
    let run = tokio::spawn(driver.run());

    // The record is appended and handed to the store, which has not written it.
    store.wait_pending(1).await;
    assert!(!store.has_persisted(1));
    store.open();

    let report = timeout(Duration::from_secs(10), run)
        .await
        .expect("the run finishes once the store writes")
        .expect("the run task");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(
        *persisted_at_ack.lock().expect("not poisoned"),
        vec![true],
        "at the acknowledgement the record was already persisted"
    );
    assert_eq!(markers(&report.state.log), vec![1]);
    assert!(report.observer_errors.is_empty());
}

/// A store that cannot write answers the acknowledgement with its failure,
/// and the step sees `NotDurable` naming the store.
#[tokio::test]
async fn a_write_failure_reaches_the_step_as_not_durable() {
    let dir = RunDir::new("ack-write-fails");
    let store = GatedStore::failing();
    let step = Arc::new(AckingStep {
        store:            store.clone(),
        persisted_at_ack: Arc::new(Mutex::new(Vec::new())),
    });
    let report = driver_with(one_node(ACKING), &dir, step, &store)
        .await_run()
        .await;

    assert_eq!(report.status, RunStatus::Failed);
    let message = report
        .state
        .history()
        .iter()
        .find(|r| r.name == "work")
        .and_then(|r| r.outcome.status.failure_info().map(|f| f.message.clone()))
        .expect("the step failed");
    assert!(message.starts_with("not durable: "), "{message}");
    assert!(
        message.contains("gated-store") && message.contains("the write failed"),
        "the store's failure is the step's error: {message}"
    );
    // The record itself was appended: the failure is the store's, not the log's.
    assert_eq!(markers(&report.state.log), vec![1]);
}

/// A crash between the append and the store's write: what was acknowledged is
/// in the loaded log with its identity; what was only queued, and what was
/// appended but never acknowledged, is not — the re-dispatched attempt emits
/// them again, at least once.
#[tokio::test]
async fn a_crash_after_an_acknowledgement_keeps_the_record_and_redispatches_the_rest() {
    let dir = RunDir::new("ack-crash");
    let graph = one_node(CRASHING);
    let store = GatedStore::new(true);
    let step = Arc::new(CrashingStep {
        store: store.clone(),
        crash: true,
    });
    let driver = driver_with(graph.clone(), &dir, step, &store);
    let run = tokio::spawn(driver.run());

    // Events 2 and 3 are appended in memory and handed to the store, which
    // never writes them; the step is waiting on 3's acknowledgement.
    store.wait_pending(3).await;
    run.abort();
    let _ = run.await;

    let prefix = store.crash_log();
    assert_eq!(
        markers(&prefix),
        vec![1],
        "the acknowledged record survived; the others did not"
    );
    let firing = prefix
        .events()
        .find_map(|event| match event {
            Event::StepStarted { firing, .. } => Some(*firing),
            _ => None,
        })
        .expect("the attempt started before the crash");

    let resumed_store = GatedStore::new(true);
    let executor: Arc<dyn executor::Executor> =
        Arc::new(executor_sandbox::HostExecutor::new(dir.path()));
    let (driver, info) = Driver::resume(
        graph.clone(),
        prefix.clone(),
        executor,
        registry_with(Arc::new(CrashingStep {
            store: resumed_store.clone(),
            crash: false,
        })),
        Arc::new(MapSecrets::empty()),
        RunConfig::new(dir.path()),
    )
    .expect("the crash log resumes");
    assert_eq!(info.redispatched, vec![firing], "the attempt runs again");
    assert_eq!(info.loaded, prefix.len());
    let resumed = driver
        .observe(resumed_store.clone() as Arc<dyn EventObserver>)
        .await_run()
        .await;
    assert_eq!(
        resumed.status,
        RunStatus::Success,
        "{:?}",
        resumed.state.errors()
    );
    assert_eq!(
        markers(&resumed.state.log),
        vec![1, 1, 2, 3],
        "the acknowledged record once from the crash, then every event of the re-dispatched attempt"
    );
    assert_eq!(
        resumed_store.persisted_markers(),
        vec![1, 2, 3],
        "the resumed store was handed the regenerated suffix and the new records, not the prefix"
    );
    assert_one_terminal_per_firing(&resumed);
    assert_replay_identical(&graph, &resumed);
}

/// Without any observer the acknowledgement is the append itself.
#[tokio::test]
async fn without_observers_the_append_is_the_acknowledgement() {
    let dir = RunDir::new("ack-no-observers");
    let store = GatedStore::new(true);
    let persisted_at_ack = Arc::new(Mutex::new(Vec::new()));
    let step = Arc::new(AckingStep {
        store,
        persisted_at_ack: persisted_at_ack.clone(),
    });
    let report = host_driver_full(
        one_node(ACKING),
        &dir,
        MapSecrets::empty(),
        RunConfig::new(dir.path()),
        registry_with(step),
    )
    .await_run()
    .await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(markers(&report.state.log), vec![1]);
    assert_eq!(output_of(&report, "work"), Value::from("acked"));
}

/// The run log gates a scope: its first attempt starts only once the run
/// log holds the scope's `scope.acquired`, so a `scope_released` point
/// that runs later is always found again on resume.
#[tokio::test]
async fn a_scope_starts_nothing_before_the_run_log_holds_its_acquisition() {
    let dir = RunDir::new("ack-scope-stored");
    let store = GatedStore::new(false);
    let step = Arc::new(AckingStep {
        store:            store.clone(),
        persisted_at_ack: Arc::new(Mutex::new(Vec::new())),
    });
    let driver = host_driver_full(
        one_node(ACKING),
        &dir,
        MapSecrets::empty(),
        RunConfig::new(dir.path()),
        registry_with(step),
    )
    .observe_run_log(store.clone() as Arc<dyn EventObserver>);
    let run = tokio::spawn(driver.run());

    store
        .wait_pending_event(|event| matches!(event, Event::ScopeAcquired { .. }))
        .await;
    sleep(Duration::from_millis(50)).await;
    assert!(
        !store.has_pending_event(|event| matches!(event, Event::StepStarted { .. })),
        "no attempt started before the run log held the acquisition"
    );

    store.open();
    let report = timeout(Duration::from_secs(10), run)
        .await
        .expect("the run ends once the store writes")
        .expect("the run task finished");
    assert_eq!(report.status, RunStatus::Success);
    assert_eq!(report.store_failure, None);
}

/// A run log that cannot store the scope's acquisition stops the run there:
/// nothing starts in the scope, and nothing fails for it.
#[tokio::test]
async fn a_run_log_that_cannot_store_the_acquisition_stops_the_run() {
    let dir = RunDir::new("ack-scope-unstored");
    let store = GatedStore::failing();
    let step = Arc::new(AckingStep {
        store:            store.clone(),
        persisted_at_ack: Arc::new(Mutex::new(Vec::new())),
    });
    let report = host_driver_full(
        one_node(ACKING),
        &dir,
        MapSecrets::empty(),
        RunConfig::new(dir.path()),
        registry_with(step),
    )
    .observe_run_log(store.clone() as Arc<dyn EventObserver>)
    .await_run()
    .await;

    let failure = report.store_failure.expect("the run stopped for its store");
    assert!(failure.contains("the write failed"), "{failure}");
    assert!(
        !report.state.log.events().any(|event| matches!(
            event,
            Event::StepStarted { .. } | Event::StepFinished { .. } | Event::ScopeFailed { .. }
        )),
        "nothing started or failed after the store failed"
    );
}

/// Counts the `scope_released` points it is asked for.
#[derive(Default)]
struct CountingHooks(AtomicUsize);

#[async_trait::async_trait]
impl ExecutionHooks for CountingHooks {
    async fn scope_released(&self, _context: &HookContext, _released: ScopeReleased) -> Vec<Note> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Vec::new()
    }
}

/// A scope released while its acquisition waits on the run log runs its
/// `scope_released` point only once the run log holds it: the point runs
/// only for a scope a resume can find.
#[tokio::test]
async fn a_scope_released_while_storing_runs_its_point_once_stored() {
    let dir = RunDir::new("ack-release-storing");
    let store = GatedStore::new(false);
    let step = Arc::new(AckingStep {
        store:            store.clone(),
        persisted_at_ack: Arc::new(Mutex::new(Vec::new())),
    });
    let hooks = Arc::new(CountingHooks::default());
    let driver = host_driver_full(
        one_node(ACKING),
        &dir,
        MapSecrets::empty(),
        RunConfig::new(dir.path()),
        registry_with(step),
    )
    .observe_run_log(store.clone() as Arc<dyn EventObserver>)
    .with_hooks(
        hooks.clone(),
        HookContext::new(RunKey::new("ack"), InvocationId::ROOT, ExecutionId::new(0)),
    );
    let handle = driver.handle();
    let run = tokio::spawn(driver.run());

    store
        .wait_pending_event(|event| matches!(event, Event::ScopeAcquired { .. }))
        .await;
    // The cancel settles the waiting attempt, and the scope is released.
    handle.cancel(CancelScopeId::ROOT).await;
    sleep(Duration::from_millis(50)).await;
    assert_eq!(
        hooks.0.load(Ordering::SeqCst),
        0,
        "no point before the run log holds the acquisition"
    );

    store.open();
    let report = timeout(Duration::from_secs(10), run)
        .await
        .expect("the run ends once the store writes")
        .expect("the run task finished");
    assert_eq!(report.status, RunStatus::Cancelled);
    assert_eq!(hooks.0.load(Ordering::SeqCst), 1, "the point ran once");
}

// ── The run log gates an attempt's start ───────────────────────────────────

/// A run log that stores every record at once except an attempt's
/// `StepStarted`: that one it holds until released or, told to fail, refuses.
struct StartGate {
    fail:      bool,
    released:  watch::Sender<bool>,
    start_seq: watch::Sender<Option<u64>>,
}

impl StartGate {
    fn holding() -> Arc<Self> {
        Arc::new(Self::new(false))
    }

    fn failing() -> Arc<Self> {
        Arc::new(Self::new(true))
    }

    fn new(fail: bool) -> Self {
        Self {
            fail,
            released: watch::channel(false).0,
            start_seq: watch::channel(None).0,
        }
    }

    fn release(&self) {
        self.released.send_replace(true);
    }

    /// Wait until the driver has handed the run log an attempt's start.
    async fn wait_start(&self) {
        let mut start = self.start_seq.subscribe();
        timeout(Duration::from_secs(10), start.wait_for(Option::is_some))
            .await
            .expect("the driver appends the start within the deadline")
            .expect("the run log outlives the wait");
    }
}

#[async_trait::async_trait]
impl EventObserver for StartGate {
    fn on_record(&self, record: &EventRecord, _recorded_at: u64, _state: &EngineState) {
        if matches!(record.event, Event::StepStarted { .. }) {
            self.start_seq.send_if_modified(|start| {
                if start.is_none() {
                    *start = Some(record.seq);
                    return true;
                }
                false
            });
        }
    }

    async fn durable(&self, seq: u64) -> Result<(), ObserveError> {
        let start = *self.start_seq.borrow();
        if start.is_none_or(|start| seq < start) {
            return Ok(());
        }
        if self.fail {
            return Err(ObserveError::new("start-gate", "the start write failed"));
        }
        let mut released = self.released.subscribe();
        released
            .wait_for(|released| *released)
            .await
            .map_err(|_| ObserveError::new("start-gate", "the run log went away"))?;
        Ok(())
    }
}

const MARKING: ir::StepKindId = ir::StepKindId::new_static("marking");

/// Notes that it ran, and succeeds.
#[derive(Default)]
struct MarkingStep {
    ran: AtomicBool,
}

impl MarkingStep {
    fn ran(&self) -> bool {
        self.ran.load(Ordering::SeqCst)
    }
}

impl ir::StepKind for MarkingStep {
    fn id(&self) -> ir::StepKindId {
        MARKING
    }

    #[expect(
        clippy::unnecessary_literal_bound,
        reason = "the `StepKind` trait fixes this signature; an impl cannot widen the lifetime"
    )]
    fn name(&self) -> &str {
        "marking"
    }
}

#[async_trait::async_trait]
impl steps::StepRunner for MarkingStep {
    async fn run(&self, _ctx: steps::StepCtx) -> Outcome {
        self.ran.store(true, Ordering::SeqCst);
        Outcome::success(json!("ran"))
    }
}

/// An attempt runs only once the run log holds its `StepStarted`, so a run
/// log without that record proves the attempt never ran.
#[tokio::test]
async fn a_step_starts_only_once_the_run_log_holds_its_start() {
    let dir = RunDir::new("start-stored");
    let run_log = StartGate::holding();
    let step = Arc::new(MarkingStep::default());
    let driver = host_driver_full(
        one_node(MARKING),
        &dir,
        MapSecrets::empty(),
        RunConfig::new(dir.path()),
        registry_with(step.clone()),
    )
    .observe_run_log(run_log.clone() as Arc<dyn EventObserver>);
    let run = tokio::spawn(driver.run());

    run_log.wait_start().await;
    sleep(Duration::from_millis(50)).await;
    assert!(
        !step.ran(),
        "the step did not run before the run log held its start"
    );

    run_log.release();
    let report = timeout(Duration::from_secs(10), run)
        .await
        .expect("the run ends once the run log holds the start")
        .expect("the run task finished");
    assert_eq!(report.status, RunStatus::Success);
    assert!(step.ran());
    assert_eq!(report.store_failure, None);
}

/// A run log that cannot store an attempt's `StepStarted` stops the run
/// before the step runs: nothing runs that a resume could not find.
#[tokio::test]
async fn a_run_log_that_cannot_store_a_start_stops_the_run_before_the_step() {
    let dir = RunDir::new("start-unstored");
    let run_log = StartGate::failing();
    let step = Arc::new(MarkingStep::default());
    let report = host_driver_full(
        one_node(MARKING),
        &dir,
        MapSecrets::empty(),
        RunConfig::new(dir.path()),
        registry_with(step.clone()),
    )
    .observe_run_log(run_log.clone() as Arc<dyn EventObserver>)
    .await_run()
    .await;

    let failure = report.store_failure.expect("the run stopped for its store");
    assert!(failure.contains("the start write failed"), "{failure}");
    assert!(!step.ran(), "the step never ran");
    assert!(
        !report
            .state
            .log
            .events()
            .any(|event| matches!(event, Event::StepFinished { .. })),
        "no attempt finished after the store failed"
    );
}
