//! The stall watchdog: `stall_timeout` as a run-wide inactivity policy.
//!
//! A run that emits no execution event for the stall budget is cancelled. Any
//! engine or lifecycle record of any execution is activity. A pending human
//! question parks the clock: while a step of the run waits on a person, the
//! run is not stalled, it is blocked. When the last pending question is
//! answered, or expires by the step's own report, the run gets a full stall
//! budget again. A pause parks it too: a paused run waits on its host, by
//! the host's own choice, and its unpause restarts the full budget. A run
//! resumed paused starts parked.
//!
//! This is separate from each attempt's active-work timer
//! ([`ir::TimeoutPolicy`]): that one bounds one step's own work, this one
//! notices a run that stopped doing anything at all.
//!
//! The watchdog does not wake on every event (a busy agent emits one per
//! stream delta). It sleeps until its deadline, re-reads the last activity
//! when it wakes, and re-arms when the run was active in the meantime.
//!
//! It reads time through a [`WatchdogClock`]: tokio's clock by default, or an
//! injected one, so a host's tests can decide exactly how much time passes
//! without depending on the host's load or the wall clock.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use engine::{EngineState, Event, EventRecord};
use serde::{Deserialize, Serialize};
use steps::{Answer, Question, QuestionExpired};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;

use crate::{
    CancelReason, CoordinatorEvent, CoordinatorHandle, CoordinatorRecord, CoordinatorState,
    ExecutionId, ExecutionObserver,
};

/// The time source the watchdog measures inactivity against.
pub trait WatchdogClock: Send + Sync {
    /// The current instant.
    fn now(&self) -> Instant;
    /// Complete once [`now`](Self::now) has reached `deadline`.
    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// Tokio's clock: the production default.
#[derive(Clone, Copy, Debug, Default)]
pub struct TokioClock;

impl WatchdogClock for TokioClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(sleep_until(deadline))
    }
}

/// A clock that moves only when [`advance`](Self::advance) moves it: for
/// tests whose verdict must not depend on how long the host takes.
#[derive(Clone)]
pub struct ManualClock {
    inner: Arc<ManualInner>,
}

struct ManualInner {
    origin:  Instant,
    elapsed: Mutex<Duration>,
    moved:   Notify,
}

impl ManualClock {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ManualInner {
                origin:  Instant::now(),
                elapsed: Mutex::new(Duration::ZERO),
                moved:   Notify::new(),
            }),
        }
    }

    /// Move the clock forward by `by`, waking every sleeper whose deadline
    /// it reaches.
    pub fn advance(&self, by: Duration) {
        *self
            .inner
            .elapsed
            .lock()
            .unwrap_or_else(PoisonError::into_inner) += by;
        self.inner.moved.notify_waiters();
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchdogClock for ManualClock {
    fn now(&self) -> Instant {
        self.inner.origin
            + *self
                .inner
                .elapsed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
    }

    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            loop {
                let moved = self.inner.moved.notified();
                tokio::pin!(moved);
                // Register before reading the time, so an advance between
                // the read and the await is not missed.
                moved.as_mut().enable();
                if self.now() >= deadline {
                    return;
                }
                moved.await;
            }
        })
    }
}

/// Why the watchdog cancelled the run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StallTimeout {
    /// The configured budget.
    pub stall_timeout_ms: u64,
    /// How long the run had been idle when the watchdog fired.
    pub idle_ms:          u64,
}

struct State {
    last_activity: Instant,
    /// Questions asked and not yet answered, by execution and question id.
    pending:       BTreeSet<(ExecutionId, String)>,
    /// The run's last recorded control is a pause.
    paused:        bool,
    tripped:       Option<StallTimeout>,
}

impl State {
    /// The clock is parked: a question waits on a person, or the host
    /// paused the run.
    fn parked(&self) -> bool {
        self.paused || !self.pending.is_empty()
    }
}

struct Inner {
    clock:   Arc<dyn WatchdogClock>,
    timeout: Duration,
    state:   Mutex<State>,
    changed: Notify,
}

impl Inner {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The watchdog: an [`ExecutionObserver`] that feeds activity, and a task
/// that cancels the run through the coordinator when the budget runs out.
/// Construct before the run, [`observe`](crate::host::HostRun::observe) a
/// clone, [`start`](Self::start) it once the coordinator exists, and
/// [`stop`](Self::stop) it after the run to learn whether it fired.
#[derive(Clone)]
pub struct StallWatchdog {
    inner: Arc<Inner>,
    stop:  CancellationToken,
}

impl StallWatchdog {
    pub fn new(timeout: Duration) -> Self {
        Self::with_clock(timeout, Arc::new(TokioClock))
    }

    /// A watchdog that measures inactivity on `clock` instead of tokio's.
    pub fn with_clock(timeout: Duration, clock: Arc<dyn WatchdogClock>) -> Self {
        let now = clock.now();
        Self {
            inner: Arc::new(Inner {
                clock,
                timeout,
                state: Mutex::new(State {
                    last_activity: now,
                    pending:       BTreeSet::new(),
                    paused:        false,
                    tripped:       None,
                }),
                changed: Notify::new(),
            }),
            stop:  CancellationToken::new(),
        }
    }

    pub fn timeout(&self) -> Duration {
        self.inner.timeout
    }

    /// Whether at least one question of the run is waiting for an answer.
    pub fn is_blocked(&self) -> bool {
        !self.inner.state().pending.is_empty()
    }

    /// Start the monitor task. `cancel` is called once when the run stalls.
    pub fn start(&self, handle: CoordinatorHandle) -> WatchdogTask {
        self.inner.state().last_activity = self.inner.clock.now();
        let inner = self.inner.clone();
        let stop = self.stop.clone();
        let task = tokio::spawn(async move {
            monitor(inner, stop, move |stall: StallTimeout| {
                handle.cancel_root_for(CancelReason::StallTimeout {
                    stall_timeout_ms: stall.stall_timeout_ms,
                    idle_ms:          stall.idle_ms,
                });
            })
            .await;
        });
        WatchdogTask {
            stop: self.stop.clone(),
            task: Some(task),
        }
    }

    /// The stall the watchdog reported, if it fired.
    pub fn tripped(&self) -> Option<StallTimeout> {
        self.inner.state().tripped.clone()
    }

    fn touch(&self) {
        self.inner.state().last_activity = self.inner.clock.now();
    }

    fn block(&self, execution: ExecutionId, question: String) {
        let mut state = self.inner.state();
        let was_blocked = !state.pending.is_empty();
        state.pending.insert((execution, question));
        drop(state);
        if !was_blocked {
            tracing::debug!("stall watchdog parked: a question is pending");
            self.inner.changed.notify_one();
        }
    }

    /// The run was paused, or unpaused: a pause parks the clock, and an
    /// unpause restarts the full budget.
    fn set_paused(&self, paused: bool) {
        let mut state = self.inner.state();
        if state.paused == paused {
            return;
        }
        state.paused = paused;
        if !paused {
            state.last_activity = self.inner.clock.now();
        }
        drop(state);
        tracing::debug!(paused, "stall watchdog follows the run's pause");
        self.inner.changed.notify_one();
    }

    fn unblock(&self, execution: ExecutionId, question: &str) {
        let mut state = self.inner.state();
        if !state.pending.remove(&(execution, question.to_owned())) {
            return;
        }
        if state.pending.is_empty() {
            // Unblocking restarts the full budget.
            state.last_activity = self.inner.clock.now();
            drop(state);
            tracing::debug!("stall watchdog resumed with a full budget");
            self.inner.changed.notify_one();
        }
    }
}

/// The running monitor. Stop it after the run; dropping it aborts the task.
pub struct WatchdogTask {
    stop: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl WatchdogTask {
    pub async fn stop(mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for WatchdogTask {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn monitor(inner: Arc<Inner>, stop: CancellationToken, cancel: impl Fn(StallTimeout)) {
    loop {
        let (deadline, blocked) = {
            let state = inner.state();
            (state.last_activity + inner.timeout, state.parked())
        };
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            () = inner.changed.notified() => {}
            () = inner.clock.sleep_until(deadline), if !blocked => {
                let mut state = inner.state();
                if state.parked() {
                    continue;
                }
                let idle = inner.clock.now().saturating_duration_since(state.last_activity);
                if idle < inner.timeout {
                    // Active in the meantime: re-arm from the newer activity.
                    continue;
                }
                let stall = StallTimeout {
                    stall_timeout_ms: u64::try_from(inner.timeout.as_millis()).unwrap_or(u64::MAX),
                    idle_ms:          u64::try_from(idle.as_millis()).unwrap_or(u64::MAX),
                };
                tracing::warn!(
                    stall_timeout_ms = stall.stall_timeout_ms,
                    idle_ms = stall.idle_ms,
                    "stall watchdog: no execution activity; cancelling the run"
                );
                state.tripped = Some(stall.clone());
                drop(state);
                cancel(stall);
                return;
            }
        }
    }
}

impl ExecutionObserver for StallWatchdog {
    fn on_engine_record(
        &self,
        execution: ExecutionId,
        record: &EventRecord,
        _recorded_at: u64,
        _: &EngineState,
    ) {
        self.touch();
        match &record.event {
            Event::StepProgressRecorded { ev, .. } => {
                if let Some(question) = Question::from_event(ev) {
                    self.block(execution, question.id);
                } else if let Some(expired) = QuestionExpired::from_event(ev) {
                    self.unblock(execution, &expired.question);
                }
            }
            Event::ControlRequested {
                ctl: ir::Control::Deliver(value),
                ..
            } => {
                if let Some(id) = Answer::from_value(value).and_then(|answer| answer.question) {
                    self.unblock(execution, &id);
                }
            }
            Event::StepFinished { firing, .. } => {
                // The firing is gone: whatever it asked is moot. Question ids
                // start with `<node>#<firing>`, so the firing is the prefix's
                // tail; match on the `#<firing>` part alone.
                let marker = format!("#{}", firing.raw());
                let ids: Vec<String> = self
                    .inner
                    .state()
                    .pending
                    .iter()
                    .filter(|(exec, id)| {
                        *exec == execution
                            && id
                                .split_once('/')
                                .map_or(id.as_str(), |(head, _)| head)
                                .ends_with(&marker)
                    })
                    .map(|(_, id)| id.clone())
                    .collect();
                for id in ids {
                    self.unblock(execution, &id);
                }
            }
            _ => {}
        }
    }

    fn on_lifecycle(&self, record: &CoordinatorRecord) {
        self.touch();
        match record.body {
            CoordinatorEvent::RunPaused => self.set_paused(true),
            CoordinatorEvent::RunUnpaused => self.set_paused(false),
            _ => {}
        }
    }

    /// A run resumed paused starts parked, as the control service starts it
    /// held.
    fn on_resumed(&self, state: &CoordinatorState) {
        if state.paused {
            self.set_paused(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::task::yield_now;
    use tokio::time::advance;

    use super::*;

    fn run_monitor(
        inner: Arc<Inner>,
        stop: CancellationToken,
        fired: Arc<AtomicUsize>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            monitor(inner, stop, move |_| {
                fired.fetch_add(1, Ordering::SeqCst);
            })
            .await;
        })
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_run_is_cancelled_once_at_the_budget() {
        let watchdog = StallWatchdog::new(Duration::from_secs(60));
        let fired = Arc::new(AtomicUsize::new(0));
        let task = run_monitor(watchdog.inner.clone(), watchdog.stop.clone(), fired.clone());
        advance(Duration::from_secs(59)).await;
        yield_now().await;
        assert_eq!(fired.load(Ordering::SeqCst), 0);
        advance(Duration::from_secs(2)).await;
        task.await.expect("the monitor ends after firing");
        assert_eq!(fired.load(Ordering::SeqCst), 1);
        let stall = watchdog.tripped().expect("tripped");
        assert_eq!(stall.stall_timeout_ms, 60_000);
        assert!(stall.idle_ms >= 60_000, "{stall:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn activity_re_arms_without_waking_the_monitor() {
        let watchdog = StallWatchdog::new(Duration::from_secs(60));
        let fired = Arc::new(AtomicUsize::new(0));
        let task = run_monitor(watchdog.inner.clone(), watchdog.stop.clone(), fired.clone());
        advance(Duration::from_secs(50)).await;
        watchdog.touch();
        advance(Duration::from_secs(50)).await;
        yield_now().await;
        assert_eq!(fired.load(Ordering::SeqCst), 0, "active 50 s ago");
        advance(Duration::from_secs(11)).await;
        task.await.expect("fires once idle");
        assert_eq!(fired.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_pending_question_parks_the_clock_and_an_answer_restarts_it_in_full() {
        let watchdog = StallWatchdog::new(Duration::from_secs(60));
        let fired = Arc::new(AtomicUsize::new(0));
        let task = run_monitor(watchdog.inner.clone(), watchdog.stop.clone(), fired.clone());
        advance(Duration::from_secs(30)).await;
        watchdog.block(ExecutionId::new(0), "gate#3".into());
        advance(Duration::from_secs(600)).await;
        yield_now().await;
        assert_eq!(fired.load(Ordering::SeqCst), 0, "blocked runs never stall");
        assert!(watchdog.is_blocked());
        watchdog.unblock(ExecutionId::new(0), "gate#3");
        advance(Duration::from_secs(59)).await;
        yield_now().await;
        assert_eq!(
            fired.load(Ordering::SeqCst),
            0,
            "a full budget after unblocking"
        );
        advance(Duration::from_secs(2)).await;
        task.await.expect("fires");
        assert_eq!(fired.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_pause_parks_the_clock_and_an_unpause_restarts_it_in_full() {
        let watchdog = StallWatchdog::new(Duration::from_secs(60));
        let fired = Arc::new(AtomicUsize::new(0));
        let task = run_monitor(watchdog.inner.clone(), watchdog.stop.clone(), fired.clone());
        advance(Duration::from_secs(30)).await;
        watchdog.set_paused(true);
        advance(Duration::from_secs(600)).await;
        yield_now().await;
        assert_eq!(fired.load(Ordering::SeqCst), 0, "paused runs never stall");
        watchdog.set_paused(false);
        advance(Duration::from_secs(59)).await;
        yield_now().await;
        assert_eq!(
            fired.load(Ordering::SeqCst),
            0,
            "a full budget after the unpause"
        );
        advance(Duration::from_secs(2)).await;
        task.await.expect("fires");
        assert_eq!(fired.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn two_pending_questions_unblock_only_when_both_are_answered() {
        let watchdog = StallWatchdog::new(Duration::from_secs(60));
        let fired = Arc::new(AtomicUsize::new(0));
        let task = run_monitor(watchdog.inner.clone(), watchdog.stop.clone(), fired.clone());
        watchdog.block(ExecutionId::new(0), "a#1".into());
        watchdog.block(ExecutionId::new(1), "b#2".into());
        watchdog.unblock(ExecutionId::new(0), "a#1");
        advance(Duration::from_secs(600)).await;
        yield_now().await;
        assert_eq!(fired.load(Ordering::SeqCst), 0);
        // A stale answer to a question nobody asked changes nothing.
        watchdog.unblock(ExecutionId::new(0), "zzz#9");
        assert!(watchdog.is_blocked());
        watchdog.unblock(ExecutionId::new(1), "b#2");
        advance(Duration::from_secs(61)).await;
        task.await.expect("fires");
        assert_eq!(fired.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn stopping_ends_the_monitor_without_firing() {
        let watchdog = StallWatchdog::new(Duration::from_secs(60));
        let fired = Arc::new(AtomicUsize::new(0));
        let task = run_monitor(watchdog.inner.clone(), watchdog.stop.clone(), fired.clone());
        watchdog.stop.cancel();
        task.await.expect("stopped");
        advance(Duration::from_secs(600)).await;
        assert_eq!(fired.load(Ordering::SeqCst), 0);
        assert!(watchdog.tripped().is_none());
    }

    /// An injected clock is the only time the watchdog sees: tokio's clock
    /// racing ahead changes nothing, and the manual clock alone decides.
    #[tokio::test(start_paused = true)]
    async fn a_manual_clock_alone_decides_the_stall() {
        let clock = ManualClock::new();
        let watchdog = StallWatchdog::with_clock(Duration::from_secs(1), Arc::new(clock.clone()));
        let fired = Arc::new(AtomicUsize::new(0));
        let task = run_monitor(watchdog.inner.clone(), watchdog.stop.clone(), fired.clone());
        advance(Duration::from_secs(600)).await;
        yield_now().await;
        assert_eq!(
            fired.load(Ordering::SeqCst),
            0,
            "tokio time is not the watchdog's"
        );
        for _ in 0..10 {
            clock.advance(Duration::from_millis(900));
            watchdog.touch();
            yield_now().await;
        }
        assert_eq!(
            fired.load(Ordering::SeqCst),
            0,
            "activity inside each budget"
        );
        clock.advance(Duration::from_millis(999));
        yield_now().await;
        assert_eq!(fired.load(Ordering::SeqCst), 0, "one millisecond short");
        clock.advance(Duration::from_millis(1));
        task.await.expect("fires at the budget");
        assert_eq!(fired.load(Ordering::SeqCst), 1);
        assert_eq!(
            watchdog.tripped(),
            Some(StallTimeout {
                stall_timeout_ms: 1000,
                idle_ms:          1000,
            })
        );
    }

    /// A monitor whose task handle is dropped (a crashed host) ends with
    /// it: it never fires into a run it no longer watches.
    #[tokio::test(start_paused = true)]
    async fn dropping_the_task_ends_the_monitor() {
        let watchdog = StallWatchdog::new(Duration::from_secs(60));
        let fired = Arc::new(AtomicUsize::new(0));
        let task = WatchdogTask {
            stop: watchdog.stop.clone(),
            task: Some(run_monitor(
                watchdog.inner.clone(),
                watchdog.stop.clone(),
                fired.clone(),
            )),
        };
        yield_now().await;
        drop(task);
        advance(Duration::from_secs(600)).await;
        yield_now().await;
        assert_eq!(fired.load(Ordering::SeqCst), 0);
        assert!(watchdog.tripped().is_none());
    }
}
