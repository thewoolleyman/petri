//! The post-exit output drain every process-running step kind shares.
//!
//! A step forwards its process's output lines on a task of its own, so the
//! forwarding keeps going through cancellation. Once the process is gone the
//! step waits for that task, and the wait is bounded by **silence of the
//! stream**, not by a clock started at the exit and not by the consumer's
//! pace. Output that keeps arriving re-arms the limit, and a consumer that is
//! slow, even one that holds a single line for longer than the limit, is
//! backpressure rather than silence: the drain keeps waiting, so a loaded run
//! never loses the tail of a process that already exited. Only a stream that
//! yields nothing for the whole limit while the forwarder is waiting on it (a
//! pipe a backgrounded grandchild holds open) stops the forwarder, and the
//! step then reports the output as possibly incomplete. A consumer that never
//! reads again is bounded by the run's own deadline, not by this drain.

use std::future::Future;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time;

/// How long a post-exit drain waits on a silent stream for its next line
/// before it gives up.
pub const DRAIN_IDLE_LIMIT: Duration = Duration::from_secs(5);

/// How a forwarder's drain ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Drain {
    /// The forwarder finished: the process's output stream closed and every
    /// line was passed on.
    Complete,
    /// The stream yielded nothing for [`DRAIN_IDLE_LIMIT`] (or the limit
    /// given) while the forwarder waited on it, after the process was gone:
    /// the forwarder was stopped, and the output it passed on may be
    /// incomplete.
    Silent,
}

/// What a forwarding task reports to its drain.
#[derive(Default)]
struct Activity {
    /// Bumped when a line is received from the stream and again when
    /// `forward` returns for it: any movement at all.
    events:  AtomicU64,
    /// The task is waiting on the stream, not on its consumer.
    waiting: AtomicBool,
}

/// A spawned task forwarding one process's output stream, with what it is
/// doing.
#[must_use = "call `finish` once the process is gone, so the forwarded output is drained"]
pub struct Forwarder {
    task:     JoinHandle<()>,
    activity: Arc<Activity>,
}

impl Forwarder {
    /// Spawn a task that receives each line of `stream` and hands it to
    /// `forward`, until the stream closes or `forward` breaks. The task
    /// records each receive and each hand-off, and whether it is waiting on
    /// the stream, which is what lets [`Self::finish`] tell a slow consumer
    /// from a silent stream.
    pub fn spawn<T, F, Fut>(mut stream: mpsc::Receiver<T>, mut forward: F) -> Self
    where
        T: Send + 'static,
        F: FnMut(T) -> Fut + Send + 'static,
        Fut: Future<Output = ControlFlow<()>> + Send + 'static,
    {
        let activity = Arc::new(Activity::default());
        let report = Arc::clone(&activity);
        let task = tokio::spawn(async move {
            loop {
                report.waiting.store(true, Ordering::SeqCst);
                let Some(item) = stream.recv().await else {
                    return;
                };
                report.waiting.store(false, Ordering::SeqCst);
                report.events.fetch_add(1, Ordering::SeqCst);
                let flow = forward(item).await;
                report.events.fetch_add(1, Ordering::SeqCst);
                if flow.is_break() {
                    return;
                }
            }
        });
        Self { task, activity }
    }

    /// Wait for the forwarder after its process is gone. Returns
    /// [`Drain::Complete`] when the task ends on its own, however long that
    /// takes while lines keep arriving or the consumer is still taking one;
    /// [`Drain::Silent`] once a whole `idle` passes with the task waiting on
    /// a stream that yields nothing, in which case the task is stopped first.
    pub async fn finish(mut self, idle: Duration) -> Drain {
        let mut seen = self.activity.events.load(Ordering::SeqCst);
        loop {
            if time::timeout(idle, &mut self.task).await.is_ok() {
                return Drain::Complete;
            }
            // `waiting` first: a line received between the two reads moves
            // `events`, so it is never mistaken for silence.
            let waiting = self.activity.waiting.load(Ordering::SeqCst);
            let now = self.activity.events.load(Ordering::SeqCst);
            if waiting && now == seen {
                self.task.abort();
                let _ = (&mut self.task).await;
                return Drain::Silent;
            }
            seen = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::time::Instant;

    use super::*;

    /// A forwarder that records every line it is handed, after `delay(line)`.
    fn recording(
        rx: mpsc::Receiver<u64>,
        delay: impl Fn(u64) -> Duration + Send + 'static,
    ) -> (Forwarder, Arc<Mutex<Vec<u64>>>) {
        let received = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&received);
        let forwarder = Forwarder::spawn(rx, move |n| {
            let sink = Arc::clone(&sink);
            let pause = delay(n);
            async move {
                time::sleep(pause).await;
                sink.lock().expect("not poisoned").push(n);
                ControlFlow::Continue(())
            }
        });
        (forwarder, received)
    }

    /// A stream of `total` lines, already queued and closed: the process
    /// that wrote them is gone.
    async fn exited_process_lines(total: u64) -> mpsc::Receiver<u64> {
        let (tx, rx) = mpsc::channel(usize::try_from(total).expect("fits"));
        for n in 1..=total {
            tx.send(n).await.expect("room for every line");
        }
        rx
    }

    /// Modelled on sandbox-driver's
    /// `a_slow_consumer_receives_every_byte_after_the_process_exits`: the
    /// stream already holds every line when the drain starts, the consumer
    /// takes far longer than the idle limit to absorb them all, and none is
    /// lost.
    #[tokio::test(start_paused = true)]
    async fn a_slow_consumer_receives_every_line_after_the_process_exits() {
        let idle = Duration::from_millis(100);
        let total: u64 = 40;
        // Each line takes a fifth of the idle limit; the whole stream takes
        // eight times the limit.
        let (forwarder, received) = recording(exited_process_lines(total).await, |_| {
            Duration::from_millis(20)
        });
        let started = Instant::now();
        assert_eq!(forwarder.finish(idle).await, Drain::Complete);
        assert!(started.elapsed() > idle * 4, "the drain outlived the limit");
        let received = received.lock().expect("not poisoned").clone();
        assert_eq!(received, (1..=total).collect::<Vec<_>>());
    }

    /// The consumer holds ONE line for longer than the idle limit after the
    /// process is gone: the forwarder is waiting on its consumer, not on a
    /// silent stream, so the drain keeps going and no line is lost. This is
    /// a loaded run whose log consumer stalls on one record; it must not
    /// cost the output's tail.
    #[tokio::test(start_paused = true)]
    async fn a_consumer_stalled_past_the_limit_on_one_line_loses_no_line() {
        let idle = Duration::from_millis(100);
        let total: u64 = 3;
        let (forwarder, received) = recording(exited_process_lines(total).await, move |n| {
            if n == 1 { idle * 3 } else { Duration::ZERO }
        });
        assert_eq!(forwarder.finish(idle).await, Drain::Complete);
        let received = received.lock().expect("not poisoned").clone();
        assert_eq!(received, (1..=total).collect::<Vec<_>>());
    }

    /// A stream held open with nothing arriving ends the drain after the
    /// idle limit and reports it.
    #[tokio::test(start_paused = true)]
    async fn a_silent_open_stream_ends_the_drain_after_the_limit() {
        let idle = Duration::from_millis(100);
        let (tx, rx) = mpsc::channel::<u64>(1);
        let (forwarder, _) = recording(rx, |_| Duration::ZERO);
        let started = Instant::now();
        assert_eq!(forwarder.finish(idle).await, Drain::Silent);
        let elapsed = started.elapsed();
        assert!(elapsed >= idle && elapsed < idle * 10, "{elapsed:?}");
        // The sender outlived the drain: the stream really was still open.
        drop(tx);
    }

    /// A stream that goes quiet after forwarding some lines is drained for
    /// one more idle period after the last line, then reported silent.
    #[tokio::test(start_paused = true)]
    async fn output_then_silence_re_arms_once_and_then_ends() {
        let idle = Duration::from_millis(100);
        let (tx, rx) = mpsc::channel::<u64>(4);
        let (forwarder, received) = recording(rx, |_| Duration::ZERO);
        let feeder = tokio::spawn(async move {
            time::sleep(Duration::from_millis(60)).await;
            let _ = tx.send(1).await;
            // Keep the stream open, silently.
            time::sleep(Duration::from_secs(5)).await;
            drop(tx);
        });
        let started = Instant::now();
        assert_eq!(forwarder.finish(idle).await, Drain::Silent);
        let elapsed = started.elapsed();
        assert!(elapsed >= idle * 2 && elapsed < idle * 10, "{elapsed:?}");
        assert_eq!(*received.lock().expect("not poisoned"), vec![1]);
        feeder.abort();
    }
}
