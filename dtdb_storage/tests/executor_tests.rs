//! Tests for the central executor / thread pool.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Condvar, Mutex};
use std::time::Duration;

use dtdb_storage::executor::{CoalesceKey, ExecutorConfig};
use dtdb_storage::{Executor, Priority, ThreadPoolExecutor};

/// A small helper to collect an ordered log of values produced by tasks.
#[derive(Clone, Default)]
struct Recorder {
    inner: Arc<Mutex<Vec<u64>>>,
}
impl Recorder {
    fn push(&self, v: u64) {
        self.inner.lock().unwrap().push(v);
    }
    fn snapshot(&self) -> Vec<u64> {
        self.inner.lock().unwrap().clone()
    }
}

/// Blocks until `n` tasks have signalled completion.
struct Latch {
    state: Mutex<usize>,
    cv: Condvar,
}
impl Latch {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(0),
            cv: Condvar::new(),
        })
    }
    fn signal(&self) {
        *self.state.lock().unwrap() += 1;
        self.cv.notify_all();
    }
    fn wait_for(&self, n: usize) {
        let mut g = self.state.lock().unwrap();
        while *g < n {
            let (ng, timeout) = self
                .cv
                .wait_timeout(g, Duration::from_secs(5))
                .unwrap();
            g = ng;
            assert!(!timeout.timed_out(), "timed out waiting for {n} signals");
        }
    }
}

#[test]
fn runs_submitted_tasks() {
    let pool = ThreadPoolExecutor::with_default();
    let counter = Arc::new(AtomicUsize::new(0));
    let latch = Latch::new();

    for _ in 0..50 {
        let counter = counter.clone();
        let latch = latch.clone();
        pool.submit(
            Priority::Normal,
            None,
            Box::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                latch.signal();
            }),
        );
    }

    latch.wait_for(50);
    assert_eq!(counter.load(Ordering::SeqCst), 50);
}

#[test]
fn high_priority_runs_before_low() {
    // Single worker so the queue ordering is fully deterministic: while the
    // first task occupies the worker, we enqueue a Low then a High; the High
    // must come out first.
    let pool = ThreadPoolExecutor::new(ExecutorConfig { worker_threads: 1 });
    let rec = Recorder::default();
    let gate = Arc::new(Barrier::new(2));
    let done = Latch::new();

    // Occupy the worker until we've finished enqueuing the next two tasks.
    {
        let gate = gate.clone();
        pool.submit(
            Priority::Normal,
            None,
            Box::new(move || {
                gate.wait();
            }),
        );
    }

    {
        let rec = rec.clone();
        let done = done.clone();
        pool.submit(
            Priority::Low,
            None,
            Box::new(move || {
                rec.push(1);
                done.signal();
            }),
        );
    }
    {
        let rec = rec.clone();
        let done = done.clone();
        pool.submit(
            Priority::High,
            None,
            Box::new(move || {
                rec.push(2);
                done.signal();
            }),
        );
    }

    // Release the worker; now the two queued tasks drain in priority order.
    gate.wait();
    done.wait_for(2);
    assert_eq!(rec.snapshot(), vec![2, 1], "High priority should run first");
}

#[test]
fn coalescing_dedups_and_runs_trailing_request() {
    let pool = ThreadPoolExecutor::new(ExecutorConfig { worker_threads: 1 });
    let runs = Arc::new(AtomicUsize::new(0));
    let key = CoalesceKey::new("job");
    let started = Latch::new();
    let release = Arc::new(Barrier::new(2));
    let done = Latch::new();

    // First submission signals that it has started, then blocks (still running,
    // holding the single worker) until the test releases it.
    {
        let runs = runs.clone();
        let started = started.clone();
        let release = release.clone();
        let done = done.clone();
        pool.submit(
            Priority::Normal,
            Some(key.clone()),
            Box::new(move || {
                runs.fetch_add(1, Ordering::SeqCst);
                started.signal();
                release.wait();
                done.signal();
            }),
        );
    }
    // Wait until the first task is actually running.
    started.wait_for(1);

    // While it runs, fire several more submissions with the same key. These
    // must collapse into a single trailing re-run, not five separate runs.
    for _ in 0..5 {
        let runs = runs.clone();
        let done = done.clone();
        pool.submit(
            Priority::Normal,
            Some(key.clone()),
            Box::new(move || {
                runs.fetch_add(1, Ordering::SeqCst);
                done.signal();
            }),
        );
    }

    // Release the first task; exactly one trailing run should follow.
    release.wait();
    done.wait_for(2);
    // Give any (erroneous) extra runs a chance to show up.
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "coalesced submissions should produce one in-flight + one trailing run"
    );
}

#[test]
fn periodic_fires_and_is_cancelled_on_handle_drop() {
    let pool = ThreadPoolExecutor::with_default();
    let ticks = Arc::new(AtomicUsize::new(0));

    let handle = {
        let ticks = ticks.clone();
        pool.submit_periodic(
            Duration::from_millis(20),
            Priority::Normal,
            Box::new(move || {
                ticks.fetch_add(1, Ordering::SeqCst);
            }),
        )
    };

    // Let it fire a few times.
    std::thread::sleep(Duration::from_millis(150));
    let observed = ticks.load(Ordering::SeqCst);
    assert!(observed >= 2, "expected several ticks, got {observed}");

    // Dropping the handle stops further ticks.
    drop(handle);
    std::thread::sleep(Duration::from_millis(60));
    let after_cancel = ticks.load(Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(80));
    assert_eq!(
        ticks.load(Ordering::SeqCst),
        after_cancel,
        "no ticks should fire after the handle is dropped"
    );
}

#[test]
fn worker_count_is_clamped_to_floor() {
    // Below-floor configuration must not panic and must still run work.
    let pool = ThreadPoolExecutor::new(ExecutorConfig { worker_threads: 0 });
    let latch = Latch::new();
    {
        let latch = latch.clone();
        pool.submit(Priority::Normal, None, Box::new(move || latch.signal()));
    }
    latch.wait_for(1);
}

#[test]
fn shutdown_joins_cleanly_and_drains() {
    let counter = Arc::new(AtomicUsize::new(0));
    {
        let pool = ThreadPoolExecutor::with_default();
        for _ in 0..200 {
            let counter = counter.clone();
            pool.submit(
                Priority::Normal,
                None,
                Box::new(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                }),
            );
        }
        // Dropping the pool drains the queue and joins all threads.
    }
    assert_eq!(
        counter.load(Ordering::SeqCst),
        200,
        "all queued work should run before shutdown completes"
    );
}
