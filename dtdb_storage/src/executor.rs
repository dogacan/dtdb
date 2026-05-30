//! A central executor for background work.
//!
//! Historically every component that needed background work spawned its own
//! dedicated OS thread (WAL sync loops, compaction loops, periodic ANALYZE,
//! API-layer flush timers). That made thread count scale with the number of
//! engines/tables and left shutdown to a scattered mix of weak-`Arc` upgrades,
//! `mpsc` disconnects, and signal flags.
//!
//! This module replaces that with a single shared [`Executor`] exposing two
//! primitives:
//!
//! * [`Executor::submit`] — run a one-shot task, with a [`Priority`] and an
//!   optional [`CoalesceKey`] that deduplicates redundant work (e.g. only one
//!   compaction per engine runs at a time, with a single trailing re-run if
//!   more work arrived while it was running).
//! * [`Executor::submit_periodic`] — register recurring work. A single
//!   scheduler thread tracks deadlines and *submits* the work into the same
//!   queue when due, so periodic tasks never occupy a worker while idle.
//!
//! The production implementation, [`ThreadPoolExecutor`], is backed by a fixed
//! pool of worker threads (the count is configurable, with a documented safe
//! floor). Dropping the executor is the single, deterministic shutdown path:
//! the scheduler stops, the queue drains, and every thread is joined.

use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// A one-shot unit of work.
pub type Task = Box<dyn FnOnce() + Send + 'static>;

/// Relative scheduling priority. Higher-priority work is dequeued first;
/// within a priority, work runs in submission order (FIFO).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    /// Background maintenance that can wait (e.g. ANALYZE).
    Low,
    /// Default.
    Normal,
    /// Latency-sensitive work (e.g. flushes, compaction) that should jump the queue.
    High,
}

/// An identity used to deduplicate work submitted via [`Executor::submit`].
///
/// While a task with a given key is queued or running, further submits with the
/// same key do not enqueue another copy. Instead the most recently submitted
/// task is remembered and run exactly once after the in-flight one completes.
/// This reproduces the old compaction "run again if more work appeared while we
/// were compacting" behavior without unbounded stacking.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CoalesceKey(Arc<str>);

impl CoalesceKey {
    pub fn new(key: impl Into<String>) -> Self {
        CoalesceKey(Arc::from(key.into().into_boxed_str()))
    }
}

/// The two scheduling primitives offered to clients.
pub trait Executor: Send + Sync + 'static {
    /// Submit a one-shot task. If `key` is `Some`, the task coalesces with any
    /// in-flight task sharing that key (see [`CoalesceKey`]).
    fn submit(&self, priority: Priority, key: Option<CoalesceKey>, task: Task);

    /// Register recurring work. The returned [`PeriodicHandle`] cancels the
    /// schedule when dropped, so lifecycle follows ownership.
    fn submit_periodic(
        &self,
        every: Duration,
        priority: Priority,
        task: Box<dyn Fn() + Send + Sync + 'static>,
    ) -> PeriodicHandle;
}

/// Cancels a registered periodic task when dropped (or via [`PeriodicHandle::cancel`]).
pub struct PeriodicHandle {
    cancelled: Arc<AtomicBool>,
    waker: Option<Weak<Scheduler>>,
}

impl PeriodicHandle {
    /// Stop the periodic task. Idempotent; also called on drop.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(weak) = &self.waker
            && let Some(scheduler) = weak.upgrade()
        {
            // Wake the scheduler so the cancelled entry is dropped promptly.
            scheduler.cv.notify_all();
        }
    }
}

impl Drop for PeriodicHandle {
    fn drop(&mut self) {
        self.cancel();
    }
}

// ---------------------------------------------------------------------------
// Work queue
// ---------------------------------------------------------------------------

struct QueuedTask {
    priority: Priority,
    /// Monotonic submission sequence, used to break ties so equal-priority work
    /// is strictly FIFO (and never starves).
    seq: u64,
    key: Option<CoalesceKey>,
    task: Task,
}

impl PartialEq for QueuedTask {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq
    }
}
impl Eq for QueuedTask {}

impl Ord for QueuedTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // `BinaryHeap` is a max-heap: highest priority first, then *smaller*
        // seq first (so reverse the seq comparison).
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for QueuedTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Per-coalesce-key bookkeeping. The key's presence in the map means a task is
/// queued or running; `pending` holds the latest task to run once the current
/// one finishes.
struct CoalesceSlot {
    running: bool,
    pending: Option<Task>,
}

struct QueueState {
    heap: BinaryHeap<QueuedTask>,
    coalesce: HashMap<CoalesceKey, CoalesceSlot>,
    seq: u64,
    shutting_down: bool,
}

struct PoolShared {
    state: Mutex<QueueState>,
    cv: Condvar,
}

impl PoolShared {
    fn submit(&self, priority: Priority, key: Option<CoalesceKey>, task: Task) {
        let mut st = self.state.lock().unwrap();
        if st.shutting_down {
            return;
        }
        if let Some(k) = &key {
            if let Some(slot) = st.coalesce.get_mut(k) {
                // Already queued or running: keep only the most recent task and
                // let it run once the in-flight one finishes.
                slot.pending = Some(task);
                return;
            }
            st.coalesce.insert(
                k.clone(),
                CoalesceSlot {
                    running: false,
                    pending: None,
                },
            );
        }
        let seq = st.seq;
        st.seq += 1;
        st.heap.push(QueuedTask {
            priority,
            seq,
            key,
            task,
        });
        drop(st);
        self.cv.notify_one();
    }

    /// Enqueue a single periodic tick. Unlike [`PoolShared::submit`], overlapping
    /// ticks are *dropped* (not queued) while the previous one is still queued or
    /// running, so a task slower than its interval can't pile up.
    fn submit_periodic_tick(
        &self,
        priority: Priority,
        key: CoalesceKey,
        task: Arc<dyn Fn() + Send + Sync + 'static>,
    ) {
        let mut st = self.state.lock().unwrap();
        if st.shutting_down || st.coalesce.contains_key(&key) {
            return;
        }
        st.coalesce.insert(
            key.clone(),
            CoalesceSlot {
                running: false,
                pending: None,
            },
        );
        let seq = st.seq;
        st.seq += 1;
        st.heap.push(QueuedTask {
            priority,
            seq,
            key: Some(key),
            task: Box::new(move || task()),
        });
        drop(st);
        self.cv.notify_one();
    }
}

fn worker_loop(shared: Arc<PoolShared>) {
    loop {
        // Dequeue the next task (or exit once the queue is drained at shutdown).
        let queued = {
            let mut st = shared.state.lock().unwrap();
            loop {
                if let Some(qt) = st.heap.pop() {
                    if let Some(k) = &qt.key
                        && let Some(slot) = st.coalesce.get_mut(k)
                    {
                        slot.running = true;
                    }
                    break qt;
                }
                if st.shutting_down {
                    return;
                }
                st = shared.cv.wait(st).unwrap();
            }
        };

        // Run the task without holding the queue lock.
        (queued.task)();

        // Coalescing follow-up: re-enqueue the latest pending task, if any.
        if let Some(k) = queued.key {
            let mut st = shared.state.lock().unwrap();
            if let Some(slot) = st.coalesce.get_mut(&k) {
                if let Some(pending) = slot.pending.take() {
                    slot.running = false;
                    let seq = st.seq;
                    st.seq += 1;
                    st.heap.push(QueuedTask {
                        priority: queued.priority,
                        seq,
                        key: Some(k),
                        task: pending,
                    });
                    drop(st);
                    shared.cv.notify_one();
                } else {
                    st.coalesce.remove(&k);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Periodic scheduler
// ---------------------------------------------------------------------------

struct ScheduledTick {
    next_fire: Instant,
    interval: Duration,
    priority: Priority,
    id: u64,
    cancelled: Arc<AtomicBool>,
    task: Arc<dyn Fn() + Send + Sync + 'static>,
}

impl PartialEq for ScheduledTick {
    fn eq(&self, other: &Self) -> bool {
        self.next_fire == other.next_fire && self.id == other.id
    }
}
impl Eq for ScheduledTick {}

impl Ord for ScheduledTick {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // `BinaryHeap` is a max-heap; we want the *earliest* deadline on top, so
        // reverse the comparison.
        other
            .next_fire
            .cmp(&self.next_fire)
            .then_with(|| other.id.cmp(&self.id))
    }
}
impl PartialOrd for ScheduledTick {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct SchedulerState {
    heap: BinaryHeap<ScheduledTick>,
    next_id: u64,
    shutting_down: bool,
}

struct Scheduler {
    pool: Arc<PoolShared>,
    state: Mutex<SchedulerState>,
    cv: Condvar,
}

impl Scheduler {
    fn add(
        self: &Arc<Self>,
        every: Duration,
        priority: Priority,
        task: Box<dyn Fn() + Send + Sync + 'static>,
    ) -> PeriodicHandle {
        let cancelled = Arc::new(AtomicBool::new(false));
        let task: Arc<dyn Fn() + Send + Sync + 'static> = Arc::from(task);
        {
            let mut st = self.state.lock().unwrap();
            let id = st.next_id;
            st.next_id += 1;
            st.heap.push(ScheduledTick {
                next_fire: Instant::now() + every,
                interval: every,
                priority,
                id,
                cancelled: cancelled.clone(),
                task,
            });
        }
        self.cv.notify_all();
        PeriodicHandle {
            cancelled,
            waker: Some(Arc::downgrade(self)),
        }
    }
}

fn scheduler_loop(scheduler: Arc<Scheduler>) {
    loop {
        let mut st = scheduler.state.lock().unwrap();
        if st.shutting_down {
            return;
        }

        // Discard cancelled ticks sitting at the top of the heap.
        while let Some(top) = st.heap.peek() {
            if top.cancelled.load(Ordering::SeqCst) {
                st.heap.pop();
            } else {
                break;
            }
        }

        let now = Instant::now();
        match st.heap.peek().map(|t| t.next_fire) {
            None => {
                let _guard = scheduler.cv.wait(st).unwrap();
            }
            Some(next_fire) if next_fire <= now => {
                let mut tick = st.heap.pop().unwrap();
                if tick.cancelled.load(Ordering::SeqCst) {
                    continue;
                }
                let priority = tick.priority;
                let key = CoalesceKey::new(format!("periodic:{}", tick.id));
                let task = tick.task.clone();
                // Reschedule from `now` so a slow run doesn't cause a backlog of
                // catch-up ticks.
                tick.next_fire = now + tick.interval;
                st.heap.push(tick);
                drop(st);
                scheduler.pool.submit_periodic_tick(priority, key, task);
            }
            Some(next_fire) => {
                let timeout = next_fire.saturating_duration_since(now);
                let _guard = scheduler.cv.wait_timeout(st, timeout).unwrap();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ThreadPoolExecutor
// ---------------------------------------------------------------------------

/// The smallest worker count we allow. A single worker lets one long-running
/// task (e.g. a large compaction) stall all latency-sensitive work, so we never
/// run below this floor regardless of configuration.
pub const MIN_WORKER_THREADS: usize = 2;

/// Configuration for [`ThreadPoolExecutor`].
#[derive(Clone, Copy, Debug)]
pub struct ExecutorConfig {
    /// Number of worker threads. Values below [`MIN_WORKER_THREADS`] are clamped
    /// up (with a warning) rather than honored.
    pub worker_threads: usize,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        let n = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(MIN_WORKER_THREADS);
        ExecutorConfig {
            worker_threads: n.max(MIN_WORKER_THREADS),
        }
    }
}

/// A shared, bounded thread pool implementing [`Executor`].
pub struct ThreadPoolExecutor {
    shared: Arc<PoolShared>,
    scheduler: Arc<Scheduler>,
    workers: Vec<JoinHandle<()>>,
    scheduler_thread: Option<JoinHandle<()>>,
}

impl ThreadPoolExecutor {
    pub fn new(config: ExecutorConfig) -> Self {
        let worker_threads = if config.worker_threads < MIN_WORKER_THREADS {
            tracing::warn!(
                requested = config.worker_threads,
                floor = MIN_WORKER_THREADS,
                "executor worker_threads below safe floor; clamping up to avoid \
                 starving latency-sensitive work"
            );
            MIN_WORKER_THREADS
        } else {
            config.worker_threads
        };

        let shared = Arc::new(PoolShared {
            state: Mutex::new(QueueState {
                heap: BinaryHeap::new(),
                coalesce: HashMap::new(),
                seq: 0,
                shutting_down: false,
            }),
            cv: Condvar::new(),
        });

        let scheduler = Arc::new(Scheduler {
            pool: shared.clone(),
            state: Mutex::new(SchedulerState {
                heap: BinaryHeap::new(),
                next_id: 0,
                shutting_down: false,
            }),
            cv: Condvar::new(),
        });

        let mut workers = Vec::with_capacity(worker_threads);
        for i in 0..worker_threads {
            let shared = shared.clone();
            workers.push(
                std::thread::Builder::new()
                    .name(format!("dtdb-worker-{i}"))
                    .spawn(move || worker_loop(shared))
                    .expect("failed to spawn executor worker thread"),
            );
        }

        let scheduler_thread = {
            let scheduler = scheduler.clone();
            std::thread::Builder::new()
                .name("dtdb-scheduler".to_string())
                .spawn(move || scheduler_loop(scheduler))
                .expect("failed to spawn executor scheduler thread")
        };

        Self {
            shared,
            scheduler,
            workers,
            scheduler_thread: Some(scheduler_thread),
        }
    }

    /// Construct a pool sized from [`ExecutorConfig::default`].
    pub fn with_default() -> Self {
        Self::new(ExecutorConfig::default())
    }
}

impl Executor for ThreadPoolExecutor {
    fn submit(&self, priority: Priority, key: Option<CoalesceKey>, task: Task) {
        self.shared.submit(priority, key, task);
    }

    fn submit_periodic(
        &self,
        every: Duration,
        priority: Priority,
        task: Box<dyn Fn() + Send + Sync + 'static>,
    ) -> PeriodicHandle {
        self.scheduler.add(every, priority, task)
    }
}

impl Drop for ThreadPoolExecutor {
    fn drop(&mut self) {
        // 1. Stop the scheduler first so no further ticks are submitted.
        self.scheduler.state.lock().unwrap().shutting_down = true;
        self.scheduler.cv.notify_all();
        if let Some(handle) = self.scheduler_thread.take() {
            let _ = handle.join();
        }

        // 2. Let workers drain the remaining queue, then exit, and join them.
        self.shared.state.lock().unwrap().shutting_down = true;
        self.shared.cv.notify_all();
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// InlineExecutor
// ---------------------------------------------------------------------------

/// A pool-less [`Executor`] that runs each task on its own freshly spawned
/// thread. Mirrors the historical "spawn a thread per request" behavior; handy
/// for tests that want work to run on independent threads without any pool
/// ordering or capacity semantics.
#[derive(Clone, Default)]
pub struct InlineExecutor;

impl Executor for InlineExecutor {
    fn submit(&self, _priority: Priority, _key: Option<CoalesceKey>, task: Task) {
        std::thread::spawn(task);
    }

    fn submit_periodic(
        &self,
        every: Duration,
        _priority: Priority,
        task: Box<dyn Fn() + Send + Sync + 'static>,
    ) -> PeriodicHandle {
        let cancelled = Arc::new(AtomicBool::new(false));
        let flag = cancelled.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(every);
                if flag.load(Ordering::SeqCst) {
                    break;
                }
                task();
            }
        });
        PeriodicHandle {
            cancelled,
            waker: None,
        }
    }
}
