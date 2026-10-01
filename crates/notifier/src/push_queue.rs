//! Bounded worker pool for Web Push sends (Repeater Signal M2 / services
//! review SVC-02).
//!
//! # Why
//!
//! Every send used to run inline in `main`'s single `select!` loop. A
//! timed-out attempt is no longer retried and a user's subscriptions are
//! sent concurrently, but a user who triggered many notifications still
//! cost up to `PUSH_SEND_TIMEOUT` (15 s) per notification, one after
//! another, and that blocked all four cycles for every other user. Now the
//! cycles only DECIDE; they hand each notification to [`PushQueue::enqueue`],
//! which never blocks and never awaits, and a fixed pool of workers sends.
//!
//! # Shape
//!
//! * A bounded queue (`push_queue_capacity` jobs) drained by a fixed number
//!   of workers (`push_workers`).
//! * A per-user in-flight cap (`push_per_user_in_flight`): at most that many
//!   of one user's jobs are being sent at once, however many they have
//!   queued, so one user (say, with 20 tarpit endpoints and many pinned
//!   lines) can hold at most that many workers. The rest of their jobs wait
//!   in a per-user FIFO while other users' jobs are dispatched round-robin.
//! * A per-user queued cap (`push_per_user_queued`) so one user can't fill
//!   the shared queue either.
//! * One job = one notification to one user = a concurrent fan-out to all
//!   of that user's subscriptions, each bounded by `SUBSCRIPTION_BUDGET` on
//!   top of `send.rs`'s own per-attempt timeout.
//!
//! # When the queue is full: drop, with a metric
//!
//! [`PushQueue::enqueue`] returns [`EnqueueOutcome::Dropped`] and
//! increments `distant_signal_notifier_push_dropped_total{reason}` (see
//! "The dropped-send metric" below for every reason). Nothing
//! is written for a dropped job, so it is exactly as if the send had failed
//! transiently: the notification is re-decided if and when its source row
//! is polled again (see "Delivery semantics"). Deferring instead (an
//! unbounded overflow list, or blocking the loop) would reintroduce the
//! unbounded memory or the stalled loop this module exists to remove.
//!
//! # Delivery semantics (DB2-25 made explicit)
//!
//! 1. **The "notification sent" bookkeeping (`*_notification_state`) is
//!    written only by a worker, only after the job was delivered**: at least
//!    one subscription answered with success, or the user has no
//!    subscriptions at all (unchanged from before: that counts as handled,
//!    so a later subscription doesn't get a backlog of stale pushes). A
//!    queued, in-flight, dropped or failed job never writes it.
//! 2. **Cursors advance independently of delivery**, exactly as before this
//!    module existed (`queries::advance_cursor_with_grace`). Line and train
//!    rows are re-read for the cursor's grace window (about two cycles)
//!    after they first appear; a notification whose job fails (or is
//!    dropped) during that window is re-decided and re-enqueued, one that
//!    fails after it is lost. That is the DB2-25 behaviour, now deliberate
//!    and tested: at-most-once-ish, bounded retries, no outbox (an outbox
//!    table needs a migration, out of scope here). The skip-check and
//!    unmatched-leg cycles are full polls, not cursors, so theirs are
//!    re-decided on every cycle until one is delivered.
//! 3. **No double send from re-reads.** While a job is queued or in flight
//!    its state row is not written yet, so a grace-window re-read decides
//!    `NotifyNow` again. Jobs are keyed by (user, target) -- a line, a
//!    tracked train, a journey leg's skip or unmatched notice -- and an
//!    enqueue whose key AND recorded state match a queued or in-flight job
//!    is [`EnqueueOutcome::Duplicate`] and does nothing.
//! 4. **Latest wins for a changed state.** An enqueue for the same key but a
//!    DIFFERENT state (the line moved on again, the delay changed) replaces
//!    the queued job, or is parked behind the in-flight one and sent after
//!    it, never concurrently with it, so the state rows are written in
//!    decision order. Both share a notification `tag`, so on the device the
//!    newer one replaces the older.
//! 5. **No re-sending forever.** Besides the grace window above, an endpoint
//!    that times out `push_prune_after_timeouts` times with no successful
//!    delivery in between is deleted, like a 404/410/401 one already is
//!    (SVC-02). A user left with no subscriptions is then "handled" and
//!    their state row is written, which stops the full-poll cycles
//!    re-deciding. The timeout counts live in memory (no migration) and
//!    reset on restart, so a tarpit gets at most that many more strikes per
//!    restart.
//! 6. **A failed bookkeeping write after a successful send** is retried a
//!    few times in the worker. If it still fails, a grace-window re-read may
//!    send the notification again (the other half of DB2-25).
//!
//! **The guarantee is at-most-once, by decision** (open-findings triage
//! DQ9, 2026-09-27): no outbox and no pending-send marker. At about 21
//! subscriptions a lost push is cheaper than the machinery to prevent it;
//! revisit (a `last_attempted_at` marker re-decided until it expires) if
//! push volume grows. What is lost is counted instead.
//!
//! # The dropped-send metric
//!
//! `distant_signal_notifier_push_dropped_total{reason}` counts every decided
//! notification that ended without being delivered, and so may never be:
//!
//! * `queue_full`, `user_queue_full`, `shutting_down`: refused at enqueue.
//! * `abandoned_on_shutdown`: still queued or in flight when the shutdown
//!   grace ran out.
//! * `send_failed`: sent, but no subscription accepted it (all timed out or
//!   failed transiently).
//! * `error`: the job could not run (loading subscriptions failed, or it
//!   panicked).
//!
//! A cursor-fed notification counted here is re-decided if its source row
//! is re-read inside the grace window, so one count is not always one lost
//! push; a steady rate is. The chart's `DistantSignalNotifierPushDropped`
//! alert fires on it. Every reason is registered at 0 when the queue
//! starts, so `increase()` sees the first drop.
//!
//! # Shutdown
//!
//! [`PushQueue::shutdown`] stops accepting jobs, lets the workers drain what
//! is queued for up to `push_shutdown_grace_secs`, then aborts whatever is
//! left. Abandoned jobs are counted (`reason="abandoned_on_shutdown"`) and
//! behave like dropped ones.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::queries::{self, PushSubscriptionRow};
use crate::send::{NotificationPayload, Pusher, SendOutcome};

/// Upper bound on one subscription's share of a job, on top of `send.rs`'s
/// own `PUSH_SEND_TIMEOUT` per attempt: it also covers the L9 send-time DNS
/// re-validation (`lookup_host` has no timeout of its own, SVC-03) and the
/// two retries a fast-failing (5xx) endpoint still gets. Reported as
/// [`SendOutcome::TimedOut`].
const SUBSCRIPTION_BUDGET: Duration = Duration::from_secs(30);

/// Attempts at the bookkeeping write after a delivered job (point 6 of the
/// module doc).
const BOOKKEEPING_ATTEMPTS: u32 = 3;

/// What a delivered job records -- and so also what identifies it: the
/// variant plus its id is the target, the whole value is the state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Delivered {
    /// `line_notification_state` at this rank.
    Line { line_id: String, rank: u8 },
    /// `train_notification_state` at this status and delay.
    Train {
        tracked_train_id: i64,
        status: String,
        delay_minutes: Option<i32>,
    },
    /// `journey_leg_notification_state.last_notified_skipped = TRUE`.
    SkippedStop { journey_leg_id: i64 },
    /// `journey_leg_notification_state.last_notified_unmatched = TRUE`.
    Unmatched { journey_leg_id: i64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Target {
    Line(String),
    Train(i64),
    SkippedStop(i64),
    Unmatched(i64),
}

impl Delivered {
    fn target(&self) -> Target {
        match self {
            Delivered::Line { line_id, .. } => Target::Line(line_id.clone()),
            Delivered::Train {
                tracked_train_id, ..
            } => Target::Train(*tracked_train_id),
            Delivered::SkippedStop { journey_leg_id } => Target::SkippedStop(*journey_leg_id),
            Delivered::Unmatched { journey_leg_id } => Target::Unmatched(*journey_leg_id),
        }
    }
}

/// One notification for one user.
#[derive(Debug)]
pub(crate) struct PushJob {
    pub user_id: String,
    pub payload: NotificationPayload,
    pub delivered: Delivered,
    /// The cycle's `now` when it decided to notify; written as
    /// `last_notified_at` so cooldowns are measured from the decision, as
    /// they were when sends were synchronous.
    pub decided_at: DateTime<Utc>,
}

type JobKey = (String, Target);

impl PushJob {
    fn key(&self) -> JobKey {
        (self.user_id.clone(), self.delivered.target())
    }
}

/// What [`PushQueue::enqueue`] did with a job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EnqueueOutcome {
    /// Accepted; a worker will send it.
    Queued,
    /// The same notification is already queued or in flight; nothing done.
    Duplicate,
    /// Replaced a queued (not yet started) job for the same target.
    Superseded,
    /// Not accepted; nothing will be sent or recorded for it.
    Dropped(DropReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DropReason {
    QueueFull,
    UserQueueFull,
    ShuttingDown,
}

impl DropReason {
    fn label(self) -> &'static str {
        match self {
            DropReason::QueueFull => "queue_full",
            DropReason::UserQueueFull => "user_queue_full",
            DropReason::ShuttingDown => "shutting_down",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PushQueueConfig {
    pub workers: usize,
    pub capacity: usize,
    pub per_user_in_flight: usize,
    pub per_user_queued: usize,
    pub prune_after_timeouts: u32,
}

/// The side effects a worker needs, abstracted so the scheduling, caps and
/// pruning can be tested without a database. [`PgBackend`] is production.
pub(crate) trait PushBackend: Send + Sync + 'static {
    fn subscriptions(
        &self,
        user_id: &str,
    ) -> impl Future<Output = anyhow::Result<Vec<PushSubscriptionRow>>> + Send;
    fn send(
        &self,
        subscription: &PushSubscriptionRow,
        payload: &NotificationPayload,
    ) -> impl Future<Output = SendOutcome> + Send;
    fn delete_subscription(&self, id: i64) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn record_delivered(
        &self,
        user_id: &str,
        delivered: &Delivered,
        at: DateTime<Utc>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}

/// Production backend: Postgres for subscriptions and bookkeeping, and
/// [`Pusher`] (the L9 public-only reqwest client) for the sends.
pub(crate) struct PgBackend {
    pool: sqlx::PgPool,
    pusher: Pusher,
}

impl PgBackend {
    pub(crate) fn new(pool: sqlx::PgPool, pusher: Pusher) -> Self {
        Self { pool, pusher }
    }
}

impl PushBackend for PgBackend {
    async fn subscriptions(&self, user_id: &str) -> anyhow::Result<Vec<PushSubscriptionRow>> {
        queries::push_subscriptions_for_user(&self.pool, user_id).await
    }

    async fn send(
        &self,
        subscription: &PushSubscriptionRow,
        payload: &NotificationPayload,
    ) -> SendOutcome {
        self.pusher.send(subscription, payload).await
    }

    async fn delete_subscription(&self, id: i64) -> anyhow::Result<()> {
        queries::delete_push_subscription(&self.pool, id).await
    }

    async fn record_delivered(
        &self,
        user_id: &str,
        delivered: &Delivered,
        at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        match delivered {
            Delivered::Line { line_id, rank } => {
                queries::upsert_line_notification_state(&self.pool, user_id, line_id, *rank, at)
                    .await
            }
            Delivered::Train {
                tracked_train_id,
                status,
                delay_minutes,
            } => {
                queries::upsert_train_notification_state(
                    &self.pool,
                    user_id,
                    *tracked_train_id,
                    status,
                    *delay_minutes,
                    at,
                )
                .await
            }
            Delivered::SkippedStop { journey_leg_id } => {
                queries::upsert_skip_notification_state(
                    &self.pool,
                    user_id,
                    *journey_leg_id,
                    true,
                    at,
                )
                .await
            }
            Delivered::Unmatched { journey_leg_id } => {
                queries::upsert_unmatched_notification_state(
                    &self.pool,
                    user_id,
                    *journey_leg_id,
                    at,
                )
                .await
            }
        }
    }
}

enum Slot {
    Queued(PushJob),
    InFlight {
        delivered: Delivered,
        /// A newer job for the same key, sent once this one finishes.
        next: Option<PushJob>,
    },
}

#[derive(Default)]
struct UserQueue {
    /// Keys whose slot is `Queued`, in arrival order.
    ready: VecDeque<JobKey>,
    /// Jobs counted against `per_user_queued`: `ready` plus parked `next`s.
    queued: usize,
    in_flight: usize,
    in_runnable: bool,
}

#[derive(Default)]
struct State {
    closed: bool,
    slots: HashMap<JobKey, Slot>,
    users: HashMap<String, UserQueue>,
    /// Users with a ready job and room under the in-flight cap, served
    /// round-robin.
    runnable: VecDeque<String>,
    queued: usize,
    in_flight: usize,
    /// The most jobs any one user has had in flight at once, per user: lets
    /// tests prove the per-user cap held over a whole run without sampling.
    #[cfg(test)]
    peak_user_in_flight: HashMap<String, usize>,
}

struct Shared<B> {
    backend: B,
    config: PushQueueConfig,
    state: Mutex<State>,
    /// Wakes idle workers when a job becomes runnable (or on close).
    work: Notify,
    /// Wakes `wait_idle` callers.
    idle: Notify,
    /// Consecutive timeouts per subscription id since its last success.
    timeouts: Mutex<HashMap<i64, u32>>,
}

pub(crate) struct PushQueue<B: PushBackend> {
    shared: Arc<Shared<B>>,
    workers: Vec<JoinHandle<()>>,
}

/// What [`PushQueue::shutdown`] left behind.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ShutdownReport {
    pub drained: bool,
    pub abandoned: usize,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic while holding the lock can only come from a bug in this
    // module's bookkeeping; the counters are still the best information
    // there is, so keep going rather than poisoning every later send.
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn metric(suffix: &str) -> String {
    common::metrics::metric_name(suffix)
}

impl<B: PushBackend> PushQueue<B> {
    /// Starts `config.workers` worker tasks on the current runtime.
    pub(crate) fn start(backend: B, config: PushQueueConfig) -> Self {
        let shared = Arc::new(Shared {
            backend,
            config,
            state: Mutex::new(State::default()),
            work: Notify::new(),
            idle: Notify::new(),
            timeouts: Mutex::new(HashMap::new()),
        });
        publish_gauges(&lock(&shared.state));
        for reason in DROPPED_REASONS {
            metrics::counter!(metric("notifier_push_dropped_total"), "reason" => *reason)
                .increment(0);
        }
        let workers = (0..config.workers.max(1))
            .map(|_| tokio::spawn(worker(Arc::clone(&shared))))
            .collect();
        Self { shared, workers }
    }

    /// Hands a notification to the workers. Never blocks and never awaits:
    /// safe to call from the main loop whatever the push endpoints are
    /// doing. See the module doc for what each outcome means.
    pub(crate) fn enqueue(&self, job: PushJob) -> EnqueueOutcome {
        let outcome = self.enqueue_inner(job);
        match outcome {
            EnqueueOutcome::Dropped(reason) => {
                tracing::warn!(
                    reason = reason.label(),
                    "push queue refused a notification; it is not sent and not recorded, so it \
                     is re-decided only if its source row is polled again"
                );
                metrics::counter!(metric("notifier_push_dropped_total"), "reason" => reason.label())
                    .increment(1);
            }
            EnqueueOutcome::Queued => {
                metrics::counter!(metric("notifier_push_enqueue_total"), "result" => "queued")
                    .increment(1);
            }
            EnqueueOutcome::Duplicate => {
                metrics::counter!(metric("notifier_push_enqueue_total"), "result" => "duplicate")
                    .increment(1);
            }
            EnqueueOutcome::Superseded => {
                metrics::counter!(metric("notifier_push_enqueue_total"), "result" => "superseded")
                    .increment(1);
            }
        }
        outcome
    }

    #[expect(
        clippy::expect_used,
        reason = "the invariant is established just above; the expect message names it"
    )]
    fn enqueue_inner(&self, job: PushJob) -> EnqueueOutcome {
        let config = self.shared.config;
        let key = job.key();
        let mut st = lock(&self.shared.state);
        if st.closed {
            return EnqueueOutcome::Dropped(DropReason::ShuttingDown);
        }
        let full = |st: &State, user_id: &str| {
            if st.queued >= config.capacity {
                Some(DropReason::QueueFull)
            } else if st.users.get(user_id).map_or(0, |u| u.queued) >= config.per_user_queued {
                Some(DropReason::UserQueueFull)
            } else {
                None
            }
        };

        match st.slots.get_mut(&key) {
            Some(Slot::Queued(existing)) => {
                if existing.delivered == job.delivered {
                    return EnqueueOutcome::Duplicate;
                }
                *existing = job;
                return EnqueueOutcome::Superseded;
            }
            Some(Slot::InFlight {
                next: Some(parked), ..
            }) => {
                if parked.delivered == job.delivered {
                    return EnqueueOutcome::Duplicate;
                }
                // A newer state while an older one is already parked: the
                // parked one hasn't started, so replace it.
                *parked = job;
                return EnqueueOutcome::Superseded;
            }
            Some(Slot::InFlight {
                delivered,
                next: None,
            }) if *delivered == job.delivered => {
                return EnqueueOutcome::Duplicate;
            }
            // Absent, or in flight with a different state: queue it (the
            // latter parked behind the in-flight job, below).
            Some(Slot::InFlight { next: None, .. }) | None => {}
        }

        if let Some(reason) = full(&st, &job.user_id) {
            return EnqueueOutcome::Dropped(reason);
        }
        st.queued += 1;
        let user_id = job.user_id.clone();
        st.users.entry(user_id.clone()).or_default().queued += 1;
        if let Some(Slot::InFlight { next, .. }) = st.slots.get_mut(&key) {
            // Parked: `finish` moves it to the front of the user's
            // queue when the in-flight job for this key completes.
            *next = Some(job);
        } else {
            st.slots.insert(key.clone(), Slot::Queued(job));
            st.users
                .get_mut(&user_id)
                .expect("inserted above")
                .ready
                .push_back(key);
            make_runnable(&mut st, &user_id, config.per_user_in_flight);
            self.shared.work.notify_one();
        }
        publish_gauges(&st);
        EnqueueOutcome::Queued
    }

    /// Jobs waiting (including ones parked behind an in-flight job).
    #[cfg(test)]
    pub(crate) fn depth(&self) -> usize {
        lock(&self.shared.state).queued
    }

    /// Jobs being sent right now.
    #[cfg(test)]
    pub(crate) fn in_flight(&self) -> usize {
        lock(&self.shared.state).in_flight
    }

    /// The most jobs `user_id` has ever had in flight at once.
    #[cfg(test)]
    pub(crate) fn peak_in_flight(&self, user_id: &str) -> usize {
        lock(&self.shared.state)
            .peak_user_in_flight
            .get(user_id)
            .copied()
            .unwrap_or(0)
    }

    /// Resolves once nothing is queued or in flight.
    #[cfg(test)]
    pub(crate) async fn wait_idle(&self) {
        loop {
            let notified = self.shared.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let st = lock(&self.shared.state);
                if st.queued == 0 && st.in_flight == 0 {
                    return;
                }
            }
            notified.await;
        }
    }

    /// Stops accepting jobs, lets the workers drain the queue for up to
    /// `grace`, then aborts them. See the module doc.
    pub(crate) async fn shutdown(self, grace: Duration) -> ShutdownReport {
        lock(&self.shared.state).closed = true;
        self.shared.work.notify_waiters();
        let abort_handles: Vec<_> = self.workers.iter().map(JoinHandle::abort_handle).collect();
        let drained = tokio::time::timeout(grace, futures_util::future::join_all(self.workers))
            .await
            .is_ok();
        if !drained {
            for handle in abort_handles {
                handle.abort();
            }
        }
        let abandoned = {
            let st = lock(&self.shared.state);
            st.queued + st.in_flight
        };
        if abandoned > 0 {
            metrics::counter!(
                metric("notifier_push_dropped_total"),
                "reason" => "abandoned_on_shutdown"
            )
            .increment(abandoned as u64);
        }
        ShutdownReport { drained, abandoned }
    }
}

fn make_runnable(st: &mut State, user_id: &str, per_user_in_flight: usize) {
    let Some(user) = st.users.get_mut(user_id) else {
        return;
    };
    if !user.in_runnable && !user.ready.is_empty() && user.in_flight < per_user_in_flight {
        user.in_runnable = true;
        st.runnable.push_back(user_id.to_string());
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "metric gauges take f64, and these counts and timestamps stay far below 2^52"
)]
fn publish_gauges(st: &State) {
    metrics::gauge!(metric("notifier_push_queue_depth")).set(st.queued as f64);
    metrics::gauge!(metric("notifier_push_in_flight")).set(st.in_flight as f64);
}

/// Pops the next job, round-robin across users under their in-flight cap,
/// and marks it in flight.
fn take_next(st: &mut State, per_user_in_flight: usize) -> Option<(JobKey, PushJob)> {
    while let Some(user_id) = st.runnable.pop_front() {
        let Some(user) = st.users.get_mut(&user_id) else {
            continue;
        };
        user.in_runnable = false;
        if user.in_flight >= per_user_in_flight {
            continue;
        }
        let Some(key) = user.ready.pop_front() else {
            continue;
        };
        user.queued -= 1;
        user.in_flight += 1;
        #[cfg(test)]
        {
            let now = user.in_flight;
            let peak = st.peak_user_in_flight.entry(user_id.clone()).or_default();
            *peak = (*peak).max(now);
        }
        st.queued -= 1;
        st.in_flight += 1;
        let job = match st.slots.remove(&key) {
            Some(Slot::Queued(job)) => job,
            other => unreachable!(
                "a key in a user's ready queue always has a Queued slot (found in-flight: {})",
                matches!(other, Some(Slot::InFlight { .. }))
            ),
        };
        st.slots.insert(
            key.clone(),
            Slot::InFlight {
                delivered: job.delivered.clone(),
                next: None,
            },
        );
        make_runnable(st, &user_id, per_user_in_flight);
        publish_gauges(st);
        return Some((key, job));
    }
    None
}

/// Marks `key`'s in-flight job done; a parked successor becomes the next
/// job for that user.
fn finish<B: PushBackend>(shared: &Shared<B>, key: &JobKey) {
    let per_user_in_flight = shared.config.per_user_in_flight;
    let mut st = lock(&shared.state);
    st.in_flight -= 1;
    let user_id = key.0.clone();
    if let Some(user) = st.users.get_mut(&user_id) {
        user.in_flight -= 1;
    }
    if let Some(Slot::InFlight {
        next: Some(next), ..
    }) = st.slots.remove(key)
    {
        st.slots.insert(key.clone(), Slot::Queued(next));
        if let Some(user) = st.users.get_mut(&user_id) {
            user.ready.push_front(key.clone());
        }
    }
    make_runnable(&mut st, &user_id, per_user_in_flight);
    if st
        .users
        .get(&user_id)
        .is_some_and(|u| u.ready.is_empty() && u.queued == 0 && u.in_flight == 0 && !u.in_runnable)
    {
        st.users.remove(&user_id);
    }
    publish_gauges(&st);
    if !st.runnable.is_empty() {
        shared.work.notify_one();
    }
    if st.closed && st.queued == 0 {
        shared.work.notify_waiters();
    }
    if st.queued == 0 && st.in_flight == 0 {
        shared.idle.notify_waiters();
    }
}

async fn worker<B: PushBackend>(shared: Arc<Shared<B>>) {
    loop {
        let notified = shared.work.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let next = {
            let mut st = lock(&shared.state);
            match take_next(&mut st, shared.config.per_user_in_flight) {
                Some(next) => {
                    if !st.runnable.is_empty() {
                        shared.work.notify_one();
                    }
                    Some(next)
                }
                None if st.closed && st.queued == 0 => return,
                None => None,
            }
        };
        let Some((key, job)) = next else {
            notified.await;
            continue;
        };
        // A panic inside one job must not leave its key marked in flight
        // forever (every later enqueue for it would be a "duplicate").
        let result = std::panic::AssertUnwindSafe(run_job(&shared, &job))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| {
                tracing::error!(user_id = %job.user_id, "push job panicked");
                JobResult::Error
            });
        metrics::counter!(metric("notifier_push_job_total"), "result" => result.label())
            .increment(1);
        if let Some(reason) = result.dropped_reason() {
            // DQ9: at-most-once, so an undelivered job is counted as dropped.
            metrics::counter!(metric("notifier_push_dropped_total"), "reason" => reason)
                .increment(1);
        }
        finish(&shared, &key);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JobResult {
    Delivered,
    NoSubscriptions,
    Failed,
    BookkeepingFailed,
    Error,
}

/// Every `reason` label `notifier_push_dropped_total` can carry; see the
/// module doc.
const DROPPED_REASONS: &[&str] = &[
    "queue_full",
    "user_queue_full",
    "shutting_down",
    "abandoned_on_shutdown",
    "send_failed",
    "error",
];

impl JobResult {
    /// The `notifier_push_dropped_total` reason for a job that ended without
    /// delivering its notification, or `None` if it was delivered (or there
    /// was nobody to deliver to). `BookkeepingFailed` was delivered; its risk
    /// is a duplicate, not a drop.
    fn dropped_reason(self) -> Option<&'static str> {
        match self {
            JobResult::Failed => Some("send_failed"),
            JobResult::Error => Some("error"),
            JobResult::Delivered | JobResult::NoSubscriptions | JobResult::BookkeepingFailed => {
                None
            }
        }
    }

    fn label(self) -> &'static str {
        match self {
            JobResult::Delivered => "delivered",
            JobResult::NoSubscriptions => "no_subscriptions",
            JobResult::Failed => "failed",
            JobResult::BookkeepingFailed => "bookkeeping_failed",
            JobResult::Error => "error",
        }
    }
}

async fn run_job<B: PushBackend>(shared: &Shared<B>, job: &PushJob) -> JobResult {
    let started = Instant::now();
    let result = run_job_inner(shared, job).await;
    metrics::histogram!(metric("notifier_push_job_duration_seconds"))
        .record(started.elapsed().as_secs_f64());
    result
}

async fn run_job_inner<B: PushBackend>(shared: &Shared<B>, job: &PushJob) -> JobResult {
    let backend = &shared.backend;
    let user_id = job.user_id.as_str();
    let subscriptions = match backend.subscriptions(user_id).await {
        Ok(subscriptions) => subscriptions,
        Err(err) => {
            tracing::error!(error = ?err, user_id, "failed to load push subscriptions");
            return JobResult::Error;
        }
    };

    let result = if subscriptions.is_empty() {
        JobResult::NoSubscriptions
    } else {
        let outcomes =
            futures_util::future::join_all(subscriptions.iter().map(|subscription| async {
                tokio::time::timeout(
                    SUBSCRIPTION_BUDGET,
                    backend.send(subscription, &job.payload),
                )
                .await
                .unwrap_or(SendOutcome::TimedOut)
            }))
            .await;
        let mut any_sent = false;
        for (subscription, outcome) in subscriptions.iter().zip(outcomes) {
            let label = match outcome {
                SendOutcome::Sent => "sent",
                SendOutcome::Expired => "expired",
                SendOutcome::TimedOut => "timeout",
                SendOutcome::TransientFailure => "failed",
            };
            metrics::counter!(metric("notifier_push_send_total"), "outcome" => label).increment(1);
            match outcome {
                SendOutcome::Sent => {
                    any_sent = true;
                    lock(&shared.timeouts).remove(&subscription.id);
                }
                SendOutcome::Expired => {
                    lock(&shared.timeouts).remove(&subscription.id);
                    prune(backend, subscription, "expired").await;
                }
                SendOutcome::TimedOut => {
                    let strikes = {
                        let mut timeouts = lock(&shared.timeouts);
                        let strikes = timeouts.entry(subscription.id).or_insert(0);
                        *strikes += 1;
                        let now = *strikes;
                        if now >= shared.config.prune_after_timeouts {
                            timeouts.remove(&subscription.id);
                        }
                        now
                    };
                    if strikes >= shared.config.prune_after_timeouts {
                        prune(backend, subscription, "timeouts").await;
                    } else {
                        tracing::warn!(
                            user_id,
                            subscription_id = subscription.id,
                            strikes,
                            "push send timed out"
                        );
                    }
                }
                SendOutcome::TransientFailure => {
                    tracing::warn!(
                        user_id,
                        subscription_id = subscription.id,
                        "transient push send failure"
                    );
                }
            }
        }
        if !any_sent {
            return JobResult::Failed;
        }
        JobResult::Delivered
    };

    for attempt in 1..=BOOKKEEPING_ATTEMPTS {
        match backend
            .record_delivered(user_id, &job.delivered, job.decided_at)
            .await
        {
            Ok(()) => return result,
            Err(err) => {
                tracing::error!(
                    error = ?err,
                    user_id,
                    attempt,
                    "failed to record a delivered notification"
                );
                if attempt < BOOKKEEPING_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
                }
            }
        }
    }
    JobResult::BookkeepingFailed
}

async fn prune<B: PushBackend>(
    backend: &B,
    subscription: &PushSubscriptionRow,
    reason: &'static str,
) {
    match backend.delete_subscription(subscription.id).await {
        Ok(()) => {
            tracing::info!(
                subscription_id = subscription.id,
                reason,
                "pruned push subscription"
            );
            metrics::counter!(metric("notifier_push_subscription_pruned_total"), "reason" => reason)
                .increment(1);
        }
        Err(err) => {
            tracing::error!(error = ?err, subscription_id = subscription.id, "failed to prune push subscription");
        }
    }
}

#[cfg(test)]
#[expect(
    clippy::cast_possible_wrap,
    reason = "test code: casts of small known test values"
)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::send::tests::{TEST_AUTH, TEST_P256DH};

    /// In-memory subscriptions and bookkeeping; sends go either through the
    /// real [`Pusher`] (to a local wiremock server), through a gate the
    /// test opens by hand (`gate://` endpoints), or through a per-id script.
    struct FakeBackend {
        pusher: Pusher,
        subscriptions: Mutex<HashMap<String, Vec<PushSubscriptionRow>>>,
        deleted: Mutex<Vec<i64>>,
        recorded: Mutex<Vec<(String, Delivered)>>,
        gate: Arc<tokio::sync::Semaphore>,
        /// Sends that have reached a `gate://` endpoint (hung or released).
        gated: AtomicUsize,
        scripted: Mutex<HashMap<i64, VecDeque<SendOutcome>>>,
    }

    impl FakeBackend {
        fn new(per_attempt_timeout: Duration) -> Self {
            Self {
                pusher: Pusher::for_local_tests(per_attempt_timeout),
                subscriptions: Mutex::new(HashMap::new()),
                deleted: Mutex::new(Vec::new()),
                recorded: Mutex::new(Vec::new()),
                gate: Arc::new(tokio::sync::Semaphore::new(0)),
                gated: AtomicUsize::new(0),
                scripted: Mutex::new(HashMap::new()),
            }
        }

        fn with_subscription(self, user_id: &str, id: i64, endpoint: &str) -> Self {
            lock(&self.subscriptions)
                .entry(user_id.to_string())
                .or_default()
                .push(PushSubscriptionRow {
                    id,
                    endpoint: endpoint.to_string(),
                    p256dh: TEST_P256DH.to_string(),
                    auth: TEST_AUTH.to_string(),
                });
            self
        }
    }

    impl PushBackend for FakeBackend {
        fn subscriptions(
            &self,
            user_id: &str,
        ) -> impl Future<Output = anyhow::Result<Vec<PushSubscriptionRow>>> {
            std::future::ready(Ok(lock(&self.subscriptions)
                .get(user_id)
                .map(|subs| {
                    subs.iter()
                        .map(|s| PushSubscriptionRow {
                            id: s.id,
                            endpoint: s.endpoint.clone(),
                            p256dh: s.p256dh.clone(),
                            auth: s.auth.clone(),
                        })
                        .collect()
                })
                .unwrap_or_default()))
        }

        async fn send(
            &self,
            subscription: &PushSubscriptionRow,
            payload: &NotificationPayload,
        ) -> SendOutcome {
            let scripted = lock(&self.scripted)
                .get_mut(&subscription.id)
                .and_then(VecDeque::pop_front);
            if let Some(outcome) = scripted {
                return outcome;
            }
            if subscription.endpoint.starts_with("gate://") {
                self.gated.fetch_add(1, Ordering::SeqCst);
                self.gate
                    .acquire()
                    .await
                    .expect("the gate is never closed")
                    .forget();
                return SendOutcome::Sent;
            }
            self.pusher.send(subscription, payload).await
        }

        fn delete_subscription(&self, id: i64) -> impl Future<Output = anyhow::Result<()>> {
            lock(&self.deleted).push(id);
            for subs in lock(&self.subscriptions).values_mut() {
                subs.retain(|s| s.id != id);
            }
            std::future::ready(Ok(()))
        }

        fn record_delivered(
            &self,
            user_id: &str,
            delivered: &Delivered,
            _at: DateTime<Utc>,
        ) -> impl Future<Output = anyhow::Result<()>> {
            lock(&self.recorded).push((user_id.to_string(), delivered.clone()));
            std::future::ready(Ok(()))
        }
    }

    fn config() -> PushQueueConfig {
        PushQueueConfig {
            workers: 8,
            capacity: 64,
            per_user_in_flight: 2,
            per_user_queued: 32,
            prune_after_timeouts: 3,
        }
    }

    fn line_job(user_id: &str, line_id: &str, rank: u8) -> PushJob {
        PushJob {
            user_id: user_id.to_string(),
            payload: NotificationPayload {
                title: "t".to_string(),
                body: "b".to_string(),
                url: "/".to_string(),
                tag: format!("line-{line_id}"),
            },
            delivered: Delivered::Line {
                line_id: line_id.to_string(),
                rank,
            },
            decided_at: Utc::now(),
        }
    }

    fn recorded(queue: &PushQueue<FakeBackend>) -> Vec<(String, Delivered)> {
        lock(&queue.shared.backend.recorded).clone()
    }

    /// Waits until `cond` holds, on a `#[tokio::test(start_paused = true)]`
    /// (current-thread, virtual-time) runtime only.
    ///
    /// There the queue's workers run on the test's own thread, so they make
    /// progress whenever this yields, and the clock only moves when every
    /// task is blocked -- the deadline is virtual, counted in idle rounds
    /// of the runtime, not wall-clock time, so CPU load can't make it
    /// expire. (The old wall-clock version flaked under load: both its own
    /// 10 s deadline and `SUBSCRIPTION_BUDGET`'s 30 s on the hung sends were
    /// real time.) A condition that never holds still fails, instantly.
    async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
        assert_eq!(
            tokio::runtime::Handle::current().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::CurrentThread,
            "eventually() needs #[tokio::test(start_paused = true)]"
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !cond() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "never happened: {what}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn tarpit() -> wiremock::MockServer {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(wiremock::ResponseTemplate::new(201).set_delay(Duration::from_secs(30)))
            .mount(&server)
            .await;
        server
    }

    async fn healthy(status: u16) -> wiremock::MockServer {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(wiremock::ResponseTemplate::new(status))
            .mount(&server)
            .await;
        server
    }

    /// SVC-02: a user whose every endpoint is a tarpit, with many
    /// notifications queued, neither slows the caller (the main loop only
    /// ever calls `enqueue`) nor delays another user's delivery -- that one
    /// is delivered and recorded while the tarpit user's sends are still
    /// hanging.
    ///
    /// Deterministic: the tarpit is a `gate://` endpoint that hangs until
    /// the test opens the gate, so every assertion is about ordering and
    /// counts, and the runtime's clock is paused, so no wait here -- the
    /// test's own or the queue's `SUBSCRIPTION_BUDGET` on the hung sends --
    /// is wall-clock time (both flaked on a loaded machine).
    #[tokio::test(start_paused = true)]
    async fn a_tarpit_user_delays_neither_the_enqueuer_nor_other_users() {
        const TARPIT_JOBS: usize = 10;
        const TARPIT_SUBSCRIPTIONS: usize = 20;
        let mut backend = FakeBackend::new(Duration::from_secs(5));
        for id in 1..=TARPIT_SUBSCRIPTIONS as i64 {
            backend = backend.with_subscription("tarpit-user", id, &format!("gate://p{id}"));
        }
        let backend = backend.with_subscription("other-user", 100, "script://");
        lock(&backend.scripted).insert(100, VecDeque::from([SendOutcome::Sent]));
        let gate = Arc::clone(&backend.gate);
        let queue = PushQueue::start(backend, config());

        // `enqueue` is synchronous: each call returning while the gate is
        // shut (so no tarpit send can have completed) is the proof that the
        // enqueuer never waits on a send.
        for line in 0..TARPIT_JOBS {
            assert_eq!(
                queue.enqueue(line_job("tarpit-user", &format!("L{line}"), 3)),
                EnqueueOutcome::Queued
            );
        }
        assert_eq!(queue.depth() + queue.in_flight(), TARPIT_JOBS);

        // The per-user cap: with 8 workers free, exactly 2 of the tarpit
        // user's jobs start, and all 2 x 20 of their sends reach the
        // endpoint and hang there; the rest wait in the user's own queue.
        eventually("both capped tarpit jobs hanging at their endpoints", || {
            queue.shared.backend.gated.load(Ordering::SeqCst) >= 2 * TARPIT_SUBSCRIPTIONS
        })
        .await;
        assert_eq!(queue.in_flight(), 2, "per-user cap: only 2 at once");
        assert_eq!(
            queue.depth(),
            TARPIT_JOBS - 2,
            "the rest wait in the tarpit user's own queue"
        );

        // Another user's push is delivered and recorded while the tarpit
        // user's sends are provably still hanging (the gate is shut).
        assert_eq!(
            queue.enqueue(line_job("other-user", "L0", 3)),
            EnqueueOutcome::Queued
        );
        eventually("the other user's delivery", || {
            recorded(&queue)
                .iter()
                .any(|(user, _)| user == "other-user")
        })
        .await;
        assert_eq!(
            gate.available_permits(),
            0,
            "the gate is still shut: the tarpit sends are still hanging"
        );
        assert_eq!(
            queue.shared.backend.gated.load(Ordering::SeqCst),
            2 * TARPIT_SUBSCRIPTIONS,
            "no further tarpit job started while the first two hang"
        );
        assert!(
            !recorded(&queue)
                .iter()
                .any(|(user, _)| user == "tarpit-user"),
            "nothing is recorded for the tarpit user while its sends hang"
        );

        // Release the tarpit: everything drains, and the cap held for the
        // whole run, not just at the moments the test looked.
        gate.add_permits(TARPIT_JOBS * TARPIT_SUBSCRIPTIONS);
        queue.wait_idle().await;
        assert_eq!(recorded(&queue).len(), TARPIT_JOBS + 1);
        assert_eq!(
            queue.peak_in_flight("tarpit-user"),
            2,
            "per-user cap: never more than 2 of the tarpit user's jobs at once"
        );
        queue.shutdown(Duration::from_secs(1)).await;
    }

    /// One user can occupy at most `per_user_in_flight` workers however
    /// many jobs they queue; other users get the remaining workers.
    #[tokio::test(start_paused = true)]
    async fn the_per_user_in_flight_cap_holds() {
        let backend = FakeBackend::new(Duration::from_secs(5))
            .with_subscription("greedy", 1, "gate://g")
            .with_subscription("modest", 2, "gate://m");
        let gate = Arc::clone(&backend.gate);
        let queue = PushQueue::start(backend, config());

        for line in 0..6 {
            queue.enqueue(line_job("greedy", &format!("L{line}"), 1));
        }
        eventually("greedy at its cap", || {
            queue.shared.backend.gated.load(Ordering::SeqCst) >= 2
        })
        .await;
        assert_eq!(queue.in_flight(), 2, "the cap, with 8 workers free");
        assert_eq!(queue.depth(), 4);

        queue.enqueue(line_job("modest", "L0", 1));
        eventually("the other user dispatched past the greedy backlog", || {
            queue.in_flight() == 3
        })
        .await;

        gate.add_permits(100);
        queue.wait_idle().await;
        assert_eq!(recorded(&queue).len(), 7);
        assert_eq!(
            queue.peak_in_flight("greedy"),
            2,
            "never more than the cap over the whole run"
        );
        queue.shutdown(Duration::from_secs(1)).await;
    }

    /// A full queue (or a user's full share of it) drops the new job: it
    /// is never sent or recorded, and the refusal doesn't wedge its key --
    /// the same notification is accepted once there is room again.
    #[tokio::test(start_paused = true)]
    async fn a_full_queue_drops_the_new_job_and_records_nothing_for_it() {
        let backend = FakeBackend::new(Duration::from_secs(5))
            .with_subscription("a", 1, "gate://a")
            .with_subscription("b", 2, "gate://b")
            .with_subscription("c", 3, "gate://c")
            .with_subscription("d", 4, "gate://d");
        let gate = Arc::clone(&backend.gate);
        let queue = PushQueue::start(
            backend,
            PushQueueConfig {
                workers: 1,
                capacity: 2,
                per_user_queued: 2,
                ..config()
            },
        );

        assert_eq!(queue.enqueue(line_job("a", "L", 1)), EnqueueOutcome::Queued);
        eventually("a in flight", || queue.in_flight() == 1).await;
        assert_eq!(queue.enqueue(line_job("b", "L", 1)), EnqueueOutcome::Queued);
        assert_eq!(queue.enqueue(line_job("c", "L", 1)), EnqueueOutcome::Queued);
        assert_eq!(queue.depth(), 2);
        assert_eq!(
            queue.enqueue(line_job("d", "L", 1)),
            EnqueueOutcome::Dropped(DropReason::QueueFull)
        );

        gate.add_permits(100);
        queue.wait_idle().await;
        let users: Vec<String> = recorded(&queue).into_iter().map(|(u, _)| u).collect();
        assert_eq!(
            users,
            ["a", "b", "c"],
            "the dropped job is not sent or recorded"
        );

        assert_eq!(
            queue.enqueue(line_job("d", "L", 1)),
            EnqueueOutcome::Queued,
            "a re-decided notification is accepted once there is room"
        );
        queue.wait_idle().await;
        assert_eq!(recorded(&queue).len(), 4);
        queue.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn one_user_cannot_take_more_than_their_share_of_the_queue() {
        let backend = FakeBackend::new(Duration::from_secs(5))
            .with_subscription("greedy", 1, "gate://g")
            .with_subscription("other", 2, "gate://o");
        let gate = Arc::clone(&backend.gate);
        let queue = PushQueue::start(
            backend,
            PushQueueConfig {
                per_user_in_flight: 1,
                per_user_queued: 3,
                ..config()
            },
        );
        queue.enqueue(line_job("greedy", "L0", 1));
        eventually("greedy in flight", || queue.in_flight() == 1).await;
        for line in 1..=3 {
            assert_eq!(
                queue.enqueue(line_job("greedy", &format!("L{line}"), 1)),
                EnqueueOutcome::Queued
            );
        }
        assert_eq!(
            queue.enqueue(line_job("greedy", "L4", 1)),
            EnqueueOutcome::Dropped(DropReason::UserQueueFull)
        );
        assert_eq!(
            queue.enqueue(line_job("other", "L0", 1)),
            EnqueueOutcome::Queued
        );
        gate.add_permits(100);
        queue.wait_idle().await;
        queue.shutdown(Duration::from_secs(1)).await;
    }

    /// Re-reads of a row inside the cursor grace window re-decide the same
    /// notification while its job is still queued or in flight: that must
    /// not send it twice. A changed state is sent after the in-flight one,
    /// never alongside it, and only the newest parked state survives.
    #[tokio::test(start_paused = true)]
    async fn re_decided_notifications_are_coalesced_and_newer_states_follow_in_order() {
        let backend =
            FakeBackend::new(Duration::from_secs(5)).with_subscription("u", 1, "gate://u");
        let gate = Arc::clone(&backend.gate);
        let queue = PushQueue::start(backend, config());

        assert_eq!(queue.enqueue(line_job("u", "L", 2)), EnqueueOutcome::Queued);
        eventually("in flight", || queue.in_flight() == 1).await;
        assert_eq!(
            queue.enqueue(line_job("u", "L", 2)),
            EnqueueOutcome::Duplicate
        );
        assert_eq!(queue.enqueue(line_job("u", "L", 3)), EnqueueOutcome::Queued);
        assert_eq!(
            queue.enqueue(line_job("u", "L", 3)),
            EnqueueOutcome::Duplicate
        );
        assert_eq!(
            queue.enqueue(line_job("u", "L", 4)),
            EnqueueOutcome::Superseded
        );
        assert_eq!(
            queue.in_flight(),
            1,
            "the parked newer state waits for the in-flight one"
        );

        gate.add_permits(100);
        queue.wait_idle().await;
        let ranks: Vec<Delivered> = recorded(&queue).into_iter().map(|(_, d)| d).collect();
        assert_eq!(
            ranks,
            [
                Delivered::Line {
                    line_id: "L".to_string(),
                    rank: 2
                },
                Delivered::Line {
                    line_id: "L".to_string(),
                    rank: 4
                },
            ]
        );
        queue.shutdown(Duration::from_secs(1)).await;
    }

    /// DB2-25, made explicit: a job whose every send fails records nothing
    /// (so nothing is marked delivered that wasn't) and releases its key,
    /// so a later re-decision is sent rather than treated as a duplicate.
    /// A user with no subscriptions is recorded as handled.
    #[tokio::test]
    async fn a_failed_delivery_records_nothing_and_can_be_retried() {
        let failing = healthy(500).await;
        let backend = FakeBackend::new(Duration::from_secs(5)).with_subscription(
            "u",
            1,
            &format!("{}/p", failing.uri()),
        );
        let queue = PushQueue::start(backend, config());

        queue.enqueue(line_job("u", "L", 2));
        queue.wait_idle().await;
        assert!(
            recorded(&queue).is_empty(),
            "all sends failed: nothing recorded"
        );
        assert!(
            lock(&queue.shared.backend.deleted).is_empty(),
            "a 5xx never prunes"
        );

        assert_eq!(
            queue.enqueue(line_job("u", "L", 2)),
            EnqueueOutcome::Queued,
            "a failed job's key is released for a retry"
        );
        queue.wait_idle().await;

        queue.enqueue(line_job("nobody-subscribed", "L", 2));
        queue.wait_idle().await;
        assert_eq!(
            recorded(&queue),
            [(
                "nobody-subscribed".to_string(),
                Delivered::Line {
                    line_id: "L".to_string(),
                    rank: 2
                }
            )]
        );
        queue.shutdown(Duration::from_secs(1)).await;
    }

    /// SVC-02 pruning: an endpoint that times out `prune_after_timeouts`
    /// times in a row is deleted; the user, left with no subscriptions, is
    /// then recorded as handled, which is what stops the full-poll cycles
    /// re-deciding the same notification forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_endpoint_is_pruned_after_consecutive_timeouts() {
        let tarpit = tarpit().await;
        let backend = FakeBackend::new(Duration::from_millis(300)).with_subscription(
            "u",
            7,
            &format!("{}/p", tarpit.uri()),
        );
        let queue = PushQueue::start(backend, config());

        for attempt in 1..=3 {
            queue.enqueue(line_job("u", "L", 2));
            queue.wait_idle().await;
            let deleted = lock(&queue.shared.backend.deleted).clone();
            if attempt < 3 {
                assert!(deleted.is_empty(), "not pruned after {attempt} timeout(s)");
            } else {
                assert_eq!(deleted, [7], "pruned on the 3rd consecutive timeout");
            }
            assert!(recorded(&queue).is_empty());
        }

        queue.enqueue(line_job("u", "L", 2));
        queue.wait_idle().await;
        assert_eq!(recorded(&queue).len(), 1, "no subscriptions left: handled");
        queue.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn a_successful_send_resets_the_timeout_count() {
        let backend =
            FakeBackend::new(Duration::from_secs(1)).with_subscription("u", 7, "script://");
        lock(&backend.scripted).insert(
            7,
            VecDeque::from([
                SendOutcome::TimedOut,
                SendOutcome::TimedOut,
                SendOutcome::Sent,
                SendOutcome::TimedOut,
                SendOutcome::TimedOut,
                SendOutcome::TransientFailure,
                SendOutcome::TimedOut,
            ]),
        );
        let queue = PushQueue::start(backend, config());
        for step in 0..7 {
            queue.enqueue(line_job("u", &format!("L{step}"), 2));
            queue.wait_idle().await;
            let deleted = lock(&queue.shared.backend.deleted).clone();
            if step < 6 {
                assert!(
                    deleted.is_empty(),
                    "step {step}: a success resets the count"
                );
            } else {
                assert_eq!(
                    deleted,
                    [7],
                    "3 timeouts since the last success (a 5xx doesn't reset)"
                );
            }
        }
        queue.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn an_expired_endpoint_is_pruned_immediately() {
        let gone = healthy(410).await;
        let backend = FakeBackend::new(Duration::from_secs(5)).with_subscription(
            "u",
            9,
            &format!("{}/p", gone.uri()),
        );
        let queue = PushQueue::start(backend, config());
        queue.enqueue(line_job("u", "L", 2));
        queue.wait_idle().await;
        assert_eq!(lock(&queue.shared.backend.deleted).clone(), [9]);
        assert!(recorded(&queue).is_empty());
        queue.shutdown(Duration::from_secs(1)).await;
    }

    /// Shutdown drains what it can within the grace period...
    #[tokio::test]
    async fn shutdown_drains_queued_jobs_within_the_grace_period() {
        let fine = healthy(201).await;
        let backend = FakeBackend::new(Duration::from_secs(5)).with_subscription(
            "u",
            1,
            &format!("{}/p", fine.uri()),
        );
        let queue = PushQueue::start(
            backend,
            PushQueueConfig {
                workers: 1,
                ..config()
            },
        );
        for line in 0..5 {
            queue.enqueue(line_job("u", &format!("L{line}"), 1));
        }
        let shared = Arc::clone(&queue.shared);
        let report = queue.shutdown(Duration::from_secs(10)).await;
        assert_eq!(
            report,
            ShutdownReport {
                drained: true,
                abandoned: 0
            }
        );
        assert_eq!(lock(&shared.backend.recorded).len(), 5);
    }

    /// ...and abandons the rest once it runs out, without hanging, and
    /// refuses new work meanwhile. Runs on paused (virtual) time, so the
    /// bound below is exact rather than a wall-clock guess.
    #[tokio::test(start_paused = true)]
    async fn shutdown_abandons_stuck_jobs_after_the_grace_period() {
        let backend =
            FakeBackend::new(Duration::from_secs(5)).with_subscription("u", 1, "gate://u");
        let queue = PushQueue::start(
            backend,
            PushQueueConfig {
                workers: 1,
                ..config()
            },
        );
        for line in 0..3 {
            queue.enqueue(line_job("u", &format!("L{line}"), 1));
        }
        eventually("in flight", || queue.in_flight() == 1).await;
        let shared = Arc::clone(&queue.shared);
        let started = tokio::time::Instant::now();
        let report = queue.shutdown(Duration::from_millis(200)).await;
        assert!(
            started.elapsed() < SUBSCRIPTION_BUDGET,
            "shutdown returns at the grace period, not when the stuck send's budget runs out"
        );
        assert_eq!(
            report,
            ShutdownReport {
                drained: false,
                abandoned: 3
            }
        );
        assert!(lock(&shared.backend.recorded).is_empty());
    }

    #[tokio::test]
    async fn enqueue_after_close_is_dropped() {
        let queue = PushQueue::start(FakeBackend::new(Duration::from_secs(1)), config());
        lock(&queue.shared.state).closed = true;
        assert_eq!(
            queue.enqueue(line_job("u", "L", 1)),
            EnqueueOutcome::Dropped(DropReason::ShuttingDown)
        );
        queue.shutdown(Duration::from_secs(1)).await;
    }

    /// DQ9 (DB2-25): delivery is at-most-once, so every way a decided
    /// notification can end undelivered is counted under a pre-registered
    /// `notifier_push_dropped_total` reason, and a delivered one is not.
    #[test]
    fn every_undelivered_outcome_has_a_registered_dropped_reason() {
        for reason in [
            DropReason::QueueFull,
            DropReason::UserQueueFull,
            DropReason::ShuttingDown,
        ] {
            assert!(DROPPED_REASONS.contains(&reason.label()), "{reason:?}");
        }
        assert!(DROPPED_REASONS.contains(&"abandoned_on_shutdown"));
        assert_eq!(JobResult::Failed.dropped_reason(), Some("send_failed"));
        assert_eq!(JobResult::Error.dropped_reason(), Some("error"));
        for job_result in [JobResult::Failed, JobResult::Error] {
            let reason = job_result.dropped_reason().unwrap();
            assert!(DROPPED_REASONS.contains(&reason), "{reason}");
        }
        assert_eq!(JobResult::Delivered.dropped_reason(), None);
        assert_eq!(JobResult::NoSubscriptions.dropped_reason(), None);
        assert_eq!(
            JobResult::BookkeepingFailed.dropped_reason(),
            None,
            "delivered; the risk there is a duplicate, not a drop"
        );
    }
}
