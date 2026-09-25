use clap::Parser;

/// CLI/env configuration for the `notifier` service.
#[derive(Debug, Parser)]
pub struct Config {
    #[arg(long, env)]
    pub database_url: String,

    /// How often the notifier polls line_status_history/train_movement_events.
    /// DESIGN.md-style "reasonable round number, revisit with real usage"
    /// posture -- not independently load-tested, matching the spec's own
    /// framing of the cooldown/threshold constants below.
    #[arg(long, env, default_value_t = 60)]
    pub poll_interval_secs: u64,

    /// Decision 5: how long a de-escalation/lateral notification is
    /// suppressed after the last one sent to this user for this line.
    #[arg(long, env, default_value_t = 20)]
    pub cooldown_minutes: i64,

    /// Decision 4: the delay, in minutes, at or above which a tracked
    /// train's delay reading becomes notify-worthy.
    #[arg(long, env, default_value_t = 15)]
    pub train_delay_threshold_minutes: i32,

    /// How long a watermark proposal must age before this crate promotes it
    /// into a cursor's real `last_processed_id` -- the grace window that
    /// stops an out-of-order COMMIT from being skipped forever. See
    /// `queries::advance_cursor_with_grace`'s own doc comment for the
    /// mechanic and for the residual case it deliberately accepts (a
    /// transaction in flight for longer than this window).
    ///
    /// 120 seconds: comfortably longer than any write transaction the three
    /// polled tables' own writers actually take (each is a single
    /// INSERT/UPSERT, or a small batch of them, inside one statement or one
    /// short transaction), while costing only "each row is examined by two
    /// or three consecutive cycles instead of one" -- every send path these
    /// cursors feed is already idempotent against a re-read. Same
    /// "reasonable round number, revisit with real usage" posture as this
    /// crate's other interval constants, not an independently measured
    /// figure.
    #[arg(long, env, default_value_t = 120)]
    pub cursor_grace_seconds: i64,

    /// Cadence for the forwarding-queue poll (Task 17/18) -- deliberately
    /// faster than `poll_interval_secs`, since the whole point of
    /// trust-consumer's forwarding signal is a quicker path to a push than
    /// waiting for train_movement_events' own slower-polled cycle. The exact
    /// value is a judgment call, not a researched figure -- see the design
    /// spec's own Open Question 3 on this cadence needing "concrete design
    /// during implementation planning."
    #[arg(long, env, default_value_t = 15)]
    pub forward_queue_poll_interval_secs: u64,

    /// Cadence for the station-skip check (Task 9, §5.2) -- an independent
    /// full poll every interval, NOT cursor/watermark-based like the other
    /// two cycles, because `station_samples` is a wholesale-replaced
    /// current snapshot with no append log to diff against (see this
    /// plan's Architecture section). A reasonable-sounding, not
    /// load-tested figure -- same "revisit with real usage" posture this
    /// crate's other interval constants are already flagged with.
    #[arg(long, env, default_value_t = 90)]
    pub skip_check_poll_interval_secs: u64,

    /// Cadence for the recurring-journey materialization sweep (spec §3.1) --
    /// one branch does BOTH the daily mint (stage 1) and the auto-commit
    /// lead-time check (stage 2), per the spec's own "checked on the same
    /// hourly cadence" wording (§3.2's 2026-09-22 addendum) -- not two
    /// separate intervals. A reasonable-sounding, not load-tested figure,
    /// same "revisit with real usage" posture as this crate's other interval
    /// constants.
    #[arg(long, env, default_value_t = 3600)]
    pub template_sweep_poll_interval_secs: u64,

    /// Spec §3.2 (2026-09-22 addendum): an `'auto'`-mode leg's commit-check
    /// only runs once "now" is within this many minutes of the leg's earliest
    /// window bound (`depart_after` if set, else `arrive_after`) -- NOT at
    /// materialization time, which is what makes `'nearest_to_now'` mean
    /// something different from `'earliest'` (see that section's own worked
    /// reasoning for why committing immediately would make the two rules
    /// degenerate into the same behavior). 120 (2 hours ahead of the window)
    /// is the spec's own suggested starting default, explicitly flagged there
    /// as "this document's own suggestion, not a second product decision" --
    /// i64, not i32, to pair directly with `chrono::Duration::minutes` the
    /// same way `cooldown_minutes` already does.
    #[arg(long, env, default_value_t = 120)]
    pub auto_commit_lead_minutes: i64,

    /// VAPID keys, PEM-encoded EC private key (`openssl ecparam -genkey
    /// -name prime256v1`) and the matching uncompressed public key --
    /// wired into web-push's VapidSignatureBuilder in Task 6. Fails fast
    /// at startup if either is empty (Task 6), matching this repo's
    /// existing "refuse to start on a missing required secret" posture
    /// (crates/api/src/app.rs's internal_token `ensure!`).
    #[arg(long, env)]
    pub vapid_private_key: String,
    #[arg(long, env)]
    pub vapid_public_key: String,
    /// The `mailto:` or `https:` VAPID "subject" contact, required by the
    /// Web Push protocol's own VAPID spec (RFC 8292) so a push service can
    /// reach the sender if a subscription is being abused.
    #[arg(long, env)]
    pub vapid_subject: String,

    #[arg(long, env, default_value = "info")]
    pub log_level: String,
}
