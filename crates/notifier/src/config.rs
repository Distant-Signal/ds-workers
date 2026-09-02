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
