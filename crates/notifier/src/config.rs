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

    /// `main.rs` builds `tracing_subscriber::fmt().with_env_filter(...)`
    /// directly from THIS field's value, NOT from
    /// `EnvFilter::from_default_env()` (which every other binary in this
    /// workspace uses, and which reads `RUST_LOG`) -- so the env var clap
    /// actually populates this field from is `LOG_LEVEL`, not `RUST_LOG`.
    ///
    /// 2026-09-25 Signal Box Audit Low finding: `notifier-deployment.yaml`
    /// set `RUST_LOG` here, which clap silently ignores (it isn't the env
    /// name this field declares) -- so `notifier.logLevel` in the Helm
    /// chart had no effect at all, and this crate's log level was
    /// permanently stuck at this field's own `"info"` default in every
    /// Helm-deployed environment, regardless of what an operator set. Fixed
    /// by setting `LOG_LEVEL` in the chart instead -- see
    /// `chart_env_wiring_tests` below, which now guards every one of this
    /// struct's declared env vars (this one included) against silently
    /// drifting out of sync with the chart again.
    #[arg(long, env, default_value = "info")]
    pub log_level: String,
}

impl Config {
    /// Fails loudly, with a clear message naming the offending field, if
    /// any of this crate's four `tokio::time::interval`-backed poll
    /// cadences is configured to zero seconds.
    ///
    /// **The bug this closes.** `tokio::time::interval(Duration::from_secs(0))`
    /// PANICS immediately (`Interval` requires a strictly-positive period) --
    /// so a misconfigured `POLL_INTERVAL_SECS=0` (or any of its three
    /// siblings) previously crashed the whole process at startup with a bare
    /// `tokio` panic message pointing at `main.rs`'s `tokio::time::interval`
    /// call, giving an operator no hint that the actual mistake was in their
    /// own config. Validating each value explicitly, before any interval is
    /// constructed, turns that into the same "refuse to start on a bad
    /// config, with a message that says why" posture this crate's own VAPID
    /// `ensure!`s already establish in `main`.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.poll_interval_secs > 0,
            "poll_interval_secs (--poll-interval-secs / POLL_INTERVAL_SECS) must be greater \
             than zero"
        );
        anyhow::ensure!(
            self.forward_queue_poll_interval_secs > 0,
            "forward_queue_poll_interval_secs (--forward-queue-poll-interval-secs / \
             FORWARD_QUEUE_POLL_INTERVAL_SECS) must be greater than zero"
        );
        anyhow::ensure!(
            self.skip_check_poll_interval_secs > 0,
            "skip_check_poll_interval_secs (--skip-check-poll-interval-secs / \
             SKIP_CHECK_POLL_INTERVAL_SECS) must be greater than zero"
        );
        anyhow::ensure!(
            self.template_sweep_poll_interval_secs > 0,
            "template_sweep_poll_interval_secs (--template-sweep-poll-interval-secs / \
             TEMPLATE_SWEEP_POLL_INTERVAL_SECS) must be greater than zero"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A config with every interval at a harmless nonzero value, so each
    /// test below only needs to override the ONE field it's exercising.
    fn valid_config() -> Config {
        Config {
            database_url: "postgres://test".to_string(),
            poll_interval_secs: 60,
            cooldown_minutes: 20,
            train_delay_threshold_minutes: 15,
            cursor_grace_seconds: 120,
            forward_queue_poll_interval_secs: 15,
            skip_check_poll_interval_secs: 90,
            template_sweep_poll_interval_secs: 3600,
            auto_commit_lead_minutes: 120,
            vapid_private_key: "test".to_string(),
            vapid_public_key: "test".to_string(),
            vapid_subject: "mailto:test@example.invalid".to_string(),
            log_level: "info".to_string(),
        }
    }

    #[test]
    fn a_fully_valid_config_passes() {
        assert!(valid_config().validate().is_ok());
    }

    #[test]
    fn a_zero_poll_interval_secs_is_rejected_with_a_clear_message() {
        let config = Config {
            poll_interval_secs: 0,
            ..valid_config()
        };
        let err = config
            .validate()
            .expect_err("zero poll_interval_secs must be rejected");
        assert!(err.to_string().contains("poll_interval_secs"));
    }

    #[test]
    fn a_zero_forward_queue_poll_interval_secs_is_rejected() {
        let config = Config {
            forward_queue_poll_interval_secs: 0,
            ..valid_config()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn a_zero_skip_check_poll_interval_secs_is_rejected() {
        let config = Config {
            skip_check_poll_interval_secs: 0,
            ..valid_config()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn a_zero_template_sweep_poll_interval_secs_is_rejected() {
        let config = Config {
            template_sweep_poll_interval_secs: 0,
            ..valid_config()
        };
        assert!(config.validate().is_err());
    }
}

/// The one invariant this crate cannot check at compile time and that was
/// already broken in production: **every env var this `Config` declares
/// (except `DATABASE_URL`, wired via the shared `distant-signal.databaseEnv`
/// helper rather than a literal line in this template -- see below) must
/// also be set on the `notifier` container in
/// `charts/distant-signal/templates/notifier-deployment.yaml`.**
///
/// Same class of bug `crates/api/src/data/config.rs`'s and
/// `crates/schedule-reference/src/config.rs`'s own `chart_env_wiring_tests`
/// modules already guard against, and this crate had TWO real instances of
/// it as of the 2026-09-25 Signal Box Audit Low pass: `log_level` was wired
/// under the wrong env var name (`RUST_LOG` instead of `LOG_LEVEL` -- see
/// that field's own doc comment for the full story), and
/// `cursor_grace_seconds`/`skip_check_poll_interval_secs`/
/// `template_sweep_poll_interval_secs`/`auto_commit_lead_minutes` were
/// declared with a working default but never wired into the chart at all --
/// each masked only because clap's own default happened to be what every
/// deployment wanted, exactly like `crates/schedule-reference`'s own
/// `TIPLOC_CRS_URL` gap before it was caught by hand.
///
/// A test is what makes the next one impossible to ship.
#[cfg(test)]
mod chart_env_wiring_tests {
    use clap::CommandFactory;

    use super::Config;

    /// Wired via `{{- include "distant-signal.databaseEnv" . }}` in
    /// `_helpers.tpl`, not a literal `- name: DATABASE_URL` line in
    /// `notifier-deployment.yaml` itself -- this test greps the raw
    /// template TEXT (no full Helm render), so a helper-included var can
    /// never be found this way. Excluded rather than reported as a false
    /// "missing" positive.
    const EXCLUDED_ENV_VARS: &[&str] = &["DATABASE_URL"];

    /// The `notifier` container's own slice of the notifier Deployment
    /// template. Scoped from its `- name: notifier` line to EOF rather than
    /// matching the whole file, matching
    /// `crates/api/src/data/config.rs`'s/`crates/schedule-reference/src/config.rs`'s
    /// equivalent helper -- so a var that only ever appears in one of this
    /// template's leading comment blocks cannot satisfy the check by
    /// accident. `notifier` is the only container in this template, so "to
    /// EOF" is the whole block.
    fn notifier_container_block() -> String {
        let chart = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../charts/distant-signal/templates/notifier-deployment.yaml");
        let rendered = std::fs::read_to_string(&chart)
            .unwrap_or_else(|err| panic!("read {}: {err}", chart.display()));
        let marker = "- name: notifier\n";
        let start = rendered.find(marker).expect(
            "the notifier Deployment must still declare a container named `notifier`; if it was \
             renamed, update this test's marker",
        );
        rendered[start..].to_string()
    }

    #[test]
    fn every_env_var_this_config_declares_is_set_on_the_charts_notifier_container() {
        let block = notifier_container_block();
        let command = Config::command();

        let declared: Vec<String> = command
            .get_arguments()
            .filter_map(|arg| arg.get_env().and_then(|env| env.to_str()))
            .map(str::to_string)
            .filter(|env| !EXCLUDED_ENV_VARS.contains(&env.as_str()))
            .collect();
        assert!(
            declared.len() >= 12,
            "sanity check: this Config declares 13 env vars total (DATABASE_URL excluded above, \
             see EXCLUDED_ENV_VARS); got {declared:?}"
        );

        let missing: Vec<&String> = declared
            .iter()
            .filter(|env| !block.contains(&format!("- name: {env}")))
            .collect();

        assert!(
            missing.is_empty(),
            "these env vars are declared by crates/notifier/src/config.rs but never set on the \
             `notifier` container in charts/distant-signal/templates/notifier-deployment.yaml, so \
             under Helm notifier silently falls back to this crate's own default for each -- \
             which for log_level in particular means the chart's notifier.logLevel value has no \
             effect at all: {missing:?}"
        );
    }

    /// The chart must not set an env var this struct no longer declares
    /// either: a stale entry in the template is a value an operator can
    /// configure that silently does nothing, and it is the same drift in
    /// the opposite direction. `DATABASE_URL` never appears as a literal
    /// `- name:` line in the raw template text (see
    /// `EXCLUDED_ENV_VARS`'s own comment), so it can never trip this check
    /// -- no exclusion needed on this side.
    #[test]
    fn the_chart_sets_no_env_var_this_config_does_not_declare() {
        let block = notifier_container_block();
        let command = Config::command();

        let declared: Vec<String> = command
            .get_arguments()
            .filter_map(|arg| arg.get_env().and_then(|env| env.to_str()))
            .map(str::to_string)
            .collect();

        let stale: Vec<&str> = block
            .lines()
            .filter_map(|line| line.trim().strip_prefix("- name: "))
            .filter(|env| env.chars().all(|c| c.is_ascii_uppercase() || c == '_'))
            .filter(|env| !declared.iter().any(|d| d == env))
            .collect();

        assert!(
            stale.is_empty(),
            "charts/distant-signal/templates/notifier-deployment.yaml sets these env vars on the \
             notifier container, but crates/notifier/src/config.rs no longer declares them, so \
             clap ignores them and any operator who configures one gets no effect at all: \
             {stale:?}"
        );
    }
}
