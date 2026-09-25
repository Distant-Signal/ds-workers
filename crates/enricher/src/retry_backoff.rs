//! Per-incident backoff for the extraction pipeline's own failure paths
//! (the three LLM calls in `llm.rs`, and `combine::combine_periods`'s
//! length/ordinal-alignment mismatch).
//!
//! Without this, a DETERMINISTIC failure -- text the configured model can
//! never parse into the expected schema, for instance -- is retried at full
//! cost on every sweep/stream/reclaim cycle forever: `chat_completion` sends
//! `temperature: 0.0`, so the exact same input reproduces the exact same
//! failure every single time, and nothing upstream advances
//! `source_text_hash`/`extraction_model_version` to make the incident stop
//! being selected (by design -- a failed attempt must never look like a
//! successful one). Three call sites (the stream consumer loop, the hourly
//! sweep, and the reclaim loop) each independently retry the same stuck
//! incident on their own schedule, so the wasted cost compounds across all
//! three.
//!
//! `crates/schedule-reference::main::CycleOutcome` solves an adjacent
//! problem with a typed `retryable`/`permanent` classification, but that
//! shape doesn't transplant cleanly here: `CycleOutcome` classifies failures
//! within ONE cycle of ONE poller against ONE delivery, and callers can
//! always say definitively "this specific error is retryable" (e.g. an I/O
//! error) or "permanent" (e.g. this ALF member's own content will never
//! parse). Here there is no single cycle to classify -- three independent
//! loops each decide to retry the same incident on their own timers -- and,
//! per `record_llm_call_metrics`'s own doc comment, `LlmClient::extract_*`
//! returns a bare `anyhow::Result` with no typed distinction between "the
//! request timed out" and any other failure, so there is no typed signal to
//! classify on even if there were one cycle to classify within.
//!
//! Instead of a typed classification, this backs off empirically, scoped to
//! one incident's one specific text (see `text_hash` below): the FIRST
//! failure against a given text is never delayed, so a genuinely transient
//! blip (a network hiccup, a momentary endpoint restart) is retried at the
//! caller's own normal cadence, same as before this fix existed. Only a
//! SECOND consecutive failure against the *same* text starts backing off,
//! doubling on every further consecutive failure and capped at
//! `MAX_BACKOFF` -- a deterministic failure quickly lands on the longest
//! interval instead of costing a full three-LLM-call attempt every cycle,
//! while a text change (a new `text_hash`) or a successful attempt clears
//! the count immediately and starts the cycle over.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// The first backed-off retry waits this long; each further consecutive
/// failure against the same text doubles it, capped at `MAX_BACKOFF`.
/// 30 minutes comfortably exceeds every one of this service's own retry
/// cadences (`reclaim_interval_secs` default 60s / `reclaim_min_idle_secs`
/// default 1000s / `sweep_interval_secs` default 3600s), so a backed-off
/// incident's next attempt is genuinely deferred rather than immediately
/// re-tried by whichever loop happens to poll next.
const BASE_BACKOFF: Duration = Duration::from_secs(30 * 60);

/// Ceiling on the exponential growth below -- an incident that has been
/// failing deterministically for days still gets re-attempted once a day
/// (in case a prompt/model change fixed it), rather than being backed off
/// into effective silence.
const MAX_BACKOFF: Duration = Duration::from_secs(24 * 60 * 60);

/// How many further consecutive failures it takes to grow from
/// `BASE_BACKOFF` to `MAX_BACKOFF` at a doubling-per-failure rate (2^9 * 30m
/// = 15360m = 256h, already past the 24h cap) -- caps the exponent so the
/// `Duration` multiplication below can never overflow regardless of how
/// many times a truly stuck incident keeps failing.
const MAX_BACKOFF_EXPONENT: u32 = 9;

/// Consecutive-failure delay for the Nth failure against the same text
/// (1-based: `consecutive_failures == 1` is the very first failure).
/// `< 2` returns `Duration::ZERO` -- see this module's doc comment for why
/// the first failure is never delayed.
fn backoff_for(consecutive_failures: u32) -> Duration {
    if consecutive_failures < 2 {
        return Duration::ZERO;
    }
    let exponent = (consecutive_failures - 2).min(MAX_BACKOFF_EXPONENT);
    BASE_BACKOFF
        .saturating_mul(1u32 << exponent)
        .min(MAX_BACKOFF)
}

struct BackoffEntry {
    /// The `hash::text_hash` this failure count applies to -- a text change
    /// (this incident's summary/description were edited) resets the count,
    /// since whatever made the old text fail may not apply to the new text
    /// at all.
    text_hash: String,
    consecutive_failures: u32,
    /// Not eligible for another attempt until this instant.
    retry_not_before: Instant,
}

/// Per-incident-id backoff state, shared (behind an `Arc`, like
/// `MismatchTracker`) across the stream consumer loop, the hourly sweep, and
/// the reclaim loop -- see this module's doc comment.
#[derive(Default)]
pub struct RetryBackoff {
    entries: Mutex<HashMap<String, BackoffEntry>>,
}

impl RetryBackoff {
    /// Whether `process_incident` should skip attempting extraction for
    /// `incident_id` right now -- i.e. it failed at least once already
    /// against this exact `current_text_hash`, and hasn't waited out that
    /// failure's backoff yet. Never skips a text this incident hasn't
    /// failed against before (a fresh incident, or one whose text just
    /// changed since its last failure).
    pub fn should_skip(&self, incident_id: &str, current_text_hash: &str) -> bool {
        self.should_skip_at(incident_id, current_text_hash, Instant::now())
    }

    fn should_skip_at(&self, incident_id: &str, current_text_hash: &str, now: Instant) -> bool {
        let entries = self.entries.lock().expect("retry backoff mutex poisoned");
        entries.get(incident_id).is_some_and(|entry| {
            entry.text_hash == current_text_hash && now < entry.retry_not_before
        })
    }

    /// Records a failed attempt against `text_hash`, returning the new
    /// consecutive-failure count for that exact text (1 on the first
    /// failure, or the first failure since the text last changed).
    pub fn record_failure(&self, incident_id: &str, text_hash: &str) -> u32 {
        self.record_failure_at(incident_id, text_hash, Instant::now())
    }

    fn record_failure_at(&self, incident_id: &str, text_hash: &str, now: Instant) -> u32 {
        let mut entries = self.entries.lock().expect("retry backoff mutex poisoned");
        let entry = entries
            .entry(incident_id.to_string())
            .and_modify(|entry| {
                if entry.text_hash != text_hash {
                    // The text changed since the last recorded failure --
                    // whatever made the old text fail may say nothing about
                    // this new text, so start counting again from scratch.
                    entry.text_hash = text_hash.to_string();
                    entry.consecutive_failures = 0;
                }
            })
            .or_insert_with(|| BackoffEntry {
                text_hash: text_hash.to_string(),
                consecutive_failures: 0,
                retry_not_before: now,
            });
        entry.consecutive_failures += 1;
        entry.retry_not_before = now + backoff_for(entry.consecutive_failures);
        entry.consecutive_failures
    }

    /// Clears any backoff state for `incident_id` -- called once extraction
    /// has actually produced a usable result for it (see `process_incident`
    /// for exactly which outcomes count), since there is nothing left to
    /// back off from.
    pub fn record_success(&self, incident_id: &str) {
        self.entries
            .lock()
            .expect("retry backoff mutex poisoned")
            .remove(incident_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_for_the_first_failure_is_zero() {
        assert_eq!(backoff_for(0), Duration::ZERO);
        assert_eq!(backoff_for(1), Duration::ZERO);
    }

    #[test]
    fn backoff_for_grows_and_caps() {
        assert_eq!(backoff_for(2), BASE_BACKOFF);
        assert_eq!(backoff_for(3), BASE_BACKOFF * 2);
        assert_eq!(backoff_for(4), BASE_BACKOFF * 4);
        // Comfortably past the point where doubling would exceed the cap.
        assert_eq!(backoff_for(20), MAX_BACKOFF);
        assert_eq!(backoff_for(1000), MAX_BACKOFF);
    }

    #[test]
    fn a_single_transient_failure_is_never_skipped() {
        let backoff = RetryBackoff::default();
        let now = Instant::now();
        let count = backoff.record_failure_at("INC-1", "hash-a", now);
        assert_eq!(count, 1);
        assert!(
            !backoff.should_skip_at("INC-1", "hash-a", now),
            "a lone failure must not gate the very next attempt -- only a repeat failure does"
        );
    }

    #[test]
    fn a_second_consecutive_failure_against_the_same_text_is_skipped_until_backoff_elapses() {
        let backoff = RetryBackoff::default();
        let now = Instant::now();
        backoff.record_failure_at("INC-1", "hash-a", now);
        backoff.record_failure_at("INC-1", "hash-a", now);

        assert!(
            backoff.should_skip_at("INC-1", "hash-a", now),
            "a second consecutive failure against unchanged text must gate the next attempt"
        );
        assert!(
            !backoff.should_skip_at("INC-1", "hash-a", now + BASE_BACKOFF),
            "once the backoff window has elapsed, the incident must be eligible again"
        );
    }

    #[test]
    fn a_text_change_resets_the_backoff() {
        let backoff = RetryBackoff::default();
        let now = Instant::now();
        backoff.record_failure_at("INC-1", "hash-a", now);
        backoff.record_failure_at("INC-1", "hash-a", now);
        assert!(backoff.should_skip_at("INC-1", "hash-a", now));

        // The incident's text changed (a new summary/description) --
        // whatever made the OLD text fail says nothing about this new one,
        // so it must not be gated by the stale backoff.
        assert!(
            !backoff.should_skip_at("INC-1", "hash-b", now),
            "a changed text hash must never be gated by a different text's backoff"
        );

        // And recording a failure against the new text starts the count
        // over rather than continuing to escalate the old one.
        let count = backoff.record_failure_at("INC-1", "hash-b", now);
        assert_eq!(
            count, 1,
            "a text change must reset the consecutive-failure count, not carry it over"
        );
    }

    #[test]
    fn a_success_clears_any_backoff_state() {
        let backoff = RetryBackoff::default();
        let now = Instant::now();
        backoff.record_failure_at("INC-1", "hash-a", now);
        backoff.record_failure_at("INC-1", "hash-a", now);
        assert!(backoff.should_skip_at("INC-1", "hash-a", now));

        backoff.record_success("INC-1");

        assert!(
            !backoff.should_skip_at("INC-1", "hash-a", now),
            "a success must clear the backoff so a later new failure starts counting from zero"
        );
    }

    #[test]
    fn an_unknown_incident_is_never_skipped() {
        let backoff = RetryBackoff::default();
        assert!(!backoff.should_skip("never seen before", "some-hash"));
    }
}
