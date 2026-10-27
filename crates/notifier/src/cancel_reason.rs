//! Cancellation pushes that say where the train now runs and why (DS-184).
//!
//! **Copy.** A cancellation push is worded from the train's
//! `ds_store::trains::cancellation` summary, the same rules the app's
//! train page uses ([`ds_store::trains::cancellation::derive_from_timetable`]):
//! "Your train was cancelled" for a full cancellation, "Your train
//! terminates at York" or "now starts from York" when it runs short, "is
//! part cancelled" for a cancelled middle section, and the reason as
//! "Reason: Signal failure." when TRUST's code has glossary text.
//!
//! **When the reason comes late.** TRUST files the reason for a
//! cancellation through a different consumer than the one that marks the
//! train cancelled, so the push is often decided before the reason is
//! stored. The push is never delayed for it: a cancellation is the push a
//! traveller most needs promptly, and it is never held (`hold.rs`). It
//! goes at once with what is known, and when it carried no reason its
//! delivery records [`FOLLOWUP_WINDOW`] ahead
//! (`train_notification_state.reason_followup_until`). This module's cycle
//! ([`run_reason_followup_cycle`]) then sends ONE follow-up when the reason
//! is stored inside that window: the same copy with the reason, and the
//! same `tag`, so it replaces the first push on the device instead of
//! adding a second one (a replaced notification does not alert again).
//! Its delivery clears the window, so there is never a third push. Past
//! the window the follow-up is dropped: by then the traveller has seen the
//! app, which shows the reason anyway.
//!
//! The follow-up honours everything the first push did: the user's
//! cancellation switch (`notification_preferences.train_cancel`), the
//! train's own switch (`train_subscriptions.notifications_enabled`), a
//! journey break notice owning the train's push, and the train still being
//! cancelled (a reinstated train gets no "cancelled" follow-up).

use chrono::{DateTime, Duration, Utc};
use ds_store::trains::cancellation::{CancellationKind, CancellationSummary};
use sqlx::PgPool;

use crate::push_queue::{Delivered, PgBackend, PushJob, PushQueue};

/// How long after a cancellation push without a reason one follow-up may
/// still be sent. TRUST's reason is normally stored within a minute; 15
/// minutes covers a consumer catching up after a restart without
/// re-alerting about a cancellation the traveller has long since dealt
/// with.
pub(crate) const FOLLOWUP_WINDOW: Duration = Duration::minutes(15);

/// What a cancellation push says happened to the train.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CancelWhat {
    Cancelled,
    TerminatesAt(String),
    StartsFrom(String),
    /// Starts late and terminates short.
    RunsOnly {
        from: String,
        to: String,
    },
    /// A cancelled middle section, or ends the summary could not name.
    PartCancelled,
}

/// The push copy's facts: what happened and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CancelCopy {
    pub what: CancelWhat,
    /// The reason's passenger text, without a trailing full stop.
    pub reason: Option<String>,
}

impl CancelCopy {
    /// The copy for a train with status `cancelled` and this summary;
    /// `None` (nothing placed it) reads as a plain cancellation.
    pub(crate) fn from_summary(summary: Option<&CancellationSummary>) -> Self {
        let Some(summary) = summary else {
            return Self {
                what: CancelWhat::Cancelled,
                reason: None,
            };
        };
        let starts = summary
            .starts_from
            .as_ref()
            .and_then(|p| p.label())
            .map(str::to_string);
        let ends = summary
            .terminates_at
            .as_ref()
            .and_then(|p| p.label())
            .map(str::to_string);
        let what = match (summary.kind, starts, ends) {
            (CancellationKind::Full, _, _) => CancelWhat::Cancelled,
            (CancellationKind::Partial, Some(from), Some(to)) => CancelWhat::RunsOnly { from, to },
            (CancellationKind::Partial, None, Some(to)) => CancelWhat::TerminatesAt(to),
            (CancellationKind::Partial, Some(from), None) => CancelWhat::StartsFrom(from),
            (CancellationKind::Partial, None, None) => CancelWhat::PartCancelled,
        };
        let reason = summary
            .reason_text()
            .map(|text| text.trim_end_matches('.').trim().to_string())
            .filter(|text| !text.is_empty());
        Self { what, reason }
    }

    /// The predicate after a subject: "was cancelled", "terminates at
    /// York".
    pub(crate) fn predicate(&self) -> String {
        match &self.what {
            CancelWhat::Cancelled => "was cancelled".to_string(),
            CancelWhat::TerminatesAt(to) => format!("terminates at {to}"),
            CancelWhat::StartsFrom(from) => format!("now starts from {from}"),
            CancelWhat::RunsOnly { from, to } => format!("now runs only from {from} to {to}"),
            CancelWhat::PartCancelled => "is part cancelled".to_string(),
        }
    }

    /// "Reason: Signal failure." or `None`.
    pub(crate) fn reason_sentence(&self) -> Option<String> {
        self.reason
            .as_ref()
            .map(|reason| format!("Reason: {reason}."))
    }
}

/// The train's summary for push copy, or `None` when nothing places the
/// cancellation (or the read fails, logged: the push still goes, worded as
/// a plain cancellation).
pub(crate) async fn summary_for_train(
    pool: &PgPool,
    trains_id: i64,
) -> Option<CancellationSummary> {
    match ds_store::trains::cancellation::derive_from_timetable(pool, trains_id).await {
        Ok(cancellation) => cancellation.as_ref().map(CancellationSummary::from),
        Err(err) => {
            tracing::warn!(trains_id, error = ?err, "could not work out a cancellation for push copy");
            None
        }
    }
}

/// One subscription whose cancellation push may still get its reason.
#[derive(Debug, sqlx::FromRow)]
struct FollowupRow {
    user_id: String,
    tracked_train_id: i64,
    trains_id: i64,
    last_notified_delay_minutes: Option<i32>,
}

/// One pass (module doc): every cancellation push still inside its window
/// whose reason is now stored gets its one follow-up.
pub(crate) async fn run_reason_followup_cycle(
    pool: &PgPool,
    queue: &PushQueue<PgBackend>,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let rows: Vec<FollowupRow> = sqlx::query_as(
        "SELECT ns.user_id, ns.tracked_train_id, ts.trains_id, ns.last_notified_delay_minutes \
         FROM train_notification_state ns \
         JOIN train_subscriptions ts ON ts.id = ns.tracked_train_id \
         JOIN train_current_state cs ON cs.trains_id = ts.trains_id \
         LEFT JOIN notification_preferences np ON np.user_id = ns.user_id \
         WHERE ns.reason_followup_until > $1 AND ns.last_notified_status = 'cancelled' \
           AND cs.status = 'cancelled' AND ts.notifications_enabled \
           AND COALESCE(np.train_cancel, TRUE) \
         ORDER BY ns.reason_followup_until \
         LIMIT 500",
    )
    .bind(now)
    .fetch_all(pool)
    .await?;
    for row in rows {
        if crate::break_notices::enabled()
            && crate::break_notices::break_owns_train(pool, &row.user_id, row.tracked_train_id)
                .await?
        {
            continue;
        }
        let summary = summary_for_train(pool, row.trains_id).await;
        let copy = CancelCopy::from_summary(summary.as_ref());
        if copy.reason.is_none() {
            continue;
        }
        let journey_context =
            crate::queries::journey_leg_for_train_subscription(pool, row.tracked_train_id).await?;
        let payload = crate::build_train_notification_payload(
            row.tracked_train_id,
            "cancelled",
            None,
            journey_context.as_ref(),
            Some(&copy),
        );
        tracing::info!(
            tracked_train_id = row.tracked_train_id,
            "cancellation reason follow-up"
        );
        crate::hold::dispatch(
            pool,
            queue,
            PushJob {
                user_id: row.user_id,
                payload,
                delivered: Delivered::Train {
                    tracked_train_id: row.tracked_train_id,
                    status: "cancelled".to_string(),
                    delay_minutes: row.last_notified_delay_minutes,
                    awaiting_reason: false,
                },
                decided_at: now,
            },
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ds_store::trains::cancellation::{CancellationPlace, CancellationReason, ReasonSource};

    fn place(name: &str) -> CancellationPlace {
        CancellationPlace {
            crs: Some(name[..3].to_uppercase()),
            name: Some(name.to_string()),
        }
    }

    fn summary(
        kind: CancellationKind,
        starts_from: Option<&str>,
        terminates_at: Option<&str>,
        reason: Option<&str>,
    ) -> CancellationSummary {
        CancellationSummary {
            kind,
            starts_from: starts_from.map(place),
            terminates_at: terminates_at.map(place),
            reason: Some(CancellationReason {
                code: Some("IA".to_string()),
                text: reason.map(str::to_string),
                source: ReasonSource::Trust,
            }),
        }
    }

    #[test]
    fn a_full_cancellation_says_cancelled_with_its_reason() {
        let copy = CancelCopy::from_summary(Some(&summary(
            CancellationKind::Full,
            None,
            None,
            Some("Signal failure (including no fault found)"),
        )));
        assert_eq!(copy.predicate(), "was cancelled");
        assert_eq!(
            copy.reason_sentence().as_deref(),
            Some("Reason: Signal failure (including no fault found).")
        );
    }

    #[test]
    fn a_partial_cancellation_names_where_the_train_now_runs() {
        let ends = CancelCopy::from_summary(Some(&summary(
            CancellationKind::Partial,
            None,
            Some("York"),
            Some("Driver."),
        )));
        assert_eq!(ends.predicate(), "terminates at York");
        assert_eq!(ends.reason_sentence().as_deref(), Some("Reason: Driver."));
        let starts = CancelCopy::from_summary(Some(&summary(
            CancellationKind::Partial,
            Some("Leeds"),
            None,
            None,
        )));
        assert_eq!(starts.predicate(), "now starts from Leeds");
        assert_eq!(starts.reason_sentence(), None);
        let both = CancelCopy::from_summary(Some(&summary(
            CancellationKind::Partial,
            Some("Leeds"),
            Some("York"),
            None,
        )));
        assert_eq!(both.predicate(), "now runs only from Leeds to York");
        let middle =
            CancelCopy::from_summary(Some(&summary(CancellationKind::Partial, None, None, None)));
        assert_eq!(middle.predicate(), "is part cancelled");
    }

    #[test]
    fn an_unplaced_cancellation_or_unnamed_place_degrades() {
        assert_eq!(CancelCopy::from_summary(None).predicate(), "was cancelled");
        let mut unnamed = summary(CancellationKind::Partial, None, Some("York"), None);
        unnamed.terminates_at = Some(CancellationPlace {
            crs: Some("YRK".to_string()),
            name: None,
        });
        assert_eq!(
            CancelCopy::from_summary(Some(&unnamed)).predicate(),
            "terminates at YRK"
        );
        // A code with no text (`PD`) gives no reason sentence.
        let mut no_text = summary(CancellationKind::Full, None, None, None);
        no_text.reason.as_mut().unwrap().text = Some("  ".to_string());
        assert_eq!(CancelCopy::from_summary(Some(&no_text)).reason, None);
    }
}
