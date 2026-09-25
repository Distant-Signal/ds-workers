//! Sends one Web Push message to one subscription, VAPID-signed. See
//! docs/superpowers/specs/2026-09-02-line-status-notifications-design.md's
//! Error handling section for the 404/410-vs-transient-failure split this
//! implements.
//!
//! The exact `web-push 0.11.0` call shape below was re-verified against
//! `https://docs.rs/web-push/0.11.0/web_push/` and the crate's own source
//! this session (the plan's own sketch predated that verification and was
//! wrong on two points, both fixed here):
//!   1. `WebPushError::EndpointNotValid`/`EndpointNotFound` each carry an
//!      `ErrorInfo` payload, not unit variants -- matched with `(_)`.
//!   2. `WebPushMessage` derives only `Debug`, not `Clone` (confirmed via
//!      `src/message.rs`'s struct definition), so a bounded retry cannot
//!      reuse one built message the way the plan's sketch assumed --
//!      `VapidSignature` DOES derive `Clone` (confirmed via its docs page's
//!      Trait Implementations list), so this rebuilds a fresh
//!      `WebPushMessageBuilder`/`WebPushMessage` from the same payload
//!      bytes and a cloned signature on each attempt instead.

use serde::Serialize;
use web_push::{
    ContentEncoding, SubscriptionInfo, VapidSignatureBuilder, WebPushClient, WebPushMessageBuilder,
};

use crate::queries::PushSubscriptionRow;

/// Exactly the SW contract this plan's Global Constraints section fixes.
/// Any change to this shape must be reflected in Task 9's push-handler
/// code -- they are two hand-written halves of the same wire contract.
#[derive(Debug, Serialize)]
pub struct NotificationPayload {
    pub title: String,
    pub body: String,
    pub url: String,
    pub tag: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SendOutcome {
    Sent,
    /// 404/410 from the push service -- caller must delete the subscription.
    Expired,
    /// Anything else (5xx, timeout, etc.) -- caller logs and moves on, no
    /// retry queue (Error handling section, spec).
    TransientFailure,
}

pub async fn send_to_subscription(
    vapid_private_key: &str,
    vapid_subject: &str,
    subscription: &PushSubscriptionRow,
    payload: &NotificationPayload,
) -> SendOutcome {
    let subscription_info = SubscriptionInfo::new(
        subscription.endpoint.clone(),
        subscription.p256dh.clone(),
        subscription.auth.clone(),
    );

    let mut signature_builder =
        match VapidSignatureBuilder::from_pem(vapid_private_key.as_bytes(), &subscription_info) {
            Ok(builder) => builder,
            Err(err) => {
                tracing::error!(error = ?err, "invalid VAPID private key"); // startup-time fail-fast (Task 6) should prevent this in practice
                return SendOutcome::TransientFailure;
            }
        };
    signature_builder.add_claim("sub", vapid_subject);
    let signature = match signature_builder.build() {
        Ok(sig) => sig,
        Err(err) => {
            tracing::error!(error = ?err, "failed to build VAPID signature");
            return SendOutcome::TransientFailure;
        }
    };

    let body = match serde_json::to_vec(payload) {
        Ok(body) => body,
        Err(err) => {
            tracing::error!(error = ?err, "failed to serialize notification payload");
            return SendOutcome::TransientFailure;
        }
    };

    let client = web_push::HyperWebPushClient::new();
    // Two bounded retries on a transient failure, per the spec's Error
    // handling section -- no dead-letter queue, no retry-after-restart
    // mechanism; a genuinely persistent change is picked up again next
    // cycle since notification_state is only updated on send success.
    // `WebPushMessage` isn't `Clone` (see this module's doc comment), so
    // each attempt rebuilds a fresh message from the same payload bytes
    // and a cloned `VapidSignature` (which IS `Clone`) rather than reusing
    // one built message across attempts.
    for attempt in 0..3 {
        let mut message_builder = WebPushMessageBuilder::new(&subscription_info);
        message_builder.set_payload(ContentEncoding::Aes128Gcm, &body);
        message_builder.set_vapid_signature(signature.clone());

        let message = match message_builder.build() {
            Ok(message) => message,
            Err(err) => {
                tracing::error!(error = ?err, "failed to build web push message");
                return SendOutcome::TransientFailure;
            }
        };

        match client.send(message).await {
            Ok(_) => return SendOutcome::Sent,
            Err(err) => match classify_web_push_error(&err) {
                SendOutcome::Expired => return SendOutcome::Expired,
                _ => tracing::warn!(error = ?err, attempt, "web push send failed, retrying"),
            },
        }
    }
    SendOutcome::TransientFailure
}

/// Whether a single push attempt's `WebPushError` means this SUBSCRIPTION
/// itself can never be delivered to again (prune it), or merely that this
/// one attempt failed for a reason that might not recur (retry it, same as
/// any other transient failure -- see this module's own "no dead-letter
/// queue" doc comment on [`send_to_subscription`]).
///
/// **The bug this closes.** Before this function existed,
/// `send_to_subscription` only recognised `EndpointNotValid`/
/// `EndpointNotFound` (410 Gone / 404 Not Found -- "this endpoint no longer
/// exists," the textbook signal every push service documents for a device
/// that unsubscribed or was wiped) as permanent. Every OTHER error,
/// including `Unauthorized` (401), fell into the catch-all "log and retry
/// next cycle" branch -- retried forever, with no cap, since this crate
/// deliberately has no dead-letter queue or failure counter (a genuinely
/// transient 5xx/timeout is expected to eventually succeed on its own).
///
/// `Unauthorized` is added here as a SECOND permanent-failure signal for a
/// narrower but equally unrecoverable reason: FCM/Mozilla autopush key a
/// subscription's endpoint to the exact VAPID `applicationServerKey` the
/// browser used when it subscribed. Rotating this crate's own
/// `config::Config::vapid_private_key`/`vapid_public_key` does not
/// retroactively update that binding -- every subscription created under
/// the OLD key pair permanently 401s against every send signed with the
/// new one, and nothing server-side ever changes that outcome (only the
/// browser re-subscribing from scratch does, which first requires this
/// crate to have pruned the dead row so a later real re-subscribe can
/// replace it). Treating a post-rotation 401 as merely transient retries
/// that subscription forever with zero chance of ever succeeding again --
/// exactly "a subscription that consistently fails to deliver... retried
/// forever with no pruning."
fn classify_web_push_error(err: &web_push::WebPushError) -> SendOutcome {
    match err {
        web_push::WebPushError::EndpointNotValid(_)
        | web_push::WebPushError::EndpointNotFound(_)
        | web_push::WebPushError::Unauthorized(_) => SendOutcome::Expired,
        _ => SendOutcome::TransientFailure,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real `WebPushError` for `status`, straight from `web-push`'s own
    /// status-code classifier (`web_push::request_builder::parse_response`)
    /// -- NOT hand-constructed. `web_push::error::ErrorInfo` (the payload
    /// every interesting `WebPushError` variant carries) is not publicly
    /// reachable outside that crate at all (`mod error;` is private, and
    /// nothing re-exports the type), so a test in this crate cannot build
    /// `WebPushError::Unauthorized(ErrorInfo { .. })` by hand -- driving the
    /// crate's own public entry point is the only way to get one, and it
    /// has the added benefit of testing this module's classification against
    /// the real status-to-error mapping rather than an assumption about it.
    fn web_push_error_for(status: http::StatusCode) -> web_push::WebPushError {
        web_push::request_builder::parse_response(status, Vec::new())
            .expect_err("every status this test passes is a non-2xx WebPushError")
    }

    #[test]
    fn a_gone_or_not_found_endpoint_is_treated_as_permanently_expired() {
        assert_eq!(
            classify_web_push_error(&web_push_error_for(http::StatusCode::GONE)),
            SendOutcome::Expired,
            "410 Gone -- the device unsubscribed or was wiped"
        );
        assert_eq!(
            classify_web_push_error(&web_push_error_for(http::StatusCode::NOT_FOUND)),
            SendOutcome::Expired,
            "404 Not Found -- same permanent-gone signal as 410"
        );
    }

    #[test]
    fn an_unauthorized_response_is_also_treated_as_permanently_expired() {
        // The VAPID-key-rotation scenario: this subscription's endpoint is
        // pinned to a key pair this crate no longer signs with, and will
        // 401 on every future attempt forever -- not merely this one.
        assert_eq!(
            classify_web_push_error(&web_push_error_for(http::StatusCode::UNAUTHORIZED)),
            SendOutcome::Expired
        );
    }

    #[test]
    fn a_server_error_is_still_transient_and_worth_retrying() {
        assert_eq!(
            classify_web_push_error(&web_push_error_for(http::StatusCode::INTERNAL_SERVER_ERROR)),
            SendOutcome::TransientFailure
        );
    }

    #[test]
    fn a_bad_request_is_still_transient_not_a_subscription_level_failure() {
        // A malformed request is this crate's own bug (or a one-off payload
        // issue), not evidence the SUBSCRIPTION itself is unreachable --
        // must not be pruned on this signal.
        assert_eq!(
            classify_web_push_error(&web_push_error_for(http::StatusCode::BAD_REQUEST)),
            SendOutcome::TransientFailure
        );
    }
}
