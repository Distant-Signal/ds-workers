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
use web_push::{ContentEncoding, SubscriptionInfo, VapidSignatureBuilder, WebPushClient, WebPushMessageBuilder};

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
    let subscription_info =
        SubscriptionInfo::new(subscription.endpoint.clone(), subscription.p256dh.clone(), subscription.auth.clone());

    let mut signature_builder = match VapidSignatureBuilder::from_pem(vapid_private_key.as_bytes(), &subscription_info) {
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
            Err(web_push::WebPushError::EndpointNotValid(_)) | Err(web_push::WebPushError::EndpointNotFound(_)) => {
                return SendOutcome::Expired;
            }
            Err(err) => {
                tracing::warn!(error = ?err, attempt, "web push send failed, retrying");
            }
        }
    }
    SendOutcome::TransientFailure
}
