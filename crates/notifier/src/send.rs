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

use std::time::Duration;

use serde::Serialize;
pub(crate) use web_push::Urgency;
use web_push::{ContentEncoding, SubscriptionInfo, WebPushMessage, WebPushMessageBuilder};

use crate::queries::PushSubscriptionRow;
use crate::vapid_key::VapidKey;

/// Bound on a single web-push send attempt -- 2026-09 security/bug review
/// Medium finding M2: `web_push::HyperWebPushClient` configures no
/// connect/request timeout of its own, and every send below then ran
/// sequentially inside the notifier's one main `select!` loop (sends now
/// run on `push_queue`'s workers instead, SVC-02), so a single hanging push
/// endpoint stalled the WHOLE
/// notification cycle -- every other user's notifications too -- for as
/// long as that endpoint stayed silent. Worse, a push endpoint is exactly
/// `endpoint: String` on a subscription an AUTHENTICATED USER registers
/// themselves (`PushSubscriptionRow`/the subscribe route), so any user
/// could point it at their own tarpit server (accept the TCP connection,
/// never respond) and stall every OTHER user's notifications on demand,
/// indefinitely, with no rate limit or auth needed beyond their own
/// account.
///
/// 15s, matching this codebase's existing precedent for an outbound HTTP
/// call with no natural response-size bound of its own --
/// `common::oauth_client::TOKEN_FETCH_TIMEOUT`, also `Duration::from_secs(15)`.
/// A real push service (FCM/Mozilla autopush/etc.) resolves a send in low
/// hundreds of milliseconds; 15s is generous headroom for a slow-but-alive
/// endpoint while still bounding the damage a tarpit endpoint can do to one
/// send attempt.
const PUSH_SEND_TIMEOUT: Duration = Duration::from_secs(15);

/// `PUSH_TTL_SECS`'s default: how long a push service keeps a push it could
/// not deliver yet (the `TTL` header, RFC 8030 §5.2). Without it web-push
/// 0.11 sends its own default of 28 days, so a phone that was offline for a
/// week showed a week-old delay or cancellation when it came back. One hour
/// covers a tunnel, a flat battery on a short trip or a phone left in a bag,
/// and every train, line and journey push is about the next hour or two, so
/// after that it is more likely wrong than useful. The same for
/// cancellations: an hour-old one has usually been replaced by the
/// journey's own state on the page. The account security notices are the
/// exception (`push_queue::SECURITY_NOTICE_TTL_SECS`).
pub(crate) const DEFAULT_PUSH_TTL_SECS: u32 = 3600;

/// The Web Push delivery headers a push is sent with: `TTL` (RFC 8030
/// §5.2), `Urgency` (§5.3) and, whenever the payload's `tag` can be one
/// ([`topic_for`]), `Topic` (§5.4).
///
/// Every class sends its tag as its `Topic` (DS-386): a push service holding
/// a push for an offline device replaces it with a newer one of the same
/// topic rather than queueing both. On the device, pushes already replace
/// each other by `NotificationPayload::tag` (the service worker's
/// `showNotification` tag), so a topic only ever replaces what the device
/// would have replaced anyway; it never hides a push the device would have
/// kept. That is what makes a resend safe: a push whose first send ended in
/// [`SendOutcome::Unknown`] or [`SendOutcome::TimedOut`] is re-decided and
/// sent again with the same tag, and whether the first copy is still held
/// by the push service or already shown, the second replaces it instead of
/// appearing beside it. Tags are per target (`train-<id>`, `line-<id>`,
/// `journey-leg-leave-<id>`, ...), so the leave-by's topic, the first class
/// to send one, collides with no other class's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DeliveryOptions {
    pub ttl_secs: u32,
    pub urgency: Urgency,
}

/// A `Topic` header's value, if `tag` can be one: at most 32 characters
/// of the URL-safe base64 alphabet (RFC 8030 §5.4). Anything else is sent
/// without a topic rather than refused by the push service.
pub(crate) fn topic_for(tag: &str) -> Option<String> {
    (!tag.is_empty()
        && tag.len() <= 32
        && tag
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'))
    .then(|| tag.to_string())
}

/// A push's `TTL`: the configured one (`PUSH_TTL_SECS`) or a fixed one for
/// its class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ttl {
    Configured,
    Fixed(u32),
    /// Until `deadline`, and at most `max_secs`: worked out when it is
    /// sent, so time spent queued counts. A push past its deadline is sent
    /// with a TTL of 0 (delivered only to a device that is online).
    Until {
        deadline: chrono::DateTime<chrono::Utc>,
        max_secs: u32,
    },
}

impl Ttl {
    /// The `TTL` in seconds at `now`, `configured` for [`Ttl::Configured`].
    pub(crate) fn secs_at(self, configured: u32, now: chrono::DateTime<chrono::Utc>) -> u32 {
        match self {
            Ttl::Configured => configured,
            Ttl::Fixed(ttl_secs) => ttl_secs,
            Ttl::Until { deadline, max_secs } => {
                let left = (deadline - now).num_seconds().max(0);
                u32::try_from(left).unwrap_or(u32::MAX).min(max_secs)
            }
        }
    }
}

/// How one class of push is delivered (`push_queue::Delivered::push_class`
/// has the mapping): its `Urgency` and its `TTL`. Every class sends its tag
/// as its `Topic` ([`DeliveryOptions`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PushClass {
    pub urgency: Urgency,
    pub ttl: Ttl,
}

/// Exactly the SW contract this plan's Global Constraints section fixes.
/// Any change to this shape must be reflected in Task 9's push-handler
/// code -- they are two hand-written halves of the same wire contract.
#[derive(Debug, Serialize)]
pub(crate) struct NotificationPayload {
    pub title: String,
    pub body: String,
    pub url: String,
    pub tag: String,
    /// Alert again when this replaces a shown notification with the same
    /// tag (`showNotification`'s `renotify`): a moved leave-by. Left out of
    /// the JSON when false, so every other payload is unchanged.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub renotify: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SendOutcome {
    Sent,
    /// 404/410 from the push service -- caller must delete the subscription.
    Expired,
    /// The endpoint stayed silent for a whole attempt (`PUSH_SEND_TIMEOUT`)
    /// or, in `push_queue`, for the whole per-subscription budget. Kept
    /// apart from [`SendOutcome::TransientFailure`] because it is the
    /// signature of a dead or deliberately tarpitting endpoint: `push_queue`
    /// counts consecutive timeouts per subscription and prunes the
    /// subscription after `push_prune_after_timeouts` of them (SVC-02).
    /// Like [`SendOutcome::Unknown`], the push service may have accepted
    /// it, so it is not retried here.
    TimedOut,
    /// The request may have reached the push service, but no answer came
    /// back: the connection failed after the request was handed to it
    /// (reset, or closed before a response). Not retried (DS-386): if the
    /// service had accepted it, a second copy would be a duplicate push.
    /// `push_queue` does not record it as delivered, so the notification
    /// is re-decided on its next read and sent again with the same tag and
    /// `Topic`, which replaces the first copy instead of showing beside it
    /// ([`DeliveryOptions`]). Counted as `outcome="unknown"`.
    Unknown,
    /// The push service certainly did not take it: a 5xx or 429 on every
    /// attempt, a connect failure on every attempt, another 4xx, a refused
    /// send-time endpoint re-validation or a local signing error. The
    /// caller logs and moves on; the notification is re-decided only if
    /// its source row is polled again (see `push_queue`'s module doc for
    /// the delivery semantics).
    TransientFailure,
}

impl SendOutcome {
    /// The `outcome` label of `notifier_push_send_total`.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            SendOutcome::Sent => "sent",
            SendOutcome::Expired => "expired",
            SendOutcome::TimedOut => "timeout",
            SendOutcome::Unknown => "unknown",
            SendOutcome::TransientFailure => "failed",
        }
    }
}

/// The configured sender every production push goes through: the VAPID
/// credentials (the private key parsed once at startup, see `vapid_key`),
/// the HTTP client and the per-attempt timeout. `Clone` is cheap (`Arc`s),
/// so each `push_queue` worker can hold one; clones share the client's
/// connection pool.
#[derive(Clone)]
pub(crate) struct Pusher {
    vapid_key: VapidKey,
    vapid_subject: std::sync::Arc<str>,
    /// The `TTL` every push is sent with (`PUSH_TTL_SECS`).
    ttl_secs: u32,
    per_attempt_timeout: Duration,
    /// `false` only for tests that target a local `http://127.0.0.1` mock,
    /// which the L9 send-time re-validation would (correctly) refuse.
    revalidate_endpoint: bool,
    /// Built per `Pusher` rather than shared process-wide (DS-386): a
    /// pooled connection is driven by a task on the runtime that opened it,
    /// and each `#[tokio::test]` has its own runtime, so a static client
    /// handed one test a connection whose runtime had just shut down
    /// ("runtime dropped the dispatch task"). Production has one runtime
    /// and one `Pusher`, so it is the same one pool either way.
    client: reqwest::Client,
}

impl std::fmt::Debug for Pusher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pusher")
            .field("vapid_key", &self.vapid_key)
            .field("vapid_subject", &self.vapid_subject)
            .field("ttl_secs", &self.ttl_secs)
            .field("per_attempt_timeout", &self.per_attempt_timeout)
            .field("revalidate_endpoint", &self.revalidate_endpoint)
            .finish_non_exhaustive()
    }
}

impl Pusher {
    /// The production sender: L9 send-time re-validation on,
    /// `PUSH_SEND_TIMEOUT` per attempt, and `ttl_secs` as every push's `TTL`.
    pub(crate) fn new(vapid_key: VapidKey, vapid_subject: &str, ttl_secs: u32) -> Self {
        Self {
            vapid_key,
            vapid_subject: vapid_subject.into(),
            ttl_secs,
            per_attempt_timeout: PUSH_SEND_TIMEOUT,
            revalidate_endpoint: true,
            client: push_http_client(),
        }
    }

    /// A sender for tests against a local wiremock server: no L9 send-time
    /// re-validation (a `127.0.0.1` endpoint would fail it) and a short,
    /// injected per-attempt timeout. Everything else is the production path.
    #[cfg(test)]
    pub(crate) fn for_local_tests(per_attempt_timeout: Duration) -> Self {
        Self {
            vapid_key: tests::test_vapid_key(),
            vapid_subject: "mailto:test@example.com".into(),
            ttl_secs: DEFAULT_PUSH_TTL_SECS,
            per_attempt_timeout,
            revalidate_endpoint: false,
            client: push_http_client(),
        }
    }

    pub(crate) async fn send(
        &self,
        subscription: &PushSubscriptionRow,
        payload: &NotificationPayload,
        class: PushClass,
    ) -> SendOutcome {
        let options = DeliveryOptions {
            ttl_secs: class.ttl.secs_at(self.ttl_secs, chrono::Utc::now()),
            urgency: class.urgency,
        };
        if self.revalidate_endpoint {
            // Production: `new` always pairs this with `PUSH_SEND_TIMEOUT`,
            // which `send_to_subscription` applies itself.
            return send_to_subscription(
                &self.client,
                &self.vapid_key,
                &self.vapid_subject,
                subscription,
                payload,
                options,
            )
            .await;
        }
        send_to_subscription_with_timeout(
            &self.client,
            &self.vapid_key,
            &self.vapid_subject,
            subscription,
            payload,
            options,
            self.per_attempt_timeout,
        )
        .await
    }
}

pub(crate) async fn send_to_subscription(
    client: &reqwest::Client,
    vapid_key: &VapidKey,
    vapid_subject: &str,
    subscription: &PushSubscriptionRow,
    payload: &NotificationPayload,
    options: DeliveryOptions,
) -> SendOutcome {
    // L9 (2026-09-26 review): `crates/api::data::notifications::
    // validate_push_endpoint` only ever validates an `endpoint` once, at
    // registration time. A DNS-rebinding-capable attacker can register a
    // subscription whose hostname resolves to a public IP that day, then
    // repoint the same name at an internal/private/loopback address by the
    // time this send actually runs -- possibly hours or days later, on a
    // completely different notifier pod invocation, with no further
    // opportunity for `api` to catch it. Re-running the SAME check here,
    // immediately before the real send, closes that window: this is a
    // fresh, real DNS resolution against the endpoint's CURRENT state, not
    // a cached judgment from registration time. See
    // `ds_runtime::outbound_endpoint_guard`'s own module doc for the shared
    // implementation both crates now call.
    if let Err(err) =
        ds_runtime::outbound_endpoint_guard::validate_outbound_url(&subscription.endpoint).await
    {
        tracing::error!(
            endpoint = %subscription.endpoint,
            error = %err,
            "push endpoint failed send-time re-validation -- it passed registration-time \
             validation but now resolves to a disallowed (private/internal/loopback) address; \
             refusing to send rather than risk an SSRF via DNS rebinding. Not treated as \
             Expired: this may be a transient resolver issue rather than a genuinely hostile \
             endpoint, so the subscription is kept and retried on the next real transition \
             rather than deleted"
        );
        return SendOutcome::TransientFailure;
    }

    send_to_subscription_with_timeout(
        client,
        vapid_key,
        vapid_subject,
        subscription,
        payload,
        options,
        PUSH_SEND_TIMEOUT,
    )
    .await
}

/// [`send_to_subscription`] with the per-attempt timeout injectable. Does NOT
/// run the L9 send-time endpoint re-validation -- that lives in the
/// production entry point [`send_to_subscription`], which every real send
/// goes through; the timeout test calls this directly because its local
/// `http://127.0.0.1` mock would (correctly) fail that guard.
///
/// Every
/// production call goes through that wrapper with `PUSH_SEND_TIMEOUT`;
/// this exists so the timeout regression test can use a short timeout in
/// REAL time. It cannot use a paused tokio clock instead: this path does
/// real network IO against a local mock server, and paused time
/// auto-advances while the runtime waits on that real TCP connect, so the
/// timeout could fire before any request ever reached the mock.
async fn send_to_subscription_with_timeout(
    client: &reqwest::Client,
    vapid_key: &VapidKey,
    vapid_subject: &str,
    subscription: &PushSubscriptionRow,
    payload: &NotificationPayload,
    options: DeliveryOptions,
    per_attempt_timeout: Duration,
) -> SendOutcome {
    send_with_retries(
        client,
        vapid_key,
        vapid_subject,
        subscription,
        payload,
        options,
        per_attempt_timeout,
    )
    .await
    .0
}

/// Attempts per send: the first plus two retries, per the spec's Error
/// handling section.
const MAX_ATTEMPTS: u32 = 3;

/// [`send_to_subscription_with_timeout`], also returning how many attempts
/// were made (for the retry tests).
///
/// # Retries (DS-386)
///
/// A send is retried only when the push service certainly did not take
/// it, so a retry can never deliver a second copy:
///
/// * the connection was never made (resolution, TCP connect or TLS
///   handshake failed), so the request was never sent;
/// * the service answered 5xx or 429. RFC 8030 §5 makes `201 Created` the
///   only "accepted"; a 5xx or 429 is an explicit "not now". A
///   `Retry-After` is not waited for: the retries are immediate and the
///   whole send is bounded by `push_queue`'s per-subscription budget, so a
///   service that is still refusing leaves it to the next re-decision.
///
/// Everything else stops at once. A timeout and a connection that failed
/// after the request was handed to it are [`SendOutcome::TimedOut`] and
/// [`SendOutcome::Unknown`]: the service may have accepted it, and
/// retrying there is exactly how a slow push service that did accept the
/// first copy received a second. The trade-off is that a push the service
/// did NOT accept waits for its re-decision (the next read of its source
/// row) instead of an immediate retry; that resend carries the same tag
/// and `Topic` ([`DeliveryOptions`]), so if the first copy did arrive the
/// second replaces it rather than showing twice. Another 4xx would fail
/// the same way again, so it is not retried either.
#[expect(
    clippy::cast_possible_truncation,
    reason = "these durations are seconds to hours, far below u64::MAX milliseconds"
)]
async fn send_with_retries(
    client: &reqwest::Client,
    vapid_key: &VapidKey,
    vapid_subject: &str,
    subscription: &PushSubscriptionRow,
    payload: &NotificationPayload,
    options: DeliveryOptions,
    per_attempt_timeout: Duration,
) -> (SendOutcome, u32) {
    let subscription_info = SubscriptionInfo::new(
        subscription.endpoint.clone(),
        subscription.p256dh.clone(),
        subscription.auth.clone(),
    );

    // The key was parsed (and checked against VAPID_PUBLIC_KEY) once at
    // startup; this only binds it to the subscription's audience.
    let mut signature_builder = vapid_key.signature_builder(&subscription_info);
    signature_builder.add_claim("sub", vapid_subject);
    let signature = match signature_builder.build() {
        Ok(sig) => sig,
        Err(err) => {
            tracing::error!(error = ?err, "failed to build VAPID signature");
            return (SendOutcome::TransientFailure, 0);
        }
    };

    let body = match serde_json::to_vec(payload) {
        Ok(body) => body,
        Err(err) => {
            tracing::error!(error = ?err, "failed to serialize notification payload");
            return (SendOutcome::TransientFailure, 0);
        }
    };

    // `WebPushMessage` isn't `Clone` (see this module's doc comment), so
    // each attempt rebuilds a fresh message from the same payload bytes
    // and a cloned `VapidSignature` (which IS `Clone`) rather than reusing
    // one built message across attempts.
    for attempt in 1..=MAX_ATTEMPTS {
        let mut message_builder = WebPushMessageBuilder::new(&subscription_info);
        message_builder.set_payload(ContentEncoding::Aes128Gcm, &body);
        message_builder.set_vapid_signature(signature.clone());
        message_builder.set_ttl(options.ttl_secs);
        message_builder.set_urgency(options.urgency);
        if let Some(topic) = topic_for(&payload.tag) {
            message_builder.set_topic(topic);
        }

        let message = match message_builder.build() {
            Ok(message) => message,
            Err(err) => {
                tracing::error!(error = ?err, "failed to build web push message");
                return (SendOutcome::TransientFailure, attempt - 1);
            }
        };

        // Wrapped in `tokio::time::timeout` (see `PUSH_SEND_TIMEOUT`'s own
        // doc comment for the tarpit-endpoint bug this closes).
        let err =
            match tokio::time::timeout(per_attempt_timeout, send_message(client, message)).await {
                Ok(Ok(())) => return (SendOutcome::Sent, attempt),
                Ok(Err(err)) => err,
                // A timed-out attempt is NOT retried (M2 follow-up, 2026-09-27):
                // an endpoint that sat silent for the whole timeout is either
                // down or a deliberate tarpit, and retrying it only tripled the
                // time one subscription could hold up the notification cycle
                // (3 x PUSH_SEND_TIMEOUT = 45s per subscription). It may also
                // have accepted the push and only been slow to say so, and a
                // retry would then deliver it twice (DS-386).
                Err(_elapsed) => {
                    tracing::warn!(
                        attempt,
                        timeout_ms = per_attempt_timeout.as_millis() as u64,
                        "web push send timed out; not retrying"
                    );
                    return (SendOutcome::TimedOut, attempt);
                }
            };
        match after_failure(&err) {
            AfterFailure::Stop(outcome) => {
                tracing::warn!(error = ?err.error(), attempt, ?outcome, "web push send failed");
                return (outcome, attempt);
            }
            AfterFailure::Retry(reason) if attempt < MAX_ATTEMPTS => {
                tracing::warn!(error = ?err.error(), attempt, reason, "web push send failed, retrying");
                metrics::counter!(
                    ds_runtime::metrics::metric_name("notifier_push_send_retry_total"),
                    "reason" => reason
                )
                .increment(1);
            }
            AfterFailure::Retry(reason) => {
                tracing::warn!(error = ?err.error(), attempt, reason, "web push send failed, giving up");
            }
        }
    }
    (SendOutcome::TransientFailure, MAX_ATTEMPTS)
}

/// What a failed attempt leads to (see [`send_with_retries`]'s Retries).
#[derive(Debug, PartialEq, Eq)]
enum AfterFailure {
    /// The service certainly did not take it: try again. The reason is the
    /// `reason` label of `notifier_push_send_retry_total`.
    Retry(&'static str),
    /// Stop with this outcome.
    Stop(SendOutcome),
}

fn after_failure(err: &PushSendError) -> AfterFailure {
    match err {
        PushSendError::Connect(_) => AfterFailure::Retry("connect"),
        PushSendError::Unanswered(_) => AfterFailure::Stop(SendOutcome::Unknown),
        PushSendError::Local(_) => AfterFailure::Stop(SendOutcome::TransientFailure),
        PushSendError::Rejected { status, error } => {
            if classify_web_push_error(error) == SendOutcome::Expired {
                AfterFailure::Stop(SendOutcome::Expired)
            } else if status.is_server_error() {
                AfterFailure::Retry("server_error")
            } else if *status == http::StatusCode::TOO_MANY_REQUESTS {
                AfterFailure::Retry("too_many_requests")
            } else {
                AfterFailure::Stop(SendOutcome::TransientFailure)
            }
        }
    }
}

/// The HTTP client push sends go through (L9 follow-up, 2026-09-27):
/// `ds_runtime::outbound_endpoint_guard::public_only_client_builder`,
/// i.e. a resolver that refuses private/internal/loopback answers at
/// CONNECT time, plus no redirects. This replaces
/// `web_push::HyperWebPushClient`, whose connector resolves the endpoint's
/// name a second time after [`send_to_subscription`]'s own
/// `validate_outbound_url` check -- and a 0-TTL rebinding name can answer
/// differently the second time, so that check alone never constrained
/// where the POST actually went. Built once per [`Pusher`] and reused
/// (connection pool). It has no retry policy beyond reqwest's default,
/// which for HTTP/1 retries nothing that was sent.
#[expect(
    clippy::expect_used,
    reason = "a client builder with static settings fails only if TLS init does, which is fatal"
)]
fn push_http_client() -> reqwest::Client {
    ds_runtime::outbound_endpoint_guard::public_only_client_builder()
        .build()
        .expect("the push HTTP client's static configuration is always valid")
}

/// Most of a push service's error body read back for the log, same bound
/// `web_push::HyperWebPushClient` applied (`MAX_RESPONSE_SIZE`, 64 KiB).
const MAX_PUSH_RESPONSE_BYTES: usize = 64 * 1024;

#[derive(Debug)]
enum PushSendError {
    /// Resolution (including a [`ds_runtime::outbound_endpoint_guard::PublicOnlyResolver`]
    /// refusal), TCP connect or TLS handshake failed: the request was never
    /// sent. Retried.
    Connect(reqwest::Error),
    /// The connection failed after the request was handed to it: the push
    /// service may have it. [`SendOutcome::Unknown`], not retried.
    Unanswered(reqwest::Error),
    /// The request could not be built: it would fail the same way again.
    Local(reqwest::Error),
    /// The push service answered with a non-success status, classified by
    /// `web_push`'s own `parse_response`.
    Rejected {
        status: http::StatusCode,
        error: web_push::WebPushError,
    },
}

impl PushSendError {
    /// The underlying error, for the log.
    fn error(&self) -> &dyn std::fmt::Debug {
        match self {
            Self::Connect(err) | Self::Unanswered(err) | Self::Local(err) => err,
            Self::Rejected { error, .. } => error,
        }
    }
}

/// One push POST: `web_push`'s own `build_request` (headers, encrypted
/// body) sent through the [`Pusher`]'s client, and its own `parse_response`
/// on a failure -- the same two halves `HyperWebPushClient::send` used,
/// with only the transport swapped.
async fn send_message(
    client: &reqwest::Client,
    message: WebPushMessage,
) -> Result<(), PushSendError> {
    let request: http::Request<Vec<u8>> = web_push::request_builder::build_request(message);
    let (parts, body) = request.into_parts();
    let mut builder = client.post(parts.uri.to_string());
    for (name, value) in &parts.headers {
        builder = builder.header(name.as_str(), value.as_bytes());
    }
    let mut response = builder.body(body).send().await.map_err(|err| {
        if err.is_connect() {
            PushSendError::Connect(err)
        } else if err.is_builder() {
            PushSendError::Local(err)
        } else {
            PushSendError::Unanswered(err)
        }
    })?;
    let status = http::StatusCode::from_u16(response.status().as_u16())
        .unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
    if status.is_success() {
        // Accepted. Its body is not used, so failing to read one must not
        // turn an accepted push into a failure (and a resend).
        return Ok(());
    }
    // Best effort: the status alone decides what happens next; the body
    // only adds the push service's error text to the log.
    let mut response_body = Vec::new();
    while response_body.len() < MAX_PUSH_RESPONSE_BYTES {
        match response.chunk().await {
            Ok(Some(chunk)) => response_body.extend_from_slice(&chunk),
            Ok(None) | Err(_) => break,
        }
    }
    response_body.truncate(MAX_PUSH_RESPONSE_BYTES);
    web_push::request_builder::parse_response(status, response_body)
        .map_err(|error| PushSendError::Rejected { status, error })
}
/// Whether a single push attempt's `WebPushError` means this SUBSCRIPTION
/// itself can never be delivered to again (prune it), or merely that this
/// one attempt failed for a reason that might not recur (a transient
/// failure; whether it is retried at once is [`after_failure`]'s call).
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
pub(crate) mod tests {
    use std::sync::Arc;

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

    // A real (freshly generated, single-purpose-for-this-test) P-256 EC
    // private key, `openssl ecparam -name prime256v1 -genkey -noout`
    // output verbatim -- signing needs a real,
    // parseable SEC1 EC key to get past signature-building and actually
    // reach the network call this test means to exercise; a bogus/empty
    // string would fail at `signature_builder.build()`, before
    // `PUSH_SEND_TIMEOUT` is ever in play, and the test would pass for the
    // wrong reason.
    pub(crate) const TEST_VAPID_PRIVATE_KEY_PEM: &str = "-----BEGIN EC PRIVATE KEY-----
MHcCAQEEINajp+9GEKgTlNQuPdFvXAHS31oSZgBg8YnjOLkOKSkVoAoGCCqGSM49
AwEHoUQDQgAEZGCEGdhU+lVKPN9eP0esU1lUjQS/QenHXBw2+YsPSjQ28Tq+6trX
MyG+KKTT16anfp8nwB3M0QVuDOSRKYCrdw==
-----END EC PRIVATE KEY-----
";

    /// [`TEST_VAPID_PRIVATE_KEY_PEM`], parsed the way `main` parses the
    /// configured key.
    pub(crate) fn test_vapid_key() -> VapidKey {
        VapidKey::parse(TEST_VAPID_PRIVATE_KEY_PEM).expect("test key parses")
    }

    // A real, correctly-shaped P-256 uncompressed point (65 bytes, leading
    // 0x04) / 16-byte auth secret, base64url-no-padding -- same reasoning
    // as `TEST_VAPID_PRIVATE_KEY_PEM` above: `set_payload`'s AES-128-GCM
    // encryption step needs real-shaped key material to get past message
    // building and actually reach the network call.
    pub(crate) const TEST_P256DH: &str =
        "BElgNxkQ0haRG9SKY_JeamLar9TZiJi-17S3_yCr7BfS8e2CU0ahMb3FiZEfdtvc-cdK2euyDsiOYvbsRdrZ7ug";
    pub(crate) const TEST_AUTH: &str = "sXOd9HnSDnFaPZIqFzICvw";

    /// M2 regression (2026-09 security/bug review): before this fix,
    /// nothing wrapped `HyperWebPushClient::send` -- whose own doc comment
    /// literally says "Never times out" -- so a subscription endpoint that
    /// accepted the connection and then never responded (exactly what an
    /// authenticated user's own tarpit server can do on demand, per
    /// `PUSH_SEND_TIMEOUT`'s own doc comment) hung this call forever,
    /// stalling the whole notification cycle for every user behind it.
    ///
    /// Runs in REAL time with a short injected per-attempt timeout against
    /// a local server that reads the request and never answers. An earlier
    /// version used a paused tokio clock with the real 15s timeout and was
    /// flaky: this test does real TCP IO, and paused time auto-advances
    /// while the runtime waits on that IO.
    ///
    /// The timed-out attempt is not retried (DS-386): the push service may
    /// have accepted it. The attempt count proves that whatever the
    /// timing; the server's count of requests may be 0 on a loaded machine
    /// (the timeout fired before the request arrived) but never 2.
    #[tokio::test]
    async fn a_hanging_push_endpoint_times_out_and_is_not_retried() {
        const TEST_PER_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(500);
        let server = SilentServer::start(Silence::Hang).await;

        let (outcome, attempts) = send_with_retries(
            &push_http_client(),
            &test_vapid_key(),
            "mailto:test@example.com",
            &server.subscription(),
            &payload(),
            test_options(),
            TEST_PER_ATTEMPT_TIMEOUT,
        )
        .await;

        assert_eq!(
            outcome,
            SendOutcome::TimedOut,
            "a send whose response never arrives must time out -- reported as TimedOut so \
             push_queue can count it towards pruning -- not hang this call forever"
        );
        assert_eq!(attempts, 1, "a timed-out attempt is not retried");
        assert!(server.requests() <= 1, "{} requests", server.requests());
    }

    /// A connection that fails after the request was sent (the server read
    /// it and hung up without answering) is `Unknown` and not retried: the
    /// push service may have accepted it, so a retry could deliver it
    /// twice (DS-386). Deterministic: the server always reads the whole
    /// request before closing, so exactly one arrives.
    #[tokio::test]
    async fn a_connection_lost_after_the_request_is_unknown_and_not_retried() {
        let server = SilentServer::start(Silence::HangUp).await;

        let (outcome, attempts) = send_with_retries(
            &push_http_client(),
            &test_vapid_key(),
            "mailto:test@example.com",
            &server.subscription(),
            &payload(),
            test_options(),
            TEST_GENEROUS_TIMEOUT,
        )
        .await;

        assert_eq!(outcome, SendOutcome::Unknown);
        assert_eq!(attempts, 1);
        assert_eq!(server.requests(), 1);
    }

    /// A connect failure never sent the request, so it is retried, up to
    /// `MAX_ATTEMPTS` in all. The port is one nothing listens on: bound,
    /// then closed.
    #[tokio::test]
    async fn a_connect_failure_is_retried() {
        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            listener.local_addr().expect("addr").port()
        };
        let subscription = PushSubscriptionRow {
            id: 1,
            endpoint: format!("http://127.0.0.1:{port}/push-endpoint"),
            p256dh: TEST_P256DH.to_string(),
            auth: TEST_AUTH.to_string(),
        };

        let (outcome, attempts) = send_with_retries(
            &push_http_client(),
            &test_vapid_key(),
            "mailto:test@example.com",
            &subscription,
            &payload(),
            test_options(),
            TEST_GENEROUS_TIMEOUT,
        )
        .await;

        assert_eq!(outcome, SendOutcome::TransientFailure);
        assert_eq!(attempts, MAX_ATTEMPTS);
    }

    /// A 5xx or 429 is the push service saying it did not take the push,
    /// so it is retried; another 4xx would only fail again, so it is not.
    /// A 201 needs no body read to count as sent.
    #[tokio::test]
    async fn only_a_server_error_or_429_is_retried() {
        for (status, outcome, attempts) in [
            (503, SendOutcome::TransientFailure, MAX_ATTEMPTS),
            (500, SendOutcome::TransientFailure, MAX_ATTEMPTS),
            (429, SendOutcome::TransientFailure, MAX_ATTEMPTS),
            (400, SendOutcome::TransientFailure, 1),
            (413, SendOutcome::TransientFailure, 1),
            (410, SendOutcome::Expired, 1),
            (201, SendOutcome::Sent, 1),
        ] {
            let server = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("POST"))
                .respond_with(wiremock::ResponseTemplate::new(status))
                .expect(u64::from(attempts))
                .mount(&server)
                .await;
            let subscription = PushSubscriptionRow {
                id: 1,
                endpoint: format!("{}/push-endpoint", server.uri()),
                p256dh: TEST_P256DH.to_string(),
                auth: TEST_AUTH.to_string(),
            };
            let sent = send_with_retries(
                &push_http_client(),
                &test_vapid_key(),
                "mailto:test@example.com",
                &subscription,
                &payload(),
                test_options(),
                TEST_GENEROUS_TIMEOUT,
            )
            .await;
            assert_eq!(sent, (outcome, attempts), "status {status}");
        }
    }

    /// Per-attempt timeout for tests that are not about the timeout: far
    /// beyond anything a local server needs, so a loaded CI runner can't
    /// turn a slow attempt into a `TimedOut`.
    const TEST_GENEROUS_TIMEOUT: Duration = Duration::from_secs(60);

    /// What a [`SilentServer`] does after reading a request.
    #[derive(Clone, Copy)]
    enum Silence {
        /// Keep the connection open and never answer.
        Hang,
        /// Close the connection without answering.
        HangUp,
    }

    /// A local HTTP/1 server that reads whole requests and never answers
    /// them, counting the requests it read.
    struct SilentServer {
        port: u16,
        requests: Arc<std::sync::atomic::AtomicUsize>,
        task: tokio::task::JoinHandle<()>,
    }

    impl SilentServer {
        async fn start(silence: Silence) -> Self {
            use tokio::io::AsyncReadExt as _;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let port = listener.local_addr().expect("addr").port();
            let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counted = Arc::clone(&requests);
            let task = tokio::spawn(async move {
                let mut held = Vec::new();
                while let Ok((mut stream, _)) = listener.accept().await {
                    let mut request = Vec::new();
                    let mut buf = [0_u8; 4096];
                    while !whole_request(&request) {
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => request.extend_from_slice(&buf[..n]),
                        }
                    }
                    if whole_request(&request) {
                        counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    match silence {
                        Silence::Hang => held.push(stream),
                        Silence::HangUp => drop(stream),
                    }
                }
            });
            Self {
                port,
                requests,
                task,
            }
        }

        fn subscription(&self) -> PushSubscriptionRow {
            PushSubscriptionRow {
                id: 1,
                endpoint: format!("http://127.0.0.1:{}/push-endpoint", self.port),
                p256dh: TEST_P256DH.to_string(),
                auth: TEST_AUTH.to_string(),
            }
        }

        fn requests(&self) -> usize {
            self.requests.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl Drop for SilentServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    /// Whether `request` holds a whole HTTP/1 request: its headers and a
    /// `content-length` body.
    fn whole_request(request: &[u8]) -> bool {
        let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
            return false;
        };
        let headers = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
        let length = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(0);
        request.len() >= end + 4 + length
    }

    /// L9 follow-up: the send path's OWN connector refuses a name that
    /// resolves to loopback, independently of `send_to_subscription`'s
    /// up-front `validate_outbound_url` (which this entry point skips) --
    /// so a rebinding name that passed that check can't be dialled at a
    /// private address on the second resolution. The mock listens on
    /// 127.0.0.1; addressing it as `localhost` must never reach it.
    #[tokio::test]
    async fn the_send_connector_refuses_a_name_that_resolves_to_loopback() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(wiremock::ResponseTemplate::new(201))
            .expect(0)
            .mount(&server)
            .await;

        let port = server.address().port();
        let subscription = PushSubscriptionRow {
            id: 1,
            endpoint: format!("http://localhost:{port}/push-endpoint"),
            p256dh: TEST_P256DH.to_string(),
            auth: TEST_AUTH.to_string(),
        };

        let outcome = send_to_subscription_with_timeout(
            &push_http_client(),
            &test_vapid_key(),
            "mailto:test@example.com",
            &subscription,
            &payload(),
            test_options(),
            TEST_GENEROUS_TIMEOUT,
        )
        .await;

        assert_eq!(outcome, SendOutcome::TransientFailure);
    }

    /// The swapped-in transport still speaks the protocol: a 201 is Sent
    /// and a 410 is Expired, straight through `web_push::parse_response`.
    #[tokio::test]
    async fn the_reqwest_transport_classifies_success_and_gone() {
        for (status, expected) in [(201, SendOutcome::Sent), (410, SendOutcome::Expired)] {
            let server = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("POST"))
                .and(wiremock::matchers::header("content-encoding", "aes128gcm"))
                .respond_with(wiremock::ResponseTemplate::new(status))
                .expect(1)
                .mount(&server)
                .await;
            let subscription = PushSubscriptionRow {
                id: 1,
                endpoint: format!("{}/push-endpoint", server.uri()),
                p256dh: TEST_P256DH.to_string(),
                auth: TEST_AUTH.to_string(),
            };
            let outcome = send_to_subscription_with_timeout(
                &push_http_client(),
                &test_vapid_key(),
                "mailto:test@example.com",
                &subscription,
                &payload(),
                test_options(),
                TEST_GENEROUS_TIMEOUT,
            )
            .await;
            assert_eq!(outcome, expected, "status {status}");
        }
    }

    fn test_options() -> DeliveryOptions {
        DeliveryOptions {
            ttl_secs: DEFAULT_PUSH_TTL_SECS,
            urgency: Urgency::Normal,
        }
    }

    /// Every push carries its `TTL` and `Urgency` headers, so a push
    /// service drops it after the TTL instead of web-push's own 28-day
    /// default.
    #[tokio::test]
    async fn a_push_carries_its_ttl_and_urgency() {
        for (options, ttl, urgency) in [
            (test_options(), "3600", "normal"),
            (
                DeliveryOptions {
                    ttl_secs: 600,
                    urgency: Urgency::High,
                },
                "600",
                "high",
            ),
        ] {
            let server = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("POST"))
                .and(wiremock::matchers::header("ttl", ttl))
                .and(wiremock::matchers::header("urgency", urgency))
                .respond_with(wiremock::ResponseTemplate::new(201))
                .expect(1)
                .mount(&server)
                .await;
            let subscription = PushSubscriptionRow {
                id: 1,
                endpoint: format!("{}/push-endpoint", server.uri()),
                p256dh: TEST_P256DH.to_string(),
                auth: TEST_AUTH.to_string(),
            };
            let outcome = send_to_subscription_with_timeout(
                &push_http_client(),
                &test_vapid_key(),
                "mailto:test@example.com",
                &subscription,
                &payload(),
                options,
                TEST_GENEROUS_TIMEOUT,
            )
            .await;
            assert_eq!(outcome, SendOutcome::Sent, "{options:?}");
        }
    }

    /// Every push's `Topic` is its tag, so a push service holding one for
    /// an offline phone replaces it with a newer one (a moved leave-by, a
    /// resend after an unknown outcome); a tag that can't be a topic is sent
    /// without one.
    #[tokio::test]
    async fn every_push_sends_its_tag_as_its_topic() {
        for (tag, topic) in [
            ("journey-leg-leave-11", Some("journey-leg-leave-11")),
            ("train-42", Some("train-42")),
            (
                "regular-summary-2026-11-09",
                Some("regular-summary-2026-11-09"),
            ),
            ("has a space", None),
            ("a-tag-that-is-much-longer-than-thirty-two", None),
        ] {
            let server = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("POST"))
                .and(wiremock::matchers::header("ttl", "480"))
                .and(wiremock::matchers::header("urgency", "high"))
                .respond_with(wiremock::ResponseTemplate::new(201))
                .expect(1)
                .mount(&server)
                .await;
            let subscription = PushSubscriptionRow {
                id: 1,
                endpoint: format!("{}/push-endpoint", server.uri()),
                p256dh: TEST_P256DH.to_string(),
                auth: TEST_AUTH.to_string(),
            };
            let payload = NotificationPayload {
                tag: tag.to_string(),
                ..payload()
            };
            let options = DeliveryOptions {
                ttl_secs: 480,
                urgency: Urgency::High,
            };
            let outcome = send_to_subscription_with_timeout(
                &push_http_client(),
                &test_vapid_key(),
                "mailto:test@example.com",
                &subscription,
                &payload,
                options,
                TEST_GENEROUS_TIMEOUT,
            )
            .await;
            assert_eq!(outcome, SendOutcome::Sent, "{tag}");
            let requests = server.received_requests().await.expect("requests");
            let sent = requests[0]
                .headers
                .get("topic")
                .map(|value| value.to_str().unwrap().to_string());
            assert_eq!(sent.as_deref(), topic, "{tag}");
        }
    }

    #[test]
    fn renotify_is_in_the_payload_only_when_set() {
        let plain = serde_json::to_value(payload()).unwrap();
        assert!(plain.get("renotify").is_none());
        let moved = serde_json::to_value(NotificationPayload {
            renotify: true,
            ..payload()
        })
        .unwrap();
        assert_eq!(moved["renotify"], serde_json::json!(true));
    }

    #[test]
    fn an_until_ttl_counts_down_to_its_deadline_and_stops_at_its_cap() {
        let now = chrono::Utc::now();
        let until = Ttl::Until {
            deadline: now + chrono::Duration::seconds(300),
            max_secs: 900,
        };
        assert_eq!(until.secs_at(3600, now), 300);
        assert_eq!(until.secs_at(3600, now - chrono::Duration::hours(1)), 900);
        assert_eq!(until.secs_at(3600, now + chrono::Duration::hours(1)), 0);
        assert_eq!(Ttl::Configured.secs_at(3600, now), 3600);
        assert_eq!(Ttl::Fixed(60).secs_at(3600, now), 60);
    }

    /// `Pusher` sends a `Ttl::Configured` class with its configured TTL
    /// and a `Ttl::Fixed` one with the class's own.
    #[tokio::test]
    async fn the_pusher_sends_its_configured_ttl_unless_the_class_fixes_one() {
        for (class, ttl, urgency) in [
            (
                PushClass {
                    urgency: Urgency::High,
                    ttl: Ttl::Configured,
                },
                "120",
                "high",
            ),
            (
                PushClass {
                    urgency: Urgency::Normal,
                    ttl: Ttl::Fixed(2_419_200),
                },
                "2419200",
                "normal",
            ),
        ] {
            let server = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("POST"))
                .and(wiremock::matchers::header("ttl", ttl))
                .and(wiremock::matchers::header("urgency", urgency))
                .respond_with(wiremock::ResponseTemplate::new(201))
                .expect(1)
                .mount(&server)
                .await;
            let pusher = Pusher {
                ttl_secs: 120,
                ..Pusher::for_local_tests(TEST_GENEROUS_TIMEOUT)
            };
            let subscription = PushSubscriptionRow {
                id: 1,
                endpoint: format!("{}/push-endpoint", server.uri()),
                p256dh: TEST_P256DH.to_string(),
                auth: TEST_AUTH.to_string(),
            };
            assert_eq!(
                pusher.send(&subscription, &payload(), class).await,
                SendOutcome::Sent,
                "{class:?}"
            );
        }
    }

    fn payload() -> NotificationPayload {
        NotificationPayload {
            title: "title".to_string(),
            body: "body".to_string(),
            url: "https://example.com/".to_string(),
            tag: "tag".to_string(),
            renotify: false,
        }
    }

    /// **L9 regression test.** A subscription whose `endpoint` resolves to
    /// a private/internal address at SEND time -- imagine it passed
    /// `crates/api`'s registration-time check when the DNS name still
    /// pointed at a public IP, then got rebound -- must never reach the
    /// real web-push send at all: `send_to_subscription` has to catch it
    /// and refuse, not merely rely on `crates/api` having caught it once
    /// in the past. Using a bare private IPv4 literal (not a hostname)
    /// means this needs no real DNS/network access to fail the check --
    /// `ds_runtime::outbound_endpoint_guard::validate_outbound_url` parses an
    /// IP literal locally.
    #[tokio::test]
    async fn refuses_to_send_when_the_endpoint_now_resolves_to_a_private_address() {
        let subscription = PushSubscriptionRow {
            id: 1,
            endpoint: "https://10.0.0.7/push".to_string(),
            p256dh: "p256dh".to_string(),
            auth: "auth".to_string(),
        };

        let outcome = send_to_subscription(
            &push_http_client(),
            &test_vapid_key(),
            "mailto:test@example.com",
            &subscription,
            &payload(),
            test_options(),
        )
        .await;

        assert_eq!(
            outcome,
            SendOutcome::TransientFailure,
            "a send-time-disallowed endpoint must be refused before any VAPID/web-push work \
             happens, and must not be classified as Expired (it might be a transient resolver \
             blip, not a genuinely dead subscription)"
        );
    }
}
