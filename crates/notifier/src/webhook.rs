//! Outbound webhooks (DS-415): a second delivery channel beside Web Push.
//!
//! A webhook destination is a user's own URL (`webhook_destinations`).
//! The push queue hands every notification job to the user's enabled
//! destinations as well as their push subscriptions, so everything upstream
//! of the queue (the decision logic, quiet hours, pauses, per-train
//! switches, cooldowns) applies to a webhook exactly as it does to a push.
//! Nothing here decides whether to notify.
//!
//! # Presets
//!
//! A destination's `kind` is a preset: it shapes the request and nothing
//! else ([`build_request`]). `generic` posts signed JSON; `ntfy` and
//! `gotify` post what those servers expect. A new preset (Discord, Slack,
//! Pushover, Telegram) is a new [`Kind`] variant, a branch in
//! [`build_request`] and a value in the table's `kind` check.
//!
//! # What is sent
//!
//! The same text a push shows (title, body, link, tag), the event kind,
//! whether it is urgent and when it was decided, plus a credits link. No
//! user id, no train or journey ids and no location. The account security
//! notices (an app was connected, a token was created) are never sent to a
//! webhook: they are about the account, not the journey, and the user's own
//! URL is a third party.
//!
//! # Signing (generic preset)
//!
//! `X-DS-Signature: v1=<hex>` is HMAC-SHA256 over `"<timestamp>.<body>"`
//! keyed with the destination's secret, where `<timestamp>` is the
//! `X-DS-Timestamp` header (Unix seconds). A receiver recomputes it and
//! rejects a stale timestamp. `X-DS-Delivery` is stable across a
//! delivery's retries so a receiver can de-duplicate.
//!
//! # Safety
//!
//! The URL is user-chosen, so the transport is
//! `ds_runtime::outbound_endpoint_guard`'s: https only; every host name is
//! resolved through the public-only resolver at connect time (private,
//! loopback, link-local, metadata and tailnet 100.64.0.0/10 addresses are
//! refused, which also defeats DNS rebinding); no redirects; no proxy.
//! Attempts are bounded ([`ATTEMPT_TIMEOUT`]) and so is their number. A
//! destination that fails [`crate::config::Config::webhook_disable_after_failures`]
//! deliveries in a row is disabled with a reason the settings page shows.

use std::time::Duration;

use base64::Engine as _;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::push_queue::Delivered;
use crate::send::NotificationPayload;

/// Per-attempt bound. Three attempts plus their pauses stay inside the
/// push queue's 30 s per-destination budget.
pub(crate) const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(8);

/// Attempts per delivery, and the pause before each retry.
const MAX_ATTEMPTS: usize = 3;
const BACKOFF: [Duration; 2] = [Duration::from_millis(500), Duration::from_secs(2)];

/// The generic preset's payload schema version.
const SCHEMA_VERSION: u32 = 1;

/// The destination a delivery needs, its secret already decrypted.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct WebhookDestination {
    pub id: i64,
    pub kind: String,
    pub url: String,
    pub secret: Option<String>,
}

/// Never prints the secret or the URL (a topic or path can be a secret).
impl std::fmt::Debug for WebhookDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookDestination")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Generic,
    Ntfy,
    Gotify,
}

impl Kind {
    pub(crate) fn parse(kind: &str) -> Option<Kind> {
        match kind {
            "generic" => Some(Kind::Generic),
            "ntfy" => Some(Kind::Ntfy),
            "gotify" => Some(Kind::Gotify),
            _ => None,
        }
    }
}

/// What a delivery says: [`NotificationPayload`] plus the event kind.
#[derive(Clone, Debug)]
pub(crate) struct WebhookEvent {
    pub event: &'static str,
    pub title: String,
    pub body: String,
    /// The path the push opens (`/journeys/1`).
    pub path: String,
    pub tag: String,
    pub urgent: bool,
    pub decided_at: DateTime<Utc>,
}

/// The event name a notification is sent as, or `None` for the ones a
/// webhook never gets (see the module doc).
pub(crate) fn event_name(delivered: &Delivered) -> Option<&'static str> {
    match delivered {
        Delivered::Line { .. } => Some("line_status"),
        Delivered::Train { .. } => Some("train_update"),
        Delivered::SkippedStop { .. } => Some("stop_skipped"),
        Delivered::Unmatched { .. } => Some("journey_attention"),
        Delivered::JourneyBreak { .. } => Some("journey_break"),
        Delivered::LeaveBy { .. } => Some("leave_by"),
        Delivered::MorningSummary { .. } => Some("morning_summary"),
        Delivered::GrantCreated { .. } | Delivered::TokenCreated { .. } => None,
    }
}

impl WebhookEvent {
    pub(crate) fn from_job(
        delivered: &Delivered,
        payload: &NotificationPayload,
        decided_at: DateTime<Utc>,
    ) -> Option<WebhookEvent> {
        let event = event_name(delivered)?;
        Some(WebhookEvent {
            event,
            title: payload.title.clone(),
            body: payload.body.clone(),
            path: payload.url.clone(),
            tag: payload.tag.clone(),
            urgent: matches!(delivered.push_class().urgency, web_push::Urgency::High),
            decided_at,
        })
    }
}

/// A request ready to send. Built without I/O so each preset's shape is
/// tested directly.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BuiltRequest {
    pub url: String,
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BuildError {
    UnknownKind,
    MissingSecret,
    BadUrl,
    Encode,
}

#[derive(Serialize)]
struct GenericBody<'a> {
    schema_version: u32,
    delivery_id: &'a str,
    event: &'a str,
    occurred_at: String,
    title: &'a str,
    body: &'a str,
    url: String,
    tag: &'a str,
    urgent: bool,
    attribution: Attribution,
}

#[derive(Serialize)]
struct Attribution {
    text: &'static str,
    url: Option<String>,
}

const ATTRIBUTION_TEXT: &str = "Sent by Distant Signal. Data credits are at the link.";

/// `base` joined with `path`, when `base` is set; otherwise `path` alone
/// (a receiver can't open it, but it still names the page).
fn absolute(base: Option<&str>, path: &str) -> String {
    match base {
        Some(base) if path.starts_with('/') => format!("{}{path}", base.trim_end_matches('/')),
        _ => path.to_string(),
    }
}

/// A delivery's id: stable across its retries, different per destination
/// and per decision.
pub(crate) fn delivery_id(dest_id: i64, event: &WebhookEvent) -> String {
    let mut hasher = Sha256::new();
    hasher.update(dest_id.to_be_bytes());
    hasher.update(event.tag.as_bytes());
    hasher.update(event.decided_at.timestamp_millis().to_be_bytes());
    hex::encode(&hasher.finalize()[..12])
}

/// `v1=<hex HMAC-SHA256 of "<timestamp>.<body>">`.
pub(crate) fn sign(secret: &str, timestamp: i64, body: &[u8]) -> Option<String> {
    // HMAC accepts any key length, so this never returns None in practice.
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).ok()?;
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    Some(format!("v1={}", hex::encode(mac.finalize().into_bytes())))
}

/// A header value for text that may not be ASCII: as is when it is,
/// otherwise RFC 2047 base64 (what ntfy documents for UTF-8 headers).
fn header_text(text: &str) -> Vec<u8> {
    let flat: String = text
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    if flat.is_ascii() {
        flat.into_bytes()
    } else {
        format!(
            "=?UTF-8?B?{}?=",
            base64::engine::general_purpose::STANDARD.encode(flat.as_bytes())
        )
        .into_bytes()
    }
}

pub(crate) fn build_request(
    dest: &WebhookDestination,
    event: &WebhookEvent,
    site_url: Option<&str>,
    now: DateTime<Utc>,
) -> Result<BuiltRequest, BuildError> {
    let kind = Kind::parse(&dest.kind).ok_or(BuildError::UnknownKind)?;
    let click = absolute(site_url, &event.path);
    match kind {
        Kind::Generic => {
            let secret = dest.secret.as_deref().ok_or(BuildError::MissingSecret)?;
            let id = delivery_id(dest.id, event);
            let body = serde_json::to_vec(&GenericBody {
                schema_version: SCHEMA_VERSION,
                delivery_id: &id,
                event: event.event,
                occurred_at: event
                    .decided_at
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                title: &event.title,
                body: &event.body,
                url: click,
                tag: &event.tag,
                urgent: event.urgent,
                attribution: Attribution {
                    text: ATTRIBUTION_TEXT,
                    url: site_url.map(|base| absolute(Some(base), "/attribution")),
                },
            })
            .map_err(|_| BuildError::Encode)?;
            let timestamp = now.timestamp();
            let signature = sign(secret, timestamp, &body).ok_or(BuildError::MissingSecret)?;
            Ok(BuiltRequest {
                url: dest.url.clone(),
                headers: vec![
                    ("content-type".into(), b"application/json".to_vec()),
                    ("x-ds-event".into(), event.event.as_bytes().to_vec()),
                    ("x-ds-delivery".into(), id.into_bytes()),
                    ("x-ds-timestamp".into(), timestamp.to_string().into_bytes()),
                    ("x-ds-signature".into(), signature.into_bytes()),
                ],
                body,
            })
        }
        Kind::Ntfy => {
            let mut headers = vec![
                ("content-type".into(), b"text/plain; charset=utf-8".to_vec()),
                ("title".into(), header_text(&event.title)),
                (
                    "priority".into(),
                    if event.urgent {
                        b"high".to_vec()
                    } else {
                        b"default".to_vec()
                    },
                ),
                ("tags".into(), b"train".to_vec()),
            ];
            if site_url.is_some() {
                headers.push(("click".into(), click.into_bytes()));
            }
            if let Some(token) = dest.secret.as_deref() {
                headers.push((
                    "authorization".into(),
                    format!("Bearer {token}").into_bytes(),
                ));
            }
            Ok(BuiltRequest {
                url: dest.url.clone(),
                headers,
                body: event.body.clone().into_bytes(),
            })
        }
        Kind::Gotify => {
            let token = dest.secret.as_deref().ok_or(BuildError::MissingSecret)?;
            let mut url = url::Url::parse(&dest.url).map_err(|_| BuildError::BadUrl)?;
            // The destination URL is the server's base; the preset owns the
            // path.
            url.set_path(&format!("{}/message", url.path().trim_end_matches('/')));
            url.set_query(None);
            let mut extras = serde_json::json!({});
            if site_url.is_some() {
                extras = serde_json::json!({
                    "client::notification": { "click": { "url": click } },
                    "client::display": { "contentType": "text/plain" },
                });
            }
            let body = serde_json::to_vec(&serde_json::json!({
                "title": event.title,
                "message": event.body,
                "priority": if event.urgent { 8 } else { 5 },
                "extras": extras,
            }))
            .map_err(|_| BuildError::Encode)?;
            Ok(BuiltRequest {
                url: url.into(),
                headers: vec![
                    ("content-type".into(), b"application/json".to_vec()),
                    ("x-gotify-key".into(), token.as_bytes().to_vec()),
                ],
                body,
            })
        }
    }
}

/// How a delivery ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WebhookOutcome {
    Delivered,
    /// Not delivered; the short reason the settings page shows.
    Failed(String),
}

/// Whether another attempt could help: a refused or reset connection, a
/// timeout, a 408, 429 or 5xx. Other 4xx answers (a wrong token, a gone
/// topic) will not change on a retry.
fn retryable_status(status: u16) -> bool {
    status == 408 || status == 429 || status >= 500
}

/// The reason shown for a failed attempt. Never includes the URL, any
/// response body or the status code: the settings page shows it to the
/// user as it is (a sentence with its full stop), and it also lands in
/// logs.
fn status_reason(status: u16) -> String {
    match status {
        429 => "The destination is limiting how often we can send.",
        408 | 500.. => "The destination had a problem receiving it.",
        _ => "The destination refused it. Check the address and any token.",
    }
    .to_string()
}

pub(crate) struct WebhookSender {
    client: reqwest::Client,
    site_url: Option<String>,
    /// Whether to check the address first. Always true outside the
    /// `insecure-test-egress` test seam.
    guarded: bool,
}

impl WebhookSender {
    pub(crate) fn new(site_url: Option<String>) -> anyhow::Result<WebhookSender> {
        let client = ds_runtime::outbound_endpoint_guard::public_only_client_builder()
            .timeout(ATTEMPT_TIMEOUT)
            .user_agent("distant-signal-webhooks/1")
            .build()?;
        Ok(WebhookSender {
            client,
            site_url,
            guarded: true,
        })
    }

    /// TEST SEAM (`insecure-test-egress`, debug builds only): a sender with
    /// a plain client and no address check, to reach servers on 127.0.0.1.
    #[cfg(all(test, feature = "insecure-test-egress"))]
    pub(crate) fn unguarded_for_tests(site_url: Option<String>) -> WebhookSender {
        WebhookSender {
            client: reqwest::Client::builder()
                .timeout(ATTEMPT_TIMEOUT)
                .redirect(reqwest::redirect::Policy::none())
                .no_proxy()
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            site_url,
            guarded: false,
        }
    }

    pub(crate) async fn send(
        &self,
        dest: &WebhookDestination,
        event: &WebhookEvent,
    ) -> WebhookOutcome {
        // A literal IP never reaches the resolver, so it is checked here;
        // a name is re-checked at connect time by the client's resolver.
        if self.guarded
            && let Err(reason) =
                ds_runtime::outbound_endpoint_guard::validate_outbound_url(&dest.url).await
        {
            return WebhookOutcome::Failed(reason);
        }
        let mut last = WebhookOutcome::Failed("The message couldn't be sent.".into());
        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(BACKOFF[attempt - 1]).await;
            }
            // Rebuilt per attempt so a retry carries a fresh timestamp.
            let request = match build_request(dest, event, self.site_url.as_deref(), Utc::now()) {
                Ok(request) => request,
                Err(BuildError::UnknownKind) => {
                    return WebhookOutcome::Failed(
                        "This kind of destination isn't supported.".into(),
                    );
                }
                Err(BuildError::MissingSecret) => {
                    return WebhookOutcome::Failed("The destination is missing its token.".into());
                }
                Err(BuildError::Encode) => {
                    return WebhookOutcome::Failed("Couldn't build the message.".into());
                }
                Err(BuildError::BadUrl) => {
                    return WebhookOutcome::Failed("The address isn't valid.".into());
                }
            };
            let mut builder = self.client.post(&request.url).body(request.body);
            for (name, value) in request.headers {
                let Ok(value) = reqwest::header::HeaderValue::from_bytes(&value) else {
                    return WebhookOutcome::Failed("Couldn't build the message.".into());
                };
                builder = builder.header(name, value);
            }
            match builder.send().await {
                Ok(response) => {
                    let status = response.status().as_u16();
                    if response.status().is_success() {
                        return WebhookOutcome::Delivered;
                    }
                    last = WebhookOutcome::Failed(status_reason(status));
                    if !retryable_status(status) {
                        return last;
                    }
                }
                Err(err) if err.is_timeout() => {
                    last = WebhookOutcome::Failed("The destination didn't answer in time.".into());
                }
                Err(err) if err.is_connect() => {
                    last = WebhookOutcome::Failed(
                        "Couldn't connect. The address may not be a public one.".into(),
                    );
                }
                Err(_) => {
                    last = WebhookOutcome::Failed("The message couldn't be sent.".into());
                }
            }
        }
        last
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(urgent: bool) -> WebhookEvent {
        WebhookEvent {
            event: "train_update",
            title: "Delayed \u{2013} 09:15 to Leeds".into(),
            body: "About 20 minutes late.".into(),
            path: "/track/7".into(),
            tag: "train-7".into(),
            urgent,
            decided_at: DateTime::parse_from_rfc3339("2026-11-09T09:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        }
    }

    fn dest(kind: &str, url: &str, secret: Option<&str>) -> WebhookDestination {
        WebhookDestination {
            id: 3,
            kind: kind.into(),
            url: url.into(),
            secret: secret.map(Into::into),
        }
    }

    fn header<'a>(request: &'a BuiltRequest, name: &str) -> Option<&'a [u8]> {
        request
            .headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_slice())
    }

    #[test]
    fn a_destination_never_debug_prints_its_secret_or_url() {
        let shown = format!(
            "{:?}",
            dest("ntfy", "https://ntfy.example/topic-x", Some("tk_secret"))
        );
        assert!(
            !shown.contains("tk_secret") && !shown.contains("topic-x"),
            "{shown}"
        );
    }

    #[test]
    fn hmac_matches_a_known_vector() {
        // RFC 4231 test case 2, over "1.hi" so the timestamp join is covered
        // by recomputing it independently.
        let mut mac = Hmac::<Sha256>::new_from_slice(b"Jefe").unwrap();
        mac.update(b"what do ya want for nothing?");
        assert_eq!(
            hex::encode(mac.finalize().into_bytes()),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        let signature = sign("Jefe", 1, b"hi").unwrap();
        let mut mac = Hmac::<Sha256>::new_from_slice(b"Jefe").unwrap();
        mac.update(b"1.hi");
        assert_eq!(
            signature,
            format!("v1={}", hex::encode(mac.finalize().into_bytes()))
        );
    }

    /// The reason is shown to the user as it is: a sentence with its full
    /// stop, and never a status code.
    #[test]
    fn failure_reasons_are_sentences_without_status_codes() {
        for status in [400, 401, 403, 404, 408, 429, 500, 502, 503] {
            let reason = status_reason(status);
            assert!(reason.ends_with('.'), "{reason}");
            assert!(!reason.chars().any(|c| c.is_ascii_digit()), "{reason}");
        }
        assert!(status_reason(401).contains("refused"));
        assert!(status_reason(503).contains("problem receiving"));
    }

    #[test]
    fn generic_is_signed_json_with_credits() {
        let now = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        let request = build_request(
            &dest("generic", "https://hooks.example/x", Some("s3cret")),
            &event(true),
            Some("https://ds.example/"),
            now,
        )
        .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["schema_version"], 1);
        assert_eq!(body["event"], "train_update");
        assert_eq!(body["url"], "https://ds.example/track/7");
        assert_eq!(body["urgent"], true);
        assert_eq!(body["occurred_at"], "2026-11-09T09:00:00Z");
        assert_eq!(body["attribution"]["url"], "https://ds.example/attribution");
        // Minimised: nothing but the documented keys.
        let mut keys: Vec<_> = body.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                "attribution",
                "body",
                "delivery_id",
                "event",
                "occurred_at",
                "schema_version",
                "tag",
                "title",
                "urgent",
                "url"
            ]
        );
        assert_eq!(
            header(&request, "x-ds-signature").unwrap(),
            sign("s3cret", 1_800_000_000, &request.body)
                .unwrap()
                .as_bytes()
        );
        assert_eq!(header(&request, "x-ds-timestamp").unwrap(), b"1800000000");
    }

    #[test]
    fn delivery_id_is_stable_and_per_destination() {
        let a = delivery_id(1, &event(false));
        assert_eq!(a, delivery_id(1, &event(true)));
        assert_ne!(a, delivery_id(2, &event(false)));
    }

    #[test]
    fn generic_without_a_secret_is_refused() {
        let err = build_request(
            &dest("generic", "https://h.example", None),
            &event(false),
            None,
            Utc::now(),
        );
        assert_eq!(err.unwrap_err(), BuildError::MissingSecret);
    }

    #[test]
    fn ntfy_posts_plain_text_with_headers() {
        let request = build_request(
            &dest("ntfy", "https://ntfy.example/trains", Some("tk_abc")),
            &event(true),
            Some("https://ds.example"),
            Utc::now(),
        )
        .unwrap();
        assert_eq!(request.url, "https://ntfy.example/trains");
        assert_eq!(request.body, b"About 20 minutes late.");
        assert_eq!(header(&request, "priority").unwrap(), b"high");
        assert_eq!(header(&request, "tags").unwrap(), b"train");
        assert_eq!(
            header(&request, "click").unwrap(),
            b"https://ds.example/track/7"
        );
        assert_eq!(header(&request, "authorization").unwrap(), b"Bearer tk_abc");
        // A non-ASCII title is RFC 2047 encoded, not sent as raw bytes.
        let title = header(&request, "title").unwrap();
        assert!(title.is_ascii() && title.starts_with(b"=?UTF-8?B?"));
    }

    #[test]
    fn ntfy_without_token_or_site_url_sends_neither_header() {
        let request = build_request(
            &dest("ntfy", "https://ntfy.example/t", None),
            &event(false),
            None,
            Utc::now(),
        )
        .unwrap();
        assert!(header(&request, "authorization").is_none());
        assert!(header(&request, "click").is_none());
        assert_eq!(header(&request, "priority").unwrap(), b"default");
    }

    #[test]
    fn gotify_posts_message_with_the_app_token() {
        let request = build_request(
            &dest("gotify", "https://gotify.example/sub/", Some("AppTok")),
            &event(false),
            Some("https://ds.example"),
            Utc::now(),
        )
        .unwrap();
        assert_eq!(request.url, "https://gotify.example/sub/message");
        assert_eq!(header(&request, "x-gotify-key").unwrap(), b"AppTok");
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["message"], "About 20 minutes late.");
        assert_eq!(body["priority"], 5);
        assert_eq!(
            body["extras"]["client::notification"]["click"]["url"],
            "https://ds.example/track/7"
        );
    }

    #[test]
    fn unknown_kind_is_refused() {
        let err = build_request(
            &dest("discord", "https://h.example", Some("x")),
            &event(false),
            None,
            Utc::now(),
        );
        assert_eq!(err.unwrap_err(), BuildError::UnknownKind);
    }

    #[test]
    fn security_notices_are_never_sent() {
        assert_eq!(event_name(&Delivered::GrantCreated { grant_id: 1 }), None);
        assert_eq!(event_name(&Delivered::TokenCreated { token_id: 1 }), None);
        assert_eq!(
            event_name(&Delivered::Line {
                line_id: "x".into(),
                rank: 1
            }),
            Some("line_status")
        );
    }

    #[test]
    fn only_server_side_statuses_are_retried() {
        assert!(retryable_status(503) && retryable_status(429) && retryable_status(408));
        assert!(!retryable_status(401) && !retryable_status(404));
    }

    #[tokio::test]
    async fn a_private_literal_is_refused_before_any_connection() {
        let sender = WebhookSender::new(None).unwrap();
        for url in [
            "https://127.0.0.1/x",
            "https://10.0.0.5/x",
            "https://100.64.1.2/x",
            "https://169.254.169.254/latest",
            "https://[::1]/x",
            "http://example.com/x",
        ] {
            let outcome = sender
                .send(&dest("generic", url, Some("s")), &event(false))
                .await;
            assert!(matches!(outcome, WebhookOutcome::Failed(_)), "{url}");
        }
    }

    /// Failures accumulate to the threshold and turn the destination off
    /// with a reason; a success resets the run. Needs `DATABASE_URL`.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p notifier \
                webhook -- --ignored`"]
    async fn repeated_failures_turn_a_destination_off() {
        const FAILURE: &str = "The destination had a problem receiving it.";
        use crate::queries;
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let pool = sqlx::PgPool::connect(&url).await.unwrap();
        let user = "TEST-WEBHOOK-1";
        let _cleanup = ds_store::test_support::FixtureCleanup::new(
            &pool,
            [format!("DELETE FROM users WHERE id = '{user}'")],
        )
        .await;
        sqlx::query("INSERT INTO users (id, name) VALUES ($1, $1) ON CONFLICT DO NOTHING")
            .bind(user)
            .execute(&pool)
            .await
            .unwrap();
        let ring =
            ds_runtime::secret_box::KeyRing::parse(&format!("k1:{}=", "A".repeat(43))).unwrap();
        let sealed = ring.seal("whsec_stored").unwrap();
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO webhook_destinations \
               (user_id, kind, name, url, secret_ciphertext, secret_nonce, secret_key_id) \
             VALUES ($1, 'generic', 'n', 'https://hooks.example/x', $2, $3, $4) RETURNING id",
        )
        .bind(user)
        .bind(&sealed.ciphertext)
        .bind(&sealed.nonce)
        .bind(&sealed.key_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        // The stored value is ciphertext, and loads decrypted.
        let stored: Vec<u8> =
            sqlx::query_scalar("SELECT secret_ciphertext FROM webhook_destinations WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(!stored.windows(6).any(|w| w == b"stored"));
        let loaded = queries::webhook_destinations_for_user(&pool, user, &ring)
            .await
            .unwrap();
        assert_eq!(loaded[0].secret.as_deref(), Some("whsec_stored"));
        // A ring without the key that sealed it skips the destination.
        let other =
            ds_runtime::secret_box::KeyRing::parse(&format!("k2:{}Q=", "A".repeat(42))).unwrap();
        assert!(
            queries::webhook_destinations_for_user(&pool, user, &other)
                .await
                .unwrap()
                .is_empty(),
            "a key ring without the key reads no destination"
        );

        queries::record_webhook_result(&pool, id, Some(FAILURE), 3)
            .await
            .unwrap();
        queries::record_webhook_result(&pool, id, Some(FAILURE), 3)
            .await
            .unwrap();
        queries::record_webhook_result(&pool, id, None, 3)
            .await
            .unwrap();
        assert_eq!(
            queries::webhook_destinations_for_user(&pool, user, &ring)
                .await
                .unwrap()
                .len(),
            1
        );
        for _ in 0..3 {
            queries::record_webhook_result(&pool, id, Some(FAILURE), 3)
                .await
                .unwrap();
        }
        assert!(
            queries::webhook_destinations_for_user(&pool, user, &ring)
                .await
                .unwrap()
                .is_empty(),
            "a destination turned off after repeated failures is not read"
        );
        let reason: String =
            sqlx::query_scalar("SELECT disabled_reason FROM webhook_destinations WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            reason.starts_with("Turned off after repeated failures"),
            "{reason}"
        );
    }
}
