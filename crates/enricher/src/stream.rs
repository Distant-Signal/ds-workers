//! Thin wrapper around the `incident-text-changed` Redis Stream / consumer
//! group. Kept separate from extraction logic (`llm.rs`) and persistence
//! (`queries.rs`) so each can be understood and tested independently.

use std::time::Duration;

use common::redis_conn::RedisConn;
use redis::AsyncCommands;

const STREAM: &str = "incident-text-changed";
const GROUP: &str = "enricher";
const CONSUMER: &str = "enricher-1";

/// How long `read_one` waits server-side for a new entry
/// (`XREADGROUP ... BLOCK`).
const READ_BLOCK_MS: usize = 5000;

// Every command is bounded by `common::redis_conn::RESPONSE_TIMEOUT`, so a
// blocking read must return well within it or a quiet stream would look
// like a dead connection.
const _: () =
    assert!((READ_BLOCK_MS as u128) * 2 < common::redis_conn::RESPONSE_TIMEOUT.as_millis());

/// Creates the consumer group if it doesn't already exist, and the stream
/// itself if this is the very first run (`MKSTREAM`). `BUSYGROUP` (group
/// already exists) is the expected steady-state outcome and is swallowed,
/// not treated as an error.
pub(crate) async fn ensure_group(conn: &mut RedisConn) -> anyhow::Result<()> {
    ensure_group_on(conn, STREAM).await
}

/// `stream`-parameterized so `redis_tests` below can exercise this against
/// a throwaway per-test stream name rather than the real, fixed
/// `incident-text-changed` stream (`GROUP`/`CONSUMER` don't need the same
/// treatment: a consumer group is scoped to the stream it was created on,
/// so `enricher`/`enricher-1` on a test's own stream can never collide with
/// the same names on the real one).
async fn ensure_group_on(conn: &mut RedisConn, stream: &str) -> anyhow::Result<()> {
    ensure_group_at(conn, stream, "$").await.map(|_| ())
}

/// [`ensure_group_on`], the group (if created) starting after `start_id`.
/// Returns whether it was created (`false`: it already existed).
async fn ensure_group_at(
    conn: &mut RedisConn,
    stream: &str,
    start_id: &str,
) -> anyhow::Result<bool> {
    let result: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream)
        .arg(GROUP)
        .arg(start_id)
        .arg("MKSTREAM")
        .query_async(conn)
        .await;

    match result {
        Ok(()) => Ok(true),
        Err(err) if err.to_string().contains("BUSYGROUP") => Ok(false),
        Err(err) => Err(err.into()),
    }
}

/// `enricher_stream_group_position_restored_total`: times
/// [`recreate_group`] found the group still there but BEHIND the last entry
/// `main` read, and moved it forward (`XGROUP SETID`). Registered at 0 by
/// `main`.
pub(crate) const GROUP_RESTORED_METRIC: &str = "enricher_stream_group_position_restored_total";

/// The `enricher` group's `last-delivered-id` (`XINFO GROUPS`), or `None`
/// (logged) when it cannot be read. `main` keeps it, advanced to every
/// entry `read_one` hands back, for [`recreate_group`].
pub(crate) async fn group_last_delivered_id(conn: &mut RedisConn) -> Option<String> {
    group_last_delivered_id_on(conn, STREAM).await
}

/// See `ensure_group_on` for why this takes `stream` explicitly.
async fn group_last_delivered_id_on(conn: &mut RedisConn, stream: &str) -> Option<String> {
    let result: anyhow::Result<Option<String>> = async {
        let reply: Vec<redis::Value> = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(stream)
            .query_async(conn)
            .await?;
        for group in reply {
            let redis::Value::Array(fields) = group else {
                continue;
            };
            let mut name: Option<String> = None;
            let mut last: Option<String> = None;
            let mut iter = fields.into_iter();
            while let (Some(key), Some(value)) = (iter.next(), iter.next()) {
                let key: String = redis::from_redis_value(&key)?;
                match key.as_str() {
                    "name" => name = redis::from_redis_value(&value).ok(),
                    "last-delivered-id" => last = redis::from_redis_value(&value).ok(),
                    _ => {}
                }
            }
            if name.as_deref() == Some(GROUP) {
                return Ok(last);
            }
        }
        Ok(None)
    }
    .await;
    result.unwrap_or_else(|err| {
        tracing::warn!(error = ?err, "could not read the consumer group's last-delivered-id");
        None
    })
}

/// Recreates the consumer group after a failed read (a no-op `BUSYGROUP`
/// when it still exists) at [`recreate_start_id`], rather than at the
/// stream's tail: entries `api` wrote between Redis coming back empty and
/// this call are then read, not left for the hourly sweep, and a group
/// deleted by hand does not replay the whole stream. `last_delivered` is
/// `main`'s copy of the group's position, refreshed from Redis afterwards.
/// Entries the lost group had delivered but not `ACKed` are not redelivered
/// (the sweep still backstops them).
///
/// When the group still exists but stands BEHIND `last_delivered` (Redis
/// restarted and reloaded an older copy of its data; 2026-10-09 redelivery
/// review), it is moved forward to it, so incidents already extracted are
/// not handed out again: `common::redis_group::restore_group_position`.
pub(crate) async fn recreate_group(
    conn: &mut RedisConn,
    last_delivered: &mut Option<String>,
) -> anyhow::Result<()> {
    recreate_group_on(conn, STREAM, last_delivered).await
}

/// See `ensure_group_on` for why this takes `stream` explicitly.
async fn recreate_group_on(
    conn: &mut RedisConn,
    stream: &str,
    last_delivered: &mut Option<String>,
) -> anyhow::Result<()> {
    let exists: bool = conn.exists(stream).await?;
    let last_generated = if exists {
        let info: Vec<redis::Value> = redis::cmd("XINFO")
            .arg("STREAM")
            .arg(stream)
            .query_async(conn)
            .await?;
        let mut last = None;
        let mut iter = info.into_iter();
        while let (Some(key), Some(value)) = (iter.next(), iter.next()) {
            let key: String = redis::from_redis_value(&key)?;
            if key == "last-generated-id" {
                last = redis::from_redis_value::<Option<String>>(&value)?;
            }
        }
        Some(last.unwrap_or_else(|| "0-0".to_string()))
    } else {
        None
    };
    let start_id = recreate_start_id(last_delivered.as_deref(), last_generated.as_deref());
    let created = ensure_group_at(conn, stream, &start_id).await?;
    if !created
        && let Some(remembered) = last_delivered.as_deref()
        && let common::redis_group::GroupPosition::Restored { was } =
            common::redis_group::restore_group_position(conn, stream, GROUP, remembered).await?
    {
        tracing::warn!(
            stream,
            group = GROUP,
            was,
            restored_to = remembered,
            "consumer group went backwards (Redis reloaded an older copy of its data); moved \
             it forward to the last entry read, so those incidents are not handed out again"
        );
        metrics::counter!(common::metrics::metric_name(GROUP_RESTORED_METRIC)).increment(1);
    }
    if let Some(id) = group_last_delivered_id_on(conn, stream).await {
        *last_delivered = Some(id);
    }
    Ok(())
}

/// Where a lost consumer group is recreated -- the same rule as
/// `movement_feed::redis_stream::recreate_start_id`, which documents it in
/// full: after `last_delivered` if the stream still holds ids at or past
/// it (intact stream, or one recreated since on a server whose clock moved
/// on), `0` if the stream is missing or its `last-generated-id` is behind
/// `last_delivered` (a new stream), `$` if the position is unknown.
fn recreate_start_id(last_delivered: Option<&str>, stream_last_generated: Option<&str>) -> String {
    let Some(last_generated) = stream_last_generated else {
        return "0".to_string();
    };
    match last_delivered {
        None => "$".to_string(),
        Some(last) if stream_id_less_than(last_generated, last) => "0".to_string(),
        Some(last) => last.to_string(),
    }
}

/// Stream ids compared as `(ms, seq)` integer pairs, never as strings.
fn stream_id_less_than(a: &str, b: &str) -> bool {
    fn parts(id: &str) -> (u64, u64) {
        let mut it = id.splitn(2, '-');
        let ms = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        let seq = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        (ms, seq)
    }
    parts(a) < parts(b)
}

/// Reads at most one new entry for this consumer, blocking up to 5s if
/// none are immediately available. Returns the entry's own stream ID
/// (needed to `ack`) paired with the `incident_id` field it carries.
pub(crate) async fn read_one(conn: &mut RedisConn) -> anyhow::Result<Option<(String, String)>> {
    read_one_on(conn, STREAM).await
}

/// See `ensure_group_on` for why this takes `stream` explicitly.
async fn read_one_on(
    conn: &mut RedisConn,
    stream: &str,
) -> anyhow::Result<Option<(String, String)>> {
    let reply: redis::streams::StreamReadReply = conn
        .xread_options(
            &[stream],
            &[">"],
            &redis::streams::StreamReadOptions::default()
                .group(GROUP, CONSUMER)
                .count(1)
                .block(READ_BLOCK_MS),
        )
        .await?;

    // `count(1)` means at most one entry is ever returned, so this flattens
    // to a single `Option` rather than nesting nested loops that would only
    // ever run their body once (which trips clippy::never_loop).
    let Some(entry) = reply
        .keys
        .into_iter()
        .flat_map(|stream_key| stream_key.ids)
        .next()
    else {
        return Ok(None);
    };

    if let Some(incident_id) = entry
        .map
        .get("incident_id")
        .and_then(|v| redis::from_redis_value::<String>(v).ok())
    {
        Ok(Some((entry.id, incident_id)))
    } else {
        // A stream entry with no `incident_id` field can never be acted
        // on -- there is nothing for `process_incident` to look up, no
        // matter how many times this exact entry is redelivered.
        // Returning an `Err` here (as this used to) left the caller's
        // error branch in `main`'s consumer loop with no `entry_id` to
        // ack (only the `Ok` arm above ever produces one), so the entry
        // stayed in the pending-entries list forever: `claim_stale`
        // would reclaim it once it went idle, log its own warning (see
        // that function), and reclaim it again next cycle, forever --
        // the same poison-message-wedges-the-queue failure mode
        // `movement-feed` already closed elsewhere in this campaign.
        // Acknowledge it here instead: a missing required field is not
        // a transient condition this entry could ever recover from.
        tracing::warn!(
            entry_id = entry.id,
            "stream entry missing incident_id field; acking and skipping"
        );
        if let Err(err) = ack_on(conn, stream, &entry.id).await {
            tracing::error!(
                error = ?err,
                entry_id = entry.id,
                "failed to ack stream entry missing incident_id field"
            );
        }
        Ok(None)
    }
}

pub(crate) async fn ack(conn: &mut RedisConn, entry_id: &str) -> anyhow::Result<()> {
    ack_on(conn, STREAM, entry_id).await
}

/// See `ensure_group_on` for why this takes `stream` explicitly.
async fn ack_on(conn: &mut RedisConn, stream: &str, entry_id: &str) -> anyhow::Result<()> {
    let _: i64 = conn.xack(stream, GROUP, &[entry_id]).await?;
    Ok(())
}

/// Reclaims entries that have sat unacked in the consumer group's
/// pending-entries list for at least `min_idle` -- the debounced retry path
/// for an extraction that failed (LLM timeout, DB error) or crashed
/// between processing and `ack`. `process_incident` deliberately skips the
/// `ack` on any transient failure so the entry stays here for this to pick
/// up later, rather than dropping it to the mercy of the hourly sweep alone
/// (which only re-triggers on a text or model-version change, not a bare
/// processing failure).
///
/// Drains the whole PEL, not just the first page: `XAUTOCLAIM` returns a
/// cursor for continuing the scan, which is followed until it reports
/// `"0-0"` (fully scanned) rather than stopping after one call's worth of
/// entries.
pub(crate) async fn claim_stale(
    conn: &mut RedisConn,
    min_idle: Duration,
) -> anyhow::Result<Vec<(String, String)>> {
    claim_stale_on(conn, STREAM, min_idle).await
}

/// See `ensure_group_on` for why this takes `stream` explicitly.
#[expect(
    clippy::cast_possible_truncation,
    reason = "these durations are seconds to hours, far below u64::MAX milliseconds"
)]
async fn claim_stale_on(
    conn: &mut RedisConn,
    stream: &str,
    min_idle: Duration,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut claimed = Vec::new();
    let mut cursor = "0-0".to_string();
    loop {
        let reply: redis::streams::StreamAutoClaimReply = conn
            .xautoclaim_options(
                stream,
                GROUP,
                CONSUMER,
                min_idle.as_millis() as u64,
                cursor,
                redis::streams::StreamAutoClaimOptions::default().count(100),
            )
            .await?;

        for entry in reply.claimed {
            if let Some(incident_id) = entry
                .map
                .get("incident_id")
                .and_then(|v| redis::from_redis_value::<String>(v).ok())
            {
                claimed.push((entry.id, incident_id));
            } else {
                // Same poison-message reasoning as `read_one`'s own fix
                // above: merely logging and skipping (the old
                // behaviour) left this entry unacked, so the NEXT sweep
                // of this same function would reclaim it again once
                // idle, forever -- an entry missing this required field
                // never becomes processable no matter how many times
                // it's reclaimed, so ack it now instead.
                tracing::warn!(
                    entry_id = entry.id,
                    "reclaimed stream entry missing incident_id field; acking and skipping"
                );
                if let Err(err) = ack_on(conn, stream, &entry.id).await {
                    tracing::error!(
                        error = ?err,
                        entry_id = entry.id,
                        "failed to ack reclaimed stream entry missing incident_id field"
                    );
                }
            }
        }

        if reply.next_stream_id == "0-0" {
            break;
        }
        cursor = reply.next_stream_id;
    }
    Ok(claimed)
}

/// Redis 7's `XINFO GROUPS` reply is an array of per-group entries, each a
/// flat array of alternating field name/value pairs. This pulls out the
/// `enricher` group's own `lag` field -- how many stream entries the
/// group's last-delivered id is behind the stream's tail. `None` if the
/// group doesn't exist yet (a fresh stream before `ensure_group` has ever
/// run, or immediately after a Redis restart wiped it -- see `main.rs`'s
/// own NOGROUP self-heal comment for that exact scenario) or if this Redis
/// server predates the `lag` field (added in Redis 7.0; this app's own
/// deployments always run Redis 7, but a self-managed external Redis might
/// not be).
pub(crate) async fn group_lag(conn: &mut RedisConn) -> anyhow::Result<Option<i64>> {
    let reply: Vec<redis::Value> = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(STREAM)
        .query_async(conn)
        .await?;

    for group in reply {
        let redis::Value::Array(fields) = group else {
            continue;
        };
        let mut name: Option<String> = None;
        let mut lag: Option<i64> = None;
        let mut iter = fields.into_iter();
        while let (Some(key), Some(value)) = (iter.next(), iter.next()) {
            let key: String = redis::from_redis_value(&key)?;
            match key.as_str() {
                "name" => name = redis::from_redis_value(&value).ok(),
                "lag" => lag = redis::from_redis_value(&value).ok(),
                _ => {}
            }
        }
        if name.as_deref() == Some(GROUP) {
            return Ok(lag);
        }
    }
    Ok(None)
}

#[cfg(test)]
mod recreate_start_id_tests {
    use super::*;

    #[test]
    fn an_intact_stream_resumes_after_the_last_delivered_entry() {
        assert_eq!(recreate_start_id(Some("100-3"), Some("250-0")), "100-3");
        assert_eq!(recreate_start_id(Some("9-0"), Some("10-0")), "9-0");
    }

    #[test]
    fn a_missing_or_newer_stream_is_read_in_full() {
        assert_eq!(recreate_start_id(Some("100-3"), None), "0");
        assert_eq!(recreate_start_id(None, None), "0");
        assert_eq!(recreate_start_id(Some("100-3"), Some("90-0")), "0");
        assert_eq!(recreate_start_id(Some("10-0"), Some("9-0")), "0");
    }

    #[test]
    fn an_unknown_position_falls_back_to_the_tail() {
        assert_eq!(recreate_start_id(None, Some("250-0")), "$");
    }
}

/// Live-Redis regression tests for the "entry missing `incident_id`" poison
/// message fix. Ignored by default (needs a real Redis) -- run explicitly
/// with `cargo test -p enricher stream:: -- --ignored`, mirroring
/// `movement-feed::redis_stream`'s own `redis_tests` convention (env var
/// name, unique-stream-per-test isolation, and manual `XADD`/`DEL` via raw
/// `redis::cmd` rather than pulling in a second stream-writing helper).
#[cfg(test)]
mod redis_tests {
    use super::*;

    fn redis_url() -> String {
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into())
    }

    /// A fresh, unique stream name per test -- see `ensure_group_on`'s doc
    /// for why `GROUP`/`CONSUMER` don't need the same treatment.
    fn unique_stream(test_name: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("incident-text-changed-test-{test_name}-{nanos}")
    }

    async fn cleanup(stream: &str) {
        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        let _: redis::RedisResult<i64> = redis::cmd("DEL").arg(stream).query_async(&mut conn).await;
    }

    /// Adds a raw entry with no `incident_id` field at all -- the exact
    /// shape that used to wedge forever (see `read_one_on`/`claim_stale_on`'s
    /// own doc comments).
    async fn xadd_without_incident_id(conn: &mut RedisConn, stream: &str) -> String {
        redis::cmd("XADD")
            .arg(stream)
            .arg("*")
            .arg("some_other_field")
            .arg("irrelevant")
            .query_async(conn)
            .await
            .unwrap()
    }

    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn read_one_acks_and_skips_an_entry_missing_incident_id() {
        let stream = unique_stream("read-one-missing-id");
        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        ensure_group_on(&mut conn, &stream).await.unwrap();
        xadd_without_incident_id(&mut conn, &stream).await;

        // Before the fix, this returned `Err` (no `entry_id` for the caller
        // to ack), so the entry stayed pending forever. Now it must be
        // acked internally and reported as "nothing to do" (`Ok(None)`).
        let result = read_one_on(&mut conn, &stream).await;
        assert!(
            result.is_ok(),
            "a missing incident_id field must not surface as an error: {result:?}"
        );
        assert_eq!(
            result.unwrap(),
            None,
            "there is nothing usable to hand back to the caller"
        );

        // The proof it was actually acked, not just silently dropped from
        // this read: the pending-entries list must now be empty, so a
        // reclaim sweep finds nothing left to retry.
        let claimed = claim_stale_on(&mut conn, &stream, Duration::from_secs(0))
            .await
            .unwrap();
        assert!(
            claimed.is_empty(),
            "the entry must have been acked, not left in the PEL for reclaim: {claimed:?}"
        );

        cleanup(&stream).await;
    }

    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn claim_stale_acks_and_skips_a_reclaimed_entry_missing_incident_id() {
        let stream = unique_stream("claim-stale-missing-id");
        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        ensure_group_on(&mut conn, &stream).await.unwrap();
        xadd_without_incident_id(&mut conn, &stream).await;

        // Deliver it into the pending-entries list directly via a raw
        // `XREADGROUP`, bypassing `read_one_on` -- which, per the fix under
        // test, would already ack a missing-`incident_id` entry itself.
        // This simulates the actual scenario `claim_stale`/`XAUTOCLAIM`
        // exists for: a consumer read the entry (so it's in the PEL) but
        // crashed before acking it.
        let _: redis::streams::StreamReadReply = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(GROUP)
            .arg(CONSUMER)
            .arg("COUNT")
            .arg(10)
            .arg("STREAMS")
            .arg(&stream)
            .arg(">")
            .query_async(&mut conn)
            .await
            .unwrap();

        let claimed = claim_stale_on(&mut conn, &stream, Duration::from_secs(0))
            .await
            .unwrap();
        assert!(
            claimed.is_empty(),
            "an entry missing incident_id must never be handed back for processing: {claimed:?}"
        );

        // Re-running the reclaim sweep must find nothing left -- proof the
        // first pass acked it rather than merely logging and moving on.
        let claimed_again = claim_stale_on(&mut conn, &stream, Duration::from_secs(0))
            .await
            .unwrap();
        assert!(
            claimed_again.is_empty(),
            "a poison entry must not be reclaimed forever: {claimed_again:?}"
        );

        cleanup(&stream).await;
    }

    async fn xadd_incident(conn: &mut RedisConn, stream: &str, incident_id: &str) {
        let _: String = redis::cmd("XADD")
            .arg(stream)
            .arg("*")
            .arg("incident_id")
            .arg(incident_id)
            .query_async(conn)
            .await
            .unwrap();
    }

    /// Reads (and acks) incident ids until a read comes back empty.
    async fn drain(conn: &mut RedisConn, stream: &str, last: &mut Option<String>) -> Vec<String> {
        let mut out = Vec::new();
        while let Some((entry_id, incident_id)) = read_one_on(conn, stream).await.unwrap() {
            ack_on(conn, stream, &entry_id).await.unwrap();
            *last = Some(entry_id);
            out.push(incident_id);
        }
        out
    }

    fn is_nogroup(err: &anyhow::Error) -> bool {
        err.downcast_ref::<redis::RedisError>()
            .and_then(redis::RedisError::code)
            == Some("NOGROUP")
    }

    /// Redis back empty and `api` already publishing before the next read:
    /// the stream is recreated by `XADD`, without the group. Those entries
    /// used to be skipped (recreated at `$`); now they are all read.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn entries_written_before_a_lost_group_is_recreated_are_not_lost() {
        let stream = unique_stream("nogroup-recreated-stream");
        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        ensure_group_on(&mut conn, &stream).await.unwrap();
        let mut last = group_last_delivered_id_on(&mut conn, &stream).await;
        xadd_incident(&mut conn, &stream, "before").await;
        assert_eq!(drain(&mut conn, &stream, &mut last).await, vec!["before"]);

        cleanup(&stream).await;
        xadd_incident(&mut conn, &stream, "after-1").await;
        xadd_incident(&mut conn, &stream, "after-2").await;

        let err = read_one_on(&mut conn, &stream)
            .await
            .expect_err("the group is gone");
        assert!(is_nogroup(&err), "{err:?}");
        recreate_group_on(&mut conn, &stream, &mut last)
            .await
            .unwrap();
        assert_eq!(
            drain(&mut conn, &stream, &mut last).await,
            vec!["after-1", "after-2"]
        );
        cleanup(&stream).await;
    }

    /// The group deleted by hand, stream intact: only what it had not yet
    /// delivered is read, not the whole stream again.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn a_group_deleted_by_hand_resumes_without_replaying_the_stream() {
        let stream = unique_stream("nogroup-destroyed");
        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        ensure_group_on(&mut conn, &stream).await.unwrap();
        let mut last = group_last_delivered_id_on(&mut conn, &stream).await;
        for i in 0..3 {
            xadd_incident(&mut conn, &stream, &format!("read-{i}")).await;
        }
        assert_eq!(drain(&mut conn, &stream, &mut last).await.len(), 3);

        let destroyed: i64 = redis::cmd("XGROUP")
            .arg("DESTROY")
            .arg(&stream)
            .arg(GROUP)
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(destroyed, 1);
        xadd_incident(&mut conn, &stream, "unread").await;

        let err = read_one_on(&mut conn, &stream)
            .await
            .expect_err("the group is gone");
        assert!(is_nogroup(&err), "{err:?}");
        recreate_group_on(&mut conn, &stream, &mut last)
            .await
            .unwrap();
        assert_eq!(drain(&mut conn, &stream, &mut last).await, vec!["unread"]);
        cleanup(&stream).await;
    }

    /// Redis restarted with an older copy of its data: the group exists but
    /// stands before entries already read. The recreate after the failed
    /// read moves it forward again, so only the new entry is read.
    #[tokio::test]
    #[ignore = "needs REDIS_URL"]
    async fn a_group_that_went_backwards_is_moved_forward_again() {
        let stream = unique_stream("rewound");
        let client = redis::Client::open(redis_url()).unwrap();
        let mut conn = common::redis_conn::connect(&client).await.unwrap();
        ensure_group_on(&mut conn, &stream).await.unwrap();
        let mut last = group_last_delivered_id_on(&mut conn, &stream).await;
        for i in 0..3 {
            xadd_incident(&mut conn, &stream, &format!("read-{i}")).await;
        }
        assert_eq!(drain(&mut conn, &stream, &mut last).await.len(), 3);
        let read_up_to = last.clone().unwrap();

        let () = redis::cmd("XGROUP")
            .arg("SETID")
            .arg(&stream)
            .arg(GROUP)
            .arg("0")
            .query_async(&mut conn)
            .await
            .unwrap();
        xadd_incident(&mut conn, &stream, "new").await;

        recreate_group_on(&mut conn, &stream, &mut last)
            .await
            .unwrap();
        assert_eq!(last.as_deref(), Some(read_up_to.as_str()));
        assert_eq!(drain(&mut conn, &stream, &mut last).await, vec!["new"]);
        cleanup(&stream).await;
    }
}
