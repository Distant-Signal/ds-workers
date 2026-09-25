//! Thin wrapper around the `incident-text-changed` Redis Stream / consumer
//! group. Kept separate from extraction logic (`llm.rs`) and persistence
//! (`queries.rs`) so each can be understood and tested independently.

use std::time::Duration;

use redis::AsyncCommands;
use redis::aio::ConnectionManager;

const STREAM: &str = "incident-text-changed";
const GROUP: &str = "enricher";
const CONSUMER: &str = "enricher-1";

/// Creates the consumer group if it doesn't already exist, and the stream
/// itself if this is the very first run (`MKSTREAM`). `BUSYGROUP` (group
/// already exists) is the expected steady-state outcome and is swallowed,
/// not treated as an error.
pub async fn ensure_group(conn: &mut ConnectionManager) -> anyhow::Result<()> {
    ensure_group_on(conn, STREAM).await
}

/// `stream`-parameterized so `redis_tests` below can exercise this against
/// a throwaway per-test stream name rather than the real, fixed
/// `incident-text-changed` stream (`GROUP`/`CONSUMER` don't need the same
/// treatment: a consumer group is scoped to the stream it was created on,
/// so `enricher`/`enricher-1` on a test's own stream can never collide with
/// the same names on the real one).
async fn ensure_group_on(conn: &mut ConnectionManager, stream: &str) -> anyhow::Result<()> {
    let result: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream)
        .arg(GROUP)
        .arg("$")
        .arg("MKSTREAM")
        .query_async(conn)
        .await;

    match result {
        Ok(()) => Ok(()),
        Err(err) if err.to_string().contains("BUSYGROUP") => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Reads at most one new entry for this consumer, blocking up to 5s if
/// none are immediately available. Returns the entry's own stream ID
/// (needed to `ack`) paired with the `incident_id` field it carries.
pub async fn read_one(conn: &mut ConnectionManager) -> anyhow::Result<Option<(String, String)>> {
    read_one_on(conn, STREAM).await
}

/// See `ensure_group_on` for why this takes `stream` explicitly.
async fn read_one_on(
    conn: &mut ConnectionManager,
    stream: &str,
) -> anyhow::Result<Option<(String, String)>> {
    let reply: redis::streams::StreamReadReply = conn
        .xread_options(
            &[stream],
            &[">"],
            &redis::streams::StreamReadOptions::default()
                .group(GROUP, CONSUMER)
                .count(1)
                .block(5000),
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

    match entry
        .map
        .get("incident_id")
        .and_then(|v| redis::from_redis_value::<String>(v).ok())
    {
        Some(incident_id) => Ok(Some((entry.id, incident_id))),
        None => {
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
}

pub async fn ack(conn: &mut ConnectionManager, entry_id: &str) -> anyhow::Result<()> {
    ack_on(conn, STREAM, entry_id).await
}

/// See `ensure_group_on` for why this takes `stream` explicitly.
async fn ack_on(conn: &mut ConnectionManager, stream: &str, entry_id: &str) -> anyhow::Result<()> {
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
pub async fn claim_stale(
    conn: &mut ConnectionManager,
    min_idle: Duration,
) -> anyhow::Result<Vec<(String, String)>> {
    claim_stale_on(conn, STREAM, min_idle).await
}

/// See `ensure_group_on` for why this takes `stream` explicitly.
async fn claim_stale_on(
    conn: &mut ConnectionManager,
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
            match entry
                .map
                .get("incident_id")
                .and_then(|v| redis::from_redis_value::<String>(v).ok())
            {
                Some(incident_id) => claimed.push((entry.id, incident_id)),
                None => {
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
pub async fn group_lag(conn: &mut ConnectionManager) -> anyhow::Result<Option<i64>> {
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
        let mut conn = client.get_connection_manager().await.unwrap();
        let _: redis::RedisResult<i64> = redis::cmd("DEL").arg(stream).query_async(&mut conn).await;
    }

    /// Adds a raw entry with no `incident_id` field at all -- the exact
    /// shape that used to wedge forever (see `read_one_on`/`claim_stale_on`'s
    /// own doc comments).
    async fn xadd_without_incident_id(conn: &mut ConnectionManager, stream: &str) -> String {
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
        let mut conn = client.get_connection_manager().await.unwrap();
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
        let mut conn = client.get_connection_manager().await.unwrap();
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
}
