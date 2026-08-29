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
    let result: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(STREAM)
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
    let reply: redis::streams::StreamReadReply = conn
        .xread_options(
            &[STREAM],
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
    let Some(entry) = reply.keys.into_iter().flat_map(|stream_key| stream_key.ids).next() else {
        return Ok(None);
    };

    let incident_id: String = entry
        .map
        .get("incident_id")
        .and_then(|v| redis::from_redis_value(v).ok())
        .ok_or_else(|| anyhow::anyhow!("stream entry missing incident_id field"))?;
    Ok(Some((entry.id, incident_id)))
}

pub async fn ack(conn: &mut ConnectionManager, entry_id: &str) -> anyhow::Result<()> {
    let _: i64 = conn.xack(STREAM, GROUP, &[entry_id]).await?;
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
pub async fn claim_stale(conn: &mut ConnectionManager, min_idle: Duration) -> anyhow::Result<Vec<(String, String)>> {
    let mut claimed = Vec::new();
    let mut cursor = "0-0".to_string();
    loop {
        let reply: redis::streams::StreamAutoClaimReply = conn
            .xautoclaim_options(
                STREAM,
                GROUP,
                CONSUMER,
                min_idle.as_millis() as u64,
                cursor,
                redis::streams::StreamAutoClaimOptions::default().count(100),
            )
            .await?;

        for entry in reply.claimed {
            match entry.map.get("incident_id").and_then(|v| redis::from_redis_value::<String>(v).ok()) {
                Some(incident_id) => claimed.push((entry.id, incident_id)),
                None => tracing::warn!(entry_id = entry.id, "reclaimed stream entry missing incident_id field; skipping"),
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
    let reply: Vec<redis::Value> = redis::cmd("XINFO").arg("GROUPS").arg(STREAM).query_async(conn).await?;

    for group in reply {
        let redis::Value::Array(fields) = group else { continue };
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
