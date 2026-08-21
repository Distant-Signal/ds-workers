//! Thin wrapper around the `incident-text-changed` Redis Stream / consumer
//! group. Kept separate from extraction logic (`llm.rs`) and persistence
//! (`queries.rs`) so each can be understood and tested independently.

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
