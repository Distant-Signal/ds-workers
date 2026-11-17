//! Thin wrapper around the `incident-text-changed` Redis Stream / consumer
//! group. Kept separate from extraction logic (`llm.rs`) and persistence
//! (`queries.rs`) so each can be understood and tested independently.

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use ds_runtime::redis_conn::RedisConn;
use ds_runtime::redis_group::{GroupCursor, GroupPosition};
use redis::AsyncCommands;
use redis::aio::ConnectionLike;

const STREAM: &str = "incident-text-changed";
const GROUP: &str = "enricher";
const CONSUMER: &str = "enricher-1";

/// How long `read_one` waits server-side for a new entry
/// (`XREADGROUP ... BLOCK`).
const READ_BLOCK_MS: usize = 5000;

/// What `read_one` actually blocks for: [`READ_BLOCK_MS`], except in this
/// crate's own tests. Every Redis test there reads its stream until it is
/// empty, and each empty read waited the full 5 s; they add their entries
/// before reading them, so none needs the wait.
const BLOCK_MS: usize = if cfg!(test) { 100 } else { READ_BLOCK_MS };

// Every command is bounded by `ds_runtime::redis_conn::RESPONSE_TIMEOUT`, so a
// blocking read must return well within it or a quiet stream would look
// like a dead connection.
const _: () =
    assert!((READ_BLOCK_MS as u128) * 2 < ds_runtime::redis_conn::RESPONSE_TIMEOUT.as_millis());

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
async fn ensure_group_at<C: ConnectionLike + Send + Sync>(
    conn: &mut C,
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

/// How far this process has got through the stream, shared by the stream
/// loop and the reclaim loop (`ds_runtime::redis_group::GroupCursor`): every
/// id either was handed is recorded when it arrives and again when it is
/// acked, so [`recreate_group`] can restore the consumer group to just below
/// the oldest entry that is still unfinished, not to the newest one read.
///
/// The enricher reads one entry at a time and leaves a failed extraction
/// pending for the reclaim loop, so an entry the stream loop read and did not
/// finish is exactly what a Redis reload that loses the pending-entries list
/// would otherwise skip for good (the group moved past it, nothing left to
/// reclaim). Holding the target below it makes the reloaded group deliver it
/// again: duplicates, which extraction tolerates, never a gap.
///
/// Both loops lock it briefly and never across an await.
#[derive(Debug, Default)]
pub(crate) struct SharedCursor(Mutex<GroupCursor>);

impl SharedCursor {
    /// A cursor starting at the group's `last-delivered-id` read at
    /// startup, or empty when the group could not be read.
    pub(crate) fn seeded(last_delivered_id: Option<&str>) -> Self {
        Self(Mutex::new(
            last_delivered_id.map_or_else(GroupCursor::new, GroupCursor::seeded),
        ))
    }

    /// A poisoned lock still holds a valid cursor (every method here is a
    /// single call into it), so keep going rather than panic a loop.
    fn lock(&self) -> MutexGuard<'_, GroupCursor> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `id` was handed to this process (`XREADGROUP`, `XAUTOCLAIM`).
    pub(crate) fn delivered(&self, id: &str) {
        self.lock().delivered(id);
    }

    /// `id` was processed and `XACK`ed.
    pub(crate) fn acked(&self, id: &str) {
        self.lock().acked(id);
    }

    /// `id` can no longer be finished (trimmed from the stream), or is
    /// given up on: stop holding the restore target below it.
    pub(crate) fn released(&self, id: &str) {
        self.lock().released(id);
    }

    /// [`GroupCursor::restore_target`].
    pub(crate) fn restore_target(&self) -> Option<String> {
        self.lock().restore_target()
    }

    /// Releases every outstanding entry whose id is older than `cutoff_ms`
    /// (milliseconds since the epoch, the id's own timestamp half), so an
    /// entry that will never be finished cannot hold the restore target
    /// below it forever. Returns how many were released.
    ///
    /// Only age ever releases an entry here, never "the pending-entries
    /// list does not show it": after a Redis reload that lost the list, an
    /// entry absent from it is unfinished, not finished (ds-ingest's
    /// review of the same cursor found exactly that loss), and the reclaim
    /// loop can no longer see it, so its age is the only bound. An entry
    /// that old was retried at the longest backoff and the sweep re-finds
    /// anything still wrong (see [`ABANDON_AFTER`]).
    ///
    /// The oldest outstanding id is the successor of the restore target
    /// whenever the target is below the highest id seen (the cursor
    /// documents the target as "just below the oldest entry handed out and
    /// not yet acked"), which is how it is found without the cursor
    /// listing its entries; `abandoned_tests` pins that.
    pub(crate) fn release_abandoned(&self, cutoff_ms: u64) -> usize {
        let mut cursor = self.lock();
        let mut released = 0;
        while let (Some(target), Some(highest)) =
            (cursor.restore_target(), cursor.highest_delivered())
        {
            let (Some(target_parts), Some(highest_parts)) = (id_parts(&target), id_parts(&highest))
            else {
                break;
            };
            if target_parts >= highest_parts {
                // Nothing outstanding: the target is the highest id.
                break;
            }
            let (oldest_ms, oldest_seq) = successor(target_parts);
            if oldest_ms >= cutoff_ms {
                break;
            }
            cursor.released(&format!("{oldest_ms}-{oldest_seq}"));
            if cursor.restore_target().as_deref() == Some(target.as_str()) {
                // The successor was not outstanding after all (the cursor
                // changed what its target means): stop rather than spin.
                break;
            }
            released += 1;
        }
        released
    }
}

/// How long an entry may stay unfinished before the restore target stops
/// waiting for it: the longest a failing incident is backed off
/// ([`crate::retry_backoff::MAX_BACKOFF`], with its prune grace), after which
/// reclaim has retried it at every window and the sweep owns it.
pub(crate) const ABANDON_AFTER: Duration = Duration::from_secs(
    crate::retry_backoff::MAX_BACKOFF.as_secs() + crate::retry_backoff::PRUNE_GRACE.as_secs(),
);

/// The cutoff for [`SharedCursor::release_abandoned`]: [`ABANDON_AFTER`]
/// before now, in milliseconds since the epoch.
#[expect(
    clippy::cast_possible_truncation,
    reason = "milliseconds since the epoch fit a u64 for hundreds of millions of years"
)]
pub(crate) fn abandon_cutoff_ms() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    now.saturating_sub(ABANDON_AFTER).as_millis() as u64
}

/// A stream id as `(ms, seq)`; a bare `ms` is `ms-0`.
fn id_parts(id: &str) -> Option<(u64, u64)> {
    let (ms, seq) = id.split_once('-').unwrap_or((id, "0"));
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

/// The smallest stream id above `(ms, seq)`.
fn successor((ms, seq): (u64, u64)) -> (u64, u64) {
    match seq.checked_add(1) {
        Some(next) => (ms, next),
        None => (ms.saturating_add(1), 0),
    }
}

/// The `enricher` group's `last-delivered-id` (`XINFO GROUPS`), or `None`
/// (logged) when it cannot be read. `main` seeds its [`SharedCursor`] with
/// it at startup.
pub(crate) async fn group_last_delivered_id(conn: &mut RedisConn) -> Option<String> {
    group_last_delivered_id_on(conn, STREAM).await
}

/// See `ensure_group_on` for why this takes `stream` explicitly.
async fn group_last_delivered_id_on<C: ConnectionLike + Send + Sync>(
    conn: &mut C,
    stream: &str,
) -> Option<String> {
    ds_runtime::redis_group::group_last_delivered_id(conn, stream, GROUP)
        .await
        .unwrap_or_else(|err| {
            tracing::warn!(error = ?err, "could not read the consumer group's last-delivered-id");
            None
        })
}

/// Recreates the consumer group after a failed read (a no-op `BUSYGROUP`
/// when it still exists) at [`recreate_start_id`], rather than at the
/// stream's tail: entries `api` wrote between Redis coming back empty and
/// this call are then read, not left for the hourly sweep, and a group
/// deleted by hand does not replay the whole stream. The start is just below
/// the oldest entry this process was handed and has not finished
/// ([`GroupCursor::restore_target`]), so what a lost group had delivered but
/// not `ACKed` is delivered again (duplicates, which extraction tolerates)
/// rather than skipped.
///
/// When the group still exists but stands BEHIND that target (Redis
/// restarted and reloaded an older copy of its data; 2026-10-09 redelivery
/// review), it is moved forward to it, so incidents already extracted are
/// not handed out again: `ds_runtime::redis_group::restore_group_cursor`.
///
/// The cursor is read and written under its lock but never held across an
/// await: the reclaim loop updates it concurrently, and its updates are safe
/// to interleave (an entry it claims is in the pending-entries list, so
/// moving the group's position cannot skip it; one it acks only raises the
/// target, which the snapshot taken here does not yet know, so at worst the
/// group is restored a little lower).
pub(crate) async fn recreate_group(
    conn: &mut RedisConn,
    cursor: &SharedCursor,
) -> anyhow::Result<()> {
    recreate_group_on(conn, STREAM, cursor).await
}

/// See `ensure_group_on` for why this takes `stream` explicitly.
async fn recreate_group_on<C: ConnectionLike + Send + Sync>(
    conn: &mut C,
    stream: &str,
    shared: &SharedCursor,
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
            let key: String = redis::from_redis_value_ref(&key)?;
            if key == "last-generated-id" {
                last = redis::from_redis_value_ref::<Option<String>>(&value)?;
            }
        }
        Some(last.unwrap_or_else(|| "0-0".to_string()))
    } else {
        None
    };
    let target = shared.restore_target();
    let start_id = recreate_start_id(target.as_deref(), last_generated.as_deref());
    // A stream that is gone or newer than everything this process read: the
    // ids the cursor holds belong to a stream that no longer exists, so none
    // can be finished and none may be restored to (a target past the new
    // stream's ids would skip its entries).
    let new_stream = start_id == "0";
    // The snapshot the restore works from (see the doc above): empty for a
    // new stream, so an existing group on it is never moved to an old id.
    let snapshot = if new_stream {
        GroupCursor::new()
    } else {
        shared.lock().clone()
    };
    let created = ensure_group_at(conn, stream, &start_id).await?;
    if new_stream {
        // Dropped only once the group exists: if the call above failed, the
        // next attempt must still see a target past the new stream's ids and
        // start at `0` again, not at `$` (which skips what `api` has written).
        *shared.lock() = GroupCursor::new();
    }
    if !created
        && let GroupPosition::Restored { was } =
            ds_runtime::redis_group::restore_group_cursor(conn, stream, GROUP, &snapshot).await?
    {
        tracing::warn!(
            stream,
            group = GROUP,
            was,
            restored_to = snapshot.restore_target(),
            "consumer group went backwards (Redis reloaded an older copy of its data); moved \
             it to just below the oldest entry not yet finished, so those incidents are not \
             skipped, and the finished ones are not handed out again"
        );
        metrics::counter!(ds_runtime::metrics::metric_name(GROUP_RESTORED_METRIC)).increment(1);
    }
    // A cursor with no target yet (a group created at `$`, a fresh stream, or
    // a startup that could not read the group) learns the group's position.
    // One that has a target keeps it: the group being ahead of it only means
    // entries were handed out that this process has not seen the reply to,
    // which stay in the pending-entries list for the reclaim loop. And while
    // anything is outstanding the target must stay below it, so the group's
    // position is never adopted over it.
    if (created || shared.restore_target().is_none()) && !has_outstanding(&shared.lock()) {
        let at = group_last_delivered_id_on(conn, stream).await;
        let mut cursor = shared.lock();
        // Checked again: the reclaim loop may have claimed an entry while
        // the group was being read.
        if !has_outstanding(&cursor)
            && let Some(at) = at
        {
            *cursor = GroupCursor::seeded(&at);
        }
    }
    Ok(())
}

/// Whether any entry is handed out and not yet finished: the restore target
/// is below the highest id seen exactly then (see
/// [`SharedCursor::release_abandoned`]).
fn has_outstanding(cursor: &GroupCursor) -> bool {
    cursor.restore_target() != cursor.highest_delivered()
}

/// Where a lost consumer group is recreated -- the same rule as
/// `movement_feed::redis_stream::recreate_start_id`, which documents it in
/// full: after `restore_target` (the id just below the oldest entry not yet
/// finished, [`GroupCursor::restore_target`]) if the stream still holds ids
/// at or past it (intact stream, or one recreated since on a server whose
/// clock moved on), `0` if the stream is missing or its `last-generated-id`
/// is behind it (a new stream), `$` if the position is unknown.
fn recreate_start_id(restore_target: Option<&str>, stream_last_generated: Option<&str>) -> String {
    let Some(last_generated) = stream_last_generated else {
        return "0".to_string();
    };
    match restore_target {
        None => "$".to_string(),
        Some(target) if stream_id_less_than(last_generated, target) => "0".to_string(),
        Some(target) => target.to_string(),
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
/// (needed to `ack`) paired with the `incident_id` field it carries. The
/// entry is recorded in `cursor` as delivered the moment it arrives, before
/// anything can fail, and stays outstanding until [`ack`] succeeds.
pub(crate) async fn read_one(
    conn: &mut RedisConn,
    cursor: &SharedCursor,
) -> anyhow::Result<Option<(String, String)>> {
    read_one_on(conn, STREAM, cursor).await
}

/// See `ensure_group_on` for why this takes `stream` explicitly.
async fn read_one_on<C: ConnectionLike + Send + Sync>(
    conn: &mut C,
    stream: &str,
    cursor: &SharedCursor,
) -> anyhow::Result<Option<(String, String)>> {
    let reply: redis::streams::StreamReadReply = conn
        .xread_options(
            &[stream],
            &[">"],
            &redis::streams::StreamReadOptions::default()
                .group(GROUP, CONSUMER)
                .count(1)
                .block(BLOCK_MS),
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
    cursor.delivered(&entry.id);

    if let Some(incident_id) = entry
        .map
        .get("incident_id")
        .and_then(|v| redis::from_redis_value_ref::<String>(v).ok())
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
        if let Err(err) = ack_on(conn, stream, cursor, &entry.id).await {
            tracing::error!(
                error = ?err,
                entry_id = entry.id,
                "failed to ack stream entry missing incident_id field"
            );
        }
        Ok(None)
    }
}

/// `XACK`s `entry_id`, and only once that succeeds marks it finished in
/// `cursor`: an entry whose ack failed (or that was never acked, left
/// pending for the reclaim loop) stays outstanding and holds the restore
/// target below it.
pub(crate) async fn ack(
    conn: &mut RedisConn,
    cursor: &SharedCursor,
    entry_id: &str,
) -> anyhow::Result<()> {
    ack_on(conn, STREAM, cursor, entry_id).await
}

/// See `ensure_group_on` for why this takes `stream` explicitly.
async fn ack_on<C: ConnectionLike + Send + Sync>(
    conn: &mut C,
    stream: &str,
    cursor: &SharedCursor,
    entry_id: &str,
) -> anyhow::Result<()> {
    let _: i64 = conn.xack(stream, GROUP, &[entry_id]).await?;
    cursor.acked(entry_id);
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
///
/// Every entry claimed is recorded in `cursor` as delivered; one Redis
/// reports as trimmed from the stream (`deleted_ids`: it can never be
/// finished) is released, so it cannot hold the restore target down.
pub(crate) async fn claim_stale(
    conn: &mut RedisConn,
    cursor: &SharedCursor,
    min_idle: Duration,
) -> anyhow::Result<Vec<(String, String)>> {
    claim_stale_on(conn, STREAM, cursor, min_idle).await
}

/// See `ensure_group_on` for why this takes `stream` explicitly.
#[expect(
    clippy::cast_possible_truncation,
    reason = "these durations are seconds to hours, far below u64::MAX milliseconds"
)]
async fn claim_stale_on<C: ConnectionLike + Send + Sync>(
    conn: &mut C,
    stream: &str,
    cursor: &SharedCursor,
    min_idle: Duration,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut claimed = Vec::new();
    let mut after = "0-0".to_string();
    loop {
        let reply: redis::streams::StreamAutoClaimReply = conn
            .xautoclaim_options(
                stream,
                GROUP,
                CONSUMER,
                min_idle.as_millis() as u64,
                after,
                redis::streams::StreamAutoClaimOptions::default().count(100),
            )
            .await?;

        for id in &reply.deleted_ids {
            cursor.released(id);
        }
        for entry in reply.claimed {
            cursor.delivered(&entry.id);
            if let Some(incident_id) = entry
                .map
                .get("incident_id")
                .and_then(|v| redis::from_redis_value_ref::<String>(v).ok())
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
                if let Err(err) = ack_on(conn, stream, cursor, &entry.id).await {
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
        after = reply.next_stream_id;
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
            let key: String = redis::from_redis_value_ref(&key)?;
            match key.as_str() {
                "name" => name = redis::from_redis_value_ref(&value).ok(),
                "lag" => lag = redis::from_redis_value_ref(&value).ok(),
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

    /// The target the cursor gives while an entry is unfinished is the id
    /// just below it, and that is where the group is recreated.
    #[test]
    fn a_target_just_below_an_unfinished_entry_is_where_the_group_restarts() {
        let target = format!("199-{}", u64::MAX);
        assert_eq!(recreate_start_id(Some(&target), Some("300-0")), target);
        // A stream whose newest id is below that target is a new stream.
        assert_eq!(recreate_start_id(Some(&target), Some("150-0")), "0");
    }
}

/// The cursor wiring against a scripted Redis
/// (`ds_runtime::redis_group::fake::FakeConn`): what the stream loop and the
/// reclaim loop tell the cursor, and where [`recreate_group_on`] puts the
/// group.
#[cfg(test)]
mod cursor_tests {
    use ds_runtime::redis_group::fake::FakeConn;
    use redis::{RedisResult, Value};

    use super::*;

    const S: &str = "s";
    /// The id just below `200-0`.
    const BELOW_200: &str = "199-18446744073709551615";
    /// The id just below `500-0`.
    const BELOW_500: &str = "499-18446744073709551615";

    fn bulk(text: &str) -> Value {
        Value::BulkString(text.as_bytes().to_vec())
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "FakeConn::new takes RedisResult replies; the helper builds one"
    )]
    fn groups(last_delivered_id: &str) -> RedisResult<Value> {
        Ok(Value::Array(vec![FakeConn::group_info(
            GROUP,
            last_delivered_id,
        )]))
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "FakeConn::new takes RedisResult replies; the helper builds one"
    )]
    fn stream_info(last_generated_id: &str) -> RedisResult<Value> {
        Ok(Value::Array(vec![
            bulk("last-generated-id"),
            bulk(last_generated_id),
        ]))
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "FakeConn::new takes RedisResult replies; the helper builds one"
    )]
    fn exists() -> RedisResult<Value> {
        Ok(Value::Int(1))
    }

    fn busygroup() -> RedisResult<Value> {
        Err(redis::RedisError::from((
            redis::ErrorKind::Client,
            "BUSYGROUP Consumer Group name already exists",
        )))
    }

    /// A cursor seeded at `100-0` that was handed `ids` and acked `acked`.
    fn cursor_after(ids: &[&str], acked: &[&str]) -> SharedCursor {
        let cursor = SharedCursor::seeded(Some("100-0"));
        for id in ids {
            cursor.delivered(id);
        }
        for id in acked {
            cursor.acked(id);
        }
        cursor
    }

    fn setid_target(conn: &FakeConn) -> Option<&str> {
        conn.sent
            .iter()
            .find(|cmd| cmd.starts_with(&["XGROUP".to_string(), "SETID".to_string()]))
            .map(|cmd| cmd[4].as_str())
    }

    /// 200-0 failed and was left pending (not acked) while 210-0 finished:
    /// a reloaded Redis that lost the group's position gets it back to just
    /// below 200-0, not to 210-0, so the failed entry is delivered again.
    #[tokio::test]
    async fn a_failed_entry_holds_the_restore_target_below_it() {
        let cursor = cursor_after(&["200-0", "210-0"], &["210-0"]);
        assert_eq!(cursor.restore_target().as_deref(), Some(BELOW_200));
        let mut conn = FakeConn::new([
            exists(),
            stream_info("300-0"),
            busygroup(),
            groups("100-0"),
            Ok(Value::Okay),
        ]);
        recreate_group_on(&mut conn, S, &cursor).await.unwrap();
        assert_eq!(setid_target(&conn), Some(BELOW_200));
        assert_eq!(
            conn.sent[2],
            ["XGROUP", "CREATE", S, GROUP, BELOW_200, "MKSTREAM"],
            "recreated at the same place it restores to"
        );
    }

    /// The reclaim loop acking the failed entry lets the target move on to
    /// the entry above it.
    #[tokio::test]
    async fn a_reclaim_ack_releases_the_target() {
        let cursor = cursor_after(&["200-0", "210-0"], &["210-0"]);
        // The reclaim loop's `ack_on`: the cursor hears of it only after
        // the XACK succeeded.
        let mut ack_conn = FakeConn::new([Ok(Value::Int(1))]);
        ack_on(&mut ack_conn, S, &cursor, "200-0").await.unwrap();
        assert_eq!(cursor.restore_target().as_deref(), Some("210-0"));

        let mut conn = FakeConn::new([
            exists(),
            stream_info("300-0"),
            busygroup(),
            groups("100-0"),
            Ok(Value::Okay),
        ]);
        recreate_group_on(&mut conn, S, &cursor).await.unwrap();
        assert_eq!(setid_target(&conn), Some("210-0"));
    }

    /// A failed `XACK` leaves the entry outstanding.
    #[tokio::test]
    async fn a_failed_ack_leaves_the_entry_outstanding() {
        let cursor = cursor_after(&["200-0"], &[]);
        let mut conn = FakeConn::new([Err(redis::RedisError::from((
            redis::ErrorKind::Io,
            "connection reset",
        )))]);
        ack_on(&mut conn, S, &cursor, "200-0")
            .await
            .expect_err("the ack failed");
        assert_eq!(cursor.restore_target().as_deref(), Some(BELOW_200));
    }

    /// `NOGROUP`: Redis came back with the stream but without the group. It
    /// is recreated at the predecessor of the oldest unfinished id, and the
    /// cursor keeps holding it.
    #[tokio::test]
    async fn a_lost_group_is_recreated_at_the_predecessor_of_the_oldest_unfinished_entry() {
        let cursor = cursor_after(&["200-0", "250-0"], &[]);
        let mut conn = FakeConn::new([exists(), stream_info("300-0"), Ok(Value::Okay)]);
        recreate_group_on(&mut conn, S, &cursor).await.unwrap();
        assert_eq!(
            conn.sent[2],
            ["XGROUP", "CREATE", S, GROUP, BELOW_200, "MKSTREAM"]
        );
        assert_eq!(conn.sent.len(), 3, "no restore, and no adopting the group");
        assert_eq!(cursor.restore_target().as_deref(), Some(BELOW_200));
    }

    /// With nothing outstanding the group is recreated after the last entry
    /// finished, as before.
    #[tokio::test]
    async fn a_lost_group_with_nothing_unfinished_resumes_after_the_last_entry() {
        let cursor = cursor_after(&["200-0", "210-0"], &["200-0", "210-0"]);
        let mut conn = FakeConn::new([
            exists(),
            stream_info("300-0"),
            Ok(Value::Okay),
            groups("210-0"),
        ]);
        recreate_group_on(&mut conn, S, &cursor).await.unwrap();
        assert_eq!(
            conn.sent[2],
            ["XGROUP", "CREATE", S, GROUP, "210-0", "MKSTREAM"]
        );
        assert_eq!(cursor.restore_target().as_deref(), Some("210-0"));
    }

    /// An empty cursor (the group could not be read at startup) creates the
    /// group at the tail and learns its position.
    #[tokio::test]
    async fn an_empty_cursor_learns_the_position_of_the_group_it_creates() {
        let cursor = SharedCursor::default();
        let mut conn = FakeConn::new([
            exists(),
            stream_info("300-0"),
            Ok(Value::Okay),
            groups("300-0"),
        ]);
        recreate_group_on(&mut conn, S, &cursor).await.unwrap();
        assert_eq!(
            conn.sent[2],
            ["XGROUP", "CREATE", S, GROUP, "$", "MKSTREAM"]
        );
        assert_eq!(cursor.restore_target().as_deref(), Some("300-0"));
    }

    /// A stream that Redis lost and `api` started again: the ids the cursor
    /// holds are from a stream that is gone, so they are dropped and the new
    /// stream is read from its start, never restored to a position past it.
    #[tokio::test]
    async fn a_new_stream_drops_the_ids_of_the_old_one() {
        let cursor = cursor_after(&["500-0"], &[]);
        let mut conn = FakeConn::new([
            exists(),
            stream_info("100-0"),
            Ok(Value::Okay),
            groups("0-0"),
        ]);
        recreate_group_on(&mut conn, S, &cursor).await.unwrap();
        assert_eq!(
            conn.sent[2],
            ["XGROUP", "CREATE", S, GROUP, "0", "MKSTREAM"]
        );
        assert_eq!(cursor.restore_target().as_deref(), Some("0-0"));

        // The same when the group already exists on the new stream: no
        // SETID past the new stream's ids.
        let cursor = cursor_after(&["500-0"], &[]);
        let mut conn = FakeConn::new([
            exists(),
            stream_info("100-0"),
            busygroup(),
            groups("50-0"),
            groups("50-0"),
        ]);
        recreate_group_on(&mut conn, S, &cursor).await.unwrap();
        assert_eq!(setid_target(&conn), None);
        assert_eq!(cursor.restore_target().as_deref(), Some("50-0"));
    }

    /// The group could not be created for a new stream: the cursor keeps
    /// its ids, so the retry starts at `0` again rather than at the tail.
    #[tokio::test]
    async fn a_failed_recreate_on_a_new_stream_keeps_the_cursor_for_the_retry() {
        let cursor = cursor_after(&["500-0"], &[]);
        let mut conn = FakeConn::new([
            exists(),
            stream_info("100-0"),
            Err(redis::RedisError::from((
                redis::ErrorKind::Io,
                "connection reset",
            ))),
        ]);
        recreate_group_on(&mut conn, S, &cursor)
            .await
            .expect_err("the create failed");
        assert_eq!(cursor.restore_target().as_deref(), Some(BELOW_500));

        let mut conn = FakeConn::new([
            exists(),
            stream_info("100-0"),
            Ok(Value::Okay),
            groups("0-0"),
        ]);
        recreate_group_on(&mut conn, S, &cursor).await.unwrap();
        assert_eq!(
            conn.sent[2],
            ["XGROUP", "CREATE", S, GROUP, "0", "MKSTREAM"],
            "the retry still reads the new stream from its start"
        );
    }

    /// A group already past the target is left alone and not adopted: the
    /// entries it has handed out that this process has not seen are pending
    /// for reclaim, and a later restore must not skip them.
    #[tokio::test]
    async fn a_group_ahead_of_the_target_is_not_adopted() {
        let cursor = cursor_after(&[], &[]);
        let mut conn =
            FakeConn::new([exists(), stream_info("300-0"), busygroup(), groups("300-0")]);
        recreate_group_on(&mut conn, S, &cursor).await.unwrap();
        assert_eq!(setid_target(&conn), None);
        assert_eq!(conn.sent.len(), 4);
        assert_eq!(cursor.restore_target().as_deref(), Some("100-0"));
    }

    /// The metric still counts a group moved forward.
    #[tokio::test]
    async fn a_restored_group_is_counted() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let cursor = cursor_after(&["250-0"], &["250-0"]);
        let mut conn = FakeConn::new([
            exists(),
            stream_info("300-0"),
            busygroup(),
            groups("100-0"),
            Ok(Value::Okay),
        ]);
        recreate_group_on(&mut conn, S, &cursor).await.unwrap();
        assert_eq!(setid_target(&conn), Some("250-0"));
        let rendered = handle.render();
        assert!(
            rendered.contains("distant_signal_enricher_stream_group_position_restored_total 1"),
            "{rendered}"
        );
    }

    /// The `XAUTOCLAIM` reply that a Redis which lost its pending-entries
    /// list gives: nothing to claim.
    #[expect(
        clippy::unnecessary_wraps,
        reason = "FakeConn::new takes RedisResult replies; the helper builds one"
    )]
    fn nothing_to_claim() -> RedisResult<Value> {
        Ok(Value::Array(vec![
            bulk("0-0"),
            Value::Array(vec![]),
            Value::Array(vec![]),
        ]))
    }

    /// The loss window ds-ingest found in the same cursor (its PR 31
    /// review): Redis reloaded an older copy of its data, so the group is
    /// behind and the pending-entries list is empty, and the next read fails.
    /// 250-0 was read and its extraction failed. Because the list no longer
    /// shows it, a rule that took "not pending" to mean "finished" would drop
    /// it from the cursor and restore the group past it, and it would never
    /// be delivered again. Nothing here does: a reclaim pass that finds
    /// nothing leaves the entry outstanding, and the group is restored to
    /// just below it.
    #[tokio::test]
    async fn a_reload_that_lost_the_pending_list_does_not_finish_an_unfinished_entry() {
        let cursor = cursor_after(&["250-0"], &[]);
        let below_250 = format!("249-{}", u64::MAX);

        let mut reclaim = FakeConn::new([nothing_to_claim()]);
        let claimed = claim_stale_on(&mut reclaim, S, &cursor, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(claimed, Vec::<(String, String)>::new());
        assert_eq!(
            cursor.restore_target().as_deref(),
            Some(below_250.as_str()),
            "an entry the pending list does not show is still unfinished"
        );

        // Only age gives an entry up, and 250-0 is far younger than the cutoff.
        assert_eq!(cursor.release_abandoned(100), 0);
        assert_eq!(cursor.restore_target().as_deref(), Some(below_250.as_str()));

        // The failed read's recreate: the group (reloaded, at 100-0) goes to
        // just below the unfinished entry, which is delivered again.
        let mut conn = FakeConn::new([
            exists(),
            stream_info("300-0"),
            busygroup(),
            groups("100-0"),
            Ok(Value::Okay),
        ]);
        recreate_group_on(&mut conn, S, &cursor).await.unwrap();
        assert_eq!(setid_target(&conn), Some(below_250.as_str()));
    }

    /// Redis says an entry was trimmed from the stream (`XAUTOCLAIM`'s
    /// deleted ids): it can never be finished, so it stops holding the target.
    #[tokio::test]
    async fn a_trimmed_entry_is_released() {
        let cursor = cursor_after(&["200-0", "210-0"], &["210-0"]);
        let mut conn = FakeConn::new([Ok(Value::Array(vec![
            bulk("0-0"),
            Value::Array(vec![]),
            Value::Array(vec![bulk("200-0")]),
        ]))]);
        claim_stale_on(&mut conn, S, &cursor, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(cursor.restore_target().as_deref(), Some("210-0"));
    }

    /// A claimed entry is recorded as delivered, so one that fails again
    /// stays outstanding.
    #[tokio::test]
    async fn a_claimed_entry_is_outstanding_until_acked() {
        let cursor = SharedCursor::seeded(Some("100-0"));
        let entry = Value::Array(vec![
            bulk("200-0"),
            Value::Array(vec![bulk("incident_id"), bulk("inc-1")]),
        ]);
        let mut conn = FakeConn::new([Ok(Value::Array(vec![
            bulk("0-0"),
            Value::Array(vec![entry]),
            Value::Array(vec![]),
        ]))]);
        let claimed = claim_stale_on(&mut conn, S, &cursor, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(claimed, vec![("200-0".to_string(), "inc-1".to_string())]);
        assert_eq!(
            cursor.restore_target().as_deref(),
            Some("199-18446744073709551615")
        );
    }
}

/// [`SharedCursor::release_abandoned`], and the `GroupCursor` behaviour it
/// relies on: the oldest unfinished id is the successor of the restore
/// target.
#[cfg(test)]
mod abandoned_tests {
    use super::*;

    #[test]
    fn entries_older_than_the_cutoff_are_released_oldest_first() {
        let cursor = SharedCursor::seeded(Some("50-0"));
        for id in ["100-0", "200-0", "300-0"] {
            cursor.delivered(id);
        }
        assert_eq!(cursor.release_abandoned(250), 2);
        assert_eq!(
            cursor.restore_target().as_deref(),
            Some("299-18446744073709551615"),
            "only 300-0 is still held"
        );
        assert_eq!(cursor.release_abandoned(250), 0);
    }

    #[test]
    fn nothing_is_released_that_is_newer_than_the_cutoff() {
        let cursor = SharedCursor::seeded(Some("50-0"));
        cursor.delivered("100-0");
        assert_eq!(cursor.release_abandoned(100), 0, "the cutoff is exclusive");
        assert_eq!(
            cursor.restore_target().as_deref(),
            Some("99-18446744073709551615")
        );
    }

    #[test]
    fn nothing_outstanding_releases_nothing() {
        let cursor = SharedCursor::seeded(Some("50-0"));
        assert_eq!(cursor.release_abandoned(u64::MAX), 0);
        cursor.delivered("100-0");
        cursor.acked("100-0");
        assert_eq!(cursor.release_abandoned(u64::MAX), 0);
        assert_eq!(cursor.restore_target().as_deref(), Some("100-0"));
        assert_eq!(SharedCursor::default().release_abandoned(u64::MAX), 0);
    }

    /// An outstanding id at the top of its millisecond has its successor in
    /// the next one.
    #[test]
    fn an_id_at_the_end_of_its_millisecond_is_released() {
        let cursor = SharedCursor::seeded(Some("50-0"));
        cursor.delivered("200-18446744073709551615");
        cursor.delivered("400-0");
        assert_eq!(cursor.release_abandoned(300), 1);
        assert_eq!(
            cursor.restore_target().as_deref(),
            Some("399-18446744073709551615")
        );
    }

    /// Released entries are all of them the old ones, even with finished
    /// entries between.
    #[test]
    fn finished_entries_between_are_skipped() {
        let cursor = SharedCursor::seeded(Some("50-0"));
        for id in ["100-0", "150-0", "200-0", "900-0"] {
            cursor.delivered(id);
        }
        cursor.acked("150-0");
        assert_eq!(cursor.release_abandoned(500), 2);
        assert_eq!(
            cursor.restore_target().as_deref(),
            Some("899-18446744073709551615")
        );
    }

    #[test]
    fn successor_carries_into_the_next_millisecond() {
        assert_eq!(successor((5, 7)), (5, 8));
        assert_eq!(successor((5, u64::MAX)), (6, 0));
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
        let mut conn = ds_runtime::redis_conn::connect(&client).await.unwrap();
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
        let mut conn = ds_runtime::redis_conn::connect(&client).await.unwrap();
        ensure_group_on(&mut conn, &stream).await.unwrap();
        xadd_without_incident_id(&mut conn, &stream).await;

        // Before the fix, this returned `Err` (no `entry_id` for the caller
        // to ack), so the entry stayed pending forever. Now it must be
        // acked internally and reported as "nothing to do" (`Ok(None)`).
        let result = read_one_on(&mut conn, &stream, &SharedCursor::default()).await;
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
        let claimed = claim_stale_on(
            &mut conn,
            &stream,
            &SharedCursor::default(),
            Duration::from_secs(0),
        )
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
        let mut conn = ds_runtime::redis_conn::connect(&client).await.unwrap();
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

        let claimed = claim_stale_on(
            &mut conn,
            &stream,
            &SharedCursor::default(),
            Duration::from_secs(0),
        )
        .await
        .unwrap();
        assert!(
            claimed.is_empty(),
            "an entry missing incident_id must never be handed back for processing: {claimed:?}"
        );

        // Re-running the reclaim sweep must find nothing left -- proof the
        // first pass acked it rather than merely logging and moving on.
        let claimed_again = claim_stale_on(
            &mut conn,
            &stream,
            &SharedCursor::default(),
            Duration::from_secs(0),
        )
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
    async fn drain(conn: &mut RedisConn, stream: &str, cursor: &SharedCursor) -> Vec<String> {
        let mut out = Vec::new();
        while let Some((entry_id, incident_id)) = read_one_on(conn, stream, cursor).await.unwrap() {
            ack_on(conn, stream, cursor, &entry_id).await.unwrap();
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
        let mut conn = ds_runtime::redis_conn::connect(&client).await.unwrap();
        ensure_group_on(&mut conn, &stream).await.unwrap();
        let cursor = SharedCursor::seeded(
            group_last_delivered_id_on(&mut conn, &stream)
                .await
                .as_deref(),
        );
        xadd_incident(&mut conn, &stream, "before").await;
        assert_eq!(drain(&mut conn, &stream, &cursor).await, vec!["before"]);

        cleanup(&stream).await;
        xadd_incident(&mut conn, &stream, "after-1").await;
        xadd_incident(&mut conn, &stream, "after-2").await;

        let err = read_one_on(&mut conn, &stream, &cursor)
            .await
            .expect_err("the group is gone");
        assert!(is_nogroup(&err), "{err:?}");
        recreate_group_on(&mut conn, &stream, &cursor)
            .await
            .unwrap();
        assert_eq!(
            drain(&mut conn, &stream, &cursor).await,
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
        let mut conn = ds_runtime::redis_conn::connect(&client).await.unwrap();
        ensure_group_on(&mut conn, &stream).await.unwrap();
        let cursor = SharedCursor::seeded(
            group_last_delivered_id_on(&mut conn, &stream)
                .await
                .as_deref(),
        );
        for i in 0..3 {
            xadd_incident(&mut conn, &stream, &format!("read-{i}")).await;
        }
        assert_eq!(drain(&mut conn, &stream, &cursor).await.len(), 3);

        let destroyed: i64 = redis::cmd("XGROUP")
            .arg("DESTROY")
            .arg(&stream)
            .arg(GROUP)
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(destroyed, 1);
        xadd_incident(&mut conn, &stream, "unread").await;

        let err = read_one_on(&mut conn, &stream, &cursor)
            .await
            .expect_err("the group is gone");
        assert!(is_nogroup(&err), "{err:?}");
        recreate_group_on(&mut conn, &stream, &cursor)
            .await
            .unwrap();
        assert_eq!(drain(&mut conn, &stream, &cursor).await, vec!["unread"]);
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
        let mut conn = ds_runtime::redis_conn::connect(&client).await.unwrap();
        ensure_group_on(&mut conn, &stream).await.unwrap();
        let cursor = SharedCursor::seeded(
            group_last_delivered_id_on(&mut conn, &stream)
                .await
                .as_deref(),
        );
        for i in 0..3 {
            xadd_incident(&mut conn, &stream, &format!("read-{i}")).await;
        }
        assert_eq!(drain(&mut conn, &stream, &cursor).await.len(), 3);
        let read_up_to = cursor.restore_target().unwrap();

        let () = redis::cmd("XGROUP")
            .arg("SETID")
            .arg(&stream)
            .arg(GROUP)
            .arg("0")
            .query_async(&mut conn)
            .await
            .unwrap();
        xadd_incident(&mut conn, &stream, "new").await;

        recreate_group_on(&mut conn, &stream, &cursor)
            .await
            .unwrap();
        assert_eq!(
            cursor.restore_target().as_deref(),
            Some(read_up_to.as_str())
        );
        assert_eq!(drain(&mut conn, &stream, &cursor).await, vec!["new"]);
        cleanup(&stream).await;
    }
}
