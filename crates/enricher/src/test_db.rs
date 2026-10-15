//! The DB-gated tests' Postgres pool.
//!
//! CI also runs these tests as the per-service enricher role
//! (`scripts/test-postgres-roles.py --mode per-service`), which has
//! `CONNECTION LIMIT 4` (charts/distant-signal/files/db-grants.yaml). An
//! unbounded test pool (sqlx's default is 10) can grow past that: a released
//! connection goes back to the pool asynchronously, so a concurrent acquire
//! opens a new one in the meantime. Once the role is at its limit, Postgres
//! refuses the connect with `too_many_connections` (53300), which sqlx treats
//! as transient and retries inside that same acquire until `acquire_timeout`
//! (30 s), even while the pool has idle connections again. The caller then
//! fails with `PoolTimedOut`: `concurrent_sweep_extracts_a_burst_in_parallel`
//! lost a whole incident's extraction that way (15 of 18 LLM requests).
//!
//! So the pool stays well inside the role's limit, with room left for the
//! previous test's connections, which Postgres counts until their backends
//! have exited. Callers past [`MAX_CONNECTIONS`] wait for an idle
//! connection instead of opening another.

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// At most half the per-service enricher role's `CONNECTION LIMIT`.
pub(crate) const MAX_CONNECTIONS: u32 = 2;

/// Connects to `DATABASE_URL` with at most [`MAX_CONNECTIONS`] connections.
pub(crate) async fn pool() -> PgPool {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
    PgPoolOptions::new()
        .max_connections(MAX_CONNECTIONS)
        .connect(&database_url)
        .await
        .expect("connect to postgres")
}
