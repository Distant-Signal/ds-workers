//! The schema gate (spec §12.2) passes for this service's role on a
//! migrated database. CI's per-service run (`test-postgres-roles.py --mode
//! per-service`) runs this as the aggregator's own role, so it proves that
//! role holds every privilege `db-grants.yaml` gives it.

use std::time::Duration;

use ds_store::schema::{DbRole, SchemaGate, wait_for_schema_with};

#[tokio::test]
#[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
async fn the_schema_gate_passes_for_the_aggregator_role() {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
    let pool = sqlx::PgPool::connect(&url).await.expect("connect");
    let gate = SchemaGate {
        deadline: Duration::ZERO,
        ..SchemaGate::for_role(DbRole::Aggregator)
    };
    wait_for_schema_with(&pool, &gate, None).await.unwrap();
}
