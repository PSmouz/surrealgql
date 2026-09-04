//! Relational join operators over binding-row streams.
//!
//! These operators combine two binding-row streams (`Value::Object`s keyed by
//! binding name — the binding-row convention, see
//! `doc/gql/V2_DESIGN.md` §3) and are deliberately language-neutral: they
//! carry no GQL IR types and can be driven by any frontend that produces
//! binding rows. Today the only consumer is the GQL planner
//! (`exec/planner/match_plan.rs`); when SurrealQL/Postgres relational joins
//! land, this module is their shared home.
//!
//! Currently only [`hash_join::HashJoin`] (equi-join / cartesian product) is
//! implemented. The relational strategies a SQL planner additionally needs —
//! nested-loop (theta joins), index-nested-loop, and sort-merge — belong here
//! alongside it.
//!
//! ## Extension point: generalized join keys
//!
//! [`HashJoin`](hash_join::HashJoin) keys on `Vec<String>` binding names and
//! extracts `<binding>.id` from each side — the only key shape GQL needs
//! (joins are always equi-joins on shared node ids). A relational SQL planner
//! needs arbitrary, asymmetric equi-join conditions; generalizing the key
//! representation to a composite `Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>`
//! (one expression per side) is the intended extension point for that work.

pub(crate) mod hash_join;

pub use hash_join::{HashJoin, JoinType};
