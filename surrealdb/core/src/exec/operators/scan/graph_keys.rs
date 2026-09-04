//! Graph adjacency key-scan machinery, shared across operators.
//!
//! This module owns the low-level pieces that turn a `(record id, direction,
//! edge tables)` triple into KV key ranges and decode the adjacency keys those
//! ranges return. It was extracted from [`super::graph`] so that operators
//! other than `GraphEdgeScan` (the GQL `Expand` / `PathExpand` operators, which
//! must enumerate edges *per input row* rather than flattening the correlation
//! away) can reuse the exact same range computation and decode logic.
//!
//! `GraphEdgeScan` is layered unchanged on top of these helpers.

use std::ops::Bound;
use std::sync::Arc;

use super::common::evaluate_bound_key;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::{ControlFlowExt, ExecutionContext, PhysicalExpr};
use crate::expr::{ControlFlow, Dir};
/// Adjacency key decode result, re-exported so callers of [`decode_graph_edge`]
/// don't need to reach into the `key::graph` module directly.
pub(crate) use crate::key::graph::DecodedGraph;
use crate::kvs::KVKey;
use crate::val::{RecordId, TableName};

/// Specification for an edge table to scan, optionally with ID range bounds.
///
/// When range bounds are present, the scan is restricted to edges whose IDs fall
/// within the specified range instead of scanning the entire table.
#[derive(Debug, Clone)]
pub struct EdgeTableSpec {
	/// The edge table name (e.g., `edge`, `knows`)
	pub table: TableName,
	/// Range start bound. When `Unbounded`, starts from the table prefix.
	pub range_start: Bound<Arc<dyn PhysicalExpr>>,
	/// Range end bound. When `Unbounded`, ends at the table suffix.
	pub range_end: Bound<Arc<dyn PhysicalExpr>>,
}

/// Compute all KV key ranges to scan for a single record + direction.
///
/// When `edge_tables` is empty, returns a single wildcard range covering all
/// edges in the given direction. Otherwise returns one range per edge table,
/// respecting any range bounds on each [`EdgeTableSpec`].
pub(crate) async fn compute_graph_ranges(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rid: &RecordId,
	dir: Dir,
	edge_tables: &[EdgeTableSpec],
	ctx: &ExecutionContext,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ControlFlow> {
	if edge_tables.is_empty() {
		// Scan all edges in this direction
		let beg = crate::key::graph::egprefix(ns_id, db_id, &rid.table, &rid.key, dir)
			.context("Failed to create graph prefix")?;
		let end = crate::key::graph::egsuffix(ns_id, db_id, &rid.table, &rid.key, dir)
			.context("Failed to create graph suffix")?;
		Ok(vec![(beg, end)])
	} else {
		let mut ranges = Vec::with_capacity(edge_tables.len());
		for spec in edge_tables {
			let beg =
				eval_graph_bound(ns_id, db_id, rid, dir, &spec.table, &spec.range_start, true, ctx)
					.await?;
			let end =
				eval_graph_bound(ns_id, db_id, rid, dir, &spec.table, &spec.range_end, false, ctx)
					.await?;
			ranges.push((beg, end));
		}
		Ok(ranges)
	}
}

/// Evaluate a single start or end bound of a graph edge key range.
///
/// `is_start` determines the fallback for `Unbounded` (prefix vs suffix) and
/// the suffix byte semantics for `Included` / `Excluded` bounds:
///
/// | Bound     | start (`is_start=true`)  | end (`is_start=false`)     |
/// |-----------|--------------------------|----------------------------|
/// | Unbounded | `ftprefix`               | `ftsuffix`                 |
/// | Included  | exact key                | key + `0xff` (include key) |
/// | Excluded  | key + `0xff` (skip past) | exact key (stop before)    |
///
/// The suffix byte is `0xff` (rather than `0x00`) so that new-format keys,
/// which append the target vertex after `fk`, are still captured by an
/// Included end / skipped by an Excluded start. The first byte after `fk`
/// in a new-format key is the first byte of the target table name's
/// storekey encoding, which is always `< 0xff` for practical table names.
#[allow(clippy::too_many_arguments)]
async fn eval_graph_bound(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rid: &RecordId,
	dir: Dir,
	edge_table: &TableName,
	bound: &Bound<Arc<dyn PhysicalExpr>>,
	is_start: bool,
	ctx: &ExecutionContext,
) -> Result<Vec<u8>, ControlFlow> {
	match bound {
		Bound::Unbounded => {
			if is_start {
				crate::key::graph::ftprefix(ns_id, db_id, &rid.table, &rid.key, dir, edge_table)
					.context("Failed to create graph table prefix")
			} else {
				crate::key::graph::ftsuffix(ns_id, db_id, &rid.table, &rid.key, dir, edge_table)
					.context("Failed to create graph table suffix")
			}
		}
		Bound::Included(expr) => {
			let fk = evaluate_bound_key(expr, ctx).await?;
			let mut key = encode_graph_key(ns_id, db_id, rid, dir, edge_table, fk)?;
			// Included start: exact key.
			// Included end: append `0xff` to include the key and any
			// new-format-with-target variant of the same fk.
			if !is_start {
				key.push(0xff);
			}
			Ok(key)
		}
		Bound::Excluded(expr) => {
			let fk = evaluate_bound_key(expr, ctx).await?;
			let mut key = encode_graph_key(ns_id, db_id, rid, dir, edge_table, fk)?;
			// Excluded start: append `0xff` to skip past both legacy and
			// new-format-with-target variants of the same fk.
			// Excluded end: exact key.
			if is_start {
				key.push(0xff);
			}
			Ok(key)
		}
	}
}

/// Encode a graph key for a specific edge table and record ID key.
fn encode_graph_key(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rid: &RecordId,
	dir: Dir,
	edge_table: &TableName,
	fk: crate::val::RecordIdKey,
) -> Result<Vec<u8>, ControlFlow> {
	crate::key::graph::new(
		ns_id,
		db_id,
		&rid.table,
		&rid.key,
		dir,
		&RecordId {
			table: edge_table.clone(),
			key: fk,
		},
	)
	.encode_key()
	.context("Failed to encode graph range key")
}

/// Decode a graph key. For legacy keys, returns the edge id; for new-format
/// keys, also returns the embedded target vertex.
///
/// Thin wrapper over [`crate::key::graph::Graph::decode_key`] that converts
/// the anyhow error into a [`ControlFlow`] with a consistent context string,
/// so call sites in the scan pipeline can stay on `?`.
pub(crate) fn decode_graph_edge(key: &[u8]) -> Result<DecodedGraph, ControlFlow> {
	crate::key::graph::Graph::decode_key(key).context("Failed to decode graph key")
}
