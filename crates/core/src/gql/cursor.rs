use crate::gql::error::{input_error, internal_error};
use crate::gql::schema::sql_value_to_gql_value;
use crate::gql::utils::{field_val_erase_owned, hash_sql_value, ErasedRecord, GQLTx};
use crate::gql::GqlError;
use crate::iam::base::BASE64;
use crate::sql::{Thing, Value as SqlValue};
use async_graphql::dynamic::{FieldFuture, FieldValue, ResolverContext};
use async_graphql::Value as GqlValue;
use base64::Engine;
use std::any::Any;

// FIXME: use async graphql structs for these types
#[derive(Clone, Debug)]
pub struct GqlEdge {
    pub gtx: GQLTx,
    pub cursor: String,
    // The actual data item (node). This is a `FieldValue` so it can be resolved
    // by async-graphql. It will typically wrap an `ErasedRecord` or a simple value.
    // node: FieldValue<'static>,
    pub edge: SqlValue,
    pub node: SqlValue,
}

#[derive(Clone, Debug, Default)]
pub struct GqlPageInfo {
    pub has_next_page: bool,
    pub has_previous_page: bool,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
}

#[derive(Clone, Debug)]
pub struct GqlConnection {
    pub gtx: GQLTx,
    pub edges: Vec<GqlEdge>,
    pub nodes: Vec<SqlValue>,
    pub page_info: GqlPageInfo,
    pub total_count: i64,
}

fn encode_cursor(thing: &Thing) -> String {
    BASE64.encode(thing.to_string().as_bytes())
}

// Helper to decode a cursor string back into a Thing ID
fn decode_cursor(cursor: &str) -> Result<Thing, GqlError> {
    let bytes = BASE64
        .decode(cursor.as_bytes())
        .map_err(|e| input_error(format!("Invalid cursor: failed to decode base64: {}", e)))?;
    let s = String::from_utf8(bytes)
        .map_err(|e| input_error(format!("Invalid cursor: failed to convert to utf8: {}", e)))?;
    s.try_into()
        .map_err(|se| input_error(format!("Invalid cursor content: {:?}", se)))
}

/// Generates a stable, content-addressable cursor for any given item in a list.
///
/// This function creates a consistent hash of the item's content, making the
/// cursor independent of the item's position in the list.
pub fn encode_cursor_for_item(item: &SqlValue) -> String {
    // The index is no longer needed for encoding, but we keep it in the
    // signature to maintain compatibility with the calling function.
    hash_sql_value(item)
}

/// A generic resolver factory for fields that return a terminal GraphQL value (scalar, enum, etc.).
///
/// It extracts a field from a parent object `P`, converts it to a `Value`,
/// and wraps it in a `FieldValue`. This works for primitives like `i64`, `bool`, `String`, etc.
///
/// - `P`: The type of the parent struct (e.g., `GqlConnection`, `GqlPageInfo`).
/// - `F`: The type of the field being resolved, which must be convertible into a `FieldValue`.
pub fn make_field_value_resolver<P, F>(
    extractor: impl Fn(&P) -> F + Clone + Send + Sync + 'static,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static
where
    P: Any + Send + Sync,
    F: Into<GqlValue> + Send,
{
    move |ctx: ResolverContext| {
        let extractor = extractor.clone();
        FieldFuture::new(async move {
            if let Some(parent) = ctx.parent_value.downcast_ref::<P>() {
                let field_value = extractor(parent);
                Ok(Some(FieldValue::value(field_value.into())))
            } else {
                let parent_type_name = std::any::type_name::<P>();
                Err(internal_error(format!(
                    "Internal Error: Expected parent of type '{}', but found something else.",
                    parent_type_name
                )).into())
            }
        })
    }
}

/// A generic resolver factory for fields that are themselves parent objects.
///
/// It extracts a nested struct from a parent object `P` and passes it down,
/// so it can be the parent for its own child resolvers.
///
/// - `P`: The type of the parent struct (e.g., `GqlConnection`, `GqlPageInfo`).
/// - `F`: The type of the field being resolved, which must be convertible into a `FieldValue`.
pub fn make_parent_object_resolver<P, F>(
    extractor: impl Fn(&P) -> F + Clone + Send + Sync + 'static,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static
where
    P: Any + Send + Sync,
    F: Any + Clone + Send + Sync,
{
    move |ctx: ResolverContext| {
        let extractor = extractor.clone();
        FieldFuture::new(async move {
            if let Some(parent) = ctx.parent_value.downcast_ref::<P>() {
                let nested_object = extractor(parent);
                Ok(Some(FieldValue::owned_any(nested_object)))
            } else {
                let parent_type_name = std::any::type_name::<P>();
                Err(internal_error(format!(
                    "Internal Error: Expected parent of type '{}', but found something else.",
                    parent_type_name
                )).into())
            }
        })
    }
}

pub fn connection_nodes_resolver() -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
    move |ctx: ResolverContext| {
        FieldFuture::new(async move {
            if let Some(conn) = ctx.parent_value.downcast_ref::<GqlConnection>() {
                // 1. Clone the Vec<SqlValue>. This is cheap if SqlValue is cheap to clone.
                let nodes_sql = conn.nodes.clone();
                let gtx = &conn.gtx;

                // 2. Convert the owned Vec<SqlValue> to a Vec<GqlValue>.
                // let nodes_gql: Vec<GqlValue> = nodes_sql
                //     .into_iter()
                //     .map(|v| sql_value_to_gql_value(v))
                //     .collect::<Result<_, _>>()?;

                let nodes_erased: Result<Vec<FieldValue>, GqlError> = nodes_sql
                    .into_iter()
                    .map(|v| match v {
                        SqlValue::Thing(t) => {
                            // Create the context tuple (GQLTx, Thing)
                            let erased: ErasedRecord = (gtx.clone(), t);
                            // Wrap it in a FieldValue that downstream resolvers can downcast.
                            Ok(field_val_erase_owned(erased))
                        }
                        // Handle cases where a node might not be a record link.
                        _ => {
                            let gql_val = sql_value_to_gql_value(v.clone())?;
                            Ok(FieldValue::value(gql_val))
                        }
                    })
                    .collect();

                // 3. Create the final GqlValue::List and pass it to FieldValue::value.
                // Ok(Some(FieldValue::value(GqlValue::List(nodes_gql))))
                Ok(Some(FieldValue::list(nodes_erased?)))
            } else {
                Err(internal_error(
                    "Internal Error: Expected parent of type '{}', but found something else."
                ).into())
            }
        })
    }
}

/// Resolver for the `edges` field on a Connection type.
pub fn connection_edges_resolver() -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
    move |ctx: ResolverContext| {
        FieldFuture::new(async move {
            if let Some(conn) = ctx.parent_value.downcast_ref::<GqlConnection>() {
                // Wrap each GqlEdgeContext so it can be the parent for the Edge resolvers.
                let edges: Vec<FieldValue> = conn
                    .edges
                    .iter()
                    .map(|edge_ctx| FieldValue::owned_any(edge_ctx.clone()))
                    .collect();
                Ok(Some(FieldValue::list(edges)))
            } else {
                Err(internal_error("Parent of Connection.edges must be GqlConnection").into())
            }
        })
    }
}

/// Resolver for the `node` field on an Edge type.
pub fn edge_node_resolver() -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
    move |ctx: ResolverContext| {
        FieldFuture::new(async move {
            if let Some(edge_ctx) = ctx.parent_value.downcast_ref::<GqlEdge>() {
                // Use the pre-extracted node_data and gtx from the edge context.
                let gtx = &edge_ctx.gtx;
                let node_sql = &edge_ctx.node;

                // Convert the node's SqlValue to the appropriate FieldValue.
                match node_sql {
                    SqlValue::Thing(t) => {
                        let erased: ErasedRecord = (gtx.clone(), t.clone());
                        Ok(Some(field_val_erase_owned(erased)))
                    }
                    _ => {
                        let gql_val = sql_value_to_gql_value(node_sql.clone())?;
                        Ok(Some(FieldValue::value(gql_val)))
                    }
                }
            } else {
                Err(internal_error("Parent of Edge.node must be GqlEdgeContext").into())
            }
        })
    }
}

/// Resolver for the `cursor` field on an Edge type.
pub fn edge_cursor_resolver() -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
    move |ctx: ResolverContext| {
        FieldFuture::new(async move {
            if let Some(edge_ctx) = ctx.parent_value.downcast_ref::<GqlEdge>() {
                // The cursor is pre-computed, just return it.
                Ok(Some(FieldValue::value(edge_ctx.cursor.clone())))
            } else {
                Err(internal_error("Parent of Edge.cursor must be GqlEdgeContext").into())
            }
        })
    }
}

/// Finds the index of an item in the full list that corresponds to a given cursor.
///
/// This function works by iterating through the list and generating a cursor for each item,
/// comparing it to the target cursor. This allows it to find the correct index regardless
//  of whether the cursor was generated from a Thing or an index.
fn find_index_for_cursor(
    all_edges: &[SqlValue],
    cursor: &str,
) -> Option<usize> {
    all_edges.iter().enumerate().find_map(|(i, item)| {
        if encode_cursor_for_item(item) == cursor {
            Some(i)
        } else {
            None
        }
    })
}

/// Implements the spec's `ApplyCursorsToEdges` logic.
///
/// This function takes the full set of edges and returns a slice of that set
/// between the `after` and `before` cursors.
pub fn apply_cursors_to_edges(
    all_edges: &[SqlValue],
    before: Option<String>,
    after: Option<String>,
) -> &[SqlValue] {
    let mut start_index = 0;
    let mut end_index = all_edges.len();

    if let Some(after_cursor) = after {
        if let Some(after_idx) = find_index_for_cursor(all_edges, &after_cursor)
        {
            start_index = after_idx + 1;
        } else {
            return &[]; // Invalid `after` cursor
        }
    }

    if let Some(before_cursor) = before {
        if let Some(before_idx) =
            find_index_for_cursor(all_edges, &before_cursor)
        {
            end_index = before_idx;
        } else {
            return &[]; // Invalid `before` cursor
        }
    }

    if start_index >= end_index {
        return &[];
    }

    &all_edges[start_index..end_index]
}

/// Calculates `hasNextPage` according to the GraphQL Cursor Connections spec.
pub fn has_next_page(
    edges: &[SqlValue],
    before: Option<&str>,
    first: Option<usize>,
) -> bool {
    if let Some(first_val) = first {
        return edges.len() > first_val;
    }

    if let Some(before_cursor) = before {
        // If the server can efficiently determine that elements exist following before, return true.
        if let Some(index) = find_index_for_cursor(edges, before_cursor) {
            return index < edges.len() - 1;
        }
    }

    false
}

/// Calculates `hasPreviousPage` according to the GraphQL Cursor Connections spec.
pub fn has_previous_page(
    edges: &[SqlValue],
    after: Option<&str>,
    last: Option<usize>,
) -> bool {
    if let Some(last_val) = last {
        return edges.len() > last_val;
    }

    if let Some(after_cursor) = after {
        // If the server can efficiently determine that elements exist prior to after, return true.
        if let Some(index) = find_index_for_cursor(edges, after_cursor) {
            return index > 0;
        }
    }

    false
}