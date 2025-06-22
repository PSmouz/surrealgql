use crate::gql::error::{input_error, internal_error};
use crate::gql::utils::hash_sql_value;
use crate::gql::GqlError;
use crate::iam::base::BASE64;
use crate::sql::{Thing, Value as SqlValue};
use async_graphql::dynamic::{FieldFuture, FieldValue, ResolverContext};
use async_graphql::Value as GqlValue;
use base64::Engine;
use std::any::Any;

// We cant use the async_graphql versions as they are generic and require
// static types at compile time, which conflicts with our dynamic schema design.
#[derive(Clone, Debug)]
pub struct EdgeContext {
    pub cursor: String,
    pub edge: SqlValue, // Only the additional edge fields, not the node itself.
    pub node: SqlValue, // The node data for nodes and the node of an edge.
}

#[derive(Clone, Debug, Default)]
pub struct PageInfo {
    pub has_next_page: bool,
    pub has_previous_page: bool,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ConnectionContext {
    pub edges: Vec<EdgeContext>,
    pub page_info: PageInfo,
    pub total_count: u64,
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum ConnectionKind {
    Field, // Connection is a nested field and has a parent table
    Table, // Connection is the root table
    Relation, // Connection is a relation of a table
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
pub fn make_value_resolver<P, F>(
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
pub fn make_object_resolver<P, F>(
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

/// A generic resolver factory for fields that return a list of objects.
///
/// It takes a closure that extracts a `Vec<T>` from the parent `P`. It then
/// correctly maps this into a `FieldValue::list`, where each item `T` is
/// wrapped in `FieldValue::owned_any` to become a parent for its own resolvers.
/// This is the correct way to resolve fields like `edges` and `nodes`.
pub fn make_list_resolver<P, F>(
    extractor: F,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static
where
    P: Any + Send + Sync,
    F: Fn(&P) -> Result<Vec<FieldValue<'static>>, GqlError> + Clone + Send + Sync + 'static,
{
    move |ctx: ResolverContext| {
        let extractor = extractor.clone();
        FieldFuture::new(async move {
            if let Some(parent) = ctx.parent_value.downcast_ref::<P>() {
                let list_of_field_values = extractor(parent)?;
                Ok(Some(FieldValue::list(list_of_field_values)))
            } else {
                let parent_type_name = std::any::type_name::<P>();
                Err(internal_error(format!(
                    "Internal Error: Expected parent of type '{}', but found something else.",
                    parent_type_name
                ))
                    .into())
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
