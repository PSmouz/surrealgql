use std::collections::{BTreeMap, HashMap};

use crate::gql::{
    cursor::{apply_cursors_to_edges, encode_cursor, has_next_page, has_previous_page,
             ConnectionContext, ConnectionKind, EdgeContext, PageInfo},
    error::{input_error, internal_error},
    ext::{IntoExt, TryAsExt},
    schema::{gql_to_sql_kind, sql_value_to_gql_value},
    tables::{build_sql_where_clause, order_by},
    utils::{GQLTx, GqlValueUtils},
};
use crate::sql::{
    statements::SelectStatement,
    Array, Cond, Expression, Field, Fields, Idiom, Kind, Operator, Part, Statement,
    Thing, Value as SqlValue, Values,
};
use async_graphql::{
    dynamic::{FieldFuture, FieldValue, ResolverContext},
    Name,
    Value as GqlValue,
};
use inflector::Inflector;

pub fn dummy_resolver() -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync +
'static {
    move |_ctx: ResolverContext| {
        FieldFuture::new(async move {
            Ok(Some(FieldValue::value("NOT YET IMPLEMENTED!".to_string())))
        })
    }
}

/// Defines the behavior of the single-record query resolver.
#[derive(Clone)]
pub enum SingleQueryKind {
    /// The query must have a non-null `id` argument.
    ById,
    /// The query must have a set of non-null arguments that match a specific unique index.
    BySpecificIndex(Vec<String>),
    /// The query accepts optional arguments for `id` or any complete unique index,
    /// but exactly one identifier (either `id` or one full index) must be provided.
    ByArbitraryIndex(Vec<String>),
}

/// A generic resolver for fields on any object type (table records or connection edges).
///
/// It intelligently handles different parent contexts:
/// - If the parent is a `Thing`, it fetches the field from the database.
/// - If the parent is a `SqlValue`, it resolves the field from the pre-fetched object data.
pub fn make_field_resolver(
    fd_path: impl Into<String>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
    let fd_path = fd_path.into();
    move |ctx: ResolverContext| {
        let fd_path = fd_path.clone();
        FieldFuture::new(async move {
            let gtx = ctx.data::<GQLTx>()?;
            let fd_name = fd_path.split('.').last().unwrap_or(&fd_path);

            trace!("PARENT VALUE: {:?}", ctx.parent_value);
            trace!("FD PATH VALUE: {:?}", &fd_path);
            trace!("FD NAME VALUE: {:?}", &fd_name);
            let sql_value = if let Some(thing) = ctx.parent_value.downcast_ref::<Thing>() {
                // CASE 1: Parent is a `Thing` (a record ID). Fetch from the database.
                trace!("Case 1: Parent is Thing, fetching path '{}' from RID {}", fd_path, thing);
                gtx.get_record_field(thing.clone(), &fd_path).await?
            } else if let Some(edge_ctx) = ctx.parent_value.downcast_ref::<EdgeContext>() {
                // CASE 2: Parent is a connection edge. Resolve from pre-fetched data.
                trace!("Case 2.1: Parent is GqlEdgeContext, resolving path '{}' from edge_data",
                    fd_path);
                let fd_name = fd_path.split('.').last().unwrap_or(&fd_path);
                if let SqlValue::Object(obj) = &edge_ctx.edge {
                    obj.get(fd_name).cloned().unwrap_or(SqlValue::None)
                } else {
                    SqlValue::None
                }
            } else if let Some(val) = ctx.parent_value.downcast_ref::<SqlValue>() {
                // CASE 3: Parent is a `SqlValue`. We need this for the case where we have
                // objects with record fields (Things) inside them. Otherwise, we would lose the
                // context and could not resolve the record fields later.
                trace!("Case 2.2: Parent is SqlValue, resolving path '{}' from object", fd_path);
                match val {
                    SqlValue::Object(obj) => obj.get(fd_name).cloned().unwrap_or(SqlValue::None),
                    _ => SqlValue::None,
                }
            } else if let Ok(parent_gql_val) = ctx.parent_value.try_to_value() {
                // Case 4: Parent is a GqlValue (a nested object from a list).
                // This is the case for `title` inside `nestedEmbeddedArray`.
                trace!("Case 4: Parent is GqlValue, resolving path '{}' from object", fd_path);
                if let GqlValue::Object(obj) = parent_gql_val {
                    // We found a GqlValue, so we can return it directly.
                    // No need for further sql_value_to_gql_value conversion.
                    return Ok(Some(FieldValue::value(
                        obj.get(&Name::new(fd_name)).cloned().unwrap_or(GqlValue::Null),
                    )));
                }
                SqlValue::None
            } else {
                trace!("parent_value: {:?}", ctx.parent_value);
                return Err(internal_error(format!(
                    "Unexpected parent type for field '{}'",
                    fd_path
                ))
                    .into());
            };

            trace!("Fetched SQL value for path '{}': {:?}", fd_path, sql_value);
            trace!("KIND of value: {:?}", sql_value.kindof());

            match sql_value {
                // Case 1: We have a `Thing` (record ID) so we cannot lose the context, because we
                // need it to resolve its fields. Except for the 'id' field, which is a scalar.
                SqlValue::Thing(thing) if fd_name != "id" => {
                    trace!("Case 1 (Thing): Returning Thing as FieldValue::owned_any for path \
                    '{}'", fd_path);
                    Ok(Some(FieldValue::owned_any(thing)))
                }
                // Case 2: We have an array of `Thing` records, which we must return as a list of
                // Things to not lose the context and fetch their fields later.
                // P.S. We cannot put this case into the sql_value_to_gql_value function because it
                // always has to return a GqlValue. But we need a FieldValue in the inner list.
                SqlValue::Array(a) if a[0].is_thing() => {
                    trace!("Case 2 (Array of Things): Returning list of Things for path '{}'", fd_path);
                    Ok(Some(FieldValue::list(a.into_iter().map(
                        |v| FieldValue::owned_any(v.record().unwrap())
                    ).collect::<Vec<FieldValue>>())))
                }
                // Case 3: We have an array of `SqlValue` objects containing at minimum one Thing
                // which we cannot yet convert to GqlValue. The `Thing` context is needed later for
                // fetching the record.
                SqlValue::Array(a) if a[0].is_object() && {
                    match &a[0] {
                        SqlValue::Object(o) => o.values().any(SqlValue::is_thing),
                        _ => false,
                    }
                } => {
                    trace!("Case 3 (Array of Objects with Things):  '{}'", fd_path);
                    Ok(Some(FieldValue::list(a.into_iter().map(|v| FieldValue::owned_any(v))
                        .collect::<Vec<FieldValue>>())))
                }
                // Case 4: Scalar or Enum value etc
                v => {
                    trace!("Case 4 (Scalar/Enum): Converting SQL value to GQL value for path '{}'", fd_path);
                    let gql_val = sql_value_to_gql_value(v)?;
                    Ok(Some(FieldValue::value(gql_val)))
                }
            }
        })
    }
}

#[allow(clippy::too_many_lines)]
pub fn make_connection_resolver(
    name_for_query_source: impl Into<String>,
    kind: ConnectionKind,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
    let query_source_name = name_for_query_source.into();

    move |ctx: ResolverContext| {
        let query_source = query_source_name.clone();
        let kind_clone = kind.clone();

        FieldFuture::new(async move {
            let args = ctx.args.as_index_map();
            trace!(
                "Connection resolver for source '{}', args: {:?}",
                query_source,
                args
            );
            let is_relation = matches!(kind_clone, ConnectionKind::Relation);
            let gtx = ctx.data::<GQLTx>()?;

            let first = args.get("first").and_then(GqlValueUtils::as_i64).map(|v| v as usize);
            let last = args.get("last").and_then(GqlValueUtils::as_i64).map(|v| v as usize);
            let after_cursor_str = args.get("after").and_then(GqlValueUtils::as_string);
            let before_cursor_str = args.get("before").and_then(GqlValueUtils::as_string);
            let order_by_arg = args.get("orderBy").and_then(GqlValueUtils::as_object);

            let order_by = order_by(order_by_arg);

            trace!("parent_value: {:?}", ctx.parent_value);
            let db_value_array = if let Some(thing) = ctx.parent_value.downcast_ref::<Thing>() {
                // CASE 1: Parent is a `Thing`. This is a first-level nested connection.
                if is_relation {
                    // Fetch from a relation table using the parent `Thing` as the `in` filter.
                    trace!("Fetching relation '{}' where in = {}", query_source, thing);
                    let ast = Statement::Select(SelectStatement {
                        what: vec![SqlValue::Table(query_source.intox())].into(),
                        expr: Fields::all(),
                        cond: Some(Cond(SqlValue::from(Expression::Binary {
                            l: SqlValue::Idiom(Idiom(vec![Part::Field("in".into())])),
                            o: Operator::Equal,
                            r: SqlValue::Thing(thing.clone()),
                        }))),
                        order: order_by,
                        ..Default::default()
                    });
                    gtx.process_stmt(ast).await?
                } else {
                    // Fetch an embedded array field from the parent `Thing`.
                    trace!("Fetching embedded field '{}' from parent {}", query_source, thing);
                    gtx.get_record_field(thing.clone(), &query_source).await?
                }
            } else if let Some(obj) = ctx.parent_value.try_to_value()?.as_object() {
                // CASE 2: Parent is a GqlValue object.
                trace!("Fetching parent connection field '{}' from parent Object", query_source);
                trace!("parent_value: {:?}", obj);
                let fd_name = query_source.split('.').last().unwrap_or(&query_source);

                let fv = FieldValue::value(
                    obj.get(&Name::new(fd_name))
                        .cloned()
                        .unwrap_or(GqlValue::Null),
                );

                if let Some(GqlValue::List(list)) = fv.as_value() {
                    let sql_list: Result<Vec<SqlValue>, _> = list
                        .iter()
                        .map(|gql_val| gql_to_sql_kind(gql_val, Kind::Any))
                        .collect();

                    SqlValue::Array(Array::from(sql_list.unwrap()))
                } else {
                    SqlValue::Array(Default::default())
                }
            } else {
                // CASE 3: No specific parent. This is a root-level connection query.
                trace!("Fetching root query for table '{}'", query_source);
                let ast = Statement::Select(SelectStatement {
                    what: vec![SqlValue::Table(query_source.intox())].into(),
                    expr: Fields::all(),
                    order: order_by,
                    ..Default::default()
                });
                gtx.process_stmt(ast).await?
            };

            if first.is_some() && last.is_some() {
                return Err(input_error("Cannot use both `first` and `last`.").into());
            }
            if first.map_or(false, |f| f > 1000) || last.map_or(false, |l| l > 1000) { // Safety limit
                return Err(input_error("Pagination limit too high (max 1000).").into());
            }

            // ---------Process the edges--------

            let all_edges: &[SqlValue] = match &db_value_array {
                SqlValue::Array(arr) => arr,
                _ => &[],
            };
            trace!("Edges slice: {:?}", all_edges);

            let total_count = all_edges.len() as u64;
            trace!("Total items found: {}", total_count);

            let edges = apply_cursors_to_edges(
                all_edges, after_cursor_str.clone(), before_cursor_str.clone());

            let mut limited_edges = edges;
            if let Some(first_val) = first {
                limited_edges = &limited_edges[..first_val.min(limited_edges.len())];
            } else if let Some(last_val) = last {
                let start = limited_edges.len().saturating_sub(last_val);
                limited_edges = &limited_edges[start..];
            }

            let ids_to_fetch: Vec<SqlValue> = limited_edges
                .iter()
                .flat_map(|edge| {
                    match edge {
                        SqlValue::Thing(_) => vec![edge.clone()],
                        SqlValue::Object(obj) => {
                            trace!("OBJ {:?}", &obj);
                            let x = obj.iter()
                                .filter_map(|(k, v)| {
                                    match v {
                                        SqlValue::Thing(thing) if k != "id" =>
                                            Some(SqlValue::Thing(thing.clone())),
                                        _ => None,
                                    }
                                })
                                .collect::<Vec<SqlValue>>();
                            trace!("XXXX {:?}", x);
                            x
                        }
                        _ => vec![],
                    }
                })
                .collect();

            // N+1
            let fetched_nodes: HashMap<Thing, SqlValue> = if !ids_to_fetch.is_empty() {
                let ast = Statement::Select(SelectStatement {
                    what: Values(ids_to_fetch),
                    expr: Fields::all(),
                    ..Default::default()
                });
                let res = gtx.process_stmt(ast).await?;

                if let SqlValue::Array(arr) = res {
                    arr.0.into_iter().filter_map(|val| {
                        if let SqlValue::Object(obj) = &val {
                            if let Some(SqlValue::Thing(id)) = obj.get("id") {
                                return Some((id.clone(), val));
                            }
                        }
                        None
                    }).collect()
                } else {
                    HashMap::new()
                }
            } else {
                HashMap::new()
            };
            trace!("fetched_nodes: {:?}", fetched_nodes);

            let edge_contexts: Vec<EdgeContext> = limited_edges
                .iter()
                .map(|item| {
                    let node = match item {
                        // Case 1: Array item is a Thing (e.g., Record link)
                        SqlValue::Thing(thing) => {
                            fetched_nodes.get(thing).unwrap()
                        }
                        // Case 2: Array item is an Object
                        SqlValue::Object(obj) =>
                            match (is_relation, obj.get("out")) {
                                // Case 2.1: We have a relation record. It must have an 'out' field.
                                // Only the fetched `out` record is the node.
                                (true, Some(SqlValue::Thing(id))) => {
                                    fetched_nodes.get(id).unwrap_or(&SqlValue::Null)
                                }
                                // Case 2.2: We have an object with fields, possibly including Things.
                                _ => {
                                    let new_map = obj
                                        .iter()
                                        .map(|(k, v)| {
                                            let val = match v {
                                                // Case 2.2.1: If a field's value is a `Thing`,
                                                // look it up and replace it. Except for id
                                                // fields, we want them as scalar strings.
                                                SqlValue::Thing(id) if k != "id" => fetched_nodes
                                                    .get(id)
                                                    .cloned() // Clone to get an owned value from the map
                                                    .unwrap_or(SqlValue::Null),
                                                // Case 2.2.2: Otherwise, keep the original value.
                                                _ => v.clone(),
                                            };
                                            (k.clone(), val)
                                        })
                                        .collect();

                                    &SqlValue::Object(new_map)
                                }
                            }
                        // Case 3: Other types (e.g., scalar values)
                        _ => {
                            item
                        }
                    };
                    trace!("node: {:?}", node);

                    let edge = if is_relation {
                        if let SqlValue::Object(obj) = item {
                            // It's a relation record. We need to filter its fields. We want all
                            // relation fields as edge fields, except 'in' and 'out' which are
                            // `Things` to the parent and target nodes.
                            let edge_fields: BTreeMap<String, SqlValue> = obj
                                .iter()
                                .filter(|(key, _)| {
                                    // Keep the 'id' and any custom fields, but discard 'in' and 'out'.
                                    **key != "in" && **key != "out"
                                })
                                .map(|(key, value)| (key.clone(), value.clone()))
                                .collect();

                            SqlValue::Object(edge_fields.into())
                        } else {
                            // This case should not happen for relations, but as a fallback,
                            // we treat it as having no specific edge data.
                            SqlValue::Null
                        }
                    } else {
                        // For non-relations (embedded arrays), there are no separate edge fields.
                        // The item itself is the node, and the edge data is null.
                        SqlValue::Null
                    };
                    trace!("edge: {:?}", edge);

                    EdgeContext {
                        cursor: encode_cursor(item),
                        edge: edge.clone(),
                        node: node.clone(),
                    }
                })
                .collect();

            let page_info = PageInfo {
                has_next_page: has_next_page(edges, before_cursor_str.as_deref(), first),
                has_previous_page: has_previous_page(edges, after_cursor_str.as_deref(), last),
                start_cursor: edge_contexts.first().map(|e| e.cursor.clone()),
                end_cursor: edge_contexts.last().map(|e| e.cursor.clone()),
            };
            let connection = ConnectionContext {
                edges: edge_contexts,
                total_count,
                page_info,
            };
            trace!("Created connection object: {:?}", connection);

            Ok(Some(FieldValue::owned_any(connection)))
        })
    }
}

pub fn make_single_query_resolver(
    tb_name: String,
    kind: SingleQueryKind,
    indexes: BTreeMap<String, Vec<String>>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
    move |ctx: ResolverContext| {
        let tb_name = tb_name.clone();
        let kind = kind.clone();
        let indexes = indexes.clone();
        FieldFuture::new(async move {
            let gtx = ctx.data::<GQLTx>()?;
            let args = ctx.args.as_index_map();

            let cond = match &kind {
                SingleQueryKind::ById => {
                    let id = args.get("id").and_then(GqlValueUtils::as_string)
                        .ok_or_else(|| input_error("Resolver expected 'id' argument."))?;

                    // Here we use the id query "shortcut" and directly query for its db entry.
                    let thing = match id.clone().try_into() {
                        Ok(t) => t,
                        Err(_) => Thing::from((tb_name, id)),
                    };

                    return match gtx.get_record_field(thing, "id").await? {
                        SqlValue::Thing(t) => {
                            Ok(Some(FieldValue::owned_any(t)))
                        }
                        _ => Ok(None),
                    };
                }
                SingleQueryKind::BySpecificIndex(required_fds) => {
                    let mut conditions = Vec::new();

                    for fd_name in required_fds {
                        let gql_name = fd_name.to_camel_case();
                        let arg_val = args.get(&Name::new(&gql_name))
                            .ok_or_else(|| input_error(format!("Resolver expected '{gql_name}' argument.")))?;
                        conditions.push((fd_name.clone(), arg_val.clone()));
                    }

                    build_sql_where_clause(&conditions)?
                }
                SingleQueryKind::ByArbitraryIndex(input_fds) => {
                    // Handle 'id' as the highest priority. If present, use it and ignore others.
                    // Here we use the id query "shortcut" and directly query for its db entry.
                    if let Some(id) = args.get("id").and_then(GqlValueUtils::as_string) {
                        let thing = match id.clone().try_into() {
                            Ok(t) => t,
                            Err(_) => Thing::from((tb_name, id)),
                        };

                        return match gtx.get_record_field(thing, "id").await? {
                            SqlValue::Thing(t) => {
                                Ok(Some(FieldValue::owned_any(t)))
                            }
                            _ => Ok(None),
                        };
                    }

                    let provided_args: BTreeMap<String, GqlValue> = input_fds.iter()
                        .filter_map(|fd_name| {
                            args.get(&Name::new(fd_name.to_camel_case()))
                                .filter(|v| !v.is_null()).map(|val| (fd_name.clone(), val.clone()))
                        })
                        .collect();

                    if provided_args.is_empty() {
                        return Err(input_error("A unique identifier argument (e.g., 'id', or a complete unique index) is required.").into());
                    }

                    let mut satisfied_indexes = Vec::new();
                    for idx_fds in indexes.values() {
                        if idx_fds.iter().all(|f| provided_args.contains_key(f)) {
                            satisfied_indexes.push(idx_fds);
                        }
                    }

                    if satisfied_indexes.len() != 1 {
                        return Err(input_error(format!("You must provide arguments for exactly one unique index. Found {} satisfied indexes.", satisfied_indexes.len())).into());
                    }

                    let target_idx_fds = satisfied_indexes.remove(0);
                    if target_idx_fds.len() != provided_args.len() {
                        return Err(input_error("Extraneous arguments provided. Please provide only the fields for one unique index.").into());
                    }

                    let conditions: Vec<(String, GqlValue)> = provided_args.into_iter().collect();
                    build_sql_where_clause(&conditions)?
                }
            };

            let ast = Statement::Select(SelectStatement {
                what: vec![SqlValue::Table(tb_name.intox())].into(),
                expr: Fields(
                    vec![Field::Single {
                        expr: SqlValue::Idiom(Idiom::from("id")),
                        alias: None,
                    }],
                    true,  // Corresponds to SELECT VALUE id ...
                ),
                cond: Some(cond),
                limit: Some(1.intox()),
                ..Default::default()
            });

            let res = gtx.process_stmt(ast).await?;

            return match res {
                // The result of SELECT VALUE is an array.
                SqlValue::Array(mut arr) if !arr.0.is_empty() => {
                    let record_id_val = arr.0.remove(0);
                    match record_id_val.try_as_thing() {
                        Ok(t) => Ok(Some(FieldValue::owned_any(t))),
                        Err(v) =>
                            Err(internal_error(format!("expected thing, found: {v:?}")).into()),
                    }
                }
                _ => Ok(None),
            };
        })
    }
}
