use std::any::Any;
use std::cmp::PartialEq;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::mem;
use std::ops::Add;
use std::sync::{Arc, LazyLock};

use super::error::{input_error, resolver_error, schema_error, GqlError};
use super::ext::IntoExt;
use super::schema::{gql_to_sql_kind, sql_value_to_gql_value};
use crate::dbs::capabilities::RouteTarget::Sql;
use crate::dbs::Session;
use crate::fnc::time::format;
use crate::gql::cursor;
use crate::gql::cursor::{make_list_resolver, make_object_resolver, make_value_resolver, ConnectionContext,
                         ConnectionKind, EdgeContext, PageInfo};
use crate::gql::error::internal_error;
use crate::gql::ext::TryAsExt;
use crate::gql::schema::{kind_to_type, unwrap_type};
use crate::gql::utils::{pluralize, GQLTx, GqlValueUtils};
use crate::iam::base::BASE64;
use crate::kvs::{Datastore, Transaction};
use crate::sql::order::{OrderList, Ordering};
use crate::sql::statements::{DefineFieldStatement, DefineTableStatement, SelectStatement};
use crate::sql::{self, Array, Ident, Literal, Operator, Part, Table, TableType, Value, Values};
use crate::sql::{Cond, Fields};
use crate::sql::{Expression, Value as SqlValue};
use crate::sql::{Idiom, Kind};
use crate::sql::{Statement, Thing};
use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::TypeRef;
use async_graphql::dynamic::{Enum, FieldValue, Type};
use async_graphql::dynamic::{EnumItem, FieldFuture};
use async_graphql::dynamic::{Field, ResolverContext};
use async_graphql::dynamic::{InputObject, Object};
use async_graphql::dynamic::{InputValue, Union};
use async_graphql::Name;
use async_graphql::Value as GqlValue;
use base64::engine::general_purpose;
use base64::Engine;
use inflector::Inflector;
use log::trace;
use sha2::{Digest, Sha256};

macro_rules! first_input {
	() => {
		InputValue::new("first", TypeRef::named(TypeRef::INT))
        .description("Returns the first *n* elements from the list.")
	};
}

macro_rules! last_input {
	() => {
		InputValue::new("last", TypeRef::named(TypeRef::INT))
        .description("Returns the last *n* elements from the list.")
	};
}

macro_rules! before_input {
	() => {
		InputValue::new("before", TypeRef::named(TypeRef::STRING))
        .description("Returns the elements in the list that come before the specified cursor.")
	};
}

macro_rules! after_input {
	() => {
		InputValue::new("after", TypeRef::named(TypeRef::STRING))
        .description("Returns the elements in the list that come after the specified cursor.")
	};
}

macro_rules! limit_input {
	() => {
		InputValue::new("limit", TypeRef::named(TypeRef::INT))
        .description("xxx")
	};
}

macro_rules! id_input {
	() => {
		InputValue::new("id", TypeRef::named(TypeRef::ID))
	};
}

/// This macro needs the order input types to be defined with `define_order_input_types`.
macro_rules! order_input {
	($name: expr) => {
		InputValue::new("orderBy", TypeRef::named(format!("{}Order", $name.to_pascal_case())))
        .description(format!("Ordering options for `{}` connections.", $name))
	};
}

macro_rules! filter_input {
	($name: expr) => {
		InputValue::new("filterBy", TypeRef::named(format!("{}Filter", $name.to_pascal_case())))
	};
}

/// This macro needs the order direction enum type defined. you may use
/// `define_order_direction_enum` for it.
macro_rules! define_order_input_types {
    (
        $types:ident,
        $base_name:expr,
        $( $field_enum_name:ident ),* $(,)?
    ) => {
        let base_name_pascal = $base_name.to_pascal_case();
        let enum_name = format!("{}OrderField", base_name_pascal);
        let obj_name = format!("{}Order", base_name_pascal);

        let order_by_enum = Enum::new(&enum_name)
            .item(EnumItem::new("ID").description(format!("{} by ID.", $base_name)))
            $(.item(EnumItem::new(stringify!($field_enum_name).to_screaming_snake_case())
                .description(format!("{} by {}.",
                $base_name, stringify!($field_enum_name).to_screaming_snake_case()))))*
            .description(format!("Properties by which {} can be ordered.", $base_name));
        $types.push(Type::Enum(order_by_enum));

        let order_by_obj = InputObject::new(&obj_name)
            .field(
                InputValue::new("field", TypeRef::named(&enum_name))
                .description(format!("The field to order {} by.", $base_name)))
            .field(
                InputValue::new("direction", TypeRef::named("OrderDirection"))
                .description("The ordering direction."))
            .description(format!("Ordering options for {} connections", $base_name));
        $types.push(Type::InputObject(order_by_obj))
    };
}

/// Adds a connection field to the specified object.
///
/// # Parameters
/// - (`obj`: The object to which the connection field is added.)
/// - `types`: The types vector to which the connection and edge types are added.
/// - `fd_name`: The name of the connection field.
/// - `node_ty_name`: The name of the node type.
/// - `connection_resolver`: The resolver for the connection field.
/// - `edges`: Additional edge fields.
/// - `args`: Additional connection arguments.
#[macro_export]
macro_rules! cursor_pagination {
    (
        $types:ident,
        $fd_name:expr,
        $node_ty_name:expr,
        $connection_resolver:expr, // The actual resolver for the connection field on $obj
        edge_fields: $edge_fields_expr:expr,
        args: [ $( $extra_connection_arg:expr ),* $(,)? ],
        is_relation: $is_relation:expr
    ) => {
        {
            // We must distinguish between relation and non-relation nodes due to the edge fields.
            // But we still want to use the original Object type for the node type. Thats why we
            // use the `is_relation` boolean to determine the type names in here.
            let edge_type_name = if $is_relation {
                format!("{}RelationEdge", $node_ty_name)
            } else {
                format!("{}Edge", $node_ty_name)
            };

            let connection_type_name = if $is_relation {
                format!("{}RelationConnection", $node_ty_name)
            } else {
                format!("{}Connection", $node_ty_name)
            };

            let mut edge = Object::new(&edge_type_name)
                .field(Field::new(
                    "cursor",
                    TypeRef::named_nn(TypeRef::STRING),
                    make_value_resolver(|e: &EdgeContext| e.cursor.clone()),
                ).description("A cursor for use in pagination."))
                .field(Field::new(
                    "node",
                    TypeRef::named($node_ty_name),
                    make_value_resolver(|e: &EdgeContext| sql_value_to_gql_value(e
                    .node.clone()).unwrap()),
                ).description("The item at the end of the edge."))
                .description("An edge in a connection.");
            for fd in $edge_fields_expr {
                edge = edge.field(fd);
            }

            let connection = Object::new(&connection_type_name)
                .field(Field::new(
                    "edges",
                    TypeRef::named_list(&edge_type_name),
                    make_list_resolver(|conn: &ConnectionContext| {
                        Ok(conn
                            .edges
                            .iter()
                            .map(|ctx| FieldValue::owned_any(ctx.clone()))
                            .collect())
                    }),
                ).description("A list of edges."))
                .field(Field::new(
                    "nodes",
                    TypeRef::named_list($node_ty_name),
                    make_list_resolver(|conn: &ConnectionContext| {
                        conn.edges
                            .iter()
                            .map(|e| {
                                let gql_val = sql_value_to_gql_value(e.node.clone())?;
                                Ok(FieldValue::value(gql_val))
                            })
                            .collect() // This collects into a Result<Vec<FieldValue>, _>
                    }),
                ).description("A list of nodes."))
                .field(Field::new(
                    "pageInfo",
                    TypeRef::named_nn("PageInfo"),
                    make_object_resolver(|conn: &ConnectionContext| conn.page_info.clone()),
                ).description("Information to aid in pagination."))
                .field(Field::new(
                    "totalCount",
                    TypeRef::named_nn(TypeRef::INT),
                    make_value_resolver(|conn: &ConnectionContext| conn.total_count),
                ).description("Identifies the total count of items in the connection."))
                .description(format!("The connection type for {}.", $node_ty_name));

            $types.push(Type::Object(edge));
            $types.push(Type::Object(connection));

            Field::new(
                $fd_name,
                TypeRef::named_nn(&connection_type_name),
                $connection_resolver,

            )
            .description(format!("The connection object for the table `{}`", $fd_name))
            .argument(after_input!())
            .argument(before_input!())
            .argument(first_input!())
            .argument(last_input!())
            $(.argument($extra_connection_arg))*
        }
    };
}

/// This macro is used to parse a field definition and add it to the object map.
/// It handles different kinds of fields, including nested fields and array fields.
/// It also manages the creation of connection fields for array types.
///
/// # Parameters
/// - `$fd`: The field definition to parse.
/// - `$types`: The types vector to which the field type is added.
/// - `$cursor`: A boolean indicating whether to use cursor pagination.
/// - `$tb_name`: The name of the table.
/// - `$map`: The object map to which the field is added.
/// - `$field_ident`: The identifier for the field.
/// - `$action_tokens`: The action tokens to execute after parsing the field.
macro_rules! parse_field {
    (
        $fd:ident,
        $types:ident,
        $cursor:ident,
        $tb_name:ident,
        $map:ident,
        |$field_ident:ident| $($action_tokens:tt)*
    ) => {
        let kind = match $fd.kind.clone() {
            Some(k) => k,
            None => continue
        };
        let kind_non_optional = kind.non_optional().clone();

        let parts: Vec<&Ident> = $fd.name.0.iter().filter_map(|part| match part {
            Part::Field(ident) => Some(ident),
            _ => None
        }).collect();

        // Should always contain at least the field name
        if parts.is_empty() { continue; }

        let fd_name = parts.as_slice().last().unwrap().to_string();
        let fd_name_gql = fd_name.to_camel_case();

        let fd_path = $fd.name.to_path()
            .replace("/", ".")
            .strip_prefix(".")
            .unwrap()
            .to_string();
        let fd_path_parent = remove_last_segment(&*fd_path.as_str());

        // Use table name for e.g., object uniqueness across multiple tables //TODO: not needed
        // for rel fields i think
        let mut path = Vec::with_capacity(parts.len() + 1);
        let table_ident = Ident::from($tb_name.clone());
        path.push(&table_ident);
        path.extend_from_slice(parts.as_slice());

        let fd_ty = kind_to_type(kind.clone(), $types, path.as_slice())?;

        // object map used to add fields step by step to the objects
        if kind_non_optional == Kind::Object {
            $map.insert(
                fd_path.clone(),
                Object::new(fd_ty.type_name())
                    .description(if let Some(ref c) = $fd.comment {
                        format!("{c}")
                    } else {
                        "".to_string()
                    }),
            );
        }

        if fd_path_parent.is_empty() { // top level field
            match kind_non_optional {
                // cursor connections only if specified in config
                Kind::Array(_, _) if $cursor => {
                    if let kind = kind.inner_kind().unwrap() {
                        let ty_ref = kind_to_type(kind.clone(), $types, path.as_slice())?;
                        let ty_name = ty_ref.type_name();

                        let $field_ident = cursor_pagination!($types, &fd_name_gql, ty_name,
                        make_connection_resolver(fd_path.as_str(), ConnectionKind::Field),
                        edge_fields: [], args: [], is_relation: false);
                        $($action_tokens)*;
                    }
                }
                _ => {
                     let $field_ident = Field::new(
                            fd_name_gql,
                            fd_ty,
                            make_field_resolver(fd_path.as_str()),
                        )
                        .description(if let Some(ref c) = $fd.comment {
                            format!("{c}")
                        } else {
                            "".to_string()
                        });
                    $($action_tokens)*;
                }
            }
        } else { // nested field
            // Array inner type is scalar, thus already set when adding the list field
            if fd_path.chars().last() == Some('*') { continue; }

            // expects the parent's `DefineFieldStatement` to come before its children as is
            // with `tx.all_tb_fields()`
            match $map.remove(&fd_path_parent) {
                Some(obj) => {
                    let new_field = match kind_non_optional {
                        Kind::Array(_, _) if $cursor => {
                            if let kind = kind.inner_kind().unwrap() {
                                let ty_ref = kind_to_type(kind.clone(), $types, path.as_slice())?;
                                let ty_name = ty_ref.type_name();

                                cursor_pagination!($types, &fd_name_gql, ty_name,
                                    make_connection_resolver(fd_path.as_str(), ConnectionKind::Field),
                                    edge_fields: [], args: [], is_relation: false)
                            } else {
                                // Fallback for safety, though inner_kind should always exist for an array
                                Field::new(fd_name_gql, fd_ty, make_field_resolver(fd_path.as_str()))
                            }
                        }
                        _ => {
                            Field::new(
                                fd_name_gql,
                                fd_ty,
                                make_field_resolver(fd_path.as_str()),
                            )
                        }
                    };
                    $map.insert(fd_path_parent.clone(), Object::from(obj)
                        .field(new_field.description(if let Some(ref c) = $fd.comment {
                            format!("{c}")
                        } else {
                            "".to_string()
                        }))
                    );
                }
                None => return Err(internal_error("Nested field should have parent object.")),
            }
        }
    };
}

fn filter_name_from_table(tb_name: impl Display) -> String {
    // format!("Filter{}", tb_name.to_string().to_sentence_case())
    format!("{}FilterInput", tb_name.to_string().to_pascal_case())
}

fn remove_last_segment(input: &str) -> String {
    let mut parts = input.rsplitn(2, '.'); // Split from the right, limit to 2 parts
    parts.next(); // Discard the last segment
    parts.next().unwrap_or("").to_string() // Take the remaining part
}

fn remove_leading_dot(input: &str) -> &str {
    input.strip_prefix('.').unwrap_or(input)
}

#[allow(clippy::too_many_arguments)]
pub async fn process_tbs(
    tbs: Arc<[DefineTableStatement]>,
    mut query: Object,
    types: &mut Vec<Type>,
    tx: &Transaction,
    ns: &str,
    db: &str,
    cursor: bool,
) -> Result<Object, GqlError> {
    // Type::Any is not supported. FIXME: throw error in the future.
    let (tables, relations): (Vec<&DefineTableStatement>, Vec<&DefineTableStatement>) = tbs
        .iter().partition(|tb| {
        match tb.kind {
            TableType::Normal => true,
            TableType::Relation(_) => false,
            TableType::Any => false,
        }
    });

    for tb in tables.iter() {
        let tb_name = tb.name.to_string();
        let first_tb_name = tb_name.clone();
        let second_tb_name = tb_name.clone();
        let tb_name_gql = tb_name.to_pascal_case();
        let tb_name_query = tb_name.to_camel_case(); // field name for the table in the query

        let mut gql_objects: BTreeMap<String, Object> = BTreeMap::new();

        let fds = tx.all_tb_fields(ns, db, &tb.name.0, None).await?;

        let mut tb_ty_obj = Object::new(tb_name_gql.clone())
            .field(Field::new(
                "id",
                TypeRef::named_nn(TypeRef::ID),
                make_field_resolver("id"),
            ))
            .implement("Record");

        // =======================================================
        // Parse Fields
        // =======================================================
        for fd in fds.iter() {
            // We have already defined "id", so we don't take any new definition for it.
            if fd.name.is_id() { continue; };

            parse_field!(fd, types, cursor, tb_name, gql_objects, |fd| tb_ty_obj = tb_ty_obj
                .field(fd));
        }

        // =======================================================
        // Add filters
        // =======================================================

        // Add additional orderBy fields here:
        define_order_input_types!(types, tb_name,);

        // =======================================================
        // Add single instance query
        // =======================================================
        query = query.field(
            Field::new(
                tb_name_query.to_singular(),
                TypeRef::named(&tb_name_gql),
                move |ctx| {
                    let tb_name = first_tb_name.clone();
                    FieldFuture::new({
                        async move {
                            let gtx = ctx.data::<GQLTx>()?;

                            let args = ctx.args.as_index_map();
                            let id = match args.get("id").and_then(GqlValueUtils::as_string) {
                                Some(i) => i,
                                None => {
                                    return Err(input_error(
                                        "Schema validation failed: No id found in arguments",
                                    )
                                        .into());
                                }
                            };
                            let thing = match id.clone().try_into() {
                                Ok(t) => t,
                                Err(_) => Thing::from((tb_name, id)),
                            };

                            match gtx.get_record_field(thing, "id").await? {
                                SqlValue::Thing(t) => {
                                    Ok(Some(FieldValue::owned_any(t)))
                                }
                                _ => Ok(None),
                            }
                        }
                    })
                },
            )
                .description(if let Some(ref c) = &tb.comment {
                    format!("{c}")
                } else {
                    format!("Generated from table `{}`\nallows querying a single record in a table by ID", &tb_name)
                })
                .argument(id_input!()),
        );

        // =======================================================
        // Add all instances query
        // =======================================================
        let tb_name_plural = pluralize(tb_name_query.clone());

        if cursor {
            query = query.field(
                cursor_pagination!(
                types,
                &tb_name_plural,
                &tb_name_gql,
                make_connection_resolver(&tb_name_query, ConnectionKind::Table),
                edge_fields: [],
                args: [
                    order_input!(&tb_name)
                ],
                is_relation: false
            ));
        } else {
            query = query.field(
                Field::new(
                    tb_name_plural,
                    TypeRef::named_nn_list_nn(&tb_name_gql),
                    move |ctx| {
                        let tb_name = second_tb_name.clone();
                        FieldFuture::new(async move {
                            let gtx = ctx.data::<GQLTx>()?;

                            let args = ctx.args.as_index_map();
                            trace!("received request with args: {args:?}");

                            // let start = args.get("start").and_then(|v| v.as_i64()).map(|s| s.intox());
                            //
                            // let limit = args.get("limit").and_then(|v| v.as_i64()).map(|l| l.intox());
                            //
                            // let order = args.get("order");
                            //
                            // let filter = args.get("filter");

                            // let orders = match order {
                            //     Some(GqlValue::Object(o)) => {
                            //         let mut orders = vec![];
                            //         let mut current = o;
                            //         loop {
                            //             let asc = current.get("asc");
                            //             let desc = current.get("desc");
                            //             match (asc, desc) {
                            //                 (Some(_), Some(_)) => {
                            //                     return Err("Found both ASC and DESC in order".into());
                            //                 }
                            //                 (Some(GqlValue::Enum(a)), None) => {
                            //                     orders.push(order!(asc, a.as_str()))
                            //                 }
                            //                 (None, Some(GqlValue::Enum(d))) => {
                            //                     orders.push(order!(desc, d.as_str()))
                            //                 }
                            //                 (_, _) => {
                            //                     break;
                            //                 }
                            //             }
                            //             if let Some(GqlValue::Object(next)) = current.get("then") {
                            //                 current = next;
                            //             } else {
                            //                 break;
                            //             }
                            //         }
                            //         Some(orders)
                            //     }
                            //     _ => None,
                            // };
                            // trace!("parsed orders: {orders:?}");

                            // let cond = match filter {
                            //     Some(f) => {
                            //         let o = match f {
                            //             GqlValue::Object(o) => o,
                            //             f => {
                            //                 error!("Found filter {f}, which should be object and should have been rejected by async graphql.");
                            //                 return Err("Value in cond doesn't fit schema".into());
                            //             }
                            //         };
                            //
                            //         let cond = cond_from_filter(o, &fds2)?;
                            //
                            //         Some(cond)
                            //     }
                            //     None => None,
                            // };
                            // trace!("parsed filter: {cond:?}");

                            // SELECT VALUE id FROM ...
                            let ast = Statement::Select({
                                SelectStatement {
                                    what: vec![SqlValue::Table(tb_name.intox())].into(),
                                    expr: Fields(
                                        vec![sql::Field::Single {
                                            expr: SqlValue::Idiom(Idiom::from("id")),
                                            alias: None,
                                        }],
                                        // this means the `value` keyword
                                        true,
                                    ),
                                    // order: orders.map(|x| Ordering::Order(OrderList(x))),
                                    // cond,
                                    // limit,
                                    // start,
                                    ..Default::default()
                                }
                            });
                            trace!("generated query ast: {ast:?}");

                            let res = gtx.process_stmt(ast).await?;

                            trace!("query result: {res:?}");

                            let res_vec =
                                match res {
                                    SqlValue::Array(a) => a,
                                    v => {
                                        error!("Found top level value, in result which should be array: {v:?}");
                                        return Err("Internal Error".into());
                                    }
                                };

                            trace!("query result array: {res_vec:?}");

                            let out: Result<Vec<FieldValue>, SqlValue> = res_vec
                                .0
                                .into_iter()
                                .map(|v| {
                                    v.try_as_thing().map(|t| {
                                        // let erased: ErasedRecord = (gtx.clone(), t);
                                        // field_val_erase_owned(erased)
                                        FieldValue::owned_any(t)
                                    })
                                })
                                .collect();

                            match out {
                                Ok(l) => Ok(Some(FieldValue::list(l))),
                                Err(v) => {
                                    Err(internal_error(format!("expected thing, found: {v:?}")).into())
                                }
                            }
                        })
                    },
                )
                    .description(if let Some(ref c) = &tb.comment {
                        format!("{c}")
                    } else {
                        format!("Generated from table `{}`\nallows querying a table with filters",
                                &tb_name)
                    })
                    .argument(limit_input!())
                    .argument(order_input!(&tb_name))
                // .argument(filter_input!(&tb_name))
            );
        }

        // =======================================================
        // Add relations
        // =======================================================

        for rel in relations.iter().filter(|stmt| {
            match &stmt.kind {
                TableType::Relation(r) => match &r.from {
                    Some(Kind::Record(tbs)) => tbs.contains(&Table::from(tb_name.clone())),
                    _ => false,
                },
                _ => false,
            }
        }) {
            let rel_name = rel.name.to_string();

            let (ins, outs) = match &rel.kind {
                TableType::Relation(r) => match (&r.from, &r.to) {
                    (Some(Kind::Record(from)), Some(Kind::Record(to))) => (from, to),
                    _ => continue,
                },
                _ => continue,
            };

            let mut fd_map: BTreeMap<String, Object> = BTreeMap::new();
            let mut fd_vec = Vec::<Field>::new();

            let fds = tx.all_tb_fields(ns, db, &rel.name.0, None).await?;

            //todo?: das hier nur n mal machen. Also nur dann wenn nicht vec ins > 1, bzw schon in map
            // possible performance improvements by skipping fields for prev relations
            for fd in fds.iter().filter(|fd|
                !matches!(fd.name.to_string().as_str(), "in" | "out" | "id")
            ) {
                parse_field!(fd, types, cursor, rel_name, fd_map, |fd| fd_vec.push(fd));
            }

            // Node type for the relation connection
            let node_ty_name = match outs.len() {
                // we have only one `to` table, thus we can use the object type directly
                1 => outs.first().unwrap().to_string().to_pascal_case(),
                // we have more than one `to` table, thus we need a union type
                _ => {
                    let mut tmp_union = Union::new(format!("{}Union", rel.name.to_raw().to_pascal_case()));
                    for n in outs {
                        tmp_union = tmp_union.possible_type(n.0.to_string().to_pascal_case());
                    }
                    // async_graphql types do not implement clone, thus we need to get the typename
                    // before the move
                    let union_name = tmp_union.type_name().to_string();
                    types.push(Type::Union(tmp_union));

                    union_name
                }
            };

            tb_ty_obj = tb_ty_obj.field(
                cursor_pagination!(
                types,
                pluralize(rel.name.to_raw().to_camel_case()),
                &node_ty_name,
                make_connection_resolver(rel.name.to_raw(), ConnectionKind::Relation),
                edge_fields: fd_vec,
                args: [
                    order_input!(&tb_name)
                ],
                is_relation: true
            ));

            define_order_input_types!(types, rel.name.to_raw(),);

            for (_, obj) in fd_map {
                types.push(Type::Object(obj));
            }
        }

        // =======================================================
        // Add types
        // =======================================================
        // for loop because Type::Object needs owned obj, not a reference
        for (_, obj) in gql_objects {
            types.push(Type::Object(obj));
        }
        types.push(Type::Object(tb_ty_obj));
    }

    Ok(query)
}

/// A generic resolver for fields on any object type (table records or connection edges).
///
/// It intelligently handles different parent contexts:
/// - If the parent is a `Thing`, it fetches the field from the database.
/// - If the parent is a `SqlValue`, it resolves the field from the pre-fetched object data.
fn make_field_resolver(
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

#[allow(clippy::too_many_lines)] // Due to detailed logic
fn make_connection_resolver(
    // For table connections: table name (e.g., "user")
    // For relation connections: relation table name (e.g., "authored_posts")
    name_for_query_source: impl Into<String>,
    // Kind of items being paginated (e.g., Kind::Record for users or relation records)
    // We'll use this to understand if it's a relation and to get table/relation schema if needed.
    // For now, we primarily use name_for_query_source and parent_rid to infer.
    // item_kind: Option<Kind>,
    // We need the schema definition of the relation if this is a relation connection
    // to correctly determine 'in'/'out' fields and target node type.
    // This is a simplification for now: assumes 'in' links to parent, 'out' to target.
    // relation_def: Option<Arc<DefineTableStatement>>, // TODO: Pass this if needed
    // is_relation: bool,
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

                // if let GqlValue::Object(obj) = val {
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
                    ..Default::default()
                });
                gtx.process_stmt(ast).await?
            };

            let first = args.get("first").and_then(GqlValueUtils::as_i64).map(|v| v as usize);
            let last = args.get("last").and_then(GqlValueUtils::as_i64).map(|v| v as usize);
            let after_cursor_str = args.get("after").and_then(GqlValueUtils::as_string);
            let before_cursor_str = args.get("before").and_then(GqlValueUtils::as_string);
            let order_by_arg = args.get("orderBy");

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

            let edges = cursor::apply_cursors_to_edges(
                all_edges, after_cursor_str.clone(), before_cursor_str.clone());

            let mut limited_edges = edges;
            if let Some(first_val) = first {
                limited_edges = &limited_edges[..first_val.min(limited_edges.len())];
            } else if let Some(last_val) = last {
                let start = limited_edges.len().saturating_sub(last_val);
                limited_edges = &limited_edges[start..];
            }

            //fixme: dont do duplicated. get from edges.
            let start_cursor = limited_edges.first().map(cursor::encode_cursor);
            let end_cursor = limited_edges.last().map(cursor::encode_cursor);


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
                        cursor: cursor::encode_cursor(item),
                        edge: edge.clone(),
                        node: node.clone(),
                    }
                })
                .collect();


            let page_info = PageInfo {
                has_next_page: cursor::has_next_page(edges, before_cursor_str.as_deref(), first),
                has_previous_page: cursor::has_previous_page(edges, after_cursor_str.as_deref(), last),
                start_cursor,
                end_cursor,
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

macro_rules! filter_impl {
	($filter:ident, $ty:ident, $name:expr) => {
		$filter = $filter.field(InputValue::new(format!("{}", $name), $ty.clone()));
	};
}

//FIXME: implement
fn dummy_resolver(
    db_name: String, // DB name (e.g., "created_at", "size")
    kind: Option<Kind>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
    move |_ctx: ResolverContext| {
        FieldFuture::new(async move {
            Ok(Some(FieldValue::value("".to_string()))) // Return `None` as a placeholder
        })
    }
}

fn filter_id() -> InputObject {
    let mut filter = InputObject::new("IDFilterInput");
    let ty = TypeRef::named(TypeRef::ID);
    filter_impl!(filter, ty, "eq");
    filter_impl!(filter, ty, "ne");
    filter
}
fn filter_from_type(
    kind: Kind,
    filter_name: String,
    types: &mut Vec<Type>,
) -> Result<InputObject, GqlError> {
    let ty = match &kind {
        Kind::Record(ts) => match ts.len() {
            1 => TypeRef::named(TypeRef::ID),
            _ => TypeRef::named(filter_name_from_table(
                ts.first().expect("ts should have exactly one element").as_str(),
            )),
        },
        //TODO: remove none
        // k => unwrap_type(kind_to_type(k.clone(), types, None)?),
        k => TypeRef::named("UNIMPLEMENTED"),
    };

    let mut filter = InputObject::new(filter_name);
    filter_impl!(filter, ty, "eq");
    filter_impl!(filter, ty, "ne");

    match kind {
        Kind::Any => {}
        Kind::Null => {}
        Kind::Bool => {}
        Kind::Bytes => {}
        Kind::Datetime => {}
        Kind::Decimal => {}
        Kind::Duration => {}
        Kind::Float => {}
        Kind::Int => {}
        Kind::Number => {}
        Kind::Object => {}
        Kind::Point => {}
        Kind::String => {}
        Kind::Uuid => {}
        Kind::Regex => {}
        Kind::Record(_) => {}
        Kind::Geometry(_) => {}
        Kind::Option(_) => {}
        Kind::Either(_) => {}
        Kind::Set(_, _) => {}
        Kind::Array(_, _) => {}
        Kind::Function(_, _) => {}
        Kind::Range => {}
        Kind::Literal(_) => {}
        Kind::References(_, _) => {}
        Kind::File(_) => {}
    };
    Ok(filter)
}

// fn cond_from_filter(
//     filter: &IndexMap<Name, GqlValue>,
//     fds: &[DefineFieldStatement],
// ) -> Result<Cond, GqlError> {
//     // val_from_filter(filter, fds).map(IntoExt::intox)
//     // Start recursion with an empty path prefix
//     val_from_filter(filter, fds, &[]).map(IntoExt::intox)
// }

// fn val_from_filter(
//     filter: &IndexMap<Name, GqlValue>,
//     fds: &[DefineFieldStatement],
//     current_path: &[String],
// ) -> Result<SqlValue, GqlError> {
//     if filter.len() != 1 {
//         let path_str = current_path.join(".");
//         return Err(resolver_error(format!("Filter object at path '{}' must have exactly one key (field, and, or, not)", path_str)));
//     }
//
//     let (k, v) = filter.iter().next().unwrap();
//     let key_str = k.as_str();
//
//     let cond = match key_str.to_lowercase().as_str() { // Keep matching lowercase for operators
//         "or" => aggregate(v, AggregateOp::Or, fds, current_path), // Pass path down
//         "and" => aggregate(v, AggregateOp::And, fds, current_path), // Pass path down
//         "not" => negate(v, fds, current_path), // Pass path down
//         _ => { // Assume it's a field name (camelCase from schema)
//             // Construct the new path segment
//             let mut next_path = current_path.to_vec();
//             next_path.push(key_str.to_string()); // Add the camelCase field name
//
//             // Find the DB field definition matching the potential full path
//             // This might require looking up the base field and checking if it's an object,
//             // then checking the sub-field within the nested structure.
//             // For simplicity here, we'll assume we can find the field kind based on the path.
//             let field_kind = find_field_kind_by_path(&next_path, fds)?; // Implement this helper
//
//             match field_kind {
//                 // If the path points to a nested object, recurse
//                 Kind::Object => {
//                     let inner_filter = v.as_object().ok_or_else(|| resolver_error(format!("Value for object filter '{}' must be an object", next_path.join("."))))?;
//                     val_from_filter(inner_filter, fds, &next_path) // Recurse with extended path
//                 }
//                 // If it's a scalar/record/enum etc., call binop
//                 _ => Ok({
//                     binop(&next_path, v, field_kind)? // Pass full path and kind
//                 })
//             }
//         }
//     };
//
//     cond
//     // if filter.len() != 1 {
//     // 	return Err(resolver_error("Table Filter must have one item"));
//     // }
//     //
//     // let (k, v) = filter.iter().next().unwrap();
//     //
//     // let cond = match k.as_str().to_lowercase().as_str() {
//     // 	"or" => aggregate(v, AggregateOp::Or, fds),
//     // 	"and" => aggregate(v, AggregateOp::And, fds),
//     // 	"not" => negate(v, fds),
//     // 	_ => binop(k.as_str(), v, fds),
//     // };
//     //
//     // cond
// }

fn parse_op(name: impl AsRef<str>) -> Result<sql::Operator, GqlError> {
    match name.as_ref() {
        "eq" => Ok(sql::Operator::Equal),
        "ne" => Ok(sql::Operator::NotEqual),
        op => Err(resolver_error(format!("Unsupported op: {op}"))),
    }
}

fn find_field_kind_by_path(path: &[String], fds: &Arc<Vec<DefineFieldStatement>>) -> Result<Kind, GqlError> {
    // Convert GQL camelCase path back to DB snake_case/dot.notation path
    // This assumes a simple reversible mapping, might need adjustment
    let db_path_str = path.iter()
        .map(|p| p.to_snake_case()) // Convert each segment
        .collect::<Vec<_>>()
        .join("."); // Join with dots

    fds.iter()
        .find(|fd| fd.name.to_string() == db_path_str)
        .and_then(|fd| fd.kind.clone())
        .ok_or_else(|| resolver_error(format!("Field definition not found for path '{}' (DB path '{}')", path.join("."), db_path_str)))
}

// fn negate(filter: &GqlValue, fds: &Arc<Vec<DefineFieldStatement>>, current_path: &[String]) -> Result<SqlValue, GqlError> {
//     let obj = filter.as_object().ok_or(resolver_error("Value of NOT must be object"))?;
//
//     let inner_cond = val_from_filter(obj, fds, current_path)?;
//     Ok(Expression::Unary { o: sql::Operator::Not, v: inner_cond }.into())
// }

enum AggregateOp {
    And,
    Or,
}

// fn aggregate(
//     filter: &GqlValue,
//     op: AggregateOp,
//     fds: &Arc<Vec<DefineFieldStatement>>,
//     current_path: &[String],
// ) -> Result<SqlValue, GqlError> {
//     let op_str = match op {
//         AggregateOp::And => "AND",
//         AggregateOp::Or => "OR",
//     };
//     let op = match op {
//         AggregateOp::And => sql::Operator::And,
//         AggregateOp::Or => sql::Operator::Or,
//     };
//     let list =
//         filter.as_list().ok_or(resolver_error(format!("Value of {op_str} should be a list")))?;
//     let filter_arr = list
//         .iter()
//         .map(|v| v.as_object().map(|o| val_from_filter(o, fds, current_path)))
//         .collect::<Option<Result<Vec<SqlValue>, GqlError>>>()
//         .ok_or(resolver_error(format!("List of {op_str} should contain objects")))??;
//
//     let mut iter = filter_arr.into_iter();
//
//     let mut cond = iter
//         .next()
//         .ok_or(resolver_error(format!("List of {op_str} should contain at least one object")))?;
//
//     for clause in iter {
//         cond = Expression::Binary {
//             l: clause,
//             o: op.clone(),
//             r: cond,
//         }
//             .into();
//     }
//
//     Ok(cond)
// }

fn binop(
    gql_path: &[String], // e.g., ["size", "width"]
    val: &GqlValue,     // e.g., { eq: 100 }
    field_kind: Kind, // The Kind of the specific field at the end of the path
) -> Result<SqlValue, GqlError> {
    let obj = val.as_object().ok_or_else(|| resolver_error(format!("Filter value for '{}' must be an object", gql_path.join("."))))?;

    if obj.len() != 1 {
        return Err(resolver_error(format!("Filter operation object for '{}' must have exactly one key (e.g., eq, gt)", gql_path.join("."))));
    }

    // Convert GQL path (camelCase) back to DB path (snake_case.dot) for SQL Idiom
    // ASSUMPTION: Simple reversible mapping. May need adjustment.
    let db_path_str = gql_path.iter().map(|p| p.to_snake_case()).collect::<Vec<_>>().join(".");
    let lhs = sql::Value::Idiom(db_path_str.intox()); // Use the full DB path

    let (k, v) = obj.iter().next().unwrap(); // k is the operator name (e.g., "eq")
    let op = parse_op(k)?; // Parse "eq", "ne", etc. (Needs expansion)

    // Convert the GQL value 'v' (e.g., Number(100)) to SQL using the specific field's Kind
    let rhs = gql_to_sql_kind(v, field_kind)?;

    Ok(sql::Expression::Binary { l: lhs, o: op, r: rhs }.into())
}

fn parse_order_input(order: Option<&GqlValue>) -> Result<Option<Vec<sql::Order>>, GqlError> {
    let Some(GqlValue::Object(o)) = order else { return Ok(None) };

    let mut orders = vec![];
    let mut current = o;

    loop {
        let Some(GqlValue::Enum(fd_name_enum)) = current.get("field") else {
            return Err(resolver_error("Order input must contain 'field' enum"));
        };
        let Some(GqlValue::Enum(direction_enum)) = current.get("direction") else {
            return Err(resolver_error("Order input must contain 'direction' enum (ASC/DESC)"));
        };

        let fd_name_screaming = fd_name_enum.as_str(); // e.g., "CREATED_AT", "SIZE_WIDTH"
        // Convert SCREAMING_SNAKE_CASE back to DB snake_case.dot notation
        let db_fd_name = fd_name_screaming.to_lowercase(); // Simple conversion, might need underscores replaced with dots

        let direction_is_asc = direction_enum.as_str() == "ASC";

        let mut order_clause = sql::Order::default();
        order_clause.value = db_fd_name.into(); // Use DB name/path
        order_clause.direction = direction_is_asc;
        orders.push(order_clause);

        // Check for chained 'then'
        if let Some(GqlValue::Object(next)) = current.get("then") {
            current = next;
        } else {
            break;
        }
    }
    Ok(Some(orders))
}


//TODO: resolve with get_record_field funktioniert fuer ein level nested.
// hier bei 'size.location.`info`' findet er obvious nicht: None -> Error


// TODO: auch bei .url oder users.0.image.size.height
// weil hier path: size.height und der nicht gefunden wird fuer query
// SELECT * FROM user:id


// 2025-04-22T19:32:25.885157Z TRACE request: surrealdb_core::gql::tables: /Volumes/Development/Dev/RustroverProjects/surrealdb/crates/core/src/gql/tables.rs:659: Creating/Running resolver for DB path 'size.location.`info`' (Kind: Some(Float)) with parent: (surrealdb_core::gql::utils::GQLTx, surrealdb_core::sql::thing::Thing)     otel.kind="server" http.request.method="POST" url.path="/graphql" network.protocol.name="http" network.protocol.version="1.1" http.request.body.size="244" user_agent.original="PostmanClient/11.36.1 (AppId=90094569-b9aa-467a-902b-a9c21e5e66af)" otel.name="POST /graphql" http.route="/graphql" http.request.id="ff04a2ef-5af1-48dd-a50d-4d1ad73bd444" client.address="127.0.0.1"
// 2025-04-22T19:32:25.885164Z TRACE request: surrealdb_core::gql::tables: /Volumes/Development/Dev/RustroverProjects/surrealdb/crates/core/src/gql/tables.rs:672: Parent is ErasedRecord for path 'size.location.`info`', RID: image:ppli74w1kj8biujquu43     otel.kind="server" http.request.method="POST" url.path="/graphql" network.protocol.name="http" network.protocol.version="1.1" http.request.body.size="244" user_agent.original="PostmanClient/11.36.1 (AppId=90094569-b9aa-467a-902b-a9c21e5e66af)" otel.name="POST /graphql" http.route="/graphql" http.request.id="ff04a2ef-5af1-48dd-a50d-4d1ad73bd444" client.address="127.0.0.1"
// 2025-04-22T19:32:25.885172Z TRACE request: surrealdb_core::gql::tables: /Volumes/Development/Dev/RustroverProjects/surrealdb/crates/core/src/gql/tables.rs:688: Field at path 'size.location.`info`' is scalar/id/terminal, fetching value via get_record_field     otel.kind="server" http.request.method="POST" url.path="/graphql" network.protocol.name="http" network.protocol.version="1.1" http.request.body.size="244" user_agent.original="PostmanClient/11.36.1 (AppId=90094569-b9aa-467a-902b-a9c21e5e66af)" otel.name="POST /graphql" http.route="/graphql" http.request.id="ff04a2ef-5af1-48dd-a50d-4d1ad73bd444" client.address="127.0.0.1"
// 2025-04-22T19:32:25.885188Z TRACE request: surrealdb::core::dbs: crates/core/src/dbs/iterator.rs:354: Iterating statement statement=SELECT * FROM image:ppli74w1kj8biujquu43 otel.kind="server" http.request.method="POST" url.path="/graphql" network.protocol.name="http" network.protocol.version="1.1" http.request.body.size="244" user_agent.original="PostmanClient/11.36.1 (AppId=90094569-b9aa-467a-902b-a9c21e5e66af)" otel.name="POST /graphql" http.route="/graphql" http.request.id="ff04a2ef-5af1-48dd-a50d-4d1ad73bd444" client.address="127.0.0.1"
// 2025-04-22T19:32:25.885258Z TRACE request: surrealdb_core::gql::tables: /Volumes/Development/Dev/RustroverProjects/surrealdb/crates/core/src/gql/tables.rs:695: Fetched value for path 'size.location.`info`': None     otel.kind="server" http.request.method="POST" url.path="/graphql" network.protocol.name="http" network.protocol.version="1.1" http.request.body.size="244" user_agent.original="PostmanClient/11.36.1 (AppId=90094569-b9aa-467a-902b-a9c21e5e66af)" otel.name="POST /graphql" http.route="/graphql" http.request.id="ff04a2ef-5af1-48dd-a50d-4d1ad73bd444" client.address="127.0.0.1"


// let mut limit_query: Option<usize> = None;
// let mut fetch_forwards = true;
// let requested_count = match (first, last) {
//     (Some(f), None) => {
//         limit_query = Some(f + 1);
//         fetch_forwards = true;
//         f
//     }
//     (None, Some(l)) => {
//         limit_query = Some(l + 1);
//         fetch_forwards = false;
//         l
//     }
//     (None, None) => {
//         limit_query = Some(20 + 1);
//         fetch_forwards = true;
//         20
//     } // Default page size
//     (Some(_), Some(_)) => unreachable!(), // Already checked
// };
//
//
// let after_thing = after_cursor_str.map(|c| decode_cursor(&c)).transpose()?;
// let before_thing = before_cursor_str.map(|c| decode_cursor(&c)).transpose()?;

// let mut sql_ordering = parse_order_by(order_by_arg)?.unwrap_or_else(|| {
//     Ordering::Order(OrderList(vec![sql::Order {
//         value: SqlValue::Idiom(Idiom::from("id")),
//         direction: true, // ASC
//     }]))
// });

// if !fetch_forwards { // Paginating backwards (last/before)
//     if let Ordering::Order(OrderList(ref mut orders)) = sql_ordering {
//         for order_item in orders.iter_mut() {
//             order_item.direction = !order_item.direction; // Reverse query order
//         }
//     }
// }

// let mut query_cond: Option<Cond> = None;
// let cursor_thing_opt = if fetch_forwards { after_thing.as_ref() } else { before_thing.as_ref() };
//
// if let Some(cursor_thing) = cursor_thing_opt {
//     if let Ordering::Order(OrderList(orders)) = &sql_ordering {
//         if let Some(primary_order) = orders.first() {
//             // Simplified: assumes cursor is the ID of the item, and ordering is on 'id'.
//             // For general cursor logic with arbitrary orderBy, the cursor must contain
//             // the sort values of the item it points to.
//             // Here, we assume primary_order.value is 'id' if a cursor is used.
//             if let SqlValue::Idiom(idiom) = &primary_order.value {
//                 if idiom.to_string() == "id" {
//                     let op_str = if fetch_forwards { // after
//                         if primary_order.direction { ">" } else { "<" } // ASC: id > cursor_id, DESC: id < cursor_id
//                     } else { // before (query order is already reversed)
//                         if primary_order.direction { ">" } else { "<" } // Reversed ASC (effectively DESC): id > cursor_id => original id < cursor_id
//                         // Reversed DESC (effectively ASC): id < cursor_id => original id > cursor_id
//                     };
//                     query_cond = Some(Cond(Expression::Binary {
//                         l: Box::new(SqlValue::Idiom(Idiom::from("id"))),
//                         o: sql::Operator::from_str(op_str).map_err(|_| internal_error("Invalid operator string"))?,
//                         r: Box::new(SqlValue::Thing(cursor_thing.clone())),
//                     }));
//                 } else {
//                     return Err(input_error("Cursor pagination is currently only supported with orderBy ID.").into());
//                 }
//             } else {
//                 return Err(input_error("Unsupported orderBy field type for cursor pagination.").into());
//             }
//         }
//     }
// }
//
// let mut select_stmt = SelectStatement {
//     what: Fields(vec![sql::Field::All], false), // SELECT *
//     expr: vec![SqlValue::Table(Table::from(query_source.clone()))].into(),
//     order: Some(sql_ordering.clone()),
//     cond: query_cond.clone(),
//     limit: limit_query.map(|l| Limit(SqlValue::Number(sql::Number::from(l)))),
//     ..Default::default()
// };
//
// let is_relation_connection = parent_rid_opt.is_some();
// if let Some(parent_rid) = &parent_rid_opt {
//     // This is a nested connection, assumed to be a relation.
//     // `query_source` is the relation table name.
//     // Assume 'in' field links to parent, 'out' to target. This needs to be configurable or schema-aware.
//     // Example: User (parent_rid) -> Authored (query_source) -> Post (target)
//     // Query: SELECT * FROM Authored WHERE in = User.id
//     let relation_filter_cond = Cond(Expression::Binary {
//         l: Box::new(SqlValue::Idiom(Idiom(vec![Part::Field("in".into())]))), // Configurable: field linking to parent
//         o: sql::Operator::Equal,
//         r: Box::new(SqlValue::Thing(parent_rid.clone())),
//     });
//     select_stmt.cond = match select_stmt.cond.take() {
//         Some(existing_cond) => Some(Cond(Expression::Binary {
//             l: Box::new(Expression::Paren(Box::new(existing_cond.0))),
//             o: sql::Operator::And,
//             r: Box::new(relation_filter_cond.0),
//         })),
//         None => Some(relation_filter_cond),
//     };
// }
//
// let ast = Statement::Select(select_stmt.clone()); // Clone for total_count later
// trace!("Connection query AST: {:?}", ast);
//
// let query_result_values = match gtx.process_stmt(ast).await? {
//     SqlValue::Array(a) => a.0,
//     v => return Err(internal_error(format!("Expected array from DB, got {:?}", v)).into()),
// };
//
// let mut fetched_items = query_result_values;
// if !fetch_forwards { // If 'last', results were fetched in reverse query order, reverse them back
//     fetched_items.reverse();
// }
//
// let mut page_info = GqlPageInfo::default();
// let has_extra_item = fetched_items.len() > requested_count;
//
// if fetch_forwards {
//     page_info.has_next_page = has_extra_item;
//     if has_extra_item { fetched_items.truncate(requested_count); }
// } else { // Paginating backwards (last/before)
//     page_info.has_previous_page = has_extra_item;
//     if has_extra_item { fetched_items.drain(0..1); } // Remove extra from beginning
// }
//
// let mut gql_edges = Vec::new();
// let mut gql_nodes = Vec::new();
//
// for item_sql_val in fetched_items {
//     let record_thing = match item_sql_val { // This is either a data record or a relation record
//         SqlValue::Thing(t) => t,
//         _ => return Err(internal_error("Expected Thing in connection result items").into()),
//     };
//
//     let node_for_edge_erased: FieldValue;
//     let cursor_for_edge: String;
//
//     if is_relation_connection {
//         // `record_thing` is the relation record (e.g., from 'Authored' table)
//         cursor_for_edge = encode_cursor(&record_thing);
//
//         // Fetch the target node of the relation. Assume 'out' field.
//         // This needs to be schema-aware for robust relation handling.
//         let target_node_id_val = gtx.get_record_field(record_thing.clone(), "out").await?;
//         let target_node_thing = match target_node_id_val {
//             SqlValue::Thing(t) => t,
//             SqlValue::Null | SqlValue::None => {
//                 // If target can be null, GQL node type should be nullable.
//                 // For now, error if target is missing for a relation.
//                 return Err(internal_error(format!("Relation edge {} target ('out' field) is null or not a Thing", record_thing)).into());
//             }
//             other => return Err(internal_error(format!("Relation edge {} target ('out' field) is not a Thing: {:?}", record_thing, other)).into()),
//         };
//         let erased_target_node: ErasedRecord = (gtx.clone(), target_node_thing);
//         node_for_edge_erased = field_val_erase_owned(erased_target_node);
//     } else {
//         // `record_thing` is the data record itself (e.g., from 'User' table)
//         cursor_for_edge = encode_cursor(&record_thing);
//         let erased_node: ErasedRecord = (gtx.clone(), record_thing);
//         node_for_edge_erased = field_val_erase_owned(erased_node);
//     }
//
//     gql_nodes.push(node_for_edge_erased.clone());
//     gql_edges.push(GqlEdge {
//         node: node_for_edge_erased,
//         cursor: cursor_for_edge,
//         additional_fields: IndexMap::new(), // Populate if edge_fields are used
//     });
// }
//
// if let Some(first_edge) = gql_edges.first() {
//     page_info.start_cursor = Some(first_edge.cursor.clone());
// }
// if let Some(last_edge) = gql_edges.last() {
//     page_info.end_cursor = Some(last_edge.cursor.clone());
// }
//
// // --- Total Count ---
// // Query for total count without pagination cursors but with relation filters
// let mut count_select_stmt = SelectStatement {
//     what: Fields(vec![sql::Field::Single {
//         expr: SqlValue::Function(sql::Function::new_json("count", vec![])),
//         alias: Some(Ident::from("total")),
//     }], false),
//     expr: vec![SqlValue::Table(Table::from(query_source.clone()))].into(),
//     cond: None, // Base condition for the set
//     group: Some(vec![SqlValue::Idiom(Idiom::from("all"))].into()),
//     ..Default::default()
// };
// if let Some(parent_rid) = &parent_rid_opt { // Re-apply relation filter for total count
//     let relation_filter_cond = Cond(Expression::Binary {
//         l: Box::new(SqlValue::Idiom(Idiom(vec![Part::Field("in".into())]))),
//         o: sql::Operator::Equal,
//         r: Box::new(SqlValue::Thing(parent_rid.clone())),
//     });
//     count_select_stmt.cond = Some(relation_filter_cond);
// }
//
// let total_count_val: i64 = match gtx.process_stmt(Statement::Select(count_select_stmt)).await {
//     Ok(SqlValue::Array(mut arr)) if !arr.0.is_empty() => {
//         if let Some(SqlValue::Object(obj)) = arr.0.pop() { // array is [{total: N}]
//             obj.get("total")
//                 .and_then(|v| v.try_as_int()) // Use helper from GqlValueUtils or similar
//                 .unwrap_or(0)
//         } else { 0 }
//     }
//     _ => {
//         trace!("Failed to get total count or unexpected result.");
//         0
//     }
// };

// let val = gtx.get_record_field(rid.clone(), fd_name.as_str()).await?;
//
// let out = match val {
//     SqlValue::Thing(rid) if fd_name != "id" => {
//         let mut tmp = field_val_erase_owned((gtx.clone(), rid.clone()));
//         match field_kind {
//             Some(Kind::Record(ts)) if ts.len() != 1 => {
//                 tmp = tmp.with_type(rid.tb.clone())
//             }
//             _ => {}
//         }
//         Ok(Some(tmp))
//     }
//     SqlValue::None | SqlValue::Null => Ok(None),
//     //TODO: Dig here to fix: internal: invalid item for enum \"StatusEnum\"
//     v => {
//         match field_kind {
//             Some(Kind::Either(ks)) if ks.len() != 1 => {}
//             _ => {}
//         }
//         let out = sql_value_to_gql_value(v.to_owned())
//             .map_err(|_| "SQL to GQL translation failed")?;
//         Ok(Some(FieldValue::value(out)))
//     }
// };
// out

// // --- Try Case 1: Parent is the top-level ErasedRecord ---
// if let Some((gtx, rid)) = ctx.parent_value.downcast_ref::<ErasedRecord>() {
//
//     // Fetch the field value directly from the database usiag the record ID
//     let val: SqlValue = gtx
//         .get_record_field(rid.clone(), fd_name.as_str())II
//         .await?; // Handle SurrealError appropriately
//
//
//     // Process the fetched value
//     match val {
//         SqlValue::Thing(nested_rid) if fd_name != "id" => {
//             trace!("Field is a Thing (Record Link) for field {} with nested ID \
//             {}", fd_name, nested_rid);
//             // Wrap the linked record's context for further resolution
//             let erased_nested = (gtx.clone(), nested_rid.clone()); // Clone GQLTx if needed
//             let mut field_value = field_val_erase_owned(erased_nested);
//
//             // Add type hint if it's a union/interface based on Kind::Record
//             match field_kind {
//                 Some(Kind::Record(ref ts)) if ts.len() != 1 => {
//                     field_value = field_value.with_type(nested_rid.tb.clone());
//                 }
//                 Some(Kind::Either(ref _ks)) if matches!(field_kind, Some(Kind::Record(_))) => {
//                     // Handle potential unions defined via Kind::Either containing Kind::Record types?
//                     // This logic might need refinement based on how unions are defined.
//                     // For now, assume Kind::Record handles the primary case.
//                     field_value = field_value.with_type(nested_rid.tb.clone());
//                 }
//                 _ => {}
//             }
//             Ok(Some(field_value))
//         }
//         SqlValue::None | SqlValue::Null => Ok(None),
//         v => {
//             // Convert any other SQL value (Scalar, Object, Array) to GraphQL Value
//             // This includes converting nested SqlValue::Object to GqlValue::Object
//             trace!("Converting fetched scalar/object/array to GQL Value {} for \
//             field {}", v, fd_name);
//             let gql_val = sql_value_to_gql_value(v) // sql_value_to_gql_value MUST handle Objects/Arrays
//                 .map_err(|e| GqlError::ResolverError(format!("SQL to GQL translation failed for field '{}': {}", fd_name, e)))?;
//             // GqlError::new(format!("SQL to GQL conversion error for field '{}': {}", fd_name, e))
//             trace!("Conversion successful! Returning GQL Value {} for field {}", gql_val, fd_name);
//             Ok(Some(FieldValue::value(gql_val))) // Return the converted GQL Value
//         }
//     }
// }
// // --- Try Case 2: Parent is already a resolved GQL Value (for nested fields) ---
// else if let Some(parent_gql_value) = ctx.parent_value.downcast_ref::<GqlValue>() {
//     trace!("Parent is GqlValue (nested) {:?} for field {}", parent_gql_value, fd_name);
//     match parent_gql_value {
//         GqlValue::Object(parent_map) => {
//             // Find the field within the parent GQL object's map
//             // Use the simple field name (e.g., "height") which was passed to this resolver instance
//             let gql_field_name = Name::new(&fd_name); // Use async_graphql::Name for lookup
//
//             if let Some(nested_gql_value) = parent_map.get(&gql_field_name) {
//                 trace!("Found field {} in parent GQL object for field {:?}",
//                     nested_gql_value,
//                     fd_name);
//                 // The value is already a GQL value, just clone and return it
//                 Ok(Some(FieldValue::value(nested_gql_value.clone())))
//             } else {
//                 // Field not found in the parent GQL object map
//                 trace!("Field {} not found in parent GQL \
//                 object", fd_name);
//                 // Return null if the field doesn't exist in the parent map
//                 // (GraphQL handles nullability based on schema type)
//                 Ok(None)
//             }
//         }
//         // Handle if parent is List? Might not occur if lists always resolve fully.
//         // GqlValue::List(_) => { ... }
//         _ => {
//             // Parent was a GqlValue, but not an Object. This indicates a schema mismatch or unexpected state.
//             Err(internal_error(format!(
//                 "Parent value for nested field '{}' was an unexpected GqlValue type: {:?}. Expected Object.",
//                 fd_name, parent_gql_value
//             )).into()) // Ensure error implements Into<async_graphql::Error>
//         }
//     }
// }
// // --- Error Case: Unknown parent type ---
// else {
//     Err(internal_error(format!(
//         "Failed to downcast parent value for field '{}'. Unexpected parent type ID: {:?}",
//         fd_name,
//         ctx.parent_value
//     )).into()) // Ensure error implements Into<async_graphql::Error>
// }


// fn make_nested_field_resolver(
//     base_db_name: String, // e.g., "size"
//     sub_db_name: String,  // e.g., "width"
//     kind: Kind,           // SQL Kind of the sub-field
// ) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
//     let full_db_path = format!("{}.{}", base_db_name, sub_db_name); // e.g. "size.width"
//     move |ctx: ResolverContext| {
//         let path = full_db_path.clone();
//         let field_kind = kind.clone(); // Kind of the sub-field itself
//         FieldFuture::new(async move {
//             // Parent value should be the ErasedRecord of the CONTAINER object (e.g., Image)
//             let (ref gtx, ref parent_rid) = ctx
//                 .parent_value
//                 .downcast_ref::<ErasedRecord>()
//                 .ok_or_else(|| internal_error(format!("failed to downcast parent for nested field {}", path)))?;
//
//             // Fetch using the full path
//             let val = gtx.get_record_field(parent_rid.clone(), &path).await?;
//
//             // Use the SAME conversion logic as make_table_field_resolver's final match arm
//             // (including the Enum fix)
//             let out = match val {
//                 SqlValue::None | SqlValue::Null => Ok(None),
//                 // Nested fields shouldn't typically be Things unless it's object<record<...>>
//                 SqlValue::Thing(_) => Err(internal_error(format!("Unexpected Thing found for nested field {}", path))),
//                 v => { // Handle scalars and potential Enums
//                     // Use the kind of the *sub-field* here
//                     let is_dynamic_enum = match &field_kind {
//                         Some(Kind::Option(inner)) => matches!(**inner, Kind::Either(ref ks) if ks.iter().all(|k| matches!(k, Kind::Literal(Literal::String(_))))),
//                         Some(Kind::Either(ref ks)) => ks.iter().all(|k| matches!(k, Kind::Literal(Literal::String(_)))),
//                         _ => false,
//                     };
//
//                     if is_dynamic_enum {
//                         // match v.to_string_lossy() { // Adjust based on SqlValue string access
//                         //     Some(db_string) => {
//                         //         let gql_enum_value_str = db_string.to_screaming_snake_case();
//                         //         let gql_enum_value = GqlValue::Enum(Name::new(gql_enum_value_str));
//                         //         Ok(Some(FieldValue::value(gql_enum_value)))
//                         //     }
//                         //     None => Ok(None), // Or error
//                         // }
//                         match v {
//                             SqlValue::Strand(s) => {
//                                 let db_string = s.as_str();
//                                 let gql_enum_value_str = db_string.to_screaming_snake_case();
//
//                                 // Use Name::new (panics on invalid GraphQL name chars, unlikely here)
//                                 // Or implement a validation check if needed before calling new()
//                                 let gql_enum_value = GqlValue::Enum(Name::new(gql_enum_value_str)); // FIX: Use Name::new
//
//                                 Ok(Some(FieldValue::value(gql_enum_value)))
//                             }
//                             // Add other SqlValue variants if they can represent your enum strings
//                             _ => {
//                                 // FIX: Use the correct variable name for the error message
//                                 error!("Expected a Strand from DB for dynamic enum field '{}', but got different value: {:?}", db_name, v); // Use db_name (from resolver args) or path (from nested resolver args)
//                                 Ok(None)
//                             }
//                         }
//                     } else {
//                         // Generic conversion
//                         let gql_value = sql_value_to_gql_value(v) // Pass owned v if needed
//                             .map_err(|e| format!("SQL to GQL translation failed for nested field '{}': {}", path, e))?;
//                         Ok(Some(FieldValue::value(gql_value)))
//                     }
//                 }
//             };
//             out
//         })
//     }
// }
//
// fn make_table_field_resolver(
//     db_name: String, // DB name (e.g., "created_at", "size")
//     kind: Option<Kind>,
// ) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
//     move |ctx: ResolverContext| {
//         let fd_name = db_name.clone(); // Use db_name passed in
//         let field_kind = kind.clone();
//         FieldFuture::new({
//             async move {
//                 let (ref gtx, ref rid) = ctx
//                     .parent_value
//                     .downcast_ref::<ErasedRecord>()
//                     .ok_or_else(|| internal_error(format!("failed to downcast parent for field {}", fd_name)))?;
//
//                 // If this field represents the BASE of a nested object (e.g. "size"),
//                 // we don't fetch its value directly. Instead, we just pass the parent's
//                 // ErasedRecord down, so the nested field resolvers can use it.
//                 if matches!(field_kind, Some(Kind::Object)) {
//                     // Check if it actually has nested structure defined via dot notation
//                     // (This check might need refinement based on how Kind::Object is populated)
//                     // For now, assume if kind is Object, we pass parent context down.
//                     let parent_erased_record = (gtx.clone(), rid.clone());
//                     // Wrap it so async-graphql knows how to handle it as the parent for sub-fields
//                     return Ok(Some(field_val_erase_owned(parent_erased_record)));
//                 }
//
//                 // Otherwise, fetch the field value as before
//                 let val = gtx.get_record_field(rid.clone(), fd_name.as_str()).await?;
//
//                 let out = match val {
//                     SqlValue::Thing(related_rid) if fd_name != "id" => { // Handle relations
//                         let mut tmp = field_val_erase_owned((gtx.clone(), related_rid.clone()));
//                         match &field_kind {
//                             Some(Kind::Record(ts)) if ts.len() != 1 => {
//                                 tmp = tmp.with_type(related_rid.tb.clone())
//                             }
//                             _ => {}
//                         }
//                         Ok(Some(tmp))
//                     }
//                     SqlValue::None | SqlValue::Null => Ok(None),
//                     v => { // Handle scalars and Enums
//                         let is_dynamic_enum = match &field_kind {
//                             Some(Kind::Option(inner)) => matches!(**inner, Kind::Either(ref ks) if ks.iter().all(|k| matches!(k, Kind::Literal(Literal::String(_))))),
//                             Some(Kind::Either(ref ks)) => ks.iter().all(|k| matches!(k, Kind::Literal(Literal::String(_)))),
//                             _ => false,
//                         };
//
//                         if is_dynamic_enum {
//                             match v.to_string_lossy() { // Adjust string access as needed
//                                 Some(db_string) => {
//                                     let gql_enum_value_str = db_string.to_screaming_snake_case();
//                                     let gql_enum_value = GqlValue::Enum(Name::new(gql_enum_value_str));
//                                     Ok(Some(FieldValue::value(gql_enum_value)))
//                                 }
//                                 None => Ok(None), // Or error
//                             }
//                         } else {
//                             // Generic conversion
//                             let gql_value = sql_value_to_gql_value(v) // Pass owned v if needed
//                                 .map_err(|e| format!("SQL to GQL translation failed for field '{}': {}", fd_name, e))?;
//                             Ok(Some(FieldValue::value(gql_value)))
//                         }
//                     }
//                 };
//                 out
//             }
//         })
//     }
// }