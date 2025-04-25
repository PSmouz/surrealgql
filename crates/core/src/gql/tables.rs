use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::mem;
use std::ops::Add;
use std::sync::{Arc, LazyLock};

use super::error::{resolver_error, schema_error, GqlError};
use super::ext::IntoExt;
use super::schema::{gql_to_sql_kind, sql_value_to_gql_value};
use crate::dbs::Session;
use crate::fnc::time::format;
use crate::gql::error::internal_error;
use crate::gql::ext::TryAsExt;
use crate::gql::schema::{kind_to_type, unwrap_type};
use crate::gql::utils::{field_val_erase_owned, ErasedRecord, GQLTx, GqlValueUtils};
use crate::kvs::{Datastore, Transaction};
use crate::sql::order::{OrderList, Ordering};
use crate::sql::statements::{DefineFieldStatement, DefineTableStatement, SelectStatement};
use crate::sql::{self, Ident, Literal, Part, Table, TableType};
use crate::sql::{Cond, Fields};
use crate::sql::{Expression, Value as SqlValue};
use crate::sql::{Idiom, Kind};
use crate::sql::{Statement, Thing};
use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::FieldFuture;
use async_graphql::dynamic::InputValue;
use async_graphql::dynamic::TypeRef;
use async_graphql::dynamic::{Enum, FieldValue, Type};
use async_graphql::dynamic::{Field, ResolverContext};
use async_graphql::dynamic::{InputObject, Object};
use async_graphql::types::connection::{Connection, Edge, PageInfo};
use async_graphql::Name;
use async_graphql::Value as GqlValue;
use inflector::Inflector;
use log::trace;

macro_rules! order {
	(asc, $field:expr) => {{
		let mut tmp = sql::Order::default();
		tmp.value = $field.into();
		tmp.direction = true;
		tmp
	}};
	(desc, $field:expr) => {{
		let mut tmp = sql::Order::default();
		tmp.value = $field.into();
		tmp
	}};
}

macro_rules! limit_input {
	() => {
		InputValue::new("limit", TypeRef::named(TypeRef::INT))
	};
}

macro_rules! start_input {
	() => {
		InputValue::new("start", TypeRef::named(TypeRef::INT))
	};
}

macro_rules! id_input {
	() => {
		InputValue::new("id", TypeRef::named_nn(TypeRef::ID))
	};
}

fn filter_name_from_table(tb_name: impl Display) -> String {
    // format!("Filter{}", tb_name.to_string().to_sentence_case())
    format!("{}FilterInput", tb_name.to_string().to_pascal_case())
}


// fn path_from_parts()
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
    session: &Session,
    datastore: &Arc<Datastore>,
    cursor: bool,
) -> Result<Object, GqlError> {
    // Type::Any is not supported. FIXME: throw error in the future.
    let (tables, relations): (Vec<&DefineTableStatement>, Vec<&DefineTableStatement>) = tbs.iter().partition(|tb| {
        match tb.kind {
            TableType::Normal => true,
            TableType::Relation(_) => false,
            TableType::Any => false,
        }
    });

    // types.push(PageInfo);

    for tb in tables.iter() {
        let tb_name = tb.name.to_string();
        let first_tb_name = tb_name.clone();
        let second_tb_name = tb_name.clone();
        let tb_name_gql = tb_name.to_pascal_case();
        let tb_name_query = tb_name.to_camel_case();

        let mut gql_objects: BTreeMap<String, Object> = BTreeMap::new();

        let fds = tx.all_tb_fields(ns, db, &tb.name.0, None).await?;
        trace!("fields '{:?}'", fds);

        let mut tb_ty_obj = Object::new(tb_name_gql.clone())
            .field(Field::new(
                "id",
                TypeRef::named_nn(TypeRef::ID),
                make_table_field_resolver(
                    "id",
                    Some(Kind::Record(vec![Table::from(tb_name)])),
                ),
            ))
            .implement("Record");

        // =======================================================
        // Parse Fields
        // =======================================================
        for fd in fds.iter() {
            // We have already defined "id", so we don't take any new definition for it.
            if fd.name.is_id() { continue; };

            let kind = match fd.kind.clone() {
                Some(k) => k,
                None => continue
            };
            let kind_non_optional = kind.non_optional().clone();
            let path = fd.name.clone(); // maybe inline

            let parts: Vec<&Ident> = path.0.iter().filter_map(|part| match part {
                Part::Field(ident) => Some(ident),
                _ => None
            }).collect();

            // Should always contain at least the field name
            if parts.is_empty() { continue; }

            let fd_name = parts.as_slice().last().unwrap().to_string();
            let fd_name_gql = fd_name.to_camel_case();

            let fd_path = fd.name.to_path()
                .replace("/", ".")
                .strip_prefix(".")
                .unwrap()
                .to_string();
            let fd_path_parent = remove_last_segment(&*fd_path.as_str());

            let fd_ty = kind_to_type(kind.clone(), types, parts.as_slice(), tb_name_gql.clone())?;

            trace!("field {:?}", fd);
            trace!("idiom path: {:?}", path);
            trace!("field_name {:?}", fd_name);
            trace!("field_name_gql {:?}", fd_name_gql);
            trace!("kind {:?}", kind);
            trace!("field_type {:?}", fd_ty);
            trace!("field_path {:?}", fd_path);
            trace!("fd_path_parent {:?}", fd_path_parent);

            // object map used to build fields step by step
            if kind_non_optional == Kind::Object {
                gql_objects.insert(
                    fd_path.clone(),
                    Object::new(fd_ty.type_name())
                        .description(if let Some(ref c) = fd.comment {
                            format!("{c}")
                        } else {
                            "".to_string()
                        }),
                );
            }

            if fd_path_parent.is_empty() {
                tb_ty_obj = tb_ty_obj
                    .field(Field::new(
                        fd_name_gql,
                        fd_ty,
                        make_table_field_resolver(fd_path.as_str(), fd.kind.clone()),
                    ))
                    .description(if let Some(ref c) = fd.comment {
                        format!("{c}")
                    } else {
                        "".to_string()
                    });
            } else {
                // Array inner type is scalar, thus already set when adding the list field
                if fd_path.chars().last() == Some('*') { continue; }

                match gql_objects.remove(&fd_path_parent) {
                    Some(obj) => {
                        gql_objects.insert(fd_path_parent.clone(), Object::from(obj)
                            .field(Field::new(
                                fd_name_gql,
                                fd_ty,
                                make_table_field_resolver(fd_path.as_str(), fd.kind.clone()),
                            ))
                            .description(if let Some(ref c) = fd.comment {
                                format!("{c}")
                            } else {
                                "".to_string()
                            }),
                        );
                    }
                    None => return Err(schema_error("Nested field should have parent object.")),
                }
            }
        }

        // =======================================================
        // Add single instance query
        // =======================================================

        let sess1 = session.to_owned();
        let kvs1 = datastore.clone();
        let fds1 = fds.clone();

        query = query.field(
            Field::new(
                tb_name_query.to_singular(),
                TypeRef::named(&tb_name_gql),
                move |ctx| {
                    let tb_name = first_tb_name.clone();
                    let kvs1 = kvs1.clone();
                    FieldFuture::new({
                        let sess1 = sess1.clone();
                        async move {
                            let gtx = GQLTx::new(&kvs1, &sess1).await?;

                            let args = ctx.args.as_index_map();
                            let id = match args.get("id").and_then(GqlValueUtils::as_string) {
                                Some(i) => i,
                                None => {
                                    return Err(internal_error(
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
                                    let erased: ErasedRecord = (gtx, t);
                                    Ok(Some(field_val_erase_owned(erased)))
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
                    format!("Generated from table `{}`\nallows querying a single record in a table by ID", tb.name)
                })
                .argument(id_input!()),
        );

        // =======================================================
        // Add filters
        // =======================================================

        // =======================================================
        // Add all instances query
        // =======================================================

        let sess2 = session.to_owned();
        let kvs2 = datastore.clone();
        let fds2 = fds.clone();

        query = query.field(
            Field::new(
                tb_name_query.to_plural(),
                TypeRef::named_nn_list_nn(&tb_name_gql),
                move |ctx| {
                    let tb_name = second_tb_name.clone();
                    let sess2 = sess2.clone();
                    let fds2 = fds.clone();
                    let kvs2 = kvs2.clone();
                    FieldFuture::new(async move {
                        let gtx = GQLTx::new(&kvs2, &sess2).await?;

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
                                    let erased: ErasedRecord = (gtx.clone(), t);
                                    field_val_erase_owned(erased)
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
                .description(if let Some(ref c) = &tb.comment { format!("{c}") } else { format!("Generated from table `{}`\nallows querying a table with filters", tb.name) })
            // .argument(limit_input!())
            // .argument(start_input!())
            // .argument(InputValue::new("order", TypeRef::named(&table_order_name)))
            // .argument(InputValue::new("filter", TypeRef::named(&table_filter_name))),
        );

        // =======================================================
        // Add types
        // =======================================================
        // for loop because Type::Object needs owned obj, not a reference
        for (_, obj) in gql_objects {
            types.push(Type::Object(obj));
        }
        types.push(Type::Object(tb_ty_obj));
    }

    // =======================================================
    // Pass 2: Define Inputs, Enums, Query Fields
    // =======================================================
    // trace!("Starting Pass 2: Defining Inputs, Enums, Queries");
    // let mut generated_filter_inputs: BTreeMap<String, Type> = BTreeMap::new(); // Avoid duplicate filter defs
    // let mut generated_order_inputs: BTreeMap<String, Type> = BTreeMap::new();
    // let mut generated_orderable_enums: BTreeMap<String, Type> = BTreeMap::new();
    //
    // // Define base scalar filters if not already present (do this once)
    // if !types.iter().any(|t| matches!(t, Type::InputObject(io) if io.type_name() == "IDFilterInput")) {
    //     types.push(Type::InputObject(filter_id())); // Assuming filter_id() defines IDFilterInput
    // }
    // // Add other base filters like StringFilterInput, IntFilterInput etc. Ensure they are defined once.
    // // Example (needs proper implementation in filter_from_type or similar):
    // if !types.iter().any(|t| matches!(t, Type::InputObject(io) if io.type_name() == "StringFilterInput")) {
    //     let string_filter = filter_from_type(Kind::String, "StringFilterInput".to_string(), types)?;
    //     generated_filter_inputs.insert("StringFilterInput".to_string(), Type::InputObject(string_filter));
    // }
    // if !types.iter().any(|t| matches!(t, Type::InputObject(io) if io.type_name() == "IntFilterInput")) {
    //     let int_filter = filter_from_type(Kind::Int, "IntFilterInput".to_string(), types)?;
    //     generated_filter_inputs.insert("IntFilterInput".to_string(), Type::InputObject(int_filter));
    // }
    // if !types.iter().any(|t| matches!(t, Type::InputObject(io) if io.type_name() == "DateTimeFilterInput")) {
    //     let dt_filter = filter_from_type(Kind::Datetime, "DateTimeFilterInput".to_string(), types)?;
    //     generated_filter_inputs.insert("DateTimeFilterInput".to_string(), Type::InputObject(dt_filter));
    // }
    // // Add BooleanFilterInput, FloatFilterInput etc.
    //


    //
    //     // Single query field (e.g., image)
    //     let single_query_name = table_name_str.to_camel_case();
    //     trace!("Pass 2: Defining single query '{}'", single_query_name);
    //     let sess2 = session.to_owned();
    //     let kvs2 = datastore.clone();
    //     let table_name_clone2 = table_name_str.clone();
    //     query = query.field(
    //         Field::new(
    //             single_query_name,
    //             TypeRef::named(&table_pascal), // Correct type ref
    //             move |ctx| { // Resolver logic remains largely the same
    //                 let tb_name = table_name_clone2.clone();
    //                 let kvs2 = kvs2.clone();
    //                 let sess2 = sess2.clone();
    //                 FieldFuture::new(async move {
    //                     // ... (fetch by ID as before) ...
    //                     let gtx = GQLTx::new(&kvs2, &sess2).await?;
    //                     let args = ctx.args.as_index_map();
    //                     let id_val = args.get("id").ok_or_else(|| internal_error("Missing ID argument"))?;
    //                     let id = id_val.as_string().ok_or_else(|| internal_error("ID must be a string"))?;
    //
    //                     let thing = match Thing::try_from(id.clone()) {
    //                         Ok(t) => t,
    //                         Err(_) => Thing::from((tb_name, id)),
    //                     };
    //                     match gtx.get_record_field(thing, "id").await? {
    //                         SqlValue::Thing(t) => Ok(Some(field_val_erase_owned((gtx, t)))),
    //                         _ => Ok(None),
    //                     }
    //                 })
    //             },
    //         )
    //             .argument(id_input!()), //     );
    // } // End Pass 2 loop

    // types.extend(generated_filter_inputs.into_values());
    // types.extend(generated_order_inputs.into_values());
    // types.extend(generated_orderable_enums.into_values());

    Ok(query)
}

fn make_table_field_resolver(
    // fd_name: impl Into<String>,
    fd_path: impl Into<String>,
    kind: Option<Kind>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
    let fd_path = fd_path.into();
    move |ctx: ResolverContext| {
        let fd_path = fd_path.clone();
        let field_kind = kind.clone();

        FieldFuture::new({
            async move {

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

                trace!(
                    "Creating/Running resolver for DB path '{}' (Kind: {:?}) with parent: {:?}",
                    fd_path,
                    field_kind,
                    ctx.parent_value // Use the user-provided trace format
                );

                // trace!("parent_value: {:?}", ctx.parent_value);
                let (ref gtx, ref rid) = ctx
                    .parent_value
                    .downcast_ref::<ErasedRecord>()
                    .ok_or_else(|| internal_error("failed to downcast"))?;

                trace!("Parent is ErasedRecord for path '{}', RID: {}", fd_path, rid);

                match field_kind {
                    // A) Field is Object or Record link (not 'id'): Pass ErasedRecord context down
                    Some(Kind::Object) | Some(Kind::Record(_)) if fd_path != "id" => {
                        trace!("Field at path '{}' is Object/Record, passing down ErasedRecord", fd_path);
                        let gtx_clone = gtx.clone();
                        let rid_clone = rid.clone();
                        let nested_context: ErasedRecord = (gtx_clone, rid_clone);
                        let field_value = field_val_erase_owned(nested_context);
                        // Optional: Add .with_type() hints for record link unions/interfaces here if needed
                        Ok(Some(field_value))
                    }

                    // B) Field is scalar/terminal/'id': Fetch value using (modified) get_record_field
                    _ => {
                        trace!("Field at path '{}' is scalar/id/terminal, fetching value via get_record_field", fd_path);

                        // Call the modified get_record_field which accepts a path string
                        let sql_value: SqlValue = gtx
                            .get_record_field(rid.clone(), &fd_path)
                            .await?;

                        trace!("Fetched value for path '{}': {:?}", fd_path, sql_value);

                        // --- Convert the fetched value ---
                        match sql_value {
                            SqlValue::None | SqlValue::Null => Ok(None),

                            // This case primarily handles 'id' now or potentially Things returned
                            // unexpectedly for scalar paths.
                            SqlValue::Thing(thing_val) => {
                                trace!("Value for path '{}' is Thing: {}", fd_path, thing_val);
                                let gql_val = sql_value_to_gql_value(SqlValue::Thing(thing_val))?;
                                Ok(Some(FieldValue::value(gql_val)))
                            }

                            // Handle scalars and Enums
                            v => {
                                trace!("Converting value {:?} for path '{}'", v, fd_path);
                                let is_dynamic_enum = match &field_kind { // Use the kind passed to factory
                                    Some(Kind::Option(inner)) => matches!(**inner, Kind::Either(ref ks) if ks.iter().all(|k| matches!(k, Kind::Literal(Literal::String(_))))),
                                    Some(Kind::Either(ref ks)) => ks.iter().all(|k| matches!(k, Kind::Literal(Literal::String(_)))),
                                    _ => false,
                                };

                                let gql_val = if is_dynamic_enum {
                                    match v {
                                        SqlValue::Strand(db_string) => {
                                            let gql_enum_member = db_string.as_str().to_screaming_snake_case();
                                            trace!("Dynamic Enum conversion: DB '{}' -> GQL '{}' for path {}", db_string.as_str(), gql_enum_member, fd_path);
                                            GqlValue::Enum(Name::new(gql_enum_member))
                                        }
                                        _ => return Err(internal_error(format!("Expected String/Strand from DB for dynamic enum at path '{}', got {:?}", fd_path, v)).into())
                                    }
                                } else {
                                    sql_value_to_gql_value(v)
                                        .map_err(|e| GqlError::ResolverError(format!("SQL to GQL translation failed for path '{}': {}", fd_path, e)))?
                                };

                                trace!("Conversion successful for path '{}': {:?}", fd_path, gql_val);
                                Ok(Some(FieldValue::value(gql_val)))
                            }
                        }
                    }
                }

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
            }
        })
    }
}

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

macro_rules! filter_impl {
	($filter:ident, $ty:ident, $name:expr) => {
		$filter = $filter.field(InputValue::new(format!("{}", $name), $ty.clone()));
	};
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
        let Some(GqlValue::Enum(field_name_enum)) = current.get("field") else {
            return Err(resolver_error("Order input must contain 'field' enum"));
        };
        let Some(GqlValue::Enum(direction_enum)) = current.get("direction") else {
            return Err(resolver_error("Order input must contain 'direction' enum (ASC/DESC)"));
        };

        let field_name_screaming = field_name_enum.as_str(); // e.g., "CREATED_AT", "SIZE_WIDTH"
        // Convert SCREAMING_SNAKE_CASE back to DB snake_case.dot notation
        let db_field_name = field_name_screaming.to_lowercase(); // Simple conversion, might need underscores replaced with dots

        let direction_is_asc = direction_enum.as_str() == "ASC";

        let mut order_clause = sql::Order::default();
        order_clause.value = db_field_name.into(); // Use DB name/path
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
