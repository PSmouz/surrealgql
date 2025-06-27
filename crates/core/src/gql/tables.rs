use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::sync::Arc;

use super::error::{input_error, resolver_error, schema_error, GqlError};
use super::ext::IntoExt;
use super::schema::{gql_to_sql_kind, sql_value_to_gql_value};
use crate::gql::cursor;
use crate::gql::cursor::{apply_cursors_to_edges, make_list_resolver, make_object_resolver, make_value_resolver, ConnectionContext, ConnectionKind, EdgeContext, PageInfo};
use crate::gql::error::internal_error;
use crate::gql::ext::TryAsExt;
use crate::gql::schema::{kind_to_type, unwrap_type};
use crate::gql::utils::{pluralize, GQLTx, GqlValueUtils};
use crate::kvs::Transaction;
use crate::sql::order::{OrderList, Ordering};
use crate::sql::statements::{DefineFieldStatement, DefineTableStatement, SelectStatement};
use crate::sql::{self, Array, Ident, Operator, Order, Part, Table, TableType, Values};
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
use inflector::Inflector;
use log::trace;


fn dummy_resolver() -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
    move |_ctx: ResolverContext| {
        FieldFuture::new(async move {
            Ok(Some(FieldValue::value("NOT YET IMPLEMENTED!".to_string())))
        })
    }
}

/// Generates a description string for a field definition.
///
/// This macro checks if the field definition has a comment and returns it as a formatted string.
/// If the comment is not present, it returns an empty string or a provided default description.
///
/// # Parameters
/// - `$fd`: The field definition to check for a comment.
/// - `$desc`: (optional) A default description to return if the field definition does not have a comment.
/// # Returns
/// - A formatted string containing the comment or the default description.
macro_rules! description {
    ($fd:ident) => {
        if let Some(ref c) = $fd.comment {
            format!("{c}")
        } else {
            "".to_string()
        }
    };
    ($fd:ident, $desc:expr) => {
        if let Some(ref c) = $fd.comment {
            format!("{c}")
        } else {
            $desc.to_string()
        }
    };
}

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
        .description("Maximum number of records to return. Use this parameter to limit result size.")
	};
}

macro_rules! start_input {
	() => {
		InputValue::new("start", TypeRef::named(TypeRef::INT))
        .description("Number of records to skip. Use this parameter with 'limit' for offset-based pagination.")
	};
}

macro_rules! id_input {
	() => {
		InputValue::new("id", TypeRef::named(TypeRef::ID))
        .description("The ID of the record to fetch. Can be a string ID or a record ID in the format 'table:id'.")
	};
}

macro_rules! input_input {
	(
        $ty: expr,
        $name: expr
    ) => {
		InputValue::new("input",
            TypeRef::named_nn(format!("{}{}Input", $ty.to_pascal_case(), $name.to_pascal_case())))
        .description("")
	};
    (
        $name: expr
    ) => {
		InputValue::new("input",
            TypeRef::named_nn($name))
        .description("")
	};
}

/// Generates an input value for ordering options based on the provided name.
///
/// This macro creates an `InputValue` for the `orderBy` argument,
/// which is used to specify ordering options for connections.
///
/// **Important**: This macro needs the order input types to be defined
/// with [`define_order_input_types!`].
///
/// # Parameters
/// - `$name`: The name of the entity for which ordering options are defined.
/// # Returns
/// - An `InputValue` for the `orderBy` argument, which is used in GraphQL queries to specify.
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

/// Defines the order input types for a given base name and fields.
/// This macro generates an enum for the order fields and an input object for ordering options.
///
/// **Important**: This macro requires the order direction enum type defined.
///
/// # Parameters
/// - `$types`: The types vector to which the order input types are added.
/// - `$base_name`: The base name for the order fields and input object.
/// - `$fields`: A vector of field names that can be used for ordering.
/// # Returns
/// - Adds an enum and an input object to the `$types` vector.
macro_rules! define_order_input_types {
    (
        $types:ident,
        $base_name:expr,
        $fields:expr
    ) => {
        let base_name_pascal = $base_name.to_pascal_case();
        let enum_name = format!("{}OrderField", base_name_pascal);
        let obj_name = format!("{}Order", base_name_pascal);

        let mut order_by_enum = Enum::new(&enum_name)
            .item(EnumItem::new("ID").description(format!("{} by ID.", $base_name)))
            .description(format!("Properties by which {} can be ordered.", $base_name));

        for field in $fields {
            order_by_enum = order_by_enum.item(
                EnumItem::new(field.to_screaming_snake_case())
                .description(format!("{} by {}.", $base_name, field.to_screaming_snake_case()))
            );
        }

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
/// - `types`: The types vector to which the connection and edge types are added.
/// - `fd_name`: The name of the connection field.
/// - `node_ty_name`: The name of the node type.
/// - `connection_resolver`: The resolver function for the connection field.
/// - `edge_fields`: Additional edge fields to include in the connection. Should be a vector of
/// `Field` objects.
/// - `args`: Additional arguments to add to the connection field. Should be a vector of
/// `InputValue` objects.
/// - `is_relation`: A boolean indicating whether the connection is for a relation or not.
/// # Returns
macro_rules! cursor_pagination {
    (
        $types:ident,
        $fd_name:expr,
        $node_ty_name:expr,
        $connection_resolver:expr,
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

/// This macro defines a CRUD operation for a singular field in a table.
/// It generates the necessary input and payload objects, as well as the field definition
/// for the operation.
/// # Parameters
/// - `$types`: The types vector to which the input and payload objects are added.
/// - `$op`: The operation type, such as "update", "add", or "remove".
/// - `$fd_name`: The name of the field.
/// - `$tb_name`: The name of the table.
/// - `$ty`: The type of the field.
macro_rules! define_singular_field_crud {
    (
        $types:ident,
        $op:literal,
        $fd_name:ident,
        $tb_name:ident,
        $ty:expr
    ) => {
        {
            let name = format!("{}{}", $tb_name.to_pascal_case(), $fd_name.to_pascal_case());
            let input_obj = define_obj!(input, $op, $fd_name, $tb_name, $ty);
            let payload_obj = define_obj!(payload, $op, $fd_name, $tb_name, $ty);

            let fd = Field::new(
                format!("{}{}", $op, &name),
                TypeRef::named(payload_obj.type_name()),
                dummy_resolver()
            )
            .description(format!("{}s the `{}` field of a `{}` table record.", $op.to_pascal_case(),
                $fd_name, $tb_name))
            .argument(input_input!($op.to_pascal_case(), &name));

            $types.push(Type::InputObject(input_obj));
            $types.push(Type::Object(payload_obj));

            fd
        }
    };
}

/// This macro defines an object or input object for a CRUD operation.
/// It generates the necessary fields and types based on the operation type and table name.
/// # Parameters
/// - `payload/input/query`: Defines whether the object is a payload, input object, or query object.
/// - `$op`: The operation type, such as "create", "update", or "delete".
/// - `$tb_name`: The name of the table.
/// - `$fd_name`: The name of the field (optional, used for singular field operations).
/// - `$ty`: The type of the field (optional, used for singular field operations).
/// # Returns
/// - An `Object` or `InputObject` type with the specified fields and types.
macro_rules! define_obj {

    (
        payload,
        $op:literal,
        $tb_name:expr
    ) => {
         Object::new(format!("{}{}Payload", $op.to_pascal_case(), $tb_name))
            .field(Field::new(
                "success",
                TypeRef::named(TypeRef::BOOLEAN),
                dummy_resolver(), //TODO: implement resolver
            ).description("Did the operation succeed?"))
            .field(Field::new(
                $tb_name.to_camel_case(),
                TypeRef::named($tb_name),
                dummy_resolver(),
            ).description(format!("The {}d {}.", $op, $tb_name))
            .description(format!("Autogenerated return type of {}{}.", $op.to_pascal_case(),$tb_name)))
    };
    (
        input,
        $op:literal,
        $tb_name:expr
    ) => {
        InputObject::new(format!("{}{}Input", $op.to_pascal_case(), $tb_name))
            .field(InputValue::new(
            "id",
            TypeRef::named(TypeRef::ID), // Align with SurrealDB and create ID if none is provided
            ).description(format!("The `{}` table record ID to {}.", $tb_name, $op)))
            .description(format!("Autogenerated input type of {}{}.", $op.to_pascal_case(),
                $tb_name.to_pascal_case()))
    };
    (
        query,
        $tb_name:expr
    ) => {
        Object::new($tb_name)
            .field(Field::new(
                "id",
                TypeRef::named_nn(TypeRef::ID),
                make_field_resolver("id"),
            ).description(format!("The {} ID.", $tb_name)))
            .description(format!("Autogenerated query type for `{}`.", $tb_name))
            .implement("Record")
    };
    (
        payload,
        $op:literal,
        $fd_name:expr,
        $tb_name:expr,
        $ty:expr
    ) => {
        {
            let name = format!("{}{}", $tb_name.to_pascal_case(), $fd_name.to_pascal_case());

             Object::new(format!("{}{}Payload", $op.to_pascal_case(), &name))
                .field(Field::new(
                    "success",
                    TypeRef::named(TypeRef::BOOLEAN),
                    dummy_resolver(), //TODO: implement resolver
                ).description("Did the operation succeed?"))
                .field(Field::new(
                    $fd_name.to_camel_case(),
                    unwrap_type($ty), // Make type optional in payload
                    dummy_resolver(),
                ).description(format!("The {}d {}.", $op, $fd_name))
                .description(format!("Autogenerated return type of {}{}.", $op.to_pascal_case(), &name)))
        }
    };
    (
        input,
        $op:literal,
        $fd_name:expr,
        $tb_name:expr,
        $ty:expr
    ) => {
        {
            let name = format!("{}{}", $tb_name.to_pascal_case(), $fd_name.to_pascal_case());

            let mut in_obj: InputObject = define_obj!(input, $op, &name);
            in_obj = in_obj.field(InputValue::new(
                $fd_name.to_camel_case(),
                TypeRef::named_nn($ty.type_name()),
            ).description(format!("The `{}` field of the `{}` table to {}.", $fd_name, $tb_name, $op)));

            in_obj
        }
    };
}

/// This macro is used to add a field to an object, typically a mutation or query object.
/// It allows for adding fields with a specific operation type (like "create", "update", or "delete")
/// and an object that implements the `type_name` method.
/// It can also be used to add a field without an operation type.
/// # Parameters
/// - `$root`: The root object to which the field is added (e.g., mutation or query).
/// - `$op`: (optional) The operation type, such as "create", "update", or "delete".
/// - `$obj`: The object to add, which should implement the `type_name` method.
/// - `$tb_name`: (optional) The table name, used for naming conventions and descriptions.
macro_rules! add_to_obj {
    /// This macro is used to add a field to an object, typically a mutation or query object.
    /// # Parameters
    /// - `$root`: The root object to which the field is added (e.g., mutation or query).
    /// - `$op`: The operation type, such as "create", "update", or "delete".
    /// - `$obj`: The object to add, which should implement the `type_name` method.
    /// - `$tb_name`: The table name, used for naming conventions and descriptions.
    (
        $root:ident,
        $op:literal,
        $obj:expr,
        $tb_name:expr
    ) => {
        $root = $root.field(
            Field::new(
                format!("{}{}", $op, $tb_name.to_pascal_case()),
                TypeRef::named($obj.type_name()),
                dummy_resolver(), //TODO: implement resolver
            )
            .description(format!("{}s a record of the `{}` table.", $op.to_pascal_case(), $tb_name))
            .argument(input_input!($op.to_pascal_case(), $tb_name))
        );
    };
    /// This macro is used to add a field to an object, typically a mutation or query object.
    /// # Parameters
    /// - `$root`: The root object to which the field is added (e.g., mutation or query).
    /// - `$obj`: The object to add, which should implement the `type_name` method.
    (
        $root:ident,
        $obj:expr
    ) => {
        $root = $root.field($obj);
    };
}

/// This macro is used to parse a field definition and add it to the object map.
/// It handles different kinds of fields, including nested fields and array fields.
/// It also manages the creation of connection fields for array types.
///
/// # Parameters
/// - `$fd`: The field definition to parse.
/// - `$tb_name`: The name of the table.
/// - `$types`: The types vector to which the field type is added.
/// - `$cursor`: A boolean indicating whether to use cursor pagination.
/// - `$query_vec`: The vector of fields for the query object.
/// - `$nested_objs_map`: The map of nested objects to which the field is added.
/// - `$order_vec`: The vector of orderable fields for the table.
/// - `$create_obj`: The input object for the createTable mutation.
/// - `$update_obj`: The input object for the updateTable mutation.
/// - `$mutation_add_vec`: The vector of addTableFieldName mutations for the table.
/// - `$mutation_update_vec`: The vector of updateTableFieldName mutations for the table.
macro_rules! parse_field {
    (
        $fd:ident,
        $tb_name:ident,
        $types:ident,
        $cursor:ident,
        $query_vec:ident,
        $nested_objs_map:ident,
        $order_vec:ident,
        $create_obj:ident,
        $update_obj:ident,
        $mutation_add_vec:ident,
        $mutation_update_vec:ident
    ) => {
        let kind: Kind = match $fd.kind.clone() {
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

        let fd_ty_input = match kind_non_optional {
            Kind::Record(_) => {
                if kind_non_optional == kind {
                    TypeRef::named_nn(TypeRef::ID)
                } else {
                    TypeRef::named(TypeRef::ID)
                }
            },
            _ => fd_ty.clone(),
        };

        if is_primitive(&kind_non_optional) {
            $order_vec.push(fd_name_gql.clone());
        }

        // object map used to add fields step by step to the objects
        if kind_non_optional == Kind::Object {
            $nested_objs_map.insert(
                fd_path.clone(),
                Object::new(fd_ty.type_name()).description(description!($fd)),
            );
        }

        if fd_path_parent.is_empty() { // top level field
            match kind_non_optional {
                // cursor connections only if specified in config
                Kind::Array(_, _) if $cursor => {
                    let kind = kind.inner_kind().unwrap();
                    let ty_ref = kind_to_type(kind.clone(), $types, path.as_slice())?;
                    let ty_name = ty_ref.type_name();

                    $query_vec.push(cursor_pagination!(
                        $types,
                        &fd_name_gql,
                        ty_name,
                        make_connection_resolver(fd_path.as_str(), ConnectionKind::Field),
                        edge_fields: [],
                        args: [],
                        is_relation: false
                    ));
                }
                _ => {
                    // Cannot use query object here directly, because parse field for relations
                    // depends on the fields being stored in a vector.
                    $query_vec.push(Field::new(
                            &fd_name_gql,
                            fd_ty.clone(),
                            make_field_resolver(fd_path.as_str()),
                        )
                        .description(description!($fd))
                    );
                    // Main create mutation object that has all fields of the table. Those that have
                    // optional kind are optional all others are non null fields. ID is always
                    // non-null in create input.
                    add_to_obj!($create_obj, InputValue::new(
                            &fd_name_gql,
                            fd_ty_input.clone() // Only the originally optional types are optional
                        )
                        .description(description!($fd))
                    );
                    // Main update mutation object that has all fields of the table. Only ID is non
                    // null in update input. All other fields are optional.
                    add_to_obj!($update_obj, InputValue::new(
                            &fd_name_gql,
                            unwrap_type(fd_ty_input.clone()) // Make every type optional
                        )
                        .description(description!($fd))
                    );
                    // For each field we also add a updateTableFieldName mutation. Here the ID and
                    // the field to update are non null, even if its kind may originally be
                    // optional.
                    $mutation_update_vec.push(define_singular_field_crud!(
                        $types,
                        "update",
                        fd_name_gql,
                        $tb_name,
                        fd_ty_input.clone()
                    ));
                    // For each field that has optional kind we also add a addTableFieldName
                    // mutation. Here the ID and the field to add are non null. This gives the
                    // option to add a field to a record that has not been set during the create
                    // mutation.
                    if kind.can_be_none() {
                        $mutation_add_vec.push(define_singular_field_crud!(
                            $types,
                            "add",
                            fd_name_gql,
                            $tb_name,
                            fd_ty_input.clone()
                        ));
                    }
                }
            }
        } else { // nested field
            // Array inner type is scalar, thus already set when adding the list field
            if fd_path.chars().last() == Some('*') { continue; }

            // expects the parent's `DefineFieldStatement` to come before its children as is
            // with `tx.all_tb_fields()`
            match $nested_objs_map.remove(&fd_path_parent) {
                Some(obj) => {
                    let new_field = match kind_non_optional {
                        Kind::Array(_, _) if $cursor => {
                            let kind = kind.inner_kind().unwrap();
                            let ty_ref = kind_to_type(kind.clone(), $types, path.as_slice())?;
                            let ty_name = ty_ref.type_name();

                            cursor_pagination!(
                                $types,
                                &fd_name_gql,
                                ty_name,
                                make_connection_resolver(fd_path.as_str(), ConnectionKind::Field),
                                edge_fields: [],
                                args: [],
                                is_relation: false
                            )
                        }
                        _ => {
                            Field::new(
                                fd_name_gql,
                                fd_ty,
                                make_field_resolver(fd_path.as_str()),
                            )
                        }
                    };
                    $nested_objs_map.insert(fd_path_parent.clone(), Object::from(obj)
                        .field(new_field.description(description!($fd)))
                    );
                }
                None => return Err(internal_error("Nested field should have parent object.")),
            }
        }
    };
}

fn remove_last_segment(input: &str) -> String {
    let mut parts = input.rsplitn(2, '.'); // Split from the right, limit to 2 parts
    parts.next(); // Discard the last segment
    parts.next().unwrap_or("").to_string() // Take the remaining part
}

#[allow(clippy::too_many_arguments)]
pub async fn process_tbs(
    tbs: Arc<[DefineTableStatement]>,
    mut query: Object,
    mut mutation: Object,
    types: &mut Vec<Type>,
    tx: &Transaction,
    ns: &str,
    db: &str,
    cursor: bool,
) -> Result<(Object, Object), GqlError> {
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

        let mut tb_fds_query = Vec::<Field>::new();
        let mut tb_nested_objs = BTreeMap::<String, Object>::new();
        // Collects all fields that can be used for ordering
        let mut tb_fds_orderable = Vec::<String>::new();
        let mut tb_fds_mutation_add = Vec::<Field>::new();
        let mut tb_fds_mutation_update = Vec::<Field>::new();

        let fds = tx.all_tb_fields(ns, db, &tb.name.0, None).await?;

        // =======================================================
        // Create objects
        // =======================================================

        let mut query_obj = define_obj!(query, &tb_name_gql);
        let mut mutation_create_obj = define_obj!(input, "create", &tb_name_gql);
        let mut mutation_update_obj = define_obj!(input, "update", &tb_name_gql);
        let mutation_delete_obj = define_obj!(input, "delete", &tb_name_gql); // no need for mut as it only needs the id field for the input
        let tb_create_payload_obj = define_obj!(payload, "create", &tb_name_gql);
        let tb_update_payload_obj = define_obj!(payload, "update", &tb_name_gql);
        let tb_delete_payload_obj = define_obj!(payload, "delete", &tb_name_gql);

        // =======================================================
        // Parse fields
        // =======================================================

        for fd in fds.iter() {
            // We have already defined "id", so we don't take any new definition for it.
            if fd.name.is_id() { continue; };

            parse_field!(
                fd,
                tb_name,
                types,
                cursor,
                tb_fds_query, // Cannot use query obj here directly, because the second call for
                // relations needs a vec to store the fields to
                tb_nested_objs,
                tb_fds_orderable,
                mutation_create_obj,
                mutation_update_obj,
                tb_fds_mutation_add,
                tb_fds_mutation_update
            );
        }
        define_order_input_types!(types, tb_name, tb_fds_orderable);

        // =======================================================
        // Parse relations
        // =======================================================

        //todo?: das hier nur n mal machen. Also nur dann wenn nicht vec ins > 1, bzw schon in map
        // possible performance improvements by skipping fields for prev relations
        if cursor {
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

                let (_, outs) = match &rel.kind {
                    TableType::Relation(r) => match (&r.from, &r.to) {
                        (Some(Kind::Record(from)), Some(Kind::Record(to))) => (from, to),
                        _ => continue,
                    },
                    _ => continue,
                };

                let mut rel_fds = Vec::<Field>::new();
                let mut rel_nested_objs = BTreeMap::<String, Object>::new();
                let mut rel_fds_orderable = Vec::<String>::new();

                let fds = tx.all_tb_fields(ns, db, &rel.name.0, None).await?;

                let mut temp = InputObject::new("temp1"); //TODO: remove
                let mut temp2 = InputObject::new("temp2"); //TODO: remove
                let mut temp3 = Vec::<Field>::new(); //TODO: remove
                let mut temp4 = Vec::<Field>::new(); //TODO: remove

                for fd in fds.iter().filter(|fd| {
                    // for cursor pagination, we only need the edge fields
                    cursor && !matches!(fd.name.to_string().as_str(), "in" | "out" | "id")
                }) {
                    parse_field!(
                        fd,
                        tb_name,
                        types,
                        cursor,
                        rel_fds,
                        rel_nested_objs,
                        rel_fds_orderable,
                        temp,
                        temp2,
                        temp3,
                        temp4
                    );
                }
                define_order_input_types!(types, &rel_name, rel_fds_orderable);

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

                tb_fds_query.push(
                    cursor_pagination!(
                        types,
                        pluralize(rel_name.to_camel_case()),
                        &node_ty_name,
                        make_connection_resolver(&rel_name, ConnectionKind::Relation),
                        edge_fields: rel_fds,
                        args: [
                            order_input!(&tb_name)
                        ],
                        is_relation: true
                    )
                );

                for obj in rel_nested_objs.into_values() {
                    types.push(Type::Object(obj));
                }
            }
        }

        // =======================================================
        // Add mutation objects to root mutation object
        // =======================================================

        for fd in tb_fds_mutation_add.into_iter() {
            add_to_obj!(mutation, fd);
        }
        add_to_obj!(mutation, "create", tb_create_payload_obj, &tb_name);
        add_to_obj!(mutation, "update", tb_update_payload_obj, &tb_name);
        for fd in tb_fds_mutation_update.into_iter() {
            add_to_obj!(mutation, fd);
        }
        add_to_obj!(mutation, "delete", tb_delete_payload_obj, &tb_name);

        // =======================================================
        // Add single query
        // =======================================================

        add_to_obj!(query,
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
            .description(description!(tb, format!("Generated from table `{}`\nallows querying a single record in a table by ID", &tb_name)))
            .argument(id_input!())
        );

        // =======================================================
        // Add list query
        // =======================================================

        let tb_name_plural = pluralize(tb_name_query.clone());

        if cursor {
            add_to_obj!(query,
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
                )
            );
        } else {
            add_to_obj!(query,
                Field::new(
                    tb_name_plural,
                    TypeRef::named_nn_list_nn(&tb_name_gql),
                    move |ctx| {
                        let tb_name = second_tb_name.clone();
                        FieldFuture::new(async move {
                            let gtx = ctx.data::<GQLTx>()?;

                            let args = ctx.args.as_index_map();
                            trace!("received request with args: {args:?}");

                            let start = args.get("start").and_then(|v| v.as_i64()).map(|s| s.intox());
                            let limit = args.get("limit").and_then(|v| v.as_i64()).map(|l| l.intox());
                            let order_by_arg = args.get("orderBy").and_then(GqlValueUtils::as_object);
                            let order_by = order_by(order_by_arg);

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
                                    order: order_by,
                                    // cond,
                                    start,
                                    limit,
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
                    .description(description!(tb,
                        format!("Generated from table `{}`\nallows querying a table with filters",&tb_name)))
                    .argument(limit_input!())
                    .argument(start_input!())
                    .argument(order_input!(&tb_name))
                // .argument(filter_input!(&tb_name))
            );
        }

        // =======================================================
        // Add types / build objects
        // =======================================================

        for fd in tb_fds_query.into_iter() {
            add_to_obj!(query_obj, fd);
        }

        types.push(Type::Object(query_obj));
        types.push(Type::InputObject(mutation_create_obj));
        types.push(Type::InputObject(mutation_update_obj));
        types.push(Type::InputObject(mutation_delete_obj));
        types.push(Type::Object(tb_create_payload_obj));
        types.push(Type::Object(tb_update_payload_obj));
        types.push(Type::Object(tb_delete_payload_obj));

        for obj in tb_nested_objs.into_values() {
            types.push(Type::Object(obj));
        }
    }

    Ok((query, mutation))
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

#[allow(clippy::too_many_lines)]
fn make_connection_resolver(
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
                        cursor: cursor::encode_cursor(item),
                        edge: edge.clone(),
                        node: node.clone(),
                    }
                })
                .collect();

            let page_info = PageInfo {
                has_next_page: cursor::has_next_page(edges, before_cursor_str.as_deref(), first),
                has_previous_page: cursor::has_previous_page(edges, after_cursor_str.as_deref(), last),
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

fn order_by(order_by_arg: Option<&IndexMap<Name, GqlValue>>) -> Option<Ordering> {
    match order_by_arg {
        Some(obj) => {
            let field = obj.get("field");
            let direction = obj.get("direction");

            let ord = match (field, direction) {
                (Some(GqlValue::Enum(n)), direction) => {
                    let fd_name = n.as_str().to_snake_case().to_lowercase();
                    let direction = direction.and_then(GqlValueUtils::as_name);

                    let mut ord = Order::default();
                    ord.value = fd_name.into();
                    ord.direction = direction
                        .map(|name| name.as_str() == "ASC")
                        .unwrap_or(true);

                    vec![ord]
                }
                _ => vec![],
            };
            Some(Ordering::Order(OrderList(ord)))
        }
        _ => {
            // Default ordering if no orderBy argument is provided.
            let mut order = Order::default();
            order.value = "id".into();
            order.direction = true; // Default to ascending order

            Some(Ordering::Order(OrderList(vec![order])))
        }
    }
}
