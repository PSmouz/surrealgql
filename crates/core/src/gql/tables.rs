use std::collections::BTreeMap;
use std::sync::Arc;

use super::error::{schema_error, GqlError};
use super::ext::IntoExt;
use super::schema::{gql_to_sql_kind, sql_value_to_gql_value};
use crate::gql::cursor::{make_list_resolver, make_object_resolver, make_value_resolver, ConnectionContext, ConnectionKind, EdgeContext};
use crate::gql::error::internal_error;
use crate::gql::ext::TryAsExt;
use crate::gql::resolvers::{dummy_resolver, make_connection_resolver, make_field_resolver, make_single_query_resolver, SingleQueryKind};
use crate::gql::schema::kind_to_type;
use crate::gql::utils::{pluralize, GQLTx, GqlTypeRefUtils, GqlValueUtils, KindUtils};
use crate::kvs::Transaction;
use crate::sql::order::{OrderList, Ordering};
use crate::sql::statements::define::config::graphql::CursorConfig;
use crate::sql::statements::{DefineTableStatement, SelectStatement};
use crate::sql::Statement;
use crate::sql::{self, Ident, Index, Operator, Order, Part, Table, TableType};
use crate::sql::{Cond, Fields};
use crate::sql::{Expression, Value as SqlValue};
use crate::sql::{Idiom, Kind};
use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::Field;
use async_graphql::dynamic::TypeRef;
use async_graphql::dynamic::{Enum, FieldValue, Type};
use async_graphql::dynamic::{EnumItem, FieldFuture};
use async_graphql::dynamic::{InputObject, Object};
use async_graphql::dynamic::{InputValue, Union};
use async_graphql::Name;
use async_graphql::Value as GqlValue;
use inflector::Inflector;

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

/// Generates an input value for ordering options based on the provided name.
///
/// This macro creates an `InputValue` for the `orderBy` argument,
/// which is used to specify ordering options for connections.
///
/// #### Important:
/// When using this macro for input types (e.g., order, filter, group), it is
/// **necessary** to define the corresponding types first. This can be easily done with the
/// [`define_input_types!`] macro.
///
/// # Parameters
/// - `$name`: The name of the entity for which ordering options are defined.
/// # Returns
/// - An `InputValue` for the `orderBy` argument, which is used in GraphQL queries to specify.
macro_rules! input {
    (FIRST) => {
		InputValue::new("first", TypeRef::named(TypeRef::INT))
        .description("Returns the first *n* elements from the list.")
	};
    (LAST) => {
		InputValue::new("last", TypeRef::named(TypeRef::INT))
        .description("Returns the last *n* elements from the list.")
	};
    (BEFORE) => {
		InputValue::new("before", TypeRef::named(TypeRef::STRING))
        .description("Returns the elements in the list that come before the specified cursor.")
	};
    (AFTER) => {
		InputValue::new("after", TypeRef::named(TypeRef::STRING))
        .description("Returns the elements in the list that come after the specified cursor.")
	};
    (START) => {
		InputValue::new("start", TypeRef::named(TypeRef::INT))
        .description("Number of records to skip. Use this parameter with 'limit' for offset-based pagination.")
	};
    (LIMIT) => {
		InputValue::new("limit", TypeRef::named(TypeRef::INT))
        .description("Maximum number of records to return. Use this parameter to limit result size.")
	};
    (ID_NON_NULL) => {
        InputValue::new("id", TypeRef::named_nn(TypeRef::ID))
            .description("The required ID of the record. Can be a string ID or a record ID in the format 'table:id'.")
    };
    (ID_OPTIONAL) => {
        InputValue::new("id", TypeRef::named(TypeRef::ID))
            .description("The ID of the record. Can be a string or a record ID ('table:id'). Only one unique identifier (id, or another unique field) can be provided.")
    };
    (ORDER, $name: expr) => {
		InputValue::new("orderBy", TypeRef::named(format!("{}Order", $name.to_pascal_case())))
        .description(format!("Ordering options for `{}` connections.", $name))
	};
    (FILTER, $name: expr) => {
		InputValue::new("filterBy", TypeRef::named(format!("{}Filter", $name.to_pascal_case())))
	};
    (GROUP, $name: expr) => {
		InputValue::new("groupBy", TypeRef::named_nn_list(format!("{}Group", $name.to_pascal_case())))
	};
    (
        $fd_name: expr,
        $ty_ref: expr
    ) => {
        InputValue::new($fd_name.to_camel_case(), $ty_ref)
        .description(format!("The {} of the record to fetch. Must be a {}. This input argument is \
         autogenerated from an unique index for this table column.", $fd_name.to_camel_case(),
        $ty_ref.type_name()))
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

/// Defines the input types for a given operation, base name, and fields. By default, it defines
/// order, filter, and group input types.
///
/// #### Important:
/// This macro **requires** the order direction enum type defined.
///
/// # Example
///
/// Given the following inputs:
///
/// - **`base_name`**: `"Home"`
/// - **`fields`**: `&[("name",Kind::String), ("created_at",Kind:Datetime)]`
///
/// The macro generates the equivalent of this GraphQL schema:
///
/// ```graphql
/// """Properties by which Home can be ordered."""
/// enum HomeOrderField {
///   """Order Home by ID."""
///   ID
///
///   """Order Home by name."""
///   NAME
///
///   """Order Home by created_at."""
///   CREATED_AT
/// }
///
/// """Ordering options for Home connections."""
/// input HomeOrder {
///   """The field to order Home by."""
///   field: HomeOrderField
///
///   """The ordering direction."""
///   direction: OrderDirection
/// }
///
/// """Fields to group Home by."""
/// enum HomeGroup {
///   """Group Home by ID."""
///   ID
///
///   """Group Home by name."""
///   NAME
///
///   """Group Home by created_at."""
///   CREATED_AT
/// }
///
/// """The filters that are available when fetching Home."""
/// input HomeFilter {
///   """Filters the Home by name."""
///   NAME: StringFilterInput
///
///   """Filters the Home by created_at."""
///   CREATED_AT: DateTimeFilterInput
/// }
///
/// ```
///
/// # Parameters
/// - `$types`: The types vector to which the order input types are added.
/// - `$base_name`: The base name for the order fields and input object.
/// - `fds`: A vector of (field names, Kind) that can be used for ordering, filtering, grouping.
/// # Returns
/// - Adds an enum and an input object to the `$types` vector.
macro_rules! define_input_types {
    (
        ORDER,
        $types:ident,
        $base_name:expr,
        $fds:expr
    ) => {
        let base_name_pascal = $base_name.to_pascal_case();
        let enum_name = format!("{}OrderField", base_name_pascal);
        let obj_name = format!("{}Order", base_name_pascal);

        let mut order_by_enum = Enum::new(&enum_name)
            .item(EnumItem::new("ID").description(format!("{} by ID.", $base_name)))
            .description(format!("Properties by which {} can be ordered.", $base_name));

        for (fd, _) in $fds {
            order_by_enum = order_by_enum.item(
                EnumItem::new(fd.to_screaming_snake_case())
                .description(format!("{} by {}.", $base_name, fd.to_screaming_snake_case()))
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
    (
        FILTER,
        $types:ident,
        $base_name:expr,
        $fds:expr
    ) => {
        let base_name_pascal = $base_name.to_pascal_case();
        let obj_name = format!("{}Filter", base_name_pascal);

        let mut filter_by_obj = InputObject::new(&obj_name)
            .description(format!("The filters that are available when fetching {}.", $base_name));

        for (fd, kind) in $fds {
            assert!(kind.is_scalar(), "Filterable fields must be scalar types.");

            filter_by_obj = filter_by_obj.field(
                InputValue::new(fd.to_camel_case(), TypeRef::named(kind.scalar_to_filter_input_name()
                .unwrap()))
                .description(format!("Filters the {} by {}.", $base_name, fd))
            );
        }

        $types.push(Type::InputObject(filter_by_obj));
    };
    (
        GROUP,
        $types:ident,
        $base_name:expr,
        $fds:expr
    ) => {
        let base_name_pascal = $base_name.to_pascal_case();
        let enum_name = format!("{}Group", base_name_pascal);

        let mut group_by_enum = Enum::new(&enum_name)
            .item(EnumItem::new("ID").description(format!("{} by ID.", $base_name)))
            .description(format!("Fields to group {} by.", $base_name));

        for (fd, _) in $fds {
            group_by_enum = group_by_enum.item(
                EnumItem::new(fd.to_screaming_snake_case())
                .description(format!("{} by {}.", $base_name, fd.to_screaming_snake_case()))
            );
        }

        $types.push(Type::Enum(group_by_enum));
    };
    (
        $types:ident,
        $base_name:expr,
        $fds:expr
    ) => {
        define_input_types!(ORDER, $types, $base_name, $fds);
        define_input_types!(FILTER, $types, $base_name, $fds);
        define_input_types!(GROUP, $types, $base_name, $fds);
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
            .argument(input!(AFTER))
            .argument(input!(BEFORE))
            .argument(input!(FIRST))
            .argument(input!(LAST))
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
            TypeRef::named_nn(TypeRef::ID),
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
                    $ty.to_optional(), // Make type optional in payload
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
/// - `$unique_map`: The map of unique index fields for the table.
/// - `$query_vec`: The vector of fields for the query object.
/// - `$nested_objs_map`: The map of nested objects to which the field is added.
/// - `input_vec`: The vector of scalar fields (fd_name, kind) for the table.
/// - `$create_obj`: The input object for the createTable mutation.
/// - `$update_obj`: The input object for the updateTable mutation.
/// - `$mutation_add_vec`: The vector of addTableFieldName mutations for the table.
/// - `$mutation_update_vec`: The vector of updateTableFieldName mutations for the table.
macro_rules! parse_field {
    (
        $fd:ident,
        $tb_name:ident,
        $types:ident,
        $cursor:expr,
        $idx_map:ident,
        $query_vec:ident,
        $nested_objs_map:ident,
        $input_vec:ident,
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

        // Use table name for e.g., object uniqueness across multiple tables
        let mut path = Vec::with_capacity(parts.len() + 1);
        let table_ident = Ident::from($tb_name.clone());
        path.push(&table_ident);
        path.extend_from_slice(parts.as_slice());

        let mut fd_ty = kind_to_type(kind.clone(), $types, path.as_slice())?;

        trace!(":::DEBUG::: field {}, kind {:?}, type {:?}, type_name {:?}", fd_name, kind.clone(),
        fd_ty.clone(), fd_ty.type_name());

        // object map used to add fields step by step to the objects
        if kind_non_optional == Kind::Object {
            $nested_objs_map.insert(
                fd_path.clone(),
                (
                    Object::new(fd_ty.type_name()).description(description!($fd)),
                    InputObject::new(format!("Create{}Input", fd_ty.type_name()))
                        .description(description!($fd)),
                    InputObject::new(format!("Update{}Input", fd_ty.type_name()))
                        .description(description!($fd))
                ),
            );
        }

        // Query field
        let fd_q = match matches!($cursor, CursorConfig::Auto)
                            && matches!(kind_non_optional, Kind::Array(_, _)) {
            true => {
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
            },
            false => {
                Field::new(
                    &fd_name_gql,
                    fd_ty.clone(),
                    make_field_resolver(fd_path.as_str()),
                )
                .description(description!($fd))
            },
        };

        // Overwrite the field type if we have a record. Because the fd_ty will be
        // `NonNull(Named("Tablename"))` for records, we cannot use this type for mutations or
        // input args. In input fields we want the user to provide the record ID, not a record
        // object.
        if kind_non_optional.is_record() {
            trace!(":::DEBUG::: KIND IS RECORD, setting field type to ID");
           fd_ty = TypeRef::named_nn(TypeRef::ID);
        };

        // Create field
        let fd_c = InputValue::new(
            &fd_name_gql,
            // Only the originally optional types are optional
            if kind_non_optional.allows_nested_kind(&[Part::All], &Kind::Object) {
                // when we have a object nested we need to replace the query object name with the
                // actual input object typename.
                fd_ty.rename(format!("Create{}Input", fd_ty.type_name()))
            } else if matches!(kind_non_optional, Kind::Geometry(_)) {
                fd_ty.rename(format!("{}Input", fd_ty.type_name()))
            } else {
                fd_ty.clone()
            }
        )
        .description(description!($fd));

        // Update field
        let fd_u = InputValue::new(
            &fd_name_gql,
            // Make every type optional
            if kind_non_optional.allows_nested_kind(&[Part::All], &Kind::Object) {
                fd_ty.rename(format!("Update{}Input", fd_ty.type_name())).to_optional()
            } else if matches!(kind_non_optional, Kind::Geometry(_)) {
                fd_ty.rename(format!("{}Input", fd_ty.type_name())).to_optional()
            } else {
                fd_ty.to_optional()
            }
        )
        .description(description!($fd));

        if fd_path_parent.is_empty() { // top level field
            // Decide, based on its kind, wether this field can be ordered by the user.
            if kind_non_optional.is_scalar() {
                $input_vec.push((fd_name_gql.clone(), kind_non_optional.clone()));
            }
            // Add input arg if there exists an unique index for this top level field.
            // Doing it here for double security, but should work outside of the if as well, as only
            // top level fields can be indexed?! However, it should be after the fd_type record
            // setter.
            if $idx_map.contains_key(&fd_name) {
                // The key in the map is the raw field name from the database, which is
                // needed for the SQL query. The value is the GraphQL InputValue, which
                // contains the camel-cased name for the schema.
                $idx_map.insert(
                    fd_name.clone(),
                    Some(fd_ty.clone()),
                    // Some(input!(&fd_name, fd_ty.clone().to_optional())),
                );
            }
            // Cannot use query object here directly, because parse field for relations
            // depends on the fields being stored in a vector.
            $query_vec.push(fd_q);
            // Main create mutation object that has all fields of the table. Those that have
            // optional kind are optional all others are non null fields. ID is always
            // non-null in create input.
            add_to_obj!($create_obj, fd_c);
            // Main update mutation object that has all fields of the table. Only ID is non
            // null in update input. All other fields are optional.
            add_to_obj!($update_obj, fd_u);

            // FIXME: make objects and arrays work as well
            if kind_non_optional.is_scalar() {
                // For each field we also add a updateTableFieldName mutation. Here the ID and
                // the field to update are non null, even if its kind may originally be
                // optional.
                $mutation_update_vec.push(define_singular_field_crud!(
                    $types,
                    "update",
                    fd_name_gql,
                    $tb_name,
                    fd_ty.to_non_null()
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
                        fd_ty.to_non_null()
                    ));
                }
            }
        } else { // nested field
            // Array inner type is scalar, thus already set when adding the list field
            if fd_path.chars().last() == Some('*') { continue; }

            // expects the parent's `DefineFieldStatement` to come before its children as is
            // with `tx.all_tb_fields()`
            match $nested_objs_map.remove(&fd_path_parent) {
                Some((obj_q, obj_c, obj_u)) => {
                    $nested_objs_map.insert(fd_path_parent.clone(),
                        (
                            Object::from(obj_q).field(fd_q),
                            InputObject::from(obj_c).field(fd_c),
                            InputObject::from(obj_u).field(fd_u),
                        )
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
    cursor: CursorConfig,
) -> Result<(Object, Object), GqlError> {
    let mut tables = Vec::<&DefineTableStatement>::new();
    let mut relations = Vec::<&DefineTableStatement>::new();

    for tb in tbs.iter() {
        match tb.kind {
            TableType::Normal => tables.push(tb),
            TableType::Relation(_) => relations.push(tb),
            TableType::Any =>
                return Err(schema_error("TableType::Any is not yet supported").into())
        }
    }

    // FIXME: cleaner loop by looping tbs and relations separately. make a big map, where we
    // store all the fields, so we can add the relations to these objects later.

    for tb in tables.into_iter() {
        let tb_name = tb.name.to_string();
        let first_tb_name = tb_name.clone();
        let second_tb_name = tb_name.clone();
        let tb_name_gql = tb_name.to_pascal_case();
        let tb_name_query = tb_name.to_camel_case(); // field name for the table in the query

        let mut tb_fds_query = Vec::<Field>::new();
        // Stores the nested objects. The tuple is (query_obj, create_input_obj, update_input_obj)
        let mut tb_nested_objs = BTreeMap::<String, (Object, InputObject, InputObject)>::new();
        let mut tb_fds_mutation_add = Vec::<Field>::new();
        let mut tb_fds_mutation_update = Vec::<Field>::new();
        // Collects all fields that can be used for ordering and filtering. These are scalar fields.
        // We use a vec due to lower memory overhead and because we only iter and don't look up.
        let mut tb_fds_scalar = Vec::<(String, Kind)>::new();
        // Collects all fields that are columns in unique indexes thus needing an input value
        // e.g., k: "email", v: Some(InputValue::new("email", TypeRef::STRING))
        let mut tb_fds_index = BTreeMap::<String, Option<TypeRef>>::new();
        // Collects all unique indexes. k: fd_name combined, v: column names (keys in the tb_fds_index)
        // e.g., cols: ["name", "email"] -> k: "NameEmail", v: ["name", "email"]
        let mut indexes = BTreeMap::<String, Vec<String>>::new();

        let fds = tx.all_tb_fields(ns, db, &tb.name.0, None).await?;
        let idxs = tx.all_tb_indexes(ns, db, &tb.name.0).await?;

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
        // Parse indexes
        // =======================================================

        for idx in idxs.iter().filter(|stmt| stmt.index == Index::Uniq) {
            let idx_cols = idx.cols.iter().map(|c| c.to_string()).collect::<Vec<_>>();
            let idx_name = idx_cols.iter().map(|c| c.to_pascal_case()).collect::<String>();

            for col in idx_cols.iter() {
                tb_fds_index.insert(col.clone(), None);
            }

            indexes.insert(
                idx_name,
                idx_cols,
            );
        }

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
                tb_fds_index,
                tb_fds_query, // Cannot use query obj here directly, because the second call for
                // relations needs a vec to store the fields to
                tb_nested_objs,
                tb_fds_scalar,
                mutation_create_obj,
                mutation_update_obj,
                tb_fds_mutation_add,
                tb_fds_mutation_update
            );
        }
        define_input_types!(types, &tb_name, &tb_fds_scalar);

        // =======================================================
        // Parse relations
        // =======================================================

        //todo?: das hier nur n mal machen. Also nur dann wenn nicht vec ins > 1, bzw schon in map
        // possible performance improvements by skipping fields for prev relations
        if matches!(cursor, CursorConfig::Auto | CursorConfig::Relation) {
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
                let mut rel_nested_objs = BTreeMap::<String, (Object, InputObject, InputObject)>::new();
                let mut rel_fds_scalar = Vec::<(String, Kind)>::new();

                let fds = tx.all_tb_fields(ns, db, &rel.name.0, None).await?;

                let mut temp = InputObject::new("temp1"); //TODO: remove
                let mut temp2 = InputObject::new("temp2"); //TODO: remove
                let mut temp3 = Vec::<Field>::new(); //TODO: remove
                let mut temp4 = Vec::<Field>::new(); //TODO: remove
                let mut rel_fds_unique = BTreeMap::<String, Option<TypeRef>>::new();

                for fd in fds.iter().filter(|fd| {
                    // for cursor pagination, we only need the edge fields
                    !matches!(fd.name.to_string().as_str(), "in" | "out" | "id")
                }) {
                    parse_field!(
                        fd,
                        rel_name,
                        types,
                        cursor,
                        rel_fds_unique,
                        rel_fds,
                        rel_nested_objs,
                        rel_fds_scalar,
                        temp,
                        temp2,
                        temp3,
                        temp4
                    );
                }
                define_input_types!(types, &rel_name, &rel_fds_scalar);

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
                            input!(ORDER, &tb_name),
                            input!(FILTER, &tb_name),
                            input!(GROUP, &tb_name),
                        ],
                        is_relation: true
                    )
                );

                for (obj_q, obj_c, obj_u) in rel_nested_objs.into_values() {
                    types.push(Type::Object(obj_q));
                    types.push(Type::InputObject(obj_c));
                    types.push(Type::InputObject(obj_u));
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

        let unique_fd_names_for_resolver = tb_fds_index
            .iter()
            // We only care about fields for which we successfully created an InputValue.
            .filter(|(_, v)| v.is_some())
            .map(|(k, _)| k.clone())
            .collect::<Vec<String>>();

        let mut single_query_fd = Field::new(
            tb_name_query.to_singular(),
            TypeRef::named(&tb_name_gql),
            make_single_query_resolver(
                first_tb_name.clone(),
                SingleQueryKind::ByArbitraryIndex(unique_fd_names_for_resolver),
                indexes.clone(),
            ),
        )
            .description(description!(tb, format!("Generated from table `{}` allows querying a single record.", &tb_name)))
            .argument(input!(ID_OPTIONAL)); // The default ID input argument
        // .directive(Directive::new("semanticNonNull").argument(
        //     "levels",
        //     GqlValue::List(vec![GqlValue::from(0)]),
        // ));

        for (fd_name, opt_ty) in tb_fds_index.iter() {
            if let Some(ty) = opt_ty {
                single_query_fd = single_query_fd.argument(input!(fd_name.as_str(), ty.clone().to_optional()));
            }
        }
        add_to_obj!(query, single_query_fd);

        // =======================================================
        // Add singleById query
        // =======================================================

        add_to_obj!(query, Field::new(
                format!("{}ById", tb_name_query.to_singular()),
                TypeRef::named(&tb_name_gql),
                make_single_query_resolver(
                    first_tb_name.clone(),
                    SingleQueryKind::ById,
                    BTreeMap::new(), // Indexes not needed for this kind
                ),
            )
            .description(description!(tb, format!("Generated from table `{}` allows querying a single record by ID.", &tb_name)))
            .argument(input!(ID_NON_NULL))
        );

        // =======================================================
        // Add singleByIndex queries
        // =======================================================

        for (idx_name, fd_names) in indexes.iter() {
            let mut single_query_fd = Field::new(
                format!("{}By{}", tb_name_query.to_singular(), idx_name),
                TypeRef::named(&tb_name_gql),
                make_single_query_resolver(
                    first_tb_name.clone(),
                    SingleQueryKind::BySpecificIndex(fd_names.clone()),
                    BTreeMap::new(), // Indexes not needed for this kind
                ),
            )
                .description(description!(tb, format!("Generated from table `{}` allows querying a single record by {}.", &tb_name, idx_name)));

            for fd_name in fd_names.iter() {
                if let Some(Some(ty)) = tb_fds_index.get(fd_name) {
                    single_query_fd = single_query_fd.argument(input!(fd_name, ty.clone().to_non_null()));
                }
            }
            add_to_obj!(query, single_query_fd);
        }

        // =======================================================
        // Add list query
        // =======================================================

        let tb_name_plural = pluralize(tb_name_query.clone());

        if matches!(cursor, CursorConfig::Auto) {
            add_to_obj!(query,
                cursor_pagination!(
                    types,
                    &tb_name_plural,
                    &tb_name_gql,
                    make_connection_resolver(&tb_name_query, ConnectionKind::Table),
                    edge_fields: [],
                    args: [
                        input!(ORDER, &tb_name),
                        input!(FILTER, &tb_name),
                        input!(GROUP, &tb_name),
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
                        format!("Generated from table `{}` allows querying a table with filters",
                        &tb_name)))
                    .argument(input!(LIMIT))
                    .argument(input!(START))
                    .argument(input!(ORDER, &tb_name))
                    .argument(input!(FILTER, &tb_name))
                    .argument(input!(GROUP, &tb_name))
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

        for (obj_q, obj_c, obj_u) in tb_nested_objs.into_values() {
            types.push(Type::Object(obj_q));
            types.push(Type::InputObject(obj_c));
            types.push(Type::InputObject(obj_u));
        }
    }

    // TODO: handle relations outside of cursor=yes(auto)
    // if !cursor {
    //     for rel in relations.iter() {
    //         let rel_name = rel.name.to_string();
    //         let first_rel_name = rel_name.clone();
    //
    //         let mut rel_fds = Vec::<Field>::new();
    //         let mut rel_nested_objs = BTreeMap::<String, Object>::new();
    //         let mut rel_fds_orderable = Vec::<String>::new();
    //
    //         let fds = tx.all_tb_fields(ns, db, &rel.name.0, None).await?;
    //         let mut query_obj = define_obj!(query, rel_name.to_pascal_case());
    //
    //         let mut temp = InputObject::new("temp1"); //TODO: remove
    //         let mut temp2 = InputObject::new("temp2"); //TODO: remove
    //         let mut temp3 = Vec::<Field>::new(); //TODO: remove
    //         let mut temp4 = Vec::<Field>::new(); //TODO: remove
    //
    //         for fd in fds.iter() {
    //             parse_field!(
    //                     fd,
    //                     rel_name,
    //                     types,
    //                     cursor,
    //                     rel_fds,
    //                     rel_nested_objs,
    //                     rel_fds_orderable,
    //                     temp,
    //                     temp2,
    //                     temp3,
    //                     temp4
    //                 );
    //         }
    //         define_order_input_types!(types, &rel_name, rel_fds_orderable);
    //
    //         add_to_obj!(query,
    //             Field::new(
    //                 rel_name.to_camel_case(),
    //                 TypeRef::named_nn_list_nn(rel_name.to_pascal_case()),
    //                 // TODO: refactor into function
    //                 move |ctx| {
    //                     let rel_name = first_rel_name.clone();
    //                     FieldFuture::new(async move {
    //                         let gtx = ctx.data::<GQLTx>()?;
    //
    //                         let args = ctx.args.as_index_map();
    //                         trace!("received request with args: {args:?}");
    //
    //                         let start = args.get("start").and_then(|v| v.as_i64()).map(|s| s.intox());
    //                         let limit = args.get("limit").and_then(|v| v.as_i64()).map(|l| l.intox());
    //                         let order_by_arg = args.get("orderBy").and_then(GqlValueUtils::as_object);
    //                         let order_by = order_by(order_by_arg);
    //
    //                         // SELECT VALUE id FROM ...
    //                         let ast = Statement::Select({
    //                             SelectStatement {
    //                                 what: vec![SqlValue::Table(rel_name.intox())].into(),
    //                                 expr: Fields(
    //                                     vec![sql::Field::Single {
    //                                         expr: SqlValue::Idiom(Idiom::from("id")),
    //                                         alias: None,
    //                                     }],
    //                                     // this means the `value` keyword
    //                                     true,
    //                                 ),
    //                                 order: order_by,
    //                                 // cond,
    //                                 start,
    //                                 limit,
    //                                 ..Default::default()
    //                             }
    //                         });
    //                         trace!("generated query ast: {ast:?}");
    //
    //                         let res = gtx.process_stmt(ast).await?;
    //
    //                         trace!("query result: {res:?}");
    //
    //                         let res_vec =
    //                             match res {
    //                                 SqlValue::Array(a) => a,
    //                                 v => {
    //                                     error!("Found top level value, in result which should be array: {v:?}");
    //                                     return Err("Internal Error".into());
    //                                 }
    //                             };
    //
    //                         trace!("query result array: {res_vec:?}");
    //
    //                         let out: Result<Vec<FieldValue>, SqlValue> = res_vec
    //                             .0
    //                             .into_iter()
    //                             .map(|v| {
    //                                 v.try_as_thing().map(|t| {
    //                                     FieldValue::owned_any(t)
    //                                 })
    //                             })
    //                             .collect();
    //
    //                         match out {
    //                             Ok(l) => Ok(Some(FieldValue::list(l))),
    //                             Err(v) => {
    //                                 Err(internal_error(format!("expected thing, found: {v:?}")).into())
    //                             }
    //                         }
    //                     })
    //                 },
    //             )
    //             .description(description!(rel, format!("Generated from relation `{}`\nallows querying a relation with filters", &rel_name)))
    //             .argument(limit_input!())
    //             .argument(start_input!())
    //             .argument(order_input!(&rel_name))
    //             // .argument(filter_input!(&rel_name))
    //         );
    //
    //         for fd in rel_fds.into_iter() {
    //             add_to_obj!(query_obj, fd);
    //         }
    //
    //         types.push(Type::Object(query_obj));
    //         for obj in rel_nested_objs.into_values() {
    //             types.push(Type::Object(obj));
    //         }
    //     }
    // }

    Ok((query, mutation))
}

pub fn order_by(order_by_arg: Option<&IndexMap<Name, GqlValue>>) -> Option<Ordering> {
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

// TODO: make even more arbitrary. The accumulater operation should be selectable like AND/OR etc.
// then split into binop and aggregate functions.
/// Builds a SQL WHERE clause from a list of GraphQL arguments.
///
/// The conditions are combined using the AND operator. This function will return
/// an error if the provided list of conditions is empty.
pub fn build_sql_where_clause(conditions: &[(String, GqlValue)]) -> Result<Cond, GqlError> {
    if conditions.is_empty() {
        return Err(internal_error("Cannot build a WHERE clause from an empty set of conditions.").into());
    }

    let mut expressions = Vec::new();
    for (fd_name, gql_val) in conditions {
        let sql_val = gql_to_sql_kind(gql_val, Kind::Any)?;
        expressions.push(Expression::Binary {
            l: SqlValue::Idiom(Idiom::from(fd_name.as_str())),
            o: Operator::Equal,
            r: sql_val,
        });
    }

    // `unwrap` is safe here because we checked for an empty slice at the start.
    let combined_expr = expressions.into_iter().reduce(|acc, expr| Expression::Binary {
        l: acc.into(),
        o: Operator::And,
        r: expr.into(),
    }).unwrap();

    Ok(Cond(SqlValue::from(combined_expr)))
}