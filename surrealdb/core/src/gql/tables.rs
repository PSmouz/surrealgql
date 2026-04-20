//! GraphQL table query generation and type construction.
//!
//! This module is responsible for generating the Query root fields and
//! Object types that correspond to each database table exposed via GraphQL.
//!
//! ## Generated Query fields
//!
//! For each table (e.g. `person`), the following Query fields are created:
//!
//! - `person(id, version)` -- single-record fetch returning `Person`
//! - `persons(first, after, last, before, orderBy, filterBy, version)` --
//!   Relay connection query returning `PersonConnection!`
//!
//! ## Generated types
//!
//! For each table, the module generates:
//!
//! - An **Object type** with a field for each defined column, plus an `id` field and any relation
//!   fields.
//! - An **orderable enum** (`{Table}OrderField`) listing sortable fields.
//! - An **order input** (`{Table}Order`) for specifying sort criteria.
//! - A **filter input** (`{Table}FilterInput`) with per-field comparison operators.
//!
//! ## Performance: CachedRecord
//!
//! List and get queries issue `SELECT *` and wrap the full result objects in
//! [`CachedRecord`] instances.  Field resolvers then extract values directly
//! from the in-memory cache, eliminating the N+1 query problem.  Record-link
//! fields (`TYPE record<target>`) issue a single additional `SELECT *` on
//! the target and wrap it in a new `CachedRecord`.
//!
//! ## Nested objects
//!
//! Fields of `TYPE object` (or `TYPE array<object>`) that have sub-field
//! definitions (e.g. `DEFINE FIELD time.createdAt`) are detected and
//! represented as dedicated GraphQL Object types rather than the opaque
//! `object` scalar.

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::Arc;

use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
	Enum, EnumItem, Field, FieldFuture, FieldValue, InputObject, InputValue, Object,
	ResolverContext, Type, TypeRef,
};
use async_graphql::{Name, Value as GqlValue};
use surrealdb_types::ToSql;

use super::cursor::{Connection, ConnectionArgs, ConnectionEdge, ConnectionNode, paginate};
use super::error::{GqlError, resolver_error};
use super::naming;
use super::relations::RelationInfo;
use super::schema::{
	SchemaContext, gql_to_sql_kind, gql_to_sql_kind_with_scope, record_id_to_raw,
	semantic_non_null_directive, sql_value_to_gql_value, sql_value_to_gql_value_with_kind,
};
use crate::catalog::providers::TableProvider;
use crate::catalog::{FieldDefinition, TableDefinition, TableType};
use crate::dbs::Session;
use crate::expr::field::Selector;
use crate::expr::order::{OrderList, Ordering};
use crate::expr::part::Part;
use crate::expr::statements::SelectStatement;
use crate::expr::{
	self, BinaryOperator, Cond, Expr, Fields, Function, FunctionCall, Idiom, Kind, Limit, Literal,
	LogicalPlan, Start, TopLevelExpr,
};
use crate::gql::error::internal_error;
use crate::gql::schema::{geometry_gql_type_name, kind_to_type_with_enum_prefix, unwrap_type};
use crate::gql::utils::{GqlValueUtils, execute_plan};
use crate::kvs::Datastore;
use crate::val::{Array as SurArray, Datetime, Object as SurObject, RecordId, TableName, Value};

/// Create an ascending `ORDER BY` clause for the given field.
fn order_asc(field_name: String) -> expr::Order {
	expr::Order {
		value: Idiom::field(field_name),
		direction: true,
		..Default::default()
	}
}

/// Create a descending `ORDER BY` clause for the given field.
fn order_desc(field_name: String) -> expr::Order {
	expr::Order {
		value: Idiom::field(field_name),
		..expr::Order::default()
	}
}

/// A record ID with an optional version for temporal queries.
/// Propagates the version from top-level queries down to field and relation resolvers,
/// ensuring consistent versioned reads across the entire query tree.
///
/// Used as a fallback when full record data is not available (e.g., from custom
/// function return values). Prefer [`CachedRecord`] when the full record data
/// has already been fetched.
#[derive(Clone, Debug)]
pub(crate) struct VersionedRecord {
	pub rid: RecordId,
	pub version: Option<Datetime>,
}

/// A record with its full field data cached from a parent query.
///
/// Field resolvers extract values directly from the cached data without issuing
/// additional database queries, eliminating the N+1 query problem. When a list
/// query fetches `SELECT * FROM table`, the full objects are preserved in
/// `CachedRecord` instances and passed to field resolvers, which simply read
/// from the in-memory data instead of issuing per-field `SELECT VALUE` queries.
///
/// For record-link fields (`TYPE record<target>`), the resolver performs a
/// single `SELECT * FROM ONLY <target>` to fetch the linked record's full data
/// and wraps it in a new `CachedRecord`, so the target's field resolvers also
/// benefit from caching.
#[derive(Clone, Debug)]
pub(crate) struct CachedRecord {
	pub rid: RecordId,
	pub version: Option<Datetime>,
	/// The full record data. Field resolvers extract values from here
	/// instead of firing individual `SELECT VALUE` queries.
	pub data: SurObject,
}

/// Convert an optional `Datetime` version to the `Expr` representation
/// used in `SelectStatement.version`.
fn version_to_expr(version: &Option<Datetime>) -> Expr {
	match version {
		Some(dt) => Expr::Literal(Literal::Datetime(dt.clone())),
		None => Expr::Literal(Literal::None),
	}
}

/// Parse the optional `version` argument from GraphQL query arguments.
fn parse_version_arg(args: &IndexMap<Name, GqlValue>) -> Result<Option<Datetime>, GqlError> {
	match args.get("version") {
		Some(GqlValue::String(s)) => {
			let dt = crate::syn::datetime(s)
				.map_err(|_| resolver_error(format!("Invalid version datetime: {s}")))?;
			Ok(Some(dt.into()))
		}
		Some(GqlValue::Null) | None => Ok(None),
		Some(_) => Err(resolver_error("version must be a datetime string")),
	}
}

fn parse_connection_args(args: &IndexMap<Name, GqlValue>) -> Result<ConnectionArgs, GqlError> {
	let first = args
		.get("first")
		.and_then(|value| value.as_i64())
		.map(|value| usize::try_from(value).map_err(|_| resolver_error("`first` must be >= 0")))
		.transpose()?;
	let last = args
		.get("last")
		.and_then(|value| value.as_i64())
		.map(|value| usize::try_from(value).map_err(|_| resolver_error("`last` must be >= 0")))
		.transpose()?;

	Ok(ConnectionArgs {
		first,
		after: args.get("after").and_then(GqlValueUtils::as_string),
		last,
		before: args.get("before").and_then(GqlValueUtils::as_string),
	})
}

fn sql_field_name_from_gql(field_name: &str, fds: &[FieldDefinition]) -> Option<String> {
	if field_name == "id" {
		return Some("id".to_string());
	}

	fds.iter().find_map(|fd| {
		if fd.name.0.len() != 1 {
			return None;
		}

		let sql_name = fd.name.to_sql();
		(naming::to_camel_case(&sql_name) == field_name).then_some(sql_name)
	})
}

/// Parse the optional `orderBy` argument from GraphQL query arguments.
fn parse_order_arg(
	args: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
) -> Result<Option<Ordering>, GqlError> {
	let Some(GqlValue::Object(order)) = args.get("orderBy") else {
		return Ok(None);
	};

	let field = order
		.get("field")
		.and_then(|value| match value {
			GqlValue::Enum(name) => Some(name.as_str()),
			_ => None,
		})
		.ok_or_else(|| resolver_error("`orderBy.field` must be an enum value"))?;
	let direction = order
		.get("direction")
		.and_then(|value| match value {
			GqlValue::Enum(name) => Some(name.as_str()),
			_ => None,
		})
		.unwrap_or("ASC");

	let field_name =
		sql_field_name_from_gql(&naming::to_camel_case(&field.to_ascii_lowercase()), fds)
			.or_else(|| sql_field_name_from_gql(&field.to_ascii_lowercase(), fds))
			.ok_or_else(|| resolver_error(format!("Unknown order field `{field}`")))?;

	let order = match direction {
		"ASC" => order_asc(field_name),
		"DESC" => order_desc(field_name),
		_ => return Err(resolver_error(format!("Unknown order direction `{direction}`"))),
	};

	Ok(Some(Ordering::Order(OrderList(vec![order]))))
}

/// Parse the optional `filterBy` argument from GraphQL query arguments.
pub(crate) fn parse_filter_arg(
	args: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
	tb_name: &str,
) -> Result<Option<Cond>, GqlError> {
	let filter = args.get("filterBy");
	match filter {
		Some(GqlValue::Object(o)) => Ok(Some(cond_from_filter(o, fds, tb_name)?)),
		Some(f) => {
			error!(
				"Found filter {f}, which should be object and should have \
				 been rejected by async graphql."
			);
			Err(resolver_error("Value in cond doesn't fit schema"))
		}
		None => Ok(None),
	}
}

// ---------------------------------------------------------------------------
// SelectStatement builder helpers
// ---------------------------------------------------------------------------

/// Build a `SELECT * FROM ONLY <record_id>` statement with an optional version.
///
/// Used by singular root queries and record-link dereferencing to fetch a single
/// record's full data for caching.
fn select_all_from_record(rid: &RecordId, version: &Option<Datetime>) -> SelectStatement {
	SelectStatement {
		what: vec![Value::RecordId(rid.clone()).into_literal()],
		fields: Fields::all(),
		only: true,
		version: version_to_expr(version),
		timeout: Expr::Literal(Literal::None),
		omit: vec![],
		with: None,
		cond: None,
		split: None,
		group: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		explain: None,
		tempfiles: false,
	}
}

/// Build a `SELECT VALUE <field> FROM ONLY <record_id>` statement with an
/// optional version.
///
/// Used by field resolvers and nested-object resolvers to fetch a single
/// field's value when the record data is not cached.
fn select_field_from_record(
	rid: &RecordId,
	field_name: &str,
	version: &Option<Datetime>,
) -> SelectStatement {
	SelectStatement {
		what: vec![Value::RecordId(rid.clone()).into_literal()],
		fields: Fields::Value(Box::new(Selector {
			expr: Expr::Idiom(Idiom::field(field_name.to_string())),
			alias: None,
		})),
		only: true,
		version: version_to_expr(version),
		timeout: Expr::Literal(Literal::None),
		omit: vec![],
		with: None,
		cond: None,
		split: None,
		group: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		explain: None,
		tempfiles: false,
	}
}

/// Build a `SELECT * FROM <table>` statement with optional filtering,
/// ordering, pagination, and versioning.
///
/// Used by the table list query and relation field resolvers.
fn select_all_from_table(
	what: Expr,
	cond: Option<Cond>,
	order: Option<Ordering>,
	limit: Option<Limit>,
	start: Option<Start>,
	version: &Option<Datetime>,
) -> SelectStatement {
	SelectStatement {
		what: vec![what],
		fields: Fields::all(),
		order,
		cond,
		limit,
		start,
		version: version_to_expr(version),
		timeout: Expr::Literal(Literal::None),
		omit: vec![],
		only: false,
		with: None,
		split: None,
		group: None,
		fetch: None,
		explain: None,
		tempfiles: false,
	}
}

/// Execute a `SelectStatement` via `LogicalPlan` and return the result.
async fn execute_select(
	ds: &Datastore,
	sess: &Session,
	stmt: SelectStatement,
) -> Result<Value, GqlError> {
	let plan = LogicalPlan {
		expressions: vec![TopLevelExpr::Expr(Expr::Select(Box::new(stmt)))],
	};
	execute_plan(ds, sess, plan).await
}

// ---------------------------------------------------------------------------
// Nested object and array sub-field resolution
// ---------------------------------------------------------------------------

/// A recursively generated nested object node built from multipart field
/// definitions like `meta.author.name` or `tags.*.details.label`.
#[derive(Clone, Debug)]
struct NestedFieldNode {
	/// The SQL field name segment (e.g. `created_at`).
	sql_name: String,
	/// The GraphQL field name segment (e.g. `createdAt`).
	gql_name: String,
	/// The generated GraphQL type name for this nested object node.
	type_name: String,
	/// The declared kind for this field, when present.
	kind: Option<Kind>,
	/// Optional comment from the field definition.
	comment: Option<String>,
	/// Whether this node represents `array<object>` values.
	is_array: bool,
	/// Whether this node is nullable in GraphQL.
	optional: bool,
	/// Nested child fields keyed by SQL field name.
	children: IndexMap<String, NestedFieldNode>,
}

impl NestedFieldNode {
	fn new(table_name: &str, path: &[String], sql_name: &str) -> Self {
		let mut name_path = Vec::with_capacity(path.len() + 1);
		name_path.push(table_name);
		name_path.extend(path.iter().map(String::as_str));
		Self {
			sql_name: sql_name.to_string(),
			gql_name: naming::to_camel_case(sql_name),
			type_name: naming::nested_type_name(&name_path),
			kind: None,
			comment: None,
			is_array: false,
			optional: true,
			children: IndexMap::new(),
		}
	}

	fn has_children(&self) -> bool {
		!self.children.is_empty()
	}
}

fn apply_node_definition(node: &mut NestedFieldNode, kind: Option<Kind>, comment: Option<String>) {
	node.kind = kind.clone();
	node.comment = comment;
	if let Some(kind) = kind {
		node.optional = kind.can_be_none();
		if kind_is_array_of_objects(&kind) {
			node.is_array = true;
		}
	}
}

fn kind_is_array_of_objects(kind: &Kind) -> bool {
	match kind {
		Kind::Array(inner, _) => matches!(**inner, Kind::Object),
		Kind::Either(ks) => ks.iter().any(kind_is_array_of_objects),
		_ => false,
	}
}

fn object_kind_metadata(kind: Option<&Kind>, is_array: bool) -> Option<bool> {
	match kind {
		None => Some(true),
		Some(Kind::Object) if !is_array => Some(false),
		Some(Kind::Array(inner, _)) if is_array && matches!(**inner, Kind::Object) => Some(false),
		Some(Kind::Either(ks)) => {
			let has_none = ks.iter().any(|k| matches!(k, Kind::None | Kind::Null));
			let non_none =
				ks.iter().filter(|k| !matches!(k, Kind::None | Kind::Null)).collect::<Vec<_>>();
			if non_none.len() == 1 {
				object_kind_metadata(non_none.first().copied(), is_array).map(|_| has_none)
			} else {
				None
			}
		}
		_ => None,
	}
}

fn insert_nested_field_path(
	nodes: &mut IndexMap<String, NestedFieldNode>,
	table_name: &str,
	full_path: &[String],
	remaining_path: &[String],
	array_paths: &[Vec<String>],
	kind: Option<Kind>,
	comment: Option<String>,
) {
	let depth = full_path.len() - remaining_path.len() + 1;
	let current_path = &full_path[..depth];
	let sql_name = &remaining_path[0];
	let node = nodes
		.entry(sql_name.clone())
		.or_insert_with(|| NestedFieldNode::new(table_name, current_path, sql_name));

	if array_paths.iter().any(|array_path| array_path == current_path) {
		node.is_array = true;
	}

	if remaining_path.len() == 1 {
		apply_node_definition(node, kind, comment);
		return;
	}

	insert_nested_field_path(
		&mut node.children,
		table_name,
		full_path,
		&remaining_path[1..],
		array_paths,
		kind,
		comment,
	);
}

fn validate_nested_node(mut node: NestedFieldNode) -> Option<NestedFieldNode> {
	let mut validated_children = IndexMap::new();
	for (_, child) in node.children.into_iter() {
		if child.has_children() {
			if let Some(child) = validate_nested_node(child) {
				validated_children.insert(child.sql_name.clone(), child);
			}
		} else {
			validated_children.insert(child.sql_name.clone(), child);
		}
	}
	node.children = validated_children;

	if !node.has_children() {
		return None;
	}

	let optional = object_kind_metadata(node.kind.as_ref(), node.is_array)?;
	node.optional = optional;
	Some(node)
}

/// Analyze field definitions for a table and build a recursive nested object
/// tree from multipart field definitions.
///
/// This supports arbitrarily deep nesting for both object paths like
/// `profile.address.city` and array-object paths like `tags.*.meta.score`.
fn detect_nested_objects(
	table_name: &str,
	fds: &[FieldDefinition],
) -> HashMap<String, NestedFieldNode> {
	let mut roots = IndexMap::<String, NestedFieldNode>::new();

	for fd in fds {
		let parts = &fd.name.0;
		if parts.len() < 2 {
			continue;
		}

		let mut field_path = Vec::new();
		let mut array_paths = Vec::new();
		let mut supported = true;

		for part in parts {
			match part {
				Part::Field(name) => field_path.push(name.clone()),
				Part::All => {
					if field_path.is_empty() {
						supported = false;
						break;
					}
					array_paths.push(field_path.clone());
				}
				_ => {
					supported = false;
					break;
				}
			}
		}

		if !supported || field_path.len() < 2 {
			continue;
		}

		insert_nested_field_path(
			&mut roots,
			table_name,
			&field_path,
			&field_path,
			&array_paths,
			fd.field_kind.clone(),
			fd.comment.clone(),
		);
	}

	for fd in fds {
		if fd.name.0.len() != 1 {
			continue;
		}
		let sql_name = fd.name.to_sql();
		if let Some(node) = roots.get_mut(&sql_name) {
			apply_node_definition(node, fd.field_kind.clone(), fd.comment.clone());
		}
	}

	roots
		.into_iter()
		.filter_map(|(sql_name, node)| validate_nested_node(node).map(|node| (sql_name, node)))
		.collect()
}

/// Build a GraphQL object type for a nested object node and all of its nested
/// descendants.
fn make_nested_object_type(
	node: &NestedFieldNode,
	types: &mut Vec<Type>,
) -> Result<Object, GqlError> {
	for child in node.children.values() {
		if child.has_children() && !has_type(types, &child.type_name) {
			let child_object = make_nested_object_type(child, types)?;
			types.push(Type::Object(child_object));
		}
	}

	let mut obj = Object::new(&node.type_name).description(
		node.comment
			.clone()
			.unwrap_or_else(|| format!("Nested object type for the `{}` field.", node.gql_name)),
	);

	for child in node.children.values() {
		let mut field = if child.has_children() {
			let fd_type = if child.is_array {
				let connection_type_name =
					naming::field_connection_type_name(&node.type_name, &child.gql_name);
				let edge_type_name = naming::field_edge_type_name(&node.type_name, &child.gql_name);
				make_named_connection_types(
					&format!("{}.{}", node.type_name, child.gql_name),
					connection_type_name.clone(),
					edge_type_name,
					TypeRef::named_nn(&child.type_name),
					types,
				);
				TypeRef::named(&connection_type_name)
			} else {
				TypeRef::named(&child.type_name)
			};

			if child.is_array {
				Field::new(
					&child.gql_name,
					fd_type,
					make_nested_object_array_subfield_resolver(
						child.sql_name.clone(),
						child.type_name.clone(),
					),
				)
				.argument(
					InputValue::new("first", TypeRef::named(TypeRef::INT))
						.description("Return the first n items from this connection."),
				)
				.argument(
					InputValue::new("after", TypeRef::named(TypeRef::STRING))
						.description("Return items after the specified cursor."),
				)
				.argument(
					InputValue::new("last", TypeRef::named(TypeRef::INT))
						.description("Return the last n items from this connection."),
				)
				.argument(
					InputValue::new("before", TypeRef::named(TypeRef::STRING))
						.description("Return items before the specified cursor."),
				)
			} else {
				Field::new(
					&child.gql_name,
					fd_type,
					make_nested_object_subfield_resolver(child.sql_name.clone()),
				)
			}
		} else {
			let Some(kind) = child.kind.clone() else {
				continue;
			};
			let enum_scope = format!("{}_{}", node.type_name, child.sql_name);
			let list_item = list_item_kind(&kind);
			let fd_type = if let Some((item_kind, _optional)) = &list_item {
				let node_type = kind_to_type_with_enum_prefix(
					item_kind.clone(),
					types,
					false,
					Some(&enum_scope),
				)?;
				let connection_type_name =
					naming::field_connection_type_name(&node.type_name, &child.gql_name);
				let edge_type_name = naming::field_edge_type_name(&node.type_name, &child.gql_name);
				make_named_connection_types(
					&format!("{}.{}", node.type_name, child.gql_name),
					connection_type_name.clone(),
					edge_type_name,
					node_type,
					types,
				);
				TypeRef::named(&connection_type_name)
			} else {
				unwrap_type(kind_to_type_with_enum_prefix(
					kind.clone(),
					types,
					false,
					Some(&enum_scope),
				)?)
			};

			if let Some((item_kind, _optional)) = list_item {
				let node_type_name = unwrap_type(kind_to_type_with_enum_prefix(
					item_kind.clone(),
					types,
					false,
					Some(&enum_scope),
				)?)
				.to_string();
				Field::new(
					&child.gql_name,
					fd_type,
					make_nested_array_field_resolver(
						child.sql_name.clone(),
						item_kind,
						node_type_name,
						Some(enum_scope.clone()),
					),
				)
				.argument(
					InputValue::new("first", TypeRef::named(TypeRef::INT))
						.description("Return the first n items from this connection."),
				)
				.argument(
					InputValue::new("after", TypeRef::named(TypeRef::STRING))
						.description("Return items after the specified cursor."),
				)
				.argument(
					InputValue::new("last", TypeRef::named(TypeRef::INT))
						.description("Return the last n items from this connection."),
				)
				.argument(
					InputValue::new("before", TypeRef::named(TypeRef::STRING))
						.description("Return items before the specified cursor."),
				)
			} else {
				Field::new(
					&child.gql_name,
					fd_type,
					make_sub_field_resolver(
						child.sql_name.clone(),
						Some(kind.clone()),
						Some(enum_scope),
					),
				)
			}
		};

		field = field.description(
			child.comment.clone().unwrap_or_else(|| format!("The `{}` field.", child.gql_name)),
		);

		let is_non_null =
			child.kind.as_ref().map(|kind| !kind.can_be_none()).unwrap_or(!child.optional);
		if is_non_null {
			field = field.directive(semantic_non_null_directive());
		}
		obj = obj.field(field);
	}

	Ok(obj)
}

fn make_nested_object_subfield_resolver(
	field_name: impl Into<String>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	let field_name = field_name.into();
	move |ctx: ResolverContext| {
		let field_name = field_name.clone();
		FieldFuture::new(async move {
			let obj = ctx.parent_value.try_downcast_ref::<SurObject>()?;
			let val = obj.get(&field_name).cloned().unwrap_or(Value::None);
			resolve_nested_object_value(val, false)
		})
	}
}

fn make_nested_object_array_subfield_resolver(
	field_name: impl Into<String>,
	node_type_name: impl Into<String>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	let field_name = field_name.into();
	let node_type_name = node_type_name.into();
	move |ctx: ResolverContext| {
		let field_name = field_name.clone();
		let node_type_name = node_type_name.clone();
		FieldFuture::new(async move {
			let args = ctx.args.as_index_map();
			let connection_args = parse_connection_args(args)?;
			let obj = ctx.parent_value.try_downcast_ref::<SurObject>()?;
			let val = obj.get(&field_name).cloned().unwrap_or(Value::None);
			resolve_nested_object_array_connection_value(val, &connection_args, &node_type_name)
		})
	}
}

fn make_nested_array_field_resolver(
	field_name: impl Into<String>,
	item_kind: Kind,
	node_type_name: impl Into<String>,
	enum_scope: Option<String>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	let field_name = field_name.into();
	let node_type_name = node_type_name.into();
	move |ctx: ResolverContext| {
		let field_name = field_name.clone();
		let item_kind = item_kind.clone();
		let node_type_name = node_type_name.clone();
		let enum_scope = enum_scope.clone();
		FieldFuture::new(async move {
			let args = ctx.args.as_index_map();
			let connection_args = parse_connection_args(args)?;
			let obj = ctx.parent_value.try_downcast_ref::<SurObject>()?;
			let val = obj.get(&field_name).cloned().unwrap_or(Value::None);
			resolve_array_field_connection_value(
				val,
				&item_kind,
				&None,
				&connection_args,
				&node_type_name,
				enum_scope.as_deref(),
			)
		})
	}
}

/// Create a resolver for a sub-field within a nested object type.
///
/// The resolver downcasts the parent value to `SurObject` and extracts the
/// named field, converting it to the appropriate GraphQL value.
fn make_sub_field_resolver(
	field_name: String,
	kind: Option<Kind>,
	enum_scope: Option<String>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	move |ctx: ResolverContext| {
		let field_name = field_name.clone();
		let field_kind = kind.clone();
		let enum_scope = enum_scope.clone();
		FieldFuture::new(async move {
			let obj = ctx.parent_value.try_downcast_ref::<SurObject>()?;

			match obj.get(&field_name) {
				Some(val) => match val {
					Value::None | Value::Null => Ok(None),
					Value::RecordId(rid) => {
						// Record-link: store as owned_any for dereferencing
						let field_val = FieldValue::owned_any(VersionedRecord {
							rid: rid.clone(),
							version: None,
						});
						let field_val = match field_kind {
							Some(Kind::Record(ref ts)) if ts.is_empty() || ts.len() > 1 => {
								field_val.with_type(naming::table_type_name(rid.table.as_str()))
							}
							_ => field_val,
						};
						Ok(Some(field_val))
					}
					Value::Geometry(g) => {
						let type_name = geometry_gql_type_name(g);
						let field_val = FieldValue::owned_any(g.clone());
						let field_val = match &field_kind {
							Some(Kind::Geometry(ks)) if ks.is_empty() || ks.len() > 1 => {
								field_val.with_type(type_name)
							}
							_ => field_val,
						};
						Ok(Some(field_val))
					}
					v => {
						let gql_val = sql_value_to_gql_value_with_kind(
							v.clone(),
							field_kind.as_ref(),
							enum_scope.as_deref(),
						)
						.map_err(async_graphql::Error::from)?;
						Ok(Some(FieldValue::value(gql_val)))
					}
				},
				None => Ok(None),
			}
		})
	}
}

/// Create a resolver for a parent field that is a nested object (`TYPE object`
/// with sub-fields). Returns the `SurObject` as `owned_any` so sub-field
/// resolvers can extract values from it.
fn make_nested_object_field_resolver(
	fd_name: impl Into<String>,
	is_array: bool,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	let fd_name = fd_name.into();
	move |ctx: ResolverContext| {
		let fd_name = fd_name.clone();
		FieldFuture::new(async move {
			// ── Fast path: extract nested object from CachedRecord ──
			if let Ok(cached) = ctx.parent_value.try_downcast_ref::<CachedRecord>() {
				let val = cached.data.get(&fd_name).cloned().unwrap_or(Value::None);
				return resolve_nested_object_value(val, is_array);
			}

			// ── Slow path: fetch via database query ──
			let ds = ctx.data::<Arc<Datastore>>()?;
			let sess = ctx.data::<Arc<Session>>()?;

			// Extract record ID and optional version
			let (rid, version) = match ctx.parent_value.try_downcast_ref::<VersionedRecord>() {
				Ok(vr) => (vr.rid.clone(), vr.version.clone()),
				Err(_) => {
					let rid = ctx.parent_value.try_downcast_ref::<RecordId>()?;
					(rid.clone(), None)
				}
			};

			// Build SELECT VALUE <field> FROM ONLY <record_id>
			let stmt = select_field_from_record(&rid, &fd_name, &version);
			let val = execute_select(ds, sess, stmt).await?;
			resolve_nested_object_value(val, is_array)
		})
	}
}

fn make_nested_object_array_field_resolver(
	fd_name: impl Into<String>,
	node_type_name: impl Into<String>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	let fd_name = fd_name.into();
	let node_type_name = node_type_name.into();
	move |ctx: ResolverContext| {
		let fd_name = fd_name.clone();
		let node_type_name = node_type_name.clone();
		FieldFuture::new(async move {
			let args = ctx.args.as_index_map();
			let connection_args = parse_connection_args(args)?;

			if let Ok(cached) = ctx.parent_value.try_downcast_ref::<CachedRecord>() {
				let val = cached.data.get(&fd_name).cloned().unwrap_or(Value::None);
				return resolve_nested_object_array_connection_value(
					val,
					&connection_args,
					&node_type_name,
				);
			}

			let ds = ctx.data::<Arc<Datastore>>()?;
			let sess = ctx.data::<Arc<Session>>()?;

			let (rid, version) = match ctx.parent_value.try_downcast_ref::<VersionedRecord>() {
				Ok(vr) => (vr.rid.clone(), vr.version.clone()),
				Err(_) => {
					let rid = ctx.parent_value.try_downcast_ref::<RecordId>()?;
					(rid.clone(), None)
				}
			};

			let stmt = select_field_from_record(&rid, &fd_name, &version);
			let val = execute_select(ds, sess, stmt).await?;
			resolve_nested_object_array_connection_value(val, &connection_args, &node_type_name)
		})
	}
}

/// Convert a nested object/array-of-object value to a GraphQL `FieldValue`.
///
/// For arrays, each `Value::Object` element becomes a `FieldValue::owned_any(SurObject(..))`.
/// For plain objects, the `SurObject` is returned directly.
fn resolve_nested_object_value(
	val: Value,
	is_array: bool,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	if is_array {
		match val {
			Value::Array(arr) => {
				let items: Vec<FieldValue> = arr
					.0
					.into_iter()
					.filter_map(|v| match v {
						Value::Object(obj) => Some(FieldValue::owned_any(obj)),
						_ => None,
					})
					.collect();
				Ok(Some(FieldValue::list(items)))
			}
			Value::None | Value::Null => Ok(None),
			_ => Ok(None),
		}
	} else {
		match val {
			Value::Object(obj) => Ok(Some(FieldValue::owned_any(obj))),
			Value::None | Value::Null => Ok(None),
			_ => {
				let out = sql_value_to_gql_value(val).map_err(async_graphql::Error::from)?;
				Ok(Some(FieldValue::value(out)))
			}
		}
	}
}

fn resolve_nested_object_array_connection_value(
	val: Value,
	args: &ConnectionArgs,
	_node_type_name: &str,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	match val {
		Value::Array(arr) => {
			let nodes = arr
				.0
				.into_iter()
				.enumerate()
				.filter_map(|(index, value)| match value {
					Value::Object(object) => Some((
						index.to_string(),
						ConnectionNode::Object {
							object,
							runtime_type_name: None,
						},
					)),
					Value::None | Value::Null => None,
					_ => None,
				})
				.collect::<Vec<_>>();
			build_node_connection_value(nodes, args)
		}
		Value::None | Value::Null => Ok(None),
		_ => Ok(None),
	}
}

/// Derive the GraphQL filter input type name for a table (e.g. `PersonFilterInput`).
pub(crate) fn filter_name_from_table(tb_name: impl Display) -> String {
	naming::filter_input_name(&tb_name.to_string())
}

// ---------------------------------------------------------------------------
// Result conversion helpers
// ---------------------------------------------------------------------------

/// Convert an array of record objects to a list of [`CachedRecord`] field values.
///
/// Each `Value::Object` in the array is wrapped in a `CachedRecord` so that
/// field resolvers can extract values directly from memory. Used by table list
/// queries, relation field resolvers, and bulk mutation results.
fn objects_to_cached_records(
	arr: SurArray,
	version: Option<Datetime>,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	let out: Result<Vec<FieldValue>, GqlError> = arr
		.0
		.into_iter()
		.map(|v| match v {
			Value::Object(obj) => {
				let rid = match obj.get("id") {
					Some(Value::RecordId(rid)) => rid.clone(),
					_ => {
						error!("Object missing 'id' field or id is not a RecordId: {obj:?}");
						return Err(internal_error("Record missing 'id' field"));
					}
				};
				Ok(FieldValue::owned_any(CachedRecord {
					rid,
					version: version.clone(),
					data: obj,
				}))
			}
			_ => {
				error!("Expected object in result, found: {v:?}");
				Err(internal_error("Expected object in result"))
			}
		})
		.collect();
	match out {
		Ok(l) => Ok(Some(FieldValue::list(l))),
		Err(e) => Err(e.into()),
	}
}

fn build_connection_value(
	items: &[CachedRecord],
	args: &ConnectionArgs,
	_node_type_name: &str,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	let window = paginate(items, args, |record| record_id_to_raw(&record.rid))
		.map_err(async_graphql::Error::from)?;

	let edges = window
		.selected
		.into_iter()
		.map(|record| ConnectionEdge {
			cursor: super::cursor::encode_cursor(&record_id_to_raw(&record.rid)),
			node: ConnectionNode::Record {
				record: record.clone(),
				runtime_type_name: None,
			},
			relation_record: None,
		})
		.collect();

	Ok(Some(FieldValue::owned_any(Connection {
		edges,
		page_info: window.page_info,
		total_count: window.total_count,
	})))
}

fn build_node_connection_value(
	items: Vec<(String, ConnectionNode)>,
	args: &ConnectionArgs,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	let window =
		paginate(&items, args, |(cursor, _)| cursor.clone()).map_err(async_graphql::Error::from)?;

	let edges = window
		.selected
		.into_iter()
		.map(|(cursor, node)| ConnectionEdge {
			cursor: super::cursor::encode_cursor(cursor),
			node: node.clone(),
			relation_record: None,
		})
		.collect();

	Ok(Some(FieldValue::owned_any(Connection {
		edges,
		page_info: window.page_info,
		total_count: window.total_count,
	})))
}

fn objects_to_connection(
	arr: SurArray,
	version: Option<Datetime>,
	args: &ConnectionArgs,
	node_type_name: &str,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	let out: Result<Vec<CachedRecord>, GqlError> = arr
		.0
		.into_iter()
		.map(|v| match v {
			Value::Object(obj) => {
				let rid = match obj.get("id") {
					Some(Value::RecordId(rid)) => rid.clone(),
					_ => return Err(internal_error("Record missing 'id' field")),
				};
				Ok(CachedRecord {
					rid,
					version: version.clone(),
					data: obj,
				})
			}
			_ => Err(internal_error("Expected object in result")),
		})
		.collect();

	build_connection_value(&out?, args, node_type_name)
}

fn relation_objects_to_connection(
	arr: SurArray,
	version: Option<Datetime>,
	args: &ConnectionArgs,
	node_type_name: &str,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	let out: Result<Vec<ConnectionEdge>, GqlError> = arr
		.0
		.into_iter()
		.map(|value| match value {
			Value::Object(obj) => {
				let rid = match obj.get("id") {
					Some(Value::RecordId(rid)) => rid.clone(),
					_ => return Err(internal_error("Relation record missing 'id' field")),
				};
				let out_rid = match obj.get("out") {
					Some(Value::RecordId(rid)) => rid.clone(),
					_ => return Err(internal_error("Relation record missing 'out' field")),
				};
				let relation_record = CachedRecord {
					rid: rid.clone(),
					version: version.clone(),
					data: obj,
				};
				Ok(ConnectionEdge {
					cursor: super::cursor::encode_cursor(&record_id_to_raw(&rid)),
					node: ConnectionNode::VersionedRecord {
						record: VersionedRecord {
							rid: out_rid.clone(),
							version: version.clone(),
						},
						runtime_type_name: (node_type_name == "record")
							.then(|| naming::table_type_name(out_rid.table.as_str())),
					},
					relation_record: Some(relation_record),
				})
			}
			_ => Err(internal_error("Expected object in relation result")),
		})
		.collect();

	make_relation_connection_value(&out?, args)
}

fn non_null_list_of_nullable(type_name: &str) -> TypeRef {
	TypeRef::NonNull(Box::new(TypeRef::List(Box::new(TypeRef::named(type_name)))))
}

fn list_item_kind(kind: &Kind) -> Option<(Kind, bool)> {
	match kind {
		Kind::Array(inner, _) => Some((inner.as_ref().clone(), false)),
		Kind::Either(ks) => {
			let has_none = ks.iter().any(|k| matches!(k, Kind::None | Kind::Null));
			let non_none: Vec<&Kind> =
				ks.iter().filter(|k| !matches!(k, Kind::None | Kind::Null)).collect();
			if non_none.len() == 1
				&& let Kind::Array(inner, _) = non_none[0]
			{
				Some((inner.as_ref().clone(), has_none))
			} else {
				None
			}
		}
		_ => None,
	}
}

fn connection_description(name: &str) -> String {
	format!("A Relay-style connection for `{name}` records.")
}

fn edge_description(name: &str) -> String {
	format!("An edge in the generated `{name}` connection.")
}

fn has_type(types: &[Type], type_name: &str) -> bool {
	types.iter().any(|ty| match ty {
		Type::Scalar(scalar) => scalar.type_name() == type_name,
		Type::Object(object) => object.type_name() == type_name,
		Type::InputObject(input_object) => input_object.type_name() == type_name,
		Type::Enum(enum_type) => enum_type.type_name() == type_name,
		Type::Interface(interface) => interface.type_name() == type_name,
		Type::Union(union_type) => union_type.type_name() == type_name,
		Type::Subscription(subscription) => subscription.type_name() == type_name,
		Type::Upload => type_name == "Upload",
	})
}

fn make_named_connection_types(
	description_name: &str,
	connection_type_name: String,
	edge_type_name: String,
	node_type: TypeRef,
	types: &mut Vec<Type>,
) -> (String, String) {
	let node_list_type_name = unwrap_type(node_type.clone()).to_string();
	let node_field_type = unwrap_type(node_type);

	let edge = Object::new(&edge_type_name)
		.description(edge_description(description_name))
		.field(
			Field::new("cursor", TypeRef::named_nn(TypeRef::STRING), |ctx| {
				FieldFuture::new(async move {
					let edge = ctx.parent_value.try_downcast_ref::<ConnectionEdge>()?;
					Ok(Some(FieldValue::value(edge.cursor.clone())))
				})
			})
			.description("An opaque cursor for this edge.")
			.directive(semantic_non_null_directive()),
		)
		.field(
			Field::new("node", node_field_type, |ctx| {
				FieldFuture::new(async move {
					let edge = ctx.parent_value.try_downcast_ref::<ConnectionEdge>()?;
					Ok(Some(edge.node.to_field_value()))
				})
			})
			.description("The item exposed at the end of this edge.")
			.directive(semantic_non_null_directive()),
		);

	let connection = Object::new(&connection_type_name)
		.description(connection_description(description_name))
		.field(
			Field::new("edges", TypeRef::named_nn_list_nn(&edge_type_name), |ctx| {
				FieldFuture::new(async move {
					let connection = ctx.parent_value.try_downcast_ref::<Connection>()?;
					let edges = connection
						.edges
						.iter()
						.cloned()
						.map(FieldValue::owned_any)
						.collect::<Vec<_>>();
					Ok(Some(FieldValue::list(edges)))
				})
			})
			.description("The edges in this page of the connection.")
			.directive(semantic_non_null_directive()),
		)
		.field(
			Field::new("nodes", non_null_list_of_nullable(&node_list_type_name), |ctx| {
				FieldFuture::new(async move {
					let connection = ctx.parent_value.try_downcast_ref::<Connection>()?;
					let nodes = connection
						.edges
						.iter()
						.map(|edge| edge.node.to_field_value())
						.collect::<Vec<_>>();
					Ok(Some(FieldValue::list(nodes)))
				})
			})
			.description("The nodes in this page of the connection.")
			.directive(semantic_non_null_directive()),
		)
		.field(
			Field::new("pageInfo", TypeRef::named_nn("PageInfo"), |ctx| {
				FieldFuture::new(async move {
					let connection = ctx.parent_value.try_downcast_ref::<Connection>()?;
					Ok(Some(FieldValue::owned_any(connection.page_info.clone())))
				})
			})
			.description("Pagination metadata for this connection.")
			.directive(semantic_non_null_directive()),
		)
		.field(
			Field::new("totalCount", TypeRef::named_nn(TypeRef::INT), |ctx| {
				FieldFuture::new(async move {
					let connection = ctx.parent_value.try_downcast_ref::<Connection>()?;
					Ok(Some(FieldValue::value(connection.total_count)))
				})
			})
			.description("The total number of matching records before pagination is applied.")
			.directive(semantic_non_null_directive()),
		);

	if !has_type(types, &edge_type_name) {
		types.push(Type::Object(edge));
	}
	if !has_type(types, &connection_type_name) {
		types.push(Type::Object(connection));
	}

	(connection_type_name, edge_type_name)
}

fn make_connection_types(
	base_name: &str,
	node_type: TypeRef,
	types: &mut Vec<Type>,
) -> (String, String) {
	make_named_connection_types(
		base_name,
		naming::connection_type_name(base_name),
		naming::edge_type_name(base_name),
		node_type,
		types,
	)
}

fn relation_edge_record<'a>(
	ctx: &'a ResolverContext<'_>,
) -> Result<&'a CachedRecord, async_graphql::Error> {
	let edge = ctx.parent_value.try_downcast_ref::<ConnectionEdge>()?;
	edge.relation_record
		.as_ref()
		.ok_or_else(|| internal_error("Missing relation metadata on connection edge").into())
}

fn make_relation_edge_field_resolver(
	fd_name: impl Into<String>,
	kind: Option<Kind>,
	enum_scope: Option<String>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	let fd_name = fd_name.into();
	move |ctx: ResolverContext| {
		let fd_name = fd_name.clone();
		let field_kind = kind.clone();
		let enum_scope = enum_scope.clone();
		FieldFuture::new(async move {
			let record = relation_edge_record(&ctx)?;
			resolve_field_from_cached_record(
				&ctx,
				record,
				&fd_name,
				&field_kind,
				enum_scope.as_deref(),
			)
			.await
		})
	}
}

fn make_relation_edge_nested_object_field_resolver(
	fd_name: impl Into<String>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	let fd_name = fd_name.into();
	move |ctx: ResolverContext| {
		let fd_name = fd_name.clone();
		FieldFuture::new(async move {
			let record = relation_edge_record(&ctx)?;
			let val = record.data.get(&fd_name).cloned().unwrap_or(Value::None);
			resolve_nested_object_value(val, false)
		})
	}
}

fn make_relation_edge_nested_object_array_field_resolver(
	fd_name: impl Into<String>,
	node_type_name: impl Into<String>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	let fd_name = fd_name.into();
	let node_type_name = node_type_name.into();
	move |ctx: ResolverContext| {
		let fd_name = fd_name.clone();
		let node_type_name = node_type_name.clone();
		FieldFuture::new(async move {
			let args = ctx.args.as_index_map();
			let connection_args = parse_connection_args(args)?;
			let record = relation_edge_record(&ctx)?;
			let val = record.data.get(&fd_name).cloned().unwrap_or(Value::None);
			resolve_nested_object_array_connection_value(val, &connection_args, &node_type_name)
		})
	}
}

fn make_relation_edge_array_field_resolver(
	fd_name: impl Into<String>,
	item_kind: Kind,
	node_type_name: impl Into<String>,
	enum_scope: Option<String>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	let fd_name = fd_name.into();
	let node_type_name = node_type_name.into();
	move |ctx: ResolverContext| {
		let fd_name = fd_name.clone();
		let item_kind = item_kind.clone();
		let node_type_name = node_type_name.clone();
		let enum_scope = enum_scope.clone();
		FieldFuture::new(async move {
			let args = ctx.args.as_index_map();
			let connection_args = parse_connection_args(args)?;
			let record = relation_edge_record(&ctx)?;
			let val = record.data.get(&fd_name).cloned().unwrap_or(Value::None);
			resolve_array_field_connection_value(
				val,
				&item_kind,
				&record.version,
				&connection_args,
				&node_type_name,
				enum_scope.as_deref(),
			)
		})
	}
}

fn make_relation_connection_value(
	items: &[ConnectionEdge],
	args: &ConnectionArgs,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	let window = paginate(items, args, |edge| {
		let record = edge.relation_record.as_ref().expect("relation edges always carry metadata");
		record_id_to_raw(&record.rid)
	})
	.map_err(async_graphql::Error::from)?;

	let edges = window.selected.into_iter().cloned().collect();

	Ok(Some(FieldValue::owned_any(Connection {
		edges,
		page_info: window.page_info,
		total_count: window.total_count,
	})))
}

fn make_relation_connection_types(
	relation: &RelationTypeContext,
	types: &mut Vec<Type>,
) -> Result<(), GqlError> {
	let node_type = TypeRef::named_nn(&relation.node_type_name);
	let node_list_type_name = unwrap_type(node_type.clone()).to_string();
	let node_field_type = unwrap_type(node_type);

	let mut edge = Object::new(&relation.edge_type_name)
		.description(edge_description(&relation.field_name))
		.field(
			Field::new("cursor", TypeRef::named_nn(TypeRef::STRING), |ctx| {
				FieldFuture::new(async move {
					let edge = ctx.parent_value.try_downcast_ref::<ConnectionEdge>()?;
					Ok(Some(FieldValue::value(edge.cursor.clone())))
				})
			})
			.description("An opaque cursor for this edge.")
			.directive(semantic_non_null_directive()),
		)
		.field(
			Field::new("node", node_field_type, |ctx| {
				FieldFuture::new(async move {
					let edge = ctx.parent_value.try_downcast_ref::<ConnectionEdge>()?;
					Ok(Some(edge.node.to_field_value()))
				})
			})
			.description("The target node connected by this relation.")
			.directive(semantic_non_null_directive()),
		)
		.field(
			Field::new("id", TypeRef::named_nn(TypeRef::ID), |ctx| {
				FieldFuture::new(async move {
					let record = relation_edge_record(&ctx)?;
					Ok(Some(FieldValue::value(record_id_to_raw(&record.rid))))
				})
			})
			.description("The relation record id.")
			.directive(semantic_non_null_directive()),
		);

	let nested_objects = detect_nested_objects(&relation.field_name, &relation.fds);
	for fd in relation.fds.iter() {
		let Some(ref kind) = fd.field_kind else {
			continue;
		};
		if fd.name.is_id() || fd.name.0.len() > 1 {
			continue;
		}

		let sql_field_name = fd.name.to_sql();
		if matches!(sql_field_name.as_str(), "in" | "out") {
			continue;
		}

		let gql_field_name = naming::to_camel_case(&sql_field_name);
		if let Some(nested) = nested_objects.get(sql_field_name.as_str()) {
			let nested_type = make_nested_object_type(nested, types)?;
			if !has_type(types, &nested.type_name) {
				types.push(Type::Object(nested_type));
			}

			let fd_type = if nested.is_array {
				let connection_type_name =
					naming::field_connection_type_name(&relation.edge_type_name, &gql_field_name);
				let edge_type_name =
					naming::field_edge_type_name(&relation.edge_type_name, &gql_field_name);
				make_named_connection_types(
					&format!("{}.{}", relation.edge_type_name, gql_field_name),
					connection_type_name.clone(),
					edge_type_name,
					TypeRef::named_nn(&nested.type_name),
					types,
				);
				TypeRef::named(&connection_type_name)
			} else {
				TypeRef::named(&nested.type_name)
			};

			let mut field = if nested.is_array {
				Field::new(
					&gql_field_name,
					fd_type,
					make_relation_edge_nested_object_array_field_resolver(
						sql_field_name.clone(),
						nested.type_name.clone(),
					),
				)
			} else {
				Field::new(
					&gql_field_name,
					fd_type,
					make_relation_edge_nested_object_field_resolver(sql_field_name.clone()),
				)
			};
			field = field.description(
				fd.comment
					.clone()
					.unwrap_or_else(|| format!("The `{gql_field_name}` relation metadata field.")),
			);
			if nested.is_array {
				field = field
					.argument(
						InputValue::new("first", TypeRef::named(TypeRef::INT))
							.description("Return the first n items from this connection."),
					)
					.argument(
						InputValue::new("after", TypeRef::named(TypeRef::STRING))
							.description("Return items after the specified cursor."),
					)
					.argument(
						InputValue::new("last", TypeRef::named(TypeRef::INT))
							.description("Return the last n items from this connection."),
					)
					.argument(
						InputValue::new("before", TypeRef::named(TypeRef::STRING))
							.description("Return items before the specified cursor."),
					);
			}
			if !nested.optional {
				field = field.directive(semantic_non_null_directive());
			}
			edge = edge.field(field);
			continue;
		}

		let enum_scope = format!("{}_{}", relation.relation_table_name.as_str(), sql_field_name);
		let list_item = list_item_kind(kind);
		let fd_type = if let Some((item_kind, _optional)) = &list_item {
			let node_type =
				kind_to_type_with_enum_prefix(item_kind.clone(), types, false, Some(&enum_scope))?;
			let connection_type_name =
				naming::field_connection_type_name(&relation.edge_type_name, &gql_field_name);
			let edge_type_name =
				naming::field_edge_type_name(&relation.edge_type_name, &gql_field_name);
			make_named_connection_types(
				&format!("{}.{}", relation.edge_type_name, gql_field_name),
				connection_type_name.clone(),
				edge_type_name,
				node_type,
				types,
			);
			TypeRef::named(&connection_type_name)
		} else {
			unwrap_type(kind_to_type_with_enum_prefix(
				kind.clone(),
				types,
				false,
				Some(&enum_scope),
			)?)
		};

		let mut field = if let Some((item_kind, _optional)) = list_item {
			let node_type_name = unwrap_type(kind_to_type_with_enum_prefix(
				item_kind.clone(),
				types,
				false,
				Some(&enum_scope),
			)?)
			.to_string();
			Field::new(
				&gql_field_name,
				fd_type,
				make_relation_edge_array_field_resolver(
					sql_field_name,
					item_kind,
					node_type_name,
					Some(enum_scope),
				),
			)
			.argument(
				InputValue::new("first", TypeRef::named(TypeRef::INT))
					.description("Return the first n items from this connection."),
			)
			.argument(
				InputValue::new("after", TypeRef::named(TypeRef::STRING))
					.description("Return items after the specified cursor."),
			)
			.argument(
				InputValue::new("last", TypeRef::named(TypeRef::INT))
					.description("Return the last n items from this connection."),
			)
			.argument(
				InputValue::new("before", TypeRef::named(TypeRef::STRING))
					.description("Return items before the specified cursor."),
			)
		} else {
			Field::new(
				&gql_field_name,
				fd_type,
				make_relation_edge_field_resolver(
					sql_field_name,
					fd.field_kind.clone(),
					Some(enum_scope),
				),
			)
		};
		field = field.description(
			fd.comment
				.clone()
				.unwrap_or_else(|| format!("The `{gql_field_name}` relation metadata field.")),
		);
		if !kind.can_be_none() {
			field = field.directive(semantic_non_null_directive());
		}
		edge = edge.field(field);
	}

	let connection = Object::new(&relation.connection_type_name)
		.description(connection_description(&relation.field_name))
		.field(
			Field::new("edges", TypeRef::named_nn_list_nn(&relation.edge_type_name), |ctx| {
				FieldFuture::new(async move {
					let connection = ctx.parent_value.try_downcast_ref::<Connection>()?;
					let edges = connection
						.edges
						.iter()
						.cloned()
						.map(FieldValue::owned_any)
						.collect::<Vec<_>>();
					Ok(Some(FieldValue::list(edges)))
				})
			})
			.description("The edges in this page of the connection.")
			.directive(semantic_non_null_directive()),
		)
		.field(
			Field::new("nodes", non_null_list_of_nullable(&node_list_type_name), |ctx| {
				FieldFuture::new(async move {
					let connection = ctx.parent_value.try_downcast_ref::<Connection>()?;
					let nodes = connection
						.edges
						.iter()
						.map(|edge| edge.node.to_field_value())
						.collect::<Vec<_>>();
					Ok(Some(FieldValue::list(nodes)))
				})
			})
			.description("The nodes in this page of the connection.")
			.directive(semantic_non_null_directive()),
		)
		.field(
			Field::new("pageInfo", TypeRef::named_nn("PageInfo"), |ctx| {
				FieldFuture::new(async move {
					let connection = ctx.parent_value.try_downcast_ref::<Connection>()?;
					Ok(Some(FieldValue::owned_any(connection.page_info.clone())))
				})
			})
			.description("Pagination metadata for this connection.")
			.directive(semantic_non_null_directive()),
		)
		.field(
			Field::new("totalCount", TypeRef::named_nn(TypeRef::INT), |ctx| {
				FieldFuture::new(async move {
					let connection = ctx.parent_value.try_downcast_ref::<Connection>()?;
					Ok(Some(FieldValue::value(connection.total_count)))
				})
			})
			.description("The total number of matching relations before pagination is applied.")
			.directive(semantic_non_null_directive()),
		);

	if !has_type(types, &relation.edge_type_name) {
		types.push(Type::Object(edge));
	}
	if !has_type(types, &relation.connection_type_name) {
		types.push(Type::Object(connection));
	}

	Ok(())
}

// ---------------------------------------------------------------------------
// Query root field builders
// ---------------------------------------------------------------------------

/// Build the query field for listing records of a table.
///
/// Creates a Relay connection field like
/// `people(first: Int, after: String, orderBy: PersonOrder, filterBy: PersonFilterInput,
/// version: Datetime): PersonConnection!`.
fn make_table_list_field(
	tb: &TableDefinition,
	fds: Arc<[FieldDefinition]>,
	kvs: Arc<Datastore>,
	connection_type_name: String,
) -> Field {
	let tb_name = tb.name.clone();
	let tb_name_str = tb_name.clone().into_string();
	let table_order_name = naming::order_input_name(&tb_name_str);
	let table_filter_name = filter_name_from_table(&tb_name);
	let node_type_name = naming::table_type_name(&tb_name_str);
	let query_field_name = naming::plural_query_name(&tb_name_str);

	Field::new(&query_field_name, TypeRef::named_nn(&connection_type_name), move |ctx| {
		let tb_name = tb_name.clone();
		let fds = fds.clone();
		let kvs = kvs.clone();
		let node_type_name = node_type_name.clone();
		FieldFuture::new(async move {
			let sess = ctx.data::<Arc<Session>>()?;
			let args = ctx.args.as_index_map();
			trace!("received request with args: {args:?}");

			let connection_args = parse_connection_args(args)?;
			let version = parse_version_arg(args)?;
			let order = parse_order_arg(args, &fds)?;
			let tb_name_str_ref = tb_name.as_str();
			let cond = parse_filter_arg(args, &fds, tb_name_str_ref)?;

			trace!("parsed order: {order:?}");
			trace!("parsed filter: {cond:?}");

			let stmt =
				select_all_from_table(Expr::Table(tb_name), cond, order, None, None, &version);
			let res = execute_select(&kvs, sess, stmt).await?;

			match res {
				Value::Array(a) => {
					objects_to_connection(a, version, &connection_args, &node_type_name)
				}
				v => {
					error!("Found top level value, in result which should be array: {v:?}");
					Err(internal_error("Unexpected result type from table query").into())
				}
			}
		})
	})
	.description(if let Some(c) = &tb.comment {
		c.clone()
	} else {
		format!("Query a paginated list of `{}` records.", naming::table_type_name(&tb_name_str))
	})
	.argument(
		InputValue::new("first", TypeRef::named(TypeRef::INT))
			.description("Return the first n records from the connection."),
	)
	.argument(
		InputValue::new("after", TypeRef::named(TypeRef::STRING))
			.description("Return records after the specified cursor."),
	)
	.argument(
		InputValue::new("last", TypeRef::named(TypeRef::INT))
			.description("Return the last n records from the connection."),
	)
	.argument(
		InputValue::new("before", TypeRef::named(TypeRef::STRING))
			.description("Return records before the specified cursor."),
	)
	.argument(
		InputValue::new("orderBy", TypeRef::named(&table_order_name))
			.description("Specify how the connection should be ordered."),
	)
	.argument(
		InputValue::new("filterBy", TypeRef::named(&table_filter_name))
			.description("Filter the records returned by this connection."),
	)
	.argument(
		InputValue::new("version", TypeRef::named("Datetime"))
			.description("Query the connection at a specific point in time."),
	)
}

/// Build the singular root query field for fetching a single record by ID.
///
/// Returns the record as a [`CachedRecord`] for efficient field resolution,
/// or `null` if the record does not exist.
fn make_table_get_field(tb: &TableDefinition, kvs: Arc<Datastore>) -> Field {
	let tb_name = tb.name.clone();
	let tb_name_str = tb_name.clone().into_string();
	let gql_field_name = naming::singular_query_name(&tb_name_str);
	let gql_type_name = naming::table_type_name(&tb_name_str);

	Field::new(&gql_field_name, TypeRef::named(&gql_type_name), move |ctx| {
		let tb_name = tb_name.clone();
		let kvs = kvs.clone();
		FieldFuture::new(async move {
			let sess = ctx.data::<Arc<Session>>()?;
			let args = ctx.args.as_index_map();
			let id = match args.get("id").and_then(GqlValueUtils::as_string) {
				Some(i) => i,
				None => {
					return Err(internal_error("Schema validation failed: No id found").into());
				}
			};
			let version = parse_version_arg(args)?;

			let record_id: RecordId = match crate::syn::record_id(&id) {
				Ok(x) => x.into(),
				Err(_) => RecordId::new(tb_name, id),
			};

			let stmt = select_all_from_record(&record_id, &version);
			let res = execute_select(&kvs, sess, stmt).await?;

			match res {
				Value::Object(obj) => {
					let rid = match obj.get("id") {
						Some(Value::RecordId(rid)) => rid.clone(),
						_ => return Ok(None),
					};
					Ok(Some(FieldValue::owned_any(CachedRecord {
						rid,
						version,
						data: obj,
					})))
				}
				_ => Ok(None),
			}
		})
	})
	.description(if let Some(c) = &tb.comment {
		c.clone()
	} else {
		format!("Fetch a single `{}` record by record id.", naming::table_type_name(&tb_name_str))
	})
	.argument(InputValue::new("id", TypeRef::named_nn(TypeRef::ID)))
	.argument(InputValue::new("version", TypeRef::named("Datetime")))
}

// ---------------------------------------------------------------------------
// Table type system builders
// ---------------------------------------------------------------------------

/// The GraphQL types generated for a single table.
///
/// Returned by [`build_table_type`] for registration on the schema.
struct TableGraphQLTypes {
	/// The table's Object type (e.g., `Person`).
	ty_obj: Object,
	/// Enum of fields that can be ordered by (e.g., `PersonOrderField`).
	orderable: Enum,
	/// The order input object (e.g., `PersonOrder`).
	order: InputObject,
	/// The filter input object (e.g., `PersonFilterInput`).
	filter: InputObject,
	/// The Relay connection type name for the table.
	connection_type_name: String,
}

#[derive(Clone)]
struct RelationTypeContext {
	field_name: String,
	relation_table_name: TableName,
	connection_type_name: String,
	edge_type_name: String,
	order_name: String,
	filter_name: String,
	node_type_name: String,
	fds: Arc<[FieldDefinition]>,
}

fn relation_node_type_name(
	rel: &RelationInfo,
	exposed_table_names: &HashSet<String>,
) -> Option<String> {
	let visible_targets = rel
		.to_tables
		.iter()
		.filter(|table_name| exposed_table_names.contains(*table_name))
		.collect::<Vec<_>>();

	match visible_targets.len() {
		0 => None,
		1 => Some(naming::table_type_name(visible_targets[0])),
		_ => Some("record".to_string()),
	}
}

fn build_relation_type_context(
	rel: &RelationInfo,
	fds: Arc<[FieldDefinition]>,
	exposed_table_names: &HashSet<String>,
	types: &mut Vec<Type>,
) -> Result<Option<RelationTypeContext>, GqlError> {
	let Some(node_type_name) = relation_node_type_name(rel, exposed_table_names) else {
		return Ok(None);
	};

	let relation_name = rel.table_name.clone().into_string();
	let field_name = naming::to_camel_case(&relation_name);
	let relation_ctx = RelationTypeContext {
		field_name: field_name.clone(),
		relation_table_name: rel.table_name.clone(),
		connection_type_name: naming::connection_type_name(&relation_name),
		edge_type_name: naming::edge_type_name(&relation_name),
		order_name: naming::order_input_name(&relation_name),
		filter_name: naming::filter_input_name(&relation_name),
		node_type_name,
		fds: fds.clone(),
	};

	let table_orderable_name = naming::order_field_enum_name(&relation_name);
	let mut orderable = Enum::new(&table_orderable_name)
		.item(EnumItem::new("ID").description("Order results by relation id."))
		.description(format!(
			"Fields that can be used to order `{}` connections.",
			relation_ctx.edge_type_name
		));

	let mut filter = InputObject::new(&relation_ctx.filter_name)
		.description(format!("Filter input for `{}` connections.", relation_ctx.field_name))
		.field(
			InputValue::new("id", TypeRef::named("IdFilterInput"))
				.description("Filter relations by record id."),
		)
		.field(
			InputValue::new("and", TypeRef::named_nn_list(&relation_ctx.filter_name))
				.description("Combine multiple filters with a logical AND."),
		)
		.field(
			InputValue::new("or", TypeRef::named_nn_list(&relation_ctx.filter_name))
				.description("Combine multiple filters with a logical OR."),
		)
		.field(
			InputValue::new("not", TypeRef::named(&relation_ctx.filter_name))
				.description("Negate the nested filter expression."),
		);

	let order = InputObject::new(&relation_ctx.order_name)
		.description(format!("Ordering options for `{}` connections.", relation_ctx.field_name))
		.field(
			InputValue::new("field", TypeRef::named(&table_orderable_name))
				.description("The relation field used for ordering."),
		)
		.field(
			InputValue::new("direction", TypeRef::named("OrderDirection"))
				.description("The direction to sort the connection in."),
		);

	for fd in fds.iter() {
		let Some(ref kind) = fd.field_kind else {
			continue;
		};
		if fd.name.is_id() || fd.name.0.len() > 1 {
			continue;
		}

		let sql_field_name = fd.name.to_sql();
		if matches!(sql_field_name.as_str(), "in" | "out") {
			continue;
		}

		let gql_field_name = naming::to_camel_case(&sql_field_name);
		let order_enum_item = naming::to_screaming_snake_case(&gql_field_name);
		let enum_scope = format!("{}_{}", relation_name, sql_field_name);
		if list_item_kind(kind).is_none() {
			orderable = orderable.item(
				EnumItem::new(&order_enum_item)
					.description(format!("Order results by `{gql_field_name}`.")),
			);

			let field_type =
				kind_to_type_with_enum_prefix(kind.clone(), types, false, Some(&enum_scope))?;
			let type_filter_name =
				naming::scalar_filter_input_name(&unwrap_type(field_type).to_string());
			let filter_exists = types.iter().any(|ty| match ty {
				Type::InputObject(io) => io.type_name() == type_filter_name,
				_ => false,
			});
			if !filter_exists {
				let type_filter = filter_from_type(
					kind.clone(),
					type_filter_name.clone(),
					types,
					Some(&enum_scope),
				)?;
				types.push(Type::InputObject(type_filter));
			}
			filter = filter.field(
				InputValue::new(&gql_field_name, TypeRef::named(&type_filter_name))
					.description(format!("Filter by `{gql_field_name}`.")),
			);
		}
	}

	if !has_type(types, &table_orderable_name) {
		types.push(Type::Enum(orderable));
	}
	if !has_type(types, &relation_ctx.order_name) {
		types.push(Type::InputObject(order));
	}
	if !has_type(types, &relation_ctx.filter_name) {
		types.push(Type::InputObject(filter));
	}

	make_relation_connection_types(&relation_ctx, types)?;
	Ok(Some(relation_ctx))
}

/// Build all GraphQL types for a single table: the Object type, orderable enum,
/// order input, and filter input.
///
/// This processes all field definitions to create typed fields, filter types,
/// and orderable items, then attaches relation fields for any relations that
/// connect to this table.
fn build_table_type(
	tb: &TableDefinition,
	fds: &[FieldDefinition],
	relations: &[RelationInfo],
	relation_type_contexts: &HashMap<String, RelationTypeContext>,
	types: &mut Vec<Type>,
) -> Result<TableGraphQLTypes, GqlError> {
	let tb_name = &tb.name;
	let tb_name_str = tb_name.clone().into_string();
	if !naming::is_valid_db_name(&tb_name_str) {
		return Err(resolver_error(format!(
			"Table `{tb_name_str}` cannot be exposed via GraphQL because it is not valid snake_case"
		)));
	}
	let gql_type_name = naming::table_type_name(&tb_name_str);

	// --- Create initial types ---

	let table_orderable_name = naming::order_field_enum_name(&tb_name_str);
	let table_order_name = naming::order_input_name(&tb_name_str);
	let table_filter_name = filter_name_from_table(tb_name);
	let (connection_type_name, _) =
		make_connection_types(&tb_name_str, TypeRef::named_nn(&gql_type_name), types);

	let mut orderable = Enum::new(&table_orderable_name)
		.item(EnumItem::new("ID").description("Order results by record id."))
		.description(format!("Fields that can be used to order `{gql_type_name}` connections."));

	let order = InputObject::new(&table_order_name)
		.description(format!("Ordering options for `{gql_type_name}` connections."))
		.field(
			InputValue::new("field", TypeRef::named(&table_orderable_name))
				.description("The field used for ordering."),
		)
		.field(
			InputValue::new("direction", TypeRef::named("OrderDirection"))
				.description("The direction to sort the connection in."),
		);

	let mut filter = InputObject::new(&table_filter_name)
		.description(format!("Filter input for `{gql_type_name}` connections."))
		.field(
			InputValue::new("id", TypeRef::named("IdFilterInput"))
				.description("Filter records by record id."),
		)
		.field(
			InputValue::new("and", TypeRef::named_nn_list(&table_filter_name))
				.description("Combine multiple filters with a logical AND."),
		)
		.field(
			InputValue::new("or", TypeRef::named_nn_list(&table_filter_name))
				.description("Combine multiple filters with a logical OR."),
		)
		.field(
			InputValue::new("not", TypeRef::named(&table_filter_name))
				.description("Negate the nested filter expression."),
		);

	types.push(Type::InputObject(filter_id()));

	let mut ty_obj = Object::new(&gql_type_name)
		.description(
			tb.comment
				.clone()
				.unwrap_or_else(|| format!("GraphQL object type for records in `{tb_name_str}`.")),
		)
		.field(
			Field::new(
				"id",
				TypeRef::named_nn(TypeRef::ID),
				make_table_field_resolver("id", Some(Kind::Record(vec![tb_name.clone()])), None),
			)
			.description("The record id.")
			.directive(semantic_non_null_directive()),
		)
		.implement("record");

	let mut existing_field_names: HashSet<String> = HashSet::new();
	existing_field_names.insert("id".to_string());

	// --- Process field definitions ---

	let nested_objects = detect_nested_objects(&tb_name_str, fds);

	for fd in fds.iter() {
		let Some(ref kind) = fd.field_kind else {
			continue;
		};
		if fd.name.is_id() {
			continue;
		}
		if fd.name.0.len() > 1 {
			continue;
		}

		let sql_field_name = fd.name.to_sql();
		if !naming::is_valid_db_name(&sql_field_name) {
			return Err(resolver_error(format!(
				"Field `{sql_field_name}` on table `{tb_name_str}` cannot be exposed via GraphQL because it is not valid snake_case"
			)));
		}
		let gql_field_name = naming::to_camel_case(&sql_field_name);
		let order_enum_item = naming::to_screaming_snake_case(&gql_field_name);
		existing_field_names.insert(gql_field_name.clone());

		// Handle nested object fields (TYPE object with children)
		if let Some(nested) = nested_objects.get(sql_field_name.as_str()) {
			let nested_type = make_nested_object_type(nested, types)?;
			if !has_type(types, &nested.type_name) {
				types.push(Type::Object(nested_type));
			}

			let fd_type = if nested.is_array {
				let connection_type_name =
					naming::field_connection_type_name(&gql_type_name, &gql_field_name);
				let edge_type_name = naming::field_edge_type_name(&gql_type_name, &gql_field_name);
				make_named_connection_types(
					&format!("{gql_type_name}.{gql_field_name}"),
					connection_type_name.clone(),
					edge_type_name,
					TypeRef::named_nn(&nested.type_name),
					types,
				);
				TypeRef::named(&connection_type_name)
			} else {
				TypeRef::named(&nested.type_name)
			};

			if !nested.is_array {
				orderable = orderable.item(
					EnumItem::new(&order_enum_item)
						.description(format!("Order results by `{gql_field_name}`.")),
				);
			}
			let mut field = if nested.is_array {
				Field::new(
					&gql_field_name,
					fd_type,
					make_nested_object_array_field_resolver(
						sql_field_name.clone(),
						nested.type_name.clone(),
					),
				)
			} else {
				Field::new(
					&gql_field_name,
					fd_type,
					make_nested_object_field_resolver(sql_field_name.clone(), false),
				)
			};
			field = field.description(if let Some(ref c) = fd.comment {
				c.clone()
			} else {
				format!("Nested object field `{gql_field_name}`.")
			});
			if nested.is_array {
				field = field
					.argument(
						InputValue::new("first", TypeRef::named(TypeRef::INT))
							.description("Return the first n items from this connection."),
					)
					.argument(
						InputValue::new("after", TypeRef::named(TypeRef::STRING))
							.description("Return items after the specified cursor."),
					)
					.argument(
						InputValue::new("last", TypeRef::named(TypeRef::INT))
							.description("Return the last n items from this connection."),
					)
					.argument(
						InputValue::new("before", TypeRef::named(TypeRef::STRING))
							.description("Return items before the specified cursor."),
					);
			}
			if !nested.optional {
				field = field.directive(semantic_non_null_directive());
			}
			ty_obj = ty_obj.field(field);
			continue;
		}

		// Handle regular fields
		let enum_scope = format!("{}_{}", tb_name_str, sql_field_name);
		let list_item = list_item_kind(kind);
		let fd_type = if let Some((item_kind, _optional)) = &list_item {
			let node_type =
				kind_to_type_with_enum_prefix(item_kind.clone(), types, false, Some(&enum_scope))?;
			let connection_type_name =
				naming::field_connection_type_name(&gql_type_name, &gql_field_name);
			let edge_type_name = naming::field_edge_type_name(&gql_type_name, &gql_field_name);
			make_named_connection_types(
				&format!("{gql_type_name}.{gql_field_name}"),
				connection_type_name.clone(),
				edge_type_name,
				node_type,
				types,
			);
			TypeRef::named(&connection_type_name)
		} else {
			unwrap_type(kind_to_type_with_enum_prefix(
				kind.clone(),
				types,
				false,
				Some(&enum_scope),
			)?)
		};
		if list_item.is_none() {
			orderable = orderable.item(
				EnumItem::new(&order_enum_item)
					.description(format!("Order results by `{gql_field_name}`.")),
			);

			let type_filter_name =
				naming::scalar_filter_input_name(&unwrap_type(fd_type.clone()).to_string());
			let filter_already_exists = types.iter().any(|t| match t {
				Type::InputObject(io) => io.type_name() == type_filter_name,
				_ => false,
			});
			if !filter_already_exists {
				let type_filter = Type::InputObject(filter_from_type(
					kind.clone(),
					type_filter_name.clone(),
					types,
					Some(&enum_scope),
				)?);
				trace!("\n{type_filter:?}\n");
				types.push(type_filter);
			}

			filter = filter.field(
				InputValue::new(&gql_field_name, TypeRef::named(&type_filter_name))
					.description(format!("Filter by `{gql_field_name}`.")),
			);
		}
		let mut field = if let Some((item_kind, _optional)) = list_item {
			let node_type_name = unwrap_type(kind_to_type_with_enum_prefix(
				item_kind.clone(),
				types,
				false,
				Some(&enum_scope),
			)?)
			.to_string();
			Field::new(
				&gql_field_name,
				fd_type,
				make_array_field_resolver(
					sql_field_name,
					item_kind,
					node_type_name,
					Some(enum_scope.clone()),
				),
			)
			.argument(
				InputValue::new("first", TypeRef::named(TypeRef::INT))
					.description("Return the first n items from this connection."),
			)
			.argument(
				InputValue::new("after", TypeRef::named(TypeRef::STRING))
					.description("Return items after the specified cursor."),
			)
			.argument(
				InputValue::new("last", TypeRef::named(TypeRef::INT))
					.description("Return the last n items from this connection."),
			)
			.argument(
				InputValue::new("before", TypeRef::named(TypeRef::STRING))
					.description("Return items before the specified cursor."),
			)
		} else {
			Field::new(
				&gql_field_name,
				fd_type,
				make_table_field_resolver(sql_field_name, fd.field_kind.clone(), Some(enum_scope)),
			)
		};
		if let Some(ref c) = fd.comment {
			field = field.description(c.clone());
		} else {
			field = field.description(format!("The `{gql_field_name}` field."));
		}
		if !kind.can_be_none() {
			field = field.directive(semantic_non_null_directive());
		}
		ty_obj = ty_obj.field(field);
	}

	// --- Add relation fields ---

	for rel in relations.iter() {
		let rel_table_str = rel.table_name.clone().into_string();
		let Some(rel_ctx) = relation_type_contexts.get(&rel_table_str) else {
			continue;
		};

		// Outgoing: this table is in the FROM list
		if rel.from_tables.contains(&tb_name_str) {
			let field_name = rel_ctx.field_name.clone();
			if !existing_field_names.contains(&field_name) {
				existing_field_names.insert(field_name.clone());
				ty_obj = ty_obj.field(make_relation_field(rel_ctx));
			} else {
				trace!(
					"Skipping outgoing relation field '{}' on table '{}': \
					 conflicts with existing field",
					field_name, tb_name_str
				);
			}
		}
	}

	Ok(TableGraphQLTypes {
		ty_obj,
		orderable,
		order,
		filter,
		connection_type_name,
	})
}

// ---------------------------------------------------------------------------
// Top-level table processing
// ---------------------------------------------------------------------------

pub async fn process_tbs(
	tbs: Arc<[TableDefinition]>,
	mut query: Object,
	types: &mut Vec<Type>,
	ctx: &SchemaContext<'_>,
	relations: &[RelationInfo],
	table_fields: &mut HashMap<String, Arc<[FieldDefinition]>>,
) -> Result<Object, GqlError> {
	// Pre-fetch field definitions for relation tables so relation connections can
	// expose edge metadata and relation-specific filter/order inputs.
	let mut relation_table_fds: HashMap<String, Arc<[FieldDefinition]>> = HashMap::new();
	for rel in relations.iter() {
		let rel_name = rel.table_name.clone().into_string();
		if let std::collections::hash_map::Entry::Vacant(e) = relation_table_fds.entry(rel_name) {
			let fds = ctx.tx.all_tb_fields(ctx.ns, ctx.db, &rel.table_name, None).await?;
			e.insert(fds);
		}
	}

	// Set of exposed table names for checking that relation targets are visible
	let exposed_table_names: HashSet<String> = tbs
		.iter()
		.filter(|tb| !matches!(tb.table_type, TableType::Relation(_)))
		.map(|t| t.name.clone().into_string())
		.collect();

	let mut relation_type_contexts = HashMap::new();
	for rel in relations.iter() {
		let rel_name = rel.table_name.clone().into_string();
		let Some(fds) = relation_table_fds.get(&rel_name).cloned() else {
			continue;
		};
		if let Some(context) = build_relation_type_context(rel, fds, &exposed_table_names, types)? {
			relation_type_contexts.insert(rel_name, context);
		}
	}

	for tb in tbs.iter() {
		if matches!(tb.table_type, TableType::Relation(_)) {
			continue;
		}
		trace!("Adding table: {}", tb.name);
		let fds = ctx.tx.all_tb_fields(ctx.ns, ctx.db, &tb.name, None).await?;
		table_fields.insert(tb.name.clone().into_string(), fds.clone());

		// Build and register the table's type system
		let tt = build_table_type(tb, &fds, relations, &relation_type_contexts, types)?;
		query = query.field(make_table_list_field(
			tb,
			fds.clone(),
			ctx.datastore.clone(),
			tt.connection_type_name.clone(),
		));
		query = query.field(make_table_get_field(tb, ctx.datastore.clone()));
		types.push(Type::Object(tt.ty_obj));
		types.push(tt.order.into());
		types.push(Type::Enum(tt.orderable));
		types.push(Type::InputObject(tt.filter));
	}

	Ok(query)
}

/// Create a field resolver for a column on a table Object type.
///
/// The resolver has two execution paths:
///
/// 1. **Fast path** -- if the parent value is a [`CachedRecord`] (the common case for list queries,
///    singular root fetches, and mutations), the field value is extracted directly from the
///    in-memory
///    record data.
/// 2. **Slow path** -- if the parent is a [`VersionedRecord`] or plain `RecordId` (e.g. from a
///    custom function return), the resolver issues a `SELECT VALUE <field> FROM ONLY <record_id>`
///    query.
///
/// Record-link fields (`TYPE record<target>`) are dereferenced: the resolver
/// fetches the target record's full data and wraps it in a new `CachedRecord`
/// so the target's own field resolvers also benefit from caching.
fn make_table_field_resolver(
	fd_name: impl Into<String>,
	kind: Option<Kind>,
	enum_scope: Option<String>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	let fd_name = fd_name.into();
	move |ctx: ResolverContext| {
		let fd_name = fd_name.clone();
		let field_kind = kind.clone();
		let enum_scope = enum_scope.clone();
		FieldFuture::new({
			async move {
				// ── Fast path: extract field from CachedRecord ──
				//
				// When the parent is a CachedRecord (from a list query, singular root fetch,
				// relation, or mutation), the full record data is already in
				// memory. Extract the requested field directly instead of
				// issuing a separate database query.
				if let Ok(cached) = ctx.parent_value.try_downcast_ref::<CachedRecord>() {
					return resolve_field_from_cached_record(
						&ctx,
						cached,
						&fd_name,
						&field_kind,
						enum_scope.as_deref(),
					)
					.await;
				}

				// ── Slow path: fetch field via database query ──
				//
				// Fallback for VersionedRecord (no cached data) or plain
				// RecordId (from custom functions, etc.).
				let ds = ctx.data::<Arc<Datastore>>()?;
				let sess = ctx.data::<Arc<Session>>()?;

				let (rid, version) = match ctx.parent_value.try_downcast_ref::<VersionedRecord>() {
					Ok(vr) => (vr.rid.clone(), vr.version.clone()),
					Err(_) => {
						let rid = ctx.parent_value.try_downcast_ref::<RecordId>()?;
						(rid.clone(), None)
					}
				};

				// Build SELECT VALUE <field> FROM ONLY <record_id>
				let stmt = select_field_from_record(&rid, &fd_name, &version);
				let val = execute_select(ds, sess, stmt).await?;
				resolve_field_value(
					&ctx,
					val,
					&fd_name,
					&field_kind,
					&version,
					enum_scope.as_deref(),
				)
				.await
			}
		})
	}
}

/// Convert a resolved field value to a GraphQL `FieldValue`.
///
/// Handles record-link dereferencing (fetching the target record's full data
/// for caching), geometry values, and scalar conversions. Used by both the
/// cached and uncached paths in `make_table_field_resolver`.
async fn resolve_field_value(
	ctx: &ResolverContext<'_>,
	val: Value,
	fd_name: &str,
	field_kind: &Option<Kind>,
	version: &Option<Datetime>,
	enum_scope: Option<&str>,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	match val {
		Value::RecordId(target_rid) if fd_name != "id" => {
			// Record-link dereferencing: fetch the full target record and
			// wrap it as CachedRecord so the target's field resolvers can
			// also benefit from caching.
			let ds = ctx.data::<Arc<Datastore>>()?;
			let sess = ctx.data::<Arc<Session>>()?;

			let stmt = select_all_from_record(&target_rid, version);
			let target_val = execute_select(ds, sess, stmt).await?;

			match target_val {
				Value::Object(obj) => {
					let field_val = FieldValue::owned_any(CachedRecord {
						rid: target_rid.clone(),
						version: version.clone(),
						data: obj,
					});
					let field_val = match field_kind {
						Some(Kind::Record(ts)) if ts.is_empty() || ts.len() > 1 => {
							field_val.with_type(naming::table_type_name(target_rid.table.as_str()))
						}
						_ => field_val,
					};
					Ok(Some(field_val))
				}
				Value::None | Value::Null => Ok(None),
				_ => Ok(None),
			}
		}
		Value::Geometry(g) => {
			let type_name = geometry_gql_type_name(&g);
			let field_val = FieldValue::owned_any(g);
			let field_val = match field_kind {
				Some(Kind::Geometry(ks)) if ks.is_empty() || ks.len() > 1 => {
					field_val.with_type(type_name)
				}
				_ => field_val,
			};
			Ok(Some(field_val))
		}
		Value::None | Value::Null => Ok(None),
		v => {
			let out = sql_value_to_gql_value_with_kind(v, field_kind.as_ref(), enum_scope)
				.map_err(async_graphql::Error::from)?;
			Ok(Some(FieldValue::value(out)))
		}
	}
}

/// Fast-path field resolution from a [`CachedRecord`].
///
/// Extracts the field value directly from the cached record data. For
/// record-link fields, fetches the linked record's full data in a single
/// `SELECT *` query (instead of N per-field queries).
async fn resolve_field_from_cached_record(
	ctx: &ResolverContext<'_>,
	cached: &CachedRecord,
	fd_name: &str,
	field_kind: &Option<Kind>,
	enum_scope: Option<&str>,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	let val = cached.data.get(fd_name).cloned().unwrap_or(Value::None);
	resolve_field_value(ctx, val, fd_name, field_kind, &cached.version, enum_scope).await
}

fn make_array_field_resolver(
	fd_name: impl Into<String>,
	item_kind: Kind,
	node_type_name: impl Into<String>,
	enum_scope: Option<String>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	let fd_name = fd_name.into();
	let node_type_name = node_type_name.into();
	move |ctx: ResolverContext| {
		let fd_name = fd_name.clone();
		let item_kind = item_kind.clone();
		let node_type_name = node_type_name.clone();
		let enum_scope = enum_scope.clone();
		FieldFuture::new(async move {
			let args = ctx.args.as_index_map();
			let connection_args = parse_connection_args(args)?;

			if let Ok(cached) = ctx.parent_value.try_downcast_ref::<CachedRecord>() {
				let val = cached.data.get(&fd_name).cloned().unwrap_or(Value::None);
				return resolve_array_field_connection_value(
					val,
					&item_kind,
					&cached.version,
					&connection_args,
					&node_type_name,
					enum_scope.as_deref(),
				);
			}

			let ds = ctx.data::<Arc<Datastore>>()?;
			let sess = ctx.data::<Arc<Session>>()?;
			let (rid, version) = match ctx.parent_value.try_downcast_ref::<VersionedRecord>() {
				Ok(vr) => (vr.rid.clone(), vr.version.clone()),
				Err(_) => {
					let rid = ctx.parent_value.try_downcast_ref::<RecordId>()?;
					(rid.clone(), None)
				}
			};

			let stmt = select_field_from_record(&rid, &fd_name, &version);
			let val = execute_select(ds, sess, stmt).await?;
			resolve_array_field_connection_value(
				val,
				&item_kind,
				&version,
				&connection_args,
				&node_type_name,
				enum_scope.as_deref(),
			)
		})
	}
}

fn resolve_array_field_connection_value(
	val: Value,
	item_kind: &Kind,
	version: &Option<Datetime>,
	args: &ConnectionArgs,
	node_type_name: &str,
	enum_scope: Option<&str>,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	let array = match val {
		Value::Array(array) => array,
		Value::None | Value::Null => return Ok(None),
		_ => return Ok(None),
	};

	let mut nodes = Vec::with_capacity(array.len());
	for (index, value) in array.0.into_iter().enumerate() {
		let node = match value {
			Value::None | Value::Null => ConnectionNode::Value(GqlValue::Null),
			Value::RecordId(rid) => ConnectionNode::VersionedRecord {
				runtime_type_name: (node_type_name == "record")
					.then(|| naming::table_type_name(rid.table.as_str())),
				record: VersionedRecord {
					rid,
					version: version.clone(),
				},
			},
			Value::Object(object) if !matches!(item_kind, Kind::Object) => ConnectionNode::Object {
				object,
				runtime_type_name: None,
			},
			other => ConnectionNode::Value(
				sql_value_to_gql_value_with_kind(other, Some(item_kind), enum_scope)
					.map_err(async_graphql::Error::from)?,
			),
		};
		nodes.push((index.to_string(), node));
	}

	build_node_connection_value(nodes, args)
}

/// Build a GraphQL field for a relation on a table type.
///
/// The field returns a Relay connection of target nodes, while the edges expose
/// the relation record metadata.
fn make_relation_field(relation: &RelationTypeContext) -> Field {
	Field::new(
		&relation.field_name,
		TypeRef::named_nn(&relation.connection_type_name),
		make_relation_field_resolver(
			relation.relation_table_name.clone(),
			relation.node_type_name.clone(),
			relation.fds.clone(),
		),
	)
	.description(format!("Outgoing `{}` relations from this record.", relation.field_name))
	.argument(
		InputValue::new("first", TypeRef::named(TypeRef::INT))
			.description("Return the first n relations from the connection."),
	)
	.argument(
		InputValue::new("after", TypeRef::named(TypeRef::STRING))
			.description("Return relations after the specified cursor."),
	)
	.argument(
		InputValue::new("last", TypeRef::named(TypeRef::INT))
			.description("Return the last n relations from the connection."),
	)
	.argument(
		InputValue::new("before", TypeRef::named(TypeRef::STRING))
			.description("Return relations before the specified cursor."),
	)
	.argument(
		InputValue::new("orderBy", TypeRef::named(&relation.order_name))
			.description("Specify how the relation connection should be ordered."),
	)
	.argument(
		InputValue::new("filterBy", TypeRef::named(&relation.filter_name))
			.description("Filter the relations returned by this connection."),
	)
}

/// Create a resolver for a relation field.
///
/// The resolver:
/// 1. Extracts the parent record's id
/// 2. Builds `SELECT * FROM <relation_table> WHERE in = $current_record`
/// 3. Optionally combines with user-supplied filter, ordering, and pagination
/// 4. Resolves the relation edge metadata plus the target node on `out`
fn make_relation_field_resolver(
	relation_table_name: TableName,
	node_type_name: String,
	rel_fds: Arc<[FieldDefinition]>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	move |ctx: ResolverContext| {
		let relation_table = relation_table_name.clone();
		let relation_node_type_name = node_type_name.clone();
		let fds = rel_fds.clone();
		FieldFuture::new(async move {
			let ds = ctx.data::<Arc<Datastore>>()?;
			let sess = ctx.data::<Arc<Session>>()?;

			// Extract record ID and optional version from parent.
			// Try CachedRecord first, then VersionedRecord, then plain RecordId.
			let (rid, version) =
				if let Ok(cached) = ctx.parent_value.try_downcast_ref::<CachedRecord>() {
					(cached.rid.clone(), cached.version.clone())
				} else if let Ok(vr) = ctx.parent_value.try_downcast_ref::<VersionedRecord>() {
					(vr.rid.clone(), vr.version.clone())
				} else {
					let rid = ctx.parent_value.try_downcast_ref::<RecordId>()?;
					(rid.clone(), None)
				};
			let args = ctx.args.as_index_map();

			let connection_args = parse_connection_args(args)?;
			let order = parse_order_arg(args, &fds)?;

			let mut base_cond = Expr::Binary {
				left: Box::new(Expr::Idiom(Idiom::field("in".to_string()))),
				op: BinaryOperator::Equal,
				right: Box::new(Value::RecordId(rid.clone()).into_literal()),
			};

			// Parse and combine user-supplied filter
			if let Some(user_cond) = parse_filter_arg(args, &fds, relation_table.as_str())? {
				base_cond = Expr::Binary {
					left: Box::new(base_cond),
					op: BinaryOperator::And,
					right: Box::new(user_cond.0),
				};
			}

			let cond = Some(Cond(base_cond));

			// Build SELECT * FROM <relation_table> WHERE ...
			// Propagate version from parent for consistent temporal queries
			let stmt = select_all_from_table(
				Expr::Table(relation_table.clone()),
				cond,
				order,
				None,
				None,
				&version,
			);

			let res = execute_select(ds, sess, stmt).await?;

			match res {
				Value::Array(a) => relation_objects_to_connection(
					a,
					version,
					&connection_args,
					&relation_node_type_name,
				),
				v => {
					error!("Expected array result for relation query, found: {v:?}");
					Err(internal_error("Unexpected result type for relation query").into())
				}
			}
		})
	}
}

macro_rules! filter_impl {
	($filter:ident, $ty:ident, $name:expr_2021) => {
		$filter = $filter.field(InputValue::new($name, $ty.clone()));
	};
}

fn filter_id() -> InputObject {
	let mut filter = InputObject::new("IdFilterInput")
		.description("Filter operations available for record id fields.");
	let ty = TypeRef::named(TypeRef::ID);
	filter = filter.field(InputValue::new("eq", ty.clone()).description("Match exactly this id."));
	filter = filter.field(InputValue::new("ne", ty.clone()).description("Exclude this id."));
	// `in` accepts a list of IDs
	let list_ty = TypeRef::named_nn_list(TypeRef::ID);
	filter =
		filter.field(InputValue::new("in", list_ty).description("Match any of the provided ids."));
	filter
}

/// Generate a filter InputObject for a field's type.
///
/// All types get `eq` and `ne` operators.  Additional operators are added
/// based on the kind:
/// - **String** -- `contains`, `startsWith`, `endsWith`, `regex`, `in`
/// - **Numeric** (Int, Float, Number, Decimal) -- `gt`, `gte`, `lt`, `lte`, `in`
/// - **Datetime** -- `gt`, `gte`, `lt`, `lte`
/// - **Record** -- `in` (list of IDs)
///
/// `option<record<T>>` is normalised to the inner record kind so filters
/// use the target table's filter type rather than a plain ID filter.
fn filter_from_type(
	kind: Kind,
	filter_name: String,
	types: &mut Vec<Type>,
	enum_scope: Option<&str>,
) -> Result<InputObject, GqlError> {
	// Normalise `option<record<T>>` (Kind::Either([None, Record([T])])) down to the
	// inner record kind so filters are generated correctly with ID-based filtering.
	let effective_kind = match &kind {
		Kind::Either(ks) => {
			let non_none: Vec<&Kind> =
				ks.iter().filter(|k| !matches!(k, Kind::None | Kind::Null)).collect();
			if non_none.len() == 1 {
				non_none[0].clone()
			} else {
				kind.clone()
			}
		}
		_ => kind.clone(),
	};

	let ty = match &effective_kind {
		Kind::Record(ts) => match ts.len() {
			1 => TypeRef::named(filter_name_from_table(
				ts.first().expect("ts should have exactly one element").as_str(),
			)),
			_ => TypeRef::named(TypeRef::ID),
		},
		k => unwrap_type(kind_to_type_with_enum_prefix(k.clone(), types, true, enum_scope)?),
	};

	// All types get eq and ne
	let mut filter = InputObject::new(filter_name);
	filter_impl!(filter, ty, "eq");
	filter_impl!(filter, ty, "ne");

	match effective_kind {
		// String: contains, startsWith, endsWith, regex, in
		Kind::String => {
			let str_ty = TypeRef::named(TypeRef::STRING);
			filter_impl!(filter, str_ty, "contains");
			filter_impl!(filter, str_ty, "startsWith");
			filter_impl!(filter, str_ty, "endsWith");
			filter_impl!(filter, str_ty, "regex");
			let list_ty = TypeRef::named_nn_list(TypeRef::STRING);
			filter_impl!(filter, list_ty, "in");
		}
		// Numeric types: gt, gte, lt, lte, in
		Kind::Int => {
			let num_ty = TypeRef::named(TypeRef::INT);
			filter_impl!(filter, num_ty, "gt");
			filter_impl!(filter, num_ty, "gte");
			filter_impl!(filter, num_ty, "lt");
			filter_impl!(filter, num_ty, "lte");
			let list_ty = TypeRef::named_nn_list(TypeRef::INT);
			filter_impl!(filter, list_ty, "in");
		}
		Kind::Float => {
			let num_ty = TypeRef::named(TypeRef::FLOAT);
			filter_impl!(filter, num_ty, "gt");
			filter_impl!(filter, num_ty, "gte");
			filter_impl!(filter, num_ty, "lt");
			filter_impl!(filter, num_ty, "lte");
			let list_ty = TypeRef::named_nn_list(TypeRef::FLOAT);
			filter_impl!(filter, list_ty, "in");
		}
		Kind::Number => {
			let num_ty = TypeRef::named("Number");
			filter_impl!(filter, num_ty, "gt");
			filter_impl!(filter, num_ty, "gte");
			filter_impl!(filter, num_ty, "lt");
			filter_impl!(filter, num_ty, "lte");
			let list_ty = TypeRef::named_nn_list("Number");
			filter_impl!(filter, list_ty, "in");
		}
		Kind::Decimal => {
			let num_ty = TypeRef::named("Decimal");
			filter_impl!(filter, num_ty, "gt");
			filter_impl!(filter, num_ty, "gte");
			filter_impl!(filter, num_ty, "lt");
			filter_impl!(filter, num_ty, "lte");
			let list_ty = TypeRef::named_nn_list("Decimal");
			filter_impl!(filter, list_ty, "in");
		}
		// Datetime: gt, gte, lt, lte
		Kind::Datetime => {
			let dt_ty = TypeRef::named("Datetime");
			filter_impl!(filter, dt_ty, "gt");
			filter_impl!(filter, dt_ty, "gte");
			filter_impl!(filter, dt_ty, "lt");
			filter_impl!(filter, dt_ty, "lte");
		}
		// Record: in (list of IDs)
		Kind::Record(_) => {
			let list_ty = TypeRef::named_nn_list(TypeRef::ID);
			filter_impl!(filter, list_ty, "in");
		}
		Kind::Any
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Duration
		| Kind::Object
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Geometry(_)
		| Kind::Either(_)
		| Kind::Set(_, _)
		| Kind::Array(_, _)
		| Kind::Function(_, _)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_) => {}
	};
	Ok(filter)
}

/// Convert a GraphQL filter input object into a SurrealQL `WHERE` condition.
///
/// The filter object may contain field-level comparison operators (`eq`, `gt`,
/// etc.), logical combinators (`and`, `or`, `not`), or a mix of both.
/// Multiple top-level keys are combined with implicit AND.
pub(super) fn cond_from_filter(
	filter: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
	tb_name: &str,
) -> Result<Cond, GqlError> {
	val_from_filter(filter, fds, tb_name).map(Cond)
}

/// Recursive filter-to-expression converter.
///
/// Single-key filters dispatch directly to the appropriate handler (field
/// comparison, AND/OR aggregation, or NOT negation).  Multi-key filters are
/// treated as implicit AND across all entries.
fn val_from_filter(
	filter: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
	tb_name: &str,
) -> Result<Expr, GqlError> {
	if filter.is_empty() {
		return Err(resolver_error("Table filter must have at least one item"));
	}

	// If there is exactly one key, use the original dispatch logic
	if filter.len() == 1 {
		let (k, v) = filter.iter().next().expect("filter has exactly one item");

		return match k.as_str().to_lowercase().as_str() {
			"or" => aggregate(v, AggregateOp::Or, fds, tb_name),
			"and" => aggregate(v, AggregateOp::And, fds, tb_name),
			"not" => negate(v, fds, tb_name),
			_ => binop(k.as_str(), v, fds, tb_name),
		};
	}

	// Multiple fields: implicit AND across all entries.
	// Separate logical operators (and/or/not) from field conditions.
	let mut exprs = Vec::with_capacity(filter.len());

	for (k, v) in filter.iter() {
		let expr = match k.as_str().to_lowercase().as_str() {
			"or" => aggregate(v, AggregateOp::Or, fds, tb_name)?,
			"and" => aggregate(v, AggregateOp::And, fds, tb_name)?,
			"not" => negate(v, fds, tb_name)?,
			_ => binop(k.as_str(), v, fds, tb_name)?,
		};
		exprs.push(expr);
	}

	let mut iter = exprs.into_iter();
	let mut combined = iter.next().expect("at least one filter entry");
	for next_expr in iter {
		combined = Expr::Binary {
			left: Box::new(combined),
			op: BinaryOperator::And,
			right: Box::new(next_expr),
		};
	}

	Ok(combined)
}

/// Operators that map directly to SurrealDB binary operators.
fn parse_binary_op(name: &str) -> Option<expr::BinaryOperator> {
	match name {
		"eq" => Some(expr::BinaryOperator::Equal),
		"ne" => Some(expr::BinaryOperator::NotEqual),
		"gt" => Some(expr::BinaryOperator::MoreThan),
		"gte" => Some(expr::BinaryOperator::MoreThanEqual),
		"lt" => Some(expr::BinaryOperator::LessThan),
		"lte" => Some(expr::BinaryOperator::LessThanEqual),
		"in" => Some(expr::BinaryOperator::Inside),
		_ => None,
	}
}

/// Operators that map to SurrealDB function calls.
/// Returns the fully-qualified function name.
fn parse_function_op(name: &str) -> Option<&'static str> {
	match name {
		"contains" => Some("string::contains"),
		"startsWith" => Some("string::starts_with"),
		"endsWith" => Some("string::ends_with"),
		"regex" => Some("string::matches"),
		_ => None,
	}
}

fn negate(filter: &GqlValue, fds: &[FieldDefinition], tb_name: &str) -> Result<Expr, GqlError> {
	let obj = filter.as_object().ok_or(resolver_error("Value of NOT must be object"))?;
	let inner_cond = val_from_filter(obj, fds, tb_name)?;

	Ok(Expr::Prefix {
		op: expr::PrefixOperator::Not,
		expr: Box::new(inner_cond),
	})
}

enum AggregateOp {
	And,
	Or,
}

fn aggregate(
	filter: &GqlValue,
	op: AggregateOp,
	fds: &[FieldDefinition],
	tb_name: &str,
) -> Result<Expr, GqlError> {
	let op_str = match op {
		AggregateOp::And => "AND",
		AggregateOp::Or => "OR",
	};
	let op = match op {
		AggregateOp::And => BinaryOperator::And,
		AggregateOp::Or => BinaryOperator::Or,
	};
	let list =
		filter.as_list().ok_or(resolver_error(format!("Value of {op_str} should be a list")))?;
	let filter_arr = list
		.iter()
		.map(|v| v.as_object().map(|o| val_from_filter(o, fds, tb_name)))
		.collect::<Option<Result<Vec<Expr>, GqlError>>>()
		.ok_or(resolver_error(format!("List of {op_str} should contain objects")))??;

	let mut iter = filter_arr.into_iter();

	let mut cond = iter
		.next()
		.ok_or(resolver_error(format!("List of {op_str} should contain at least one object")))?;

	for clause in iter {
		cond = Expr::Binary {
			left: Box::new(clause),
			op: op.clone(),
			right: Box::new(cond),
		}
	}

	Ok(cond)
}

/// Convert a single field's filter object to a SurrealQL expression.
///
/// The filter object maps operator names (`eq`, `gt`, `contains`, etc.) to
/// values.  Binary operators produce `field <op> value` expressions; function
/// operators produce `fn(field, value)` calls.  Multiple operators on the
/// same field are combined with AND.
fn binop(
	field_name: &str,
	val: &GqlValue,
	fds: &[FieldDefinition],
	tb_name: &str,
) -> Result<Expr, GqlError> {
	let obj = val.as_object().ok_or(resolver_error("Field filter should be object"))?;

	let Some(fd) = fds.iter().find(|fd| {
		let sql_name = fd.name.to_sql();
		sql_name == field_name || naming::to_camel_case(&sql_name) == field_name
	}) else {
		if field_name == "id" {
			return binop_for_id(obj);
		}
		return Err(resolver_error(format!("Field `{field_name}` not found")));
	};

	if obj.is_empty() {
		return Err(resolver_error("Field filter must have at least one operator"));
	}

	let sql_field_name = fd.name.to_sql();
	let field_kind = fd.field_kind.clone().unwrap_or_default();
	let enum_scope = format!("{tb_name}_{sql_field_name}");
	let mut exprs = Vec::with_capacity(obj.len());

	for (k, v) in obj.iter() {
		let op_name = k.as_str();
		let lhs = Expr::Idiom(Idiom::field(sql_field_name.clone()));

		if let Some(binary_op) = parse_binary_op(op_name) {
			let rhs_kind = if op_name == "in" {
				Kind::Array(Box::new(field_kind.clone()), None)
			} else {
				field_kind.clone()
			};
			let rhs = gql_to_sql_kind_with_scope(v, rhs_kind, Some(&enum_scope))?;
			exprs.push(Expr::Binary {
				left: Box::new(lhs),
				op: binary_op,
				right: Box::new(rhs.into_literal()),
			});
		} else if let Some(fn_name) = parse_function_op(op_name) {
			// Function-call operators: string::contains(field, value)
			let rhs = gql_to_sql_kind(v, Kind::String)?;
			exprs.push(Expr::FunctionCall(Box::new(FunctionCall {
				receiver: Function::Normal(fn_name.to_string()),
				arguments: vec![lhs, rhs.into_literal()],
			})));
		} else {
			return Err(resolver_error(format!("Unsupported filter operator: {op_name}")));
		}
	}

	// Combine multiple operators with AND
	let mut iter = exprs.into_iter();
	let mut combined = iter.next().expect("at least one operator");
	for next_expr in iter {
		combined = Expr::Binary {
			left: Box::new(combined),
			op: BinaryOperator::And,
			right: Box::new(next_expr),
		};
	}

	Ok(combined)
}

/// Handle binary operators for the `id` field which doesn't appear in field definitions.
fn binop_for_id(obj: &IndexMap<Name, GqlValue>) -> Result<Expr, GqlError> {
	if obj.is_empty() {
		return Err(resolver_error("ID filter must have at least one operator"));
	}

	let mut exprs = Vec::with_capacity(obj.len());

	for (k, v) in obj.iter() {
		let op_name = k.as_str();
		let lhs = Expr::Idiom(Idiom::field("id".to_string()));

		if let Some(binary_op) = parse_binary_op(op_name) {
			let rhs_kind = if op_name == "in" {
				Kind::Array(Box::new(Kind::Record(vec![])), None)
			} else {
				Kind::Record(vec![])
			};
			let rhs = gql_to_sql_kind(v, rhs_kind)?;
			exprs.push(Expr::Binary {
				left: Box::new(lhs),
				op: binary_op,
				right: Box::new(rhs.into_literal()),
			});
		} else {
			return Err(resolver_error(format!("Unsupported ID filter operator: {op_name}")));
		}
	}

	let mut iter = exprs.into_iter();
	let mut combined = iter.next().expect("at least one operator");
	for next_expr in iter {
		combined = Expr::Binary {
			left: Box::new(combined),
			op: BinaryOperator::And,
			right: Box::new(next_expr),
		};
	}

	Ok(combined)
}
