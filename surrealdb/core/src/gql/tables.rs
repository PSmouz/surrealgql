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
use crate::expr::lookup::{LookupKind, LookupSubject};
use crate::expr::operator::BooleanOperator;
use crate::expr::order::{OrderList, Ordering};
use crate::expr::part::Part;
use crate::expr::statements::SelectStatement;
use crate::expr::{
	self, BinaryOperator, Cond, Dir, Expr, Fields, Function, FunctionCall, Idiom, Kind,
	KindLiteral, Limit, Literal, LogicalPlan, Param, Start, TopLevelExpr,
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
	filter_spec: &FilterObjectSpec,
	table_filter_registry: &TableFilterRegistry,
) -> Result<Option<Cond>, GqlError> {
	let filter = args.get("filterBy");
	match filter {
		Some(GqlValue::Object(o)) => {
			Ok(Some(cond_from_filter_spec(o, filter_spec, table_filter_registry)?))
		}
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
		Some(Kind::Object) => Some(false),
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
	match normalize_filter_kind(kind) {
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
	root_names: RootTypeNames,
	filter_spec: FilterObjectSpec,
) -> Field {
	let tb_name = tb.name.clone();
	let table_order_name = root_names.order_name.clone();
	let table_filter_name = root_names.filter_name.clone();
	let node_type_name = root_names.type_name.clone();
	let query_field_name = root_names.plural_field_name.clone();
	let connection_type_name = root_names.connection_type_name.clone();
	let entity_description_name = root_names.entity_description_name.clone();

	Field::new(&query_field_name, TypeRef::named_nn(&connection_type_name), move |ctx| {
		let tb_name = tb_name.clone();
		let fds = fds.clone();
		let kvs = kvs.clone();
		let node_type_name = node_type_name.clone();
		let filter_spec = filter_spec.clone();
		FieldFuture::new(async move {
			let sess = ctx.data::<Arc<Session>>()?;
			let table_filter_registry = ctx.data::<Arc<TableFilterRegistry>>()?;
			let args = ctx.args.as_index_map();
			trace!("received request with args: {args:?}");

			let connection_args = parse_connection_args(args)?;
			let version = parse_version_arg(args)?;
			let order = parse_order_arg(args, &fds)?;
			let cond = parse_filter_arg(args, &filter_spec, table_filter_registry.as_ref())?;

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
		format!("Query a paginated list of `{entity_description_name}` records.")
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
fn make_table_get_field(
	tb: &TableDefinition,
	kvs: Arc<Datastore>,
	root_names: RootTypeNames,
) -> Field {
	let tb_name = tb.name.clone();
	let gql_field_name = root_names.singular_field_name.clone();
	let gql_type_name = root_names.type_name.clone();
	let entity_description_name = root_names.entity_description_name.clone();

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
		format!("Fetch a single `{entity_description_name}` record by record id.")
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
	/// Recursive filter metadata used to compile `filterBy` arguments.
	filter_spec: FilterObjectSpec,
	/// The Relay connection type name for the table.
	connection_type_name: String,
}

#[derive(Clone)]
struct RootTypeNames {
	base_name: String,
	singular_field_name: String,
	plural_field_name: String,
	type_name: String,
	connection_type_name: String,
	order_name: String,
	orderable_name: String,
	filter_name: String,
	entity_description_name: String,
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
	edge_filter_spec: FilterObjectSpec,
	root_filter_spec: RelationListFilterSpec,
}

fn root_type_names_for_table(table_name: &str) -> RootTypeNames {
	RootTypeNames {
		base_name: table_name.to_string(),
		singular_field_name: naming::singular_query_name(table_name),
		plural_field_name: naming::plural_query_name(table_name),
		type_name: naming::table_type_name(table_name),
		connection_type_name: naming::connection_type_name(table_name),
		order_name: naming::order_input_name(table_name),
		orderable_name: naming::order_field_enum_name(table_name),
		filter_name: naming::filter_input_name(table_name),
		entity_description_name: naming::table_type_name(table_name),
	}
}

fn root_type_names_for_relation(table_name: &str) -> RootTypeNames {
	RootTypeNames {
		base_name: naming::relation_root_base_name(table_name),
		singular_field_name: naming::relation_singular_query_name(table_name),
		plural_field_name: naming::relation_plural_query_name(table_name),
		type_name: naming::relation_type_name(table_name),
		connection_type_name: naming::relation_connection_type_name(table_name),
		order_name: naming::relation_order_input_name(table_name),
		orderable_name: naming::relation_order_field_enum_name(table_name),
		filter_name: naming::relation_filter_input_name(table_name),
		entity_description_name: naming::relation_type_name(table_name),
	}
}

#[derive(Clone)]
pub(crate) struct FilterObjectSpec {
	pub(crate) type_name: String,
	pub(crate) description: String,
	pub(crate) fields: Vec<FilterFieldSpec>,
}

#[derive(Clone)]
pub(crate) struct FilterFieldSpec {
	gql_name: String,
	description: String,
	kind: FilterFieldKind,
}

#[derive(Clone)]
enum FilterFieldKind {
	Scalar(ScalarFilterSpec),
	Object {
		sql_name: String,
		object: Box<FilterObjectSpec>,
	},
	TableObject {
		sql_name: String,
		table_name: String,
		filter_type_name: String,
	},
	List {
		sql_name: String,
		list: Box<ListFilterSpec>,
	},
	Relation(Box<RelationListFilterSpec>),
}

#[derive(Clone)]
struct ScalarFilterSpec {
	sql_name: String,
	kind: Kind,
	enum_scope: Option<String>,
	filter_name: String,
}

#[derive(Clone)]
struct ListFilterSpec {
	type_name: String,
	description: String,
	field_kind: Kind,
	item: ListItemFilterSpec,
}

#[derive(Clone)]
enum ListItemFilterSpec {
	Scalar {
		kind: Kind,
		enum_scope: Option<String>,
		filter_name: String,
	},
	Object(Box<FilterObjectSpec>),
	TableObject {
		table_name: String,
		filter_type_name: String,
	},
}

#[derive(Clone)]
struct RelationListFilterSpec {
	type_name: String,
	description: String,
	relation_table_name: TableName,
	edge_filter: Box<FilterObjectSpec>,
}

pub(crate) type TableFilterRegistry = HashMap<String, FilterObjectSpec>;

fn field_list_filter_name(type_name: &str, field_name: &str) -> String {
	format!(
		"{}{}ListFilterInput",
		naming::to_pascal_case(type_name),
		naming::to_pascal_case(field_name)
	)
}

fn between_input_name(filter_name: &str) -> String {
	format!("{filter_name}BetweenInput")
}

fn scalar_field_description(gql_field_name: &str) -> String {
	format!("Filter by `{gql_field_name}`.")
}

fn build_scalar_filter_field_spec(
	sql_name: String,
	kind: Kind,
	description: String,
	types: &mut Vec<Type>,
	enum_scope: Option<String>,
) -> Result<FilterFieldSpec, GqlError> {
	let gql_name = naming::to_camel_case(&sql_name);
	let filter_name = ensure_scalar_filter_input_v2(&kind, types, enum_scope.as_deref())?;
	Ok(FilterFieldSpec {
		gql_name,
		description,
		kind: FilterFieldKind::Scalar(ScalarFilterSpec {
			sql_name,
			kind,
			enum_scope,
			filter_name,
		}),
	})
}

fn build_nested_filter_object_spec(
	node: &NestedFieldNode,
	types: &mut Vec<Type>,
	registry: &TableFilterRegistry,
) -> Result<FilterObjectSpec, GqlError> {
	let type_name = naming::scalar_filter_input_name(&node.type_name);
	let mut fields = Vec::new();
	for child in node.children.values() {
		let description =
			child.comment.clone().unwrap_or_else(|| scalar_field_description(&child.gql_name));
		if child.has_children() {
			let object = build_nested_filter_object_spec(child, types, registry)?;
			if child.is_array {
				fields.push(FilterFieldSpec {
					gql_name: child.gql_name.clone(),
					description,
					kind: FilterFieldKind::List {
						sql_name: child.sql_name.clone(),
						list: Box::new(ListFilterSpec {
							type_name: field_list_filter_name(&node.type_name, &child.gql_name),
							description: format!("List filter for `{}`.", child.gql_name),
							field_kind: child
								.kind
								.clone()
								.unwrap_or(Kind::Array(Box::new(Kind::Object), None)),
							item: ListItemFilterSpec::Object(Box::new(object)),
						}),
					},
				});
			} else {
				fields.push(FilterFieldSpec {
					gql_name: child.gql_name.clone(),
					description,
					kind: FilterFieldKind::Object {
						sql_name: child.sql_name.clone(),
						object: Box::new(object),
					},
				});
			}
			continue;
		}

		let Some(kind) = child.kind.clone() else {
			continue;
		};
		let enum_scope = Some(format!("{}_{}", node.type_name, child.sql_name));
		if let Some((item_kind, _)) = list_item_kind(&kind) {
			let item = match normalize_filter_kind(&item_kind) {
				Kind::Record(ts) if ts.len() == 1 => ListItemFilterSpec::TableObject {
					table_name: ts[0].to_string(),
					filter_type_name: filter_name_from_table(ts[0].as_str()),
				},
				_ => ListItemFilterSpec::Scalar {
					filter_name: ensure_scalar_filter_input_v2(
						&item_kind,
						types,
						enum_scope.as_deref(),
					)?,
					kind: item_kind,
					enum_scope,
				},
			};
			fields.push(FilterFieldSpec {
				gql_name: child.gql_name.clone(),
				description,
				kind: FilterFieldKind::List {
					sql_name: child.sql_name.clone(),
					list: Box::new(ListFilterSpec {
						type_name: field_list_filter_name(&node.type_name, &child.gql_name),
						description: format!("List filter for `{}`.", child.gql_name),
						field_kind: kind,
						item,
					}),
				},
			});
		} else if matches!(normalize_filter_kind(&kind), Kind::Record(ts) if ts.len() == 1) {
			let Kind::Record(ts) = normalize_filter_kind(&kind) else {
				unreachable!()
			};
			fields.push(FilterFieldSpec {
				gql_name: child.gql_name.clone(),
				description,
				kind: FilterFieldKind::TableObject {
					sql_name: child.sql_name.clone(),
					table_name: ts[0].to_string(),
					filter_type_name: filter_name_from_table(ts[0].as_str()),
				},
			});
		} else {
			fields.push(build_scalar_filter_field_spec(
				child.sql_name.clone(),
				kind,
				description,
				types,
				enum_scope,
			)?);
		}
	}

	let _ = registry;
	Ok(FilterObjectSpec {
		type_name,
		description: format!("Filter input for the nested `{}` object.", node.gql_name),
		fields,
	})
}

fn build_table_filter_spec(
	root_names: &RootTypeNames,
	tb_name_str: &str,
	gql_type_name: &str,
	fds: &[FieldDefinition],
	relations: &[RelationInfo],
	relation_type_contexts: &HashMap<String, RelationTypeContext>,
	types: &mut Vec<Type>,
	_registry: &TableFilterRegistry,
) -> Result<FilterObjectSpec, GqlError> {
	let nested_objects = detect_nested_objects(tb_name_str, fds);
	let mut fields = Vec::new();
	fields.push(build_scalar_filter_field_spec(
		"id".to_string(),
		Kind::Record(vec![]),
		"Filter records by record id.".to_string(),
		types,
		None,
	)?);

	for fd in fds {
		let Some(kind) = fd.field_kind.clone() else {
			continue;
		};
		if fd.name.is_id() || fd.name.0.len() > 1 {
			continue;
		}

		let sql_field_name = fd.name.to_sql();
		let gql_field_name = naming::to_camel_case(&sql_field_name);
		let description =
			fd.comment.clone().unwrap_or_else(|| scalar_field_description(&gql_field_name));

		if let Some(nested) = nested_objects.get(sql_field_name.as_str()) {
			let object = build_nested_filter_object_spec(nested, types, _registry)?;
			if nested.is_array {
				fields.push(FilterFieldSpec {
					gql_name: gql_field_name,
					description,
					kind: FilterFieldKind::List {
						sql_name: sql_field_name,
						list: Box::new(ListFilterSpec {
							type_name: field_list_filter_name(gql_type_name, &nested.gql_name),
							description: format!("List filter for `{}`.", nested.gql_name),
							field_kind: kind,
							item: ListItemFilterSpec::Object(Box::new(object)),
						}),
					},
				});
			} else {
				fields.push(FilterFieldSpec {
					gql_name: gql_field_name,
					description,
					kind: FilterFieldKind::Object {
						sql_name: sql_field_name,
						object: Box::new(object),
					},
				});
			}
			continue;
		}

		let enum_scope = Some(format!("{}_{}", tb_name_str, sql_field_name));
		if let Some((item_kind, _)) = list_item_kind(&kind) {
			let item = match normalize_filter_kind(&item_kind) {
				Kind::Record(ts) if ts.len() == 1 => ListItemFilterSpec::TableObject {
					table_name: ts[0].to_string(),
					filter_type_name: filter_name_from_table(ts[0].as_str()),
				},
				_ => ListItemFilterSpec::Scalar {
					filter_name: ensure_scalar_filter_input_v2(
						&item_kind,
						types,
						enum_scope.as_deref(),
					)?,
					kind: item_kind,
					enum_scope,
				},
			};
			fields.push(FilterFieldSpec {
				gql_name: gql_field_name.clone(),
				description,
				kind: FilterFieldKind::List {
					sql_name: sql_field_name,
					list: Box::new(ListFilterSpec {
						type_name: field_list_filter_name(gql_type_name, &gql_field_name),
						description: format!("List filter for `{gql_field_name}`."),
						field_kind: kind,
						item,
					}),
				},
			});
		} else if matches!(normalize_filter_kind(&kind), Kind::Record(ts) if ts.len() == 1) {
			let Kind::Record(ts) = normalize_filter_kind(&kind) else {
				unreachable!()
			};
			fields.push(FilterFieldSpec {
				gql_name: gql_field_name,
				description,
				kind: FilterFieldKind::TableObject {
					sql_name: sql_field_name,
					table_name: ts[0].to_string(),
					filter_type_name: filter_name_from_table(ts[0].as_str()),
				},
			});
		} else {
			fields.push(build_scalar_filter_field_spec(
				sql_field_name,
				kind,
				description,
				types,
				enum_scope,
			)?);
		}
	}

	for rel in relations {
		let rel_name = rel.table_name.clone().into_string();
		let Some(rel_ctx) = relation_type_contexts.get(&rel_name) else {
			continue;
		};
		if rel.from_tables.contains(&tb_name_str.to_string()) {
			fields.push(FilterFieldSpec {
				gql_name: rel_ctx.field_name.clone(),
				description: format!("Filter via outgoing `{}` relations.", rel_ctx.field_name),
				kind: FilterFieldKind::Relation(Box::new(rel_ctx.root_filter_spec.clone())),
			});
		}
	}

	Ok(FilterObjectSpec {
		type_name: root_names.filter_name.clone(),
		description: format!("Filter input for `{gql_type_name}` connections."),
		fields,
	})
}

fn build_relation_edge_filter_spec(
	relation_name: &str,
	field_name: &str,
	fds: &[FieldDefinition],
	node_table_name: Option<&str>,
	node_filter_name: Option<&str>,
	types: &mut Vec<Type>,
	registry: &TableFilterRegistry,
) -> Result<FilterObjectSpec, GqlError> {
	let nested_objects = detect_nested_objects(relation_name, fds);
	let mut fields = vec![build_scalar_filter_field_spec(
		"id".to_string(),
		Kind::Record(vec![]),
		"Filter relations by record id.".to_string(),
		types,
		None,
	)?];

	for fd in fds {
		let Some(kind) = fd.field_kind.clone() else {
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
		let description = fd
			.comment
			.clone()
			.unwrap_or_else(|| format!("Filter by `{gql_field_name}` relation metadata."));

		if let Some(nested) = nested_objects.get(sql_field_name.as_str()) {
			let object = build_nested_filter_object_spec(nested, types, registry)?;
			if nested.is_array {
				fields.push(FilterFieldSpec {
					gql_name: gql_field_name,
					description,
					kind: FilterFieldKind::List {
						sql_name: sql_field_name,
						list: Box::new(ListFilterSpec {
							type_name: field_list_filter_name(field_name, &nested.gql_name),
							description: format!(
								"List filter for `{}` relation metadata.",
								nested.gql_name
							),
							field_kind: kind,
							item: ListItemFilterSpec::Object(Box::new(object)),
						}),
					},
				});
			} else {
				fields.push(FilterFieldSpec {
					gql_name: gql_field_name,
					description,
					kind: FilterFieldKind::Object {
						sql_name: sql_field_name,
						object: Box::new(object),
					},
				});
			}
			continue;
		}

		let enum_scope = Some(format!("{}_{}", relation_name, sql_field_name));
		if let Some((item_kind, _)) = list_item_kind(&kind) {
			fields.push(FilterFieldSpec {
				gql_name: gql_field_name.clone(),
				description,
				kind: FilterFieldKind::List {
					sql_name: sql_field_name,
					list: Box::new(ListFilterSpec {
						type_name: field_list_filter_name(field_name, &gql_field_name),
						description: format!(
							"List filter for `{gql_field_name}` relation metadata."
						),
						field_kind: kind,
						item: ListItemFilterSpec::Scalar {
							filter_name: ensure_scalar_filter_input_v2(
								&item_kind,
								types,
								enum_scope.as_deref(),
							)?,
							kind: item_kind,
							enum_scope,
						},
					}),
				},
			});
		} else {
			fields.push(build_scalar_filter_field_spec(
				sql_field_name,
				kind,
				description,
				types,
				enum_scope,
			)?);
		}
	}

	if let (Some(table_name), Some(filter_type_name)) = (node_table_name, node_filter_name) {
		fields.push(FilterFieldSpec {
			gql_name: "node".to_string(),
			description: "Filter by fields on the related target node.".to_string(),
			kind: FilterFieldKind::TableObject {
				sql_name: "out".to_string(),
				table_name: table_name.to_string(),
				filter_type_name: filter_type_name.to_string(),
			},
		});
	}

	Ok(FilterObjectSpec {
		type_name: naming::filter_input_name(relation_name),
		description: format!("Filter input for `{field_name}` connections."),
		fields,
	})
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
	table_filter_registry: &TableFilterRegistry,
	types: &mut Vec<Type>,
) -> Result<Option<RelationTypeContext>, GqlError> {
	let Some(node_type_name) = relation_node_type_name(rel, exposed_table_names) else {
		return Ok(None);
	};

	let relation_name = rel.table_name.clone().into_string();
	let field_name = naming::to_camel_case(&relation_name);
	let node_table_name = (rel.to_tables.len() == 1).then(|| rel.to_tables[0].clone());
	let node_filter_name = node_table_name.as_deref().map(filter_name_from_table);
	let edge_filter_spec = build_relation_edge_filter_spec(
		&relation_name,
		&field_name,
		&fds,
		node_table_name.as_deref(),
		node_filter_name.as_deref(),
		types,
		table_filter_registry,
	)?;
	build_filter_object_input(&edge_filter_spec, types)?;
	let root_filter_spec = RelationListFilterSpec {
		type_name: field_list_filter_name(
			&naming::table_type_name(
				&rel.from_tables.first().cloned().unwrap_or_else(|| relation_name.clone()),
			),
			&field_name,
		),
		description: format!("Relation list filter for `{field_name}`."),
		relation_table_name: rel.table_name.clone(),
		edge_filter: Box::new(edge_filter_spec.clone()),
	};
	build_relation_list_filter_input(&root_filter_spec, types)?;
	let relation_ctx = RelationTypeContext {
		field_name: field_name.clone(),
		relation_table_name: rel.table_name.clone(),
		connection_type_name: naming::connection_type_name(&relation_name),
		edge_type_name: naming::edge_type_name(&relation_name),
		order_name: naming::order_input_name(&relation_name),
		filter_name: edge_filter_spec.type_name.clone(),
		node_type_name,
		fds: fds.clone(),
		edge_filter_spec,
		root_filter_spec,
	};

	let table_orderable_name = naming::order_field_enum_name(&relation_name);
	let mut orderable = Enum::new(&table_orderable_name)
		.item(EnumItem::new("ID").description("Order results by relation id."))
		.description(format!(
			"Fields that can be used to order `{}` connections.",
			relation_ctx.edge_type_name
		));

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
		if list_item_kind(kind).is_none() {
			orderable = orderable.item(
				EnumItem::new(&order_enum_item)
					.description(format!("Order results by `{gql_field_name}`.")),
			);
		}
	}

	if !has_type(types, &table_orderable_name) {
		types.push(Type::Enum(orderable));
	}
	if !has_type(types, &relation_ctx.order_name) {
		types.push(Type::InputObject(order));
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
	table_filter_registry: &TableFilterRegistry,
	types: &mut Vec<Type>,
	root_names: &RootTypeNames,
	include_relation_fields: bool,
) -> Result<TableGraphQLTypes, GqlError> {
	let tb_name = &tb.name;
	let tb_name_str = tb_name.clone().into_string();
	if !naming::is_valid_db_name(&tb_name_str) {
		return Err(resolver_error(format!(
			"Table `{tb_name_str}` cannot be exposed via GraphQL because it is not valid snake_case"
		)));
	}
	let gql_type_name = root_names.type_name.clone();

	// --- Create initial types ---

	let table_orderable_name = root_names.orderable_name.clone();
	let table_order_name = root_names.order_name.clone();
	let (connection_type_name, _) = make_named_connection_types(
		&root_names.entity_description_name,
		root_names.connection_type_name.clone(),
		naming::edge_type_name(&root_names.base_name),
		TypeRef::named_nn(&gql_type_name),
		types,
	);

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
	let filter_spec = build_table_filter_spec(
		root_names,
		&tb_name_str,
		&gql_type_name,
		fds,
		relations,
		relation_type_contexts,
		types,
		table_filter_registry,
	)?;
	build_filter_object_input(&filter_spec, types)?;

	let mut ty_obj = Object::new(&gql_type_name)
		.description(tb.comment.clone().unwrap_or_else(|| {
			format!("GraphQL object type for records in `{}`.", root_names.entity_description_name)
		}))
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

	if include_relation_fields {
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
	}

	Ok(TableGraphQLTypes {
		ty_obj,
		orderable,
		order,
		filter_spec,
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
	table_filter_registry_out: &mut TableFilterRegistry,
) -> Result<Object, GqlError> {
	let mut table_fds_cache: HashMap<String, Arc<[FieldDefinition]>> = HashMap::new();
	for tb in tbs.iter() {
		let fds = ctx.tx.all_tb_fields(ctx.ns, ctx.db, &tb.name, None).await?;
		table_fields.insert(tb.name.clone().into_string(), fds.clone());
		table_fds_cache.insert(tb.name.clone().into_string(), fds);
	}

	let mut table_filter_registry = TableFilterRegistry::new();
	for tb in tbs.iter() {
		let tb_name_str = tb.name.clone().into_string();
		let root_names = match &tb.table_type {
			TableType::Relation(_) => root_type_names_for_relation(&tb_name_str),
			_ => root_type_names_for_table(&tb_name_str),
		};
		table_filter_registry.insert(
			tb_name_str.clone(),
			FilterObjectSpec {
				type_name: root_names.filter_name.clone(),
				description: format!("Filter input for `{}` connections.", root_names.type_name),
				fields: Vec::new(),
			},
		);
	}

	// Set of exposed table names for checking that relation targets are visible
	let exposed_table_names: HashSet<String> = tbs
		.iter()
		.filter(|tb| !matches!(&tb.table_type, TableType::Relation(_)))
		.map(|t| t.name.clone().into_string())
		.collect();

	let mut relation_type_contexts = HashMap::new();
	for rel in relations.iter() {
		let rel_name = rel.table_name.clone().into_string();
		let Some(fds) = table_fds_cache.get(&rel_name).cloned() else {
			continue;
		};
		if let Some(context) = build_relation_type_context(
			rel,
			fds,
			&exposed_table_names,
			&table_filter_registry,
			types,
		)? {
			relation_type_contexts.insert(rel_name, context);
		}
	}

	for tb in tbs.iter() {
		trace!("Adding table: {}", tb.name);
		let fds = table_fds_cache
			.get(tb.name.as_str())
			.cloned()
			.unwrap_or_else(|| Arc::<[FieldDefinition]>::from([]));
		let tb_name_str = tb.name.clone().into_string();
		let root_names = match &tb.table_type {
			TableType::Relation(_) => root_type_names_for_relation(&tb_name_str),
			_ => root_type_names_for_table(&tb_name_str),
		};
		let include_relation_fields = !matches!(&tb.table_type, TableType::Relation(_));

		// Build and register the table's type system
		let tt = build_table_type(
			tb,
			&fds,
			relations,
			&relation_type_contexts,
			&table_filter_registry,
			types,
			&root_names,
			include_relation_fields,
		)?;
		table_filter_registry.insert(tb.name.clone().into_string(), tt.filter_spec.clone());
		query = query.field(make_table_list_field(
			tb,
			fds.clone(),
			ctx.datastore.clone(),
			root_names.clone(),
			tt.filter_spec.clone(),
		));
		query = query.field(make_table_get_field(tb, ctx.datastore.clone(), root_names));
		types.push(Type::Object(tt.ty_obj));
		types.push(tt.order.into());
		types.push(Type::Enum(tt.orderable));
	}

	*table_filter_registry_out = table_filter_registry;

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
			relation.edge_filter_spec.clone(),
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
	filter_spec: FilterObjectSpec,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	move |ctx: ResolverContext| {
		let relation_table = relation_table_name.clone();
		let relation_node_type_name = node_type_name.clone();
		let fds = rel_fds.clone();
		let filter_spec = filter_spec.clone();
		FieldFuture::new(async move {
			let ds = ctx.data::<Arc<Datastore>>()?;
			let sess = ctx.data::<Arc<Session>>()?;
			let table_filter_registry = ctx.data::<Arc<TableFilterRegistry>>()?;

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
			if let Some(user_cond) =
				parse_filter_arg(args, &filter_spec, table_filter_registry.as_ref())?
			{
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

fn normalize_filter_kind(kind: &Kind) -> Kind {
	match kind {
		Kind::Literal(literal) => match literal {
			KindLiteral::String(_) => kind.clone(),
			KindLiteral::Integer(_) => Kind::Int,
			KindLiteral::Float(_) => Kind::Float,
			KindLiteral::Decimal(_) => Kind::Decimal,
			KindLiteral::Duration(_) => Kind::Duration,
			KindLiteral::Bool(_) => Kind::Bool,
			KindLiteral::Object(_) => Kind::Object,
			KindLiteral::Array(kinds) => {
				let len = Some(kinds.len() as u64);
				if let Some(first) = kinds.first()
					&& kinds.iter().all(|kind| kind == first)
				{
					Kind::Array(Box::new(normalize_filter_kind(first)), len)
				} else {
					Kind::Array(Box::new(Kind::Any), len)
				}
			}
		},
		Kind::Either(ks) => {
			let non_none =
				ks.iter().filter(|k| !matches!(k, Kind::None | Kind::Null)).collect::<Vec<_>>();
			if non_none.iter().all(|kind| matches!(kind, Kind::Literal(KindLiteral::String(_)))) {
				return kind.clone();
			}
			let non_none = non_none.into_iter().map(normalize_filter_kind).collect::<Vec<_>>();
			match non_none.as_slice() {
				[single] => single.clone(),
				_ => Kind::Either(non_none),
			}
		}
		_ => kind.clone(),
	}
}

fn is_ordered_filter_kind(kind: &Kind) -> bool {
	matches!(
		normalize_filter_kind(kind),
		Kind::Int | Kind::Float | Kind::Number | Kind::Decimal | Kind::Datetime | Kind::Duration
	)
}

fn is_string_filter_kind(kind: &Kind) -> bool {
	matches!(normalize_filter_kind(kind), Kind::String)
}

fn is_geometry_filter_kind(kind: &Kind) -> bool {
	matches!(normalize_filter_kind(kind), Kind::Geometry(_))
}

fn supports_inside_ops(kind: &Kind) -> bool {
	matches!(
		normalize_filter_kind(kind),
		Kind::String
			| Kind::Int
			| Kind::Float
			| Kind::Number
			| Kind::Decimal
			| Kind::Datetime
			| Kind::Duration
			| Kind::Uuid
			| Kind::Record(_)
	)
}

fn list_membership_supported(item: &ListItemFilterSpec) -> bool {
	matches!(item, ListItemFilterSpec::Scalar { .. })
}

fn scalar_input_type_name(
	kind: &Kind,
	types: &mut Vec<Type>,
	enum_scope: Option<&str>,
) -> Result<String, GqlError> {
	let effective_kind = normalize_filter_kind(kind);
	let name = match &effective_kind {
		Kind::Record(ts) => match ts.len() {
			1 => return Ok(filter_name_from_table(ts[0].as_str())),
			_ => TypeRef::ID.to_string(),
		},
		k => unwrap_type(kind_to_type_with_enum_prefix(k.clone(), types, true, enum_scope)?)
			.to_string(),
	};
	Ok(name)
}

fn ensure_between_input(filter_name: &str, value_type_name: &str, types: &mut Vec<Type>) -> String {
	let between_name = between_input_name(filter_name);
	if !has_type(types, &between_name) {
		types.push(Type::InputObject(
			InputObject::new(&between_name)
				.description(format!("Inclusive range bounds for `{filter_name}` filters."))
				.field(
					InputValue::new("gte", TypeRef::named(value_type_name))
						.description("Match values greater than or equal to this bound."),
				)
				.field(
					InputValue::new("lte", TypeRef::named(value_type_name))
						.description("Match values less than or equal to this bound."),
				),
		));
	}
	between_name
}

fn ensure_search_filter_support(types: &mut Vec<Type>) {
	if !has_type(types, "SearchBooleanMode") {
		types.push(Type::Enum(
			Enum::new("SearchBooleanMode")
				.description("Boolean mode used by SurrealDB full-text `matches` filters.")
				.item(EnumItem::new("AND").description("Require all search terms to match."))
				.item(EnumItem::new("OR").description("Require any search term to match.")),
		));
	}
	if !has_type(types, "SearchMatchInput") {
		types.push(Type::InputObject(
			InputObject::new("SearchMatchInput")
				.description("Full-text search criteria for SurrealDB `matches` filters.")
				.field(
					InputValue::new("query", TypeRef::named_nn(TypeRef::STRING))
						.description("The full-text query string."),
				)
				.field(
					InputValue::new("reference", TypeRef::named(TypeRef::INT))
						.description("Optional full-text match reference number."),
				)
				.field(
					InputValue::new("mode", TypeRef::named("SearchBooleanMode"))
						.description("Whether search terms are combined with AND or OR semantics."),
				),
		));
	}
}

fn ensure_scalar_filter_input_v2(
	kind: &Kind,
	types: &mut Vec<Type>,
	enum_scope: Option<&str>,
) -> Result<String, GqlError> {
	let filter_name =
		naming::scalar_filter_input_name(&scalar_input_type_name(kind, types, enum_scope)?);
	if has_type(types, &filter_name) {
		return Ok(filter_name);
	}

	let effective_kind = normalize_filter_kind(kind);
	let value_type_name = scalar_input_type_name(&effective_kind, types, enum_scope)?;
	let mut filter = InputObject::new(&filter_name)
		.description(format!("Filter operations available for `{value_type_name}` values."))
		.field(
			InputValue::new("eq", TypeRef::named(&value_type_name))
				.description("Match values equal to the provided value."),
		)
		.field(
			InputValue::new("exactEq", TypeRef::named(&value_type_name))
				.description("Match values using SurrealDB exact equality semantics."),
		)
		.field(
			InputValue::new("ne", TypeRef::named(&value_type_name))
				.description("Exclude values equal to the provided value."),
		)
		.field(
			InputValue::new("in", TypeRef::named_nn_list(&value_type_name))
				.description("Match any value contained in the provided list."),
		)
		.field(
			InputValue::new("notIn", TypeRef::named_nn_list(&value_type_name))
				.description("Exclude values contained in the provided list."),
		)
		.field(
			InputValue::new("isNull", TypeRef::named(TypeRef::BOOLEAN))
				.description("Match whether the value is `null`."),
		)
		.field(
			InputValue::new("isNone", TypeRef::named(TypeRef::BOOLEAN))
				.description("Match whether the value is SurrealDB `NONE`."),
		)
		.field(
			InputValue::new("exists", TypeRef::named(TypeRef::BOOLEAN))
				.description("Match whether the field exists with a non-`NONE` value."),
		)
		.field(
			InputValue::new("allEq", TypeRef::named(&value_type_name))
				.description("Match values where all members equal the provided value."),
		)
		.field(
			InputValue::new("anyEq", TypeRef::named(&value_type_name))
				.description("Match values where any member equals the provided value."),
		);

	if is_ordered_filter_kind(&effective_kind) {
		let between_name = ensure_between_input(&filter_name, &value_type_name, types);
		filter = filter
			.field(
				InputValue::new("gt", TypeRef::named(&value_type_name))
					.description("Match values greater than the provided value."),
			)
			.field(
				InputValue::new("gte", TypeRef::named(&value_type_name))
					.description("Match values greater than or equal to the provided value."),
			)
			.field(
				InputValue::new("lt", TypeRef::named(&value_type_name))
					.description("Match values less than the provided value."),
			)
			.field(
				InputValue::new("lte", TypeRef::named(&value_type_name))
					.description("Match values less than or equal to the provided value."),
			)
			.field(
				InputValue::new("between", TypeRef::named(&between_name))
					.description("Match values within an inclusive range."),
			);
	}

	if is_string_filter_kind(&effective_kind) {
		ensure_search_filter_support(types);
		filter = filter
			.field(
				InputValue::new("contains", TypeRef::named(TypeRef::STRING))
					.description("Match strings containing the provided substring."),
			)
			.field(
				InputValue::new("notContains", TypeRef::named(TypeRef::STRING))
					.description("Exclude strings containing the provided substring."),
			)
			.field(
				InputValue::new("startsWith", TypeRef::named(TypeRef::STRING))
					.description("Match strings starting with the provided prefix."),
			)
			.field(
				InputValue::new("endsWith", TypeRef::named(TypeRef::STRING))
					.description("Match strings ending with the provided suffix."),
			)
			.field(
				InputValue::new("like", TypeRef::named(TypeRef::STRING))
					.description("Match strings using SQL-like `%` and `_` wildcards."),
			)
			.field(
				InputValue::new("notLike", TypeRef::named(TypeRef::STRING))
					.description("Exclude strings matching the provided wildcard pattern."),
			)
			.field(
				InputValue::new("allLike", TypeRef::named(TypeRef::STRING))
					.description("Match values where every member matches the wildcard pattern."),
			)
			.field(
				InputValue::new("anyLike", TypeRef::named(TypeRef::STRING))
					.description("Match values where any member matches the wildcard pattern."),
			)
			.field(
				InputValue::new("regex", TypeRef::named(TypeRef::STRING))
					.description("Match strings against a regular expression."),
			)
			.field(
				InputValue::new("matches", TypeRef::named("SearchMatchInput"))
					.description("Match strings using SurrealDB full-text search."),
			);
	}

	if is_geometry_filter_kind(&effective_kind) {
		filter = filter
			.field(
				InputValue::new("contains", TypeRef::named(&value_type_name))
					.description("Match geometries that contain the provided geometry."),
			)
			.field(
				InputValue::new("inside", TypeRef::named(&value_type_name))
					.description("Match geometries inside the provided geometry."),
			)
			.field(
				InputValue::new("outside", TypeRef::named(&value_type_name))
					.description("Match geometries outside the provided geometry."),
			)
			.field(
				InputValue::new("intersects", TypeRef::named(&value_type_name))
					.description("Match geometries that intersect the provided geometry."),
			);
	} else if supports_inside_ops(&effective_kind) {
		filter = filter
			.field(
				InputValue::new("inside", TypeRef::named_nn_list(&value_type_name))
					.description("Match values inside the provided collection."),
			)
			.field(
				InputValue::new("notInside", TypeRef::named_nn_list(&value_type_name))
					.description("Exclude values inside the provided collection."),
			)
			.field(
				InputValue::new("allInside", TypeRef::named_nn_list(&value_type_name)).description(
					"Match values where all members are inside the provided collection.",
				),
			)
			.field(
				InputValue::new("anyInside", TypeRef::named_nn_list(&value_type_name)).description(
					"Match values where any member is inside the provided collection.",
				),
			)
			.field(
				InputValue::new("noneInside", TypeRef::named_nn_list(&value_type_name))
					.description(
						"Match values where no members are inside the provided collection.",
					),
			);
	}

	types.push(Type::InputObject(filter));
	Ok(filter_name)
}

fn build_filter_object_input(
	spec: &FilterObjectSpec,
	types: &mut Vec<Type>,
) -> Result<(), GqlError> {
	if has_type(types, &spec.type_name) {
		return Ok(());
	}

	let mut input = InputObject::new(&spec.type_name)
		.description(spec.description.clone())
		.field(
			InputValue::new("and", TypeRef::named_nn_list(&spec.type_name))
				.description("Combine multiple filters with a logical AND."),
		)
		.field(
			InputValue::new("or", TypeRef::named_nn_list(&spec.type_name))
				.description("Combine multiple filters with a logical OR."),
		)
		.field(
			InputValue::new("not", TypeRef::named(&spec.type_name))
				.description("Negate the nested filter expression."),
		);

	for field in &spec.fields {
		let type_name = match &field.kind {
			FilterFieldKind::Scalar(spec) => {
				ensure_scalar_filter_input_v2(&spec.kind, types, spec.enum_scope.as_deref())?
			}
			FilterFieldKind::Object {
				object,
				..
			} => {
				build_filter_object_input(object, types)?;
				object.type_name.clone()
			}
			FilterFieldKind::TableObject {
				filter_type_name,
				..
			} => filter_type_name.clone(),
			FilterFieldKind::List {
				list,
				..
			} => {
				build_list_filter_input(list, types)?;
				list.type_name.clone()
			}
			FilterFieldKind::Relation(relation) => {
				build_relation_list_filter_input(relation, types)?;
				relation.type_name.clone()
			}
		};

		input = input.field(
			InputValue::new(&field.gql_name, TypeRef::named(&type_name))
				.description(field.description.clone()),
		);
	}

	types.push(Type::InputObject(input));
	Ok(())
}

fn build_list_filter_input(list: &ListFilterSpec, types: &mut Vec<Type>) -> Result<(), GqlError> {
	if has_type(types, &list.type_name) {
		return Ok(());
	}

	let item_type_name = match &list.item {
		ListItemFilterSpec::Scalar {
			kind,
			enum_scope,
			..
		} => ensure_scalar_filter_input_v2(kind, types, enum_scope.as_deref())?,
		ListItemFilterSpec::Object(object) => {
			build_filter_object_input(object, types)?;
			object.type_name.clone()
		}
		ListItemFilterSpec::TableObject {
			filter_type_name,
			..
		} => filter_type_name.clone(),
	};

	let mut input = InputObject::new(&list.type_name)
		.description(list.description.clone())
		.field(
			InputValue::new("some", TypeRef::named(&item_type_name))
				.description("Match when at least one list item satisfies the nested filter."),
		)
		.field(
			InputValue::new("every", TypeRef::named(&item_type_name))
				.description("Match when every list item satisfies the nested filter."),
		)
		.field(
			InputValue::new("none", TypeRef::named(&item_type_name))
				.description("Match when no list items satisfy the nested filter."),
		);

	if list_membership_supported(&list.item)
		&& let ListItemFilterSpec::Scalar {
			kind,
			enum_scope,
			..
		} = &list.item
	{
		let value_type_name = scalar_input_type_name(kind, types, enum_scope.as_deref())?;
		input = input
			.field(
				InputValue::new("contains", TypeRef::named(&value_type_name))
					.description("Match lists containing the provided value."),
			)
			.field(
				InputValue::new("containsNot", TypeRef::named(&value_type_name))
					.description("Match lists that do not contain the provided value."),
			)
			.field(
				InputValue::new("containsAll", TypeRef::named_nn_list(&value_type_name))
					.description("Match lists containing all provided values."),
			)
			.field(
				InputValue::new("containsAny", TypeRef::named_nn_list(&value_type_name))
					.description("Match lists containing any provided values."),
			)
			.field(
				InputValue::new("containsNone", TypeRef::named_nn_list(&value_type_name))
					.description("Match lists containing none of the provided values."),
			);
	}

	types.push(Type::InputObject(input));
	Ok(())
}

fn build_relation_list_filter_input(
	relation: &RelationListFilterSpec,
	types: &mut Vec<Type>,
) -> Result<(), GqlError> {
	if has_type(types, &relation.type_name) {
		return Ok(());
	}
	build_filter_object_input(&relation.edge_filter, types)?;
	types.push(Type::InputObject(
		InputObject::new(&relation.type_name)
			.description(relation.description.clone())
			.field(
				InputValue::new("some", TypeRef::named(&relation.edge_filter.type_name))
					.description(
						"Match when at least one relation edge satisfies the nested filter.",
					),
			)
			.field(
				InputValue::new("every", TypeRef::named(&relation.edge_filter.type_name))
					.description("Match when every relation edge satisfies the nested filter."),
			)
			.field(
				InputValue::new("none", TypeRef::named(&relation.edge_filter.type_name))
					.description("Match when no relation edges satisfy the nested filter."),
			),
	));
	Ok(())
}

fn append_field(prefix: Option<&Idiom>, sql_name: &str) -> Idiom {
	let mut idiom = prefix.cloned().unwrap_or_else(|| Idiom::field(sql_name.to_string()));
	if prefix.is_some() {
		idiom = idiom.push(Part::Field(sql_name.to_string()));
	}
	idiom
}

fn combine_with_and(mut exprs: Vec<Expr>) -> Result<Expr, GqlError> {
	let mut iter = exprs.drain(..);
	let mut combined =
		iter.next().ok_or_else(|| resolver_error("Filter must contain at least one expression"))?;
	for next in iter {
		combined = Expr::Binary {
			left: Box::new(combined),
			op: BinaryOperator::And,
			right: Box::new(next),
		};
	}
	Ok(combined)
}

fn combine_with_or(mut exprs: Vec<Expr>) -> Result<Expr, GqlError> {
	let mut iter = exprs.drain(..);
	let mut combined =
		iter.next().ok_or_else(|| resolver_error("Filter must contain at least one expression"))?;
	for next in iter {
		combined = Expr::Binary {
			left: Box::new(combined),
			op: BinaryOperator::Or,
			right: Box::new(next),
		};
	}
	Ok(combined)
}

fn cond_from_filter_spec(
	filter: &IndexMap<Name, GqlValue>,
	spec: &FilterObjectSpec,
	registry: &TableFilterRegistry,
) -> Result<Cond, GqlError> {
	Ok(Cond(expr_from_filter_spec(filter, spec, registry, None)?))
}

fn expr_from_filter_spec(
	filter: &IndexMap<Name, GqlValue>,
	spec: &FilterObjectSpec,
	registry: &TableFilterRegistry,
	prefix: Option<&Idiom>,
) -> Result<Expr, GqlError> {
	if filter.is_empty() {
		return Err(resolver_error("Filter must have at least one item"));
	}

	let mut exprs = Vec::with_capacity(filter.len());
	for (key, value) in filter {
		match key.as_str() {
			"and" => exprs.push(aggregate_v2(value, spec, registry, prefix, true)?),
			"or" => exprs.push(aggregate_v2(value, spec, registry, prefix, false)?),
			"not" => {
				let obj = value
					.as_object()
					.ok_or_else(|| resolver_error("Value of `not` must be an object"))?;
				exprs.push(Expr::Prefix {
					op: expr::PrefixOperator::Not,
					expr: Box::new(expr_from_filter_spec(obj, spec, registry, prefix)?),
				});
			}
			field_name => {
				let field =
					spec.fields
						.iter()
						.find(|candidate| candidate.gql_name == field_name)
						.ok_or_else(|| resolver_error(format!("Field `{field_name}` not found")))?;
				exprs.push(expr_from_filter_field(field, value, registry, prefix)?);
			}
		}
	}

	combine_with_and(exprs)
}

fn aggregate_v2(
	value: &GqlValue,
	spec: &FilterObjectSpec,
	registry: &TableFilterRegistry,
	prefix: Option<&Idiom>,
	is_and: bool,
) -> Result<Expr, GqlError> {
	let list = value.as_list().ok_or_else(|| {
		resolver_error(if is_and {
			"Value of `and` must be a list"
		} else {
			"Value of `or` must be a list"
		})
	})?;
	let mut exprs = Vec::with_capacity(list.len());
	for item in list {
		let obj = item
			.as_object()
			.ok_or_else(|| resolver_error("Logical filter lists must contain objects"))?;
		exprs.push(expr_from_filter_spec(obj, spec, registry, prefix)?);
	}
	if is_and {
		combine_with_and(exprs)
	} else {
		combine_with_or(exprs)
	}
}

fn expr_from_filter_field(
	field: &FilterFieldSpec,
	value: &GqlValue,
	registry: &TableFilterRegistry,
	prefix: Option<&Idiom>,
) -> Result<Expr, GqlError> {
	match &field.kind {
		FilterFieldKind::Scalar(spec) => {
			let obj = value
				.as_object()
				.ok_or_else(|| resolver_error("Field filter should be an object"))?;
			let lhs = Expr::Idiom(append_field(prefix, &spec.sql_name));
			expr_from_scalar_filter(obj, lhs, &spec.kind, spec.enum_scope.as_deref())
		}
		FilterFieldKind::Object {
			sql_name,
			object,
		} => {
			let obj = value
				.as_object()
				.ok_or_else(|| resolver_error("Nested object filter should be an object"))?;
			let next_prefix = append_field(prefix, sql_name);
			expr_from_filter_spec(obj, object, registry, Some(&next_prefix))
		}
		FilterFieldKind::TableObject {
			sql_name,
			table_name,
			..
		} => {
			let obj = value
				.as_object()
				.ok_or_else(|| resolver_error("Record-link filter should be an object"))?;
			let next_prefix = append_field(prefix, sql_name);
			let target = registry.get(table_name).ok_or_else(|| {
				resolver_error(format!(
					"No filter metadata registered for linked table `{table_name}`"
				))
			})?;
			expr_from_filter_spec(obj, target, registry, Some(&next_prefix))
		}
		FilterFieldKind::List {
			sql_name,
			list,
		} => {
			let obj = value
				.as_object()
				.ok_or_else(|| resolver_error("List filter should be an object"))?;
			let lhs = append_field(prefix, sql_name);
			expr_from_list_filter(obj, list, registry, &lhs)
		}
		FilterFieldKind::Relation(relation) => {
			let obj = value
				.as_object()
				.ok_or_else(|| resolver_error("Relation filter should be an object"))?;
			expr_from_relation_list_filter(obj, relation, registry)
		}
	}
}

fn expr_from_scalar_filter(
	filter: &IndexMap<Name, GqlValue>,
	lhs: Expr,
	kind: &Kind,
	enum_scope: Option<&str>,
) -> Result<Expr, GqlError> {
	if filter.is_empty() {
		return Err(resolver_error("Field filter must have at least one operator"));
	}
	let field_kind = normalize_filter_kind(kind);
	let mut exprs = Vec::with_capacity(filter.len());

	for (op, value) in filter {
		let expr = match op.as_str() {
			"eq" => make_binary_expr(
				lhs.clone(),
				BinaryOperator::Equal,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			"exactEq" => make_binary_expr(
				lhs.clone(),
				BinaryOperator::ExactEqual,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			"ne" => make_binary_expr(
				lhs.clone(),
				BinaryOperator::NotEqual,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			"in" => make_binary_expr(
				lhs.clone(),
				BinaryOperator::Inside,
				value,
				Kind::Array(Box::new(field_kind.clone()), None),
				enum_scope,
			)?,
			"notIn" => make_binary_expr(
				lhs.clone(),
				BinaryOperator::NotInside,
				value,
				Kind::Array(Box::new(field_kind.clone()), None),
				enum_scope,
			)?,
			"isNull" => make_boolean_match(lhs.clone(), value, Literal::Null)?,
			"isNone" => make_boolean_match(lhs.clone(), value, Literal::None)?,
			"exists" => make_exists_expr(lhs.clone(), value)?,
			"gt" => make_binary_expr(
				lhs.clone(),
				BinaryOperator::MoreThan,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			"gte" => make_binary_expr(
				lhs.clone(),
				BinaryOperator::MoreThanEqual,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			"lt" => make_binary_expr(
				lhs.clone(),
				BinaryOperator::LessThan,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			"lte" => make_binary_expr(
				lhs.clone(),
				BinaryOperator::LessThanEqual,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			"between" => make_between_expr(lhs.clone(), value, field_kind.clone(), enum_scope)?,
			"allEq" => make_binary_expr(
				lhs.clone(),
				BinaryOperator::AllEqual,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			"anyEq" => make_binary_expr(
				lhs.clone(),
				BinaryOperator::AnyEqual,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			"contains" => make_contains_expr(lhs.clone(), value, &field_kind, enum_scope, false)?,
			"notContains" => make_contains_expr(lhs.clone(), value, &field_kind, enum_scope, true)?,
			"startsWith" => make_string_function_expr("string::starts_with", lhs.clone(), value)?,
			"endsWith" => make_string_function_expr("string::ends_with", lhs.clone(), value)?,
			"like" => make_like_expr(lhs.clone(), value, false, false)?,
			"notLike" => make_like_expr(lhs.clone(), value, true, false)?,
			"allLike" => make_like_expr(lhs.clone(), value, false, true)?,
			"anyLike" => make_like_expr(lhs.clone(), value, false, true)?,
			"regex" => make_string_function_expr("string::matches", lhs.clone(), value)?,
			"matches" => make_search_match_expr(lhs.clone(), value)?,
			"inside" => make_inside_expr(
				lhs.clone(),
				BinaryOperator::Inside,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			"notInside" => make_inside_expr(
				lhs.clone(),
				BinaryOperator::NotInside,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			"allInside" => make_inside_expr(
				lhs.clone(),
				BinaryOperator::AllInside,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			"anyInside" => make_inside_expr(
				lhs.clone(),
				BinaryOperator::AnyInside,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			"noneInside" => make_inside_expr(
				lhs.clone(),
				BinaryOperator::NoneInside,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			"outside" => make_binary_expr(
				lhs.clone(),
				BinaryOperator::Outside,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			"intersects" => make_binary_expr(
				lhs.clone(),
				BinaryOperator::Intersects,
				value,
				field_kind.clone(),
				enum_scope,
			)?,
			other => return Err(resolver_error(format!("Unsupported filter operator: {other}"))),
		};
		exprs.push(expr);
	}

	combine_with_and(exprs)
}

fn make_binary_expr(
	lhs: Expr,
	op: BinaryOperator,
	value: &GqlValue,
	kind: Kind,
	enum_scope: Option<&str>,
) -> Result<Expr, GqlError> {
	let rhs = gql_to_sql_kind_with_scope(value, kind, enum_scope)?;
	Ok(Expr::Binary {
		left: Box::new(lhs),
		op,
		right: Box::new(rhs.into_literal()),
	})
}

fn make_boolean_match(lhs: Expr, value: &GqlValue, literal: Literal) -> Result<Expr, GqlError> {
	let flag = match value {
		GqlValue::Boolean(flag) => *flag,
		_ => return Err(resolver_error("Boolean filter operators require a boolean value")),
	};
	let eq = Expr::Binary {
		left: Box::new(lhs),
		op: BinaryOperator::Equal,
		right: Box::new(Expr::Literal(literal)),
	};
	if flag {
		Ok(eq)
	} else {
		Ok(Expr::Prefix {
			op: expr::PrefixOperator::Not,
			expr: Box::new(eq),
		})
	}
}

fn make_exists_expr(lhs: Expr, value: &GqlValue) -> Result<Expr, GqlError> {
	let flag = match value {
		GqlValue::Boolean(flag) => *flag,
		_ => return Err(resolver_error("`exists` requires a boolean value")),
	};
	let expr = Expr::Binary {
		left: Box::new(lhs),
		op: BinaryOperator::NotEqual,
		right: Box::new(Expr::Literal(Literal::None)),
	};
	if flag {
		Ok(expr)
	} else {
		Ok(Expr::Prefix {
			op: expr::PrefixOperator::Not,
			expr: Box::new(expr),
		})
	}
}

fn make_between_expr(
	lhs: Expr,
	value: &GqlValue,
	kind: Kind,
	enum_scope: Option<&str>,
) -> Result<Expr, GqlError> {
	let obj = value.as_object().ok_or_else(|| resolver_error("`between` must be an object"))?;
	let mut exprs = Vec::new();
	if let Some(gte) = obj.get("gte") {
		exprs.push(make_binary_expr(
			lhs.clone(),
			BinaryOperator::MoreThanEqual,
			gte,
			kind.clone(),
			enum_scope,
		)?);
	}
	if let Some(lte) = obj.get("lte") {
		exprs.push(make_binary_expr(lhs, BinaryOperator::LessThanEqual, lte, kind, enum_scope)?);
	}
	if exprs.is_empty() {
		return Err(resolver_error("`between` must include at least one bound"));
	}
	combine_with_and(exprs)
}

fn make_string_function_expr(fn_name: &str, lhs: Expr, value: &GqlValue) -> Result<Expr, GqlError> {
	let rhs = gql_to_sql_kind(value, Kind::String)?;
	Ok(Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal(fn_name.to_string()),
		arguments: vec![lhs, rhs.into_literal()],
	})))
}

fn escape_like_pattern(pattern: &str) -> String {
	let mut out = String::from("^");
	for ch in pattern.chars() {
		match ch {
			'%' => out.push_str(".*"),
			'_' => out.push('.'),
			'.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' | '\\' => {
				out.push('\\');
				out.push(ch);
			}
			other => out.push(other),
		}
	}
	out.push('$');
	out
}

fn make_like_expr(
	lhs: Expr,
	value: &GqlValue,
	negate: bool,
	use_collection_op: bool,
) -> Result<Expr, GqlError> {
	let pattern = value
		.as_string()
		.ok_or_else(|| resolver_error("LIKE operators require a string pattern"))?;
	let regex = GqlValue::String(escape_like_pattern(&pattern));
	let expr = if use_collection_op {
		make_string_function_expr("string::matches", lhs, &regex)?
	} else {
		make_string_function_expr("string::matches", lhs, &regex)?
	};
	if negate {
		Ok(Expr::Prefix {
			op: expr::PrefixOperator::Not,
			expr: Box::new(expr),
		})
	} else {
		Ok(expr)
	}
}

fn make_search_match_expr(lhs: Expr, value: &GqlValue) -> Result<Expr, GqlError> {
	let obj = value.as_object().ok_or_else(|| resolver_error("`matches` must be an object"))?;
	let query = obj
		.get("query")
		.and_then(GqlValueUtils::as_string)
		.ok_or_else(|| resolver_error("`matches.query` is required"))?;
	let rf = obj
		.get("reference")
		.and_then(GqlValue::as_i64)
		.map(|v| u8::try_from(v).map_err(|_| resolver_error("`matches.reference` must fit in u8")))
		.transpose()?;
	let operator = match obj
		.get("mode")
		.and_then(|value| match value {
			GqlValue::Enum(name) => Some(name.as_str()),
			_ => None,
		})
		.unwrap_or("AND")
	{
		"AND" => BooleanOperator::And,
		"OR" => BooleanOperator::Or,
		other => return Err(resolver_error(format!("Unknown search boolean mode `{other}`"))),
	};
	Ok(Expr::Binary {
		left: Box::new(lhs),
		op: BinaryOperator::Matches(expr::operator::MatchesOperator {
			rf,
			operator,
		}),
		right: Box::new(Expr::Literal(Literal::String(query))),
	})
}

fn make_contains_expr(
	lhs: Expr,
	value: &GqlValue,
	kind: &Kind,
	enum_scope: Option<&str>,
	negate: bool,
) -> Result<Expr, GqlError> {
	if is_string_filter_kind(kind) {
		let expr = make_string_function_expr("string::contains", lhs, value)?;
		if negate {
			return Ok(Expr::Prefix {
				op: expr::PrefixOperator::Not,
				expr: Box::new(expr),
			});
		}
		return Ok(expr);
	}

	make_binary_expr(
		lhs,
		if negate {
			BinaryOperator::NotContain
		} else {
			BinaryOperator::Contain
		},
		value,
		kind.clone(),
		enum_scope,
	)
}

fn make_inside_expr(
	lhs: Expr,
	op: BinaryOperator,
	value: &GqlValue,
	kind: Kind,
	enum_scope: Option<&str>,
) -> Result<Expr, GqlError> {
	make_binary_expr(lhs, op, value, Kind::Array(Box::new(kind), None), enum_scope)
}

fn expr_from_list_item_filter(
	value: &GqlValue,
	item: &ListItemFilterSpec,
	registry: &TableFilterRegistry,
) -> Result<Expr, GqlError> {
	match item {
		ListItemFilterSpec::Scalar {
			kind,
			enum_scope,
			..
		} => {
			let obj = value
				.as_object()
				.ok_or_else(|| resolver_error("List item filter must be an object"))?;
			expr_from_scalar_filter(
				obj,
				Expr::Param(Param::new("this".to_string())),
				kind,
				enum_scope.as_deref(),
			)
		}
		ListItemFilterSpec::Object(object) => {
			let obj = value
				.as_object()
				.ok_or_else(|| resolver_error("List item filter must be an object"))?;
			expr_from_filter_spec(obj, object, registry, None)
		}
		ListItemFilterSpec::TableObject {
			table_name,
			..
		} => {
			let obj = value
				.as_object()
				.ok_or_else(|| resolver_error("List item filter must be an object"))?;
			let spec = registry.get(table_name).ok_or_else(|| {
				resolver_error(format!(
					"No filter metadata registered for linked table `{table_name}`"
				))
			})?;
			expr_from_filter_spec(obj, spec, registry, None)
		}
	}
}

fn where_filtered_idiom(base: &Idiom, predicate: Expr) -> Expr {
	let mut idiom = base.clone();
	idiom = idiom.push(Part::Where(predicate));
	Expr::Idiom(idiom)
}

fn array_len_expr(expr: Expr) -> Expr {
	Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal("array::len".to_string()),
		arguments: vec![expr],
	}))
}

fn expr_from_list_filter(
	filter: &IndexMap<Name, GqlValue>,
	list: &ListFilterSpec,
	registry: &TableFilterRegistry,
	field_idiom: &Idiom,
) -> Result<Expr, GqlError> {
	if filter.is_empty() {
		return Err(resolver_error("List filter must have at least one operator"));
	}
	let mut exprs = Vec::new();
	for (op, value) in filter {
		let expr = match op.as_str() {
			"some" => {
				let predicate = expr_from_list_item_filter(value, &list.item, registry)?;
				Expr::Binary {
					left: Box::new(array_len_expr(where_filtered_idiom(field_idiom, predicate))),
					op: BinaryOperator::MoreThan,
					right: Box::new(Expr::Literal(Literal::Integer(0))),
				}
			}
			"none" => {
				let predicate = expr_from_list_item_filter(value, &list.item, registry)?;
				Expr::Binary {
					left: Box::new(array_len_expr(where_filtered_idiom(field_idiom, predicate))),
					op: BinaryOperator::Equal,
					right: Box::new(Expr::Literal(Literal::Integer(0))),
				}
			}
			"every" => {
				let predicate = expr_from_list_item_filter(value, &list.item, registry)?;
				let inverted = Expr::Prefix {
					op: expr::PrefixOperator::Not,
					expr: Box::new(predicate),
				};
				Expr::Binary {
					left: Box::new(array_len_expr(where_filtered_idiom(field_idiom, inverted))),
					op: BinaryOperator::Equal,
					right: Box::new(Expr::Literal(Literal::Integer(0))),
				}
			}
			"contains" | "containsNot" | "containsAll" | "containsAny" | "containsNone" => {
				let ListItemFilterSpec::Scalar {
					kind,
					enum_scope,
					..
				} = &list.item
				else {
					return Err(resolver_error(format!(
						"Operator `{op}` is only supported for scalar list filters"
					)));
				};
				let rhs_kind = if matches!(op.as_str(), "contains" | "containsNot") {
					kind.clone()
				} else {
					Kind::Array(Box::new(kind.clone()), None)
				};
				let rhs = gql_to_sql_kind_with_scope(value, rhs_kind, enum_scope.as_deref())?;
				let op = match op.as_str() {
					"contains" => BinaryOperator::Contain,
					"containsNot" => BinaryOperator::NotContain,
					"containsAll" => BinaryOperator::ContainAll,
					"containsAny" => BinaryOperator::ContainAny,
					"containsNone" => BinaryOperator::ContainNone,
					_ => unreachable!(),
				};
				Expr::Binary {
					left: Box::new(Expr::Idiom(field_idiom.clone())),
					op,
					right: Box::new(rhs.into_literal()),
				}
			}
			other => {
				return Err(resolver_error(format!("Unsupported list filter operator: {other}")));
			}
		};
		exprs.push(expr);
	}
	combine_with_and(exprs)
}

fn relation_lookup_expr(relation_table_name: &TableName, predicate: Expr) -> Expr {
	Expr::Idiom(Idiom(vec![Part::Lookup(expr::Lookup {
		kind: LookupKind::Graph(Dir::Out),
		expr: None,
		only: false,
		what: vec![LookupSubject::Table {
			table: relation_table_name.clone(),
			referencing_field: None,
		}],
		cond: Some(Cond(predicate)),
		split: None,
		group: None,
		order: None,
		limit: None,
		start: None,
		alias: None,
	})]))
}

fn expr_from_relation_list_filter(
	filter: &IndexMap<Name, GqlValue>,
	relation: &RelationListFilterSpec,
	registry: &TableFilterRegistry,
) -> Result<Expr, GqlError> {
	if filter.is_empty() {
		return Err(resolver_error("Relation filter must have at least one operator"));
	}
	let mut exprs = Vec::new();
	for (op, value) in filter {
		let predicate = {
			let obj = value.as_object().ok_or_else(|| {
				resolver_error("Relation list operators require an object filter")
			})?;
			expr_from_filter_spec(obj, &relation.edge_filter, registry, None)?
		};
		let expr = match op.as_str() {
			"some" => Expr::Binary {
				left: Box::new(array_len_expr(relation_lookup_expr(
					&relation.relation_table_name,
					predicate,
				))),
				op: BinaryOperator::MoreThan,
				right: Box::new(Expr::Literal(Literal::Integer(0))),
			},
			"none" => Expr::Binary {
				left: Box::new(array_len_expr(relation_lookup_expr(
					&relation.relation_table_name,
					predicate,
				))),
				op: BinaryOperator::Equal,
				right: Box::new(Expr::Literal(Literal::Integer(0))),
			},
			"every" => {
				let inverted = Expr::Prefix {
					op: expr::PrefixOperator::Not,
					expr: Box::new(predicate),
				};
				Expr::Binary {
					left: Box::new(array_len_expr(relation_lookup_expr(
						&relation.relation_table_name,
						inverted,
					))),
					op: BinaryOperator::Equal,
					right: Box::new(Expr::Literal(Literal::Integer(0))),
				}
			}
			other => {
				return Err(resolver_error(format!(
					"Unsupported relation list filter operator: {other}"
				)));
			}
		};
		exprs.push(expr);
	}
	combine_with_and(exprs)
}
