//! GraphQL mutation generation.
//!
//! Generates GitHub-style single-record mutations for each table in the schema,
//! along with typed input and payload objects.
//!
//! - Record mutations: `create{Table}`, `update{Table}`, `upsert{Table}`,
//!   `delete{Table}`
//! - Relation creation: `relate{Table}`
//! - All mutations accept `input: XxxInput!`
//! - All mutations return `XxxPayload!`

use std::collections::BTreeMap;
use std::sync::Arc;

use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
	Field, FieldFuture, FieldValue, InputObject, InputValue, Object, Type, TypeRef,
};
use async_graphql::{Name, Value as GqlValue};
use surrealdb_types::ToSql;

use super::error::{GqlError, resolver_error};
use super::naming;
use super::schema::{
	SchemaContext, gql_to_sql_kind_with_scope, kind_to_type_with_enum_prefix, record_id_to_raw,
	unwrap_type,
};
use super::tables::{CachedRecord, VersionedRecord};
use super::utils::{GqlValueUtils, execute_plan};
use crate::catalog::providers::TableProvider;
use crate::catalog::{FieldDefinition, TableDefinition, TableType};
use crate::dbs::Session;
use crate::expr::statements::{
	CreateStatement, DeleteStatement, RelateStatement, UpdateStatement, UpsertStatement,
};
use crate::expr::{Data, Expr, Kind, Literal, LogicalPlan, Output, TopLevelExpr};
use crate::kvs::Datastore;
use crate::val::{Object as SurObject, RecordId, TableName, Value};

/// Parse a record ID from a table name and user-provided ID string.
///
/// Attempts to parse the full `table:id` string as a proper record ID (handling
/// numeric keys, etc.), falling back to a plain string key.
fn parse_record_id(table_name: &TableName, id: &str) -> Result<RecordId, GqlError> {
	if let Ok(x) = crate::syn::record_id(id) {
		return Ok(x.into());
	}
	let rid_str = format!("{table_name}:{id}");
	match crate::syn::record_id(&rid_str) {
		Ok(x) => Ok(x.into()),
		Err(_) => Ok(RecordId::new(table_name.clone(), id.to_string())),
	}
}

/// Parse a full record ID string (e.g., "person:alice").
fn parse_full_record_id(id_str: &str) -> Result<RecordId, GqlError> {
	crate::syn::record_id(id_str)
		.map(|x| x.into())
		.map_err(|e| resolver_error(format!("Invalid record ID: {id_str}: {e}")))
}

/// Convert a GraphQL input object to a SurrealDB Object for use as CONTENT/MERGE data.
///
/// Iterates over the input fields, finds each field's Kind from the field definitions,
/// and converts the GraphQL value to the corresponding SurrealDB value.
fn gql_input_to_sql_object(
	input: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
	skip_fields: &[&str],
	tb_name: &str,
) -> Result<SurObject, GqlError> {
	let mut map = BTreeMap::new();
	for (key, val) in input {
		let key_str = key.as_str();
		if skip_fields.contains(&key_str) {
			continue;
		}
		if matches!(val, GqlValue::Null) {
			continue;
		}
		let Some((sql_field_name, kind)) = fds.iter().find_map(|fd| {
			if fd.name.0.len() != 1 {
				return None;
			}
			let sql_field_name = fd.name.to_sql();
			let gql_field_name = naming::to_camel_case(&sql_field_name);
			((sql_field_name == key_str) || (gql_field_name == key_str))
				.then_some((sql_field_name, fd.field_kind.clone().unwrap_or(Kind::Any)))
		}) else {
			continue;
		};
		let enum_scope = format!("{tb_name}_{sql_field_name}");
		let sql_val = gql_to_sql_kind_with_scope(val, kind, Some(&enum_scope))?;
		map.insert(sql_field_name, sql_val);
	}
	Ok(SurObject(map))
}

/// Map a field's `Kind` to a GraphQL `TypeRef` suitable for mutation input types.
///
/// This differs from `kind_to_type(kind, types, true)` in that `record<target>`
/// fields are mapped to `ID` (the user passes a record ID string) rather than
/// the target table's output Object type (which is not a valid input type).
fn kind_to_input_type_ref(
	kind: Kind,
	types: &mut Vec<Type>,
	enum_scope: Option<&str>,
) -> Result<TypeRef, GqlError> {
	let optional = kind.can_be_none();

	// Check if the kind (after stripping option) is a record type.
	// Record fields should be represented as ID in input types (the user passes
	// a record ID string, not a nested object).
	match &kind {
		Kind::Record(_) => {
			let ty = TypeRef::named(TypeRef::ID);
			return Ok(if optional {
				ty
			} else {
				TypeRef::NonNull(Box::new(ty))
			});
		}
		Kind::Either(ks) => {
			// Strip None/Null variants to see what's underneath
			let non_none: Vec<&Kind> =
				ks.iter().filter(|k| !matches!(k, Kind::None | Kind::Null)).collect();
			if non_none.len() == 1
				&& let Kind::Record(_) = non_none[0]
			{
				// option<record<T>> -> nullable ID
				return Ok(TypeRef::named(TypeRef::ID));
			}
		}
		_ => {}
	}

	// For all other kinds, delegate to the standard kind_to_type with is_input=true
	kind_to_type_with_enum_prefix(kind, types, true, enum_scope)
}

#[derive(Clone, Copy)]
enum MutationKind {
	Create,
	Update,
	Upsert,
	Delete,
	Relate,
}

impl MutationKind {
	fn as_str(self) -> &'static str {
		match self {
			MutationKind::Create => "create",
			MutationKind::Update => "update",
			MutationKind::Upsert => "upsert",
			MutationKind::Delete => "delete",
			MutationKind::Relate => "relate",
		}
	}

	fn title(self) -> &'static str {
		match self {
			MutationKind::Create => "Create",
			MutationKind::Update => "Update",
			MutationKind::Upsert => "Upsert",
			MutationKind::Delete => "Delete",
			MutationKind::Relate => "Relate",
		}
	}
}

#[derive(Clone)]
enum PayloadEntity {
	Record(CachedRecord),
	RelationEdge(super::cursor::ConnectionEdge),
}

impl PayloadEntity {
	fn to_field_value(&self) -> FieldValue<'static> {
		match self {
			PayloadEntity::Record(record) => FieldValue::owned_any(record.clone()),
			PayloadEntity::RelationEdge(edge) => FieldValue::owned_any(edge.clone()),
		}
	}
}

#[derive(Clone)]
struct MutationPayloadValue {
	entity: Option<PayloadEntity>,
	success: bool,
	message: Option<String>,
}

#[derive(Clone)]
struct MutationTypeNames {
	field_name: String,
	input_name: String,
	payload_name: String,
}

#[derive(Clone)]
struct MutationTableContext {
	cap_name: String,
	entity_type_name: String,
	entity_field_name: String,
	tb_name_str: String,
	tb_name: TableName,
	is_relation: bool,
	fds: Arc<[FieldDefinition]>,
	kvs: Arc<Datastore>,
}

fn mutation_names(tc: &MutationTableContext, kind: MutationKind) -> MutationTypeNames {
	let operation = kind.as_str();
	let input_name = naming::mutation_input_name(operation, &tc.tb_name_str);
	let payload_name = naming::mutation_payload_name(operation, &tc.tb_name_str);
	let field_name = format!("{}{cap_name}", operation, cap_name = tc.cap_name);

	MutationTypeNames {
		field_name,
		input_name,
		payload_name,
	}
}

fn register_payload_type(
	tc: &MutationTableContext,
	kind: MutationKind,
	types: &mut Vec<Type>,
) -> String {
	let names = mutation_names(tc, kind);
	let entity_field_name = tc.entity_field_name.clone();
	let entity_type_name = tc.entity_type_name.clone();
	let payload = Object::new(&names.payload_name)
		.description(format!(
			"Return payload for the `{}` mutation on `{}`.",
			names.field_name, tc.tb_name_str
		))
		.field(
			Field::new(&entity_field_name, TypeRef::named(&entity_type_name), move |ctx| {
				FieldFuture::new(async move {
					let payload = ctx.parent_value.try_downcast_ref::<MutationPayloadValue>()?;
					Ok(payload.entity.as_ref().map(PayloadEntity::to_field_value))
				})
			})
			.description(format!("The `{}` entity returned by this mutation.", tc.tb_name_str)),
		)
		.field(
			Field::new("success", TypeRef::named_nn(TypeRef::BOOLEAN), |ctx| {
				FieldFuture::new(async move {
					let payload = ctx.parent_value.try_downcast_ref::<MutationPayloadValue>()?;
					Ok(Some(FieldValue::value(payload.success)))
				})
			})
			.description("Whether the mutation completed successfully."),
		)
		.field(
			Field::new("message", TypeRef::named(TypeRef::STRING), |ctx| {
				FieldFuture::new(async move {
					let payload = ctx.parent_value.try_downcast_ref::<MutationPayloadValue>()?;
					Ok(Some(FieldValue::value(
						payload.message.clone().map_or(GqlValue::Null, GqlValue::from),
					)))
				})
			})
			.description("A human-readable message describing the mutation result."),
		);

	types.push(Type::Object(payload));
	names.payload_name
}

fn build_input_field(name: &str, ty: TypeRef, description: String) -> InputValue {
	InputValue::new(name, ty).description(description)
}

fn generate_input_types(
	tc: &MutationTableContext,
	types: &mut Vec<Type>,
) -> Result<Vec<MutationTypeNames>, GqlError> {
	let create_kind = if tc.is_relation {
		MutationKind::Relate
	} else {
		MutationKind::Create
	};
	let create_names = mutation_names(tc, create_kind);
	let update_names = mutation_names(tc, MutationKind::Update);
	let upsert_names = mutation_names(tc, MutationKind::Upsert);
	let delete_names = mutation_names(tc, MutationKind::Delete);

	let create_desc = if tc.is_relation {
		format!("Parameters for relating `{}` records.", tc.tb_name_str)
	} else {
		format!("Parameters for creating a `{}` record.", tc.tb_name_str)
	};
	let mut create_input = InputObject::new(&create_names.input_name).description(create_desc);
	let mut update_input = InputObject::new(&update_names.input_name)
		.description(format!("Parameters for updating a `{}` record.", tc.tb_name_str));
	let mut upsert_input = InputObject::new(&upsert_names.input_name)
		.description(format!("Parameters for upserting a `{}` record.", tc.tb_name_str));
	let delete_input = InputObject::new(&delete_names.input_name)
		.description(format!("Parameters for deleting a `{}` record.", tc.tb_name_str))
		.field(build_input_field(
			"id",
			TypeRef::named_nn(TypeRef::ID),
			format!("The record id of the `{}` to delete.", tc.tb_name_str),
		));

	create_input = create_input.field(build_input_field(
		"id",
		TypeRef::named(TypeRef::ID),
		"Optionally provide the record id to create.".to_string(),
	));
	update_input = update_input.field(build_input_field(
		"id",
		TypeRef::named_nn(TypeRef::ID),
		format!("The record id of the `{}` to update.", tc.tb_name_str),
	));
	upsert_input = upsert_input.field(build_input_field(
		"id",
		TypeRef::named(TypeRef::ID),
		"Optionally provide the record id to create or replace.".to_string(),
	));

	for fd in tc.fds.iter() {
		let Some(ref kind) = fd.field_kind else {
			continue;
		};
		if fd.name.is_id() || fd.name.0.len() > 1 {
			continue;
		}

		let fd_name = fd.name.to_sql();
		let gql_field_name = naming::to_camel_case(&fd_name);
		let enum_scope = format!("{}_{}", tc.tb_name_str, fd_name);
		let required_type = kind_to_input_type_ref(kind.clone(), types, Some(&enum_scope))?;
		let optional_type = unwrap_type(required_type.clone());
		let field_description =
			fd.comment.clone().unwrap_or_else(|| format!("Set the `{gql_field_name}` field."));

		match fd_name.as_str() {
			"in" | "out" if tc.is_relation => {
				create_input = create_input.field(build_input_field(
					&gql_field_name,
					TypeRef::named_nn(TypeRef::ID),
					field_description.clone(),
				));
				update_input = update_input.field(build_input_field(
					&gql_field_name,
					TypeRef::named(TypeRef::ID),
					format!("Update the `{gql_field_name}` endpoint."),
				));
				upsert_input = upsert_input.field(build_input_field(
					&gql_field_name,
					TypeRef::named_nn(TypeRef::ID),
					field_description,
				));
			}
			_ => {
				create_input = create_input.field(build_input_field(
					&gql_field_name,
					required_type.clone(),
					field_description.clone(),
				));
				update_input = update_input.field(build_input_field(
					&gql_field_name,
					optional_type,
					format!("Update the `{gql_field_name}` field."),
				));
				upsert_input = upsert_input.field(build_input_field(
					&gql_field_name,
					required_type,
					field_description,
				));
			}
		}
	}

	types.push(Type::InputObject(create_input));
	types.push(Type::InputObject(update_input));
	types.push(Type::InputObject(upsert_input));
	types.push(Type::InputObject(delete_input));

	Ok(vec![create_names, update_names, upsert_names, delete_names])
}

pub async fn process_mutations(
	tbs: Arc<[TableDefinition]>,
	types: &mut Vec<Type>,
	schema_ctx: &SchemaContext<'_>,
) -> Result<Object, GqlError> {
	let mut mutation = Object::new("Mutation");

	for tb in tbs.iter() {
		let tb_name = tb.name.clone();
		let tb_name_str = tb_name.clone().into_string();
		let is_relation = matches!(tb.table_type, TableType::Relation(_));
		let fds = schema_ctx.tx.all_tb_fields(schema_ctx.ns, schema_ctx.db, &tb.name, None).await?;

		let ctx = MutationTableContext {
			cap_name: naming::table_type_name(&tb_name_str),
			entity_type_name: if is_relation {
				naming::edge_type_name(&tb_name_str)
			} else {
				naming::table_type_name(&tb_name_str)
			},
			entity_field_name: naming::payload_entity_field_name(&tb_name_str),
			tb_name_str,
			tb_name,
			is_relation,
			fds,
			kvs: schema_ctx.datastore.clone(),
		};

		let names = generate_input_types(&ctx, types)?;
		let create_kind = if ctx.is_relation {
			MutationKind::Relate
		} else {
			MutationKind::Create
		};

		register_payload_type(&ctx, create_kind, types);
		register_payload_type(&ctx, MutationKind::Update, types);
		register_payload_type(&ctx, MutationKind::Upsert, types);
		register_payload_type(&ctx, MutationKind::Delete, types);

		let create_names = names[0].clone();
		let update_names = names[1].clone();
		let upsert_names = names[2].clone();
		let delete_names = names[3].clone();

		mutation = mutation.field(add_create_field(&ctx, create_kind, &create_names));
		mutation = mutation.field(add_update_field(&ctx, &update_names));
		mutation = mutation.field(add_upsert_field(&ctx, &upsert_names));
		mutation = mutation.field(add_delete_field(&ctx, &delete_names));
	}

	Ok(mutation)
}

fn add_create_field(
	tc: &MutationTableContext,
	kind: MutationKind,
	names: &MutationTypeNames,
) -> Field {
	let fds = tc.fds.clone();
	let kvs = tc.kvs.clone();
	let tb_name = tc.tb_name.clone();
	let is_relation = tc.is_relation;
	let payload_name = names.payload_name.clone();
	let input_name = names.input_name.clone();
	let field_name = names.field_name.clone();
	Field::new(&field_name, TypeRef::named_nn(&payload_name), move |ctx| {
		let fds = fds.clone();
		let kvs = kvs.clone();
		let tb_name = tb_name.clone();
		FieldFuture::new(async move {
			let sess = ctx.data::<Arc<Session>>()?;
			let input = get_input_object(ctx.args.as_index_map())?;
			let id_opt = input.get("id").and_then(GqlValueUtils::as_string);
			let entity = if is_relation {
				execute_relate_create(&kvs, sess, &tb_name, input, &fds, id_opt).await?
			} else {
				execute_normal_create(&kvs, sess, &tb_name, input, &fds, id_opt).await?
			};
			Ok(Some(FieldValue::owned_any(MutationPayloadValue {
				success: entity.is_some(),
				message: Some(format!("{} `{}` succeeded.", kind.title(), tb_name)),
				entity,
			})))
		})
	})
	.description(if tc.is_relation {
		format!("Creates a `{}` relation.", tc.tb_name_str)
	} else {
		format!("Creates a `{}` record.", tc.tb_name_str)
	})
	.argument(InputValue::new("input", TypeRef::named_nn(&input_name)).description(format!(
		"Parameters for {}{}.",
		kind.title(),
		tc.cap_name
	)))
}

fn add_update_field(tc: &MutationTableContext, names: &MutationTypeNames) -> Field {
	let fds = tc.fds.clone();
	let kvs = tc.kvs.clone();
	let tb_name = tc.tb_name.clone();
	let is_relation = tc.is_relation;
	let payload_name = names.payload_name.clone();
	let input_name = names.input_name.clone();
	let field_name = names.field_name.clone();
	Field::new(&field_name, TypeRef::named_nn(&payload_name), move |ctx| {
		let fds = fds.clone();
		let kvs = kvs.clone();
		let tb_name = tb_name.clone();
		FieldFuture::new(async move {
			let sess = ctx.data::<Arc<Session>>()?;
			let input = get_input_object(ctx.args.as_index_map())?;
			let id_str = get_required_input_id(input)?;
			let rid = parse_record_id(&tb_name, &id_str)?;
			let content = gql_input_to_sql_object(input, &fds, &["id"], tb_name.as_str())?;
			let data = if content.0.is_empty() {
				None
			} else {
				Some(Data::MergeExpression(Value::Object(content).into_literal()))
			};
			let stmt = UpdateStatement {
				only: true,
				what: vec![Value::RecordId(rid).into_literal()],
				data,
				cond: None,
				output: None,
				timeout: Expr::Literal(Literal::None),
				..Default::default()
			};
			let plan = LogicalPlan {
				expressions: vec![TopLevelExpr::Expr(Expr::Update(Box::new(stmt)))],
			};
			let res = execute_plan(&kvs, sess, plan).await?;
			let entity = extract_payload_entity(res, is_relation)?;
			Ok(Some(FieldValue::owned_any(MutationPayloadValue {
				success: entity.is_some(),
				message: Some(format!("Updated `{}` successfully.", tb_name)),
				entity,
			})))
		})
	})
	.description(format!("Updates a `{}` record.", tc.tb_name_str))
	.argument(
		InputValue::new("input", TypeRef::named_nn(&input_name))
			.description(format!("Parameters for Update{}.", tc.cap_name)),
	)
}

fn add_upsert_field(tc: &MutationTableContext, names: &MutationTypeNames) -> Field {
	let fds = tc.fds.clone();
	let kvs = tc.kvs.clone();
	let tb_name = tc.tb_name.clone();
	let is_relation = tc.is_relation;
	let payload_name = names.payload_name.clone();
	let input_name = names.input_name.clone();
	let field_name = names.field_name.clone();
	Field::new(&field_name, TypeRef::named_nn(&payload_name), move |ctx| {
		let fds = fds.clone();
		let kvs = kvs.clone();
		let tb_name = tb_name.clone();
		FieldFuture::new(async move {
			let sess = ctx.data::<Arc<Session>>()?;
			let input = get_input_object(ctx.args.as_index_map())?;
			let id_opt = input.get("id").and_then(GqlValueUtils::as_string);

			let entity = if is_relation && id_opt.is_none() {
				execute_relate_create(&kvs, sess, &tb_name, input, &fds, None).await?
			} else {
				let what = match id_opt {
					Some(ref id_str) => {
						let rid = parse_record_id(&tb_name, id_str)?;
						vec![Value::RecordId(rid).into_literal()]
					}
					None => vec![Expr::Table(tb_name.clone())],
				};
				let content = gql_input_to_sql_object(input, &fds, &["id"], tb_name.as_str())?;
				let data = if content.0.is_empty() {
					None
				} else {
					Some(Data::ContentExpression(Value::Object(content).into_literal()))
				};
				let stmt = UpsertStatement {
					only: true,
					what,
					data,
					cond: None,
					output: None,
					timeout: Expr::Literal(Literal::None),
					..Default::default()
				};
				let plan = LogicalPlan {
					expressions: vec![TopLevelExpr::Expr(Expr::Upsert(Box::new(stmt)))],
				};
				let res = execute_plan(&kvs, sess, plan).await?;
				extract_payload_entity(res, is_relation)?
			};

			Ok(Some(FieldValue::owned_any(MutationPayloadValue {
				success: entity.is_some(),
				message: Some(format!("Upserted `{}` successfully.", tb_name)),
				entity,
			})))
		})
	})
	.description(format!("Creates or updates a `{}` record.", tc.tb_name_str))
	.argument(
		InputValue::new("input", TypeRef::named_nn(&input_name))
			.description(format!("Parameters for Upsert{}.", tc.cap_name)),
	)
}

fn add_delete_field(tc: &MutationTableContext, names: &MutationTypeNames) -> Field {
	let kvs = tc.kvs.clone();
	let tb_name = tc.tb_name.clone();
	let is_relation = tc.is_relation;
	let payload_name = names.payload_name.clone();
	let input_name = names.input_name.clone();
	let field_name = names.field_name.clone();
	Field::new(&field_name, TypeRef::named_nn(&payload_name), move |ctx| {
		let kvs = kvs.clone();
		let tb_name = tb_name.clone();
		FieldFuture::new(async move {
			let sess = ctx.data::<Arc<Session>>()?;
			let input = get_input_object(ctx.args.as_index_map())?;
			let id_str = get_required_input_id(input)?;
			let rid = parse_record_id(&tb_name, &id_str)?;
			let stmt = DeleteStatement {
				only: true,
				what: vec![Value::RecordId(rid).into_literal()],
				cond: None,
				output: Some(Output::Before),
				timeout: Expr::Literal(Literal::None),
				..Default::default()
			};
			let plan = LogicalPlan {
				expressions: vec![TopLevelExpr::Expr(Expr::Delete(Box::new(stmt)))],
			};
			let res = execute_plan(&kvs, sess, plan).await?;
			let entity = extract_payload_entity(res, is_relation)?;
			let success = entity.is_some();
			let message = if success {
				format!("Deleted `{}` successfully.", tb_name)
			} else {
				format!("No `{}` record matched the provided id.", tb_name)
			};
			Ok(Some(FieldValue::owned_any(MutationPayloadValue {
				success,
				message: Some(message),
				entity,
			})))
		})
	})
	.description(format!("Deletes a `{}` record.", tc.tb_name_str))
	.argument(
		InputValue::new("input", TypeRef::named_nn(&input_name))
			.description(format!("Parameters for Delete{}.", tc.cap_name)),
	)
}

fn get_input_object(
	args: &IndexMap<Name, GqlValue>,
) -> Result<&IndexMap<Name, GqlValue>, GqlError> {
	args.get("input").ok_or_else(|| resolver_error("Missing required `input` argument")).and_then(
		|value| value.as_object().ok_or_else(|| resolver_error("`input` must be an object")),
	)
}

fn get_required_input_id(input: &IndexMap<Name, GqlValue>) -> Result<String, GqlError> {
	input
		.get("id")
		.and_then(GqlValueUtils::as_string)
		.ok_or_else(|| resolver_error("Missing required `input.id` argument"))
}

async fn execute_normal_create(
	kvs: &Arc<Datastore>,
	sess: &Arc<Session>,
	tb_name: &TableName,
	data_obj: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
	id_opt: Option<String>,
) -> Result<Option<PayloadEntity>, async_graphql::Error> {
	let content = gql_input_to_sql_object(data_obj, fds, &["id"], tb_name.as_str())?;
	let what = match id_opt {
		Some(id_str) => {
			let rid = parse_record_id(tb_name, &id_str)?;
			vec![Value::RecordId(rid).into_literal()]
		}
		None => vec![Expr::Table(tb_name.clone())],
	};
	let data = if content.0.is_empty() {
		None
	} else {
		Some(Data::ContentExpression(Value::Object(content).into_literal()))
	};
	let stmt = CreateStatement {
		only: true,
		what,
		data,
		output: None,
		timeout: Expr::Literal(Literal::None),
	};
	let plan = LogicalPlan {
		expressions: vec![TopLevelExpr::Expr(Expr::Create(Box::new(stmt)))],
	};
	let res = execute_plan(kvs, sess, plan).await?;
	extract_payload_entity(res, false)
}

async fn execute_relate_create(
	kvs: &Arc<Datastore>,
	sess: &Arc<Session>,
	tb_name: &TableName,
	data_obj: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
	id_opt: Option<String>,
) -> Result<Option<PayloadEntity>, async_graphql::Error> {
	let in_str = data_obj
		.get("in")
		.and_then(GqlValueUtils::as_string)
		.ok_or_else(|| resolver_error("Relation create requires `input.in`"))?;
	let out_str = data_obj
		.get("out")
		.and_then(GqlValueUtils::as_string)
		.ok_or_else(|| resolver_error("Relation create requires `input.out`"))?;

	let from_rid = parse_full_record_id(&in_str)?;
	let to_rid = parse_full_record_id(&out_str)?;
	let content = gql_input_to_sql_object(data_obj, fds, &["id", "in", "out"], tb_name.as_str())?;
	let through = match id_opt {
		Some(id_str) => {
			let rid = parse_record_id(tb_name, &id_str)?;
			Value::RecordId(rid).into_literal()
		}
		None => Expr::Table(tb_name.clone()),
	};
	let data = if content.0.is_empty() {
		None
	} else {
		Some(Data::ContentExpression(Value::Object(content).into_literal()))
	};
	let stmt = RelateStatement {
		only: true,
		through,
		from: Value::RecordId(from_rid).into_literal(),
		to: Value::RecordId(to_rid).into_literal(),
		data,
		output: None,
		timeout: Expr::Literal(Literal::None),
	};
	let plan = LogicalPlan {
		expressions: vec![TopLevelExpr::Expr(Expr::Relate(Box::new(stmt)))],
	};
	let res = execute_plan(kvs, sess, plan).await?;
	extract_payload_entity(res, true)
}

fn extract_cached_record(val: Value) -> Result<Option<CachedRecord>, async_graphql::Error> {
	match val {
		Value::Object(obj) => {
			let rid = match obj.get("id") {
				Some(Value::RecordId(rid)) => rid.clone(),
				_ => return Err(resolver_error("Mutation result missing `id` field").into()),
			};
			Ok(Some(CachedRecord {
				rid,
				version: None,
				data: obj,
			}))
		}
		Value::None | Value::Null => Ok(None),
		_ => {
			error!("Unexpected mutation result type: {val:?}");
			Err(resolver_error("Unexpected mutation result").into())
		}
	}
}

fn relation_edge_from_record(
	record: &CachedRecord,
) -> Result<super::cursor::ConnectionEdge, async_graphql::Error> {
	let out_rid = match record.data.get("out") {
		Some(Value::RecordId(rid)) => rid.clone(),
		_ => return Err(resolver_error("Relation mutation result missing `out` field").into()),
	};

	Ok(super::cursor::ConnectionEdge {
		cursor: super::cursor::encode_cursor(&record_id_to_raw(&record.rid)),
		node: super::cursor::ConnectionNode::VersionedRecord {
			record: VersionedRecord {
				rid: out_rid.clone(),
				version: record.version.clone(),
			},
			runtime_type_name: None,
		},
		relation_record: Some(record.clone()),
	})
}

fn extract_payload_entity(
	val: Value,
	is_relation: bool,
) -> Result<Option<PayloadEntity>, async_graphql::Error> {
	let Some(record) = extract_cached_record(val)? else {
		return Ok(None);
	};

	if is_relation {
		return Ok(Some(PayloadEntity::RelationEdge(relation_edge_from_record(&record)?)));
	}

	Ok(Some(PayloadEntity::Record(record)))
}
