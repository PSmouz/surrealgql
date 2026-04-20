//! GraphQL authentication mutations.
//!
//! Generates `signIn` and `signUp` mutation fields by inspecting database
//! access definitions. Each Record access method that has a SIGNIN clause
//! contributes to the `signIn` mutation, and each that has a SIGNUP clause
//! contributes to the `signUp` mutation.
//!
//! The mutations accept an `access` name and a `variables` object (JSON scalar),
//! and return a structured authentication payload.

use std::sync::Arc;

use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
	Enum, EnumItem, Field, FieldFuture, FieldValue, InputValue, Object, Type, TypeRef,
};
use async_graphql::{Name, Value as GqlValue};

use super::error::{GqlError, auth_error, resolver_error};
use super::naming;
use super::schema::semantic_non_null_directive;
use super::utils::GqlValueUtils;
use crate::catalog::{AccessDefinition, AccessType};
use crate::dbs::Session;
use crate::iam::clear;
use crate::iam::token::Token;
use crate::iam::{signin, signup, verify};
use crate::kvs::Datastore;
use crate::types::PublicVariables;

#[derive(Clone, Debug)]
struct AuthenticationPayload {
	success: bool,
	token: Option<String>,
}

/// Inspect all database access definitions and add `signIn` / `signUp`
/// mutation fields to the provided Mutation object.
///
/// - `signIn(access: AccessMethod!, variables: JSON!): AuthenticationPayload!` is added when at
///   least one Record access method with a SIGNIN clause exists.
/// - `signUp(access: AccessMethod!, variables: JSON!): AuthenticationPayload!` is added when at
///   least one Record access method with a SIGNUP clause exists.
///
/// The `variables` argument accepts an arbitrary JSON object containing the
/// authentication variables (e.g., `{ email: "user@example.com", pass: "secret" }`).
///
/// Returns the (possibly unchanged) mutation object.
pub fn add_auth_mutations(
	mutation: Object,
	types: &mut Vec<Type>,
	accesses: &[AccessDefinition],
	ns: &str,
	db: &str,
	datastore: &Arc<Datastore>,
) -> Object {
	let has_signin = accesses.iter().any(|ac| match &ac.access_type {
		AccessType::Record(rec) => rec.signin.is_some(),
		_ => false,
	});

	let has_signup = accesses.iter().any(|ac| match &ac.access_type {
		AccessType::Record(rec) => rec.signup.is_some(),
		_ => false,
	});

	let mut mutation = mutation;
	let payload_type_name = "AuthenticationPayload";
	let access_enum_name = "AccessMethod";

	let mut payload = Object::new(payload_type_name)
		.description("The result of a GraphQL authentication operation.")
		.field(
			Field::new("success", TypeRef::named_nn(TypeRef::BOOLEAN), |ctx| {
				FieldFuture::new(async move {
					let payload = ctx.parent_value.try_downcast_ref::<AuthenticationPayload>()?;
					Ok(Some(FieldValue::value(payload.success)))
				})
			})
			.description("Whether the authentication operation completed successfully.")
			.directive(semantic_non_null_directive()),
		)
		.field(
			Field::new("token", TypeRef::named(TypeRef::STRING), |ctx| {
				FieldFuture::new(async move {
					let payload = ctx.parent_value.try_downcast_ref::<AuthenticationPayload>()?;
					Ok(Some(FieldValue::value(
						payload.token.clone().map_or(GqlValue::Null, GqlValue::from),
					)))
				})
			})
			.description(
				"The access token returned by the authentication operation, when applicable.",
			),
		);

	payload = payload.directive(semantic_non_null_directive());
	types.push(Type::Object(payload));

	let mut access_enum = Enum::new(access_enum_name)
		.description("The record access methods available for GraphQL authentication.");
	let mut access_enum_map = IndexMap::new();
	for access in accesses {
		if matches!(access.access_type, AccessType::Record(_)) {
			let raw_name = access.name.clone();
			let enum_name = naming::to_screaming_snake_case(&raw_name);
			access_enum_map.insert(enum_name.clone(), raw_name.clone());
			access_enum = access_enum.item(
				EnumItem::new(enum_name)
					.description(format!("Use the `{raw_name}` access method.")),
			);
		}
	}
	if !access_enum_map.is_empty() {
		types.push(Type::Enum(access_enum));
	}

	if has_signin {
		let kvs = datastore.clone();
		let ns_name = ns.to_string();
		let db_name = db.to_string();
		let access_enum_map = access_enum_map.clone();
		mutation = mutation.field(
			Field::new("signIn", TypeRef::named_nn(payload_type_name), move |ctx| {
				let kvs = kvs.clone();
				let ns_name = ns_name.clone();
				let db_name = db_name.clone();
				let access_enum_map = access_enum_map.clone();
				FieldFuture::new(async move {
					let sess = ctx.data::<Arc<Session>>()?;
					let args = ctx.args.as_index_map();

					let access = args
						.get("access")
						.and_then(|value| match value {
							GqlValue::Enum(name) => access_enum_map.get(name.as_str()).cloned(),
							GqlValue::String(name) => Some(name.clone()),
							_ => None,
						})
						.ok_or_else(|| resolver_error("Missing required 'access' argument"))?;

					let variables = args
						.get("variables")
						.and_then(GqlValueUtils::as_object)
						.ok_or_else(|| {
							resolver_error(
								"Missing required 'variables' argument (must be an object)",
							)
						})?;

					let vars = build_public_variables(&ns_name, &db_name, &access, variables)?;

					// Create a fresh session for the signin operation
					let mut auth_sess = Session {
						ns: Some(ns_name.clone()),
						db: Some(db_name.clone()),
						..Default::default()
					};
					auth_sess.ip.clone_from(&sess.ip);
					auth_sess.or.clone_from(&sess.or);

					let token = signin::signin(&kvs, &mut auth_sess, vars).await.map_err(|e| {
						warn!("GraphQL signIn failed: {e}");
						auth_error("There was a problem with authentication")
					})?;

					let access_token = match token {
						Token::Access(t) => t,
						Token::WithRefresh {
							access,
							..
						} => access,
					};

					Ok(Some(FieldValue::owned_any(AuthenticationPayload {
						success: true,
						token: Some(access_token),
					})))
				})
			})
			.description("Sign in using a database access method.")
			.argument(
				InputValue::new("access", TypeRef::named_nn(access_enum_name))
					.description("The access method used for the sign-in operation."),
			)
			.argument(InputValue::new("variables", TypeRef::named_nn("JSON"))),
		);
	}

	if has_signup {
		let kvs = datastore.clone();
		let ns_name = ns.to_string();
		let db_name = db.to_string();
		let access_enum_map = access_enum_map.clone();
		mutation = mutation.field(
			Field::new("signUp", TypeRef::named_nn(payload_type_name), move |ctx| {
				let kvs = kvs.clone();
				let ns_name = ns_name.clone();
				let db_name = db_name.clone();
				let access_enum_map = access_enum_map.clone();
				FieldFuture::new(async move {
					let sess = ctx.data::<Arc<Session>>()?;
					let args = ctx.args.as_index_map();

					let access = args
						.get("access")
						.and_then(|value| match value {
							GqlValue::Enum(name) => access_enum_map.get(name.as_str()).cloned(),
							GqlValue::String(name) => Some(name.clone()),
							_ => None,
						})
						.ok_or_else(|| resolver_error("Missing required 'access' argument"))?;

					let variables = args
						.get("variables")
						.and_then(GqlValueUtils::as_object)
						.ok_or_else(|| {
							resolver_error(
								"Missing required 'variables' argument (must be an object)",
							)
						})?;

					let vars = build_public_variables(&ns_name, &db_name, &access, variables)?;

					// Create a fresh session for the signup operation
					let mut auth_sess = Session {
						ns: Some(ns_name.clone()),
						db: Some(db_name.clone()),
						..Default::default()
					};
					auth_sess.ip.clone_from(&sess.ip);
					auth_sess.or.clone_from(&sess.or);

					let token = signup::signup(&kvs, &mut auth_sess, vars).await.map_err(|e| {
						warn!("GraphQL signUp failed: {e}");
						auth_error("There was a problem with authentication")
					})?;

					let access_token = match token {
						Token::Access(t) => t,
						Token::WithRefresh {
							access,
							..
						} => access,
					};

					Ok(Some(FieldValue::owned_any(AuthenticationPayload {
						success: true,
						token: Some(access_token),
					})))
				})
			})
			.description("Sign up using a database access method.")
			.argument(
				InputValue::new("access", TypeRef::named_nn(access_enum_name))
					.description("The access method used for the sign-up operation."),
			)
			.argument(InputValue::new("variables", TypeRef::named_nn("JSON"))),
		);
	}

	{
		let kvs = datastore.clone();
		let ns_name = ns.to_string();
		let db_name = db.to_string();
		mutation = mutation.field(
			Field::new("authenticate", TypeRef::named_nn(payload_type_name), move |ctx| {
				let kvs = kvs.clone();
				let ns_name = ns_name.clone();
				let db_name = db_name.clone();
				FieldFuture::new(async move {
					let sess = ctx.data::<Arc<Session>>()?;
					let token = ctx
						.args
						.get("token")
						.and_then(|value| value.string().ok())
						.map(str::to_owned)
						.ok_or_else(|| resolver_error("Missing required 'token' argument"))?;
					let mut auth_sess = Session {
						ns: Some(ns_name.clone()),
						db: Some(db_name.clone()),
						..Default::default()
					};
					auth_sess.ip.clone_from(&sess.ip);
					auth_sess.or.clone_from(&sess.or);
					verify::token(&kvs, &mut auth_sess, &token).await.map_err(|e| {
						warn!("GraphQL authenticate failed: {e}");
						auth_error("There was a problem with authentication")
					})?;
					Ok(Some(FieldValue::owned_any(AuthenticationPayload {
						success: true,
						token: Some(token),
					})))
				})
			})
			.description("Validate an access token and return it when it is still valid.")
			.argument(
				InputValue::new("token", TypeRef::named_nn(TypeRef::STRING))
					.description("The access token to validate."),
			),
		);
	}

	mutation = mutation.field(
		Field::new("invalidate", TypeRef::named_nn(TypeRef::BOOLEAN), |ctx| {
			FieldFuture::new(async move {
				let sess = ctx.data::<Arc<Session>>()?;
				let mut auth_sess = (**sess).clone();
				clear::clear(&mut auth_sess).map_err(|e| auth_error(e.to_string()))?;
				Ok(Some(FieldValue::value(true)))
			})
		})
		.description("Invalidate the current authenticated session context.")
		.directive(semantic_non_null_directive()),
	);

	mutation
}

/// Build a `PublicVariables` map from a GraphQL object value.
///
/// Sets the system variables `NS`, `DB`, and `AC` from the provided
/// namespace, database, and access method name, then converts each
/// key-value pair from the GraphQL object to a public variable.
///
/// Values are converted directly to their natural types (strings stay
/// as strings, numbers as numbers, booleans as booleans) without going
/// through the SurrealQL parser, since auth variables are user-provided
/// credentials, not SurrealQL expressions.
fn build_public_variables(
	ns: &str,
	db: &str,
	access: &str,
	variables: &IndexMap<Name, GqlValue>,
) -> Result<PublicVariables, GqlError> {
	let mut vars = PublicVariables::new();

	// Set the system-level routing variables
	vars.insert("NS", ns.to_string());
	vars.insert("DB", db.to_string());
	vars.insert("AC", access.to_string());

	// Convert user-provided variables directly to PublicVariables
	for (key, val) in variables {
		let key_str = key.as_str();
		// Skip system variables the user might have accidentally included
		if matches!(key_str, "NS" | "ns" | "DB" | "db" | "AC" | "ac") {
			continue;
		}

		match val {
			GqlValue::Null => continue,
			GqlValue::String(s) => vars.insert(key_str.to_string(), s.clone()),
			GqlValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					vars.insert(key_str.to_string(), i);
				} else if let Some(f) = n.as_f64() {
					vars.insert(key_str.to_string(), f);
				} else {
					vars.insert(key_str.to_string(), n.to_string());
				}
			}
			GqlValue::Boolean(b) => vars.insert(key_str.to_string(), *b),
			GqlValue::Enum(s) => vars.insert(key_str.to_string(), s.as_str().to_string()),
			// For complex types (lists, nested objects), convert to string
			other => vars.insert(key_str.to_string(), other.to_string()),
		}
	}

	Ok(vars)
}
