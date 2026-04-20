//! GraphQL authentication mutations.
//!
//! Generates GitHub-style auth mutations that mirror the underlying IAM
//! capabilities while keeping a consistent `input: XxxInput!` / `XxxPayload!`
//! schema surface, except for `invalidate`, which has no input object.

use std::sync::Arc;

use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
	Field, FieldFuture, FieldValue, InputObject, InputValue, Object, Type, TypeRef,
};
use async_graphql::{Name, Value as GqlValue};

use super::error::{GqlError, auth_error, resolver_error};
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
struct AuthPayloadValue {
	success: bool,
	message: Option<String>,
	token: Option<String>,
}

#[derive(Clone)]
struct AuthMutationNames {
	field_name: String,
	input_name: String,
	payload_name: String,
}

#[derive(Clone)]
enum SignInMode {
	Generic,
	Root,
	Namespace,
	Database,
	Access,
}

#[derive(Clone)]
enum SignUpMode {
	Generic,
	Access,
}

pub fn add_auth_mutations(
	mutation: Object,
	types: &mut Vec<Type>,
	accesses: &[AccessDefinition],
	ns: &str,
	db: &str,
	datastore: &Arc<Datastore>,
) -> Result<Object, GqlError> {
	let has_signin = accesses.iter().any(|access| match &access.access_type {
		AccessType::Record(record) => record.signin.is_some(),
		_ => false,
	});
	let has_signup = accesses.iter().any(|access| match &access.access_type {
		AccessType::Record(record) => record.signup.is_some(),
		_ => false,
	});

	let generic_signin_names = AuthMutationNames {
		field_name: "signin".to_string(),
		input_name: "SigninInput".to_string(),
		payload_name: "SigninPayload".to_string(),
	};
	let root_signin_names = AuthMutationNames {
		field_name: "signinRoot".to_string(),
		input_name: "SigninRootInput".to_string(),
		payload_name: "SigninRootPayload".to_string(),
	};
	let ns_signin_names = AuthMutationNames {
		field_name: "signinNS".to_string(),
		input_name: "SigninNSInput".to_string(),
		payload_name: "SigninNSPayload".to_string(),
	};
	let db_signin_names = AuthMutationNames {
		field_name: "signinDB".to_string(),
		input_name: "SigninDBInput".to_string(),
		payload_name: "SigninDBPayload".to_string(),
	};
	let access_signin_names = AuthMutationNames {
		field_name: "signinAccess".to_string(),
		input_name: "SigninAccessInput".to_string(),
		payload_name: "SigninAccessPayload".to_string(),
	};
	let generic_signup_names = AuthMutationNames {
		field_name: "signup".to_string(),
		input_name: "SignupInput".to_string(),
		payload_name: "SignupPayload".to_string(),
	};
	let access_signup_names = AuthMutationNames {
		field_name: "signupAccess".to_string(),
		input_name: "SignupAccessInput".to_string(),
		payload_name: "SignupAccessPayload".to_string(),
	};
	let authenticate_names = AuthMutationNames {
		field_name: "authenticate".to_string(),
		input_name: "AuthenticateInput".to_string(),
		payload_name: "AuthenticatePayload".to_string(),
	};

	register_signin_input(types, &generic_signin_names.input_name)?;
	register_signin_root_input(types, &root_signin_names.input_name)?;
	register_signin_ns_input(types, &ns_signin_names.input_name)?;
	register_signin_db_input(types, &db_signin_names.input_name)?;
	if has_signin {
		register_access_input(types, &access_signin_names.input_name, true)?;
	}
	register_signup_input(types, &generic_signup_names.input_name)?;
	if has_signup {
		register_access_input(types, &access_signup_names.input_name, false)?;
	}
	register_authenticate_input(types, &authenticate_names.input_name)?;

	register_token_payload(
		types,
		&generic_signin_names.payload_name,
		"Return payload for the `signin` mutation.",
	)?;
	register_token_payload(
		types,
		&root_signin_names.payload_name,
		"Return payload for the `signinRoot` mutation.",
	)?;
	register_token_payload(
		types,
		&ns_signin_names.payload_name,
		"Return payload for the `signinNS` mutation.",
	)?;
	register_token_payload(
		types,
		&db_signin_names.payload_name,
		"Return payload for the `signinDB` mutation.",
	)?;
	if has_signin {
		register_token_payload(
			types,
			&access_signin_names.payload_name,
			"Return payload for the `signinAccess` mutation.",
		)?;
	}
	register_token_payload(
		types,
		&generic_signup_names.payload_name,
		"Return payload for the `signup` mutation.",
	)?;
	if has_signup {
		register_token_payload(
			types,
			&access_signup_names.payload_name,
			"Return payload for the `signupAccess` mutation.",
		)?;
	}
	register_token_payload(
		types,
		&authenticate_names.payload_name,
		"Return payload for the `authenticate` mutation.",
	)?;
	register_message_payload(
		types,
		"InvalidatePayload",
		"Return payload for the `invalidate` mutation.",
	)?;

	let mut mutation = mutation;

	mutation = mutation.field(make_signin_field(
		&generic_signin_names,
		"Sign in using the provided root, namespace, database, or access credentials.",
		SignInMode::Generic,
		ns,
		db,
		datastore,
	));
	mutation = mutation.field(make_signin_field(
		&root_signin_names,
		"Sign in as a root user.",
		SignInMode::Root,
		ns,
		db,
		datastore,
	));
	mutation = mutation.field(make_signin_field(
		&ns_signin_names,
		"Sign in as a namespace user.",
		SignInMode::Namespace,
		ns,
		db,
		datastore,
	));
	mutation = mutation.field(make_signin_field(
		&db_signin_names,
		"Sign in as a database user.",
		SignInMode::Database,
		ns,
		db,
		datastore,
	));
	if has_signin {
		mutation = mutation.field(make_signin_field(
			&access_signin_names,
			"Sign in using a database access method.",
			SignInMode::Access,
			ns,
			db,
			datastore,
		));
	}
	if has_signup {
		mutation = mutation.field(make_signup_field(
			&generic_signup_names,
			"Sign up using a database access method.",
			SignUpMode::Generic,
			ns,
			db,
			datastore,
		));
		mutation = mutation.field(make_signup_field(
			&access_signup_names,
			"Sign up using a database access method.",
			SignUpMode::Access,
			ns,
			db,
			datastore,
		));
	}

	mutation = mutation.field(make_authenticate_field(&authenticate_names, ns, db, datastore));
	mutation = mutation.field(make_invalidate_field("invalidate", "InvalidatePayload"));

	Ok(mutation)
}

fn register_signin_input(types: &mut Vec<Type>, type_name: &str) -> Result<(), GqlError> {
	let input = InputObject::new(type_name)
		.description("Parameters for signing in with a selected auth flow.")
		.field(
			InputValue::new("namespace", TypeRef::named(TypeRef::STRING))
				.description("Optionally choose the namespace to authenticate against."),
		)
		.field(
			InputValue::new("database", TypeRef::named(TypeRef::STRING))
				.description("Optionally choose the database to authenticate against."),
		)
		.field(
			InputValue::new("access", TypeRef::named(TypeRef::STRING))
				.description("Optionally choose the access method to authenticate against."),
		)
		.field(
			InputValue::new("username", TypeRef::named(TypeRef::STRING))
				.description("Provide the username for root, namespace, or database sign-in."),
		)
		.field(
			InputValue::new("password", TypeRef::named(TypeRef::STRING))
				.description("Provide the password for root, namespace, or database sign-in."),
		)
		.field(
			InputValue::new("variables", TypeRef::named("JSON"))
				.description("Provide access-method variables as a JSON object."),
		);
	types.push(Type::InputObject(input));
	Ok(())
}

fn register_signin_root_input(types: &mut Vec<Type>, type_name: &str) -> Result<(), GqlError> {
	let input = InputObject::new(type_name)
		.description("Parameters for signing in as a root user.")
		.field(
			InputValue::new("username", TypeRef::named_nn(TypeRef::STRING))
				.description("The root username."),
		)
		.field(
			InputValue::new("password", TypeRef::named_nn(TypeRef::STRING))
				.description("The root password."),
		);
	types.push(Type::InputObject(input));
	Ok(())
}

fn register_signin_ns_input(types: &mut Vec<Type>, type_name: &str) -> Result<(), GqlError> {
	let input = InputObject::new(type_name)
		.description("Parameters for signing in as a namespace user.")
		.field(
			InputValue::new("namespace", TypeRef::named(TypeRef::STRING))
				.description("Optionally override the current namespace."),
		)
		.field(
			InputValue::new("username", TypeRef::named_nn(TypeRef::STRING))
				.description("The namespace username."),
		)
		.field(
			InputValue::new("password", TypeRef::named_nn(TypeRef::STRING))
				.description("The namespace password."),
		);
	types.push(Type::InputObject(input));
	Ok(())
}

fn register_signin_db_input(types: &mut Vec<Type>, type_name: &str) -> Result<(), GqlError> {
	let input = InputObject::new(type_name)
		.description("Parameters for signing in as a database user.")
		.field(
			InputValue::new("namespace", TypeRef::named(TypeRef::STRING))
				.description("Optionally override the current namespace."),
		)
		.field(
			InputValue::new("database", TypeRef::named(TypeRef::STRING))
				.description("Optionally override the current database."),
		)
		.field(
			InputValue::new("username", TypeRef::named_nn(TypeRef::STRING))
				.description("The database username."),
		)
		.field(
			InputValue::new("password", TypeRef::named_nn(TypeRef::STRING))
				.description("The database password."),
		);
	types.push(Type::InputObject(input));
	Ok(())
}

fn register_signup_input(types: &mut Vec<Type>, type_name: &str) -> Result<(), GqlError> {
	let input = InputObject::new(type_name)
		.description("Parameters for signing up with a database access method.")
		.field(
			InputValue::new("namespace", TypeRef::named(TypeRef::STRING))
				.description("Optionally override the current namespace."),
		)
		.field(
			InputValue::new("database", TypeRef::named(TypeRef::STRING))
				.description("Optionally override the current database."),
		)
		.field(
			InputValue::new("access", TypeRef::named_nn(TypeRef::STRING))
				.description("The database access method to use for sign-up."),
		)
		.field(
			InputValue::new("variables", TypeRef::named("JSON"))
				.description("Provide access-method variables as a JSON object."),
		);
	types.push(Type::InputObject(input));
	Ok(())
}

fn register_access_input(
	types: &mut Vec<Type>,
	type_name: &str,
	is_signin: bool,
) -> Result<(), GqlError> {
	let verb = if is_signin {
		"signing in"
	} else {
		"signing up"
	};
	let input = InputObject::new(type_name)
		.description(format!("Parameters for {verb} with a database access method."))
		.field(
			InputValue::new("namespace", TypeRef::named(TypeRef::STRING))
				.description("Optionally override the current namespace."),
		)
		.field(
			InputValue::new("database", TypeRef::named(TypeRef::STRING))
				.description("Optionally override the current database."),
		)
		.field(
			InputValue::new("access", TypeRef::named_nn(TypeRef::STRING))
				.description("The database access method to use."),
		)
		.field(
			InputValue::new("variables", TypeRef::named("JSON"))
				.description("Provide access-method variables as a JSON object."),
		);
	types.push(Type::InputObject(input));
	Ok(())
}

fn register_authenticate_input(types: &mut Vec<Type>, type_name: &str) -> Result<(), GqlError> {
	let input = InputObject::new(type_name)
		.description("Parameters for validating an access token.")
		.field(
			InputValue::new("token", TypeRef::named_nn(TypeRef::STRING))
				.description("The access token to validate."),
		);
	types.push(Type::InputObject(input));
	Ok(())
}

fn register_token_payload(
	types: &mut Vec<Type>,
	type_name: &str,
	description: &str,
) -> Result<(), GqlError> {
	let payload = Object::new(type_name)
		.description(description)
		.field(
			Field::new("success", TypeRef::named_nn(TypeRef::BOOLEAN), |ctx| {
				FieldFuture::new(async move {
					let payload = ctx.parent_value.try_downcast_ref::<AuthPayloadValue>()?;
					Ok(Some(FieldValue::value(payload.success)))
				})
			})
			.description("Whether the authentication operation completed successfully.")
			.directive(semantic_non_null_directive()),
		)
		.field(
			Field::new("message", TypeRef::named(TypeRef::STRING), |ctx| {
				FieldFuture::new(async move {
					let payload = ctx.parent_value.try_downcast_ref::<AuthPayloadValue>()?;
					Ok(Some(FieldValue::value(
						payload.message.clone().map_or(GqlValue::Null, GqlValue::from),
					)))
				})
			})
			.description("A human-readable message describing the authentication result."),
		)
		.field(
			Field::new("token", TypeRef::named(TypeRef::STRING), |ctx| {
				FieldFuture::new(async move {
					let payload = ctx.parent_value.try_downcast_ref::<AuthPayloadValue>()?;
					Ok(Some(FieldValue::value(
						payload.token.clone().map_or(GqlValue::Null, GqlValue::from),
					)))
				})
			})
			.description(
				"The access token returned by the authentication operation, when available.",
			),
		)
		.directive(semantic_non_null_directive());
	types.push(Type::Object(payload));
	Ok(())
}

fn register_message_payload(
	types: &mut Vec<Type>,
	type_name: &str,
	description: &str,
) -> Result<(), GqlError> {
	let payload = Object::new(type_name)
		.description(description)
		.field(
			Field::new("success", TypeRef::named_nn(TypeRef::BOOLEAN), |ctx| {
				FieldFuture::new(async move {
					let payload = ctx.parent_value.try_downcast_ref::<AuthPayloadValue>()?;
					Ok(Some(FieldValue::value(payload.success)))
				})
			})
			.description("Whether the authentication operation completed successfully.")
			.directive(semantic_non_null_directive()),
		)
		.field(
			Field::new("message", TypeRef::named(TypeRef::STRING), |ctx| {
				FieldFuture::new(async move {
					let payload = ctx.parent_value.try_downcast_ref::<AuthPayloadValue>()?;
					Ok(Some(FieldValue::value(
						payload.message.clone().map_or(GqlValue::Null, GqlValue::from),
					)))
				})
			})
			.description("A human-readable message describing the authentication result."),
		)
		.directive(semantic_non_null_directive());
	types.push(Type::Object(payload));
	Ok(())
}

fn make_signin_field(
	names: &AuthMutationNames,
	description: &str,
	mode: SignInMode,
	default_ns: &str,
	default_db: &str,
	datastore: &Arc<Datastore>,
) -> Field {
	let payload_name = names.payload_name.clone();
	let input_name = names.input_name.clone();
	let field_name = names.field_name.clone();
	let field_name_for_log = field_name.clone();
	let kvs = datastore.clone();
	let default_ns = default_ns.to_string();
	let default_db = default_db.to_string();

	Field::new(&field_name, TypeRef::named_nn(&payload_name), move |ctx| {
		let kvs = kvs.clone();
		let default_ns = default_ns.clone();
		let default_db = default_db.clone();
		let mode = mode.clone();
		let field_name_for_log = field_name_for_log.clone();
		FieldFuture::new(async move {
			let request_sess = ctx.data::<Arc<Session>>()?;
			let input = required_input_object(&ctx)?;
			let (namespace, database, access, username, password, variables) =
				resolve_signin_inputs(&mode, input, &default_ns, &default_db)?;
			let vars = build_signin_variables(
				namespace.as_deref(),
				database.as_deref(),
				access.as_deref(),
				username.as_deref(),
				password.as_deref(),
				variables,
			)?;
			let mut auth_sess =
				build_auth_session(request_sess, namespace.as_ref(), database.as_ref());

			let token = signin::signin(&kvs, &mut auth_sess, vars).await.map_err(|e| {
				warn!("GraphQL {} failed: {e}", field_name_for_log);
				auth_error("There was a problem with authentication")
			})?;

			Ok(Some(FieldValue::owned_any(AuthPayloadValue {
				success: true,
				message: Some("Authentication succeeded.".to_string()),
				token: Some(access_token_from_token(token)),
			})))
		})
	})
	.description(description)
	.argument(
		InputValue::new("input", TypeRef::named_nn(&input_name))
			.description(format!("Parameters for `{field_name}`.")),
	)
}

fn make_signup_field(
	names: &AuthMutationNames,
	description: &str,
	mode: SignUpMode,
	default_ns: &str,
	default_db: &str,
	datastore: &Arc<Datastore>,
) -> Field {
	let payload_name = names.payload_name.clone();
	let input_name = names.input_name.clone();
	let field_name = names.field_name.clone();
	let field_name_for_log = field_name.clone();
	let kvs = datastore.clone();
	let default_ns = default_ns.to_string();
	let default_db = default_db.to_string();

	Field::new(&field_name, TypeRef::named_nn(&payload_name), move |ctx| {
		let kvs = kvs.clone();
		let default_ns = default_ns.clone();
		let default_db = default_db.clone();
		let mode = mode.clone();
		let field_name_for_log = field_name_for_log.clone();
		FieldFuture::new(async move {
			let request_sess = ctx.data::<Arc<Session>>()?;
			let input = required_input_object(&ctx)?;
			let (namespace, database, access, variables) =
				resolve_signup_inputs(&mode, input, &default_ns, &default_db)?;
			let vars = build_signup_variables(
				namespace.as_str(),
				database.as_str(),
				access.as_str(),
				variables,
			)?;
			let mut auth_sess = build_auth_session(request_sess, Some(&namespace), Some(&database));

			let token = signup::signup(&kvs, &mut auth_sess, vars).await.map_err(|e| {
				warn!("GraphQL {} failed: {e}", field_name_for_log);
				auth_error("There was a problem with authentication")
			})?;

			Ok(Some(FieldValue::owned_any(AuthPayloadValue {
				success: true,
				message: Some("Registration succeeded.".to_string()),
				token: Some(access_token_from_token(token)),
			})))
		})
	})
	.description(description)
	.argument(
		InputValue::new("input", TypeRef::named_nn(&input_name))
			.description(format!("Parameters for `{field_name}`.")),
	)
}

fn make_authenticate_field(
	names: &AuthMutationNames,
	default_ns: &str,
	default_db: &str,
	datastore: &Arc<Datastore>,
) -> Field {
	let payload_name = names.payload_name.clone();
	let input_name = names.input_name.clone();
	let kvs = datastore.clone();
	let default_ns = default_ns.to_string();
	let default_db = default_db.to_string();

	Field::new(&names.field_name, TypeRef::named_nn(&payload_name), move |ctx| {
		let kvs = kvs.clone();
		let default_ns = default_ns.clone();
		let default_db = default_db.clone();
		FieldFuture::new(async move {
			let request_sess = ctx.data::<Arc<Session>>()?;
			let input = required_input_object(&ctx)?;
			let token = required_string_field(input, "token")?;
			let mut auth_sess =
				build_auth_session(request_sess, Some(&default_ns), Some(&default_db));

			verify::token(&kvs, &mut auth_sess, &token).await.map_err(|e| {
				warn!("GraphQL authenticate failed: {e}");
				auth_error("There was a problem with authentication")
			})?;

			Ok(Some(FieldValue::owned_any(AuthPayloadValue {
				success: true,
				message: Some("The token is valid.".to_string()),
				token: Some(token),
			})))
		})
	})
	.description("Validate an access token and return it when it is still valid.")
	.argument(
		InputValue::new("input", TypeRef::named_nn(&input_name))
			.description("Parameters for `authenticate`."),
	)
}

fn make_invalidate_field(field_name: &str, payload_name: &str) -> Field {
	Field::new(field_name, TypeRef::named_nn(payload_name), move |ctx| {
		FieldFuture::new(async move {
			let sess = ctx.data::<Arc<Session>>()?;
			let mut auth_sess = (**sess).clone();
			clear::clear(&mut auth_sess).map_err(|e| auth_error(e.to_string()))?;

			Ok(Some(FieldValue::owned_any(AuthPayloadValue {
				success: true,
				message: Some("The current session has been invalidated.".to_string()),
				token: None,
			})))
		})
	})
	.description("Invalidate the current authenticated session context.")
}

fn required_input_object<'a>(
	ctx: &'a async_graphql::dynamic::ResolverContext<'a>,
) -> Result<&'a IndexMap<Name, GqlValue>, GqlError> {
	ctx.args
		.as_index_map()
		.get("input")
		.and_then(GqlValueUtils::as_object)
		.ok_or_else(|| resolver_error("Missing required 'input' argument"))
}

fn optional_string_field(
	input: &IndexMap<Name, GqlValue>,
	field_name: &str,
) -> Result<Option<String>, GqlError> {
	match input.get(field_name) {
		None | Some(GqlValue::Null) => Ok(None),
		Some(GqlValue::String(value)) => Ok(Some(value.clone())),
		Some(value) => {
			Err(resolver_error(format!("Expected `{field_name}` to be a string, got `{value}`")))
		}
	}
}

fn required_string_field(
	input: &IndexMap<Name, GqlValue>,
	field_name: &str,
) -> Result<String, GqlError> {
	optional_string_field(input, field_name)?
		.ok_or_else(|| resolver_error(format!("Missing required `{field_name}` field")))
}

fn optional_object_field<'a>(
	input: &'a IndexMap<Name, GqlValue>,
	field_name: &str,
) -> Result<Option<&'a IndexMap<Name, GqlValue>>, GqlError> {
	match input.get(field_name) {
		None | Some(GqlValue::Null) => Ok(None),
		Some(value) => value.as_object().map(Some).ok_or_else(|| {
			resolver_error(format!("Expected `{field_name}` to be a JSON object, got `{value}`"))
		}),
	}
}

fn resolve_signin_inputs<'a>(
	mode: &SignInMode,
	input: &'a IndexMap<Name, GqlValue>,
	default_ns: &str,
	default_db: &str,
) -> Result<
	(
		Option<String>,
		Option<String>,
		Option<String>,
		Option<String>,
		Option<String>,
		Option<&'a IndexMap<Name, GqlValue>>,
	),
	GqlError,
> {
	match mode {
		SignInMode::Generic => {
			let namespace = optional_string_field(input, "namespace")?;
			let database = optional_string_field(input, "database")?;
			let access = optional_string_field(input, "access")?;
			let username = optional_string_field(input, "username")?;
			let password = optional_string_field(input, "password")?;
			let variables = optional_object_field(input, "variables")?;
			let (namespace, database) = resolve_generic_signin_scope(
				namespace,
				database,
				access.as_deref(),
				default_ns,
				default_db,
			);
			Ok((namespace, database, access, username, password, variables))
		}
		SignInMode::Root => Ok((
			None,
			None,
			None,
			Some(required_string_field(input, "username")?),
			Some(required_string_field(input, "password")?),
			None,
		)),
		SignInMode::Namespace => Ok((
			Some(
				optional_string_field(input, "namespace")?
					.unwrap_or_else(|| default_ns.to_string()),
			),
			None,
			None,
			Some(required_string_field(input, "username")?),
			Some(required_string_field(input, "password")?),
			None,
		)),
		SignInMode::Database => Ok((
			Some(
				optional_string_field(input, "namespace")?
					.unwrap_or_else(|| default_ns.to_string()),
			),
			Some(
				optional_string_field(input, "database")?.unwrap_or_else(|| default_db.to_string()),
			),
			None,
			Some(required_string_field(input, "username")?),
			Some(required_string_field(input, "password")?),
			None,
		)),
		SignInMode::Access => Ok((
			Some(
				optional_string_field(input, "namespace")?
					.unwrap_or_else(|| default_ns.to_string()),
			),
			Some(
				optional_string_field(input, "database")?.unwrap_or_else(|| default_db.to_string()),
			),
			Some(required_string_field(input, "access")?),
			None,
			None,
			optional_object_field(input, "variables")?,
		)),
	}
}

fn resolve_signup_inputs<'a>(
	mode: &SignUpMode,
	input: &'a IndexMap<Name, GqlValue>,
	default_ns: &str,
	default_db: &str,
) -> Result<(String, String, String, Option<&'a IndexMap<Name, GqlValue>>), GqlError> {
	let namespace =
		optional_string_field(input, "namespace")?.unwrap_or_else(|| default_ns.to_string());
	let database =
		optional_string_field(input, "database")?.unwrap_or_else(|| default_db.to_string());
	let variables = optional_object_field(input, "variables")?;
	let access = match mode {
		SignUpMode::Generic => required_string_field(input, "access")?,
		SignUpMode::Access => required_string_field(input, "access")?,
	};

	Ok((namespace, database, access, variables))
}

fn resolve_generic_signin_scope(
	namespace: Option<String>,
	database: Option<String>,
	access: Option<&str>,
	default_ns: &str,
	default_db: &str,
) -> (Option<String>, Option<String>) {
	let should_default_ns = namespace.is_none() && (database.is_some() || access.is_some());
	let should_default_db = database.is_none() && access.is_some() && namespace.is_none();

	(
		namespace.or_else(|| should_default_ns.then(|| default_ns.to_string())),
		database.or_else(|| should_default_db.then(|| default_db.to_string())),
	)
}

fn build_auth_session(
	request_sess: &Arc<Session>,
	namespace: Option<&String>,
	database: Option<&String>,
) -> Session {
	let mut auth_sess = Session {
		ns: namespace.cloned(),
		db: database.cloned(),
		..Default::default()
	};
	auth_sess.ip.clone_from(&request_sess.ip);
	auth_sess.or.clone_from(&request_sess.or);
	auth_sess
}

fn access_token_from_token(token: Token) -> String {
	match token {
		Token::Access(token) => token,
		Token::WithRefresh {
			access,
			..
		} => access,
	}
}

fn build_signin_variables(
	ns: Option<&str>,
	db: Option<&str>,
	access: Option<&str>,
	username: Option<&str>,
	password: Option<&str>,
	variables: Option<&IndexMap<Name, GqlValue>>,
) -> Result<PublicVariables, GqlError> {
	let mut vars = PublicVariables::new();

	if let Some(variables) = variables {
		insert_public_variables(&mut vars, variables);
	}

	if let Some(ns) = ns {
		vars.insert("NS", ns.to_string());
	}
	if let Some(db) = db {
		vars.insert("DB", db.to_string());
	}
	if let Some(access) = access {
		vars.insert("AC", access.to_string());
	}
	if let Some(username) = username {
		vars.insert("user", username.to_string());
	}
	if let Some(password) = password {
		vars.insert("pass", password.to_string());
	}

	Ok(vars)
}

fn build_signup_variables(
	ns: &str,
	db: &str,
	access: &str,
	variables: Option<&IndexMap<Name, GqlValue>>,
) -> Result<PublicVariables, GqlError> {
	let mut vars = PublicVariables::new();

	if let Some(variables) = variables {
		insert_public_variables(&mut vars, variables);
	}

	vars.insert("NS", ns.to_string());
	vars.insert("DB", db.to_string());
	vars.insert("AC", access.to_string());

	Ok(vars)
}

fn insert_public_variables(vars: &mut PublicVariables, variables: &IndexMap<Name, GqlValue>) {
	for (key, val) in variables {
		let key_str = key.as_str();
		if matches!(key_str, "NS" | "ns" | "DB" | "db" | "AC" | "ac") {
			continue;
		}

		match val {
			GqlValue::Null => continue,
			GqlValue::String(s) => {
				vars.insert(key_str.to_string(), s.clone());
			}
			GqlValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					vars.insert(key_str.to_string(), i);
				} else if let Some(f) = n.as_f64() {
					vars.insert(key_str.to_string(), f);
				} else {
					vars.insert(key_str.to_string(), n.to_string());
				}
			}
			GqlValue::Boolean(b) => {
				vars.insert(key_str.to_string(), *b);
			}
			GqlValue::Enum(s) => {
				vars.insert(key_str.to_string(), s.as_str().to_string());
			}
			other => {
				vars.insert(key_str.to_string(), other.to_string());
			}
		}
	}
}
