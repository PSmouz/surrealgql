//! Shared helpers for executing GraphQL (`.graphql`) test cases.
//!
//! GraphQL tests don't go through the SurrealQL parser/executor: the source is
//! executed against the `async_graphql` dynamic schema that surrealdb-core
//! generates from the database catalog (the same path the `/graphql` HTTP
//! endpoint uses), with the datastore and session injected into the request
//! context. Used by both the test runner (`cmd::run`) and the bench runner
//! (`cmd::bench`).

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use surrealdb_core::dbs::Session;
use surrealdb_core::graphql::cache::GraphQLSchemaCache;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{Number, Value as SurValue};

use crate::tests::case::TestCase;

/// Returns the GraphQL source of a test case with the `/** ... */` config
/// comment blanked out.
///
/// The config comment is valid SurrealQL but not valid GraphQL (GraphQL only
/// has `#` line comments), so it must be removed before execution. Every
/// non-newline character in the comment span is replaced with a space so that
/// the line/column positions of the remaining source — and therefore of any
/// GraphQL error locations — are unchanged.
pub fn case_source(case: &TestCase) -> String {
	let Some(range) = case.config.range.clone() else {
		return case.source.clone();
	};
	// `range` covers the config body; widen it to include the surrounding
	// `/**` and `*/` delimiters (see `CaseConfig::parse`).
	let range = range.start - 3..range.end + 2;
	let mut source = String::with_capacity(case.source.len());
	for (idx, c) in case.source.char_indices() {
		if range.contains(&idx) && c != '\n' {
			source.push(' ');
		} else {
			source.push(c);
		}
	}
	source
}

/// Returns the request variables from the test case's `[graphql]` config
/// section, or empty variables when none are configured.
pub fn request_variables(case: &TestCase) -> Result<async_graphql::Variables> {
	let Some(variables) = case.config.parsed.graphql.variables.as_ref() else {
		return Ok(async_graphql::Variables::default());
	};
	let json =
		serde_json::to_value(variables).context("Could not convert [graphql] variables to JSON")?;
	Ok(async_graphql::Variables::from_json(json))
}

/// Builds the `async_graphql::Request` for a test case: the blanked source
/// plus the optional `[graphql]` config section (request variables and
/// operation name), with the datastore and session attached to the request
/// context for the resolvers — mirroring the server's `/graphql` handler.
pub fn build_request(
	case: &TestCase,
	dbs: &Arc<Datastore>,
	session: &Session,
) -> Result<async_graphql::Request> {
	let mut request = async_graphql::Request::new(case_source(case))
		.data(Arc::clone(dbs))
		.data(Arc::new(session.clone()));

	request.variables = request_variables(case)?;
	if let Some(operation) = case.config.parsed.graphql.operation.as_ref() {
		request = request.operation_name(operation);
	}

	Ok(request)
}

/// Generates the GraphQL schema for the session's namespace/database.
///
/// Schema-generation failures (e.g. `DEFINE CONFIG GRAPHQL` missing) are part
/// of the testable surface, so they are returned as the error string a test
/// can match on rather than as a harness error.
pub async fn generate_schema(
	dbs: &Arc<Datastore>,
	session: &Session,
) -> Result<async_graphql::dynamic::Schema, String> {
	GraphQLSchemaCache::default().get_schema(dbs, session).await.map_err(|e| e.to_string())
}

/// Converts an executed GraphQL response into a test result: the response
/// `data` as a value, or the response errors joined into an error string.
pub fn response_to_result(response: async_graphql::Response) -> Result<SurValue, String> {
	if response.errors.is_empty() {
		Ok(graphql_value_to_value(response.data))
	} else {
		Err(response.errors.iter().map(|e| e.message.as_str()).collect::<Vec<_>>().join("\n"))
	}
}

/// Converts an `async_graphql` value into a SurrealDB value for result
/// matching.
///
/// GraphQL responses are plain JSON-shaped data, so only the JSON-like
/// variants occur. Object key order is not preserved (SurrealDB objects sort
/// their keys), matching the documented behaviour of the GraphQL endpoint.
fn graphql_value_to_value(value: async_graphql::Value) -> SurValue {
	match value {
		async_graphql::Value::Null => SurValue::Null,
		async_graphql::Value::Boolean(x) => SurValue::Bool(x),
		async_graphql::Value::Number(x) => {
			if let Some(i) = x.as_i64() {
				SurValue::Number(Number::Int(i))
			} else {
				// A JSON number that doesn't fit i64; `as_f64` only returns
				// `None` for a u64 above i64::MAX, which f64 approximates.
				SurValue::Number(Number::Float(
					x.as_f64().or_else(|| x.as_u64().map(|x| x as f64)).unwrap_or(f64::NAN),
				))
			}
		}
		async_graphql::Value::String(x) => SurValue::String(x),
		async_graphql::Value::Enum(x) => SurValue::String(x.to_string()),
		async_graphql::Value::Binary(x) => SurValue::Bytes(x.to_vec().into()),
		async_graphql::Value::List(x) => {
			SurValue::Array(x.into_iter().map(graphql_value_to_value).collect())
		}
		async_graphql::Value::Object(x) => {
			let map: BTreeMap<String, SurValue> =
				x.into_iter().map(|(k, v)| (k.to_string(), graphql_value_to_value(v))).collect();
			SurValue::Object(map.into())
		}
	}
}
