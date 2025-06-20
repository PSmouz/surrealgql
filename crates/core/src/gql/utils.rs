use std::sync::Arc;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Session;
use crate::err::Error;
use crate::iam::Error as IamError;
use crate::kvs::Datastore;
use crate::kvs::LockType;
use crate::kvs::TransactionType;
use crate::sql;
use crate::sql::Function;
use crate::sql::Statement;
use crate::sql::{FlowResultExt, Ident};
use crate::sql::{Thing, Value as SqlValue};

use super::error::GqlError;
use async_graphql::dynamic::FieldValue;
use async_graphql::{dynamic::indexmap::IndexMap, Name, Value as GqlValue};
use base64::engine::general_purpose;
use base64::Engine;
use md5::Digest;
use reblessive::TreeStack;
use sha2::Sha256;

pub(crate) trait GqlValueUtils {
    fn as_i64(&self) -> Option<i64>;
    fn as_string(&self) -> Option<String>;
    fn as_list(&self) -> Option<&Vec<GqlValue>>;
    fn as_object(&self) -> Option<&IndexMap<Name, GqlValue>>;
}

impl GqlValueUtils for GqlValue {
    fn as_i64(&self) -> Option<i64> {
        if let GqlValue::Number(n) = self {
            n.as_i64()
        } else {
            None
        }
    }

    fn as_string(&self) -> Option<String> {
        if let GqlValue::String(s) = self {
            Some(s.to_owned())
        } else {
            None
        }
    }
    fn as_list(&self) -> Option<&Vec<GqlValue>> {
        if let GqlValue::List(a) = self {
            Some(a)
        } else {
            None
        }
    }
    fn as_object(&self) -> Option<&IndexMap<Name, GqlValue>> {
        if let GqlValue::Object(o) = self {
            Some(o)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct GQLTx {
    opt: Options,
    ctx: Context,
}

impl GQLTx {
    pub async fn new(kvs: &Arc<Datastore>, sess: &Session) -> Result<Self, GqlError> {
        kvs.check_anon(sess).map_err(|_| {
            Error::IamError(IamError::NotAllowed {
                actor: "anonymous".to_string(),
                action: "process".to_string(),
                resource: "graphql".to_string(),
            })
        })?;

        let tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
        let tx = Arc::new(tx);
        let mut ctx = kvs.setup_ctx()?;
        ctx.set_transaction(tx);

        sess.context(&mut ctx);

        Ok(GQLTx {
            ctx: ctx.freeze(),
            opt: kvs.setup_options(sess),
        })
    }

    pub async fn get_record_field(
        &self,
        rid: Thing,
        // field: impl Into<Part>,
        // part: &[Part],
        // path: &[&Ident]
        field_path: &str,
    ) -> Result<SqlValue, GqlError> {
        let parts: Vec<sql::Part> = field_path.split('.')
            .filter(|s| !s.is_empty())
            .map(|s| sql::Part::Field(Ident::from(s.to_string())))
            .collect();

        if parts.is_empty() {
            // Or return a more specific error if an empty path is invalid
            return Ok(SqlValue::Null);
        }
        let mut stack = TreeStack::new();
        // let part = [field.into()];
        let value = SqlValue::Thing(rid);
        stack
            .enter(|stk| value.get(stk, &self.ctx, &self.opt, None, &*parts))
            .finish()
            .await
            .catch_return()
            .map_err(Into::into)
    }

    pub async fn process_stmt(&self, stmt: Statement) -> Result<SqlValue, GqlError> {
        let mut stack = TreeStack::new();

        let res = stack
            .enter(|stk| stmt.compute(stk, &self.ctx, &self.opt, None))
            .finish()
            .await
            .catch_return()?;

        Ok(res)
    }

    pub async fn run_fn(&self, name: &str, args: Vec<SqlValue>) -> Result<SqlValue, GqlError> {
        let mut stack = TreeStack::new();
        let fun = sql::Value::Function(Box::new(Function::Custom(name.to_string(), args)));

        let res = stack
            // .enter(|stk| fnc::run(stk, &self.ctx, &self.opt, None, name, args))
            .enter(|stk| fun.compute(stk, &self.ctx, &self.opt, None))
            .finish()
            .await
            .catch_return()?;

        Ok(res)
    }
}

pub type ErasedRecord = (GQLTx, Thing);

pub fn field_val_erase_owned(val: ErasedRecord) -> FieldValue<'static> {
    FieldValue::owned_any(val)
}
// also, when the cursor pagination is for a entire db table, say e.g. not one home but homes. this homes is also wrapped with the cursor pagination as u can see from the schema generation. but then it throws the downcast error as the cox.parent_value is null as this is the query for the "new" table.

/// Recursively serializes an `SqlValue` into a canonical, stable string format.
/// This is essential for generating consistent hashes.
pub fn canonicalize_sql_value(value: &SqlValue) -> String {
    match value {
        // For objects, iterate over the already-sorted keys of the BTreeMap.
        SqlValue::Object(obj) => {
            let pairs: String = obj
                .iter()
                .map(|(k, v)| {
                    // Recursively canonicalize the value.
                    format!("\"{}\":{}", k, canonicalize_sql_value(v))
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{}}}", pairs)
        }
        // For arrays, iterate in order and canonicalize each element.
        SqlValue::Array(arr) => {
            let items: String = arr
                .0
                .iter()
                .map(canonicalize_sql_value)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{}]", items)
        }
        // Ensure scalars have a consistent representation.
        SqlValue::Strand(s) => format!("\"{}\"", s), // Quote strings
        SqlValue::Thing(t) => format!("\"{}\"", t),   // Quote things
        SqlValue::Number(n) => n.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        SqlValue::None | SqlValue::Null => "null".to_string(),
        // Add other types as needed, ensuring a stable string format.
        other => format!("\"{}\"", other.to_string()), // Fallback with quotes
    }
}

/// Hashes a canonicalized SqlValue to create a stable, content-addressable cursor.
pub fn hash_sql_value(value: &SqlValue) -> String {
    // 1. Get the canonical string representation of the value.
    let canonical_string = canonicalize_sql_value(value);

    // 2. Hash the string using a stable algorithm like SHA-256.
    let mut hasher = Sha256::new();
    hasher.update(canonical_string.as_bytes());
    let hash_result = hasher.finalize();

    // 3. Base64-encode the raw hash bytes to create a clean cursor string.
    general_purpose::URL_SAFE_NO_PAD.encode(hash_result)
}