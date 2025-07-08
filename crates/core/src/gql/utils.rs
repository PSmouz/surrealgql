use std::borrow::Cow;
use std::ops::Deref;
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
use crate::sql::Statement;
use crate::sql::{FlowResultExt, Ident};
use crate::sql::{Function, Kind};
use crate::sql::{Thing, Value as SqlValue};

use super::error::GqlError;
use async_graphql::dynamic::{Type, TypeRef};
use async_graphql::{dynamic::indexmap::IndexMap, Name, Value as GqlValue};
use reblessive::TreeStack;

pub(crate) trait GqlValueUtils {
    fn as_i64(&self) -> Option<i64>;
    fn as_string(&self) -> Option<String>;
    fn as_list(&self) -> Option<&Vec<GqlValue>>;
    fn as_object(&self) -> Option<&IndexMap<Name, GqlValue>>;
    fn as_name(&self) -> Option<&Name>;
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

    fn as_name(&self) -> Option<&Name> {
        if let GqlValue::Enum(e) = self {
            Some(e)
        } else {
            None
        }
    }
}

pub(crate) trait GqlTypeRefUtils {
    fn rename(&self, name: impl Into<String>) -> Self;
    fn to_non_null(&self) -> Self;
    /// Unwraps a `TypeRef` from a `NonNull` type, returning the inner type.
    /// If the type is not `NonNull`, it returns the type as is.
    ///
    /// This is useful for handling cases where you want to work with the underlying type
    /// without the `NonNull` wrapper.
    ///
    /// # Parameters:
    /// - `ty`: The `TypeRef` to unwrap.
    /// # Returns:
    /// - The unwrapped `TypeRef` if it was `NonNull`, or the original type if not.
    fn to_optional(&self) -> Self;
}

impl GqlTypeRefUtils for TypeRef {
    fn rename(&self, name: impl Into<String>) -> Self {
        match self {
            TypeRef::Named(_) => {
                TypeRef::Named(Cow::from(name.into()))
            }
            TypeRef::List(inner) => {
                TypeRef::List(Box::new(inner.rename(name)))
            }
            TypeRef::NonNull(inner) => {
                TypeRef::NonNull(Box::new(inner.rename(name)))
            }
        }
    }

    /// Converts a `TypeRef` to a non-nullable type.
    fn to_non_null(&self) -> Self {
        match self {
            TypeRef::Named(_) => TypeRef::NonNull(Box::new(self.clone())),
            TypeRef::List(inner) => TypeRef::List(Box::new(inner.to_non_null())),
            TypeRef::NonNull(_) => self.clone(),
        }
    }

    /// Converts a `TypeRef` to an optional type.
    fn to_optional(&self) -> Self {
        match self {
            // TypeRef::NonNull(t) => t.into_inner().to_optional(),
            TypeRef::NonNull(t) => t.deref().to_optional(),
            _ => self.clone(),
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

/// Wrapper around inflector's to_plural() method. We need this to avoid naming
/// conflicts in cases where the plural form equals the singular one. We solve
/// this by appending `List` to the pluralized name.
pub fn pluralize(input: String) -> String {
    // We need to get the singular and plural forms (e.g. "images" being the sql table name)
    // Otherwise, we would append `List` even though the singular form is different.
    let plural = inflector::string::pluralize::to_plural(&input);
    let singular = inflector::string::singularize::to_singular(&input);
    if plural == singular {
        format!("{}List", plural)
    } else {
        plural
    }
}

pub fn is_primitive(ty: &Kind) -> bool {
    match ty {
        Kind::Bool |
        Kind::Datetime |
        Kind::Decimal |
        Kind::Duration |
        Kind::Float |
        Kind::Int |
        Kind::Number |
        Kind::String |
        Kind::Uuid
        => true,
        _ => false
    }
}

/// This function is needed because the `Type` enum in async-graphql only has a pub crate method
///  name. Thus, it is inaccessible for us.
pub fn get_type_name(t: &Type) -> &str {
    match t {
        Type::Scalar(s) => s.type_name(),
        Type::Object(obj) => obj.type_name(),
        Type::InputObject(io) => io.type_name(),
        Type::Enum(e) => e.type_name(),
        Type::Interface(i) => i.type_name(),
        Type::Union(u) => u.type_name(),
        Type::Subscription(s) => s.type_name(),
        Type::Upload => "Upload",
    }
}
