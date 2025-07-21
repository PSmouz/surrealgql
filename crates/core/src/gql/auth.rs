use crate::add_to_obj;
use crate::gql::cursor::make_value_resolver;
use crate::gql::error::auth_error;
use crate::gql::schema::gql_to_sql_kind;
use crate::gql::utils::{GQLTx, GqlValueUtils};
use crate::gql::GqlError;
use crate::iam::{
    clear::clear,
    signin::signin,
    signup::signup,
    verify::token,
};
use crate::sql::statements::DefineAccessStatement;
use crate::sql::{AccessType, Base, Cond, Data, Expression, Kind, Object as SqlObject, Subquery, Value
as SqlValue};
use async_graphql::dynamic::{Enum, EnumItem, Field, FieldFuture, FieldValue, InputObject,
                             InputValue, Object as GqlObject, ResolverContext,
                             Type, TypeRef};
use async_graphql::{Name, Value as GqlValue};
use inflector::Inflector;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
// FIXME: use input or auth error??
//FIXME: what if these fields/types already exist? document for the user that these fields
// are reserved for authentication purposes and should not be used in the schema.
// session.to_owned(); may be helpful


#[derive(Clone, Debug, Copy, PartialEq)]
pub enum AuthAction {
    Signin,
    Signup,
}

#[derive(Clone, Debug, Copy)]
pub enum TokenAction {
    Authenticate,
    Invalidate,
}

#[derive(Clone, Debug)]
pub struct AuthenticationPayload {
    success: bool,
    token: Option<String>,
}

/// A versatile macro for creating `InputValue` instances for common GraphQL arguments.
///
/// This macro handles authentication fields, dynamic type fields, and provides
/// consistent syntax for optionality.
macro_rules! input {
    // --- INTERNAL HELPERS ---
    // Helper for static types like String and ID
    (@impl $name:expr, $ty:expr, $desc:expr, NON_NULL) => {
        InputValue::new($name, TypeRef::named_nn($ty)).description($desc)
    };
    (@impl $name:expr, $ty:expr, $desc:expr, NULLABLE) => {
        InputValue::new($name, TypeRef::named($ty)).description($desc)
    };
    // Helper for dynamic types passed as a string name
    (@impl_dynamic $name:expr, $type_name:expr, $desc:expr, NON_NULL) => {
        InputValue::new($name, TypeRef::named_nn($type_name)).description($desc)
    };
    (@impl_dynamic $name:expr, $type_name:expr, $desc:expr, NULLABLE) => {
        InputValue::new($name, TypeRef::named($type_name)).description($desc)
    };

    // --- STATIC KEYWORD VARIANTS ---
    (NAMESPACE, $nullability:ident) => { input!(@impl "namespace", TypeRef::STRING, "The namespace for authentication.", $nullability) };

    (DATABASE, $nullability:ident) => { input!(@impl "database", TypeRef::STRING, "The database for authentication.", $nullability) };

    (USERNAME, $nullability:ident) => { input!(@impl "username", TypeRef::STRING, "The username for authentication.", $nullability) };

    (PASSWORD, $nullability:ident) => { input!(@impl "password", TypeRef::STRING, "The password for authentication.", $nullability) };

    (TOKEN, $nullability:ident) => { input!(@impl "token", TypeRef::STRING, "The authentication token to validate.", $nullability) };

    (ACCESS, $type_name:expr, $nullability:ident) => {
        input!(@impl_dynamic "access", $type_name, "The access method to use.", $nullability)
    };

    (CREDENTIALS, $type_name:expr, $nullability:ident) => {
        input!(@impl_dynamic "credentials", $type_name, "The credentials for authentication.", $nullability)
    };

    ($fd_name: expr, $desc:expr, $nullability:ident) => { input!(@impl $fd_name,TypeRef::STRING,
    $desc, $nullability) };

    // --- DYNAMIC TYPE VARIANTS ---
    (INPUT, $type_name:expr, $desc:expr) => {
        input!(@impl_dynamic "input", $type_name, $desc, NON_NULL)
    };
    (INPUT, $type_name:expr) => {
        input!(@impl_dynamic "input", $type_name, "The input object for this mutation.", NON_NULL)
    };
}

pub async fn process_acs(
    acs: Arc<[DefineAccessStatement]>,
    mut mutation: GqlObject,
    types: &mut Vec<Type>,
) -> Result<GqlObject, GqlError> {
    let mut singin_vars_map = BTreeMap::new(); // e.g., "USER_ACCESS" -> ["user", "password"]
    let mut signup_vars_map = BTreeMap::new();
    let mut signin_enum = Enum::new("SigninAccessEnum")
        .description("An enum representing the access methods for signing in.");
    let mut signup_enum = Enum::new("SignupAccessEnum")
        .description("An enum representing the access methods for signing up.");

    let payload_obj = GqlObject::new("AuthenticationPayload")
        .field(Field::new("success", TypeRef::named(TypeRef::BOOLEAN),
                          make_value_resolver(|p: &AuthenticationPayload| p.success.clone())))
        .field(Field::new("token", TypeRef::named(TypeRef::STRING),
                          make_value_resolver(|p: &AuthenticationPayload|
                              match &p.token {
                                  Some(s) => GqlValue::from(s.clone()),
                                  None => GqlValue::Null,
                              }
                          )));

    // We can allow us multiple iters over acs, because we expect it to be small. Normally, there
    // aren't thousands of accesses.
    for ac in acs.iter() {
        let name = ac.name.to_string(); // e.g., "user_access"
        let name_gql = name.to_pascal_case(); // e.g., "UserAccess"
        let name_enum = name.to_screaming_snake_case(); // e.g., "USER_ACCESS"

        trace!(":::Processing access control::: stmt {:?}", &ac);
        if let AccessType::Record(record_access) = &ac.kind {
            if let Some(signin) = &record_access.signin {
                trace!("signin: {:?}", signin);
                let signin_vars = extract_signin_variables(signin); // "user", "password", etc.
                trace!("signin vars: {:?}", signin_vars);

                let mut vars = Vec::new(); // params: "user", "password",
                let mut creds_in_obj = InputObject::new(format!("signin{}CredentialsInput", &name_gql));

                for var in signin_vars.into_iter() {
                    let fd_gql = var.to_camel_case();
                    add_to_obj!(creds_in_obj, input!(&fd_gql, format!("The {} of the {}.", &fd_gql,
                        &name_gql), NON_NULL));

                    vars.push(var);
                }

                signin_enum = signin_enum.item(
                    EnumItem::new(&name_enum)
                        .description(format!("The access method for signing in as a {}.", &name_gql))
                );

                singin_vars_map.insert(name_enum.clone(), vars);
                types.push(Type::InputObject(creds_in_obj));
            }

            if let Some(signup) = &record_access.signup {
                trace!("signup: {:?}", signup);
                let signup_vars = extract_signin_variables(signup); // "user", "password", etc.
                trace!("signup vars: {:?}", signup_vars);

                let mut vars = Vec::new(); // params: "user", "password",
                let mut creds_in_obj = InputObject::new(format!("signup{}CredentialsInput", &name_gql));

                for var in signup_vars.into_iter() {
                    let fd_gql = var.to_camel_case();
                    add_to_obj!(creds_in_obj, input!(&fd_gql, format!("The {} of the {}.", &fd_gql,
                        &name_gql), NON_NULL));

                    vars.push(var);
                }

                signup_enum = signup_enum.item(
                    EnumItem::new(&name_enum)
                        .description(format!("The access method for signing up as a {}.", &name_gql))
                );

                signup_vars_map.insert(name_enum.clone(), vars);
                types.push(Type::InputObject(creds_in_obj));
            }
        }
    }

    // ===============================
    // Add authenticate field
    // ===============================

    add_to_obj!(mutation,
        Field::new(
            "authenticate",
            TypeRef::named(payload_obj.type_name()),
            make_auth_resolver(TokenAction::Authenticate),
        )
            .argument(input!(TOKEN, NON_NULL))
            .description("Authenticate a user with a token. The token must be valid and not expired.")
    );

    // ===============================
    // Add signin field
    // ===============================

    let mut all_signin_obj = InputObject::new("SigninAccessInput");

    for v in singin_vars_map.values().flatten() {
        add_to_obj!(all_signin_obj,
            input!(v.to_camel_case(), format!("The {} sign-in credentials.", &v.to_camel_case()),
                NON_NULL)
        );
    }

    add_to_obj!(mutation,
        Field::new(
            "signin",
            TypeRef::named(payload_obj.type_name()),
            // None because we want the generic resolver
            make_access_resolver(AuthAction::Signin, None, singin_vars_map.clone()),
        )
            .argument(input!(NAMESPACE, NULLABLE))
            .argument(input!(DATABASE, NULLABLE))
            .argument(input!(USERNAME, NULLABLE))
            .argument(input!(PASSWORD, NULLABLE))
            .argument(input!(ACCESS, signin_enum.type_name(), NULLABLE))
            .argument(input!(CREDENTIALS, all_signin_obj.type_name(), NULLABLE))
            .description("Sign in as a user. The access must be valid and not expired.")
    );

    types.push(Type::InputObject(all_signin_obj));
    types.push(Type::Enum(signin_enum));

    // ===============================
    // Add signin access field
    // ===============================

    for ac in acs.iter() {
        let name = ac.name.to_string();
        let name_gql = name.to_pascal_case();

        if let AccessType::Record(record_access) = &ac.kind {
            if let Some(_) = &record_access.signin {
                add_to_obj!(
                    mutation,
                    Field::new(
                        format!("signin{}Access", &name_gql),
                        TypeRef::named(payload_obj.type_name()),
                        make_access_resolver(AuthAction::Signin, Some(name), singin_vars_map.clone())
                    )
                        .argument(input!(NAMESPACE, NON_NULL))
                        .argument(input!(DATABASE, NON_NULL))
                        .argument(input!(CREDENTIALS, format!("signin{}CredentialsInput",
                        &name_gql), NON_NULL))
                        .description(format!("Sign in as a {}.", &name_gql))
                );
            }
        }
    }

    // ===============================
    // Add signinRoot field
    // ===============================

    add_to_obj!(
        mutation,
        Field::new(
            "signinRoot",
            TypeRef::named(payload_obj.type_name()),
            make_system_signin_resolver(Base::Root),
        )
            .argument(input!(USERNAME, NON_NULL))
            .argument(input!(PASSWORD, NON_NULL))
            .description("Sign in as a root user.")
    );

    // ===============================
    // Add signinNamespace field
    // ===============================

    add_to_obj!(
        mutation,
        Field::new(
            "signinNS",
            TypeRef::named(payload_obj.type_name()),
            make_system_signin_resolver(Base::Ns),
        )
            .argument(input!(NAMESPACE, NON_NULL))
            .argument(input!(USERNAME, NON_NULL))
            .argument(input!(PASSWORD, NON_NULL))
            .description("Sign in as a namespace user.")
    );

    // ===============================
    // Add signinDatabase field
    // ===============================

    add_to_obj!(
        mutation,
        Field::new(
            "signinDB",
            TypeRef::named(payload_obj.type_name()),
            make_system_signin_resolver(Base::Db),
        )
            .argument(input!(NAMESPACE, NON_NULL))
            .argument(input!(DATABASE, NON_NULL))
            .argument(input!(USERNAME, NON_NULL))
            .argument(input!(PASSWORD, NON_NULL))
            .description("Sign in as a namespace user.")
    );

    // ===============================
    // Add signup field
    // ===============================

    let mut all_signup_obj = InputObject::new("SignupAccessInput");

    for v in signup_vars_map.values().flatten() {
        add_to_obj!(all_signup_obj,
            input!(v.to_camel_case(), format!("The {} sign-up credentials.", &v.to_camel_case()),
                NON_NULL)
        );
    }

    add_to_obj!(mutation,
        Field::new(
            "signup",
            TypeRef::named(payload_obj.type_name()),
            // None because we want the generic resolver
            make_access_resolver(AuthAction::Signup, None, signup_vars_map.clone()),
        )
            .argument(input!(NAMESPACE, NON_NULL))
            .argument(input!(DATABASE, NON_NULL))
            .argument(input!(ACCESS, signup_enum.type_name(), NON_NULL))
            .argument(input!(CREDENTIALS, all_signup_obj.type_name(), NON_NULL))
            .description("Sign up a user. The access must be valid and not expired.")
    );

    types.push(Type::InputObject(all_signup_obj));
    types.push(Type::Enum(signup_enum));

    // ===============================
    // Add signup access field
    // ===============================

    for ac in acs.iter() {
        let name = ac.name.to_string();
        let name_gql = name.to_pascal_case();

        if let AccessType::Record(record_access) = &ac.kind {
            if let Some(_) = &record_access.signup {
                add_to_obj!(
                    mutation,
                    Field::new(
                        format!("signup{}Access", &name_gql),
                        TypeRef::named(payload_obj.type_name()),
                        make_access_resolver(AuthAction::Signup, Some(name), signup_vars_map.clone())
                    )
                        .argument(input!(NAMESPACE, NON_NULL))
                        .argument(input!(DATABASE, NON_NULL))
                        .argument(input!(CREDENTIALS, format!("signup{}CredentialsInput",
                        &name_gql), NON_NULL))
                        .description(format!("Sign up as a {}.", &name_gql))
                );
            }
        }
    }

    // ===============================
    // Add invalidate field
    // ===============================

    add_to_obj!(mutation,
        Field::new(
            "invalidate",
            TypeRef::named(payload_obj.type_name()),
            make_auth_resolver(TokenAction::Invalidate),
        )
            .description("Invalidate the current authentication token. This will log out the user \
             but not remove the token from the database.")
    );

    // push last, because we need it for the type_names previously
    types.push(Type::Object(payload_obj));

    Ok(mutation)
}

/// A generic resolver for `signin` and `signup` mutations against a specific, hardcoded scope.
///
/// # Parameters
/// - `action`: The action to perform, either `Signin` or `Signup`.
/// - `access`: An optional string representing the original access name (e.g., "user_access").
/// - `vars_map`: A map of access types to their required variables, e.g., "USER_ACCESS" -> ["user", "password"]
pub fn make_access_resolver(
    action: AuthAction,
    access: Option<String>,
    vars_map: BTreeMap<String, Vec<String>>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
    move |ctx: ResolverContext| {
        let action = action; //TODO: investigate copy trait and if & bowwor here or just remove
        // this line
        let access_opt = access.clone();
        let vars_map = vars_map.clone();

        FieldFuture::new(async move {
            let gtx = ctx.data::<GQLTx>()?;
            let args = ctx.args.as_index_map();
            let mut session = gtx.session().clone();
            let kvs = gtx.kvs();

            let mut params = BTreeMap::new();

            // Case 1: Access is provided as a string, e.g., "user", "admin",
            // => specific access resolver
            if let Some(access) = access_opt {
                let access_name = access.to_screaming_snake_case();
                let required_vars = vars_map.get(&access_name)
                    .ok_or_else(|| auth_error(format!("Access type '{}' not found.", access_name)))?;
                let credentials = args.get("credentials")
                    .and_then(GqlValueUtils::as_object)
                    .ok_or_else(|| auth_error("Credentials are required."))?;

                // var: e.g., "user", "password", etc.
                for var in required_vars.iter() {
                    let gql_val = credentials.get(&Name::new(var.to_camel_case()))
                        .ok_or_else(|| auth_error(format!("Missing required credential: {}", var)))?;
                    let sql_val = gql_to_sql_kind(gql_val, Kind::String)?;

                    params.insert(var.to_string(), sql_val);
                }
                //TODO: bug: here original name use, not the snake case
                params.insert("AC".to_string(), SqlValue::from(access_name));

                // Case 2: Access is provided as an enum, e.g., "USER_ACCESS"
                // => generic access resolver
            } else if let Some(access_enum) = args.get("access").and_then(GqlValueUtils::as_string) {
                let required_vars = vars_map.get(&access_enum.to_screaming_snake_case())
                    .ok_or_else(|| auth_error(format!("Access type '{}' not found.", access_enum)))?;
                let credentials = args.get("credentials")
                    .and_then(GqlValueUtils::as_object)
                    .ok_or_else(|| auth_error("Credentials are required."))?;

                for var in required_vars.iter() {
                    // Could also use .and_then(GqlValueUtils::as_string), like in branch 3
                    let gql_val = credentials.get(&Name::new(var.to_camel_case()))
                        .ok_or_else(|| auth_error(format!("Missing required credential: {}", var)))?;
                    let sql_val = gql_to_sql_kind(gql_val, Kind::String)?;

                    params.insert(var.to_string(), sql_val);
                }
                //TODO: bug: here original name use, not the snake case
                params.insert("AC".to_string(), SqlValue::from(access_enum));

                // Case 3: Access is not provided AND signin
                // => generic go through root, namespace, or database user
            } else if action == AuthAction::Signin {
                // If no access is provided, we assume the user is trying to sign in with a root, namespace, or database user.
                // We will use the username and password arguments directly.
                let user = args.get("username").and_then(GqlValueUtils::as_string)
                    .ok_or_else(|| auth_error("Username is required."))?;
                let pass = args.get("password").and_then(GqlValueUtils::as_string)
                    .ok_or_else(|| auth_error("Password is required."))?;

                params.insert("user".to_string(), SqlValue::from(user));
                params.insert("pass".to_string(), SqlValue::from(pass));

                if let Some(ns) = args.get("namespace").and_then(GqlValueUtils::as_string) {
                    params.insert("NS".to_string(), SqlValue::from(ns));
                }

                if let Some(db) = args.get("database").and_then(GqlValueUtils::as_string) {
                    params.insert("DB".to_string(), SqlValue::from(db));
                }
            } else {
                return Err(auth_error("Missing `Access` for sign-up action.").into());
            }

            match action {
                AuthAction::Signin => {
                    match signin(&kvs, &mut session, SqlObject::from(params)).await {
                        Ok(data) => {
                            let payload = AuthenticationPayload {
                                success: true,
                                token: Some(data.token),
                            };

                            Ok(Some(FieldValue::owned_any(payload)))
                        }
                        Err(e) => Err(auth_error(e).into()),
                    }
                }
                AuthAction::Signup => {
                    match signup(&kvs, &mut session, SqlObject::from(params)).await {
                        Ok(data) => {
                            let payload = AuthenticationPayload {
                                success: true,
                                token: data.token,
                            };

                            Ok(Some(FieldValue::owned_any(payload)))
                        }
                        Err(e) => Err(auth_error(e).into()),
                    }
                }
            }
        })
    }
}

/// A resolver for system-level sign-in (ROOT, NAMESPACE, DATABASE).
pub fn make_system_signin_resolver(
    base: Base,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
    move |ctx: ResolverContext| {
        let base = base.clone();
        FieldFuture::new(async move {
            let gtx = ctx.data::<GQLTx>()?;
            let args = ctx.args.as_index_map();
            let mut session = gtx.session().clone();
            let kvs = gtx.kvs();

            let user = args.get("username").and_then(GqlValueUtils::as_string).ok_or_else(|| auth_error("Username is required."))?;
            let pass = args.get("password").and_then(GqlValueUtils::as_string).ok_or_else(|| auth_error("Password is required."))?;

            let mut vars = BTreeMap::new();
            vars.insert("user".to_string(), user);
            vars.insert("pass".to_string(), pass);

            match base {
                Base::Root => {}
                Base::Ns => {
                    let ns = args.get("namespace").and_then(GqlValueUtils::as_string).ok_or_else(|| auth_error("Namespace is required."))?;
                    vars.insert("NS".to_string(), ns);
                }
                Base::Db => {
                    let ns = args.get("namespace").and_then(GqlValueUtils::as_string).ok_or_else(|| auth_error("Namespace is required."))?;
                    let db = args.get("database").and_then(GqlValueUtils::as_string).ok_or_else(|| auth_error("Database is required."))?;
                    vars.insert("NS".to_string(), ns);
                    vars.insert("DB".to_string(), db);
                }
                _ => {} // TODO(gguillemas): remove this case in 3.0.0
            }

            match signin(&kvs, &mut session, SqlObject::from(vars)).await {
                Ok(data) => {
                    let payload = AuthenticationPayload {
                        success: true,
                        token: Some(data.token),
                    };

                    Ok(Some(FieldValue::owned_any(payload)))
                }
                Err(e) => Err(auth_error(e).into()),
            }
        })
    }
}

/// A resolver for token-based actions like AUTHENTICATE and INVALIDATE.
pub fn make_auth_resolver(
    action: TokenAction,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
    move |ctx: ResolverContext| {
        let action = action;
        FieldFuture::new(async move {
            let gtx = ctx.data::<GQLTx>()?;
            let kvs = gtx.kvs();
            let mut session = gtx.session().clone();

            let res = match action {
                TokenAction::Authenticate => {
                    let token_str = ctx.args.get("token").unwrap().string()
                        .map_err(|_| auth_error("Token is required for authentication."))?;

                    token(kvs, &mut session, token_str).await
                }
                TokenAction::Invalidate => clear(&mut session)
            };

            match res {
                Ok(_) => {
                    let payload = AuthenticationPayload {
                        success: true,
                        token: None,
                    };

                    Ok(Some(FieldValue::owned_any(payload)))
                }
                Err(e) => return Err(auth_error(e).into()),
            }
        })
    }
}

fn extract_signin_variables(signin_value: &SqlValue) -> Vec<String> {
    let mut variables = HashSet::new();
    extract_variables_recursive(signin_value, &mut variables);

    // Filter out system parameters that are automatically provided
    let filtered_vars: Vec<String> = variables
        .into_iter()
        .filter(|var| {
            // Exclude system parameters that are automatically set by the GraphQL context
            !matches!(var.to_uppercase().as_str(), "NS" | "DB" | "AC")
        })
        .collect();

    let mut sorted_vars = filtered_vars;
    sorted_vars.sort();
    sorted_vars
}

fn extract_variables_recursive(value: &SqlValue, variables: &mut HashSet<String>) {
    match value {
        SqlValue::Param(param) => {
            variables.insert(param.0.to_string());
        }
        SqlValue::Expression(expr) => {
            match &**expr {
                Expression::Binary { l, r, .. } => {
                    extract_variables_recursive(l, variables);
                    extract_variables_recursive(r, variables);
                }
                Expression::Unary { v, .. } => {
                    extract_variables_recursive(v, variables);
                }
            }
        }
        SqlValue::Function(func) => {
            for arg in func.args() {
                extract_variables_recursive(arg, variables);
            }
        }
        SqlValue::Subquery(subquery) => {
            match &**subquery {
                Subquery::Select(select) => {
                    if let Some(cond) = &select.cond {
                        extract_variables_from_cond(cond, variables);
                    }
                    // Also check fields and other parts of SELECT
                    for field in &select.expr.0 {
                        extract_variables_from_field(field, variables);
                    }
                }
                Subquery::Create(create) => {
                    // Handle CREATE statements (common in SIGNUP clauses)
                    if let Some(data) = &create.data {
                        extract_variables_from_data(data, variables);
                    }
                }
                Subquery::Update(update) => {
                    // Handle UPDATE statements
                    if let Some(data) = &update.data {
                        extract_variables_from_data(data, variables);
                    }
                    if let Some(cond) = &update.cond {
                        extract_variables_from_cond(cond, variables);
                    }
                }
                _ => {}
            }
        }
        SqlValue::Array(array) => {
            for item in &array.0 {
                extract_variables_recursive(item, variables);
            }
        }
        SqlValue::Object(object) => {
            for (_, val) in &object.0 {
                extract_variables_recursive(val, variables);
            }
        }
        _ => {}
    }
}

fn extract_variables_from_cond(cond: &Cond, variables: &mut HashSet<String>) {
    extract_variables_recursive(&cond.0, variables);
}

fn extract_variables_from_field(field: &crate::sql::Field, variables: &mut HashSet<String>) {
    match field {
        crate::sql::Field::Single { expr, .. } => {
            extract_variables_recursive(expr, variables);
        }
        crate::sql::Field::All => {}
    }
}

fn extract_variables_from_data(data: &Data, variables: &mut HashSet<String>) {
    match data {
        Data::SetExpression(obj) => {
            for (_, _, val) in obj {
                extract_variables_recursive(val, variables);
            }
        }
        Data::UpdateExpression(ops) => {
            for (_, _, val) in ops {
                extract_variables_recursive(val, variables);
            }
        }
        Data::PatchExpression(val) => {
            extract_variables_recursive(val, variables);
        }
        Data::MergeExpression(val) => {
            extract_variables_recursive(val, variables);
        }
        Data::ReplaceExpression(val) => {
            extract_variables_recursive(val, variables);
        }
        Data::ContentExpression(val) => {
            extract_variables_recursive(val, variables);
        }
        Data::SingleExpression(val) => {
            extract_variables_recursive(val, variables);
        }
        Data::ValuesExpression(vals) => {
            for row in vals {
                for (_, val) in row {
                    extract_variables_recursive(val, variables);
                }
            }
        }
        Data::EmptyExpression => {}
        Data::UnsetExpression(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbs::Session;
    use crate::gql::schema::generate_schema;
    use crate::kvs::Datastore;
    use crate::sql::{Array, Ident, Operator, Param, Value};
    use async_graphql::dynamic::Schema;
    use serde_json::json;

    async fn mock(ast: &str) -> Schema {
        let ds = Datastore::new("memory").await.unwrap();
        let sess = Session::owner().with_ns("test").with_db("test");

        ds.execute(ast, &sess, None).await.unwrap();
        let schema = generate_schema(&Arc::new(ds), &sess).await.unwrap();

        // (ds, sess, schema) (Datastore, Session, Schema)
        schema
    }

    #[tokio::test]
    async fn test_extract_variables_recursive() {

        // let mut sess = Session {
        //     ns: Some("test".to_string()),
        //     db: Some("test".to_string()),
        //     ..Default::default()
        // };
        // let mut vars: HashMap<&str, Value> = HashMap::new();
        // vars.insert("user", "user".into());
        // vars.insert("pass", "pass".into());
        // let res = db_access(
        //     &ds,
        //     &mut sess,
        //     "test".to_string(),
        //     "test".to_string(),
        //     "user".to_string(),
        //     vars.into(),
        // )
        //     .await;

        // assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
        // assert_eq!(sess.ns, Some("test".to_string()));
        // assert_eq!(sess.db, Some("test".to_string()));
        // assert_eq!(sess.au.id(), "user:test");
        // assert!(sess.au.is_record());
        // assert_eq!(sess.au.level().ns(), Some("test"));
        // assert_eq!(sess.au.level().db(), Some("test"));
        // assert_eq!(sess.au.level().id(), Some("user:test"));
        // // Record users should not have roles
        // assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
        // assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
        // assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
        // // Expiration should match the defined duration
        // let exp = sess.exp.unwrap();
        // // Expiration should match the current time plus session duration with some margin
        // let min_exp = (Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
        // let max_exp = (Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
        // assert!(
        //     exp > min_exp && exp < max_exp,
        //     "Session expiration is expected to follow the defined duration"
        // );
    }

    #[tokio::test]
    async fn test_extract_variables_from_cond() {}

    #[tokio::test]
    async fn test_extract_variables_from_field() {}

    #[tokio::test]
    async fn test_extract_variables_from_data() {}

    #[tokio::test]
    async fn test_authenticate() {
        // Test with a valid token
        {
            let ast = r#"
            DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER DURATION FOR SESSION 15m, FOR TOKEN 15m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;
            let schema = mock(ast).await;

            // First, sign in to get a valid token
            let signin_query = r#"
            mutation SigninRoot {
                signinRoot(username: "root", password: "root") {
                    token
                }
            }"#;
            let signin_res = schema.execute(signin_query).await;
            let signin_json = signin_res.data.into_json().unwrap();
            let token = signin_json["signinRoot"]["token"].as_str().unwrap();

            // Now, authenticate with the obtained token
            let auth_query = format!(
                r#"
            mutation Authenticate {{
                authenticate(token: "{}") {{
                    success
                    token
                }}
            }}"#,
                token
            );

            let auth_res = schema.execute(&auth_query).await;
            let auth_json = auth_res.data.into_json().unwrap();
            let data = &auth_json["authenticate"];

            assert_eq!(data["success"], json!(true));
            assert!(data["token"].is_null(), "Token should be null on successful authentication");
        }
        // Test with an expired token
        {
            let ast = r#"
            DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER DURATION FOR SESSION 1m, FOR
            TOKEN 1s;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;
            let schema = mock(ast).await;

            // First, sign in to get a valid token
            let signin_query = r#"
            mutation SigninRoot {
                signinRoot(username: "root", password: "root") {
                    token
                }
            }"#;
            let signin_res = schema.execute(signin_query).await;
            let signin_json = signin_res.data.into_json().unwrap();
            let token = signin_json["signinRoot"]["token"].as_str().unwrap().to_string();

            // Wait for the token to expire
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

            let auth_query = format!(
                r#"
            mutation Authenticate {{
                authenticate(token: "{}") {{
                    success
                    token
                }}
            }}"#,
                token
            );

            let auth_res = schema.execute(&auth_query).await;
            let json = auth_res.data.into_json().unwrap();
            let err = auth_res.errors.get(0).unwrap();

            assert!(json.is_null(), "Data should be null");
            assert_eq!(err.message, "Authentication Error: The token has expired");
        }
        // Test with an empty token
        {
            let ast = r#"
            DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER DURATION FOR SESSION 15m, FOR TOKEN 15m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;
            let schema = mock(ast).await;

            let query = r#"
            mutation Authenticate {
                authenticate(token: "") {
                    success
                    token
                }
            }"#;

            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let err = res.errors.get(0).unwrap();

            assert!(json.is_null(), "Data should be null");
            assert_eq!(err.message, "Authentication Error: There was a problem with authentication");
        }
    }

    #[tokio::test]
    async fn test_invalidate() {}

    #[tokio::test]
    async fn test_signin_root() {
        // Test with correct credentials
        {
            let ast = r#"
            DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER DURATION FOR SESSION 15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation SigninRoot {
                signinRoot(username: "root", password: "root") {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let data = &json["signinRoot"];

            assert_eq!(data["success"], json!(true));
            assert!(data["token"].as_str().is_some(), "Token should be a string");
            assert!(!data["token"].as_str().unwrap().is_empty(), "Token should not be empty");
        }
        // Test with incorrect credentials
        {
            let ast = r#"
            DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER DURATION FOR SESSION 15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation SigninRoot {
                signinRoot(username: "", password: "root") {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let err = res.errors.get(0).unwrap();

            assert!(json.is_null(), "Data should be null");
            assert_eq!(err.message, "Authentication Error: There was a problem with authentication");
        }
        // Test with missing credentials
        {
            let ast = r#"
            DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER DURATION FOR SESSION 15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation SigninRoot {
                signinRoot(password: "root") {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let err = res.errors.get(0).unwrap();

            assert!(json.is_null(), "Data should be null");
            assert_eq!(err.message, "Field \"signinRoot\" argument \"username\" of type \"Mutation\" is required but not provided");
        }
    }

    #[tokio::test]
    async fn test_signin_ns() {
        // Test with correct credentials
        {
            let ast = r#"
            DEFINE USER username ON NAMESPACE PASSWORD '123456' ROLES EDITOR DURATION FOR SESSION
             15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation SigninNS {
                signinNS(
                    namespace: "test",
                    username: "username",
                    password: "123456"
                ) {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let data = &json["signinNS"];

            assert_eq!(data["success"], json!(true));
            assert!(data["token"].as_str().is_some(), "Token should be a string");
            assert!(!data["token"].as_str().unwrap().is_empty(), "Token should not be empty");
        }
        // Test with incorrect credentials
        {
            let ast = r#"
            DEFINE USER username ON NAMESPACE PASSWORD '123456' ROLES EDITOR DURATION FOR SESSION
             15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation SigninNS {
                signinNS(
                    namespace: "test",
                    username: "username",
                    password: "wrong_password"
                ) {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let err = res.errors.get(0).unwrap();

            assert!(json.is_null(), "Data should be null");
            assert_eq!(err.message, "Authentication Error: There was a problem with authentication");
        }
        // Test with missing credentials
        {
            let ast = r#"
            DEFINE USER username ON NAMESPACE PASSWORD '123456' ROLES EDITOR DURATION FOR SESSION
             15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation SigninNS {
                signinNS(
                    username: "username",
                    password: "123456"
                ) {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let err = res.errors.get(0).unwrap();

            assert!(json.is_null(), "Data should be null");
            assert_eq!(err.message, "Field \"signinNS\" argument \"namespace\" of type \"Mutation\" is required but not provided");
        }
    }

    #[tokio::test]
    async fn test_signin_db() {
        // Test with correct credentials
        {
            let ast = r#"
            DEFINE USER username ON DATABASE PASSWORD '123456' ROLES OWNER DURATION FOR SESSION 15m,
             FOR
            TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation SigninDB {
                signinDB(
                    namespace: "test",
                    database: "test",
                    username: "username",
                    password: "123456"
                ) {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let data = &json["signinDB"];

            assert_eq!(data["success"], json!(true));
            assert!(data["token"].as_str().is_some(), "Token should be a string");
            assert!(!data["token"].as_str().unwrap().is_empty(), "Token should not be empty");
        }
        // Test with incorrect credentials
        {
            let ast = r#"
            DEFINE USER username ON DATABASE PASSWORD '123456' ROLES OWNER DURATION FOR SESSION 15m,
             FOR
            TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation SigninDB {
                signinDB(
                    namespace: "test",
                    database: "test",
                    username: "username",
                    password: "wrong_password"
                ) {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let err = res.errors.get(0).unwrap();

            assert!(json.is_null(), "Data should be null");
            assert_eq!(err.message, "Authentication Error: There was a problem with authentication");
        }
        // Test with missing credentials
        {
            let ast = r#"
            DEFINE USER username ON DATABASE PASSWORD '123456' ROLES OWNER DURATION FOR SESSION
            15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation SigninDB {
                signinDB(
                    namespace: "test",
                    username: "username",
                    password: "123456"
                ) {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let err = res.errors.get(0).unwrap();

            assert!(json.is_null(), "Data should be null");
            assert_eq!(err.message, "Field \"signinDB\" argument \"database\" of type \"Mutation\" is required but not provided");
        }
    }

    #[tokio::test]
    async fn test_signin_access() {}

    #[tokio::test]
    async fn test_signin_generic_root() {
        // Test with correct credentials
        {
            let ast = r#"
            DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER DURATION FOR SESSION 15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation Signin {
                signin(username: "root", password: "root") {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let data = &json["signin"];

            assert_eq!(data["success"], json!(true));
            assert!(data["token"].as_str().is_some(), "Token should be a string");
            assert!(!data["token"].as_str().unwrap().is_empty(), "Token should not be empty");
        }
        // Test with incorrect credentials
        {
            let ast = r#"
            DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER DURATION FOR SESSION 15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation signin {
                signin(username: "", password: "root") {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let err = res.errors.get(0).unwrap();

            assert!(json.is_null(), "Data should be null");
            assert_eq!(err.message, "Authentication Error: There was a problem with authentication");
        }
        // Test with missing credentials
        {
            let ast = r#"
            DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER DURATION FOR SESSION 15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation signin {
                signin(password: "root") {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let err = res.errors.get(0).unwrap();

            assert!(json.is_null(), "Data should be null");
            assert_eq!(err.message, "Authentication Error: Username is required.");
        }
    }

    #[tokio::test]
    async fn test_signin_generic_ns() {
        // Test with correct credentials
        {
            let ast = r#"
            DEFINE USER username ON NAMESPACE PASSWORD '123456' ROLES OWNER DURATION FOR SESSION
            15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation Signin {
                signin(
                    namespace: "test",
                    username: "username",
                    password: "123456"
                ) {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let data = &json["signin"];

            assert_eq!(data["success"], json!(true));
            assert!(data["token"].as_str().is_some(), "Token should be a string");
            assert!(!data["token"].as_str().unwrap().is_empty(), "Token should not be empty");
        }
        // Test with incorrect credentials
        {
            let ast = r#"
            DEFINE USER username ON NAMESPACE PASSWORD '123456' ROLES OWNER DURATION FOR SESSION
            15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation Signin {
                signin(
                    namespace: "test",
                    username: "username",
                    password: "wrong_password"
                ) {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let err = res.errors.get(0).unwrap();

            assert!(json.is_null(), "Data should be null");
            assert_eq!(err.message, "Authentication Error: There was a problem with authentication");
        }
        // Test with missing credentials/wrong "scope"
        {
            let ast = r#"
            DEFINE USER username ON NAMESPACE PASSWORD '123456' ROLES OWNER DURATION FOR SESSION
            15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation Signin {
                signin(
                    username: "username",
                    password: "123456"
                ) {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let err = res.errors.get(0).unwrap();

            assert!(json.is_null(), "Data should be null");
            assert_eq!(err.message, "Authentication Error: There was a problem with authentication");
        }
    }

    #[tokio::test]
    async fn test_signin_generic_db() {
        // Test with correct credentials
        {
            let ast = r#"
            DEFINE USER username ON DATABASE PASSWORD '123456' ROLES OWNER DURATION FOR SESSION
            15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation Signin {
                signin(
                    namespace: "test",
                    database: "test",
                    username: "username",
                    password: "123456"
                ) {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let data = &json["signin"];

            assert_eq!(data["success"], json!(true));
            assert!(data["token"].as_str().is_some(), "Token should be a string");
            assert!(!data["token"].as_str().unwrap().is_empty(), "Token should not be empty");
        }
        // Test with incorrect credentials
        {
            let ast = r#"
            DEFINE USER username ON DATABASE PASSWORD '123456' ROLES OWNER DURATION FOR SESSION
            15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation Signin {
                signin(
                    namespace: "test",
                    database: "test",
                    username: "username",
                    password: "wrong_password"
                ) {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let err = res.errors.get(0).unwrap();

            assert!(json.is_null(), "Data should be null");
            assert_eq!(err.message, "Authentication Error: There was a problem with authentication");
        }
        // Test with missing credentials
        {
            let ast = r#"
            DEFINE USER username ON DATABASE PASSWORD '123456' ROLES OWNER DURATION FOR SESSION
            15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation Signin {
                signin(
                    namespace: "test",
                    username: "username",
                    password: "123456"
                ) {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let err = res.errors.get(0).unwrap();

            assert!(json.is_null(), "Data should be null");
            assert_eq!(err.message, "Authentication Error: There was a problem with authentication");
        }
        // Test with missing credentials
        {
            let ast = r#"
            DEFINE USER username ON DATABASE PASSWORD '123456' ROLES OWNER DURATION FOR SESSION
            15m, FOR TOKEN 1m;

            DEFINE TABLE book SCHEMAFULL;
            DEFINE FIELD name ON TABLE book TYPE string;
            DEFINE CONFIG GRAPHQL AUTO;
            "#;

            let query = r#"
            mutation Signin {
                signin(
                    database: "test",
                    username: "username",
                    password: "123456"
                ) {
                    success
                    token
                }
            }"#;

            let schema = mock(ast).await;
            let res = schema.execute(query).await;
            let json = res.data.into_json().unwrap();
            let err = res.errors.get(0).unwrap();

            assert!(json.is_null(), "Data should be null");
            assert_eq!(err.message, "Authentication Error: No signin target to either SC or DB or NS or KV");
        }
    }

    #[tokio::test]
    async fn test_signin_generic_access() {}

    #[tokio::test]
    async fn test_signup_access() {}

    #[tokio::test]
    async fn test_signup_generic_access() {}

    // Helper function to simplify test assertions by running the recursive extraction
    // and returning a sorted Vec<String> of variable names.
    fn run_extraction(value: &Value) -> Vec<String> {
        let mut variables = HashSet::new();
        let sql_value: Value = value.clone().into();
        extract_variables_recursive(&sql_value, &mut variables);
        let mut result: Vec<String> = variables.into_iter().collect();
        result.sort();
        result
    }

    // Tests for the main public function: `extract_signin_variables`
    #[test]
    fn test_extract_signin_variables_filters_system_params() {
        let value = Value::Array(Array(vec![
            Value::Param(Param(Ident("NS".into()))),
            Value::Param(Param(Ident("DB".into()))),
            Value::Param(Param(Ident("AC".into()))),
            Value::Param(Param(Ident("regular_param".into()))),
        ]));
        let result = extract_signin_variables(&value.into());
        assert_eq!(result, vec!["regular_param"]);
    }

    #[test]
    fn test_extract_signin_variables_sorts_output() {
        let value = Value::Array(Array(vec![
            Value::Param(Param(Ident("zeta".into()))),
            Value::Param(Param(Ident("alpha".into()))),
            Value::Param(Param(Ident("beta".into()))),
        ]));
        let result = extract_signin_variables(&value.into());
        assert_eq!(result, vec!["alpha", "beta", "zeta"]);
    }

    // Tests for the recursive extraction logic: `extract_variables_recursive`
    #[test]
    fn test_extract_variables_from_param() {
        let value = Value::Param(Param(Ident("user_id".to_string())));
        let result = run_extraction(&value);
        assert_eq!(result, vec!["user_id"]);
    }

    #[test]
    fn test_extract_variables_from_binary_expression() {
        let value = Value::Expression(Box::new(Expression::Binary {
            l: Value::Param(Param(Ident("email".to_string()))).into(),
            o: Operator::Equal,
            r: Value::Param(Param(Ident("pass".to_string()))).into(),
        }));
        let result = run_extraction(&value);
        assert_eq!(result, vec!["email", "pass"]);
    }

    #[test]
    fn test_extract_variables_from_unary_expression() {
        let value = Value::Expression(Box::new(Expression::Unary {
            o: Operator::Sub,
            v: Value::Param(Param(Ident("amount".to_string()))).into(),
        }));
        let result = run_extraction(&value);
        assert_eq!(result, vec!["amount"]);
    }

    // #[test]
    // fn test_extract_variables_from_function() {
    //     let value = Value::Function(Box::new(Function::Normal(
    //         "crypto::argon2::compare".to_string(),
    //         vec![
    //             Value::Idiom(Idiom(vec![Field::Ident(Ident("pass".into()))])),
    //             Value::Param(Param(Ident("pass".into()))),
    //         ],
    //     )));
    //     let result = run_extraction(&value);
    //     assert_eq!(result, vec!["pass"]);
    // }

    #[test]
    fn test_extract_variables_from_array() {
        let value = Value::Array(Array(vec![
            Value::Param(Param(Ident("var1".into()))),
            Value::Strand("static".into()),
            Value::Param(Param(Ident("var2".into()))),
        ]));
        let result = run_extraction(&value);
        assert_eq!(result, vec!["var1", "var2"]);
    }

    // #[test]
    // fn test_extract_variables_from_object() {
    //     let value = Value::Object(Object(
    //         vec![(
    //             "key".into(),
    //             Value::Param(Param(Ident("value_param".into()))),
    //         )]
    //             .into_iter()
    //             .collect(),
    //     ));
    //     let result = run_extraction(&value);
    //     assert_eq!(result, vec!["value_param"]);
    // }
    //
    // #[test]
    // fn test_extract_variables_from_non_param_values() {
    //     let value = Value::Array(Array(vec![
    //         Value::Number(1.into()),
    //         Value::String("hello".into()),
    //         Value::Bool(true),
    //     ]));
    //     let result = run_extraction(&value);
    //     assert!(result.is_empty(), "Expected no variables to be found");
    // }
    //
    // // Tests for subquery branches
    // #[test]
    // fn test_extract_variables_from_select_subquery() {
    //     // Mimics a SIGNIN clause: SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass)
    //     let value = Value::Subquery(Box::new(Subquery::Select(SelectStatement {
    //         cond: Some(Cond(Value::Expression(Box::new(Expression::Binary {
    //             l: Value::Expression(Box::new(Expression::Binary {
    //                 l: Value::Idiom(Idiom(vec![Field::Ident(Ident("email".into()))])).into(),
    //                 o: Operator::Equal,
    //                 r: Value::Param(Param(Ident("email".into()))).into(),
    //             }))).into(),
    //             o: Operator::And,
    //             r: Value::Function(Box::new(Function::Normal(
    //                 "crypto::argon2::compare".into(),
    //                 vec![
    //                     Value::Idiom(Idiom(vec![Field::Ident(Ident("pass".into()))])),
    //                     Value::Param(Param(Ident("pass".into()))),
    //                 ],
    //             ))).into(),
    //         })))).into()),
    //         ..Default::default()
    //     }));
    //
    //     let result = run_extraction(&value);
    //     assert_eq!(result, vec!["email", "pass"]);
    // }
    //
    // #[test]
    // fn test_extract_variables_from_create_subquery() {
    //     // Mimics a SIGNUP clause: CREATE user SET email = $email, pass = crypto::argon2::generate($pass)
    //     let set_expr = SetExpression(vec![
    //         (
    //             Idiom(vec![Field::Ident(Ident("email".into()))]),
    //             Operator::Equal,
    //             Value::Param(Param(Ident("email".into()))),
    //         ),
    //         (
    //             Idiom(vec![Field::Ident(Ident("pass".into()))]),
    //             Operator::Equal,
    //             Value::Function(Box::new(Function::Normal(
    //                 "crypto::argon2::generate".into(),
    //                 vec![Value::Param(Param(Ident("pass".into())))],
    //             ))),
    //         ),
    //     ]);
    //
    //     let value = Value::Subquery(Box::new(Subquery::Create(CreateStatement {
    //         what: Values(vec![Value::Table(Table("user".into()))]),
    //         data: Some(Data::SetExpression(set_expr)),
    //         ..Default::default()
    //     })));
    //
    //     let result = run_extraction(&value);
    //     assert_eq!(result, vec!["email", "pass"]);
    // }
    //
    // // Tests for specific helper functions
    // #[test]
    // fn test_extract_from_cond() {
    //     let mut variables = HashSet::new();
    //     let cond = Cond(Value::Param(Param(Ident("cond_param".to_string()))).into());
    //     extract_variables_from_cond(&cond, &mut variables);
    //     assert!(variables.contains("cond_param"));
    // }
    //
    // #[test]
    // fn test_extract_from_field_single() {
    //     let mut variables = HashSet::new();
    //     let field = Field::Single {
    //         expr: Value::Param(Param(Ident("field_param".to_string()))).into(),
    //         alias: None,
    //     };
    //     extract_variables_from_field(&field, &mut variables);
    //     assert!(variables.contains("field_param"));
    // }
    //
    // #[test]
    // fn test_extract_from_field_all() {
    //     let mut variables = HashSet::new();
    //     let field = Field::All;
    //     extract_variables_from_field(&field, &mut variables);
    //     assert!(variables.is_empty(), "Field::All should not add any variables");
    // }
    //
    // #[test]
    // fn test_extract_from_data_set_expression() {
    //     let mut variables = HashSet::new();
    //     let data = Data::SetExpression(SetExpression(vec![(
    //         Idiom(vec![Field::Ident(Ident("name".into()))]),
    //         Operator::Equal,
    //         Value::Param(Param(Ident("name_param".to_string()))),
    //     )]));
    //     extract_variables_from_data(&data, &mut variables);
    //     assert!(variables.contains("name_param"));
    // }
    //
    // #[test]
    // fn test_extract_from_data_content_expression() {
    //     let mut variables = HashSet::new();
    //     let data = Data::ContentExpression(Value::Param(Param(Ident("content_param".to_string()))).into());
    //     extract_variables_from_data(&data, &mut variables);
    //     assert!(variables.contains("content_param"));
    // }
}