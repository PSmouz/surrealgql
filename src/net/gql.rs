use async_graphql::http::GraphiQLSource;
use axum::response::Html;
use axum::routing::post_service;
use axum::Router;
use std::sync::Arc;

use surrealdb::gql::cache::Pessimistic;
use surrealdb::kvs::Datastore;

use crate::gql::GraphQL;

pub(super) async fn router<S>(ds: Arc<Datastore>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let service = GraphQL::new(Pessimistic, ds);
    //TODO(psmouz): make graphiql service out of this to use get_service. Make optional/configurable
    // lastly, remove hardcoded headers for introspection and get them dynamically, maybe via
    // route params?!

    Router::new().route("/graphql", post_service(service).get(
        || async {
            Html(
                // GraphiQLSource::build().endpoint("/graphql").finish()
                //TODO(psmouz): track issues https://github.com/async-graphql/async-graphql/issues/1731
                // and https://github.com/async-graphql/async-graphql/issues/1712
                // graphiql deprecated umd in favor of esm modules from version 5. This breaks
                // everything!
                GraphiQLSource::build()
                    .endpoint("/graphql")
                    // .header("Authorization", "Bearer [token]")
                    // Important: echo -n 'root:root' | base64
                    // root:root for local test
                    .header("Authorization", "Basic cm9vdDpyb290")
                    .header("surreal-ns", "test")
                    .header("surreal-db", "test")
                    .plugins(&[async_graphql::http::graphiql_plugin_explorer()])
                    .finish()
                    .replace("@17", "@18")
                    .replace(
                        "ReactDOM.render(",
                        "ReactDOM.createRoot(document.getElementById(\"graphiql\")).render(",
                    )
                    // The explorer style can be found!
                    .replace("https://unpkg.com/@graphiql/plugin-explorer/dist/style.css", "https://unpkg.com/@graphiql/plugin-explorer@5.0.0-rc.1/dist/style.css")
                    .replace("https://unpkg.com/@graphiql/plugin-explorer/dist/index.umd.js", "https://unpkg.com/@graphiql/plugin-explorer@5.0.0-rc.1/dist/index.umd.js")
                    .replace("https://unpkg.com/graphiql/graphiql.min.js", "https://unpkg.com/graphiql@5.0.0-rc.1/graphiql.min.js")
                    .replace("https://unpkg.com/graphiql/graphiql.min.css", "https://unpkg.com/graphiql@5.0.0-rc.1/graphiql.min.css")
            )
        }
    ))
}