use axum::routing::post_service;
use axum::Router;
use std::sync::Arc;

use surrealdb::gql::cache::Pessimistic;
use surrealdb::kvs::Datastore;

use crate::gql::{GraphQL, GraphiQL};

pub(super) async fn router<S>(ds: Arc<Datastore>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let service = GraphQL::new(Pessimistic, ds);
    //TODO(psmouz): remove hardcoded headers for introspection and get them dynamically, maybe via
    // route params?!
    let graphiql = GraphiQL::new();

    Router::new()
        .route("/graphql", post_service(service).get_service(graphiql))
}