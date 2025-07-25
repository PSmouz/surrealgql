use std::{
    convert::Infallible,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use async_graphql::{
    http::{create_multipart_mixed_stream, is_accept_multipart_mixed},
    Executor, ParseRequestError,
};
use async_graphql_axum::{
    rejection::GraphQLRejection, GraphQLBatchRequest, GraphQLRequest, GraphQLResponse,
};
use axum::response::Html;
use axum::{
    body::{Body, HttpBody},
    extract::FromRequest,
    http::{Request as HttpRequest, Response as HttpResponse},
    response::IntoResponse,
    BoxError,
};
use bytes::Bytes;
use futures_util::{future::BoxFuture, StreamExt};
use surrealdb::dbs::capabilities::RouteTarget;
use surrealdb::dbs::Session;
use surrealdb::gql::cache::{Invalidator, SchemaCache};
use surrealdb::gql::error::resolver_error;
use surrealdb::kvs::Datastore;
use tower_service::Service;

use crate::err::Error as SurrealError;

/// A GraphQL service.
#[derive(Clone)]
pub struct GraphQL<I: Invalidator> {
    cache: SchemaCache<I>,
    // datastore: Arc<Datastore>,
}

impl<I: Invalidator> GraphQL<I> {
    /// Create a GraphQL handler.
    pub fn new(invalidator: I, datastore: Arc<Datastore>) -> Self {
        let _ = invalidator;
        GraphQL {
            cache: SchemaCache::new(datastore),
            // datastore,
        }
    }
}

impl<B, I> Service<HttpRequest<B>> for GraphQL<I>
where
    B: HttpBody<Data=Bytes> + Send + 'static,
    B::Data: Into<Bytes>,
    B::Error: Into<BoxError>,
    I: Invalidator,
{
    type Response = HttpResponse<Body>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: HttpRequest<B>) -> Self::Future {
        let cache = self.cache.clone();
        let req = req.map(Body::new);

        Box::pin(async move {
            // Check if capabilities allow querying the requested HTTP route
            if !cache.datastore.allows_http_route(&RouteTarget::GraphQL) {
                warn!(
					"Capabilities denied HTTP route request attempt, target: '{}'",
					&RouteTarget::GraphQL
				);
                return Ok(
                    SurrealError::ForbiddenRoute(RouteTarget::GraphQL.to_string()).into_response()
                );
            }

            let session =
                req.extensions().get::<Session>().expect("session extractor should always succeed");

            let Some(_ns) = session.ns.as_ref() else {
                return Ok(to_rejection(resolver_error("No namespace specified")).into_response());
            };
            let Some(_db) = session.db.as_ref() else {
                return Ok(to_rejection(resolver_error("No database specified")).into_response());
            };

            #[cfg(debug_assertions)]
            {
                let state = req
                    .extensions()
                    .get::<crate::net::AppState>()
                    .expect("state extractor should always succeed");
                debug_assert!(Arc::ptr_eq(&state.datastore, &cache.datastore));
            }

            let executor = match cache.get_schema(session).await {
                Ok(e) => e,
                Err(e) => {
                    info!(?e, "error generating schema");
                    return Ok(to_rejection(e).into_response());
                }
            };
            let is_accept_multipart_mixed = req
                .headers()
                .get("accept")
                .and_then(|value| value.to_str().ok())
                .map(is_accept_multipart_mixed)
                .unwrap_or_default();

            if is_accept_multipart_mixed {
                let req = match GraphQLRequest::<GraphQLRejection>::from_request(req, &()).await {
                    Ok(req) => req,
                    Err(err) => return Ok(err.into_response()),
                };
                let stream = Executor::execute_stream(&executor, req.0, None);
                let body = Body::from_stream(
                    create_multipart_mixed_stream(stream, Duration::from_secs(30))
                        .map(Ok::<_, std::io::Error>),
                );
                Ok(HttpResponse::builder()
                    .header("content-type", "multipart/mixed; boundary=graphql")
                    .body(body)
                    .expect("BUG: invalid response"))
            } else {
                let req =
                    match GraphQLBatchRequest::<GraphQLRejection>::from_request(req, &()).await {
                        Ok(req) => req,
                        Err(err) => return Ok(err.into_response()),
                    };
                Ok(GraphQLResponse(executor.execute_batch(req.0).await).into_response())
            }
        })
    }
}

fn to_rejection(err: impl std::error::Error + Send + Sync + 'static) -> GraphQLRejection {
    GraphQLRejection(ParseRequestError::InvalidRequest(Box::new(err)))
}

#[derive(Clone)]
pub struct GraphiQL;

impl GraphiQL {
    pub fn new() -> Self {
        GraphiQL
    }
}

impl<B> Service<HttpRequest<B>> for GraphiQL
where
    B: HttpBody<Data=Bytes> + Send + 'static,
    B::Data: Into<Bytes>,
    B::Error: Into<BoxError>,
{
    type Response = HttpResponse<Body>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: HttpRequest<B>) -> Self::Future {
        let req = req.map(Body::new);

        let path = req.uri().path().to_owned();

        Box::pin(async move {
            // This is a simple way to parse. A real-world app might use a more robust router library
            // or pass params via extensions, but for this use case, splitting is clear.
            // let parts: Vec<&str> = path.split('/').collect();
            // let (ns, db) = if parts.len() >= 4 {
            // 	(parts[2].to_string(), parts[3].to_string())
            // } else {
            // 	// Default values if the path is not as expected
            // 	("test".to_string(), "test".to_string())
            // };

            let html = Html(
                r#"
				<!doctype html>
                <html lang="en">
                <head>
                  <meta charset="UTF-8"/>
                  <meta content="width=device-width, initial-scale=1.0" name="viewport"/>
                  <title>SurrealDB GraphiQL</title>
                  <style>
                    body {
                      margin: 0;
                    }
                    #graphiql {
                      height: 100dvh;
                    }

                    .loading {
                      height: 100%;
                      display: flex;
                      align-items: center;
                      justify-content: center;
                      font-size: 4rem;
                    }
                  </style>
                  <link href="https://esm.sh/graphiql/dist/style.css" rel="stylesheet"/>
                  <link href="https://esm.sh/@graphiql/plugin-explorer/dist/style.css"
                        rel="stylesheet"/>
                  <script type="importmap">
                    {
                      "imports": {
                        "react": "https://esm.sh/react@19.1.0",
                        "react/": "https://esm.sh/react@19.1.0/",

                        "react-dom": "https://esm.sh/react-dom@19.1.0",
                        "react-dom/": "https://esm.sh/react-dom@19.1.0/",

                        "graphiql": "https://esm.sh/graphiql?standalone&external=react,react-dom,@graphiql/react,graphql",
                        "graphiql/": "https://esm.sh/graphiql/",
                        "@graphiql/plugin-explorer": "https://esm.sh/@graphiql/plugin-explorer?standalone&external=react,@graphiql/react,graphql",
                        "@graphiql/react": "https://esm.sh/@graphiql/react?standalone&external=react,react-dom,graphql,@graphiql/toolkit,@emotion/is-prop-valid",

                        "@graphiql/toolkit": "https://esm.sh/@graphiql/toolkit?standalone&external=graphql",
                        "graphql": "https://esm.sh/graphql@16.11.0",
                        "@emotion/is-prop-valid": "data:text/javascript,"
                      }
                    }
                  </script>
                  <script type="module">
                    import React from 'react';
                    import ReactDOM from 'react-dom/client';
                    import {GraphiQL, HISTORY_PLUGIN} from 'graphiql';
                    import {createGraphiQLFetcher} from '@graphiql/toolkit';
                    import {explorerPlugin} from '@graphiql/plugin-explorer';
                    import 'graphiql/setup-workers/esm.sh';

                    const createUrl = (endpoint, subscription = false) => {
                      const url = new URL(endpoint, window.location.origin);
                      if (subscription) {
                        url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
                      }
                      return url.toString();
                    };
                    const fetcher = createGraphiQLFetcher({
                      url: createUrl('/graphql'),
                      fetch: (url, opts) => fetch(url, {...opts, credentials: 'same-origin'}),
                    });
                    const plugins = [HISTORY_PLUGIN, explorerPlugin()];

                    function App() {
                      return React.createElement(React.Fragment, null,
                          React.createElement(
                              GraphiQL,
                              {
                                fetcher,
                                plugins,
                                defaultEditorToolsVisibility: true,
                              },
                          ),
                      );
                    }

                    const root = ReactDOM.createRoot(document.getElementById('graphiql'));
                    root.render(React.createElement(App));
                  </script>
                </head>
                <body>
                <div id="graphiql">
                  <div class="loading">Loading…</div>
                </div>
                </body>
                </html>
				"#
            );

            Ok(html.into_response())
        })
    }
}
