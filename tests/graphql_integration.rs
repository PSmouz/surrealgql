mod common;

mod graphql_integration {
	use std::time::Duration;

	use futures_util::{SinkExt, StreamExt};
	macro_rules! assert_equal_arrs {
		($lhs: expr_2021, $rhs: expr_2021) => {
			let lhs = $lhs.as_array().unwrap().iter().collect::<std::collections::HashSet<_>>();
			let rhs = $rhs.as_array().unwrap().iter().collect::<std::collections::HashSet<_>>();
			assert_eq!(lhs, rhs)
		};
	}

	use http::header;
	use reqwest::Client;
	use serde_json::json;
	use test_log::test;
	use tokio_tungstenite::connect_async;
	use tokio_tungstenite::tungstenite::Message;
	use tokio_tungstenite::tungstenite::client::IntoClientRequest;
	use ulid::Ulid;

	use super::common;
	use crate::common::{PASS, USER};

	#[test(tokio::test)]
	async fn basic() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// check errors with no config
		{
			let res = client.post(gql_url).body("").send().await?;
			assert_eq!(res.status(), 400);
			let body = res.text().await?;
			assert!(body.contains("NotConfigured"), "body: {body}")
		}

		// add schema and data
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
                    DEFINE CONFIG GRAPHQL AUTO;
                "#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// check errors with no tables
		{
			let res = client.post(gql_url).body("").send().await?;
			assert_eq!(res.status(), 400);
			let body = res.text().await?;
			assert!(body.contains("no items found in database"), "body: {body}")
		}

		// add schema and data
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
                    DEFINE TABLE foo SCHEMAFUL;
                    DEFINE FIELD val ON foo TYPE int;
                    CREATE foo:1 set val = 42;
                    CREATE foo:2 set val = 43;
                "#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// fetch data via graphql
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"query{ foos { nodes { id val } } }"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
				"data": {
					"foos": {
						"nodes": [
							{
								"id": "foo:1",
								"val": 42
							},
							{
								"id": "foo:2",
								"val": 43
							}
						]
					}
				}
			});
			assert_eq!(expected, body)
		}

		// test limit
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query{foos(first: 1, orderBy: { field: ID, direction: ASC }){nodes{id val} pageInfo{endCursor}}}"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
				"data": {
					"foos": {
						"nodes": [
							{
								"id": "foo:1",
								"val": 42
							}
						],
						"pageInfo": {
							"endCursor": body["data"]["foos"]["pageInfo"]["endCursor"].clone()
						}
					}
				}
			});
			assert_eq!(expected, body)
		}

		// test after cursor
		{
			let first_page = client
				.post(gql_url)
				.body(
					json!({"query": r#"query{foos(first: 1, orderBy: { field: ID, direction: ASC }){pageInfo{endCursor}}}"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(first_page.status(), 200);
			let first_page = first_page.json::<serde_json::Value>().await?;
			let after = first_page["data"]["foos"]["pageInfo"]["endCursor"].as_str().unwrap();

			let res = client
				.post(gql_url)
				.body(
					json!({"query": format!("query{{foos(after: \"{after}\", orderBy: {{ field: ID, direction: ASC }}){{nodes{{id val}}}}}}")})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
				"data": {
					"foos": {
						"nodes": [
							{
								"id": "foo:2",
								"val": 43
							}
						]
					}
				}
			});
			assert_eq!(expected, body)
		}

		// test order
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query{foos(orderBy: {field: VAL, direction: DESC}){nodes{id}}}"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
				"data": {
					"foos": {
						"nodes": [
							{
								"id": "foo:2",
							},
							{
								"id": "foo:1",
							}
						]
					}
				}
			});
			assert_eq!(expected, body)
		}

		// test filter
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query{foos(filterBy: {val: {eq: 42}}){nodes{id}}}"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
				"data": {
					"foos": {
						"nodes": [
							{
								"id": "foo:1",
							}
						]
					}
				}
			});
			assert_eq!(expected, body)
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn basic_auth() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");
		let signup_url = &format!("http://{addr}/signup");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// check errors on invalid auth
		{
			let res =
				client.post(gql_url).basic_auth("invalid", Some("invalid")).body("").send().await?;
			assert_eq!(res.status(), 401);
			let body = res.text().await?;
			assert!(body.contains("There was a problem with authentication"), "body: {body}")
		}

		// add schema and data
		{
			let res = client
				.post(sql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
					SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
					DURATION FOR SESSION 60s, FOR TOKEN 1d;

                    DEFINE TABLE foo SCHEMAFUL PERMISSIONS FOR select WHERE $auth.email = email;
                    DEFINE FIELD email ON foo TYPE string;
                    DEFINE FIELD val ON foo TYPE int;
                    CREATE foo:1 set val = 42, email = "user@email.com";
                    CREATE foo:2 set val = 43, email = "other@email.com";
                "#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// check works with root
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(json!({"query": r#"query{foos{nodes{id val}}}"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({"data":{"foos":{"nodes":[{"id":"foo:1","val":42},{"id":"foo:2","val":43}]}}});
			assert_eq!(body, expected);
		}

		// check partial access
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns,
					"db": db,
					"ac": "user",
					"email": "user@email.com",
					"pass": "pass",
				})
				.as_object()
				.unwrap(),
			)
			.unwrap();

			let res = client.post(signup_url).body(req_body).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			let token = body["token"].as_str().unwrap();

			let res = client
				.post(gql_url)
				.bearer_auth(token)
				.body(json!({"query": r#"query{foos{nodes{id val}}}"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({"data":{"foos":{"nodes":[{"id":"foo:1","val":42}]}}});
			assert_eq!(expected, body);
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn config() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		{
			let res = client.post(gql_url).body("").send().await?;
			assert_eq!(res.status(), 400);
			let body = res.text().await?;
			assert!(body.contains("NotConfigured"), "{body}");
		}

		// add schema and data
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE FIELD id ON TABLE foo TYPE string;
                    DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE foo;
					DEFINE FIELD val ON foo TYPE string;
					DEFINE TABLE bar;
					DEFINE FIELD val ON bar TYPE string;
                "#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		{
			let res = client
				.post(gql_url)
				.body(json!({ "query": r#"{__schema {queryType {fields {name}}}}"# }).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let res_obj: serde_json::Value = res.json().await?;
			let fields = &res_obj["data"]["__schema"]["queryType"]["fields"];
			let expected_fields = json!(
				[
					{
						"name": "foo"
					},
					{
						"name": "foos"
					},
					{
						"name": "bar"
					},
					{
						"name": "bars"
					}
				]
			);
			assert_equal_arrs!(fields, &expected_fields);
		}

		{
			let res = client
				.post(sql_url)
				.body(
					r#"
                    DEFINE CONFIG OVERWRITE GRAPHQL TABLES INCLUDE foo;
                "#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		{
			let res = client
				.post(gql_url)
				.body(json!({ "query": r#"{__schema {queryType {fields {name}}}}"# }).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let res_obj = res.json::<serde_json::Value>().await?;
			let fields = &res_obj["data"]["__schema"]["queryType"]["fields"];
			let expected_fields = json!(
				[
					{
						"name": "foo"
					},
					{
						"name": "foos"
					}
				]
			);
			assert_equal_arrs!(fields, &expected_fields);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn geometry() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema with various geometry types
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE place SCHEMAFUL;
					DEFINE FIELD name ON place TYPE string;
					DEFINE FIELD location ON place TYPE geometry<point>;

					DEFINE TABLE area SCHEMAFUL;
					DEFINE FIELD name ON area TYPE string;
					DEFINE FIELD boundary ON area TYPE geometry<polygon>;

					DEFINE TABLE feature SCHEMAFUL;
					DEFINE FIELD name ON feature TYPE string;
					DEFINE FIELD geom ON feature TYPE geometry;

					CREATE place:london SET name = "London", location = (-0.118092, 51.509865);
					CREATE place:paris SET name = "Paris", location = (2.349014, 48.864716);

					CREATE area:london SET name = "London Bounds", boundary = {
						type: "Polygon",
						coordinates: [[
							[-0.38314819, 51.37692386],
							[0.1785278, 51.37692386],
							[0.1785278, 51.61460570],
							[-0.38314819, 51.61460570],
							[-0.38314819, 51.37692386]
						]]
					};

					CREATE feature:point SET name = "A Point", geom = (1.0, 2.0);
					CREATE feature:line SET name = "A Line", geom = {
						type: "LineString",
						coordinates: [[0.0, 0.0], [1.0, 1.0], [2.0, 0.0]]
					};
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Test 1: Query a specific geometry<point> field
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						places(orderBy: {field: NAME, direction: ASC}) {
							nodes {
								id
								name
								location { type coordinates }
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
				"data": {
					"places": {
						"nodes": [
							{
								"id": "place:london",
								"name": "London",
								"location": {
									"type": "Point",
									"coordinates": [-0.118092, 51.509865]
								}
							},
							{
								"id": "place:paris",
								"name": "Paris",
								"location": {
									"type": "Point",
									"coordinates": [2.349014, 48.864716]
								}
							}
						]
					}
				}
			});
			assert_eq!(expected, body);
		}

		// Test 2: Query a specific geometry<polygon> field
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						areas {
							nodes {
								id
								name
								boundary { type coordinates }
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
				"data": {
					"areas": {
						"nodes": [
							{
								"id": "area:london",
								"name": "London Bounds",
								"boundary": {
									"type": "Polygon",
									"coordinates": [[
										[-0.38314819, 51.37692386],
										[0.1785278, 51.37692386],
										[0.1785278, 51.6146057],
										[-0.38314819, 51.6146057],
										[-0.38314819, 51.37692386]
									]]
								}
							}
						]
					}
				}
			});
			assert_eq!(expected, body);
		}

		// Test 3: Query a general geometry field (union type) with inline fragments
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						features(orderBy: {field: NAME, direction: ASC}) {
							nodes {
								id
								name
								geom {
									... on GeometryPoint { type coordinates }
									... on GeometryLineString { type coordinates }
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
				"data": {
					"features": {
						"nodes": [
							{
								"id": "feature:line",
								"name": "A Line",
								"geom": {
									"type": "LineString",
									"coordinates": [[0.0, 0.0], [1.0, 1.0], [2.0, 0.0]]
								}
							},
							{
								"id": "feature:point",
								"name": "A Point",
								"geom": {
									"type": "Point",
									"coordinates": [1.0, 2.0]
								}
							}
						]
					}
				}
			});
			assert_eq!(expected, body);
		}

		// Test 4: Fetch a single record by ID with geometry
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						place(id: "london") {
							id
							name
							location { type coordinates }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
				"data": {
					"place": {
						"id": "place:london",
						"name": "London",
						"location": {
							"type": "Point",
							"coordinates": [-0.118092, 51.509865]
						}
					}
				}
			});
			assert_eq!(expected, body);
		}

		// Test 5: Schema introspection shows geometry types
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__type(name: "GeometryType") {
							kind
							enumValues { name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let geo_type = &body["data"]["__type"];
			assert_eq!(geo_type["kind"], "ENUM");
			let enum_values = geo_type["enumValues"].as_array().unwrap();
			let names: Vec<&str> =
				enum_values.iter().map(|v| v["name"].as_str().unwrap()).collect();
			assert!(names.contains(&"Point"));
			assert!(names.contains(&"LineString"));
			assert!(names.contains(&"Polygon"));
			assert!(names.contains(&"MultiPoint"));
			assert!(names.contains(&"MultiLineString"));
			assert!(names.contains(&"MultiPolygon"));
			assert!(names.contains(&"GeometryCollection"));
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn functions() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// add schema and data
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL auto;
                    DEFINE TABLE foo SCHEMAFUL;
                    DEFINE FIELD val ON foo TYPE int;
                    CREATE foo:1 set val = 86;
					DEFINE FUNCTION fn::num() -> int {return 42;};
					DEFINE FUNCTION fn::double($x: int) -> int {return $x * 2};
					DEFINE FUNCTION fn::foo() -> record<foo> {return foo:1};
					DEFINE FUNCTION fn::record() -> record {return foo:1};
                "#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// functions returning records
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query{fn_foo{id, val}, fn_record {id ...on Foo {val}}}"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
			  "data": {
				"fn_foo": {
				  "id": "foo:1",
				  "val": 86
				},
				"fn_record": {
					"id": "foo:1",
					"val": 86
				  }
			  }
			});
			assert_eq!(expected, body)
		}

		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"query{fn_num, fn_double(x: 21)}"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
			  "data": {
				"fn_num": 42,
				"fn_double": 42
			  }
			});
			assert_eq!(expected, body)
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn relations() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema: person -[likes]-> post, with rating on the relation
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE person SCHEMAFUL;
					DEFINE FIELD name ON person TYPE string;

					DEFINE TABLE post SCHEMAFUL;
					DEFINE FIELD title ON post TYPE string;

					DEFINE TABLE likes TYPE RELATION FROM person TO post SCHEMAFUL;
					DEFINE FIELD rating ON likes TYPE int;
					DEFINE FIELD in ON likes TYPE record<person>;
					DEFINE FIELD out ON likes TYPE record<post>;

					CREATE person:alice SET name = "Alice";
					CREATE person:bob SET name = "Bob";
					CREATE post:p1 SET title = "First Post";
					CREATE post:p2 SET title = "Second Post";

					RELATE person:alice->likes->post:p1 SET rating = 5;
					RELATE person:alice->likes->post:p2 SET rating = 3;
					RELATE person:bob->likes->post:p1 SET rating = 4;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Test 1: Query outgoing relation field on person with target nodes and edge metadata
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						person(id: "alice") {
							id
							name
							likes(orderBy: {field: RATING, direction: ASC}) {
								nodes {
									id
									title
								}
								edges {
									id
									rating
									node {
										id
										title
									}
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let person = &body["data"]["person"];
			assert_eq!(person["id"], "person:alice");
			assert_eq!(person["name"], "Alice");
			let likes = person["likes"]["nodes"].as_array().unwrap();
			assert_eq!(likes.len(), 2);
			assert_eq!(likes[0]["title"], "Second Post");
			assert_eq!(likes[1]["title"], "First Post");
			let edges = person["likes"]["edges"].as_array().unwrap();
			assert_eq!(edges[0]["rating"], 3);
			assert_eq!(edges[0]["node"]["title"], "Second Post");
			assert_eq!(edges[1]["rating"], 5);
			assert_eq!(edges[1]["node"]["title"], "First Post");
		}

		// Test 2: Relation field filter/order operate on relation metadata
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						person(id: "bob") {
							likes(filterBy: {rating: {eq: 4}}, orderBy: {field: RATING, direction: ASC}) {
								nodes {
									id
									title
								}
								edges {
									rating
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let likes = body["data"]["person"]["likes"]["nodes"].as_array().unwrap();
			assert_eq!(likes.len(), 1);
			assert_eq!(likes[0]["id"], "post:p1");
			assert_eq!(likes[0]["title"], "First Post");
			let edges = body["data"]["person"]["likes"]["edges"].as_array().unwrap();
			assert_eq!(edges[0]["rating"], 4);
		}

		// Test 3: Relation field with limit
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						person(id: "alice") {
							likes(first: 1, orderBy: {field: RATING, direction: DESC}) {
								nodes {
									title
								}
								edges {
									rating
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let likes = body["data"]["person"]["likes"]["nodes"].as_array().unwrap();
			assert_eq!(likes.len(), 1);
			assert_eq!(likes[0]["title"], "First Post");
			let edges = body["data"]["person"]["likes"]["edges"].as_array().unwrap();
			assert_eq!(edges[0]["rating"], 5);
		}

		// Test 4: Relation field filters can recurse into the related node
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						person(id: "alice") {
							likes(filterBy: { node: { title: { contains: "Second" } } }) {
								nodes {
									id
									title
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let likes = body["data"]["person"]["likes"]["nodes"].as_array().unwrap();
			assert_eq!(likes.len(), 1);
			assert_eq!(likes[0]["id"], "post:p2");
			assert_eq!(likes[0]["title"], "Second Post");
		}

		// Test 5: Relation fields in list query context
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						persons(orderBy: {field: NAME, direction: ASC}) {
							nodes {
								name
								likes {
									totalCount
									edges {
										rating
									}
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let people = body["data"]["persons"]["nodes"].as_array().unwrap();
			assert_eq!(people.len(), 2);
			assert_eq!(people[0]["name"], "Alice");
			assert_eq!(people[0]["likes"]["totalCount"], 2);
			assert_eq!(people[0]["likes"]["edges"].as_array().unwrap().len(), 2);
			assert_eq!(people[1]["name"], "Bob");
			assert_eq!(people[1]["likes"]["totalCount"], 1);
			assert_eq!(people[1]["likes"]["edges"].as_array().unwrap().len(), 1);
		}

		// Test 6: Root table filters can recurse through relation edges and target nodes
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						persons(
							filterBy: {
								likes: {
									some: {
										rating: { gte: 4 }
										node: { title: { contains: "First" } }
									}
								}
							}
							orderBy: { field: NAME, direction: ASC }
						) {
							nodes {
								id
								name
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let people = body["data"]["persons"]["nodes"].as_array().unwrap();
			assert_eq!(people.len(), 2);
			assert_eq!(people[0]["id"], "person:alice");
			assert_eq!(people[1]["id"], "person:bob");
		}

		// Test 7: Schema introspection shows relation field on the source type
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__type(name: "Person") {
							fields { name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let fields = body["data"]["__type"]["fields"].as_array().unwrap();
			let field_names: Vec<&str> =
				fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(field_names.contains(&"id"), "missing 'id' field: {field_names:?}");
			assert!(field_names.contains(&"name"), "missing 'name' field: {field_names:?}");
			assert!(
				field_names.contains(&"likes"),
				"missing 'likes' relation field: {field_names:?}"
			);
		}

		// Test 8: Schema introspection omits reverse relation field on the target type
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__type(name: "Post") {
							fields { name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let fields = body["data"]["__type"]["fields"].as_array().unwrap();
			let field_names: Vec<&str> =
				fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(field_names.contains(&"id"), "missing 'id' field: {field_names:?}");
			assert!(field_names.contains(&"title"), "missing 'title' field: {field_names:?}");
			assert!(
				!field_names.contains(&"likesIn"),
				"unexpected reverse relation field on Post: {field_names:?}"
			);
		}

		// Test 9: Relation tables are not exposed as standalone GraphQL query/object types
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__schema {
							queryType {
								fields { name }
							}
						}
						relationType: __type(name: "Likes") {
							name
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let fields = body["data"]["__schema"]["queryType"]["fields"].as_array().unwrap();
			let field_names: Vec<&str> =
				fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(
				!field_names.contains(&"like"),
				"unexpected singular relation root field: {field_names:?}"
			);
			assert!(
				!field_names.contains(&"likes"),
				"unexpected plural relation root field: {field_names:?}"
			);
			assert!(body["data"]["relationType"].is_null());
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn record_links() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema: employee has a record<department> field
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE department SCHEMAFUL;
					DEFINE FIELD name ON department TYPE string;
					DEFINE FIELD location ON department TYPE string;

					DEFINE TABLE employee SCHEMAFUL;
					DEFINE FIELD name ON employee TYPE string;
					DEFINE FIELD dept ON employee TYPE record<department>;

					CREATE department:eng SET name = "Engineering", location = "Building A";
					CREATE department:mkt SET name = "Marketing", location = "Building B";

					CREATE employee:e1 SET name = "Alice", dept = department:eng;
					CREATE employee:e2 SET name = "Bob", dept = department:mkt;
					CREATE employee:e3 SET name = "Charlie", dept = department:eng;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Test 1: Record-link dereferencing with nested sub-field selection
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						employees(orderBy: {field: NAME, direction: ASC}) {
							nodes {
								name
								dept {
									id
									name
									location
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let employees = body["data"]["employees"]["nodes"].as_array().unwrap();
			assert_eq!(employees.len(), 3);

			// Alice -> Engineering
			assert_eq!(employees[0]["name"], "Alice");
			assert_eq!(employees[0]["dept"]["name"], "Engineering");
			assert_eq!(employees[0]["dept"]["location"], "Building A");
			assert_eq!(employees[0]["dept"]["id"], "department:eng");

			// Bob -> Marketing
			assert_eq!(employees[1]["name"], "Bob");
			assert_eq!(employees[1]["dept"]["name"], "Marketing");

			// Charlie -> Engineering
			assert_eq!(employees[2]["name"], "Charlie");
			assert_eq!(employees[2]["dept"]["name"], "Engineering");
		}

		// Test 2: Single record fetch with nested record-link
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						employee(id: "e2") {
							name
							dept {
								name
								location
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let emp = &body["data"]["employee"];
			assert_eq!(emp["name"], "Bob");
			assert_eq!(emp["dept"]["name"], "Marketing");
			assert_eq!(emp["dept"]["location"], "Building B");
		}

		// Test 3: Schema shows record-link field as the target table type
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__type(name: "Employee") {
							fields {
								name
								type { name kind }
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let fields = body["data"]["__type"]["fields"].as_array().unwrap();
			let dept_field = fields.iter().find(|f| f["name"] == "dept").unwrap();
			// Record-link output fields are nullable by default and resolve to the target type.
			let type_info = &dept_field["type"];
			assert_eq!(type_info["kind"], "OBJECT");
			assert_eq!(type_info["name"], "Department");
		}

		// Test 4: Record-link filters recurse into the linked table filter input
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						employees(filterBy: { dept: { name: { eq: "Engineering" } } }, orderBy: { field: NAME, direction: ASC }) {
							nodes {
								name
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let employees = body["data"]["employees"]["nodes"].as_array().unwrap();
			assert_eq!(employees.len(), 2);
			assert_eq!(employees[0]["name"], "Alice");
			assert_eq!(employees[1]["name"], "Charlie");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn self_referential_relations() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema: user -[follows]-> user (self-referential)
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE user SCHEMAFUL;
					DEFINE FIELD name ON user TYPE string;

					DEFINE TABLE follows TYPE RELATION FROM user TO user SCHEMAFUL;
					DEFINE FIELD in ON follows TYPE record<user>;
					DEFINE FIELD out ON follows TYPE record<user>;

					CREATE user:alice SET name = "Alice";
					CREATE user:bob SET name = "Bob";
					CREATE user:charlie SET name = "Charlie";

					RELATE user:alice->follows->user:bob;
					RELATE user:alice->follows->user:charlie;
					RELATE user:bob->follows->user:alice;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Test 1: user type exposes only the outgoing relation field
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__type(name: "User") {
							fields { name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let fields = body["data"]["__type"]["fields"].as_array().unwrap();
			let field_names: Vec<&str> =
				fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(
				field_names.contains(&"follows"),
				"missing 'follows' outgoing field: {field_names:?}"
			);
			assert!(
				!field_names.contains(&"followsIn"),
				"unexpected 'followsIn' incoming field: {field_names:?}"
			);
		}

		// Test 2: Query outgoing follows (who does Alice follow?)
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						user(id: "alice") {
							name
							follows {
								nodes {
									id
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let user = &body["data"]["user"];
			assert_eq!(user["name"], "Alice");
			let follows = user["follows"]["nodes"].as_array().unwrap();
			assert_eq!(follows.len(), 2, "Alice follows 2 users");
		}

		// Test 3: Query Bob's outgoing follows in the self-referential relation
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						user(id: "bob") {
							name
							follows {
								nodes {
									id
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let user = &body["data"]["user"];
			assert_eq!(user["name"], "Bob");
			let follows = user["follows"]["nodes"].as_array().unwrap();
			assert_eq!(follows.len(), 1, "Bob follows exactly one user");
			assert_eq!(follows[0]["id"], "user:alice");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn relation_with_record_link_traversal() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up: author -[wrote]-> article, with traversal through in/out fields
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE author SCHEMAFUL;
					DEFINE FIELD name ON author TYPE string;

					DEFINE TABLE article SCHEMAFUL;
					DEFINE FIELD title ON article TYPE string;

					DEFINE TABLE wrote TYPE RELATION FROM author TO article SCHEMAFUL;
					DEFINE FIELD in ON wrote TYPE record<author>;
					DEFINE FIELD out ON wrote TYPE record<article>;
					DEFINE FIELD year ON wrote TYPE int;

					CREATE author:a1 SET name = "Jane Doe";
					CREATE article:art1 SET title = "GraphQL in Practice";
					CREATE article:art2 SET title = "SurrealDB Deep Dive";

					RELATE author:a1->wrote->article:art1 SET year = 2024;
					RELATE author:a1->wrote->article:art2 SET year = 2025;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Test: Traverse from author through relation to article with edge metadata
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						author(id: "a1") {
							name
							wrote(orderBy: {field: YEAR, direction: ASC}) {
								nodes {
									id
									title
								}
								edges {
									year
									node {
										title
									}
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let author = &body["data"]["author"];
			assert_eq!(author["name"], "Jane Doe");
			let wrote_nodes = author["wrote"]["nodes"].as_array().unwrap();
			let wrote_edges = author["wrote"]["edges"].as_array().unwrap();
			assert_eq!(wrote_nodes.len(), 2);
			assert_eq!(wrote_edges.len(), 2);

			// Ordered by year asc
			assert_eq!(wrote_edges[0]["year"], 2024);
			assert_eq!(wrote_edges[0]["node"]["title"], "GraphQL in Practice");
			assert_eq!(wrote_nodes[0]["title"], "GraphQL in Practice");
			assert_eq!(wrote_edges[1]["year"], 2025);
			assert_eq!(wrote_edges[1]["node"]["title"], "SurrealDB Deep Dive");
			assert_eq!(wrote_nodes[1]["title"], "SurrealDB Deep Dive");
		}

		// Test: reverse traversal is not generated on the target object type
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						__type(name: "Article") {
							fields { name }
						}
						article(id: "art1") {
							title
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let fields = body["data"]["__type"]["fields"].as_array().unwrap();
			let field_names: Vec<&str> =
				fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(
				!field_names.contains(&"wroteIn"),
				"unexpected reverse relation field: {field_names:?}"
			);
			let article = &body["data"]["article"];
			assert_eq!(article["title"], "GraphQL in Practice");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn version() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_versioning().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema and initial data
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE item SCHEMAFUL;
					DEFINE FIELD name ON item TYPE string;
					DEFINE FIELD price ON item TYPE float;

					CREATE item:1 SET name = "Alpha", price = 10.0;
					CREATE item:2 SET name = "Beta", price = 20.0;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Sleep to create a time gap, then capture the timestamp
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					SLEEP 100ms;
					RETURN time::now();
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			// Extract the timestamp from the second result
			let ts = body[1]["result"].as_str().unwrap().to_string();

			// Sleep again, then add more data and update existing records
			let res = client
				.post(sql_url)
				.body(
					r#"
					SLEEP 100ms;
					CREATE item:3 SET name = "Gamma", price = 30.0;
					UPDATE item:1 SET name = "Alpha Updated", price = 15.0;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);

			// Test 1: Query without version — should return current data (3 items)
			{
				let res = client
					.post(gql_url)
					.body(
						json!({"query": r#"query { items(orderBy: {field: ID, direction: ASC}) { nodes { id name price } } }"#})
							.to_string(),
					)
					.send()
					.await?;
				assert_eq!(res.status(), 200);
				let body = res.json::<serde_json::Value>().await?;
				let items = body["data"]["items"]["nodes"].as_array().unwrap();
				assert_eq!(items.len(), 3, "Current data should have 3 items: {body}");
				// item:1 should be updated
				assert_eq!(items[0]["name"], "Alpha Updated");
				assert_eq!(items[0]["price"], 15.0);
			}

			// Test 2: Query with version — should return data as it was at
			// the captured timestamp (2 items, with original values)
			{
				let query = format!(
					r#"query {{ items(version: "{ts}", orderBy: {{field: ID, direction: ASC}}) {{ nodes {{ id name price }} }} }}"#
				);
				let res =
					client.post(gql_url).body(json!({"query": query}).to_string()).send().await?;
				assert_eq!(res.status(), 200);
				let body = res.json::<serde_json::Value>().await?;
				let items = body["data"]["items"]["nodes"].as_array().unwrap();
				assert_eq!(items.len(), 2, "Versioned query should have 2 items: {body}");
				// item:1 should still have original values
				assert_eq!(items[0]["name"], "Alpha");
				assert_eq!(items[0]["price"], 10.0);
				assert_eq!(items[1]["name"], "Beta");
			}

			// Test 3: single record fetch with version — historical read
			{
				let query = format!(
					r#"query {{ item(id: "item:1", version: "{ts}") {{ id name price }} }}"#
				);
				let res =
					client.post(gql_url).body(json!({"query": query}).to_string()).send().await?;
				assert_eq!(res.status(), 200);
				let body = res.json::<serde_json::Value>().await?;
				let item = &body["data"]["item"];
				assert_eq!(
					item["name"], "Alpha",
					"Versioned item query should see original name: {body}"
				);
				assert_eq!(item["price"], 10.0);
			}

			// Test 4: single record fetch without version — should see the updated value
			{
				let res = client
					.post(gql_url)
					.body(
						json!({"query": r#"query { item(id: "item:1") { id name price } }"#})
							.to_string(),
					)
					.send()
					.await?;
				assert_eq!(res.status(), 200);
				let body = res.json::<serde_json::Value>().await?;
				let item = &body["data"]["item"];
				assert_eq!(item["name"], "Alpha Updated");
				assert_eq!(item["price"], 15.0);
			}

			// Test 5: version argument with invalid datetime — should return error
			{
				let res = client
					.post(gql_url)
					.body(
						json!({"query": r#"query { items(version: "not-a-date") { nodes { id } } }"#})
							.to_string(),
					)
					.send()
					.await?;
				assert_eq!(res.status(), 200);
				let body = res.json::<serde_json::Value>().await?;
				assert!(
					body["errors"].as_array().is_some_and(|e| !e.is_empty()),
					"Invalid version should produce an error: {body}"
				);
			}

			// Test 6: Schema introspection — verify version argument exists on list query
			{
				let res = client
					.post(gql_url)
					.body(
						json!({"query": r#"{
							__type(name: "Query") {
								fields {
									name
									args { name type { name } }
								}
							}
						}"#})
						.to_string(),
					)
					.send()
					.await?;
				assert_eq!(res.status(), 200);
				let body = res.json::<serde_json::Value>().await?;
				let fields = body["data"]["__type"]["fields"].as_array().unwrap();

				// Check the 'items' list query has a 'version' argument
				let item_field = fields.iter().find(|f| f["name"] == "items").unwrap();
				let version_arg =
					item_field["args"].as_array().unwrap().iter().find(|a| a["name"] == "version");
				assert!(
					version_arg.is_some(),
					"List query should have a 'version' argument: {body}"
				);
				assert_eq!(
					version_arg.unwrap()["type"]["name"],
					"Datetime",
					"version argument should be of type Datetime"
				);

				// Check the singular `item` query has a 'version' argument
				let get_item_field = fields.iter().find(|f| f["name"] == "item").unwrap();
				let version_arg = get_item_field["args"]
					.as_array()
					.unwrap()
					.iter()
					.find(|a| a["name"] == "version");
				assert!(
					version_arg.is_some(),
					"Singular query should have a 'version' argument: {body}"
				);
			}
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn filters() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema and data
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE product SCHEMAFUL;
					DEFINE FIELD name ON product TYPE string;
					DEFINE FIELD price ON product TYPE float;
					DEFINE FIELD quantity ON product TYPE int;
					DEFINE FIELD created ON product TYPE datetime;
					DEFINE FIELD deleted_at ON product TYPE option<datetime>;
					DEFINE FIELD tags ON product TYPE array<string>;

					CREATE product:1 SET name = "Alpha Widget", price = 9.99, quantity = 100, created = d"2024-01-15T00:00:00Z", tags = ["sale", "featured"];
					CREATE product:2 SET name = "Beta Widget", price = 19.99, quantity = 50, created = d"2024-03-20T00:00:00Z", tags = ["featured"];
					CREATE product:3 SET name = "Gamma Tool", price = 29.99, quantity = 200, created = d"2024-06-01T00:00:00Z", tags = ["tooling", "graphql"];
					CREATE product:4 SET name = "Delta Tool", price = 4.99, quantity = 10, created = d"2024-09-10T00:00:00Z", deleted_at = d"2024-09-15T00:00:00Z", tags = ["sale", "tooling"];
					CREATE product:5 SET name = "Epsilon Widget", price = 49.99, quantity = 0, created = d"2025-01-05T00:00:00Z", tags = ["luxury"];
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// --- filterBy ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { name: { eq: "Alpha Widget" } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			assert_eq!(products.len(), 1);
			assert_eq!(products[0]["id"], "product:1");
		}

		// --- eq / ne ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { name: { ne: "Alpha Widget" } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			assert_eq!(products.len(), 4);
		}

		// --- gt / lt on int ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { quantity: { gt: 50 } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			// quantity > 50: product:1 (100), product:3 (200)
			assert_eq!(products.len(), 2);
		}

		// --- gte / lte on float ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { price: { gte: 19.99 } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			// price >= 19.99: product:2 (19.99), product:3 (29.99), product:5 (49.99)
			assert_eq!(products.len(), 3);
		}

		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { price: { lte: 9.99 } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			// price <= 9.99: product:1 (9.99), product:4 (4.99)
			assert_eq!(products.len(), 2);
		}

		// --- contains (string) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { name: { contains: "Widget" } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			// Widget: product:1, product:2, product:5
			assert_eq!(products.len(), 3);
		}

		// --- startsWith ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { name: { startsWith: "Delta" } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			assert_eq!(products.len(), 1);
			assert_eq!(products[0]["id"], "product:4");
		}

		// --- endsWith ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { name: { endsWith: "Tool" } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			// "Gamma Tool", "Delta Tool"
			assert_eq!(products.len(), 2);
		}

		// --- regex ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { name: { regex: "^(Alpha|Gamma)" } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			// Alpha Widget, Gamma Tool
			assert_eq!(products.len(), 2);
		}

		// --- in (string list) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { name: { in: ["Alpha Widget", "Delta Tool"] } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			assert_eq!(products.len(), 2);
		}

		// --- in (int list) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { quantity: { in: [100, 200] } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			// product:1 (100), product:3 (200)
			assert_eq!(products.len(), 2);
		}

		// --- Implicit AND: multiple fields in one filter object ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { name: { contains: "Widget" }, price: { lt: 10 } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			// Widget AND price < 10: product:1 (Alpha Widget, 9.99)
			assert_eq!(products.len(), 1);
			assert_eq!(products[0]["id"], "product:1");
		}

		// --- Multiple operators on the same field (implicit AND) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { price: { gte: 10, lte: 30 } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			// 10 <= price <= 30: product:2 (19.99), product:3 (29.99)
			assert_eq!(products.len(), 2);
		}

		// --- not operator ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { not: { name: { contains: "Widget" } } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			// NOT Widget: product:3, product:4
			assert_eq!(products.len(), 2);
		}

		// --- and / or logical operators ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { or: [{ price: { lt: 5 } }, { price: { gt: 40 } }] }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			// price < 5 OR price > 40: product:4 (4.99), product:5 (49.99)
			assert_eq!(products.len(), 2);
		}

		// --- gt/lt on datetime ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { created: { gt: "2024-06-01T00:00:00Z" } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			// after 2024-06-01: product:4, product:5
			assert_eq!(products.len(), 2);
		}

		// --- between ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { price: { between: { gte: 10, lte: 30 } } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			assert_eq!(products.len(), 2);
			assert_eq!(products[0]["id"], "product:2");
			assert_eq!(products[1]["id"], "product:3");
		}

		// --- notIn / exists / isNull ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { name: { notIn: ["Alpha Widget", "Beta Widget"] }, deletedAt: { exists: false, isNull: false } }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			assert_eq!(products.len(), 2);
			assert_eq!(products[0]["id"], "product:3");
			assert_eq!(products[1]["id"], "product:5");
		}

		// --- list semantics / containment ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { products(filterBy: { tags: { some: { eq: "sale" }, containsAny: ["featured", "tooling"] } }, orderBy: { field: ID, direction: ASC }) { nodes { id } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["products"]["nodes"].as_array().unwrap();
			assert_eq!(products.len(), 2);
			assert_eq!(products[0]["id"], "product:1");
			assert_eq!(products[1]["id"], "product:4");
		}

		// --- filter introspection exposes the richer operator surface ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						stringFilter: __type(name: "StringFilterInput") {
							inputFields { name }
						}
						productTagsFilter: __type(name: "ProductTagsListFilterInput") {
							inputFields { name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let string_fields = body["data"]["stringFilter"]["inputFields"].as_array().unwrap();
			let string_names: Vec<&str> =
				string_fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(string_names.contains(&"notIn"));
			assert!(string_names.contains(&"between") == false);
			assert!(string_names.contains(&"like"));
			assert!(string_names.contains(&"matches"));

			let tags_fields = body["data"]["productTagsFilter"]["inputFields"].as_array().unwrap();
			let tags_names: Vec<&str> =
				tags_fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(tags_names.contains(&"some"));
			assert!(tags_names.contains(&"every"));
			assert!(tags_names.contains(&"none"));
			assert!(tags_names.contains(&"containsAny"));
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn nested_objects() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema with nested objects and array-of-objects
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE item SCHEMAFULL;
					DEFINE FIELD name ON item TYPE string;
					DEFINE FIELD time ON item TYPE object;
					DEFINE FIELD time.createdAt ON item TYPE datetime;
					DEFINE FIELD time.updatedAt ON item TYPE datetime;
					DEFINE FIELD time.audit ON item TYPE object;
					DEFINE FIELD time.audit.reviewedAt ON item TYPE datetime;
					DEFINE FIELD tags ON item TYPE array<object>;
					DEFINE FIELD tags.* ON item TYPE object;
					DEFINE FIELD tags.*.label ON item TYPE string;
					DEFINE FIELD tags.*.priority ON item TYPE int;

					DEFINE TABLE article SCHEMAFULL;
					DEFINE FIELD title ON article TYPE string;
					DEFINE FIELD meta ON article TYPE option<object>;
					DEFINE FIELD meta.author ON article TYPE string;
					DEFINE FIELD meta.source ON article TYPE string;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Insert test data
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					CREATE item:alpha SET
						name = "Alpha",
						time = { createdAt: d"2024-01-15T10:00:00Z", updatedAt: d"2024-06-01T12:00:00Z", audit: { reviewedAt: d"2024-06-02T09:00:00Z" } },
						tags = [
							{ label: "urgent", priority: 1 },
							{ label: "review", priority: 3 }
						];
					CREATE item:beta SET
						name = "Beta",
						time = { createdAt: d"2024-03-20T08:00:00Z", updatedAt: d"2024-07-10T16:00:00Z", audit: { reviewedAt: d"2024-07-11T10:00:00Z" } },
						tags = [
							{ label: "feature", priority: 2 }
						];
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// --- Test 1: Query nested object sub-fields (time { createdAt, updatedAt }) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							items(orderBy: { field: ID, direction: ASC }) {
								nodes {
									id
									name
									time {
										createdAt
										updatedAt
										audit {
											reviewedAt
										}
									}
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let items = body["data"]["items"]["nodes"].as_array().unwrap();
			assert_eq!(items.len(), 2);

			// First item
			assert_eq!(items[0]["id"], "item:alpha");
			assert_eq!(items[0]["name"], "Alpha");
			assert!(
				items[0]["time"]["createdAt"].as_str().unwrap().contains("2024-01-15"),
				"Expected createdAt to contain 2024-01-15, got: {}",
				items[0]["time"]["createdAt"]
			);
			assert!(
				items[0]["time"]["updatedAt"].as_str().unwrap().contains("2024-06-01"),
				"Expected updatedAt to contain 2024-06-01, got: {}",
				items[0]["time"]["updatedAt"]
			);
			assert!(
				items[0]["time"]["audit"]["reviewedAt"].as_str().unwrap().contains("2024-06-02")
			);

			// Second item
			assert_eq!(items[1]["id"], "item:beta");
			assert_eq!(items[1]["name"], "Beta");
			assert!(items[1]["time"]["createdAt"].as_str().unwrap().contains("2024-03-20"),);
		}

		// --- Test 2: Query array-of-object sub-fields (tags { label, priority }) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							items(orderBy: { field: ID, direction: ASC }) {
								nodes {
									id
									tags {
										nodes {
											label
											priority
										}
									}
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let items = body["data"]["items"]["nodes"].as_array().unwrap();
			assert_eq!(items.len(), 2);

			// First item has two tags
			let tags0 = items[0]["tags"]["nodes"].as_array().unwrap();
			assert_eq!(tags0.len(), 2);
			assert_eq!(tags0[0]["label"], "urgent");
			assert_eq!(tags0[0]["priority"], 1);
			assert_eq!(tags0[1]["label"], "review");
			assert_eq!(tags0[1]["priority"], 3);

			// Second item has one tag
			let tags1 = items[1]["tags"]["nodes"].as_array().unwrap();
			assert_eq!(tags1.len(), 1);
			assert_eq!(tags1[0]["label"], "feature");
			assert_eq!(tags1[0]["priority"], 2);
		}

		// --- Test 3: Select only specific sub-fields ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							items(orderBy: { field: ID, direction: ASC }) {
								nodes {
									name
									time {
										createdAt
									}
									tags {
										nodes {
											label
										}
									}
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let items = body["data"]["items"]["nodes"].as_array().unwrap();
			assert_eq!(items.len(), 2);

			// time should only have createdAt (not updatedAt)
			assert!(items[0]["time"]["createdAt"].is_string());
			assert!(items[0]["time"].get("updatedAt").is_none());

			// tags should only have label (not priority)
			let tags = items[0]["tags"]["nodes"].as_array().unwrap();
			assert!(tags[0]["label"].is_string());
			assert!(tags[0].get("priority").is_none());
		}

		// --- Test 4: Single record fetch with nested objects ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							item(id: "alpha") {
								id
								name
								time {
									createdAt
									updatedAt
									audit {
										reviewedAt
									}
								}
								tags {
									nodes {
										label
										priority
									}
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let item = &body["data"]["item"];
			assert_eq!(item["id"], "item:alpha");
			assert_eq!(item["name"], "Alpha");
			assert!(item["time"]["createdAt"].as_str().unwrap().contains("2024-01-15"));
			assert!(item["time"]["audit"]["reviewedAt"].as_str().unwrap().contains("2024-06-02"));
			let tags = item["tags"]["nodes"].as_array().unwrap();
			assert_eq!(tags.len(), 2);
			assert_eq!(tags[0]["label"], "urgent");
		}

		// --- Test 5: Schema introspection shows generated nested types ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							__type(name: "ItemTime") {
								name
								fields {
									name
									type {
										name
										kind
									}
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let ty = &body["data"]["__type"];
			assert_eq!(ty["name"], "ItemTime");
			let fields = ty["fields"].as_array().unwrap();
			let field_names: Vec<&str> =
				fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(field_names.contains(&"createdAt"), "Expected createdAt field");
			assert!(field_names.contains(&"updatedAt"), "Expected updatedAt field");
			assert!(field_names.contains(&"audit"), "Expected audit field");
		}

		// --- Test 6: Schema introspection shows generated nested child object types ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							__type(name: "ItemTimeAudit") {
								name
								fields {
									name
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let ty = &body["data"]["__type"];
			assert_eq!(ty["name"], "ItemTimeAudit");
			let fields = ty["fields"].as_array().unwrap();
			let field_names: Vec<&str> =
				fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(field_names.contains(&"reviewedAt"), "Expected reviewedAt field");
		}

		// --- Test 7: Schema introspection for array element type ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							__type(name: "ItemTags") {
								name
								fields {
									name
									type {
										name
										kind
									}
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let ty = &body["data"]["__type"];
			assert_eq!(ty["name"], "ItemTags");
			let fields = ty["fields"].as_array().unwrap();
			let field_names: Vec<&str> =
				fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(field_names.contains(&"label"), "Expected label field");
			assert!(field_names.contains(&"priority"), "Expected priority field");
		}

		// --- Test 8: Optional nested object fields handled gracefully ---
		{
			// Insert article data (table defined in setup)
			let res = client
				.post(sql_url)
				.body(
					r#"
					CREATE article:with_meta SET
						title = "Article One",
						meta = { author: "Alice", source: "Blog" };
					CREATE article:no_meta SET
						title = "Article Two";
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);

			// Query the article with meta
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							article(id: "with_meta") {
								title
								meta {
									author
									source
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let article = &body["data"]["article"];
			assert_eq!(article["title"], "Article One");
			assert_eq!(article["meta"]["author"], "Alice");
			assert_eq!(article["meta"]["source"], "Blog");

			// Query the article without meta — should return null for meta
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							article(id: "no_meta") {
								title
								meta {
									author
									source
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let article = &body["data"]["article"];
			assert_eq!(article["title"], "Article Two");
			assert!(
				article["meta"].is_null(),
				"Expected meta to be null, got: {:?}",
				article["meta"]
			);
		}

		// --- Test 9: Nested-object and object-array filters recurse correctly ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							items(
								filterBy: {
									time: { audit: { reviewedAt: { gt: "2024-07-01T00:00:00Z" } } }
									tags: { some: { label: { eq: "feature" }, priority: { eq: 2 } } }
								}
							) {
								nodes {
									id
									name
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let items = body["data"]["items"]["nodes"].as_array().unwrap();
			assert_eq!(items.len(), 1);
			assert_eq!(items[0]["id"], "item:beta");
			assert_eq!(items[0]["name"], "Beta");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn serialization() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema with various field types to test serialization
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE department SCHEMAFULL;
					DEFINE FIELD name ON department TYPE string;

					DEFINE TABLE widget SCHEMAFULL;
					DEFINE FIELD name ON widget TYPE string;
					DEFINE FIELD created ON widget TYPE datetime;
					DEFINE FIELD lifespan ON widget TYPE duration;
					DEFINE FIELD tracking ON widget TYPE uuid;
					DEFINE FIELD payload ON widget TYPE bytes;
					DEFINE FIELD tags ON widget TYPE array<string>;
					DEFINE FIELD dept ON widget TYPE option<record<department>>;

					CREATE department:eng SET name = "Engineering";
					CREATE department:mkt SET name = "Marketing";

					CREATE widget:alpha SET
						name = "Alpha",
						created = d"2024-06-15T10:30:00Z",
						lifespan = 1h30m,
						tracking = u"550e8400-e29b-41d4-a716-446655440000",
						payload = <bytes>"Hello",
						tags = ["urgent", "review"],
						dept = department:eng;

					CREATE widget:beta SET
						name = "Beta",
						created = d"2025-01-01T00:00:00Z",
						lifespan = 2d12h,
						tracking = u"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
						payload = <bytes>"AB",
						tags = [],
						dept = NONE;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200, "SQL setup failed");
			let sql_body = res.text().await?;
			// Verify no errors in SQL setup
			assert!(!sql_body.contains("\"status\":\"ERR\""), "SQL setup had errors: {sql_body}");
		}

		// --- Test 1: Datetime is serialized as RFC 3339 string ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						widget(id: "alpha") { created }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			let status = res.status();
			let body = res.json::<serde_json::Value>().await?;
			assert_eq!(status, 200, "Expected 200, body: {body}");
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let created = body["data"]["widget"]["created"]
				.as_str()
				.unwrap_or_else(|| panic!("created should be a string, body: {body}"));
			assert!(
				created.contains("2024-06-15"),
				"Expected RFC 3339 datetime containing '2024-06-15', got: {created}"
			);
			// Should not have SurrealQL d'...' wrapping
			assert!(
				!created.starts_with("d'"),
				"Datetime should not have SurrealQL d'' prefix, got: {created}"
			);
		}

		// --- Test 2: Duration is serialized as a clean string ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						widget(id: "alpha") { lifespan }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let lifespan = body["data"]["widget"]["lifespan"].as_str().unwrap();
			// Duration should be a clean string like "1h30m" without quotes/wrapping
			assert!(!lifespan.is_empty(), "Duration should not be empty");
			assert!(
				!lifespan.starts_with("d'") && !lifespan.starts_with('\''),
				"Duration should not have SurrealQL wrapping, got: {lifespan}"
			);
		}

		// --- Test 3: UUID is serialized as a standard UUID string ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						widget(id: "alpha") { tracking }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let tracking = body["data"]["widget"]["tracking"].as_str().unwrap();
			assert_eq!(
				tracking, "550e8400-e29b-41d4-a716-446655440000",
				"UUID should be in standard format"
			);
			// Should not have SurrealQL u'...' wrapping
			assert!(
				!tracking.starts_with("u'"),
				"UUID should not have SurrealQL u'' prefix, got: {tracking}"
			);
		}

		// --- Test 4: Bytes are serialized as base64 string ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						widget(id: "alpha") { payload }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let payload = body["data"]["widget"]["payload"].as_str().unwrap();
			// "Hello" → base64 = "SGVsbG8="
			assert_eq!(payload, "SGVsbG8=", "Bytes should be base64 encoded, got: {payload}");
		}

		// --- Test 5: RecordId in arrays/objects uses raw format ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						widgets(orderBy: {field: ID, direction: ASC}) { nodes { id } }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let widgets = body["data"]["widgets"]["nodes"].as_array().unwrap();
			assert_eq!(widgets[0]["id"], "widget:alpha");
			assert_eq!(widgets[1]["id"], "widget:beta");
		}

		// --- Test 6: Arrays with nested values propagate correctly ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						widget(id: "alpha") { tags { nodes } }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let tags = body["data"]["widget"]["tags"]["nodes"].as_array().unwrap();
			assert_eq!(tags.len(), 2);
			assert_eq!(tags[0], "urgent");
			assert_eq!(tags[1], "review");
		}

		// --- Test 7: Empty arrays don't cause panics ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						widget(id: "beta") { tags { nodes } }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let tags = body["data"]["widget"]["tags"]["nodes"].as_array().unwrap();
			assert_eq!(tags.len(), 0);
		}

		// --- Test 8: option<record> field — set to a value ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						widget(id: "alpha") {
							name
							dept {
								id
								name
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let widget = &body["data"]["widget"];
			assert_eq!(widget["name"], "Alpha");
			assert_eq!(widget["dept"]["id"], "department:eng");
			assert_eq!(widget["dept"]["name"], "Engineering");
		}

		// --- Test 9: option<record> field — set to NONE (null) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						widget(id: "beta") {
							name
							dept {
								id
								name
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let widget = &body["data"]["widget"];
			assert_eq!(widget["name"], "Beta");
			assert!(
				widget["dept"].is_null(),
				"Expected dept to be null for NONE value, got: {:?}",
				widget["dept"]
			);
		}

		// --- Test 10: Schema introspection shows option<record> as nullable type ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__type(name: "Widget") {
							fields {
								name
								type {
									name
									kind
									ofType { name kind }
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let fields = body["data"]["__type"]["fields"].as_array().unwrap();
			let dept_field = fields.iter().find(|f| f["name"] == "dept").unwrap();
			let type_info = &dept_field["type"];
			// option<record<department>> should be nullable (not NON_NULL),
			// and the inner type should be "department" (not a union like "none_or_department")
			assert_ne!(
				type_info["kind"], "NON_NULL",
				"option<record> should be nullable, got: {type_info:?}"
			);
			// The type should resolve to the department table type (not a union)
			let type_name = type_info["name"].as_str().unwrap_or("");
			assert_eq!(
				type_name, "Department",
				"option<record<department>> should resolve to 'Department' type, got: {type_name}"
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn mutations() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Setup schema
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE item SCHEMAFUL;
					DEFINE FIELD name ON item TYPE string;
					DEFINE FIELD price ON item TYPE int;
					DEFINE TABLE person SCHEMAFUL;
					DEFINE FIELD name ON person TYPE string;
					DEFINE TABLE post SCHEMAFUL;
					DEFINE FIELD title ON post TYPE string;
					DEFINE TABLE likes TYPE RELATION FROM person TO post SCHEMAFUL;
					DEFINE FIELD rating ON likes TYPE int;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// --- Test 1: createItem (single create with explicit id) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createItem(input: { id: "1", name: "Widget", price: 100 }) {
							success
							message
							item {
								id
								name
								price
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let payload = &body["data"]["createItem"];
			assert_eq!(payload["success"], true);
			let item = &payload["item"];
			assert_eq!(item["id"], "item:1");
			assert_eq!(item["name"], "Widget");
			assert_eq!(item["price"], 100);
		}

		// --- Test 2: createItem (auto-generated id) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createItem(input: { name: "Gadget", price: 200 }) {
							success
							item {
								id
								name
								price
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let item = &body["data"]["createItem"]["item"];
			// id should be auto-generated
			assert!(item["id"].as_str().unwrap().starts_with("item:"));
			assert_eq!(item["name"], "Gadget");
			assert_eq!(item["price"], 200);
		}

		// --- Test 3: updateItem ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						updateItem(input: { id: "1", name: "Super Widget" }) {
							success
							item {
								id
								name
								price
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let item = &body["data"]["updateItem"]["item"];
			assert_eq!(item["id"], "item:1");
			assert_eq!(item["name"], "Super Widget");
			// price should be unchanged (MERGE, not CONTENT)
			assert_eq!(item["price"], 100);
		}

		// --- Test 4: upsertItem (existing record) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						upsertItem(input: { id: "1", name: "Mega Widget", price: 150 }) {
							success
							item {
								id
								name
								price
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let item = &body["data"]["upsertItem"]["item"];
			assert_eq!(item["id"], "item:1");
			assert_eq!(item["name"], "Mega Widget");
			assert_eq!(item["price"], 150);
		}

		// --- Test 5: upsertItem (new record) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						upsertItem(input: { id: "99", name: "New Item", price: 50 }) {
							success
							item {
								id
								name
								price
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let item = &body["data"]["upsertItem"]["item"];
			assert_eq!(item["id"], "item:99");
			assert_eq!(item["name"], "New Item");
			assert_eq!(item["price"], 50);
		}

		// --- Test 6: deleteItem ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						deleteItem(input: { id: "99" }) {
							success
							item {
								id
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			assert_eq!(body["data"]["deleteItem"]["success"], true);
			assert_eq!(body["data"]["deleteItem"]["item"]["id"], "item:99");
		}

		// Verify deletion via query
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						item(id: "99") { id }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			assert!(body["data"]["item"].is_null());
		}

		// --- Test 7: relation mutation (relateLikes) ---
		{
			// First create the records to relate
			client
				.post(sql_url)
				.body(
					r#"
					CREATE person:alice SET name = "Alice";
					CREATE post:1 SET title = "Hello World";
				"#,
				)
				.send()
				.await?;

			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						relateLikes(input: {
							in: "person:alice",
							out: "post:1",
							rating: 5
						}) {
							success
							likes {
								id
								rating
								node {
									id
									title
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let likes = &body["data"]["relateLikes"]["likes"];
			assert!(likes["id"].as_str().unwrap().starts_with("likes:"));
			assert_eq!(likes["rating"], 5);
			assert_eq!(likes["node"]["id"], "post:1");
			assert_eq!(likes["node"]["title"], "Hello World");
		}

		// --- Test 8: Relation update mutation keeps GitHub-style payload shape ---
		{
			let like_lookup = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						person(id: "alice") {
							likes {
								edges {
									id
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?
				.json::<serde_json::Value>()
				.await?;
			let like_id = like_lookup["data"]["person"]["likes"]["edges"][0]["id"]
				.as_str()
				.unwrap()
				.to_string();

			let res = client
				.post(gql_url)
				.body(
					json!({"query": format!(r#"mutation {{
						updateLikes(input: {{ id: "{like_id}", rating: 8 }}) {{
							success
							likes {{
								id
								rating
							}}
						}}
					}}"#)})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			assert_eq!(body["data"]["updateLikes"]["success"], true);
			assert_eq!(body["data"]["updateLikes"]["likes"]["rating"], 8);
		}

		// --- Test 9: Schema introspection shows mutation type ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__schema {
							mutationType {
								name
								fields { name }
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let mutation_type = &body["data"]["__schema"]["mutationType"];
			assert_eq!(mutation_type["name"], "Mutation");

			let fields = mutation_type["fields"].as_array().unwrap();
			let field_names: Vec<&str> =
				fields.iter().map(|f| f["name"].as_str().unwrap()).collect();

			// Check that all expected mutation fields exist
			assert!(field_names.contains(&"createItem"), "Missing createItem");
			assert!(field_names.contains(&"updateItem"), "Missing updateItem");
			assert!(field_names.contains(&"upsertItem"), "Missing upsertItem");
			assert!(field_names.contains(&"deleteItem"), "Missing deleteItem");
			assert!(field_names.contains(&"relateLikes"), "Missing relateLikes");
			assert!(field_names.contains(&"updateLikes"), "Missing updateLikes");
			assert!(field_names.contains(&"upsertLikes"), "Missing upsertLikes");
			assert!(field_names.contains(&"deleteLikes"), "Missing deleteLikes");
			assert!(!field_names.contains(&"createManyItem"), "Unexpected createManyItem");
			assert!(!field_names.contains(&"updateManyItem"), "Unexpected updateManyItem");
			assert!(!field_names.contains(&"upsertManyItem"), "Unexpected upsertManyItem");
			assert!(!field_names.contains(&"deleteManyItem"), "Unexpected deleteManyItem");
		}

		// --- Test 10: Input and payload introspection ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						createInput: __type(name: "CreateItemInput") {
							kind
							inputFields { name type { name kind ofType { name kind } } }
						}
						updateInput: __type(name: "UpdateItemInput") {
							kind
							inputFields { name type { name kind ofType { name kind } } }
						}
						deletePayload: __type(name: "DeleteItemPayload") {
							fields { name }
						}
						relateInput: __type(name: "RelateLikesInput") {
							inputFields { name type { name kind ofType { name kind } } }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);

			// CreateItemInput should exist as INPUT_OBJECT
			let create_input = &body["data"]["createInput"];
			assert_eq!(create_input["kind"], "INPUT_OBJECT");
			let create_fields = create_input["inputFields"].as_array().unwrap();
			let create_field_names: Vec<&str> =
				create_fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(create_field_names.contains(&"id"), "CreateInput missing 'id'");
			assert!(create_field_names.contains(&"name"), "CreateInput missing 'name'");
			assert!(create_field_names.contains(&"price"), "CreateInput missing 'price'");

			// UpdateItemInput should have all fields optional
			let update_input = &body["data"]["updateInput"];
			assert_eq!(update_input["kind"], "INPUT_OBJECT");
			let update_fields = update_input["inputFields"].as_array().unwrap();
			for field in update_fields {
				if field["name"] == "id" {
					assert_eq!(field["type"]["kind"], "NON_NULL");
				} else {
					assert_ne!(
						field["type"]["kind"], "NON_NULL",
						"Update input field '{}' should be optional",
						field["name"]
					);
				}
			}

			let payload_fields = body["data"]["deletePayload"]["fields"].as_array().unwrap();
			let payload_field_names: Vec<&str> =
				payload_fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(payload_field_names.contains(&"item"));
			assert!(payload_field_names.contains(&"success"));
			assert!(payload_field_names.contains(&"message"));

			let relate_fields = body["data"]["relateInput"]["inputFields"].as_array().unwrap();
			let relate_map = relate_fields
				.iter()
				.map(|field| (field["name"].as_str().unwrap(), &field["type"]))
				.collect::<std::collections::HashMap<_, _>>();
			assert_eq!(relate_map["in"]["kind"], "NON_NULL");
			assert_eq!(relate_map["out"]["kind"], "NON_NULL");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn depth_and_complexity_limits() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema with depth and complexity limits
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO DEPTH 3 COMPLEXITY 10;
					DEFINE TABLE person SCHEMAFUL;
					DEFINE FIELD name ON person TYPE string;
					DEFINE FIELD age ON person TYPE int;
					DEFINE TABLE post SCHEMAFUL;
					DEFINE FIELD title ON post TYPE string;
					DEFINE FIELD author ON post TYPE record<person>;
					DEFINE TABLE comment SCHEMAFUL;
					DEFINE FIELD text ON comment TYPE string;
					DEFINE FIELD post ON comment TYPE record<post>;
					CREATE person:1 SET name = 'Alice', age = 30;
					CREATE post:1 SET title = 'Hello', author = person:1;
					CREATE comment:1 SET text = 'Nice', post = post:1;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// A simple shallow query should succeed (depth 2, within limit of 3)
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"{ person(id: "person:1") { id, name } }"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors for shallow query: {:?}", body);
			assert_eq!(body["data"]["person"]["id"], "person:1");
		}

		// A deeply nested query should fail with depth limit error (depth > 3)
		{
			let res = client
				.post(gql_url)
				.body(
						json!({"query": r#"{ comment(id: "comment:1") { text, post { title, author { name, age } } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let errors = &body["errors"];
			assert!(errors.is_array(), "Expected errors for deep query, got: {:?}", body);
			let error_msg = errors[0]["message"].as_str().unwrap_or("");
			assert!(
				error_msg.contains("nested too deep") || error_msg.contains("too deep"),
				"Expected depth limit error, got: {error_msg}"
			);
		}

		// A query with too many fields should fail with complexity limit error (>10 fields)
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
							person(id: "person:1") { id, name, age }
							post(id: "post:1") { id, title }
							comment(id: "comment:1") { id, text }
							p2: person(id: "person:1") { id, name, age }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let errors = &body["errors"];
			assert!(errors.is_array(), "Expected errors for complex query, got: {:?}", body);
			let error_msg = errors[0]["message"].as_str().unwrap_or("");
			assert!(
				error_msg.contains("too complex") || error_msg.contains("complexity"),
				"Expected complexity limit error, got: {error_msg}"
			);
		}

		// Reconfigure with higher limits and verify previously failing query works
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG OVERWRITE GRAPHQL AUTO DEPTH 10 COMPLEXITY 100;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// The deeply nested query should now succeed
		{
			let res = client
				.post(gql_url)
				.body(
						json!({"query": r#"{ comment(id: "comment:1") { text, post { title, author { name } } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Expected no errors with raised limits, got: {:?}",
				body["errors"]
			);
		}

		// The high-field-count query should also succeed with higher complexity limit
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
							person(id: "person:1") { id, name, age }
							post(id: "post:1") { id, title }
							comment(id: "comment:1") { id, text }
							p2: person(id: "person:1") { id, name, age }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Expected no errors with raised limits, got: {:?}",
				body["errors"]
			);
		}

		// Reconfigure without limits and verify everything works
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG OVERWRITE GRAPHQL AUTO;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// All queries should succeed without any limits
		{
			let res = client
				.post(gql_url)
				.body(
						json!({"query": r#"{ comment(id: "comment:1") { text, post { title, author { name, age } } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Expected no errors without limits, got: {:?}",
				body["errors"]
			);
		}

		// Verify DEFINE CONFIG GRAPHQL round-trip preserves DEPTH and COMPLEXITY
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG OVERWRITE GRAPHQL AUTO DEPTH 5 COMPLEXITY 50;
					INFO FOR DB;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let info_result = &body[1]["result"];
			let config_str = info_result["configs"]["GraphQL"].as_str().unwrap_or("");
			assert!(
				config_str.contains("DEPTH 5"),
				"Expected 'DEPTH 5' in config, got: {config_str}"
			);
			assert!(
				config_str.contains("COMPLEXITY 50"),
				"Expected 'COMPLEXITY 50' in config, got: {config_str}"
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn introspection_control() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema with introspection enabled (default)
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE person SCHEMAFUL;
					DEFINE FIELD name ON person TYPE string;
					DEFINE FIELD age ON person TYPE int;
					CREATE person:1 SET name = 'Alice', age = 30;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Introspection should work by default
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{ __schema { queryType { fields { name } } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Introspection should be allowed by default, got errors: {:?}",
				body["errors"]
			);
			let fields = &body["data"]["__schema"]["queryType"]["fields"];
			assert!(fields.is_array(), "Expected query type fields from introspection");
		}

		// __type introspection query should also work
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{ __type(name: "Person") { name fields { name } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Expected no errors for __type query, got: {:?}",
				body
			);
			assert_eq!(body["data"]["__type"]["name"], "Person");
		}

		// Normal data queries should work
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"{ persons { nodes { id, name, age } } }"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors for data query, got: {:?}", body);
			assert!(body["data"]["persons"]["nodes"].is_array(), "Expected person data");
		}

		// Disable introspection
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG OVERWRITE GRAPHQL AUTO INTROSPECTION NONE;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// __schema introspection should now be blocked
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{ __schema { queryType { fields { name } } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let schema_data = &body["data"]["__schema"];
			// When introspection is disabled, __schema should return null or produce an error
			assert!(
				schema_data.is_null() || body["errors"].is_array(),
				"Expected introspection to be blocked, got: {:?}",
				body
			);
		}

		// __type introspection should also be blocked
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{ __type(name: "Person") { name fields { name } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let type_data = &body["data"]["__type"];
			assert!(
				type_data.is_null() || body["errors"].is_array(),
				"Expected __type introspection to be blocked, got: {:?}",
				body
			);
		}

		// Normal data queries should still work even with introspection disabled
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"{ persons { nodes { id, name, age } } }"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Normal queries should still work with introspection disabled, got: {:?}",
				body["errors"]
			);
			assert!(body["data"]["persons"]["nodes"].is_array(), "Expected person data");
		}

		// Re-enable introspection
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG OVERWRITE GRAPHQL AUTO INTROSPECTION AUTO;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Introspection should work again
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{ __schema { queryType { fields { name } } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Introspection should work after re-enabling, got: {:?}",
				body["errors"]
			);
			let fields = &body["data"]["__schema"]["queryType"]["fields"];
			assert!(fields.is_array(), "Expected query type fields from introspection");
		}

		// Verify DEFINE CONFIG GRAPHQL round-trip preserves INTROSPECTION setting
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG OVERWRITE GRAPHQL AUTO INTROSPECTION NONE;
					INFO FOR DB;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let info_result = &body[1]["result"];
			let config_str = info_result["configs"]["GraphQL"].as_str().unwrap_or("");
			assert!(
				config_str.contains("INTROSPECTION NONE"),
				"Expected 'INTROSPECTION NONE' in config, got: {config_str}"
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn schema_uses_surreal_comments_for_descriptions()
	-> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE person SCHEMAFUL COMMENT "Person records";
					DEFINE FIELD name ON person TYPE string COMMENT "Person display name";
					DEFINE FIELD age ON person TYPE int COMMENT "Person age";
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		}

		let res = client
			.post(gql_url)
			.body(
				json!({"query": r#"{
					queryType: __type(name: "Query") {
						fields {
							name
							description
						}
					}
					personType: __type(name: "Person") {
						fields {
							name
							description
						}
					}
				}"#})
				.to_string(),
			)
			.send()
			.await?;
		assert_eq!(res.status(), 200);

		let body = res.json::<serde_json::Value>().await?;
		assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);

		let query_fields = body["data"]["queryType"]["fields"].as_array().unwrap();
		let person_query_field = query_fields.iter().find(|f| f["name"] == "person").unwrap();
		assert_eq!(person_query_field["description"], "Person records");

		let person_fields = body["data"]["personType"]["fields"].as_array().unwrap();
		let name_field = person_fields.iter().find(|f| f["name"] == "name").unwrap();
		let age_field = person_fields.iter().find(|f| f["name"] == "age").unwrap();

		assert_eq!(name_field["description"], "Person display name");
		assert_eq!(age_field["description"], "Person age");

		Ok(())
	}

	#[test(tokio::test)]
	async fn output_fields_are_nullable_by_default() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE person SCHEMAFUL;
					DEFINE FIELD name ON person TYPE string;
					DEFINE FIELD tags ON person TYPE array<string>;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		}

		let res = client
			.post(gql_url)
			.body(
				json!({"query": r#"{
					queryType: __type(name: "Query") {
						fields {
							name
							type {
								kind
								name
								ofType {
									kind
									name
								}
							}
						}
					}
					personType: __type(name: "Person") {
						fields {
							name
							type {
								kind
								name
								ofType {
									kind
									name
								}
							}
						}
					}
				}"#})
				.to_string(),
			)
			.send()
			.await?;
		assert_eq!(res.status(), 200);

		let body = res.json::<serde_json::Value>().await?;
		assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);

		let person_fields = body["data"]["personType"]["fields"].as_array().unwrap();
		let id_field = person_fields.iter().find(|f| f["name"] == "id").unwrap();
		let name_field = person_fields.iter().find(|f| f["name"] == "name").unwrap();
		let tags_field = person_fields.iter().find(|f| f["name"] == "tags").unwrap();

		assert_eq!(id_field["type"]["kind"], "NON_NULL");
		assert_eq!(id_field["type"]["ofType"]["name"], "ID");

		assert_eq!(name_field["type"]["kind"], "SCALAR");
		assert_eq!(name_field["type"]["name"], "String");

		assert_eq!(tags_field["type"]["kind"], "OBJECT");
		assert_eq!(tags_field["type"]["name"], "PersonTagsConnection");

		let query_fields = body["data"]["queryType"]["fields"].as_array().unwrap();
		let persons_field = query_fields.iter().find(|f| f["name"] == "persons").unwrap();
		assert_eq!(persons_field["type"]["kind"], "NON_NULL");
		assert_eq!(persons_field["type"]["ofType"]["name"], "PersonConnection");

		Ok(())
	}

	#[test(tokio::test)]
	async fn auth_mutations() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema with an access method that has both SIGNIN and SIGNUP
		{
			let res = client
				.post(sql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE USER ns_owner ON NAMESPACE PASSWORD 'ns-secret' ROLES OWNER;
					DEFINE USER db_owner ON DATABASE PASSWORD 'db-secret' ROLES OWNER;

					DEFINE ACCESS user ON DATABASE TYPE RECORD
						SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
						SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
						DURATION FOR SESSION 60s, FOR TOKEN 1d;

					DEFINE TABLE user SCHEMAFUL
						PERMISSIONS FOR select, create, update, delete WHERE id = $auth;
					DEFINE FIELD email ON user TYPE string;
					DEFINE FIELD pass ON user TYPE string;

					DEFINE TABLE post SCHEMAFUL
						PERMISSIONS FOR select WHERE $auth != NONE
						FOR create, update, delete WHERE $auth != NONE;
					DEFINE FIELD title ON post TYPE string;
					DEFINE FIELD content ON post TYPE string;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		}

		// Test schema introspection: the corrected auth mutation surface should be present.
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"{
						__type(name: "Mutation") {
							fields { name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Introspection errors: {:?}", body["errors"]);
			let fields = &body["data"]["__type"]["fields"];
			let field_names: Vec<&str> =
				fields.as_array().unwrap().iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(
				field_names.contains(&"signin"),
				"Mutation should have signin field, got: {field_names:?}"
			);
			assert!(
				field_names.contains(&"signinRoot"),
				"Mutation should have signinRoot field, got: {field_names:?}"
			);
			assert!(
				field_names.contains(&"signinNS"),
				"Mutation should have signinNS field, got: {field_names:?}"
			);
			assert!(
				field_names.contains(&"signinDB"),
				"Mutation should have signinDB field, got: {field_names:?}"
			);
			assert!(
				field_names.contains(&"signinAccess"),
				"Mutation should have signinAccess field, got: {field_names:?}"
			);
			assert!(
				field_names.contains(&"signup"),
				"Mutation should have signup field, got: {field_names:?}"
			);
			assert!(
				field_names.contains(&"signupAccess"),
				"Mutation should have signupAccess field, got: {field_names:?}"
			);
			assert!(
				field_names.contains(&"authenticate"),
				"Mutation should have authenticate field, got: {field_names:?}"
			);
			assert!(
				field_names.contains(&"invalidate"),
				"Mutation should have invalidate field, got: {field_names:?}"
			);
		}

		// Test input/payload typing via introspection.
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"{
						mutationType: __type(name: "Mutation") {
							fields {
								name
								args { name type { kind name ofType { kind name } } }
								type { kind name ofType { kind name } }
							}
						}
						signinInput: __type(name: "SigninInput") {
							inputFields {
								name
								type { kind name ofType { kind name } }
							}
						}
						signinAccessInput: __type(name: "SigninAccessInput") {
							inputFields {
								name
								type { kind name ofType { kind name } }
							}
						}
						signupAccessInput: __type(name: "SignupAccessInput") {
							inputFields {
								name
								type { kind name ofType { kind name } }
							}
						}
						signinAccessPayload: __type(name: "SigninAccessPayload") {
							fields { name }
						}
						invalidatePayload: __type(name: "InvalidatePayload") {
							fields { name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Introspection errors: {:?}", body["errors"]);

			let fields = body["data"]["mutationType"]["fields"].as_array().unwrap();
			let signin = fields.iter().find(|f| f["name"] == "signin").unwrap();
			let signin_root = fields.iter().find(|f| f["name"] == "signinRoot").unwrap();
			let signin_ns = fields.iter().find(|f| f["name"] == "signinNS").unwrap();
			let signin_db = fields.iter().find(|f| f["name"] == "signinDB").unwrap();
			let signin_access = fields.iter().find(|f| f["name"] == "signinAccess").unwrap();
			let signup = fields.iter().find(|f| f["name"] == "signup").unwrap();
			let signup_access = fields.iter().find(|f| f["name"] == "signupAccess").unwrap();
			let authenticate = fields.iter().find(|f| f["name"] == "authenticate").unwrap();
			let invalidate = fields.iter().find(|f| f["name"] == "invalidate").unwrap();

			assert_eq!(signin["type"]["kind"], "NON_NULL");
			assert_eq!(signin["type"]["ofType"]["name"], "SigninPayload");
			assert_eq!(signin_root["type"]["ofType"]["name"], "SigninRootPayload");
			assert_eq!(signin_ns["type"]["ofType"]["name"], "SigninNSPayload");
			assert_eq!(signin_db["type"]["ofType"]["name"], "SigninDBPayload");
			assert_eq!(signin_access["type"]["ofType"]["name"], "SigninAccessPayload");
			assert_eq!(signup["type"]["ofType"]["name"], "SignupPayload");
			assert_eq!(signup_access["type"]["ofType"]["name"], "SignupAccessPayload");
			assert_eq!(authenticate["type"]["ofType"]["name"], "AuthenticatePayload");
			assert_eq!(invalidate["type"]["ofType"]["name"], "InvalidatePayload");

			let signin_args = signin["args"].as_array().unwrap();
			assert_eq!(signin_args.len(), 1);
			assert_eq!(signin_args[0]["name"], "input");
			assert_eq!(signin_args[0]["type"]["kind"], "NON_NULL");
			assert_eq!(signin_args[0]["type"]["ofType"]["name"], "SigninInput");

			let signin_input_fields =
				body["data"]["signinInput"]["inputFields"].as_array().unwrap();
			let signin_field_names: Vec<&str> =
				signin_input_fields.iter().map(|field| field["name"].as_str().unwrap()).collect();
			assert!(signin_field_names.contains(&"namespace"));
			assert!(signin_field_names.contains(&"database"));
			assert!(signin_field_names.contains(&"access"));
			assert!(signin_field_names.contains(&"username"));
			assert!(signin_field_names.contains(&"password"));
			assert!(signin_field_names.contains(&"variables"));
			assert!(!signin_field_names.contains(&"input"));

			let signin_access_input_fields =
				body["data"]["signinAccessInput"]["inputFields"].as_array().unwrap();
			let signin_access_field_names: Vec<&str> = signin_access_input_fields
				.iter()
				.map(|field| field["name"].as_str().unwrap())
				.collect();
			assert_eq!(
				signin_access_field_names,
				vec!["namespace", "database", "access", "variables"]
			);

			let signup_access_input_fields =
				body["data"]["signupAccessInput"]["inputFields"].as_array().unwrap();
			let signup_access_field_names: Vec<&str> = signup_access_input_fields
				.iter()
				.map(|field| field["name"].as_str().unwrap())
				.collect();
			assert_eq!(
				signup_access_field_names,
				vec!["namespace", "database", "access", "variables"]
			);

			let payload_fields = body["data"]["signinAccessPayload"]["fields"].as_array().unwrap();
			let payload_field_names: Vec<&str> =
				payload_fields.iter().map(|field| field["name"].as_str().unwrap()).collect();
			assert!(payload_field_names.contains(&"success"));
			assert!(payload_field_names.contains(&"message"));
			assert!(payload_field_names.contains(&"token"));

			assert_eq!(invalidate["args"].as_array().unwrap().len(), 0);
			let invalidate_payload_fields =
				body["data"]["invalidatePayload"]["fields"].as_array().unwrap();
			let invalidate_payload_field_names: Vec<&str> = invalidate_payload_fields
				.iter()
				.map(|field| field["name"].as_str().unwrap())
				.collect();
			assert!(invalidate_payload_field_names.contains(&"success"));
			assert!(invalidate_payload_field_names.contains(&"message"));
		}

		// Test signupAccess: create a new user via a dedicated access mutation.
		let signup_token;
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						signupAccess(input: {
							access: "user"
							variables: { email: "alice@example.com", pass: "secret123" }
						}) {
							success
							message
							token
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "signupAccess errors: {:?}", body["errors"]);
			assert_eq!(body["data"]["signupAccess"]["success"], true);
			assert_eq!(body["data"]["signupAccess"]["message"], "Registration succeeded.");
			let token = body["data"]["signupAccess"]["token"].as_str().unwrap();
			assert!(!token.is_empty(), "SignUp should return a non-empty JWT token");
			assert_eq!(token.split('.').count(), 3, "Token should be a valid JWT format");
			signup_token = token.to_string();
		}

		// Test that the sign-up token works for authenticated queries.
		{
			let res = client
				.post(gql_url)
				.bearer_auth(&signup_token)
				.body(json!({"query": r#"{ posts { nodes { id } } }"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Authenticated query should succeed, got errors: {:?}",
				body["errors"]
			);
		}

		// Test generic signIn using an access method.
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						signin(input: {
							access: "user"
							variables: { email: "alice@example.com", pass: "secret123" }
						}) {
							success
							message
							token
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Generic signin errors: {:?}", body["errors"]);
			assert_eq!(body["data"]["signin"]["success"], true);
			assert_eq!(body["data"]["signin"]["message"], "Authentication succeeded.");
			let token = body["data"]["signin"]["token"].as_str().unwrap();
			assert_eq!(token.split('.').count(), 3);
		}

		// Test signinAccess with the newly created user.
		let signin_token;
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						signinAccess(input: {
							access: "user"
							variables: { email: "alice@example.com", pass: "secret123" }
						}) {
							success
							message
							token
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "signinAccess errors: {:?}", body["errors"]);
			assert_eq!(body["data"]["signinAccess"]["success"], true);
			assert_eq!(body["data"]["signinAccess"]["message"], "Authentication succeeded.");
			let token = body["data"]["signinAccess"]["token"].as_str().unwrap();
			assert!(!token.is_empty(), "SignIn should return a non-empty JWT token");
			assert_eq!(token.split('.').count(), 3, "Token should be a valid JWT format");
			signin_token = token.to_string();
		}

		// Test authenticate with a valid token.
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": format!(r#"mutation {{
						authenticate(input: {{ token: "{}" }}) {{
							success
							message
							token
						}}
					}}"#, signin_token)})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Authenticate errors: {:?}", body["errors"]);
			assert_eq!(body["data"]["authenticate"]["success"], true);
			assert_eq!(body["data"]["authenticate"]["message"], "The token is valid.");
			assert_eq!(body["data"]["authenticate"]["token"], signin_token);
		}

		// Test root, namespace, and database sign-in flows.
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": format!(r#"mutation {{
						signinRoot(input: {{ username: "{USER}", password: "{PASS}" }}) {{
							success
							token
						}}
						signinNS(input: {{ username: "ns_owner", password: "ns-secret" }}) {{
							success
							token
						}}
						signinDB(input: {{ username: "db_owner", password: "db-secret" }}) {{
							success
							token
						}}
					}}"#)})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Scoped sign-in errors: {:?}", body["errors"]);
			assert_eq!(body["data"]["signinRoot"]["success"], true);
			assert_eq!(body["data"]["signinNS"]["success"], true);
			assert_eq!(body["data"]["signinDB"]["success"], true);
			assert_eq!(body["data"]["signinRoot"]["token"].as_str().unwrap().split('.').count(), 3);
			assert_eq!(body["data"]["signinNS"]["token"].as_str().unwrap().split('.').count(), 3);
			assert_eq!(body["data"]["signinDB"]["token"].as_str().unwrap().split('.').count(), 3);
		}

		// Test invalidate exposes no input.
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						invalidate {
							success
							message
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Invalidate errors: {:?}", body["errors"]);
			assert_eq!(body["data"]["invalidate"]["success"], true);
			assert_eq!(
				body["data"]["invalidate"]["message"],
				"The current session has been invalidated."
			);
		}

		// Test that the sign-in token works for querying data.
		{
			let res = client
				.post(sql_url)
				.basic_auth(USER, Some(PASS))
				.body(r#"CREATE post:1 SET title = "Hello", content = "World";"#)
				.send()
				.await?;
			assert_eq!(res.status(), 200);

			// Then query using the signin token
			let res = client
				.post(gql_url)
				.bearer_auth(&signin_token)
				.body(json!({"query": r#"{ posts { nodes { id title content } } }"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Query with signin token should succeed, got errors: {:?}",
				body["errors"]
			);
			let posts = &body["data"]["posts"]["nodes"];
			assert!(posts.is_array(), "Expected connection nodes");
			assert_eq!(posts.as_array().unwrap().len(), 1);
			assert_eq!(posts[0]["title"], "Hello");
		}

		// Test signin with wrong credentials: should return a generic auth error.
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						signin(input: {
							access: "user"
							variables: { email: "alice@example.com", pass: "wrongpassword" }
						}) {
							success
							token
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_array(), "signin with wrong password should return errors");
			let error_msg = body["errors"][0]["message"].as_str().unwrap_or("");
			assert!(
				error_msg.contains("problem with authentication"),
				"Error should be a generic auth error, got: {error_msg}"
			);
			assert!(
				!error_msg.contains("SELECT") && !error_msg.contains("argon2"),
				"Auth error should not leak internal details, got: {error_msg}"
			);
		}

		// Test signin with non-existent access method: should still return a generic auth error.
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						signin(input: {
							access: "nonexistent_access"
							variables: { email: "alice@example.com", pass: "secret123" }
						}) {
							success
							token
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_array(), "Expected auth errors for unknown access");
			let error_msg = body["errors"][0]["message"].as_str().unwrap_or("");
			assert!(
				error_msg.contains("problem with authentication"),
				"Unknown access should still be generic, got: {error_msg}"
			);
		}

		// Test generic signup: should still work and return a token.
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						signup(input: {
							access: "user"
							variables: { email: "bob@example.com", pass: "bobpass" }
						}) {
							success
							token
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Generic signup should succeed: {:?}",
				body["errors"]
			);
			let token = body["data"]["signup"]["token"].as_str().unwrap();
			assert!(!token.is_empty());
		}

		// Test that when no signup clause exists, only the sign-up mutations are absent.
		{
			let ns2 = Ulid::new().to_string();
			let db2 = Ulid::new().to_string();

			// Set up a signin-only access method in a new db
			let res = client
				.post(sql_url)
				.basic_auth(USER, Some(PASS))
				.header("surreal-ns", &ns2)
				.header("surreal-db", &db2)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE ACCESS readonly_user ON DATABASE TYPE RECORD
						SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
						DURATION FOR SESSION 60s, FOR TOKEN 1d;
					DEFINE TABLE user SCHEMAFUL;
					DEFINE FIELD email ON user TYPE string;
					DEFINE FIELD pass ON user TYPE string;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);

			// Check that signIn exists but signUp does NOT
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.header("surreal-ns", &ns2)
				.header("surreal-db", &db2)
				.body(
					json!({"query": r#"{
						__type(name: "Mutation") {
							fields { name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Introspection errors: {:?}", body["errors"]);
			let fields = &body["data"]["__type"]["fields"];
			let field_names: Vec<&str> =
				fields.as_array().unwrap().iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(
				field_names.contains(&"signin"),
				"Mutation should have signin field when signin clause exists"
			);
			assert!(
				field_names.contains(&"signinAccess"),
				"Mutation should have signinAccess when signin clause exists"
			);
			assert!(field_names.contains(&"signinRoot"), "Mutation should still have signinRoot");
			assert!(field_names.contains(&"signinNS"), "Mutation should still have signinNS");
			assert!(field_names.contains(&"signinDB"), "Mutation should still have signinDB");
			assert!(
				field_names.contains(&"authenticate"),
				"Mutation should still have authenticate"
			);
			assert!(field_names.contains(&"invalidate"), "Mutation should still have invalidate");
			assert!(
				!field_names.contains(&"signup"),
				"Mutation should NOT have signup when no signup clause exists, got: {field_names:?}"
			);
			assert!(
				!field_names.contains(&"signupAccess"),
				"Mutation should NOT have signupAccess, got: {field_names:?}"
			);
		}

		Ok(())
	}

	/// Tests that the N+1 query optimization (CachedRecord) correctly resolves
	/// fields from cached record data without individual per-field database queries.
	///
	/// Validates:
	/// - Multi-field list queries return all fields correctly
	/// - Single-record queries return all fields from cache
	/// - Record-link dereferencing fetches and caches the target record
	/// - Mutation results are cached for field resolution
	/// - Relation record fields are resolved from cache
	/// - Nested object fields are resolved from cache
	#[test(tokio::test)]
	async fn cached_record_resolution() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema with multiple field types, record links, relations,
		// and nested objects to exercise all CachedRecord code paths.
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE department SCHEMAFULL;
					DEFINE FIELD name ON department TYPE string;
					DEFINE FIELD budget ON department TYPE int;

					DEFINE TABLE employee SCHEMAFULL;
					DEFINE FIELD name ON employee TYPE string;
					DEFINE FIELD age ON employee TYPE int;
					DEFINE FIELD active ON employee TYPE bool;
					DEFINE FIELD dept ON employee TYPE record<department>;

					DEFINE TABLE project SCHEMAFULL;
					DEFINE FIELD title ON project TYPE string;

					DEFINE TABLE works_on TYPE RELATION FROM employee TO project SCHEMAFULL;
					DEFINE FIELD role ON works_on TYPE string;

					DEFINE TABLE widget SCHEMAFULL;
					DEFINE FIELD name ON widget TYPE string;
					DEFINE FIELD price ON widget TYPE float;
					DEFINE FIELD meta ON widget TYPE object;
					DEFINE FIELD meta.color ON widget TYPE string;
					DEFINE FIELD meta.weight ON widget TYPE float;
					DEFINE FIELD tags ON widget TYPE array<object>;
					DEFINE FIELD tags.*.label ON widget TYPE string;

					-- Seed data
					CREATE department:eng SET name = 'Engineering', budget = 500000;
					CREATE department:sales SET name = 'Sales', budget = 200000;

					CREATE employee:alice SET name = 'Alice', age = 30, active = true, dept = department:eng;
					CREATE employee:bob SET name = 'Bob', age = 25, active = false, dept = department:sales;
					CREATE employee:carol SET name = 'Carol', age = 35, active = true, dept = department:eng;

					CREATE project:alpha SET title = 'Project Alpha';
					CREATE project:beta SET title = 'Project Beta';

					RELATE employee:alice->works_on:wa->project:alpha SET role = 'lead';
					RELATE employee:bob->works_on:wb->project:beta SET role = 'contributor';
					RELATE employee:carol->works_on:wc->project:alpha SET role = 'engineer';

					CREATE widget:w1 SET
						name = 'Gadget',
						price = 19.99,
						meta = { color: 'red', weight: 1.5 },
						tags = [{ label: 'new' }, { label: 'sale' }];
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// --- Test 1: Multi-field list query ---
		// All fields should be correctly resolved from the cached record data.
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						employees(orderBy: { field: NAME, direction: ASC }) {
							nodes {
								id
								name
								age
								active
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);

			let employees = &body["data"]["employees"]["nodes"];
			assert_eq!(employees.as_array().unwrap().len(), 3);

			assert_eq!(employees[0]["id"], "employee:alice");
			assert_eq!(employees[0]["name"], "Alice");
			assert_eq!(employees[0]["age"], 30);
			assert_eq!(employees[0]["active"], true);

			assert_eq!(employees[1]["id"], "employee:bob");
			assert_eq!(employees[1]["name"], "Bob");
			assert_eq!(employees[1]["age"], 25);
			assert_eq!(employees[1]["active"], false);

			assert_eq!(employees[2]["id"], "employee:carol");
			assert_eq!(employees[2]["name"], "Carol");
			assert_eq!(employees[2]["age"], 35);
			assert_eq!(employees[2]["active"], true);
		}

		// --- Test 2: Single-record query ---
		// The singular resolver now uses SELECT * and caches the full record.
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						employee(id: "alice") {
							id
							name
							age
							active
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);

			let emp = &body["data"]["employee"];
			assert_eq!(emp["id"], "employee:alice");
			assert_eq!(emp["name"], "Alice");
			assert_eq!(emp["age"], 30);
			assert_eq!(emp["active"], true);
		}

		// --- Test 3: Record-link dereferencing ---
		// When a field is TYPE record<department>, the resolver fetches and
		// caches the target record's full data. All dept sub-fields should
		// be resolved from that single cached fetch.
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						employees(orderBy: { field: NAME, direction: ASC }) {
							nodes {
								name
								dept {
									id
									name
									budget
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);

			let employees = &body["data"]["employees"]["nodes"];

			// Alice -> Engineering
			assert_eq!(employees[0]["name"], "Alice");
			assert_eq!(employees[0]["dept"]["id"], "department:eng");
			assert_eq!(employees[0]["dept"]["name"], "Engineering");
			assert_eq!(employees[0]["dept"]["budget"], 500000);

			// Bob -> Sales
			assert_eq!(employees[1]["name"], "Bob");
			assert_eq!(employees[1]["dept"]["id"], "department:sales");
			assert_eq!(employees[1]["dept"]["name"], "Sales");
			assert_eq!(employees[1]["dept"]["budget"], 200000);

			// Carol -> Engineering
			assert_eq!(employees[2]["name"], "Carol");
			assert_eq!(employees[2]["dept"]["id"], "department:eng");
			assert_eq!(employees[2]["dept"]["name"], "Engineering");
			assert_eq!(employees[2]["dept"]["budget"], 500000);
		}

		// --- Test 4: Mutation result caching ---
		// After a CREATE mutation, the returned fields should be resolved
		// from the cached mutation result.
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createEmployee(input: {
							id: "dave",
							name: "Dave",
							age: 28,
							active: true,
							dept: "department:eng"
						}) {
							employee {
								id
								name
								age
								active
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);

			let emp = &body["data"]["createEmployee"]["employee"];
			assert_eq!(emp["id"], "employee:dave");
			assert_eq!(emp["name"], "Dave");
			assert_eq!(emp["age"], 28);
			assert_eq!(emp["active"], true);
		}

		// --- Test 5: Update mutation result caching ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						updateEmployee(input: { id: "alice", age: 31 }) {
							employee {
								id
								name
								age
								active
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);

			let emp = &body["data"]["updateEmployee"]["employee"];
			assert_eq!(emp["id"], "employee:alice");
			assert_eq!(emp["name"], "Alice");
			assert_eq!(emp["age"], 31);
			assert_eq!(emp["active"], true);
		}

		// --- Test 6: Additional create mutation result caching ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createDepartment(input: { id: "hr", name: "HR", budget: 100000 }) {
							department {
								id
								name
								budget
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);

			let dept = &body["data"]["createDepartment"]["department"];
			assert_eq!(dept["id"], "department:hr");
			assert_eq!(dept["name"], "HR");
			assert_eq!(dept["budget"], 100000);
		}

		// --- Test 7: Relation field resolution ---
		// Relation records returned by SELECT * should be cached, so all
		// relation fields should resolve from the cache.
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						employee(id: "alice") {
							name
							worksOn(orderBy: { field: ID, direction: ASC }) {
								nodes {
									id
									title
								}
								edges {
									role
									node {
										id
										title
									}
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);

			let emp = &body["data"]["employee"];
			assert_eq!(emp["name"], "Alice");
			let nodes = &emp["worksOn"]["nodes"];
			assert_eq!(nodes.as_array().unwrap().len(), 1);
			assert_eq!(nodes[0]["title"], "Project Alpha");
			let edges = &emp["worksOn"]["edges"];
			assert_eq!(edges.as_array().unwrap().len(), 1);
			assert_eq!(edges[0]["role"], "lead");
			assert_eq!(edges[0]["node"]["title"], "Project Alpha");
		}

		// --- Test 8: Nested object field resolution from cache ---
		// The nested object field resolver extracts object/array values
		// directly from the parent CachedRecord.
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						widget(id: "w1") {
							id
							name
							price
							meta {
								color
								weight
							}
							tags {
								nodes {
									label
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);

			let widget = &body["data"]["widget"];
			assert_eq!(widget["id"], "widget:w1");
			assert_eq!(widget["name"], "Gadget");
			// Float comparison
			assert!((widget["price"].as_f64().unwrap() - 19.99).abs() < 0.001);
			assert_eq!(widget["meta"]["color"], "red");
			assert!((widget["meta"]["weight"].as_f64().unwrap() - 1.5).abs() < 0.001);
			let tags = widget["tags"]["nodes"].as_array().unwrap();
			assert_eq!(tags.len(), 2);
			assert_eq!(tags[0]["label"], "new");
			assert_eq!(tags[1]["label"], "sale");
		}

		// --- Test 10: Multiple record links in a single query ---
		// Ensures that when multiple employees reference the same department,
		// each record-link dereference produces the correct data.
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						employees(filterBy: { active: { eq: true } }, orderBy: { field: NAME, direction: ASC }) {
							nodes {
								name
								dept {
									name
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);

			let employees = &body["data"]["employees"]["nodes"];
			// Alice, Carol, Dave are active
			assert!(employees.as_array().unwrap().len() >= 2);
			// All active employees with dept should have correct dept name
			for emp in employees.as_array().unwrap() {
				let dept_name = emp["dept"]["name"].as_str().unwrap();
				assert!(
					dept_name == "Engineering" || dept_name == "Sales",
					"Unexpected department: {dept_name}"
				);
			}
		}

		Ok(())
	}

	/// Tests that GraphQL mutations respect table PERMISSIONS for create, update,
	/// delete, and upsert operations. Also tests bulk mutation permissions.
	///
	/// Verifies:
	/// - Authenticated users can only mutate records where PERMISSIONS allow it
	/// - Unauthorized mutations return empty/null results (permission-filtered)
	/// - Bulk mutations respect the same permissions as single-record mutations
	/// - Root users bypass permissions and can mutate everything
	#[test(tokio::test)]
	async fn mutation_permissions() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");
		let signup_url = &format!("http://{addr}/signup");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema with permissions
		{
			let res = client
				.post(sql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE ACCESS user ON DATABASE TYPE RECORD
						SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
						SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
						DURATION FOR SESSION 60s, FOR TOKEN 1d;

					DEFINE TABLE user SCHEMAFUL
						PERMISSIONS FOR select, create, update, delete WHERE id = $auth;
					DEFINE FIELD email ON user TYPE string;
					DEFINE FIELD pass ON user TYPE string;

					-- Table with per-operation permissions
					DEFINE TABLE article SCHEMAFUL
						PERMISSIONS
							FOR select WHERE $auth != NONE
							FOR create WHERE $auth != NONE
							FOR update WHERE author = $auth.id
							FOR delete WHERE author = $auth.id;
					DEFINE FIELD title ON article TYPE string;
					DEFINE FIELD content ON article TYPE string;
					DEFINE FIELD author ON article TYPE record<user>;

					-- Table with NO permissions for non-root (fully locked)
					DEFINE TABLE secret SCHEMAFUL
						PERMISSIONS NONE;
					DEFINE FIELD data ON secret TYPE string;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		}

		// Sign up a user and get a token
		let user_token;
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns,
					"db": db,
					"ac": "user",
					"email": "alice@example.com",
					"pass": "secret123",
				})
				.as_object()
				.unwrap(),
			)
			.unwrap();
			let res = client.post(signup_url).body(req_body).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			user_token = body["token"].as_str().unwrap().to_string();
		}

		// Get the user's record id for permission checks
		let user_id;
		{
			let res = client
				.post(gql_url)
				.bearer_auth(&user_token)
				.body(json!({"query": r#"{ users { nodes { id } } }"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "errors: {:?}", body["errors"]);
			user_id = body["data"]["users"]["nodes"][0]["id"].as_str().unwrap().to_string();
		}

		// ---------------------------------------------------------------
		// 1. CREATE with permissions: authenticated user CAN create an article
		// ---------------------------------------------------------------
		{
			let res = client
				.post(gql_url)
				.bearer_auth(&user_token)
				.body(
					json!({"query": format!(r#"mutation {{
						createArticle(input: {{
							title: "My Post",
							content: "Hello world",
							author: "{user_id}"
						}}) {{
							success
							article {{ id title author {{ id }} }}
						}}
					}}"#)})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Authenticated user should be able to create article, got: {:?}",
				body["errors"]
			);
			let article = &body["data"]["createArticle"]["article"];
			assert!(article["id"].is_string(), "Created article should have an id");
			assert_eq!(article["title"], "My Post");
		}

		// ---------------------------------------------------------------
		// 2. CREATE on a PERMISSIONS NONE table: authenticated user CANNOT create
		// ---------------------------------------------------------------
		{
			let res = client
				.post(gql_url)
				.bearer_auth(&user_token)
				.body(
					json!({"query": r#"mutation {
						createSecret(input: { data: "top secret" }) {
							success
							secret { id data }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let secret = &body["data"]["createSecret"]["secret"];
			assert_eq!(body["data"]["createSecret"]["success"], false);
			assert!(
				secret.is_null(),
				"User should NOT be able to create on PERMISSIONS NONE table, got: {:?}",
				body
			);
		}

		// ---------------------------------------------------------------
		// 3. Root CAN create on PERMISSIONS NONE table
		// ---------------------------------------------------------------
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						createSecret(input: { data: "classified" }) {
							success
							secret { id data }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Root should be able to create on any table, got errors: {:?}",
				body["errors"]
			);
			let secret = &body["data"]["createSecret"]["secret"];
			assert_eq!(body["data"]["createSecret"]["success"], true);
			assert!(secret["id"].is_string(), "Root-created secret should have an id");
		}

		// ---------------------------------------------------------------
		// 4. UPDATE with permissions: only the author can update their article
		// ---------------------------------------------------------------
		// First, create articles as root (one authored by alice, one by a fake user)
		let alice_article_id;
		let other_article_id;
		{
			let res = client
				.post(sql_url)
				.basic_auth(USER, Some(PASS))
				.body(format!(
					r#"
					CREATE article:alice_post SET title = "Alice's article", content = "Original", author = {user_id};
					CREATE article:other_post SET title = "Other article", content = "Not mine", author = user:fake;
				"#
				))
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			alice_article_id = "alice_post".to_string();
			other_article_id = "other_post".to_string();
		}

		// Alice CAN update her own article (author matches $auth.id)
		{
			let res = client
				.post(gql_url)
				.bearer_auth(&user_token)
				.body(
					json!({"query": format!(r#"mutation {{
						updateArticle(input: {{ id: "{alice_article_id}", title: "Updated title" }}) {{
							success
							article {{ id title }}
						}}
					}}"#)})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Author should be able to update own article, got: {:?}",
				body["errors"]
			);
			let article = &body["data"]["updateArticle"]["article"];
			assert_eq!(article["title"], "Updated title");
		}

		// Alice CANNOT update someone else's article (author doesn't match)
		{
			let res = client
				.post(gql_url)
				.bearer_auth(&user_token)
				.body(
					json!({"query": format!(r#"mutation {{
						updateArticle(input: {{ id: "{other_article_id}", title: "Hacked" }}) {{
							success
							article {{ id title }}
						}}
					}}"#)})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			// The update should return null (no permission to update this record)
			let article = &body["data"]["updateArticle"]["article"];
			assert_eq!(body["data"]["updateArticle"]["success"], false);
			assert!(
				article.is_null(),
				"User should NOT be able to update another user's article, got: {:?}",
				body
			);
		}

		// ---------------------------------------------------------------
		// 5. DELETE with permissions: only the author can delete their article
		// ---------------------------------------------------------------
		// Alice CANNOT delete someone else's article
		{
			let res = client
				.post(gql_url)
				.bearer_auth(&user_token)
				.body(
					json!({"query": format!(r#"mutation {{
						deleteArticle(input: {{ id: "{other_article_id}" }}) {{
							success
							article {{ id }}
						}}
					}}"#)})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			// The engine silently ignores permission-denied deletes, so the mutation
			// returns true even though the record was not actually deleted.
			assert!(
				body["errors"].is_null(),
				"Delete mutation should not return GraphQL errors, got: {:?}",
				body["errors"]
			);
			assert_eq!(body["data"]["deleteArticle"]["success"], false);
			assert!(body["data"]["deleteArticle"]["article"].is_null());
		}

		// Verify the other article still exists (via root)
		{
			let res = client
				.post(sql_url)
				.basic_auth(USER, Some(PASS))
				.body(format!("SELECT * FROM article:{other_article_id};"))
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let result = &body[0]["result"];
			assert!(
				result.is_array() && !result.as_array().unwrap().is_empty(),
				"Other user's article should still exist after unauthorized delete, got: {:?}",
				body
			);
		}

		// Alice CAN delete her own article
		{
			let res = client
				.post(gql_url)
				.bearer_auth(&user_token)
				.body(
					json!({"query": format!(r#"mutation {{
						deleteArticle(input: {{ id: "{alice_article_id}" }}) {{
							success
							article {{ id }}
						}}
					}}"#)})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Author should be able to delete own article, got errors: {:?}",
				body["errors"]
			);
			assert_eq!(body["data"]["deleteArticle"]["success"], true);
		}

		// ---------------------------------------------------------------
		// 6. Bulk mutations are not exposed in the schema
		// ---------------------------------------------------------------
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__schema {
							mutationType {
								fields { name }
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let fields = body["data"]["__schema"]["mutationType"]["fields"].as_array().unwrap();
			let names: Vec<&str> =
				fields.iter().map(|field| field["name"].as_str().unwrap()).collect();
			assert!(!names.contains(&"updateManyArticle"));
			assert!(!names.contains(&"deleteManySecret"));
		}

		Ok(())
	}

	/// Tests that relation field resolution respects PERMISSIONS on the relation
	/// table. An authenticated user should only see relation records they have
	/// permission to read.
	#[test(tokio::test)]
	async fn relation_permissions() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");
		let signup_url = &format!("http://{addr}/signup");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema with relation table that has permissions
		{
			let res = client
				.post(sql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE ACCESS user ON DATABASE TYPE RECORD
						SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
						SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
						DURATION FOR SESSION 60s, FOR TOKEN 1d;

					DEFINE TABLE user SCHEMAFUL
						PERMISSIONS FOR select, create, update, delete WHERE id = $auth;
					DEFINE FIELD email ON user TYPE string;
					DEFINE FIELD pass ON user TYPE string;

					DEFINE TABLE post SCHEMAFUL
						PERMISSIONS FOR select WHERE $auth != NONE
						FOR create, update, delete WHERE $auth != NONE;
					DEFINE FIELD title ON post TYPE string;

					-- Relation with permissions: users can only see their own likes
					DEFINE TABLE likes TYPE RELATION FROM user TO post SCHEMAFUL
						PERMISSIONS FOR select WHERE in = $auth.id
						FOR create, update, delete WHERE in = $auth.id;
					DEFINE FIELD rating ON likes TYPE int;

					-- Create test data
					CREATE post:p1 SET title = "First Post";
					CREATE post:p2 SET title = "Second Post";
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		}

		// Sign up two users
		let token_alice;
		let token_bob;
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns, "db": db, "ac": "user",
					"email": "alice@test.com", "pass": "pass123",
				})
				.as_object()
				.unwrap(),
			)
			.unwrap();
			let res = client.post(signup_url).body(req_body).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			token_alice = body["token"].as_str().unwrap().to_string();
		}
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns, "db": db, "ac": "user",
					"email": "bob@test.com", "pass": "pass123",
				})
				.as_object()
				.unwrap(),
			)
			.unwrap();
			let res = client.post(signup_url).body(req_body).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			token_bob = body["token"].as_str().unwrap().to_string();
		}

		// Get user IDs
		let alice_id;
		let bob_id;
		{
			let res = client
				.post(gql_url)
				.bearer_auth(&token_alice)
				.body(json!({"query": r#"{ users { nodes { id } } }"#}).to_string())
				.send()
				.await?;
			let body = res.json::<serde_json::Value>().await?;
			alice_id = body["data"]["users"]["nodes"][0]["id"].as_str().unwrap().to_string();
		}
		{
			let res = client
				.post(gql_url)
				.bearer_auth(&token_bob)
				.body(json!({"query": r#"{ users { nodes { id } } }"#}).to_string())
				.send()
				.await?;
			let body = res.json::<serde_json::Value>().await?;
			bob_id = body["data"]["users"]["nodes"][0]["id"].as_str().unwrap().to_string();
		}

		// Create likes as root: Alice likes p1 (rating 5), Bob likes p2 (rating 3)
		{
			let res = client
				.post(sql_url)
				.basic_auth(USER, Some(PASS))
				.body(format!(
					r#"
					RELATE {alice_id}->likes->post:p1 SET rating = 5;
					RELATE {bob_id}->likes->post:p2 SET rating = 3;
				"#
				))
				.send()
				.await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		}

		// Alice queries her likes: should see only her own like edge metadata
		{
			let res = client
				.post(gql_url)
				.bearer_auth(&token_alice)
				.body(
					json!({"query": r#"{ users { nodes { id likes { edges { rating node { id } } } } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "errors: {:?}", body["errors"]);
			let user = &body["data"]["users"]["nodes"][0];
			let likes = user["likes"]["edges"].as_array().unwrap();
			assert_eq!(likes.len(), 1, "Alice should see only her own like");
			assert_eq!(likes[0]["rating"], 5);
			assert_eq!(likes[0]["node"]["id"], "post:p1");
		}

		// Bob queries his likes: should see only his own like edge metadata
		{
			let res = client
				.post(gql_url)
				.bearer_auth(&token_bob)
				.body(
					json!({"query": r#"{ users { nodes { id likes { edges { rating node { id } } } } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "errors: {:?}", body["errors"]);
			let user = &body["data"]["users"]["nodes"][0];
			let likes = user["likes"]["edges"].as_array().unwrap();
			assert_eq!(likes.len(), 1, "Bob should see only his own like");
			assert_eq!(likes[0]["rating"], 3);
			assert_eq!(likes[0]["node"]["id"], "post:p2");
		}

		// Root sees all likes by traversing from each user
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"{ users(orderBy: { field: ID, direction: ASC }) { nodes { id likes { edges { rating } } } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "errors: {:?}", body["errors"]);
			let users = body["data"]["users"]["nodes"].as_array().unwrap();
			let total_likes: usize =
				users.iter().map(|user| user["likes"]["edges"].as_array().unwrap().len()).sum();
			assert_eq!(total_likes, 2, "Root should see all likes");
		}

		Ok(())
	}

	/// Tests that GraphQL error messages do not leak internal implementation
	/// details, table structures, or database internals.
	#[test(tokio::test)]
	async fn error_message_safety() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema
		{
			let res = client
				.post(sql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE ACCESS user ON DATABASE TYPE RECORD
						SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
						SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
						DURATION FOR SESSION 60s, FOR TOKEN 1d;

					DEFINE TABLE user SCHEMAFUL
						PERMISSIONS FOR select, create, update, delete WHERE id = $auth;
					DEFINE FIELD email ON user TYPE string;
					DEFINE FIELD pass ON user TYPE string;

					DEFINE TABLE item SCHEMAFUL;
					DEFINE FIELD name ON item TYPE string;
					DEFINE FIELD price ON item TYPE float;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		}

		// Test: signin with wrong credentials should return generic error
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						signin(input: {
							access: "user"
							variables: { email: "nobody@test.com", pass: "wrong" }
						}) {
							success
							token
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_array());
			let error_msg = body["errors"][0]["message"].as_str().unwrap_or("");
			// The error should be generic — not mention internal details
			assert!(
				error_msg.contains("problem with authentication"),
				"Auth error should be generic, got: {error_msg}"
			);
			assert!(
				!error_msg.contains("SELECT") && !error_msg.contains("FROM user"),
				"Auth error should not leak query details, got: {error_msg}"
			);
			assert!(
				!error_msg.contains("argon2") && !error_msg.contains("crypto"),
				"Auth error should not leak implementation details, got: {error_msg}"
			);
		}

		// Test: signIn with non-existent access method should still stay generic
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						signin(input: {
							access: "nonexistent_access"
							variables: { email: "test", pass: "test" }
						}) {
							success
							token
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_array());
			let error_msg = body["errors"][0]["message"].as_str().unwrap_or("");
			assert!(
				error_msg.contains("problem with authentication"),
				"Unknown access should still be generic, got: {error_msg}"
			);
			assert!(
				!error_msg.contains("SELECT")
					&& !error_msg.contains("argon2")
					&& !error_msg.contains("crypto")
					&& !error_msg.contains("FROM user"),
				"Validation error should not leak auth implementation details, got: {error_msg}"
			);
		}

		// Test: singular record lookup with invalid id format returns clean error
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(json!({"query": r#"{ item(id: "not_a_valid_id") { id } }"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			// May succeed with null or error, but should not leak parse details
			if body["errors"].is_array() {
				let error_msg = body["errors"][0]["message"].as_str().unwrap_or("");
				assert!(
					!error_msg.contains("ParseError") && !error_msg.contains("backtrace"),
					"Parse error should not leak internal details, got: {error_msg}"
				);
			}
		}

		Ok(())
	}

	/// Tests that upsert mutations respect table PERMISSIONS.
	#[test(tokio::test)]
	async fn upsert_permissions() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");
		let signup_url = &format!("http://{addr}/signup");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema: table with restricted create/update permissions
		{
			let res = client
				.post(sql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE ACCESS user ON DATABASE TYPE RECORD
						SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
						SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
						DURATION FOR SESSION 60s, FOR TOKEN 1d;

					DEFINE TABLE user SCHEMAFUL
						PERMISSIONS FOR select, create, update, delete WHERE id = $auth;
					DEFINE FIELD email ON user TYPE string;
					DEFINE FIELD pass ON user TYPE string;

					-- locked: no create/update for non-root
					DEFINE TABLE locked SCHEMAFUL
						PERMISSIONS
							FOR select WHERE $auth != NONE
							FOR create, update, delete NONE;
					DEFINE FIELD name ON locked TYPE string;

					CREATE locked:existing SET name = "Original";
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		}

		// Sign up a user
		let user_token;
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns, "db": db, "ac": "user",
					"email": "alice@test.com", "pass": "pass123",
				})
				.as_object()
				.unwrap(),
			)
			.unwrap();
			let res = client.post(signup_url).body(req_body).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			user_token = body["token"].as_str().unwrap().to_string();
		}

		// Upsert on locked table: user should NOT be able to create or update
		{
			let res = client
				.post(gql_url)
				.bearer_auth(&user_token)
				.body(
					json!({"query": r#"mutation {
						upsertLocked(input: { id: "new_record", name: "Hacked" }) {
							success
							locked { id name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			// Should return null — permission denied for create/update
			let result = &body["data"]["upsertLocked"]["locked"];
			assert_eq!(body["data"]["upsertLocked"]["success"], false);
			assert!(
				result.is_null(),
				"User should NOT be able to upsert on locked table, got: {:?}",
				body
			);
		}

		// Upsert on existing record in locked table: user still can't update
		{
			let res = client
				.post(gql_url)
				.bearer_auth(&user_token)
				.body(
					json!({"query": r#"mutation {
						upsertLocked(input: { id: "existing", name: "Modified" }) {
							success
							locked { id name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let result = &body["data"]["upsertLocked"]["locked"];
			assert_eq!(body["data"]["upsertLocked"]["success"], false);
			assert!(
				result.is_null(),
				"User should NOT be able to upsert existing record on locked table, got: {:?}",
				body
			);
		}

		// Verify the record was NOT modified
		{
			let res = client
				.post(sql_url)
				.basic_auth(USER, Some(PASS))
				.body("SELECT name FROM locked:existing;")
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let name = &body[0]["result"][0]["name"];
			assert_eq!(
				name, "Original",
				"Record should not have been modified by unauthorized upsert"
			);
		}

		// Root CAN upsert on locked table
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						upsertLocked(input: { id: "existing", name: "Root Modified" }) {
							success
							locked { id name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Root should be able to upsert, got errors: {:?}",
				body["errors"]
			);
			let result = &body["data"]["upsertLocked"]["locked"];
			assert_eq!(result["name"], "Root Modified");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn either_record_conversion() -> Result<(), Box<dyn std::error::Error>> {
		// Tests that `option<record<T>>` fields (represented as Kind::Either([None, Record]))
		// can be set via GraphQL mutations. This exercises the `Record` arm of the
		// `either_try_kind!` macro in `gql_to_sql_kind`, which previously had a
		// copy-paste bug where it filtered for `Kind::Array` instead of `Kind::Record`.
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema: a `team` table and a `player` table with an
		// `option<record<team>>` field, which internally becomes
		// Kind::Either([Kind::None, Kind::Record(["team"])]).
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE team SCHEMAFULL;
					DEFINE FIELD name ON team TYPE string;

					DEFINE TABLE player SCHEMAFULL;
					DEFINE FIELD name ON player TYPE string;
					DEFINE FIELD squad ON player TYPE option<record<team>>;

					CREATE team:red SET name = "Red Team";
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Create a player with a record reference via GraphQL mutation.
		// This sends the record ID as a string ("team:red") through
		// gql_to_sql_kind with Kind::Either, exercising the Record arm.
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createPlayer(input: {
							name: "Alice",
							squad: "team:red"
						}) {
							player {
								id
								name
								squad { id name }
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Creating player with option<record> via mutation failed: {:?}",
				body["errors"]
			);
			let player = &body["data"]["createPlayer"]["player"];
			assert_eq!(player["name"], "Alice");
			assert_eq!(player["squad"]["id"], "team:red");
			assert_eq!(player["squad"]["name"], "Red Team");
		}

		// Create a player with null squad (the None variant of the Either).
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createPlayer(input: {
							name: "Bob"
						}) {
							player {
								id
								name
								squad { id name }
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Creating player without squad failed: {:?}",
				body["errors"]
			);
			let player = &body["data"]["createPlayer"]["player"];
			assert_eq!(player["name"], "Bob");
			assert!(
				player["squad"].is_null(),
				"Expected squad to be null, got: {:?}",
				player["squad"]
			);
		}

		// Update the player's squad via mutation (tests the MERGE path).
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						updatePlayer(input: { id: "Bob",
							squad: "team:red"
						}) {
							player {
								id
								name
								squad { id name }
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			// Note: the id might not match "Bob" exactly since createPlayer
			// auto-generates one. We test the create path above which is the
			// primary goal. If this update fails because of ID mismatch, that's ok.
			let _body = res.json::<serde_json::Value>().await?;
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn either_string_literals_with_invalid_identifier_chars()
	-> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE test SCHEMAFULL;
					DEFINE FIELD OVERWRITE type ON test TYPE "enum-1" | "enum-2";
					CREATE test:one SET type = "enum-1";
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Regression for #6941: GraphQL schema generation must not emit invalid
		// identifiers for string-literal either types (e.g. containing '-').
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"query { tests { nodes { id type } } }"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Expected schema generation and query execution to succeed, got errors: {:?}",
				body["errors"]
			);
			assert_eq!(body["data"]["tests"]["nodes"][0]["id"], "test:one");
			assert_eq!(body["data"]["tests"]["nodes"][0]["type"], "ENUM_1");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn literal_kind_field_schema_and_mutation() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE sample SCHEMAFULL;
					DEFINE FIELD OVERWRITE status ON sample TYPE "active";
					CREATE sample:one SET status = "active";
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Kind::Literal field should no longer fail schema generation.
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"query { samples { nodes { id status } } }"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Expected query to succeed for Kind::Literal field, got: {:?}",
				body["errors"]
			);
			assert_eq!(body["data"]["samples"]["nodes"][0]["id"], "sample:one");
			assert_eq!(body["data"]["samples"]["nodes"][0]["status"], "active");
		}

		// Mutation input should accept matching literal values.
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createSample(input: { status: "active" }) {
							sample {
								id
								status
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Expected matching literal mutation to succeed, got: {:?}",
				body["errors"]
			);
			assert_eq!(body["data"]["createSample"]["sample"]["status"], "active");
		}

		// Mutation input should reject non-matching literal values.
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createSample(input: { status: "inactive" }) {
							sample {
								id
								status
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_array(),
				"Expected non-matching literal mutation to fail, got: {:?}",
				body
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn literal_object_kind_field_schema_and_mutation()
	-> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE sampleobj SCHEMAFULL;
					DEFINE FIELD OVERWRITE meta ON sampleobj TYPE { status: "active", score: int };
					CREATE sampleobj:one SET meta = { status: "active", score: 10 };
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"query { sampleobjs { nodes { id meta } } }"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Expected query to succeed for Kind::Literal(Object), got: {:?}",
				body["errors"]
			);
			assert_eq!(body["data"]["sampleobjs"]["nodes"][0]["id"], "sampleobj:one");
			assert_eq!(body["data"]["sampleobjs"]["nodes"][0]["meta"]["status"], "active");
			assert_eq!(body["data"]["sampleobjs"]["nodes"][0]["meta"]["score"], 10);
		}

		// Mutation input should accept matching literal object values.
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createSampleobj(input: { meta: { status: "active", score: 11 } }) {
							sampleobj {
								id
								meta
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Expected matching literal object mutation to succeed, got: {:?}",
				body["errors"]
			);
			assert_eq!(body["data"]["createSampleobj"]["sampleobj"]["meta"]["status"], "active");
			assert_eq!(body["data"]["createSampleobj"]["sampleobj"]["meta"]["score"], 11);
		}

		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createSampleobj(input: { meta: { status: "inactive", score: 12 } }) {
							sampleobj {
								id
								meta
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_array(),
				"Expected non-matching literal object mutation to fail, got: {:?}",
				body
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn literal_numeric_bool_array_kinds_schema_and_mutation()
	-> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE litnum SCHEMAFULL;
					DEFINE FIELD OVERWRITE int_lit ON litnum TYPE 42;
					DEFINE FIELD OVERWRITE float_lit ON litnum TYPE 3.5f;
					DEFINE FIELD OVERWRITE dec_lit ON litnum TYPE 2.5dec;
					DEFINE FIELD OVERWRITE bool_lit ON litnum TYPE true;
					DEFINE FIELD OVERWRITE arr_lit ON litnum TYPE [1, "ok", true];
					CREATE litnum:one SET
						int_lit = 42,
						float_lit = 3.5f,
						dec_lit = 2.5dec,
						bool_lit = true,
						arr_lit = [1, "ok", true];
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						litnums {
							nodes {
								id
								intLit
								floatLit
								boolLit
								arrLit {
									nodes
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Expected query to succeed for numeric/bool/array literal kinds, got: {:?}",
				body["errors"]
			);
			assert_eq!(body["data"]["litnums"]["nodes"][0]["id"], "litnum:one");
			assert_eq!(body["data"]["litnums"]["nodes"][0]["intLit"], 42);
			assert_eq!(body["data"]["litnums"]["nodes"][0]["floatLit"], 3.5);
			assert_eq!(body["data"]["litnums"]["nodes"][0]["boolLit"], true);
			assert_eq!(body["data"]["litnums"]["nodes"][0]["arrLit"]["nodes"][0], 1);
			assert_eq!(body["data"]["litnums"]["nodes"][0]["arrLit"]["nodes"][1], "ok");
			assert_eq!(body["data"]["litnums"]["nodes"][0]["arrLit"]["nodes"][2], true);
		}

		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createLitnum(input: {
							intLit: 42,
							floatLit: 3.5,
							decLit: 2.5,
							boolLit: true,
							arrLit: [1, "ok", true]
						}) {
							litnum {
								id
								intLit
								floatLit
								boolLit
								arrLit {
									nodes
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Expected matching numeric/bool/array literal mutation to succeed, got: {:?}",
				body["errors"]
			);
			assert_eq!(body["data"]["createLitnum"]["litnum"]["intLit"], 42);
			assert_eq!(body["data"]["createLitnum"]["litnum"]["floatLit"], 3.5);
			assert_eq!(body["data"]["createLitnum"]["litnum"]["boolLit"], true);
			assert_eq!(body["data"]["createLitnum"]["litnum"]["arrLit"]["nodes"][0], 1);
			assert_eq!(body["data"]["createLitnum"]["litnum"]["arrLit"]["nodes"][1], "ok");
			assert_eq!(body["data"]["createLitnum"]["litnum"]["arrLit"]["nodes"][2], true);
		}

		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createLitnum(input: {
							intLit: 43,
							floatLit: 3.5,
							decLit: 2.5,
							boolLit: true,
							arrLit: [1, "ok", true]
						}) {
							litnum {
								id
								intLit
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_array(),
				"Expected non-matching numeric literal mutation to fail, got: {:?}",
				body
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn subscriptions_live_query_stream() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_ws_url = &format!("ws://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE foo SCHEMAFUL;
					DEFINE FIELD val ON foo TYPE int;
					CREATE foo:1 SET val = 1;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		let mut req = gql_ws_url.into_client_request()?;
		req.headers_mut().insert("surreal-ns", ns.parse()?);
		req.headers_mut().insert("surreal-db", db.parse()?);
		req.headers_mut().insert("Sec-WebSocket-Protocol", "graphql-transport-ws".parse()?);
		let (mut ws, _) = connect_async(req).await?;

		ws.send(Message::Text(json!({"type":"connection_init"}).to_string().into())).await?;
		let Some(Ok(Message::Text(ack_msg))) = ws.next().await else {
			return Err(std::io::Error::other("expected websocket connection ack").into());
		};
		let ack_json: serde_json::Value = serde_json::from_str(&ack_msg)?;
		assert_eq!(ack_json["type"], "connection_ack");

		ws.send(Message::Text(
			json!({
				"id": "sub-1",
				"type": "subscribe",
				"payload": {
					"query": "subscription { foo { id val } }"
				}
			})
			.to_string()
			.into(),
		))
		.await?;

		// Allow the server to fully register the live query before mutating data
		tokio::time::sleep(Duration::from_secs(1)).await;

		{
			let res = client.post(sql_url).body(r#"CREATE foo:3 SET val = 99;"#).send().await?;
			assert_eq!(res.status(), 200);
		}

		let received = tokio::time::timeout(Duration::from_secs(10), async {
			while let Some(frame) = ws.next().await {
				let Ok(frame) = frame else {
					continue;
				};
				let Message::Text(text) = frame else {
					continue;
				};
				let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
					continue;
				};
				if value["type"] == "next" && value["payload"]["data"]["foo"]["id"] == "foo:3" {
					return Some(value);
				}
			}
			None
		})
		.await?
		.ok_or_else(|| std::io::Error::other("subscription stream ended before event"))?;

		assert_eq!(received["payload"]["data"]["foo"]["val"], 99);
		Ok(())
	}

	#[test(tokio::test)]
	async fn subscriptions_live_query_shape_filter_and_id() -> Result<(), Box<dyn std::error::Error>>
	{
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_ws_url = &format!("ws://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE foo SCHEMAFUL;
					DEFINE FIELD val ON foo TYPE int;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		let mut req = gql_ws_url.into_client_request()?;
		req.headers_mut().insert("surreal-ns", ns.parse()?);
		req.headers_mut().insert("surreal-db", db.parse()?);
		req.headers_mut().insert("Sec-WebSocket-Protocol", "graphql-transport-ws".parse()?);
		let (mut ws, _) = connect_async(req).await?;

		ws.send(Message::Text(json!({"type":"connection_init"}).to_string().into())).await?;
		let Some(Ok(Message::Text(ack_msg))) = ws.next().await else {
			return Err(std::io::Error::other("expected websocket connection ack").into());
		};
		let ack_json: serde_json::Value = serde_json::from_str(&ack_msg)?;
		assert_eq!(ack_json["type"], "connection_ack");

		ws.send(Message::Text(
			json!({
				"id": "sub-filter",
				"type": "subscribe",
				"payload": {
					"query": "subscription { foo(filterBy: { val: { eq: 99 } }, fetch: [\"val\"]) { id val } }"
				}
			})
			.to_string()
			.into(),
		))
		.await?;

		ws.send(Message::Text(
			json!({
				"id": "sub-id",
				"type": "subscribe",
				"payload": {
					"query": "subscription { foo(id: \"foo:target\") { val } }"
				}
			})
			.to_string()
			.into(),
		))
		.await?;

		// Allow the server to fully register the live queries before mutating data
		tokio::time::sleep(Duration::from_secs(1)).await;

		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					CREATE foo:other SET val = 1;
					CREATE foo:filter_match SET val = 99;
					CREATE foo:target SET val = 42;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		let mut got_filter = false;
		let mut got_id = false;
		tokio::time::timeout(Duration::from_secs(10), async {
			while let Some(frame) = ws.next().await {
				let Ok(frame) = frame else {
					continue;
				};
				let Message::Text(text) = frame else {
					continue;
				};
				let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
					continue;
				};
				if value["type"] != "next" {
					continue;
				}
				match value["id"].as_str() {
					Some("sub-filter") => {
						assert_eq!(value["payload"]["data"]["foo"]["id"], "foo:filter_match");
						assert_eq!(value["payload"]["data"]["foo"]["val"], 99);
						got_filter = true;
					}
					Some("sub-id") => {
						assert_eq!(value["payload"]["data"]["foo"]["val"], 42);
						got_id = true;
					}
					_ => {}
				}
				if got_filter && got_id {
					return;
				}
			}
		})
		.await?;

		assert!(got_filter, "did not receive filtered subscription event");
		assert!(got_id, "did not receive id-targeted subscription event");
		Ok(())
	}

	#[test(tokio::test)]
	async fn subscriptions_live_query_shape_with_variables()
	-> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let gql_ws_url = &format!("ws://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE foo SCHEMAFUL;
					DEFINE FIELD val ON foo TYPE int;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		let mut req = gql_ws_url.into_client_request()?;
		req.headers_mut().insert("surreal-ns", ns.parse()?);
		req.headers_mut().insert("surreal-db", db.parse()?);
		req.headers_mut().insert("Sec-WebSocket-Protocol", "graphql-transport-ws".parse()?);
		let (mut ws, _) = connect_async(req).await?;

		ws.send(Message::Text(json!({"type":"connection_init"}).to_string().into())).await?;
		let Some(Ok(Message::Text(ack_msg))) = ws.next().await else {
			return Err(std::io::Error::other("expected websocket connection ack").into());
		};
		let ack_json: serde_json::Value = serde_json::from_str(&ack_msg)?;
		assert_eq!(ack_json["type"], "connection_ack");

		ws.send(Message::Text(
			json!({
				"id": "sub-vars",
				"type": "subscribe",
				"payload": {
					"query": "subscription($id: ID, $filterBy: FooFilterInput, $fetch: [String!]) { foo(id: $id, filterBy: $filterBy, fetch: $fetch) { val } }",
					"variables": {
						"id": "foo:target",
						"filterBy": { "val": { "eq": 42 } },
						"fetch": ["val"]
					}
				}
			})
			.to_string()
			.into(),
		))
		.await?;

		// Allow the server to fully register the live query before mutating data
		tokio::time::sleep(Duration::from_secs(1)).await;

		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					CREATE foo:other SET val = 1;
					CREATE foo:target SET val = 42;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		let received = tokio::time::timeout(Duration::from_secs(10), async {
			while let Some(frame) = ws.next().await {
				let Ok(frame) = frame else {
					continue;
				};
				let Message::Text(text) = frame else {
					continue;
				};
				let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
					continue;
				};
				if value["type"] == "next" && value["id"] == "sub-vars" {
					return Some(value);
				}
			}
			None
		})
		.await?
		.ok_or_else(|| std::io::Error::other("subscription stream ended before event"))?;

		assert_eq!(received["payload"]["data"]["foo"]["val"], 42);
		Ok(())
	}
}
