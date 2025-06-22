use crate::dbs::Session;
use crate::gql::schema::generate_schema;
use crate::kvs::Datastore;
use async_graphql::dynamic::Schema;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// A singleton to hold the initialized database and GraphQL schema.
/// This is initialized only once for all tests in this file.
struct GqlSingleton {
    datastore: Arc<Datastore>,
    session: Session,
    schema: Schema,
}
static SCHEMA_SINGLETON: OnceCell<Arc<GqlSingleton>> = OnceCell::const_new();
static CURSOR_SINGLETON: OnceCell<Arc<GqlSingleton>> = OnceCell::const_new();
static NO_CURSOR_SINGLETON: OnceCell<Arc<GqlSingleton>> = OnceCell::const_new();

/// Gets or initializes the singleton. This ensures that the database
/// setup and schema generation runs only once.
async fn get_singleton(singleton: &'static OnceCell<Arc<GqlSingleton>>, ast: &'static str) ->
&'static Arc<GqlSingleton> {
    singleton
        .get_or_init(|| async {
            let datastore = Arc::new(Datastore::new("memory").await.unwrap());
            let session = Session::owner().with_ns("test").with_db("test");
            datastore.execute(ast, &session, None).await.unwrap();

            let schema = generate_schema(&datastore, &session).await.unwrap();

            Arc::new(GqlSingleton {
                datastore,
                session,
                schema,
            })
        })
        .await
}

// ========================================================
// TEST CASES
// ========================================================

mod schema_generation {
    use super::*;
    use async_graphql::registry::{MetaField, MetaType, Registry};
    use serde_json::json;

    const SCHEMA_AST: &str = r#"
        DEFINE TABLE book SCHEMAFULL TYPE NORMAL;
        DEFINE FIELD title                  ON book TYPE string;
        DEFINE FIELD is_published           ON book TYPE bool;
        DEFINE FIELD edition_id             ON book TYPE uuid;
        DEFINE FIELD page_count             ON book TYPE int;
        DEFINE FIELD rating                 ON book TYPE float;
        DEFINE FIELD price                  ON book TYPE decimal;
        DEFINE FIELD cover_image            ON book TYPE bytes;
        DEFINE FIELD publisher              ON book TYPE record<publisher>;
        DEFINE FIELD published_on           ON book TYPE datetime;
        DEFINE FIELD estimated_reading_time ON book TYPE duration;

        DEFINE TABLE publisher SCHEMAFULL TYPE NORMAL;
        DEFINE FIELD name ON publisher TYPE string;

        UPSERT publisher:oreilly SET name = "O'Reilly Media";

        UPSERT book:graphql_essentials SET
            title = "GraphQL: The Definitive Guide",
            is_published = true,
            edition_id = u"a1b2c3d4-e5f6-7890-1234-567890abcdef",
            page_count = 350,
            rating = 4.8,
            price = 49.99,
            cover_image = b"4920616D20736F6D65206279746573",
            publisher = publisher:oreilly,
            published_on = d"2022-10-25T00:00:00Z",
            estimated_reading_time = 8h30m
        ;

        DEFINE TABLE images SCHEMAFULL TYPE NORMAL; // User uses plural table name
        DEFINE TABLE aircraft SCHEMAFULL TYPE NORMAL; // User singular=plural table name

        DEFINE CONFIG GRAPHQL AUTO CURSOR;
    "#;

    fn find_type<'a>(registry: &'a Registry, name: &str) -> &'a MetaType {
        registry.types.get(name).unwrap_or_else(|| panic!("Type '{}' should exist in the registry", name))
    }

    fn find_field<'a>(
        meta_obj: &'a MetaType,
        field_name: &str,
    ) -> Option<&'a MetaField> {
        match meta_obj {
            MetaType::Object { fields, .. } => fields.get(field_name),
            _ => None,
        }
    }

    #[tokio::test]
    async fn test_all_required_types_are_generated() {
        let singleton = get_singleton(&SCHEMA_SINGLETON, SCHEMA_AST).await;
        let registry = singleton.schema.registry();

        let registered_type_names: Vec<_> = registry.types.keys().collect();
        trace!("registered types: {:?}", registered_type_names);

        // Standard Scalars
        assert!(registry.types.contains_key("String"));
        assert!(registry.types.contains_key("Boolean"));
        assert!(registry.types.contains_key("Int"));
        assert!(registry.types.contains_key("Float"));
        assert!(registry.types.contains_key("ID"));
        // Custom Scalars
        assert!(registry.types.contains_key("UUID"));
        assert!(registry.types.contains_key("Number"));
        assert!(registry.types.contains_key("DateTime"));
        assert!(registry.types.contains_key("Duration"));
        assert!(registry.types.contains_key("Record"));
        // Geometry Types
        assert!(registry.types.contains_key("Geometry"));
        assert!(registry.types.contains_key("GeometryType"));
        // assert!(registry.types.contains_key("Point")); // fixme: not working -> panics
        // assert!(registry.types.contains_key("LineString"));
        // assert!(registry.types.contains_key("Polygon"));
        // assert!(registry.types.contains_key("MultiPoint"));
        // assert!(registry.types.contains_key("MultiLineString"));
        // assert!(registry.types.contains_key("MultiPolygon"));
        // Connection Types
        assert!(registry.types.contains_key("PageInfo"));
        assert!(registry.types.contains_key("OrderDirection"));
    }

    #[tokio::test]
    async fn test_query_table_names() {
        let singleton = get_singleton(&SCHEMA_SINGLETON, SCHEMA_AST).await;
        let registry = singleton.schema.registry();

        let query_root = find_type(registry, "Query");

        assert!(
            find_field(query_root, "book").is_some(),
            "Query object should have a 'book' field for single record fetching."
        );
        assert!(
            find_field(query_root, "books").is_some(),
            "Query object should have a 'books' field for multiple record fetching."
        );
        assert!(
            find_field(query_root, "image").is_some(),
            "Query object should have a 'image' field for single record fetching."
        );
        assert!(
            find_field(query_root, "images").is_some(),
            "Query object should have a 'images' field for multiple record fetching."
        );
        assert!(
            find_field(query_root, "aircraft").is_some(),
            "Query object should have a 'aircraft' field for single record fetching."
        );
        assert!(
            find_field(query_root, "aircraftList").is_some(),
            "Query object should have a 'aircraft' field for multiple record fetching."
        );
    }

    #[tokio::test]
    async fn test_all_scalar_types_resolve_correctly() {
        let singleton = get_singleton(&SCHEMA_SINGLETON, SCHEMA_AST).await;

        let query = r#"
            query GetBookDetails {
              book(id: "book:graphql_essentials") {
                id
                title
                isPublished

                pageCount
                rating
                price
                coverImage
                publishedOn
                estimatedReadingTime
                publisher {
                  name
                }
              }
            }
        "#; // editionId

        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();
        let book = &json["book"];

        assert_eq!(book["id"], json!("book:graphql_essentials"));
        assert_eq!(book["title"], json!("GraphQL: The Definitive Guide"));
        assert_eq!(book["isPublished"], json!(true));
        // assert_eq!(book["editionId"], json!("a1b2c3d4-e5f6-7890-1234-567890abcdef")); //fixme:broken
        assert_eq!(book["pageCount"], json!(350));
        assert_eq!(book["rating"], json!(4.8));
        assert_eq!(book["price"], json!("49.99dec"));
        assert_eq!(book["coverImage"], json!([73,32,97,109,32,115,111,109,101,32,98,121,116,101,115]));
        assert_eq!(book["publishedOn"], json!("2022-10-25T00:00:00+00:00"));
        assert_eq!(book["estimatedReadingTime"], json!("8h30m"));
        assert_eq!(book["publisher"]["name"], json!("O'Reilly Media"));
    }

    #[tokio::test]
    async fn test_naming_conventions() {
        let singleton = get_singleton(&CURSOR_SINGLETON, SCHEMA_AST).await;
        let registry = singleton.schema.registry();

        let book_type = find_type(registry, "Book");

        let query = r#"
            query GetBookDetails {
              book(id: "book:graphql_essentials") {
                id
              }
            }
        "#;

        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert!(find_field(book_type, "title").is_some(), "Book type should have a 'title'.");
        assert!(find_field(book_type, "Title").is_none(), "Book type should not have a 'Title' field.");
        assert!(find_field(book_type, "isPublished").is_some(), "Book type should have a 'isPublished' field.");
        assert!(find_field(book_type, "is_published").is_none(), "Book type should not have a \
        'is_published' field.");
        assert_eq!(json["book"]["id"], json!("book:graphql_essentials"), "Book ID should remain unchanged.");
    }

    #[tokio::test]
    async fn test_connection_order_by() {
        let singleton = get_singleton(&SCHEMA_SINGLETON, SCHEMA_AST).await;
        let registry = singleton.schema.registry();

        let order_input_type = find_type(registry, "BookOrder");
        assert!(matches!(order_input_type, MetaType::InputObject { .. }), "BookOrder should be an InputObject");

        if let MetaType::InputObject { input_fields, .. } = order_input_type {
            let field_arg = input_fields.get("field").expect("BookOrder should have a 'field' argument");
            assert_eq!(field_arg.ty, "BookOrderField", "field argument should be of type BookOrderField");

            let direction_arg = input_fields.get("direction").expect("BookOrder should have a 'direction' argument");
            assert_eq!(direction_arg.ty, "OrderDirection", "direction argument should be of type OrderDirection");
        }

        let order_enum_type = find_type(registry, "BookOrderField");
        assert!(matches!(order_enum_type, MetaType::Enum { .. }), "BookOrderField should be an Enum");

        if let MetaType::Enum { enum_values, .. } = order_enum_type {
            assert!(enum_values.contains_key("ID"), "BookOrderField should contain an 'ID' value");
        }
    }

    #[tokio::test]
    async fn test_connection_structure_and_field_types() {
        let singleton = get_singleton(&SCHEMA_SINGLETON, SCHEMA_AST).await;
        let registry = singleton.schema.registry();

        let query_root = find_type(registry, "Query");
        let books_field = find_field(query_root, "books").unwrap();
        assert_eq!(books_field.ty, "BookConnection!", "Root 'books' field should return a non-null BookConnection");

        assert!(books_field.args.contains_key("after"));
        assert!(books_field.args.contains_key("before"));
        assert!(books_field.args.contains_key("first"));
        assert!(books_field.args.contains_key("last"));
        assert_eq!(books_field.args.get("orderBy").unwrap().ty, "BookOrder");

        let connection_type = find_type(registry, "BookConnection");
        assert!(matches!(connection_type, MetaType::Object { .. }));

        let edges_field = find_field(connection_type, "edges").unwrap();
        assert_eq!(edges_field.ty, "[BookEdge]", "Connection 'edges' field should be a list of BookEdge");

        let nodes_field = find_field(connection_type, "nodes").unwrap();
        assert_eq!(nodes_field.ty, "[Book]", "Connection 'nodes' field should be a list of Book");

        let page_info_field = find_field(connection_type, "pageInfo").unwrap();
        assert_eq!(page_info_field.ty, "PageInfo!", "Connection 'pageInfo' field should be a non-null PageInfo");

        let total_count_field = find_field(connection_type, "totalCount").unwrap();
        assert_eq!(total_count_field.ty, "Int!", "Connection 'totalCount' field should be a non-null Int");

        let edge_type = find_type(registry, "BookEdge");
        assert!(matches!(edge_type, MetaType::Object { .. }));

        let cursor_field = find_field(edge_type, "cursor").unwrap();
        assert_eq!(cursor_field.ty, "String!", "Edge 'cursor' field should be a non-null String");

        let node_field = find_field(edge_type, "node").unwrap();
        assert_eq!(node_field.ty, "Book", "Edge 'node' field should be of type Book");

        let page_info_type = find_type(registry, "PageInfo");
        assert!(matches!(page_info_type, MetaType::Object { .. }), "PageInfo should be an Object");

        let has_next_page_field = find_field(page_info_type, "hasNextPage").unwrap();
        assert_eq!(has_next_page_field.ty, "Boolean!", "PageInfo 'hasNextPage' field should be a non-null Boolean");

        let has_previous_page_field = find_field(page_info_type, "hasPreviousPage").unwrap();
        assert_eq!(has_previous_page_field.ty, "Boolean!", "PageInfo 'hasPreviousPage' field should be a non-null Boolean");

        let start_cursor_field = find_field(page_info_type, "startCursor").unwrap();
        assert_eq!(start_cursor_field.ty, "String", "PageInfo 'startCursor' field should be a String");

        let end_cursor_field = find_field(page_info_type, "endCursor").unwrap();
        assert_eq!(end_cursor_field.ty, "String", "PageInfo 'endCursor' field should be a String");
    }
}

mod e2e_queries_cursor {
    use crate::gql::test::{get_singleton, CURSOR_SINGLETON};
    use serde_json::json;

    const AST: &str = r#"
        DEFINE TABLE aircraft SCHEMAFULL TYPE NORMAL;
        DEFINE FIELD manufacturer ON aircraft TYPE string;
        DEFINE FIELD model ON aircraft TYPE string;
        DEFINE FIELD airline ON aircraft TYPE record<airline>;
        DEFINE FIELD capacity ON aircraft TYPE int;

        DEFINE TABLE airline SCHEMAFULL TYPE NORMAL;
        DEFINE FIELD name ON airline TYPE string;
        DEFINE FIELD aircraft ON airline TYPE array<record<aircraft>>;
        DEFINE FIELD routes ON airline TYPE array<int>;
        DEFINE FIELD miles_and_more ON airline TYPE array<object>;
        DEFINE FIELD miles_and_more[*].tier ON airline TYPE string;
        DEFINE FIELD miles_and_more[*].points ON airline TYPE int;
        DEFINE FIELD maintenance_schedules ON airline TYPE array<object>;
        DEFINE FIELD maintenance_schedules[*].aircraft_model ON airline TYPE record<aircraft>;
        DEFINE FIELD maintenance_schedules[*].checks ON airline TYPE array<object>;
        DEFINE FIELD maintenance_schedules[*].checks[*].check_type ON airline TYPE string;

        DEFINE TABLE airport SCHEMAFULL TYPE NORMAL;
        DEFINE FIELD name ON airport TYPE string;

        UPSERT airport:eddf SET name = "Frankfurt Airport";
        UPSERT airport:klax SET name = "Los Angeles International Airport";
        UPSERT airport:kjfk SET name = "John F. Kennedy International Airport";

        UPSERT airline:lh SET
            name = "Lufthansa",
            aircraft = [
                aircraft:a320,
                aircraft:a350,
                aircraft:a380
            ],
            routes = [135, 400, 401, 456, 457],
            miles_and_more = [
                { tier: "HON Circle", points: 100000 },
                { tier: "Senator", points: 60000 },
                { tier: "Frequent Traveller", points: 35000 }
            ],
            maintenance_schedules = [
                {aircraft_model: aircraft:a380, checks: [{check_type: 'A-Check'}, {check_type:'C-Check'}]},
                {aircraft_model: aircraft:a350, checks: [{check_type: 'Line Maintenance'}]}
            ]
        ;

        UPSERT aircraft:a380 SET
            manufacturer = "Airbus",
            model = "A380-800",
            airline = airline:lh,
            capacity = 555
        ;

        UPSERT aircraft:a350 SET
            manufacturer = "Airbus",
            model = "A350-900",
            airline = airline:lh,
            capacity = 314
        ;

        UPSERT aircraft:a320 SET
            manufacturer = "Airbus",
            model = "A320-200",
            airline = airline:lh,
            capacity = 150
        ;

        DEFINE TABLE visits SCHEMAFULL TYPE RELATION IN airline OUT airport;
        DEFINE FIELD is_home_base ON visits TYPE bool;

        RELATE airline:lh->visits->airport:eddf SET is_home_base = true;
        RELATE airline:lh->visits->airport:klax SET is_home_base = false;
        RELATE airline:lh->visits->airport:kjfk SET is_home_base = false;

        DEFINE CONFIG GRAPHQL AUTO CURSOR;
    "#;

    #[tokio::test]
    async fn test_record() {
        let singleton = get_singleton(&CURSOR_SINGLETON, AST).await;
        let query = r#"
            query {
              aircraft(id: "aircraft:a380") {
                model
                manufacturer
                capacity
                airline {
                  name
                }
              }
            }
        "#;
        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json["aircraft"]["model"], json!("A380-800"));
        assert_eq!(json["aircraft"]["manufacturer"], json!("Airbus"));
        assert_eq!(json["aircraft"]["capacity"], json!(555));
        assert_eq!(json["aircraft"]["airline"]["name"], json!("Lufthansa"));
    }

    #[tokio::test]
    async fn test_relation() {
        let singleton = get_singleton(&CURSOR_SINGLETON, AST).await;
        let query = r#"
            query Airline {
              airline(id: "airline:lh") {
                name
                visits {
                  totalCount
                  pageInfo {
                    hasNextPage
                    hasPreviousPage
                    startCursor
                    endCursor
                  }
                  nodes {
                    id
                    name
                  }
                  edges {
                    cursor
                    node {
                      id
                      name
                    }
                    isHomeBase
                  }
                }
              }
            }
        "#;
        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json, json!({
            "airline": {
                "name": "Lufthansa",
                "visits": {
                    "totalCount": 3,
                    "pageInfo": {
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                        "startCursor": "Qxok7nblmdzbdevZ4NCXcXbOu6nX06B8KlR1T-7cATc",
                        "endCursor": "D_jpMLqDEqK1sE6bd79Le3-cdPmJVntNOq3D22mxEXc"
                    },
                    "nodes": [
                        {
                            "id": "airport:klax",
                            "name": "Los Angeles International Airport"
                        },
                        {
                            "id": "airport:eddf",
                            "name": "Frankfurt Airport"
                        },
                        {
                            "id": "airport:kjfk",
                            "name": "John F. Kennedy International Airport"
                        }
                    ],
                    "edges": [
                        {
                            "cursor": "Qxok7nblmdzbdevZ4NCXcXbOu6nX06B8KlR1T-7cATc",
                            "node": {
                                "id": "airport:klax",
                                "name": "Los Angeles International Airport"
                            },
                            "isHomeBase": false
                        },
                        {
                            "cursor": "eiT9hWqFtrjfGph7imISRr0goP5vBFhMi1S2rCUvgXw",
                            "node": {
                                "id": "airport:eddf",
                                "name": "Frankfurt Airport"
                            },
                            "isHomeBase": true
                        },
                        {
                            "cursor": "D_jpMLqDEqK1sE6bd79Le3-cdPmJVntNOq3D22mxEXc",
                            "node": {
                                "id": "airport:kjfk",
                                "name": "John F. Kennedy International Airport"
                            },
                            "isHomeBase": false
                        }
                    ]
                }
            }
        }));

        // assert_eq!(json["airline"]["name"], json!("Lufthansa"));
        // assert_eq!(json["airline"]["visits"]["totalCount"], json!(3));
        // assert_eq!(json["airline"]["visits"]["pageInfo"]["hasNextPage"], json!(false));
        // assert_eq!(json["airline"]["visits"]["pageInfo"]["hasPreviousPage"], json!(false));
        // assert_eq!(json["airline"]["visits"]["pageInfo"]["startCursor"], json!("7XX9H9J4Oyis10ws2EtAmcJzyJ6vV61XzEXFU_wi5Ls"));
        // assert_eq!(json["airline"]["visits"]["pageInfo"]["endCursor"], json!("ZKp964JtZ1b6ezsl2REUEPIFszUPbm5l7sYIAgynVFg"));
        //
        // // let visits_nodes = &json["airline"]["visits"]["nodes"];)
        // assert_eq!(json["airline"]["visits"]["nodes"][0]["id"], json!("airport:eddf"));
        // assert_eq!(json["airline"]["visits"]["nodes"][0]["name"], json!("Frankfurt Airport"));
        // assert_eq!(json["airline"]["visits"]["edges"][0]["cursor"], json!("R9cjNs8DEVRxIrS7ULozBKwIJqydW8wY-PjBKyab6-g"));
        // assert_eq!(json["airline"]["visits"]["edges"][0]["node"]["id"], json!("airport:eddf"));
        // assert_eq!(json["airline"]["visits"]["edges"][0]["node"]["name"], json!("Frankfurt Airport"));
        // assert_eq!(json["airline"]["visits"]["edges"][0]["isHomeBase"], json!(true));
        //
        // assert_eq!(json["airline"]["visits"]["nodes"][1]["id"], json!("airport:klax"));
        // assert_eq!(json["airline"]["visits"]["nodes"][1]["name"], json!("Los Angeles International Airport"));
        // assert_eq!(json["airline"]["visits"]["edges"][1]["cursor"], json!("ZKp964JtZ1b6ezsl2REUEPIFszUPbm5l7sYIAgynVFg"));
        // assert_eq!(json["airline"]["visits"]["edges"][1]["node"]["id"], json!("airport:klax"));
        // assert_eq!(json["airline"]["visits"]["edges"][1]["node"]["name"], json!("Los Angeles International Airport"));
        // assert_eq!(json["airline"]["visits"]["edges"][1]["isHomeBase"], json!(false));
        //
        // assert_eq!(json["airline"]["visits"]["nodes"][2]["id"], json!("airport:kjfk"));
        // assert_eq!(json["airline"]["visits"]["nodes"][2]["name"], json!("John F. Kennedy International Airport"));
        // assert_eq!(json["airline"]["visits"]["edges"][2]["cursor"], json!("7XX9H9J4Oyis10ws2EtAmcJzyJ6vV61XzEXFU_wi5Ls"));
        // assert_eq!(json["airline"]["visits"]["edges"][2]["node"]["id"], json!("airport:kjfk"));
        // assert_eq!(json["airline"]["visits"]["edges"][2]["node"]["name"], json!("John F. Kennedy International Airport"));
        // assert_eq!(json["airline"]["visits"]["edges"][2]["isHomeBase"], json!(false));
    }

    #[tokio::test]
    async fn test_array_scalar() {
        let singleton = get_singleton(&CURSOR_SINGLETON, AST).await;
        let query = r#"
            query Airline {
                airline(id: "lh") {
                    routes {
                        totalCount
                        pageInfo {
                            hasNextPage
                            hasPreviousPage
                            startCursor
                            endCursor
                        }
                        edges {
                            cursor
                            node
                        }
                        nodes
                    }
                }
            }
        "#;
        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json, json!({
            "airline": {
                "routes": {
                    "totalCount": 5,
                    "pageInfo": {
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                        "startCursor": "E2cQd7ZqKYdKJXi1JAMZCS7yoQQyKOQz6bAGteU-dRM",
                        "endCursor": "NTdnsjkJmGPhPKlU4gpmyddfd3uvI59W45mVjeSb950"
                    },
                    "edges": [
                        {
                            "cursor": "E2cQd7ZqKYdKJXi1JAMZCS7yoQQyKOQz6bAGteU-dRM",
                            "node": 135
                        },
                        {
                            "cursor": "JtIoZj8TqIWSoS0Wz5WHyqsDiLJi1tnxJu1i-TM6ypQ",
                            "node": 400
                        },
                        {
                            "cursor": "3Kra0c_OQ3c1uBqwJfd25YV-SFWMR_aWDmpfJZVmSoU",
                            "node": 401
                        },
                        {
                            "cursor": "s6jg4fmrG_46NvIx9nb3i7MKUZ0rIebFMMDu6Ou0pdA",
                            "node": 456
                        },
                        {
                            "cursor": "NTdnsjkJmGPhPKlU4gpmyddfd3uvI59W45mVjeSb950",
                            "node": 457
                        }
                    ],
                    "nodes": [
                        135,
                        400,
                        401,
                        456,
                        457
                    ]
                }
            }
        }));
    }

    #[tokio::test]
    async fn test_array_records() {
        let singleton = get_singleton(&CURSOR_SINGLETON, AST).await;
        let query = r#"
            query Airline {
                airline(id: "lh") {
                    aircraft {
                        totalCount
                        edges {
                            cursor
                            node {
                                id
                                capacity
                                manufacturer
                                model
                            }
                        }
                        nodes {
                            id
                            capacity
                            manufacturer
                            model
                        }
                        pageInfo {
                            hasNextPage
                            hasPreviousPage
                            startCursor
                            endCursor
                        }
                    }
                }
            }
        "#;
        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json, json!({
            "airline": {
                "aircraft": {
                    "totalCount": 3,
                    "edges": [
                        {
                            "cursor": "NH39k_QMq0QSU7XCZJJ1hcxLaGqciqVSv7Isv1nG_uc",
                            "node": {
                                "id": "aircraft:a320",
                                "capacity": 150,
                                "manufacturer": "Airbus",
                                "model": "A320-200"
                            }
                        },
                        {
                            "cursor": "vXXouKbuMDU8V0GQhgn3JnDIXfFOfV8eceWf9I2Wd-A",
                            "node": {
                                "id": "aircraft:a350",
                                "capacity": 314,
                                "manufacturer": "Airbus",
                                "model": "A350-900"
                            }
                        },
                        {
                            "cursor": "osHa1Td3sBAcNeBcBbXEKr2WMkGugopno8Ebdv6Ixl0",
                            "node": {
                                "id": "aircraft:a380",
                                "capacity": 555,
                                "manufacturer": "Airbus",
                                "model": "A380-800"
                            }
                        }
                    ],
                    "nodes": [
                        {
                            "id": "aircraft:a320",
                            "capacity": 150,
                            "manufacturer": "Airbus",
                            "model": "A320-200"
                        },
                        {
                            "id": "aircraft:a350",
                            "capacity": 314,
                            "manufacturer": "Airbus",
                            "model": "A350-900"
                        },
                        {
                            "id": "aircraft:a380",
                            "capacity": 555,
                            "manufacturer": "Airbus",
                            "model": "A380-800"
                        }
                    ],
                    "pageInfo": {
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                        "startCursor": "NH39k_QMq0QSU7XCZJJ1hcxLaGqciqVSv7Isv1nG_uc",
                        "endCursor": "osHa1Td3sBAcNeBcBbXEKr2WMkGugopno8Ebdv6Ixl0"
                    }
                }
            }
        }));
    }

    #[tokio::test]
    async fn test_array_embedded_object() {
        let singleton = get_singleton(&CURSOR_SINGLETON, AST).await;
        let query = r#"
            query Airline {
                airline(id: "lh") {
                    milesAndMore {
                        totalCount
                        edges {
                            cursor
                            node {
                                points
                                tier
                            }
                        }
                        nodes {
                            points
                            tier
                        }
                        pageInfo {
                            hasNextPage
                            hasPreviousPage
                            startCursor
                            endCursor
                        }
                    }
                }
            }
        "#;
        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json, json!({
            "airline": {
                "milesAndMore": {
                    "totalCount": 3,
                    "edges": [
                        {
                            "cursor": "pktrdsxTYF3gIT3rRL-Kkik7kQoadsDYL2aideiulgY",
                            "node": {
                                "points": 100000,
                                "tier": "HON Circle"
                            }
                        },
                        {
                            "cursor": "t4OjyBwXioJjvI3J_6RIVWLjD_1ANABrifl88DQT0Cg",
                            "node": {
                                "points": 60000,
                                "tier": "Senator"
                            }
                        },
                        {
                            "cursor": "n3b4WJrUA-e0rmVT2UmsWtL63OF-2GFSft516U5te-A",
                            "node": {
                                "points": 35000,
                                "tier": "Frequent Traveller"
                            }
                        }
                    ],
                    "nodes": [
                        {
                            "points": 100000,
                            "tier": "HON Circle"
                        },
                        {
                            "points": 60000,
                            "tier": "Senator"
                        },
                        {
                            "points": 35000,
                            "tier": "Frequent Traveller"
                        }
                    ],
                    "pageInfo": {
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                        "startCursor": "pktrdsxTYF3gIT3rRL-Kkik7kQoadsDYL2aideiulgY",
                        "endCursor": "n3b4WJrUA-e0rmVT2UmsWtL63OF-2GFSft516U5te-A"
                    }
                }
            }
        }));
    }

    #[tokio::test]
    async fn test_array_embedded_nested() {
        let singleton = get_singleton(&CURSOR_SINGLETON, AST).await;
        let query = r#"
            query Airline {
                airline(id: "lh") {
                    maintenanceSchedules {
                        totalCount
                        edges {
                            cursor
                            node {
                                aircraftModel {
                                    id
                                    model
                                }
                                checks {
                                    totalCount
                                    pageInfo {
                                        hasNextPage
                                        hasPreviousPage
                                        startCursor
                                        endCursor
                                    }
                                    nodes {
                                        checkType
                                    }
                                    edges {
                                        cursor
                                        node {
                                            checkType
                                        }
                                    }
                                }
                            }
                        }
                        nodes {
                            checks {
                                totalCount
                                pageInfo {
                                    hasNextPage
                                    hasPreviousPage
                                    startCursor
                                    endCursor
                                }
                                nodes {
                                    checkType
                                }
                                edges {
                                    cursor
                                    node {
                                        checkType
                                    }
                                }
                            }
                            aircraftModel {
                                id
                                model
                            }
                        }
                        pageInfo {
                            hasNextPage
                            hasPreviousPage
                            startCursor
                            endCursor
                        }
                    }
                }
            }
        "#;
        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json, json!({
            "airline": {
                "maintenanceSchedules": {
                    "totalCount": 2,
                    "edges": [
                        {
                            "cursor": "AT_xom8psJ5xboTBYHpnPcADbnU5wCOUJabw4FSP_MA",
                            "node": {
                                "aircraftModel": {
                                    "id": null,
                                    "model": null
                                },
                                "checks": {
                                    "totalCount": 2,
                                    "pageInfo": {
                                        "hasNextPage": false,
                                        "hasPreviousPage": false,
                                        "startCursor": "sRRMWLSxf8auMAwGNzvxjS4U9k4_7Tll97xPoZP0lWY",
                                        "endCursor": "-qUBehwu9T3WGap094d9ZKHoBsy3JT_1rxuVc12ZtE4"
                                    },
                                    "nodes": [
                                        {
                                            "checkType": "A-Check"
                                        },
                                        {
                                            "checkType": "C-Check"
                                        }
                                    ],
                                    "edges": [
                                        {
                                            "cursor": "sRRMWLSxf8auMAwGNzvxjS4U9k4_7Tll97xPoZP0lWY",
                                            "node": {
                                                "checkType": "A-Check"
                                            }
                                        },
                                        {
                                            "cursor": "-qUBehwu9T3WGap094d9ZKHoBsy3JT_1rxuVc12ZtE4",
                                            "node": {
                                                "checkType": "C-Check"
                                            }
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "cursor": "NIMbvTOokRlvBY-td12aJJG7R36JRVuRe7zEZ4wB8xA",
                            "node": {
                                "aircraftModel": {
                                    "id": null,
                                    "model": null
                                },
                                "checks": {
                                    "totalCount": 1,
                                    "pageInfo": {
                                        "hasNextPage": false,
                                        "hasPreviousPage": false,
                                        "startCursor": "jLunm_EFwOBMJTpV5jQFC9wIC30m0fpPdoSWt1LEwPI",
                                        "endCursor": "jLunm_EFwOBMJTpV5jQFC9wIC30m0fpPdoSWt1LEwPI"
                                    },
                                    "nodes": [
                                        {
                                            "checkType": "Line Maintenance"
                                        }
                                    ],
                                    "edges": [
                                        {
                                            "cursor": "jLunm_EFwOBMJTpV5jQFC9wIC30m0fpPdoSWt1LEwPI",
                                            "node": {
                                                "checkType": "Line Maintenance"
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    "nodes": [
                        {
                            "checks": {
                                "totalCount": 2,
                                "pageInfo": {
                                    "hasNextPage": false,
                                    "hasPreviousPage": false,
                                    "startCursor": "sRRMWLSxf8auMAwGNzvxjS4U9k4_7Tll97xPoZP0lWY",
                                    "endCursor": "-qUBehwu9T3WGap094d9ZKHoBsy3JT_1rxuVc12ZtE4"
                                },
                                "nodes": [
                                    {
                                        "checkType": "A-Check"
                                    },
                                    {
                                        "checkType": "C-Check"
                                    }
                                ],
                                "edges": [
                                    {
                                        "cursor": "sRRMWLSxf8auMAwGNzvxjS4U9k4_7Tll97xPoZP0lWY",
                                        "node": {
                                            "checkType": "A-Check"
                                        }
                                    },
                                    {
                                        "cursor": "-qUBehwu9T3WGap094d9ZKHoBsy3JT_1rxuVc12ZtE4",
                                        "node": {
                                            "checkType": "C-Check"
                                        }
                                    }
                                ]
                            },
                            "aircraftModel": {
                                "id": null,
                                "model": null
                            }
                        },
                        {
                            "checks": {
                                "totalCount": 1,
                                "pageInfo": {
                                    "hasNextPage": false,
                                    "hasPreviousPage": false,
                                    "startCursor": "jLunm_EFwOBMJTpV5jQFC9wIC30m0fpPdoSWt1LEwPI",
                                    "endCursor": "jLunm_EFwOBMJTpV5jQFC9wIC30m0fpPdoSWt1LEwPI"
                                },
                                "nodes": [
                                    {
                                        "checkType": "Line Maintenance"
                                    }
                                ],
                                "edges": [
                                    {
                                        "cursor": "jLunm_EFwOBMJTpV5jQFC9wIC30m0fpPdoSWt1LEwPI",
                                        "node": {
                                            "checkType": "Line Maintenance"
                                        }
                                    }
                                ]
                            },
                            "aircraftModel": {
                                "id": null,
                                "model": null
                            }
                        }
                    ],
                    "pageInfo": {
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                        "startCursor": "AT_xom8psJ5xboTBYHpnPcADbnU5wCOUJabw4FSP_MA",
                        "endCursor": "NIMbvTOokRlvBY-td12aJJG7R36JRVuRe7zEZ4wB8xA"
                    }
                }
            }
        }));
    }

    #[tokio::test]
    async fn test_array_root_query() {
        let singleton = get_singleton(&CURSOR_SINGLETON, AST).await;

        let query = r#"
            query AircraftList {
                aircraftList {
                    edges {
                        cursor
                        node {
                            id
                            capacity
                            manufacturer
                            model
                            airline {
                                id
                                name
                            }
                        }
                    }
                    nodes {
                        id
                        capacity
                        manufacturer
                        model
                        airline {
                            id
                            name
                        }
                    }
                    pageInfo {
                        hasNextPage
                        hasPreviousPage
                        startCursor
                        endCursor
                    }
                    totalCount
                }
            }
        "#;

        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json, json!({
            "aircraftList": {
                "edges": [
                    {
                        "cursor": "YpzIAbBU0t2UVRPN7sISgFsG5QfI3lqmzAwT98RaPmA",
                        "node": {
                            "id": "aircraft:a320",
                            "capacity": 150,
                            "manufacturer": "Airbus",
                            "model": "A320-200",
                            "airline": {
                                "id": "airline:lh",
                                "name": "Lufthansa"
                            }
                        }
                    },
                    {
                        "cursor": "oXg9fHR2NJYB3LGhftQ6v0mY-SQCvJQW1fKIxr_bWec",
                        "node": {
                            "id": "aircraft:a350",
                            "capacity": 314,
                            "manufacturer": "Airbus",
                            "model": "A350-900",
                            "airline": {
                                "id": "airline:lh",
                                "name": "Lufthansa"
                            }
                        }
                    },
                    {
                        "cursor": "MkFwfulfjDk6JEC7oXlhxoq0c6mhF_Uxx83_jHonk5c",
                        "node": {
                            "id": "aircraft:a380",
                            "capacity": 555,
                            "manufacturer": "Airbus",
                            "model": "A380-800",
                            "airline": {
                                "id": "airline:lh",
                                "name": "Lufthansa"
                            }
                        }
                    }
                ],
                "nodes": [
                    {
                        "id": "aircraft:a320",
                        "capacity": 150,
                        "manufacturer": "Airbus",
                        "model": "A320-200",
                        "airline": {
                            "id": "airline:lh",
                            "name": "Lufthansa"
                        }
                    },
                    {
                        "id": "aircraft:a350",
                        "capacity": 314,
                        "manufacturer": "Airbus",
                        "model": "A350-900",
                        "airline": {
                            "id": "airline:lh",
                            "name": "Lufthansa"
                        }
                    },
                    {
                        "id": "aircraft:a380",
                        "capacity": 555,
                        "manufacturer": "Airbus",
                        "model": "A380-800",
                        "airline": {
                            "id": "airline:lh",
                            "name": "Lufthansa"
                        }
                    }
                ],
                "pageInfo": {
                    "hasNextPage": false,
                    "hasPreviousPage": false,
                    "startCursor": "YpzIAbBU0t2UVRPN7sISgFsG5QfI3lqmzAwT98RaPmA",
                    "endCursor": "MkFwfulfjDk6JEC7oXlhxoq0c6mhF_Uxx83_jHonk5c"
                },
                "totalCount": 3
            }
        }));
    }
}

mod e2e_queries {
    use crate::gql::test::{get_singleton, NO_CURSOR_SINGLETON};
    use serde_json::json;

    const AST: &str = r#"
        DEFINE TABLE aircraft SCHEMAFULL TYPE NORMAL;
        DEFINE FIELD manufacturer ON aircraft TYPE string;
        DEFINE FIELD model ON aircraft TYPE string;
        DEFINE FIELD airline ON aircraft TYPE record<airline>;
        DEFINE FIELD capacity ON aircraft TYPE int;

        DEFINE TABLE airline SCHEMAFULL TYPE NORMAL;
        DEFINE FIELD name ON airline TYPE string;
        DEFINE FIELD aircraft ON airline TYPE array<record<aircraft>>;
        DEFINE FIELD routes ON airline TYPE array<int>;
        DEFINE FIELD miles_and_more ON airline TYPE array<object>;
        DEFINE FIELD miles_and_more[*].tier ON airline TYPE string;
        DEFINE FIELD miles_and_more[*].points ON airline TYPE int;
        DEFINE FIELD maintenance_schedules ON airline TYPE array<object>;
        DEFINE FIELD maintenance_schedules[*].aircraft_model ON airline TYPE record<aircraft>;
        DEFINE FIELD maintenance_schedules[*].checks ON airline TYPE array<object>;
        DEFINE FIELD maintenance_schedules[*].checks[*].check_type ON airline TYPE string;

        DEFINE TABLE airport SCHEMAFULL TYPE NORMAL;
        DEFINE FIELD name ON airport TYPE string;

        UPSERT airport:eddf SET name = "Frankfurt Airport";
        UPSERT airport:klax SET name = "Los Angeles International Airport";
        UPSERT airport:kjfk SET name = "John F. Kennedy International Airport";

        UPSERT airline:lh SET
            name = "Lufthansa",
            aircraft = [
                aircraft:a320,
                aircraft:a350,
                aircraft:a380
            ],
            routes = [135, 400, 401, 456, 457],
            miles_and_more = [
                { tier: "HON Circle", points: 100000 },
                { tier: "Senator", points: 60000 },
                { tier: "Frequent Traveller", points: 35000 }
            ],
            maintenance_schedules = [
                {aircraft_model: aircraft:a380, checks: [{check_type: 'A-Check'}, {check_type:'C-Check'}]},
                {aircraft_model: aircraft:a350, checks: [{check_type: 'Line Maintenance'}]}
            ]
        ;

        UPSERT aircraft:a380 SET
            manufacturer = "Airbus",
            model = "A380-800",
            airline = airline:lh,
            capacity = 555
        ;

        UPSERT aircraft:a350 SET
            manufacturer = "Airbus",
            model = "A350-900",
            airline = airline:lh,
            capacity = 314
        ;

        UPSERT aircraft:a320 SET
            manufacturer = "Airbus",
            model = "A320-200",
            airline = airline:lh,
            capacity = 150
        ;

        DEFINE TABLE visits SCHEMAFULL TYPE RELATION IN airline OUT airport;
        DEFINE FIELD is_home_base ON visits TYPE bool;

        RELATE airline:lh->visits->airport:eddf SET is_home_base = true;
        RELATE airline:lh->visits->airport:klax SET is_home_base = false;
        RELATE airline:lh->visits->airport:kjfk SET is_home_base = false;

        DEFINE CONFIG GRAPHQL AUTO;
    "#;

    #[tokio::test]
    async fn test_record() {
        let singleton = get_singleton(&NO_CURSOR_SINGLETON, AST).await;
        let query = r#"
            query Aircraft {
                aircraft(id: "a380") {
                    airline {
                        id
                        name
                    }
                }
            }
        "#;
        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json, json!({
            "aircraft": {
                "airline": {
                    "id": "airline:lh",
                    "name": "Lufthansa"
                }
            }
        }));
    }

    #[tokio::test]
    async fn test_relation() {
        let singleton = get_singleton(&NO_CURSOR_SINGLETON, AST).await;
        let query = r#"
            query Airline {
                airline(id: "lh") {
                    id
                    name
                    routes
                    visits {
                        totalCount
                        pageInfo {
                            hasNextPage
                            hasPreviousPage
                            startCursor
                            endCursor
                        }
                        nodes {
                            id
                            name
                        }
                        edges {
                            cursor
                            isHomeBase
                            node {
                                id
                                name
                            }
                        }
                    }
                }
            }
        "#;
        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json, json!({
            "airline": {
                "id": "airline:lh",
                "name": "Lufthansa",
                "routes": [
                    135,
                    400,
                    401,
                    456,
                    457
                ],
                "visits": {
                    "totalCount": 3,
                    "pageInfo": {
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                        "startCursor": "DY1vgI2gmU5TFgEo5lA_y2ZtVQlQrU5JoQ1qpnrFRD0",
                        "endCursor": "tSN0dtba-lwi1htOK9k3Uoj-ZvgOmRvo20gX2PDGacs"
                    },
                    "nodes": [
                        {
                            "id": "airport:kjfk",
                            "name": "John F. Kennedy International Airport"
                        },
                        {
                            "id": "airport:klax",
                            "name": "Los Angeles International Airport"
                        },
                        {
                            "id": "airport:eddf",
                            "name": "Frankfurt Airport"
                        }
                    ],
                    "edges": [
                        {
                            "cursor": "DY1vgI2gmU5TFgEo5lA_y2ZtVQlQrU5JoQ1qpnrFRD0",
                            "isHomeBase": false,
                            "node": {
                                "id": "airport:kjfk",
                                "name": "John F. Kennedy International Airport"
                            }
                        },
                        {
                            "cursor": "Sd8HAAWbztGEFMoF13r_ohy6rhPv8xx2_COOCqPka2I",
                            "isHomeBase": false,
                            "node": {
                                "id": "airport:klax",
                                "name": "Los Angeles International Airport"
                            }
                        },
                        {
                            "cursor": "tSN0dtba-lwi1htOK9k3Uoj-ZvgOmRvo20gX2PDGacs",
                            "isHomeBase": true,
                            "node": {
                                "id": "airport:eddf",
                                "name": "Frankfurt Airport"
                            }
                        }
                    ]
                }
            }
        }));
    }

    #[tokio::test]
    async fn test_array_scalar() {
        let singleton = get_singleton(&NO_CURSOR_SINGLETON, AST).await;
        let query = r#"
            query Airline {
                airline(id: "airline:lh") {
                    name
                    routes
                    id
                }
            }
        "#;
        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json, json!({
            "airline": {
                "name": "Lufthansa",
                "routes": [
                    135,
                    400,
                    401,
                    456,
                    457
                ],
                "id": "airline:lh"
            }
        }));
    }

    #[tokio::test]
    async fn test_array_records() {
        let singleton = get_singleton(&NO_CURSOR_SINGLETON, AST).await;
        let query = r#"
            query Airline {
                airline(id: "airline:lh") {
                    aircraft {
                        id
                        capacity
                        manufacturer
                        model
                    }
                }
            }
        "#;
        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json, json!({
            "airline": {
                "aircraft": [
                    {
                        "id": "aircraft:a320",
                        "capacity": 150,
                        "manufacturer": "Airbus",
                        "model": "A320-200"
                    },
                    {
                        "id": "aircraft:a350",
                        "capacity": 314,
                        "manufacturer": "Airbus",
                        "model": "A350-900"
                    },
                    {
                        "id": "aircraft:a380",
                        "capacity": 555,
                        "manufacturer": "Airbus",
                        "model": "A380-800"
                    }
                ]
            }
        }));
    }

    #[tokio::test]
    async fn test_array_embedded_object() {
        let singleton = get_singleton(&NO_CURSOR_SINGLETON, AST).await;
        let query = r#"
            query Airline {
                airline(id: "lh") {
                    milesAndMore {
                        points
                        tier
                    }
                }
            }
        "#;
        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json, json!({
            "airline": {
                "milesAndMore": [
                    {
                        "points": 100000,
                        "tier": "HON Circle"
                    },
                    {
                        "points": 60000,
                        "tier": "Senator"
                    },
                    {
                        "points": 35000,
                        "tier": "Frequent Traveller"
                    }
                ]
            }
        }));
    }

    #[tokio::test]
    async fn test_array_embedded_nested() {
        let singleton = get_singleton(&NO_CURSOR_SINGLETON, AST).await;
        let query = r#"
            query Airline {
                airline(id: "lh") {
                    maintenanceSchedules {
                        checks {
                            checkType
                        }
                        aircraftModel {
                            id
                            model
                        }
                    }
                }
            }
        "#;
        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json, json!({
            "airline": {
                "maintenanceSchedules": [
                    {
                        "checks": [
                            {
                                "checkType": "A-Check"
                            },
                            {
                                "checkType": "C-Check"
                            }
                        ],
                        "aircraftModel": {
                            "id": "aircraft:a380",
                            "model": "A380-800"
                        }
                    },
                    {
                        "checks": [
                            {
                                "checkType": "Line Maintenance"
                            }
                        ],
                        "aircraftModel": {
                            "id": "aircraft:a350",
                            "model": "A350-900"
                        }
                    }
                ]
            }
        }));
    }

    #[tokio::test]
    async fn test_array_root_query() {
        let singleton = get_singleton(&NO_CURSOR_SINGLETON, AST).await;

        let query = r#"
            query AircraftList {
                aircraftList {
                    id
                    capacity
                    manufacturer
                    model
                    airline {
                        id
                        name
                    }
                }
            }
        "#;

        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json, json!({
            "aircraftList": [
                {
                    "id": "aircraft:a320",
                    "capacity": 150,
                    "manufacturer": "Airbus",
                    "model": "A320-200",
                    "airline": {
                        "id": "airline:lh",
                        "name": "Lufthansa"
                    }
                },
                {
                    "id": "aircraft:a350",
                    "capacity": 314,
                    "manufacturer": "Airbus",
                    "model": "A350-900",
                    "airline": {
                        "id": "airline:lh",
                        "name": "Lufthansa"
                    }
                },
                {
                    "id": "aircraft:a380",
                    "capacity": 555,
                    "manufacturer": "Airbus",
                    "model": "A380-800",
                    "airline": {
                        "id": "airline:lh",
                        "name": "Lufthansa"
                    }
                }
            ]
        }));
    }
}
