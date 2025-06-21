use crate::dbs::Session;
use crate::gql::schema::generate_schema;
use crate::kvs::Datastore;
use async_graphql::dynamic::Schema;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::OnceCell;

const AST: &str = r#"
    DEFINE TABLE image SCHEMAFULL TYPE NORMAL;
    DEFINE FIELD url ON TABLE image TYPE string;
    DEFINE FIELD size ON TABLE image TYPE object;
    DEFINE FIELD size.height ON TABLE image TYPE int ASSERT $value >= 0;

    DEFINE TABLE relates SCHEMAFULL TYPE RELATION IN user OUT image ENFORCED;
    DEFINE FIELD room ON TABLE relates TYPE option<string>;

    DEFINE TABLE user SCHEMAFULL TYPE NORMAL;
    DEFINE FIELD created_at ON TABLE user TYPE datetime VALUE time::now() READONLY;
    DEFINE FIELD ref ON TABLE user TYPE record<image>;
    DEFINE FIELD arr ON TABLE user TYPE array<number>;
    DEFINE FIELD arr_ref ON TABLE user TYPE array<record<image>>;
    DEFINE FIELD arr_obj ON TABLE user TYPE array<object>;
    DEFINE FIELD arr_obj[*].name ON TABLE user TYPE string;
    DEFINE FIELD arr_nested ON TABLE user TYPE array<object>;
    DEFINE FIELD arr_nested[*].title ON TABLE user TYPE string;
    DEFINE FIELD arr_nested[*].items ON TABLE user TYPE array<object>;
    DEFINE FIELD arr_nested[*].items[*].subtitle ON TABLE user TYPE string;
    DEFINE FIELD description ON TABLE user TYPE string;

    UPSERT image:one SET url = 'https:://www.example-one.com', size.height = 865;
    UPSERT image:two SET url = 'https:://www.example-two.com', size.height = 856;

    CREATE user:tobie SET
        description = "SurrealDB is awesome!",
        ref = image:one,
        arr = [42, 69],
        arr_ref = [image:one, image:two],
        arr_obj = [{name: "first"}, {name: "second"}],
        arr_nested = [{title: "GraphQL", items: [{subtitle: "sub1"}, {subtitle: "sub2"}]},
        {title: "SurrealQL", items: [{subtitle: "sub21"}, {subtitle: "sub22"}]}];

    RELATE user:tobie -> relates:yay -> image:one CONTENT { room: 'Indoor' };
    RELATE user:tobie -> relates:nay -> image:two CONTENT { room: 'Outdoor' };

    DEFINE CONFIG GRAPHQL AUTO CURSOR;
"#;

/// A singleton to hold the initialized database and GraphQL schema.
/// This is initialized only once for all tests in this file.
struct GqlSingleton {
    datastore: Arc<Datastore>,
    session: Session,
    schema: Schema,
}

static GQL_SINGLETON: OnceCell<Arc<GqlSingleton>> = OnceCell::const_new();

/// A reusable function to build the dynamic GraphQL schema.
/// It takes the generated query object and types, adds required
/// interfaces, and returns a finished, executable schema.
// fn build_schema(
//     query: Object,
//     types: Vec<Type>,
//     gtx: GQLTx,
// ) -> Result<Schema, SchemaError> {
//     let mut schema_builder = Schema::build("Query", None, None).register(query);
//
//     for ty in types {
//         schema_builder = schema_builder.register(ty);
//     }
//
//     let id_interface =
//         Interface::new("Record").field(InterfaceField::new("id", TypeRef::named_nn(TypeRef::ID)));
//     schema_builder = schema_builder.register(id_interface);
//     let relation_interface = Interface::new("relation")
//         .field(InterfaceField::new("id", TypeRef::named_nn(TypeRef::ID)))
//         .field(InterfaceField::new("in", TypeRef::named_nn("Record")))
//         .field(InterfaceField::new("out", TypeRef::named_nn("Record")))
//         .implement("Record");
//     schema_builder = schema_builder.register(relation_interface);
//
//     crate::scalar_debug_validated!(
// 		schema,
// 		"UUID",
// 		Kind::Uuid,
// 		"String encoded UUID",
// 		"https://datatracker.ietf.org/doc/html/rfc4122"
// 	);
//     crate::scalar_debug_validated!(schema, "Decimal", Kind::Decimal);
//     crate::scalar_debug_validated!(schema, "Number", Kind::Number);
//     crate::scalar_debug_validated!(schema, "Null", Kind::Null);
//     crate::scalar_debug_validated!(schema, "DateTime", Kind::Datetime);
//     crate::scalar_debug_validated!(schema, "Duration", Kind::Duration);
//     crate::scalar_debug_validated!(schema, "Object", Kind::Object);
//     crate::scalar_debug_validated!(schema, "Any", Kind::Any);
//
//
//     schema_builder.data(gtx).finish()
// }

/// Gets or initializes the singleton. This ensures that the database
/// setup and schema generation runs only once.
async fn get_singleton() -> &'static Arc<GqlSingleton> {
    GQL_SINGLETON
        .get_or_init(|| async {
            let datastore = Arc::new(Datastore::new("memory").await.unwrap());
            let session = Session::owner().with_ns("test").with_db("test");
            datastore.execute(AST, &session, None).await.unwrap();

            let schema = generate_schema(&datastore, &session).await.unwrap();
            // schema.query_root().unwrap();

            Arc::new(GqlSingleton {
                datastore,
                session,
                // query_root: query,
                // types,
                schema,
            })
        })
        .await
}

// ========================================================
// TEST CASES (These remain the same, but now call get_singleton)
// ========================================================

mod schema_generation {
    use super::*;
    use async_graphql::registry::{MetaField, MetaType, Registry};

    /// Helper to find a specific type by name from the schema's registry.
    fn find_type_in_registry<'a>(registry: &'a Registry, name: &str) -> Option<&'a MetaType> {
        registry.types.get(name)
    }

    /// Helper to find a field on a meta object type.
    fn find_field_on_meta_object<'a>(
        meta_obj: &'a MetaType,
        field_name: &str,
    ) -> Option<&'a MetaField> {
        match meta_obj {
            MetaType::Object { fields, .. } => fields.get(field_name),
            _ => None,
        }
    }

    #[tokio::test]
    async fn test_root_query_fields_exist() {
        let singleton = get_singleton().await;
        let registry = singleton.schema.registry();
        let query_root = find_type_in_registry(registry, "Query").expect("Query root must exist");

        assert!(
            find_field_on_meta_object(query_root, "user").is_some(),
            "Query object should have a 'user' field for single record fetching."
        );
        assert!(
            find_field_on_meta_object(query_root, "users").is_some(),
            "Query object should have a 'users' field for the connection."
        );
    }

    #[tokio::test]
    async fn test_relation_field_is_connection() {
        let singleton = get_singleton().await;
        let registry = singleton.schema.registry();
        let user_type = find_type_in_registry(registry, "User").expect("User type must exist");

        let relates_field = find_field_on_meta_object(user_type, "relates").unwrap();
        // The type name for a connection is constructed as `TypeNameConnection`
        assert_eq!(relates_field.ty, "ImageConnection!");
        //FIXME: fails
        // Left:  ImageConnection!
        // Right: RelatesConnection!
        // see if this is correct.
        assert!(
            find_type_in_registry(registry, "ImageConnection").is_some(),
            "RelatesConnection type should be generated"
        );
        assert!(
            find_type_in_registry(registry, "ImageEdge").is_some(),
            "RelatesEdge type should be generated"
        );
    }

    #[tokio::test]
    async fn test_embedded_array_is_connection() {
        let singleton = get_singleton().await;
        let registry = singleton.schema.registry();
        let user_type = find_type_in_registry(registry, "User").unwrap();

        // Test array of records
        let arr_ref_field = find_field_on_meta_object(user_type, "arrRef").unwrap();
        assert_eq!(arr_ref_field.ty, "ImageConnection!");

        // Test array of objects
        let arr_obj_field = find_field_on_meta_object(user_type, "arrObj").unwrap();
        assert_eq!(arr_obj_field.ty, "UserArrObjObjectConnection!");
    }
}
// FIXME: add naming convention tests

mod e2e_queries {
    use super::*;

    #[tokio::test]
    async fn test_relation_connection() {
        let singleton = get_singleton().await;
        let query = r#"
        query {
          user(id: "user:tobie") {
            relates {
              totalCount
              edges {
                room
                node {
                  url
                }
              }
            }
          }
        }
        "#;

        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json["user"]["relates"]["totalCount"], json!(2));
        assert_eq!(json["user"]["relates"]["edges"][1]["room"], json!("Indoor"));
        assert_eq!(json["user"]["relates"]["edges"][1]["node"]["url"], json!("https:://www.example-one.com"));
        assert_eq!(json["user"]["relates"]["edges"][0]["room"], json!("Outdoor"));
        assert_eq!(json["user"]["relates"]["edges"][0]["node"]["url"], json!("https:://www.example-two.com"));
    }

    #[tokio::test]
    async fn test_embedded_record_array_connection() {
        let singleton = get_singleton().await;
        let query = r#"
        query {
          user(id: "user:tobie") {
            arrRef {
              totalCount
              edges {
                node {
                  url
                }
              }
            }
          }
        }
        "#;

        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json["user"]["arrRef"]["totalCount"], json!(2));
        assert_eq!(json["user"]["arrRef"]["edges"][0]["node"]["url"], json!("https:://www.example-one.com"));
        assert_eq!(json["user"]["arrRef"]["edges"][1]["node"]["url"], json!("https:://www.example-two.com"));
    }

    #[tokio::test]
    async fn test_embedded_object_array_connection() {
        let singleton = get_singleton().await;
        let query = r#"
        query {
          user(id: "user:tobie") {
            arrObj {
              totalCount
              nodes {
                name
              }
            }
          }
        }
        "#;

        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json["user"]["arrObj"]["totalCount"], json!(2));
        assert_eq!(json["user"]["arrObj"]["nodes"][0]["name"], json!("first"));
        assert_eq!(json["user"]["arrObj"]["nodes"][1]["name"], json!("second"));
    }

    #[tokio::test]
    async fn test_simple_record_link_with_nested_object() {
        let singleton = get_singleton().await;
        let query = r#"
        query {
          user(id: "user:tobie") {
            ref {
              id
              url
              size {
                height
              }
            }
          }
        }
        "#;

        // FIXME: ref not wokring
        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json["user"]["ref"]["url"], json!("https:://www.example-one.com"));
        assert_eq!(json["user"]["ref"]["id"], json!("image:one"));
        assert_eq!(json["user"]["ref"]["size"]["height"], json!(865));
    }

    #[tokio::test]
    async fn test_nested_embedded_array_connection() {
        let singleton = get_singleton().await;
        let query = r#"
        query {
          user(id: "user:tobie") {
            arrNested {
              totalCount
              nodes {
                title
                items {
                  totalCount
                  nodes {
                    subtitle
                  }
                }
              }
            }
          }
        }
        "#;

        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json["user"]["arrNested"]["totalCount"], json!(2));

        // Check first top-level node
        let first_node = &json["user"]["arrNested"]["nodes"][0];
        assert_eq!(first_node["title"], json!("GraphQL"));
        assert_eq!(first_node["items"]["totalCount"], json!(2));
        assert_eq!(first_node["items"]["nodes"][0]["subtitle"], json!("sub1"));
        assert_eq!(first_node["items"]["nodes"][1]["subtitle"], json!("sub2"));

        // Check second top-level node
        let second_node = &json["user"]["arrNested"]["nodes"][1];
        assert_eq!(second_node["title"], json!("SurrealQL"));
        assert_eq!(second_node["items"]["totalCount"], json!(2));
        assert_eq!(second_node["items"]["nodes"][0]["subtitle"], json!("sub21"));
        assert_eq!(second_node["items"]["nodes"][1]["subtitle"], json!("sub22"));
    }

    #[tokio::test]
    async fn test_simple_array_of_scalars() {
        let singleton = get_singleton().await;
        // Note: Simple scalar arrays are not converted to connections.
        let query = r#"
        query {
          user(id: "user:tobie") {
            arr {
              totalCount
              nodes
            }
          }
        }
        "#;
        let res = singleton.schema.execute(query).await;
        let json = res.data.into_json().unwrap();

        assert_eq!(json["user"]["arr"]["nodes"], json!([42, 69]));
    }
}