/**
[env]
imports = ["../_schema.surql"]

[test]
run = false
reason = "Disables GraphQL introspection on top of the shared schema"
*/
DEFINE CONFIG OVERWRITE GRAPHQL TABLES AUTO INTROSPECTION NONE;
