/**
[env]
imports = ["../_schema.surql"]

[test]
run = false
reason = "Hides all functions from the GraphQL schema"
*/
DEFINE CONFIG OVERWRITE GRAPHQL TABLES AUTO FUNCTIONS NONE;
