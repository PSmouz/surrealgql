/**
[env]
imports = ["../_schema.surql"]

[test]
run = false
reason = "Configures a maximum GraphQL query depth of 2"
*/
DEFINE CONFIG OVERWRITE GRAPHQL TABLES AUTO DEPTH 2;
