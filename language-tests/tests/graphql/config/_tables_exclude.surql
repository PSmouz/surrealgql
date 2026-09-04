/**
[env]
imports = ["../_schema.surql"]

[test]
run = false
reason = "Excludes the event table from the GraphQL config"
*/
DEFINE CONFIG OVERWRITE GRAPHQL TABLES EXCLUDE event;
