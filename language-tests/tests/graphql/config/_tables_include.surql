/**
[env]
imports = ["../_schema.surql"]

[test]
run = false
reason = "Restricts the GraphQL config to only expose the person table"
*/
DEFINE CONFIG OVERWRITE GRAPHQL TABLES INCLUDE person;
