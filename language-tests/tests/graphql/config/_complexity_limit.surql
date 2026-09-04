/**
[env]
imports = ["../_schema.surql"]

[test]
run = false
reason = "Configures a maximum GraphQL query complexity of 3"
*/
DEFINE CONFIG OVERWRITE GRAPHQL TABLES AUTO COMPLEXITY 3;
