/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
run = false
reason = "Defines a table but no GraphQL config, so schema generation fails"
*/
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
CREATE person:alice SET name = "Alice";
