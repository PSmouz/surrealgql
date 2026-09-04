/**
[env]
namespace = true
database = true
imports = ["gql/_schema.surql"]
# GQL MATCH only runs on the streaming engine, so eval::gql requires a
# streaming planner strategy (same constraint as top-level GQL, which the
# harness routes away from compute-only for `.gql` files).
planner-strategy = ["all-ro", "best-effort-ro"]

[env.capabilities]
allow-eval-query = ["system"]
allow-experimental = ["gql"]

[test]
reason = "eval::gql runs an ISO GQL query from within SurrealQL on the streaming engine, in the same transaction"

[[test.results]]
value = "[{ name: 'A' }, { name: 'B' }, { name: 'C' }]"
*/

RETURN eval::gql("MATCH (n:person) RETURN n.name AS name ORDER BY name");
