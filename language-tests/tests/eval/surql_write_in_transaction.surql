/**
[env]
namespace = true
database = true

[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = "eval::surql runs writes in the caller's transaction; the created row is visible to a following statement"

[[test.results]]
value = "[{ id: person:1, name: 'A' }]"

[[test.results]]
value = "[{ name: 'A' }]"
*/

eval::surql("CREATE person:1 SET name = 'A'");
SELECT name FROM person;
