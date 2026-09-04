/**
[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = "eval::surql evaluates a nested SurrealQL query and returns the value of its final statement"

[[test.results]]
value = "2"
*/

RETURN eval::surql("RETURN 1 + 1");
