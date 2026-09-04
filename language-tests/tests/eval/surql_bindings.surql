/**
[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = "Caller-supplied bindings are bound as params for the nested query"

[[test.results]]
value = "5"
*/

RETURN eval::surql("RETURN $a + $b", { a: 2, b: 3 });
