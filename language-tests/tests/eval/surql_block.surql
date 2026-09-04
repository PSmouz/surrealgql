/**
[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = "Multiple statements are evaluated by wrapping them in a block, which threads LET bindings across statements and returns the final value"

[[test.results]]
value = "20"
*/

RETURN eval::surql("{ LET $x = 10; RETURN $x * 2 }");
