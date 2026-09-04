/**
[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = "eval runs in an isolated context: a local parameter at the call site is not visible to the evaluated query (only explicit bindings are)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'isolated'"
*/

LET $secret = 42;
RETURN eval::surql("RETURN $secret ?? 'isolated'");
