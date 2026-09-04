/**
[env.capabilities]
allow-eval-query = ["system"]
deny-arbitrary-query = ["system"]

[test]
reason = "eval is also gated by the arbitrary-query capability: denying arbitrary queries for the subject denies eval even when eval is explicitly allowed (the gate cannot be bypassed)"

[[test.results]]
error = "Function 'eval::surql' is not allowed to be executed"
*/

RETURN eval::surql("RETURN 1");
