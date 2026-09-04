/**
[env.capabilities]
allow-functions = true

[test]
reason = "eval is denied out of the box: with no allow-eval-query, even a system user cannot invoke it"

[[test.results]]
error = "Function 'eval::surql' is not allowed to be executed"
*/

RETURN eval::surql("RETURN 1");
