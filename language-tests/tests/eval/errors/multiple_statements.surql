/**
[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = "eval::surql evaluates a single statement; bare multi-statement input is rejected and must be wrapped in a block"

[[test.results]]
error = 'There was a problem running the eval::surql() function. eval::surql evaluates a single statement; wrap multiple statements in a block, e.g. eval::surql("{ ... }")'
*/

RETURN eval::surql("CREATE foo:1; SELECT * FROM foo");
