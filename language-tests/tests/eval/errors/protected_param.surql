/**
[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = "Caller bindings cannot overwrite protected params such as $session"

[[test.results]]
error = "'session' is a protected variable and cannot be set"
*/

RETURN eval::surql("RETURN $session", { session: 1 });
