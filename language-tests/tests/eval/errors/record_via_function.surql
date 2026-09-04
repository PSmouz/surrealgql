/**
[env]
imports = ["eval/_record_eval_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bob" }

[env.capabilities]
allow-eval-query = ["record", "system"]
deny-arbitrary-query = ["record"]

[test]
reason = "A record user calling an owner-defined function that calls eval is still seen as `record` (auth limiting never escalates the subject). eval is allowed for `record` here, but the arbitrary-query gate denies `record`, so the call is denied — proving the subject is not escalated to the definer's level."

[[test.results]]
error = "Function 'eval::surql' is not allowed to be executed"
*/

RETURN fn::run_eval();
