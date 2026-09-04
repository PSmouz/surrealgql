/**
[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = "Transaction-control statements are rejected inside eval, since it runs nested in the caller's transaction"

[[test.results]]
error = "There was a problem running the eval::surql() function. only query statements may be evaluated; transaction-control and session statements (BEGIN, CANCEL, COMMIT, USE, LIVE, KILL, OPTION, SHOW, and access statements) are not allowed inside eval"
*/

RETURN eval::surql("BEGIN TRANSACTION; RETURN 1; COMMIT TRANSACTION");
