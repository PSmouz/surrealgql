/**
[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = "A query evaluated by eval may itself call eval (within the depth limit)"

[[test.results]]
value = "3"
*/

RETURN eval::surql("RETURN eval::surql('RETURN 1 + 2')");
