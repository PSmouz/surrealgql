/**
# 1000 `item` rows for the recursive COMPUTED-field bench. A computed field uses the
# `COMPUTED` clause: it is NOT stored — it is evaluated at READ time, every time the
# field is selected. Here `score COMPUTED fn::sum_to(n)` runs a recursive user
# function per record on each SELECT that projects `score`. (Contrast with the
# dynamic `VALUE` field in `recursion-items-dynamic-1000`, evaluated at write time.)
# `n` is 10..30 so the recursion depth stays under the default
# `max_computation_depth` (120). `run = false`.

[env]
imports = ["./recursion-functions.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE item SCHEMALESS;
	-- Computed field (read-time): resolved on every SELECT that projects it; never stored.
	DEFINE FIELD score ON item TYPE int COMPUTED fn::sum_to(n);

	CREATE |item:1..1000| SET n = rand::int(10, 30) RETURN NONE;

	RETURN 'OK';
}
