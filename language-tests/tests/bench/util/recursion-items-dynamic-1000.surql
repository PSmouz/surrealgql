/**
# 1000 `item` rows for the recursive DYNAMIC-field bench. A dynamic field uses the
# `VALUE` clause: it is evaluated at WRITE time (on create/update) and the result is
# stored on the record. Here `score VALUE fn::sum_to(n)` recomputes a recursive user
# function for every record on each write. (Contrast with the COMPUTED field in
# `recursion-items-computed-1000`, which evaluates at READ time.) `n` is 10..30: each
# recursion level consumes several `dive` units and the field evaluation adds its own
# frames, so the depth stays comfortably under the default `max_computation_depth`
# (120). `run = false`.

[env]
imports = ["./recursion-functions.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE item SCHEMALESS;
	-- Dynamic field (write-time): recomputed and stored on every create/update.
	DEFINE FIELD score ON item VALUE fn::sum_to(n);

	CREATE |item:1..1000| SET n = rand::int(10, 30) RETURN NONE;

	RETURN 'OK';
}
