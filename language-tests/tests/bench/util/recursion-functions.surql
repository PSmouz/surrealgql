/**
# Recursive user-function definitions shared by the `recursion/` benches. Graph
# recursion is already covered (`graph/recursive`, `shortest_path`, `path_collect`);
# these are recursive *SurrealQL user functions* (a function calling itself):
#
#  - `fn::fib`     — exponential branching recursion (two self-calls per level);
#                    depth = n, call count grows ~φ^n. Stresses the call machinery.
#  - `fn::sum_to`  — linear recursion (one self-call per level); depth = n, exactly
#                    n calls. Predictable per-invocation cost, invoked per row both
#                    inline and via dynamic (`VALUE`) / computed (`COMPUTED`) fields.
#
# Recursion depth stays well under the default `max_computation_depth` (120).
# `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FUNCTION fn::fib($n: int) -> int {
		IF $n < 2 {
			RETURN $n;
		};
		RETURN fn::fib($n - 1) + fn::fib($n - 2);
	};

	DEFINE FUNCTION fn::sum_to($n: int) -> int {
		IF $n <= 0 {
			RETURN 0;
		};
		RETURN $n + fn::sum_to($n - 1);
	};

	RETURN 'OK';
}
