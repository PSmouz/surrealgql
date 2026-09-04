/**
[env]
imports = ["bench/util/recursion-functions.surql"]

[bench]
reason = "Recursive user function: a single fn::fib(20) call (exponential branching recursion, ~13.5k self-calls)"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

RETURN fn::fib(20);
