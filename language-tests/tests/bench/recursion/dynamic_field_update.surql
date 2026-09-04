/**
[env]
imports = ["bench/util/recursion-items-dynamic-1000.surql"]

[bench]
reason = "Recursive dynamic field (write-time): UPDATE recomputes the `score VALUE fn::sum_to(n)` field for every record (1000 rows)"
run = true
rebuild = true
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

UPDATE item SET touched = true;
