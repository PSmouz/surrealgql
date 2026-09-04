/**
[env]
imports = ["bench/util/recursion-items-computed-1000.surql"]

[bench]
reason = "Recursive computed field (read-time): SELECT resolves the `score COMPUTED fn::sum_to(n)` field for every record (1000 rows)"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT score FROM item;
