/**
[env]
imports = ["bench/util/recursion-items-dynamic-1000.surql"]

[bench]
reason = "Recursive user function invoked per row at read time: fn::sum_to(n) projected over 1000 records"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT VALUE fn::sum_to(n) FROM item;
