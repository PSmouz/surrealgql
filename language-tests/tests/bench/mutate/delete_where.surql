/**
[env]
imports = ["bench/util/records-10000.surql"]

[bench]
reason = "Delete all records matching a predicate"
run = true
rebuild = true
warmup = "2s"
sample-size = 10
measurement-time = "20s"

[test]
run = false
*/

DELETE record WHERE number = 21;
